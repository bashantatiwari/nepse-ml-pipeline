import sys
import time
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from src.constants.companyIdMap import companyIdMap
from src.constants.url import historyUrl
from src.utils.params import getParams


RAW_COMPANY_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "company-wise"


CONNECT_TIMEOUT = 30
READ_TIMEOUT = 90
PAGE_SIZE = 50
MAX_RETRIES = 5


def get_session(company_symbol: str) -> requests.Session:
    """
    Create a fresh ShareSansar session for one company.
    This first visits the company page to collect XSRF cookie.
    """

    session = requests.Session()

    company_page_url = f"https://www.sharesansar.com/company/{company_symbol}"

    user_agent = (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    initial_headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    response = session.get(
        company_page_url,
        headers=initial_headers,
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )

    response.raise_for_status()

    xsrf_token = session.cookies.get("XSRF-TOKEN")

    if xsrf_token is None:
        print("Available cookies:", session.cookies.get_dict())
        raise ValueError("XSRF-TOKEN cookie not found.")

    xsrf_token = unquote(xsrf_token)

    ajax_headers = {
        "User-Agent": user_agent,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.sharesansar.com",
        "Referer": company_page_url,
        "X-XSRF-TOKEN": xsrf_token,
    }

    session.headers.update(ajax_headers)

    return session


def create_session_with_retry(company_symbol: str):
    """
    Try creating session multiple times.
    If ShareSansar is slow or internet drops, it retries instead of crashing.
    """

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Creating session for {company_symbol}... attempt {attempt}/{MAX_RETRIES}")
            return get_session(company_symbol)

        except requests.exceptions.RequestException as e:
            print(f"Session connection failed for {company_symbol}")
            print(e)

        except Exception as e:
            print(f"Unexpected session error for {company_symbol}")
            print(e)

        wait_time = attempt * 10
        print(f"Waiting {wait_time} seconds before retrying...")
        time.sleep(wait_time)

    return None


def is_existing_csv_valid(csv_path: Path) -> bool:
    """
    Check whether a CSV already exists and has data.
    This is useful when you copy old CSV files from GitHub.
    """

    if not csv_path.exists():
        return False

    try:
        df = pd.read_csv(csv_path)

        if df.empty:
            print(f"Existing CSV is empty: {csv_path.name}")
            return False

        return True

    except Exception as e:
        print(f"Existing CSV is unreadable/corrupted: {csv_path.name}")
        print(e)
        return False


def clean_page_dataframe(page_data: list) -> pd.DataFrame:
    """
    Clean one page of historical data.
    """

    df = pd.DataFrame(page_data)

    if df.empty:
        return df

    if "DT_Row_Index" in df.columns:
        df = df.drop(columns=["DT_Row_Index"])

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    return df


def post_with_retry(session, company_symbol, company_id, start, size):
    """
    POST request with retry.
    Handles timeout and CSRF token expiry.
    """

    data_params = getParams(start, size, company_id)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(
                historyUrl,
                data=data_params,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )

            if response.status_code == 419:
                print(f"CSRF token expired for {company_symbol}. Refreshing session...")
                session = create_session_with_retry(company_symbol)

                if session is None:
                    return None, None

                response = session.post(
                    historyUrl,
                    data=data_params,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                )

            if response.status_code == 200:
                return response, session

            print(f"Non-200 response for {company_symbol} at start={start}")
            print("Status code:", response.status_code)
            print(response.text[:300])

        except requests.exceptions.RequestException as e:
            print(f"POST request failed for {company_symbol} at start={start}")
            print(e)

        wait_time = attempt * 10
        print(f"Waiting {wait_time} seconds before retrying POST...")
        time.sleep(wait_time)

    return None, session


def fetch_total_records(session, company_symbol: str, company_id: int):
    """
    Fetch total number of records for one company.
    """

    response, session = post_with_retry(
        session=session,
        company_symbol=company_symbol,
        company_id=company_id,
        start=1,
        size=1,
    )

    if response is None:
        return None, session

    json_response = response.json()
    total_records = json_response.get("recordsTotal", 0)

    return total_records, session


def fetch_company_historical_data(company_symbol: str, company_id: int) -> None:
    print(f"Collecting historical data for {company_symbol}...")
    print("........................................")

    RAW_COMPANY_DATA_DIR.mkdir(parents=True, exist_ok=True)

    final_output_path = RAW_COMPANY_DATA_DIR / f"{company_symbol}.csv"
    partial_output_path = RAW_COMPANY_DATA_DIR / f"{company_symbol}.part.csv"
    checkpoint_path = RAW_COMPANY_DATA_DIR / f"{company_symbol}.checkpoint.txt"

    # 1. Skip if CSV already exists
    # This will work for CSV files you copied from GitHub.
    if is_existing_csv_valid(final_output_path):
        print(f"{company_symbol}.csv already exists and has data. Skipping...")
        print("........................................")
        return

    # 2. If CSV exists but is empty/corrupted, remove it
    if final_output_path.exists():
        print(f"Removing invalid CSV: {final_output_path.name}")
        final_output_path.unlink()

    # 3. Create ShareSansar session safely
    session = create_session_with_retry(company_symbol)

    if session is None:
        print(f"Could not connect to ShareSansar for {company_symbol}. Skipping for now.")
        print("Run the script again later.")
        print("........................................")
        return

    # 4. Get total records
    total_records, session = fetch_total_records(
        session=session,
        company_symbol=company_symbol,
        company_id=company_id,
    )

    if total_records is None:
        print(f"Could not fetch total records for {company_symbol}. Skipping for now.")
        print("........................................")
        return

    if total_records == 0:
        print(f"No records found for {company_symbol}")
        print("........................................")
        return

    print(f"Total records found for {company_symbol}: {total_records}")

    # 5. Resume from checkpoint if available
    if checkpoint_path.exists():
        try:
            start = int(checkpoint_path.read_text().strip())
            print(f"Resuming {company_symbol} from start={start}")
        except Exception:
            print(f"Invalid checkpoint for {company_symbol}. Restarting company.")
            start = 1
            checkpoint_path.unlink(missing_ok=True)
            partial_output_path.unlink(missing_ok=True)
    else:
        start = 1

    # 6. Fetch page by page
    while start <= total_records:
        response, session = post_with_retry(
            session=session,
            company_symbol=company_symbol,
            company_id=company_id,
            start=start,
            size=PAGE_SIZE,
        )

        if response is None:
            print(f"Could not fetch page start={start} for {company_symbol}.")
            print("Progress is saved. Run the script again later.")
            print("........................................")
            return

        page_data = response.json().get("data", [])

        if not page_data:
            print(f"No more data found for {company_symbol}")
            break

        page_df = clean_page_dataframe(page_data)

        if page_df.empty:
            print(f"Empty page received for {company_symbol} at start={start}")
            break

        # Save every page immediately
        if partial_output_path.exists():
            page_df.to_csv(
                partial_output_path,
                mode="a",
                header=False,
                index=False,
            )
        else:
            page_df.to_csv(
                partial_output_path,
                index=False,
            )

        print(f"Saved {company_symbol} page starting from row {start}")

        start += PAGE_SIZE
        checkpoint_path.write_text(str(start))

        # Delay between pages
        time.sleep(2)

    # 7. Finalize partial CSV
    if not partial_output_path.exists():
        print(f"No data collected for {company_symbol}")
        print("........................................")
        return

    df = pd.read_csv(partial_output_path)

    if df.empty:
        print(f"Partial CSV is empty for {company_symbol}")
        print("........................................")
        return

    df = df.drop_duplicates()

    # Oldest date first
    df = df.iloc[::-1].reset_index(drop=True)

    df.to_csv(final_output_path, index=False)

    partial_output_path.unlink(missing_ok=True)
    checkpoint_path.unlink(missing_ok=True)

    print(f"Saved final {company_symbol} data to: {final_output_path}")
    print("Collection completed.")
    print("........................................")


def main() -> None:
    for company_symbol, company_id in companyIdMap.items():
        fetch_company_historical_data(company_symbol, company_id)

        # Delay between companies so website is not overloaded
        print("Waiting 5 seconds before next company...")
        time.sleep(5)


if __name__ == "__main__":
    main()