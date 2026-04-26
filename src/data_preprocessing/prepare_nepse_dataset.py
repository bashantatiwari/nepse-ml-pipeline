import sys
from pathlib import Path

import pandas as pd


# Project root = nepse-ml-pipeline/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))


RAW_COMPANY_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "company-wise"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

OUTPUT_FILE = PROCESSED_DATA_DIR / "nepse_merged_historical_data.csv"


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names so all company CSV files have consistent headers.
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(".", "", regex=False)
    )

    return df


def prepare_nepse_dataset() -> None:
    """
    Merge all company-wise CSV files into one big table.

    Input:
        data/raw/company-wise/*.csv

    Output:
        data/processed/nepse_merged_historical_data.csv
    """

    if not RAW_COMPANY_DATA_DIR.exists():
        print(f"Raw company-wise data folder does not exist: {RAW_COMPANY_DATA_DIR}")
        return

    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(RAW_COMPANY_DATA_DIR.glob("*.csv"))

    if not csv_files:
        print(f"No CSV files found inside: {RAW_COMPANY_DATA_DIR}")
        return

    all_dataframes = []

    print(f"Found {len(csv_files)} CSV files.")
    print("Merging company-wise CSV files...")
    print("........................................")

    for csv_file in csv_files:
        company_symbol = csv_file.stem

        try:
            df = pd.read_csv(csv_file)

            if df.empty:
                print(f"Skipping empty file: {csv_file.name}")
                continue

            df = clean_column_names(df)

            # Add company symbol column so we know which company each row belongs to
            df["symbol"] = company_symbol

            all_dataframes.append(df)

            print(f"Loaded {csv_file.name}: {len(df)} rows")

        except Exception as e:
            print(f"Failed to read {csv_file.name}")
            print(e)

    if not all_dataframes:
        print("No valid dataframes found to merge.")
        return

    merged_df = pd.concat(all_dataframes, ignore_index=True)

    # Remove exact duplicate rows
    before_duplicates = len(merged_df)
    merged_df = merged_df.drop_duplicates()
    after_duplicates = len(merged_df)

    removed_duplicates = before_duplicates - after_duplicates

    # Try to sort by symbol and date if published_date exists
    if "published_date" in merged_df.columns:
        merged_df["published_date"] = pd.to_datetime(
            merged_df["published_date"],
            errors="coerce"
        )

        merged_df = merged_df.sort_values(
            by=["symbol", "published_date"],
            ascending=[True, True]
        ).reset_index(drop=True)

    else:
        merged_df = merged_df.sort_values(
            by=["symbol"],
            ascending=True
        ).reset_index(drop=True)

    merged_df.to_csv(OUTPUT_FILE, index=False)

    print("........................................")
    print("Merge completed successfully.")
    print(f"Total CSV files found: {len(csv_files)}")
    print(f"Valid files merged: {len(all_dataframes)}")
    print(f"Total rows before removing duplicates: {before_duplicates}")
    print(f"Duplicate rows removed: {removed_duplicates}")
    print(f"Final rows: {len(merged_df)}")
    print(f"Total companies: {merged_df['symbol'].nunique()}")
    print(f"Saved merged dataset to: {OUTPUT_FILE}")


if __name__ == "__main__":
    prepare_nepse_dataset()