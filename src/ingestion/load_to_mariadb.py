import logging
import pandas as pd
from src.config.settings import RAW_DATA_DIR
from src.storage.columnstore_client import ColumnStoreClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace("per_change", "percent_change", regex=False)
    )
    return df


def load_data():
    csv_file = RAW_DATA_DIR / "NABIL.csv"
    if not csv_file.exists():
        logger.error(f"File not found: {csv_file}")
        return

    logger.info(f"Reading data from {csv_file}")
    df = pd.read_csv(csv_file)
    df = clean_column_names(df)
    df["symbol"] = "NABIL"
    df["published_date"] = pd.to_datetime(
        df["published_date"], errors="coerce"
    ).dt.date
    df = df.dropna(subset=["published_date", "close"])
    df = df.drop_duplicates(subset=["symbol", "published_date"], keep="last")

    # Use ColumnStore for warehouse table
    client = ColumnStoreClient()
    client.init_tables()

    logger.info(f"Total rows to process: {len(df)}")

    # ColumnStore has no UNIQUE KEY — deduplicate at application layer
    # Check existing dates and skip them
    with client.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT published_date FROM raw_nabil_prices WHERE symbol = 'NABIL'")
        existing_dates = {row[0] for row in cursor.fetchall()}

    new_df = df[~df["published_date"].isin(existing_dates)]
    logger.info(f"New rows to insert: {len(new_df)}, Skipped (duplicates): {len(df) - len(new_df)}")

    if new_df.empty:
        logger.info("No new rows to insert.")
        return

    records = []
    for _, row in new_df.iterrows():
        records.append((
            row["symbol"],
            row["published_date"],
            None if pd.isna(row.get("open"))           else row.get("open"),
            None if pd.isna(row.get("high"))           else row.get("high"),
            None if pd.isna(row.get("low"))            else row.get("low"),
            None if pd.isna(row.get("close"))          else row.get("close"),
            None if pd.isna(row.get("percent_change")) else row.get("percent_change"),
            None if pd.isna(row.get("traded_quantity"))else row.get("traded_quantity"),
            None if pd.isna(row.get("traded_amount"))  else row.get("traded_amount"),
            None if pd.isna(row.get("status"))         else row.get("status"),
        ))

    insert_query = """
        INSERT INTO raw_nabil_prices (
            symbol, published_date, open, high, low, close,
            percent_change, traded_quantity, traded_amount, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with client.get_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany(insert_query, records)
        logger.info(f"Inserted {len(records)} rows into ColumnStore raw_nabil_prices.")


if __name__ == "__main__":
    load_data()
