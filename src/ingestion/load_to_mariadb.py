import logging

import pandas as pd

from src.config.settings import RAW_DATA_DIR
from src.storage.mariadb_client import MariaDBClient

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

    # Convert date column
    df["published_date"] = pd.to_datetime(
        df["published_date"],
        errors="coerce"
    ).dt.date

    # Drop rows missing critical values
    df = df.dropna(subset=["published_date", "close"])

    client = MariaDBClient()

    try:
        client.init_tables()
    except Exception as e:
        logger.error(f"Failed to initialize tables: {e}")
        return

    logger.info(f"Total rows read from CSV: {len(df)}")

    try:
        with client.get_connection() as conn:
            cursor = conn.cursor()

            # Get existing dates to avoid duplicates
            cursor.execute(
                "SELECT published_date FROM raw_nabil_prices WHERE symbol = 'NABIL'"
            )
            existing_dates = {row[0] for row in cursor.fetchall()}

            records_to_insert = []
            skipped_count = 0

            for _, row in df.iterrows():
                if row["published_date"] in existing_dates:
                    skipped_count += 1
                    continue

                record = tuple(
                    None if pd.isna(v) else v
                    for v in (
                        row["symbol"],
                        row["published_date"],
                        row.get("open"),
                        row.get("high"),
                        row.get("low"),
                        row.get("close"),
                        row.get("percent_change"),
                        row.get("traded_quantity"),
                        row.get("traded_amount"),
                        row.get("status"),
                    )
                )

                records_to_insert.append(record)

            logger.info(f"Rows skipped (already exist): {skipped_count}")

            if not records_to_insert:
                logger.info("No new rows to insert.")
                return

            insert_query = """
                INSERT INTO raw_nabil_prices (
                    symbol,
                    published_date,
                    open,
                    high,
                    low,
                    close,
                    percent_change,
                    traded_quantity,
                    traded_amount,
                    status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            cursor.executemany(insert_query, records_to_insert)
            conn.commit()

            logger.info(
                f"Rows successfully inserted: {cursor.rowcount}"
            )

    except Exception as e:
        logger.error(f"Failed during database operation: {e}")
        raise


if __name__ == "__main__":
    load_data()