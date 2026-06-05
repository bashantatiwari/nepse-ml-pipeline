import logging

import pandas as pd

from src.config.settings import PROCESSED_DATA_DIR, RAW_DATA_DIR
from src.storage.mariadb_client import MariaDBClient
from src.storage.redis_client import RedisClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_raw_data() -> pd.DataFrame:
    """Fetch raw NABIL data from MariaDB, falling back to CSV if unavailable."""
    db_client = MariaDBClient()
    try:
        with db_client.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM raw_nabil_prices ORDER BY published_date ASC")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            
            if not df.empty:
                logger.info(f"Successfully loaded {len(df)} rows from MariaDB.")
                return df
    except Exception as e:
        logger.warning(f"Could not load from MariaDB: {e}. Falling back to CSV.")
    
    # Fallback to CSV
    csv_file = RAW_DATA_DIR / "NABIL.csv"
    if csv_file.exists():
        df = pd.read_csv(csv_file)
        
        # Clean column names
        df.columns = (
            df.columns.str.strip().str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace("per_change", "percent_change", regex=False)
        )
             
        df["symbol"] = "NABIL"
        logger.info(f"Successfully loaded {len(df)} rows from CSV fallback.")
        return df
    
    logger.error("Could not load data from MariaDB or CSV.")
    return pd.DataFrame()


def save_to_mariadb(df: pd.DataFrame):
    """Save processed features to MariaDB."""
    client = MariaDBClient()
    try:
        with client.get_connection() as conn:
            cursor = conn.cursor()
            
            # Fetch existing dates to prevent duplicates in Python
            cursor.execute("SELECT published_date FROM processed_nabil_features WHERE symbol = 'NABIL'")
            existing_dates = {row[0] for row in cursor.fetchall()}
            
            records_to_insert = []
            skipped_count = 0
            
            for _, row in df.iterrows():
                # Ensure date is compared correctly
                pub_date = row["published_date"]
                if isinstance(pub_date, pd.Timestamp):
                    pub_date = pub_date.date()
                    
                if pub_date in existing_dates:
                    skipped_count += 1
                else:
                    records_to_insert.append((
                        row["symbol"],
                        pub_date,
                        row.get("open"),
                        row.get("high"),
                        row.get("low"),
                        row.get("close"),
                        row.get("percent_change"),
                        row.get("traded_quantity"),
                        row.get("traded_amount"),
                        row.get("status"),
                        row.get("price_range"),
                        row.get("daily_return"),
                        row.get("close_lag_1"),
                        row.get("close_lag_3"),
                        row.get("close_lag_7"),
                        row.get("rolling_mean_7"),
                        row.get("rolling_mean_14"),
                        row.get("rolling_std_7"),
                        row.get("next_close"),
                        row.get("target_change"),
                        row.get("target_pct_change")
                    ))
            
            if not records_to_insert:
                logger.info("No new processed rows to insert into MariaDB.")
                return

            insert_query = """
                INSERT INTO processed_nabil_features (
                    symbol, published_date, open, high, low, close, percent_change,
                    traded_quantity, traded_amount, status, price_range, daily_return,
                    close_lag_1, close_lag_3, close_lag_7, rolling_mean_7, rolling_mean_14,
                    rolling_std_7, next_close, target_change, target_pct_change
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.executemany(insert_query, records_to_insert)
            conn.commit()
            logger.info(f"Successfully inserted {cursor.rowcount} processed rows into MariaDB. Skipped {skipped_count}.")
    except Exception as e:
        logger.error(f"Failed to insert processed data into MariaDB (will continue to CSV/Redis): {e}")


def engineer_features():
    """Main pipeline function to process time-series data."""
    df = get_raw_data()
    if df.empty:
        return
        
    initial_rows = len(df)
        
    df["published_date"] = pd.to_datetime(df["published_date"])
    df = df.sort_values("published_date").reset_index(drop=True)
    
    # Standard Features
    df["price_range"] = df["high"] - df["low"]
    
    # Handle percentage change (could be named per_change or percent_change)
    if "percent_change" in df.columns:
        df["daily_return"] = df["percent_change"]
    else:
        df["daily_return"] = df["close"].pct_change() * 100
        
    # Standardize volume/turnover names if they are different
    if "traded_quantity" not in df.columns and "volume" in df.columns:
        df["traded_quantity"] = df["volume"]
    if "traded_amount" not in df.columns and "turnover" in df.columns:
        df["traded_amount"] = df["turnover"]
    
    # Lag Features
    df["close_lag_1"] = df["close"].shift(1)
    df["close_lag_3"] = df["close"].shift(3)
    df["close_lag_7"] = df["close"].shift(7)
    
    # Rolling Metrics
    df["rolling_mean_7"] = df["close"].rolling(window=7).mean()
    df["rolling_mean_14"] = df["close"].rolling(window=14).mean()
    df["rolling_std_7"] = df["close"].rolling(window=7).std()
    
    # Regression Target Creation
    df["next_close"] = df["close"].shift(-1)
    df["target_change"] = df["next_close"] - df["close"]
    
    # Safe division to prevent dividing by zero if close is 0
    df["target_pct_change"] = 0.0
    valid_close = df["close"] > 0
    df.loc[valid_close, "target_pct_change"] = (df.loc[valid_close, "target_change"] / df.loc[valid_close, "close"]) * 100
    
    # Drop rows with NaN values generated by lags and rolling windows (and the last row with missing next_close)
    df = df.dropna().reset_index(drop=True)
    
    final_rows = len(df)
    rows_removed = initial_rows - final_rows
    
    logger.info("Feature engineering completed.")
    logger.info(f"Rows before preprocessing: {initial_rows}")
    logger.info(f"Rows removed (due to NA lags, rolling windows, or missing future target): {rows_removed}")
    logger.info(f"Rows after preprocessing (modeling ready): {final_rows}")
    
    # Save to Local CSV
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = PROCESSED_DATA_DIR / "nabil_features.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"Processed data saved to {csv_path}")
    
    # Save to MariaDB
    save_to_mariadb(df)
    
    # Save to Redis Cache
    redis_client = RedisClient()
    redis_client.cache_dataframe("processed_nabil_features", df)


if __name__ == "__main__":
    engineer_features()
