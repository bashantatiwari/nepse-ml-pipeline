import logging
import pandas as pd
from src.config.settings import PROCESSED_DATA_DIR, RAW_DATA_DIR
from src.storage.mariadb_client import MariaDBClient
from src.storage.redis_client import RedisClient
from src.utils.logger import get_logger
logger = get_logger(__name__)

def get_raw_data() -> pd.DataFrame:
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
    csv_file = RAW_DATA_DIR / "NABIL.csv"
    if csv_file.exists():
        df = pd.read_csv(csv_file)
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
    client = MariaDBClient()
    records = []
    for _, row in df.iterrows():
        pub_date = row["published_date"]
        if isinstance(pub_date, pd.Timestamp):
            pub_date = pub_date.date()
        records.append((
            row["symbol"],
            pub_date,
            None if pd.isna(row.get("open"))             else row.get("open"),
            None if pd.isna(row.get("high"))             else row.get("high"),
            None if pd.isna(row.get("low"))              else row.get("low"),
            None if pd.isna(row.get("close"))            else row.get("close"),
            None if pd.isna(row.get("percent_change"))   else row.get("percent_change"),
            None if pd.isna(row.get("traded_quantity"))  else row.get("traded_quantity"),
            None if pd.isna(row.get("traded_amount"))    else row.get("traded_amount"),
            None if pd.isna(row.get("status"))           else row.get("status"),
            None if pd.isna(row.get("price_range"))      else row.get("price_range"),
            None if pd.isna(row.get("daily_return"))     else row.get("daily_return"),
            None if pd.isna(row.get("close_lag_1"))      else row.get("close_lag_1"),
            None if pd.isna(row.get("close_lag_3"))      else row.get("close_lag_3"),
            None if pd.isna(row.get("close_lag_7"))      else row.get("close_lag_7"),
            None if pd.isna(row.get("rolling_mean_7"))   else row.get("rolling_mean_7"),
            None if pd.isna(row.get("rolling_mean_14"))  else row.get("rolling_mean_14"),
            None if pd.isna(row.get("rolling_std_7"))    else row.get("rolling_std_7"),
            None if pd.isna(row.get("next_close"))       else row.get("next_close"),
            None if pd.isna(row.get("target_change"))    else row.get("target_change"),
            None if pd.isna(row.get("target_pct_change")) else row.get("target_pct_change"),
            None if pd.isna(row.get("return_lag_1"))     else row.get("return_lag_1"),
            None if pd.isna(row.get("return_lag_3"))     else row.get("return_lag_3"),
            None if pd.isna(row.get("return_lag_7"))     else row.get("return_lag_7"),
            None if pd.isna(row.get("momentum_7"))       else row.get("momentum_7"),
            None if pd.isna(row.get("volatility_7"))     else row.get("volatility_7"),
            None if pd.isna(row.get("volume_change"))    else row.get("volume_change"),
        ))
    insert_query = """
        INSERT IGNORE INTO processed_nabil_features (
            symbol, published_date, open, high, low, close, percent_change,
            traded_quantity, traded_amount, status, price_range, daily_return,
            close_lag_1, close_lag_3, close_lag_7, rolling_mean_7, rolling_mean_14,
            rolling_std_7, next_close, target_change, target_pct_change,
            return_lag_1, return_lag_3, return_lag_7, momentum_7, volatility_7, volume_change
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with client.get_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany(insert_query, records)
        conn.commit()
        inserted = cursor.rowcount
        skipped = len(records) - inserted
        logger.info(f"Processed rows inserted: {inserted}, skipped (duplicates): {skipped}.")

def engineer_features():
    df = get_raw_data()
    if df.empty:
        return
    initial_rows = len(df)
    df["published_date"] = pd.to_datetime(df["published_date"])
    df = df.sort_values("published_date").reset_index(drop=True)

    # --- Price-level features ---
    df["price_range"] = df["high"] - df["low"]
    if "percent_change" in df.columns:
        df["daily_return"] = df["percent_change"]
    else:
        df["daily_return"] = df["close"].pct_change() * 100
    if "traded_quantity" not in df.columns and "volume" in df.columns:
        df["traded_quantity"] = df["volume"]
    if "traded_amount" not in df.columns and "turnover" in df.columns:
        df["traded_amount"] = df["turnover"]

    # --- Lagged close prices (safe: purely historical) ---
    df["close_lag_1"] = df["close"].shift(1)
    df["close_lag_3"] = df["close"].shift(3)
    df["close_lag_7"] = df["close"].shift(7)

    # --- Rolling features shifted by 1 to exclude same-day close ---
    df["rolling_mean_7"]  = df["close"].shift(1).rolling(window=7).mean()
    df["rolling_mean_14"] = df["close"].shift(1).rolling(window=14).mean()
    df["rolling_std_7"]   = df["close"].shift(1).rolling(window=7).std()

    # --- Stationary features for tree-based models ---
    daily_pct = df["close"].pct_change()                        # daily return as fraction
    df["return_lag_1"]  = daily_pct.shift(1)                   # yesterday's return
    df["return_lag_3"]  = daily_pct.shift(3)                   # 3-day-ago return
    df["return_lag_7"]  = daily_pct.shift(7)                   # 7-day-ago return
    df["momentum_7"]    = df["close"].shift(1) / df["close"].shift(7) - 1  # 7-day price momentum
    df["volatility_7"]  = daily_pct.shift(1).rolling(window=7).std()       # rolling return volatility
    df["volume_change"] = df["traded_quantity"].pct_change().shift(1)       # volume momentum

    # --- Target variables ---
    df["next_close"] = df["close"].shift(-1)
    df["target_change"] = df["next_close"] - df["close"]
    df["target_pct_change"] = 0.0
    valid_close = df["close"] > 0
    df.loc[valid_close, "target_pct_change"] = (
        df.loc[valid_close, "target_change"] / df.loc[valid_close, "close"]
    ) * 100

    df = df.dropna().reset_index(drop=True)
    final_rows = len(df)
    logger.info("Feature engineering completed.")
    logger.info(f"Rows before preprocessing: {initial_rows}")
    logger.info(f"Rows removed: {initial_rows - final_rows}")
    logger.info(f"Rows after preprocessing: {final_rows}")

    try:
        PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = PROCESSED_DATA_DIR / "nabil_features.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"Processed data saved to {csv_path}")
    except PermissionError as e:
        logger.warning(f"Could not write CSV (permission denied): {e}. Continuing without CSV.")

    save_to_mariadb(df)

    try:
        redis_client = RedisClient()
        redis_client.cache_dataframe("processed_nabil_features", df)
        logger.info("Processed data cached in Redis.")
    except Exception as e:
        logger.warning(f"Redis cache failed (non-fatal): {e}")

if __name__ == "__main__":
    engineer_features()
