import pandas as pd
from sqlalchemy import create_engine

DB_USER = "nepse"
DB_PASSWORD = "Nepse123"
DB_HOST = "localhost"
DB_PORT = 3308
DB_NAME = "nepse_db"

RAW_TABLE = "nepse_raw"
PROCESSED_TABLE = "nepse_processed"

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", con=engine)

print("Raw data loaded from database")
print("Shape before preprocessing:", df.shape)

df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")

df = df.sort_values(["symbol", "published_date"])

df["per_change"] = df["per_change"].fillna(0)

important_cols = [
    "published_date",
    "open",
    "high",
    "low",
    "close",
    "traded_quantity",
    "traded_amount",
    "symbol"
]

df = df.dropna(subset=important_cols)

price_cols = ["open", "high", "low", "close"]

for col in price_cols:
    df = df[df[col] > 0]

df["price_range"] = df["high"] - df["low"]
df["daily_return"] = (df["close"] - df["open"]) / df["open"]

df["next_close"] = df.groupby("symbol")["close"].shift(-1)

df["target"] = (df["next_close"] > df["close"]).astype(int)

df = df.dropna(subset=["next_close"])

df.to_sql(
    PROCESSED_TABLE,
    con=engine,
    if_exists="replace",
    index=False
)

print("Preprocessing completed")
print("Shape after preprocessing:", df.shape)
print(f"Processed data saved to database table: {PROCESSED_TABLE}")