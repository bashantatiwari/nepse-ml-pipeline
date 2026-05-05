import pandas as pd
from redis_utils import load_dataframe_from_redis, save_dataframe_to_redis

df = load_dataframe_from_redis("nepse_raw_df")

print("Preprocessing started")
print("Shape before preprocessing:", df.shape)

# Convert date
df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")

# Sort by company and date
df = df.sort_values(["symbol", "published_date"])

# Fill missing percentage change
df["per_change"] = df["per_change"].fillna(0)

# Remove missing important values
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

# Remove invalid price rows
price_cols = ["open", "high", "low", "close"]

for col in price_cols:
    df = df[df[col] > 0]

# Feature engineering
df["price_range"] = df["high"] - df["low"]
df["daily_return"] = (df["close"] - df["open"]) / df["open"]

# Create target inside each company group
df["next_close"] = df.groupby("symbol")["close"].shift(-1)
df["target"] = (df["next_close"] > df["close"]).astype(int)

# Remove rows where next_close is missing
df = df.dropna(subset=["next_close"])

save_dataframe_to_redis(df, "nepse_processed_df")

print("Preprocessing with Redis completed")
print("Shape after preprocessing:", df.shape)