import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# File path
# -----------------------------
CSV_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline/data/processed/nepse_merged_historical_data.csv"

# -----------------------------
# Database connection settings
# -----------------------------
DB_USER = "nepse"
DB_PASSWORD = "Nepse123"
DB_HOST = "localhost"
DB_PORT = 3308   # change if docker ps shows different port
DB_NAME = "nepse_db"

TABLE_NAME = "nepse_raw"

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# -----------------------------
# 1. Read CSV
# -----------------------------
df = pd.read_csv(CSV_PATH)

print("CSV loaded successfully")
print("Shape:", df.shape)

# -----------------------------
# 2. Clean values MySQL cannot accept
# -----------------------------
df = df.replace([np.inf, -np.inf], np.nan)

# Optional: check missing values after replacing inf
print("Missing values after replacing inf:")
print(df.isna().sum())

# Convert pandas NaN to Python None for SQL compatibility
df = df.where(pd.notnull(df), None)

# -----------------------------
# 3. Upload to MariaDB ColumnStore
# -----------------------------
df.to_sql(
    name=TABLE_NAME,
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=5000
)

print(f"Data ingestion completed. Table created: {TABLE_NAME}")
print(f"Rows inserted: {len(df)}")