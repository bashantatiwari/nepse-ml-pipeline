import numpy as np
import pandas as pd
from redis_utils import save_dataframe_to_redis

CSV_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline/data/processed/nepse_merged_historical_data.csv"

df = pd.read_csv(CSV_PATH)

print("CSV loaded successfully")
print("Original shape:", df.shape)

df = df.replace([np.inf, -np.inf], np.nan)

sample_df = df.head(10000)

save_dataframe_to_redis(sample_df, "nepse_raw_df")

print("Data ingestion with Redis completed.")