import pandas as pd
import redis
import pyarrow as pa
import pyarrow.ipc

redis_conn = redis.Redis(host="127.0.0.1", port=6379)

df = pd.read_csv("/home/bashanta/Desktop/nepse-ml-pipeline/data/processed/nepse_merged_historical_data.csv")
sample_df = df.head(100)

table = pa.Table.from_pandas(sample_df)
sink = pa.BufferOutputStream()
writer = pa.ipc.new_stream(sink, table.schema)
writer.write_table(table)
writer.close()

redis_conn.set("nepse_dataframe_sample", sink.getvalue().to_pybytes())

retrieved_data = redis_conn.get("nepse_dataframe_sample")
reader = pa.ipc.open_stream(pa.BufferReader(retrieved_data))
table_restored = reader.read_all()
df_restored = table_restored.to_pandas()

print("Original dataframe shape:", sample_df.shape)
print("Restored dataframe shape:", df_restored.shape)
print(df_restored.head())