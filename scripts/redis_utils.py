import pandas as pd
import redis
import pyarrow as pa
import pyarrow.ipc


def get_redis_connection():
    return redis.Redis(host="127.0.0.1", port=6379)


def save_dataframe_to_redis(df, key):
    redis_conn = get_redis_connection()

    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()

    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()

    redis_conn.set(key, sink.getvalue().to_pybytes())

    print(f"Dataframe saved to Redis with key: {key}")
    print("Shape saved:", df.shape)


def load_dataframe_from_redis(key):
    redis_conn = get_redis_connection()

    retrieved_data = redis_conn.get(key)

    if retrieved_data is None:
        raise ValueError(f"No data found in Redis for key: {key}")

    reader = pa.ipc.open_stream(pa.BufferReader(retrieved_data))
    table_restored = reader.read_all()
    df_restored = table_restored.to_pandas()

    print(f"Dataframe loaded from Redis with key: {key}")
    print("Shape loaded:", df_restored.shape)

    return df_restored