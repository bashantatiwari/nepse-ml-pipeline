import pandas as pd
import joblib
from redis_utils import load_dataframe_from_redis
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

MODEL_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline/models/nepse_model_redis.pkl"
TEST_DATA_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline/data/processed/test_data_redis.csv"

df = load_dataframe_from_redis("nepse_processed_df")

print("Processed data loaded from Redis")
print("Shape:", df.shape)

feature_cols = [
    "open",
    "high",
    "low",
    "close",
    "per_change",
    "traded_quantity",
    "traded_amount",
    "price_range",
    "daily_return"
]

available_features = [col for col in feature_cols if col in df.columns]

X = df[available_features]
y = df["target"]

data = X.copy()
data["target"] = y
data = data.dropna()

X = data[available_features]
y = data["target"]

print("Features used:", available_features)
print("Training data shape:", X.shape)

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

model = RandomForestClassifier(
    n_estimators=100,
    random_state=42
)

model.fit(X_train, y_train)

joblib.dump(model, MODEL_PATH)

test_df = X_test.copy()
test_df["target"] = y_test
test_df.to_csv(TEST_DATA_PATH, index=False)

print("Redis-based model training completed")
print(f"Model saved to: {MODEL_PATH}")
print(f"Test data saved to: {TEST_DATA_PATH}")