import pandas as pd
import joblib
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report

MODEL_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline/models/nepse_model_redis.pkl"
TEST_DATA_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline/data/processed/test_data_redis.csv"
REPORT_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline/outputs/evaluation_report_redis.txt"

model = joblib.load(MODEL_PATH)
test_df = pd.read_csv(TEST_DATA_PATH)

X_test = test_df.drop(columns=["target"])
y_test = test_df["target"]

y_pred = model.predict(X_test)

accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred, zero_division=0)
recall = recall_score(y_test, y_pred, zero_division=0)
f1 = f1_score(y_test, y_pred, zero_division=0)

report = f"""
NEPSE Redis Pipeline Model Evaluation Report
===========================================

Task:
Predict whether the next trading day's closing price will increase.

Target:
1 = Next day's close price increased
0 = Next day's close price did not increase

Accuracy  : {accuracy:.4f}
Precision : {precision:.4f}
Recall    : {recall:.4f}
F1 Score  : {f1:.4f}

Classification Report:
{classification_report(y_test, y_pred, zero_division=0)}
"""

print(report)

with open(REPORT_PATH, "w") as file:
    file.write(report)

print(f"Evaluation report saved to: {REPORT_PATH}")