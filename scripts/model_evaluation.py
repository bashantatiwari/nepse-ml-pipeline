import pandas as pd
import joblib
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report

MODEL_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline/models/nepse_model.pkl"
TEST_DATA_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline/data/processed/test_data.csv"
REPORT_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline/outputs/evaluation_report.txt"

model = joblib.load(MODEL_PATH)
print("Model loaded successfully")

test_df = pd.read_csv(TEST_DATA_PATH)
print("Test data loaded successfully")
print("Test data shape:", test_df.shape)

X_test = test_df.drop(columns=["target"])
y_test = test_df["target"]

y_pred = model.predict(X_test)

accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred, zero_division=0)
recall = recall_score(y_test, y_pred, zero_division=0)
f1 = f1_score(y_test, y_pred, zero_division=0)

report = f"""
NEPSE Model Evaluation Report
=============================

Model Task:
Predict whether the next trading day's closing price will increase or not.

Target:
1 = Next day's close price increased
0 = Next day's close price did not increase

Evaluation Metrics:
Accuracy  : {accuracy:.4f}
Precision : {precision:.4f}
Recall    : {recall:.4f}
F1 Score  : {f1:.4f}

Detailed Classification Report:
{classification_report(y_test, y_pred, zero_division=0)}
"""

print(report)

with open(REPORT_PATH, "w") as file:
    file.write(report)

print(f"Evaluation report saved to: {REPORT_PATH}")