import json
import logging
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from src.config.settings import PROCESSED_DATA_DIR, PROJECT_ROOT
from src.storage.columnstore_client import ColumnStoreClient

try:
    from evidently.metrics import (
        ColumnDriftMetric,
        DatasetDriftMetric,
        DatasetMissingValuesMetric,
        RegressionQualityMetric,
    )
    from evidently.report import Report
    EVIDENTLY_AVAILABLE = True
except ImportError:
    EVIDENTLY_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KEY_FEATURES = [
    "close", "open", "high", "low",
    "traded_quantity", "traded_amount",
    "rolling_mean_7", "rolling_mean_14",
    "rolling_std_7", "daily_return",
]

REFERENCE_DAYS = 180
CURRENT_DAYS   = 30
DRIFT_THRESHOLD = 0.3
RMSE_THRESHOLD  = 20.0


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_processed_data() -> pd.DataFrame:
    client = ColumnStoreClient()
    try:
        with client.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM processed_nabil_features ORDER BY published_date ASC"
            )
            rows = cursor.fetchall()
            if rows:
                cols = [d[0] for d in cursor.description]
                df = pd.DataFrame(rows, columns=cols)
                logger.info(f"Loaded {len(df)} rows from ColumnStore.")
                return df
    except Exception as e:
        logger.warning(f"ColumnStore failed: {e}. Trying CSV.")

    csv_path = PROCESSED_DATA_DIR / "nabil_features.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} rows from CSV fallback.")
        return df

    logger.error("No data source available.")
    return pd.DataFrame()


def fetch_predictions() -> pd.DataFrame:
    client = ColumnStoreClient()
    try:
        with client.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM predictions ORDER BY prediction_date ASC")
            rows = cursor.fetchall()
            if rows:
                cols = [d[0] for d in cursor.description]
                df = pd.DataFrame(rows, columns=cols)
                logger.info(f"Loaded {len(df)} prediction rows from ColumnStore.")
                return df
    except Exception as e:
        logger.warning(f"Could not load predictions from ColumnStore: {e}")
    return pd.DataFrame()


def make_sliding_windows(df: pd.DataFrame):
    df["published_date"] = pd.to_datetime(df["published_date"])
    df = df.sort_values("published_date").reset_index(drop=True)
    max_date = df["published_date"].max()
    current_start   = max_date - pd.Timedelta(days=CURRENT_DAYS)
    reference_end   = current_start
    reference_start = reference_end - pd.Timedelta(days=REFERENCE_DAYS)
    reference = df[
        (df["published_date"] > reference_start) &
        (df["published_date"] <= reference_end)
    ].copy()
    current = df[df["published_date"] > current_start].copy()
    logger.info(
        f"Sliding window — Reference: {len(reference)} rows, Current: {len(current)} rows"
    )
    if len(reference) < 30 or len(current) < 10:
        logger.warning("Sliding window too small. Falling back to 80/20 split.")
        split_idx = int(len(df) * 0.8)
        reference = df.iloc[:split_idx].copy()
        current   = df.iloc[split_idx:].copy()
    return reference, current


def extract_column_drift(metrics_list: list) -> dict:
    results = {}
    for m in metrics_list:
        if m.get("metric") == "ColumnDriftMetric":
            r = m.get("result", {})
            col = r.get("column_name", "unknown")
            results[col] = {
                "drift_detected": r.get("drift_detected", False),
                "drift_score":    round(r.get("drift_score", 0), 4),
                "stattest":       r.get("stattest_name", ""),
            }
    return results


def run_data_drift_report(reference, current, report_dir):
    logger.info("Running Data Drift check...")
    cols = [c for c in KEY_FEATURES if c in reference.columns]
    metrics = [DatasetDriftMetric(), DatasetMissingValuesMetric()]
    for col in cols:
        metrics.append(ColumnDriftMetric(column_name=col))
    report = Report(metrics=metrics)
    report.run(reference_data=reference[cols], current_data=current[cols])
    report.save_html(str(report_dir / "data_drift_report.html"))
    raw = report.as_dict()
    metrics_list = raw.get("metrics", [])
    dataset_drift = {}
    for m in metrics_list:
        if m.get("metric") == "DatasetDriftMetric":
            r = m.get("result", {})
            dataset_drift = {
                "drift_detected":  r.get("dataset_drift", False),
                "drift_share":     round(r.get("share_of_drifted_columns", 0), 4),
                "drifted_columns": r.get("number_of_drifted_columns", 0),
                "total_columns":   r.get("number_of_columns", 0),
            }
            break
    column_drift = extract_column_drift(metrics_list)
    retrain = dataset_drift.get("drift_share", 0) >= DRIFT_THRESHOLD
    return {
        "check": "data_drift", "timestamp": now_utc(),
        "dataset_drift": dataset_drift, "column_drift": column_drift,
        "retraining_recommended": retrain,
    }


def run_target_drift_report(reference, current, report_dir):
    if "next_close" not in reference.columns:
        return {"check": "target_drift", "skipped": True, "reason": "next_close missing"}
    logger.info("Running Target Drift check...")
    cols = [c for c in KEY_FEATURES if c in reference.columns] + ["next_close"]
    report = Report(metrics=[ColumnDriftMetric(column_name="next_close")])
    report.run(reference_data=reference[cols], current_data=current[cols])
    report.save_html(str(report_dir / "target_drift_report.html"))
    raw = report.as_dict()
    column_drift = extract_column_drift(raw.get("metrics", []))
    result = column_drift.get("next_close", {})
    retrain = result.get("drift_detected", False)
    return {
        "check": "target_drift", "timestamp": now_utc(),
        "target_column": "next_close", "result": result,
        "retraining_recommended": retrain,
    }


def run_regression_performance_report(features_df, report_dir):
    predictions_df = fetch_predictions()
    if predictions_df.empty:
        return {"check": "regression_performance", "skipped": True,
                "reason": "No prediction history in ColumnStore"}
    features_df = features_df.copy()
    features_df["published_date"] = pd.to_datetime(features_df["published_date"])
    predictions_df["prediction_date"] = pd.to_datetime(predictions_df["prediction_date"])
    features_sorted = features_df.sort_values("published_date").reset_index(drop=True)
    next_close_map = {}
    for i in range(len(features_sorted) - 1):
        d = features_sorted.loc[i, "published_date"]
        next_close_map[d] = features_sorted.loc[i + 1, "close"]
    predictions_df["actual_next_close"] = predictions_df["prediction_date"].map(next_close_map)
    merged = predictions_df.dropna(subset=["actual_next_close", "predicted_next_close"])
    if len(merged) < 5:
        return {"check": "regression_performance", "skipped": True,
                "reason": f"Only {len(merged)} joined rows (need ≥5)"}
    eval_df = pd.DataFrame({
        "target":     merged["actual_next_close"].values,
        "prediction": merged["predicted_next_close"].values,
    })
    half = max(1, len(eval_df) // 2)
    report = Report(metrics=[RegressionQualityMetric()])
    report.run(reference_data=eval_df.iloc[:half], current_data=eval_df.iloc[half:])
    report.save_html(str(report_dir / "regression_performance_report.html"))
    raw = report.as_dict()
    perf = {}
    for m in raw.get("metrics", []):
        if m.get("metric") == "RegressionQualityMetric":
            r = m.get("result", {}).get("current", {})
            perf = {
                "rmse": round(r.get("rmse", 0), 4),
                "mae":  round(r.get("mean_abs_error", 0), 4),
                "r2":   round(r.get("r2_score", 0), 4),
            }
            break
    retrain = perf.get("rmse", 0) > RMSE_THRESHOLD
    return {
        "check": "regression_performance", "timestamp": now_utc(),
        "metrics": perf, "retraining_recommended": retrain,
    }


def save_summary(summaries, report_dir):
    overall_retrain = any(
        s.get("retraining_recommended", False)
        for s in summaries
        if not s.get("skipped") and not s.get("error")
    )
    combined = {
        "generated_at": now_utc(),
        "retraining_recommended": overall_retrain,
        "checks": summaries,
    }
    json_path = report_dir / "monitoring_summary.json"
    with open(json_path, "w") as f:
        json.dump(combined, f, indent=2)
    logger.info(f"Summary saved → {json_path}")
    flag_path = report_dir / "RETRAIN_NEEDED"
    if overall_retrain:
        flag_path.touch()
    elif flag_path.exists():
        flag_path.unlink()
    return combined


def run_evidently_monitoring():
    if not EVIDENTLY_AVAILABLE:
        logger.error("Evidently not installed.")
        return
    df = fetch_processed_data()
    if df.empty:
        logger.error("No data. Aborting.")
        return
    reference, current = make_sliding_windows(df)
    report_dir = PROJECT_ROOT / "reports" / "monitoring"
    report_dir.mkdir(parents=True, exist_ok=True)
    summaries = []
    for name, fn, args in [
        ("data_drift",             run_data_drift_report,             (reference, current, report_dir)),
        ("target_drift",           run_target_drift_report,           (reference, current, report_dir)),
        ("regression_performance", run_regression_performance_report, (df, report_dir)),
    ]:
        try:
            summaries.append(fn(*args))
        except Exception as e:
            logger.error(f"{name} failed: {e}", exc_info=True)
            summaries.append({"check": name, "error": str(e)})
    combined = save_summary(summaries, report_dir)
    logger.info(f"Monitoring done. Retraining recommended: {combined['retraining_recommended']}")


if __name__ == "__main__":
    run_evidently_monitoring()
