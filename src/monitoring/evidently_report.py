import logging

import pandas as pd

from src.config.settings import PROCESSED_DATA_DIR, PROJECT_ROOT
from src.storage.mariadb_client import MariaDBClient

try:
    from evidently.metric_preset import DataDriftPreset, RegressionPreset
    from evidently.report import Report
    EVIDENTLY_AVAILABLE = True
except ImportError:
    EVIDENTLY_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def fetch_data() -> pd.DataFrame:
    """Fetch processed data from CSV first, fallback to MariaDB."""
    csv_path = PROCESSED_DATA_DIR / "nabil_features.csv"
    if csv_path.exists():
        logger.info(f"Loading data from {csv_path}")
        return pd.read_csv(csv_path)
    
    logger.info("CSV not found. Attempting to load from MariaDB...")
    db_client = MariaDBClient()
    try:
        with db_client.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM processed_nabil_features ORDER BY published_date ASC")
            rows = cursor.fetchall()
            if rows:
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                logger.info("Successfully loaded data from MariaDB.")
                return df
    except Exception as e:
        logger.error(f"Failed to load from MariaDB: {e}")
        
    return pd.DataFrame()


def run_evidently_monitoring():
    """Generates an Evidently HTML report comparing current vs reference data."""
    if not EVIDENTLY_AVAILABLE:
        logger.error("Evidently AI is not installed. Please install 'evidently>=0.4.0,<0.5.0'.")
        return

    df = fetch_data()
    if df.empty:
        logger.error("No data available to run monitoring.")
        return

    if "published_date" in df.columns:
        df["published_date"] = pd.to_datetime(df["published_date"])
        df = df.sort_values("published_date").reset_index(drop=True)

    # 1. Split Data Chronologically (80% Reference, 20% Current)
    # This emulates drift over time. In production, you might compare 
    # historical training data vs just the last 7 days of inference.
    split_index = int(len(df) * 0.8)
    reference_data = df.iloc[:split_index]
    current_data = df.iloc[split_index:]

    logger.info(f"Reference Data Rows (Historical): {len(reference_data)}")
    logger.info(f"Current Data Rows (Recent): {len(current_data)}")

    # 2. Configure Presets
    presets = [DataDriftPreset()]
    
    # Optional: If historic model predictions were saved back to this dataset, 
    # we could monitor regression drift. Usually this requires joining the predictions table.
    if "predicted_next_close" in df.columns and "next_close" in df.columns:
        logger.info("Prediction column found. Adding Regression Performance Monitoring.")
        presets.append(RegressionPreset())
    else:
        logger.info("Historic prediction column not found in dataset. Skipping Regression Performance Monitoring.")

    # 3. Generate Report
    logger.info("Generating Evidently AI Data Drift Report...")
    try:
        report = Report(metrics=presets)
        report.run(reference_data=reference_data, current_data=current_data)

        # 4. Save Report
        monitoring_dir = PROJECT_ROOT / "reports" / "monitoring"
        monitoring_dir.mkdir(parents=True, exist_ok=True)
        report_path = monitoring_dir / "nabil_data_drift_report.html"
        
        report.save_html(str(report_path))
        logger.info(f"Successfully saved Evidently monitoring HTML report to {report_path}")
    except Exception as e:
        logger.error(f"Failed to generate Evidently report: {e}")


if __name__ == "__main__":
    run_evidently_monitoring()
