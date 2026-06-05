import json
import logging
from datetime import date, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def generate_and_save_prediction():
    """Generates the daily prediction and saves it to MariaDB or local JSON."""
    # Dynamic imports to prevent DAG parsing errors if dependencies are missing on the scheduler
    from src.config.settings import PROCESSED_DATA_DIR
    from src.serving.prediction_service import PredictionService
    from src.storage.mariadb_client import MariaDBClient
    
    logger = logging.getLogger(__name__)
    service = PredictionService()
    
    # Generate the prediction
    result = service.predict_next_close()
    
    # Attempt to save to MariaDB predictions table
    db_client = MariaDBClient()
    try:
        with db_client.get_connection() as conn:
            cursor = conn.cursor()
            query = """
                INSERT INTO predictions (
                    symbol, prediction_date, latest_close, predicted_next_close,
                    predicted_change, predicted_pct_change, model_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(query, (
                result["company"],
                date.today(),
                result["latest_close"],
                result["predicted_next_close"],
                result["predicted_change"],
                result["predicted_pct_change"],
                result["model_version"]
            ))
            conn.commit()
            logger.info("Saved prediction successfully to MariaDB.")
    except Exception as e:
        logger.warning(f"Failed to save prediction to MariaDB: {e}. Falling back to JSON.")
        
    # Always save a local JSON copy as fallback/easy access
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_DATA_DIR / "latest_prediction.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=4)
    logger.info(f"Saved latest prediction backup to {out_path}")


default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="daily_prediction_dag",
    default_args=default_args,
    schedule="0 18 * * *",  # Run daily at 18:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Daily pipeline: Ingest raw data, preprocess features, generate prediction, and trigger monitoring.",
) as dag:

    ingest_nabil_data = BashOperator(
        task_id="ingest_nabil_data",
        bash_command="cd /opt/airflow && python -m src.ingestion.load_to_mariadb || python -m src.ingestion.load_to_mariadb"
    )

    preprocess_nabil_data = BashOperator(
        task_id="preprocess_nabil_data",
        bash_command="cd /opt/airflow && python -m src.preprocessing.feature_engineering || python -m src.preprocessing.feature_engineering"
    )

    generate_prediction = PythonOperator(
        task_id="generate_prediction",
        python_callable=generate_and_save_prediction
    )

    # Placeholder for Batch 7 Evidently step
    monitoring_placeholder = BashOperator(
        task_id="monitoring_placeholder",
        bash_command="echo 'Monitoring step to be implemented'"
    )

    ingest_nabil_data >> preprocess_nabil_data >> generate_prediction >> monitoring_placeholder
