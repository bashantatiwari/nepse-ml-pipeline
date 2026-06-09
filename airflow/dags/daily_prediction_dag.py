import json
import logging
from datetime import date, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "retries": 1,
}


def generate_and_save_prediction():
    from src.config.settings import PROCESSED_DATA_DIR
    from src.serving.prediction_service import PredictionService
    from src.storage.mariadb_client import MariaDBClient

    logger = logging.getLogger(__name__)

    # Ensure tables exist
    MariaDBClient().init_tables()

    service = PredictionService()
    result = service.predict_next_close()

    db_client = MariaDBClient()
    try:
        with db_client.get_connection() as conn:
            cursor = conn.cursor()
            query = """
                INSERT INTO predictions (
                    symbol, prediction_date, latest_close, predicted_next_close,
                    predicted_change, predicted_pct_change, model_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    latest_close=VALUES(latest_close),
                    predicted_next_close=VALUES(predicted_next_close),
                    predicted_change=VALUES(predicted_change),
                    predicted_pct_change=VALUES(predicted_pct_change),
                    model_version=VALUES(model_version)
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
        logger.error(f"Failed to save prediction to MariaDB: {e}")
        raise

    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_DATA_DIR / "latest_prediction.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=4)
    logger.info(f"Saved latest prediction backup to {out_path}")


with DAG(
    dag_id="daily_prediction_dag",
    default_args=default_args,
    schedule="0 18 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Daily pipeline: Ingest, preprocess, predict, monitor.",
) as dag:

    ingest_nabil_data = BashOperator(
        task_id="ingest_nabil_data",
        bash_command="cd /opt/airflow && python -m src.ingestion.load_to_mariadb",
    )

    preprocess_nabil_data = BashOperator(
        task_id="preprocess_nabil_data",
        bash_command="cd /opt/airflow && python -m src.preprocessing.feature_engineering",
    )

    generate_prediction = PythonOperator(
        task_id="generate_prediction",
        python_callable=generate_and_save_prediction,
    )

    run_monitoring = BashOperator(
        task_id="run_monitoring",
        bash_command="cd /opt/airflow && python -m src.monitoring.evidently_report",
    )

    ingest_nabil_data >> preprocess_nabil_data >> generate_prediction >> run_monitoring
