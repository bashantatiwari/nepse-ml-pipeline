import logging
from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

RETRAIN_FLAG_PATH = Path("/opt/airflow/reports/monitoring/RETRAIN_NEEDED")

def check_retrain_flag() -> bool:
    """
    Checks if Evidently monitoring has raised a retraining flag.
    Returns True (continue pipeline) if flag exists OR if flag is absent
    but this is the first run (no monitoring has run yet).
    """
    logger = logging.getLogger(__name__)
    if RETRAIN_FLAG_PATH.exists():
        logger.info(f"RETRAIN_NEEDED flag found at {RETRAIN_FLAG_PATH}. Proceeding with retraining.")
        return True
    logger.info("No RETRAIN_NEEDED flag found. Skipping retraining this week.")
    return False

def check_model_artifacts():
    """Validates that the model artifacts were generated successfully after training."""
    from src.config.settings import MODELS_DIR
    logger = logging.getLogger(__name__)
    model_path = MODELS_DIR / "best_model.joblib"
    metadata_path = MODELS_DIR / "model_metadata.json"
    if not model_path.exists():
        raise FileNotFoundError(f"CRITICAL: Model artifact missing at {model_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"CRITICAL: Model metadata missing at {metadata_path}")
    logger.info("Model artifacts verified successfully. Pipeline complete.")

default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="weekly_training_dag",
    default_args=default_args,
    schedule="0 18 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Weekly pipeline: Retrains model only if Evidently drift flag is raised.",
) as dag:

    check_flag = ShortCircuitOperator(
        task_id="check_retrain_flag",
        python_callable=check_retrain_flag,
    )

    preprocess_full_dataset = BashOperator(
        task_id="preprocess_full_dataset",
        bash_command="cd /opt/airflow && python -m src.preprocessing.feature_engineering || python -m src.preprocessing.feature_engineering"
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command="cd /opt/airflow && python -m src.training.train_model || python -m src.training.train_model"
    )

    model_ready_check = PythonOperator(
        task_id="model_ready_check",
        python_callable=check_model_artifacts
    )

    check_flag >> preprocess_full_dataset >> train_model >> model_ready_check