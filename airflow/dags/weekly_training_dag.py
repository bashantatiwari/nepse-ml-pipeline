import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


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
    schedule="0 18 * * 0",  # Run Sunday evening at 18:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Weekly pipeline: Preprocess full historic dataset and retrain model.",
) as dag:

    # Feature engineering over the full dataset
    preprocess_full_dataset = BashOperator(
        task_id="preprocess_full_dataset",
        bash_command="cd /opt/airflow && python -m src.preprocessing.feature_engineering || python -m src.preprocessing.feature_engineering"
    )

    # Model training with MLflow integration
    train_model = BashOperator(
        task_id="train_model",
        bash_command="cd /opt/airflow && python -m src.training.train_model || python -m src.training.train_model"
    )

    # Ensure models were updated successfully
    model_ready_check = PythonOperator(
        task_id="model_ready_check",
        python_callable=check_model_artifacts
    )

    preprocess_full_dataset >> train_model >> model_ready_check
