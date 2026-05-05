from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PYTHON_PATH = "/home/bashanta/miniconda3/envs/course_pipeline/bin/python"
PROJECT_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline"

with DAG(
    dag_id="nepse_redis_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Four-stage NEPSE ML pipeline using Redis to move data between tasks",
) as dag:

    data_ingestion = BashOperator(
        task_id="data_ingestion_redis",
        bash_command=f"cd {PROJECT_PATH}/scripts && {PYTHON_PATH} data_ingestion_redis.py",
    )

    data_preprocessing = BashOperator(
        task_id="data_preprocessing_redis",
        bash_command=f"cd {PROJECT_PATH}/scripts && {PYTHON_PATH} data_preprocessing_redis.py",
    )

    model_training = BashOperator(
        task_id="model_training_redis",
        bash_command=f"cd {PROJECT_PATH}/scripts && {PYTHON_PATH} model_training_redis.py",
    )

    model_evaluation = BashOperator(
        task_id="model_evaluation_redis",
        bash_command=f"cd {PROJECT_PATH}/scripts && {PYTHON_PATH} model_evaluation_redis.py",
    )

    data_ingestion >> data_preprocessing >> model_training >> model_evaluation