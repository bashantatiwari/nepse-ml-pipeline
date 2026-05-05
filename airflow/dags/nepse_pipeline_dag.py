from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PYTHON_PATH = "/home/bashanta/miniconda3/envs/course_pipeline/bin/python"
PROJECT_PATH = "/home/bashanta/Desktop/nepse-ml-pipeline"


with DAG(
    dag_id="nepse_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="DAG for running the NEPSE ML pipeline",
) as dag:

    data_ingestion = BashOperator(
        task_id="data_ingestion",
        bash_command=f"{PYTHON_PATH} {PROJECT_PATH}/scripts/data_ingestion.py",
    )

    data_preprocessing = BashOperator(
        task_id="data_preprocessing",
        bash_command=f"{PYTHON_PATH} {PROJECT_PATH}/scripts/data_preprocessing.py",
    )

    model_training = BashOperator(
        task_id="model_training",
        bash_command=f"{PYTHON_PATH} {PROJECT_PATH}/scripts/model_training.py",
    )

    model_evaluation = BashOperator(
        task_id="model_evaluation",
        bash_command=f"{PYTHON_PATH} {PROJECT_PATH}/scripts/model_evaluation.py",
    )

    data_ingestion >> data_preprocessing >> model_training >> model_evaluation