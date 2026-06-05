# NEPSE MLOps Pipeline (NABIL Time-Series)

This is an MLOps pipeline for predicting the next-day closing price of NABIL stock using historical time-series data.

## Workflow: Windows → GitHub → Linux

1. **Develop/Restructure on Windows**: Write the code, structure the project, ensure scripts can be run locally as modules.
2. **Push to GitHub**: Commit the project and push to a remote repository.
3. **Clone on Linux**: Clone the repository on a Linux server/VM.
4. **Run Docker Compose**: Deploy the infrastructure (MariaDB ColumnStore, Redis, MLflow, Airflow, FastAPI) using `docker-compose up -d` on Linux.

## Project Structure
- `data/`: Contains raw, processed, and reference data.
- `src/`: Source code for ingestion, preprocessing, training, serving, and utilities.
- `airflow/dags/`: Airflow DAGs for orchestration.
- `models/`: Trained model artifacts.
- `reports/`: Evaluation metrics and monitoring reports (Evidently AI).

## Running Locally as Modules
You can run individual steps locally before deploying:
```bash
python -m src.ingestion.load_to_mariadb
python -m src.preprocessing.feature_engineering
python -m src.training.train_model
```
