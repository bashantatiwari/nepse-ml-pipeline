# NEPSE ML Pipeline — NABIL Bank Closing Price Prediction

An end-to-end MLOps pipeline that predicts the next-day closing price of NABIL Bank stock using historical NEPSE data.

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow |
| Data warehouse | MariaDB ColumnStore |
| Metadata / tracking | MariaDB (InnoDB) + MLflow |
| Cache | Redis |
| Serving | FastAPI |
| Monitoring | Evidently AI |
| Dashboard | Streamlit |

## Getting started

```bash
git clone https://github.com/bashantatiwari/nepse-ml-pipeline.git
cd nepse-ml-pipeline
git checkout refactor-nabil-ml-pipeline
cp .env.example .env
docker compose up -d --build
```

| Service | URL |
|---|---|
| Airflow | http://localhost:8080 (airflow / airflow) |
| MLflow | http://localhost:5000 |
| FastAPI | http://localhost:8000/docs |
| Streamlit | http://localhost:8501 |

## Pipeline

Two Airflow DAGs run on schedule:

- `daily_prediction_dag` — runs at 18:00 daily. Ingests raw prices → engineers features → generates and stores next-day prediction → runs Evidently drift report.
- `weekly_training_dag` — runs at 18:00 every Sunday. Retrains models on full dataset → logs metrics to MLflow → selects best model by RMSE.

## Project structure

```
airflow/dags/        # DAG definitions
src/
  ingestion/         # Load raw CSV into ColumnStore
  preprocessing/     # Feature engineering (rolling windows, lag features)
  training/          # Model training and evaluation
  serving/           # FastAPI app
  monitoring/        # Evidently drift reports
  storage/           # ColumnStore and Redis clients
  registry/          # MLflow utilities
docker/              # MariaDB and ColumnStore init scripts
data/raw/            # Seed data (NABIL.csv)
```

## API endpoints

```
GET  /health     # Model load status
POST /predict    # Next-day closing price prediction
GET  /monitor    # Latest drift report summary
```

## Known limitations

- `data/raw/NABIL.csv` is static seed data — no live scraping in the current version
- MLflow artifact upload fails due to a UID mismatch between containers (`/mlflow` permission denied)
- CMAPI server is not running in the ColumnStore container — not required for table operations