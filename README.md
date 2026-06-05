# NEPSE MLOps Pipeline (NABIL Time-Series Regression)

## Project Overview
This project refactors a historical multi-company stock classification tool into a focused, production-grade MLOps pipeline for **NABIL Bank (NABIL)**. It implements continuous data ingestion, chronological feature engineering, automated model selection, inference serving, and data drift monitoring. 

### Why NABIL?
NABIL is one of the highest-volume and most consistent blue-chip stocks on the Nepal Stock Exchange (NEPSE). Focusing on a single, high-liquidity stock reduces external sector-noise and allows for a pure time-series forecasting approach.

### Why Time-Series Regression?
The previous pipeline used binary classification (Predicting UP or DOWN). This has been upgraded to a **Regression** problem. Predicting the exact future value allows downstream systems to calculate risk, profit margins, and execute threshold-based algorithmic trading rather than guessing blindly on binary outcomes.

### The Regression Targets
To prevent data leakage, targets are engineered strictly chronologically:
- `next_close`: The absolute closing price for the *next* trading day (`close.shift(-1)`). This is the direct prediction target.
- `target_change`: The absolute difference between tomorrow's close and today's close.
- `target_pct_change`: The percentage difference.

---

## Architecture

```text
  [NABIL.csv] 
       │ (Daily Airflow Cron)
       ▼
[MariaDB ColumnStore (Raw Table)] 
       │ (Preprocessing & Lags)
       ▼
[Redis Cache] ──▶ [MariaDB (Processed Table)]
       │
       ▼ (Weekly Airflow Cron)
[Train: Baseline / LinReg / RandomForest]
       │
       ├─▶ [MLflow Registry (Metrics & Artifacts)]
       │
       ▼
[FastAPI Serving Layer] ◀── [Client Requests]
       │
       ▼ (Daily Airflow Cron)
[Evidently AI (Data Drift HTML Reports)]
```

*Note: For coursework stability on Linux/Docker, a standard MariaDB 11 image is utilized under the `mariadb-columnstore` service name. This provides immediate compatibility with Airflow's backend and standard Python connectors while mimicking the columnar interface.*

---

## Workflow: Windows → GitHub → Linux

The development lifecycle for this project involves:
1. **Restructuring on Windows**: Code is written and tested locally. Models gracefully fallback to CSVs if DBs are missing.
2. **Push to GitHub**: Changes are committed to the `refactor-nabil-ml-pipeline` branch.
3. **Deploy to Linux (Docker)**: The environment is spun up seamlessly via `docker-compose`.

### Linux Setup Commands

```bash
# 1. Clone the repository
git clone https://github.com/your-username/nepse-ml-pipeline.git
cd nepse-ml-pipeline

# 2. Checkout the correct branch
git checkout refactor-nabil-ml-pipeline

# 3. Setup environment variables
cp .env.example .env

# 4. Boot the MLOps infrastructure
docker compose up -d --build
```

### URLs & Ports

Once Docker is running, the services are available at:
- **FastAPI (Swagger UI)**: http://localhost:8000/docs
- **MLflow Tracking**: http://localhost:5000
- **Airflow Web UI**: http://localhost:8080 (Login: `airflow` / `airflow`)

---

## Manual Execution Commands

You can run individual pipeline modules manually. If executing locally on Windows (without Docker), ensure your `PYTHONPATH` includes the project root.

```bash
# 1. Ingestion
python -m src.ingestion.load_to_mariadb

# 2. Feature Engineering
python -m src.preprocessing.feature_engineering

# 3. Model Training
python -m src.training.train_model

# 4. Monitoring (Evidently Report)
python -m src.monitoring.evidently_report

# 5. Standalone FastAPI Server
uvicorn src.serving.main:app --host 0.0.0.0 --port 8000
```

---

## Uncommitted Artifacts

The following directories and files are dynamically generated and intentionally ignored by Git (`.gitignore`):
- `.env` (Credentials)
- `mlruns/` (Local MLflow SQLite/Artifacts)
- `models/*.joblib` and `models/*.json` (Heavy serialized models)
- `reports/monitoring/*.html` (Dynamic HTML reports)
- `data/processed/` (Intermediate datasets)

## Troubleshooting

- **FastAPI starts but `/predict` fails**: Ensure the model is trained. Run `python -m src.training.train_model` to generate `models/best_model.joblib`.
- **Airflow Webserver keeps restarting**: Ensure MariaDB is fully healthy. Docker Compose is set up to wait, but slower systems might require a restart: `docker compose restart airflow-webserver`.
- **Evidently AI ImportError**: Ensure you are running `evidently>=0.4.0` as pinned in `requirements.txt`.
