import json
import logging
from pathlib import Path
from fastapi import FastAPI, HTTPException
from src.serving.prediction_service import PredictionService
from src.serving.schemas import HealthResponse, PredictionResponse, MonitorResponse
from src.storage.mariadb_client import MariaDBClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="NABIL Price Prediction API",
    description="MLOps prediction serving layer",
    version="1.0"
)

MONITORING_SUMMARY_PATH = Path("/opt/airflow/reports/monitoring/monitoring_summary.json")

# Global prediction service instance (lazy loaded)
prediction_service = None

@app.on_event("startup")
def startup_event():
    global prediction_service
    try:
        db = MariaDBClient()
        db.init_tables()
        logger.info("MariaDB tables initialized successfully.")
    except Exception as e:
        logger.error(f"MariaDB table initialization failed: {e}")
    try:
        prediction_service = PredictionService()
        logger.info("Prediction service initialized successfully on startup.")
    except Exception as e:
        logger.error(f"Startup initialization failed (artifact missing?): {e}")

@app.get("/health", response_model=HealthResponse)
def health_check():
    """Checks if the API is running and model artifacts are successfully loaded."""
    if prediction_service is None or prediction_service.model is None:
        return HealthResponse(
            status="degraded",
            message="API is running, but model artifacts are missing. Training required."
        )
    return HealthResponse(
        status="healthy",
        message="API is running and model is loaded."
    )

@app.post("/predict", response_model=PredictionResponse)
def predict():
    """Fetches the latest data and predicts the next day's closing price."""
    global prediction_service
    if prediction_service is None:
        try:
            prediction_service = PredictionService()
        except FileNotFoundError as fnf_err:
            raise HTTPException(status_code=404, detail=str(fnf_err))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Prediction service initialization failed: {e}")
    try:
        result = prediction_service.predict_next_close()
        return PredictionResponse(**result)
    except FileNotFoundError as fnf_err:
        raise HTTPException(status_code=404, detail=str(fnf_err))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/monitor", response_model=MonitorResponse)
def monitor():
    """Returns the latest Evidently monitoring summary including drift detection and retraining recommendations."""
    # Check shared volume path first, then local fallback
    candidates = [
        Path("/opt/airflow/reports/monitoring/monitoring_summary.json"),
        Path("/app/reports/monitoring/monitoring_summary.json"),
    ]
    summary_path = None
    for p in candidates:
        if p.exists():
            summary_path = p
            break

    if summary_path is None:
        raise HTTPException(
            status_code=404,
            detail="Monitoring report not found. Run the Evidently monitoring script first: "
                   "docker compose exec airflow-scheduler bash -c "
                   "'cd /opt/airflow && python -m src.monitoring.evidently_report'"
        )
    try:
        with open(summary_path) as f:
            data = json.load(f)
        return MonitorResponse(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse monitoring summary: {e}")
