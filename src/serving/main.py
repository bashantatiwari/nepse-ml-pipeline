import logging

from fastapi import FastAPI, HTTPException

from src.serving.prediction_service import PredictionService
from src.serving.schemas import HealthResponse, PredictionResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="NABIL Price Prediction API",
    description="MLOps prediction serving layer",
    version="1.0"
)

# Global prediction service instance (lazy loaded)
prediction_service = None

@app.on_event("startup")
def startup_event():
    global prediction_service
    try:
        prediction_service = PredictionService()
        logger.info("Prediction service initialized successfully on startup.")
    except Exception as e:
        logger.error(f"Startup initialization failed (artifact missing?): {e}")
        # We do not block startup, allowing /health to report degraded status


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
    # Attempt initialization again if it failed on startup
    global prediction_service
    if prediction_service is None:
        try:
            prediction_service = PredictionService()
        except FileNotFoundError as fnf_err:
            raise HTTPException(status_code=404, detail=str(fnf_err))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Prediction service initialization failed: {e}")
            
    # Execute prediction
    try:
        result = prediction_service.predict_next_close()
        return PredictionResponse(**result)
    except FileNotFoundError as fnf_err:
        # Expected error if processed data is missing
        raise HTTPException(status_code=404, detail=str(fnf_err))
    except Exception as e:
        # General failure during prediction
        raise HTTPException(status_code=500, detail=str(e))
