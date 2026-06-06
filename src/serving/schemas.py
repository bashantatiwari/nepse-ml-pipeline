from pydantic import BaseModel

class HealthResponse(BaseModel):
    status: str
    message: str

class PredictionResponse(BaseModel):
    company: str
    latest_close: float
    predicted_next_close: float
    predicted_change: float
    predicted_pct_change: float
    model_version: str
