from pydantic import BaseModel
from typing import Any, Dict, List, Optional

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

class DriftResult(BaseModel):
    drift_detected: bool
    drift_score: float
    stattest: str

class DatasetDrift(BaseModel):
    drift_detected: bool
    drift_share: float
    drifted_columns: int
    total_columns: int

class CheckResult(BaseModel):
    check: str
    timestamp: Optional[str] = None
    skipped: Optional[bool] = None
    reason: Optional[str] = None
    tip: Optional[str] = None
    window: Optional[str] = None
    dataset_drift: Optional[DatasetDrift] = None
    column_drift: Optional[Dict[str, DriftResult]] = None
    result: Optional[DriftResult] = None
    target_column: Optional[str] = None
    metrics: Optional[Dict[str, float]] = None
    rmse_threshold: Optional[float] = None
    prediction_rows_used: Optional[int] = None
    retraining_recommended: Optional[bool] = None

class MonitorConfig(BaseModel):
    reference_days: int
    current_days: int
    drift_threshold: float
    rmse_threshold: float

class MonitorResponse(BaseModel):
    generated_at: str
    retraining_recommended: bool
    config: MonitorConfig
    checks: List[CheckResult]
