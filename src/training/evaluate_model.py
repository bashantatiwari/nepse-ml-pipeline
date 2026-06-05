import json
import logging

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from src.config.settings import EVALUATION_REPORTS_DIR

logger = logging.getLogger(__name__)


def evaluate_predictions(y_true, y_pred) -> dict:
    """Calculates regression evaluation metrics (MAE, RMSE, R2, MAPE)."""
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)
    
    # Safe MAPE calculation to avoid division by zero
    mask = y_true != 0
    if mask.any():
        mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    else:
        mape = np.nan
        
    return {
        "MAE": float(mae),
        "RMSE": float(rmse),
        "R2": float(r2),
        "MAPE": float(mape)
    }


def save_evaluation_report(model_name: str, metrics: dict):
    """Saves evaluation metrics to a JSON file."""
    EVALUATION_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = EVALUATION_REPORTS_DIR / f"{model_name}_metrics.json"
    
    try:
        with open(report_path, "w") as f:
            json.dump(metrics, f, indent=4)
        logger.info(f"Saved evaluation metrics to {report_path}")
    except Exception as e:
        logger.error(f"Failed to save evaluation metrics: {e}")
