import json
import logging
from typing import Any, Dict

import joblib
import pandas as pd

from src.config.settings import MODELS_DIR, PROCESSED_DATA_DIR
from src.storage.mariadb_client import MariaDBClient

logger = logging.getLogger(__name__)


class PredictionService:
    def __init__(self):
        self.model_path = MODELS_DIR / "best_model.joblib"
        self.metadata_path = MODELS_DIR / "model_metadata.json"
        
        self.model = None
        self.metadata = None
        self.features = []
        self.model_name = "unknown"
        
        self._load_model_artifacts()

    def _load_model_artifacts(self):
        """Loads the saved ML model and exact feature requirements."""
        if not self.model_path.exists():
            raise FileNotFoundError("Model file missing. Please run: python -m src.training.train_model")
            
        if not self.metadata_path.exists():
            raise FileNotFoundError("Model metadata missing. Please rerun: python -m src.training.train_model")

        try:
            self.model = joblib.load(self.model_path)
            with open(self.metadata_path, "r") as f:
                self.metadata = json.load(f)
                
            self.features = self.metadata.get("features", [])
            if not self.features:
                raise ValueError("Model metadata is corrupted: missing feature list.")
                
            self.model_name = self.metadata.get("model_name", "local-best-model")
            logger.info(f"Successfully loaded model '{self.model_name}' and metadata.")
        except Exception as e:
            logger.error(f"Error loading model artifacts: {e}")
            raise RuntimeError(f"Error loading model artifacts: {e}")

    def _get_latest_data(self) -> pd.Series:
        """Fetches the latest available feature vector for NABIL."""
        # 1. Try MariaDB
        db_client = MariaDBClient()
        try:
            with db_client.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM processed_nabil_features ORDER BY published_date DESC LIMIT 1")
                row = cursor.fetchone()
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    latest_row = pd.Series(dict(zip(columns, row)))
                    logger.info("Loaded latest processed row from MariaDB.")
                    return latest_row
        except Exception as e:
            logger.warning(f"Failed to fetch latest data from MariaDB: {e}")

        # 2. Try CSV Fallback
        csv_path = PROCESSED_DATA_DIR / "nabil_features.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            if not df.empty:
                # Sort chronologically and take the last row
                if "published_date" in df.columns:
                    df["published_date"] = pd.to_datetime(df["published_date"])
                    df = df.sort_values("published_date")
                latest_row = df.iloc[-1]
                logger.info("Loaded latest processed row from CSV fallback.")
                return latest_row

        raise FileNotFoundError("Processed data missing. Please run: python -m src.preprocessing.feature_engineering")

    def predict_next_close(self) -> Dict[str, Any]:
        """Orchestrates fetching data, predicting, and computing deltas."""
        latest_row = self._get_latest_data()
        
        try:
            # Map features explicitly maintaining the exact trained order
            feature_values = []
            for feat in self.features:
                if feat not in latest_row:
                    raise ValueError(f"Required feature '{feat}' is missing from the latest data.")
                feature_values.append(latest_row[feat])
                
            # Model input requires a 2D array
            X_input = [feature_values]
            
            # Predict
            predicted_next_close = float(self.model.predict(X_input)[0])
            latest_close = float(latest_row["close"])
            
            # Compute deltas
            predicted_change = predicted_next_close - latest_close
            predicted_pct_change = (predicted_change / latest_close) * 100 if latest_close != 0 else 0.0
            
            return {
                "company": "NABIL",
                "latest_close": round(latest_close, 2),
                "predicted_next_close": round(predicted_next_close, 2),
                "predicted_change": round(predicted_change, 2),
                "predicted_pct_change": round(predicted_pct_change, 2),
                "model_version": self.model_name
            }
            
        except Exception as e:
            logger.error(f"Prediction logic failed: {e}")
            raise RuntimeError(f"Prediction logic failed: {e}")
