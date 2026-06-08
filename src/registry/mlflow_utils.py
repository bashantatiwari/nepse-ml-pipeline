import logging
import requests

import mlflow
from mlflow.exceptions import MlflowException

from src.config.settings import MLFLOW_TRACKING_URI

logger = logging.getLogger(__name__)


class MLflowManager:
    def __init__(self, experiment_name: str = "NABIL_Price_Prediction"):
        self.tracking_uri = MLFLOW_TRACKING_URI
        self.experiment_name = experiment_name
        self.is_available = self._check_availability()
        
        if self.is_available:
            mlflow.set_tracking_uri(self.tracking_uri)
            try:
                mlflow.set_experiment(self.experiment_name)
                logger.info(f"Connected to MLflow at {self.tracking_uri}. Experiment: {self.experiment_name}")
            except Exception as e:
                logger.warning(f"MLflow is accessible but failed to set experiment: {e}")
                self.is_available = False

    def _check_availability(self) -> bool:
        """Checks if the MLflow server is reachable without crashing the app."""
        try:
            # Quick HTTP check to see if MLflow server is responding
            response = requests.get(f"{self.tracking_uri}/health", timeout=2)
            if response.status_code == 200:
                return True
            return False
        except requests.exceptions.RequestException:
            logger.warning(f"MLflow server is unreachable at {self.tracking_uri}. MLflow logging will be safely skipped.")
            return False

    def log_run(self, run_name: str, model, params: dict, metrics: dict, features: list):
        """Logs parameters, metrics, features, and model artifact to MLflow."""
        if not self.is_available:
            logger.info(f"Skipping MLflow logging for '{run_name}' (MLflow unavailable).")
            return

        try:
            with mlflow.start_run(run_name=run_name):
                mlflow.log_params(params)
                mlflow.log_metrics(metrics)
                mlflow.log_param("features", ", ".join(features))
                
                # Log the scikit-learn model
                mlflow.sklearn.log_model(model, artifact_path="model")
                logger.info(f"Successfully logged run '{run_name}' to MLflow.")
        except Exception as e:
            logger.error(f"Failed to log run to MLflow: {e}")
