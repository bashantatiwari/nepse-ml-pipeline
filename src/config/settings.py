import os
from pathlib import Path

# Project root is two levels up from this file (src/config/settings.py)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data Paths
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
REFERENCE_DATA_DIR = DATA_DIR / "reference"

# Model Paths
MODELS_DIR = PROJECT_ROOT / "models"
EVALUATION_REPORTS_DIR = PROJECT_ROOT / "reports" / "evaluation"
MONITORING_REPORTS_DIR = PROJECT_ROOT / "reports" / "monitoring"

# MariaDB Settings
MARIADB_HOST = os.getenv("MARIADB_HOST", "localhost")
MARIADB_PORT = int(os.getenv("MARIADB_PORT", 3306))
MARIADB_USER = os.getenv("MARIADB_USER", "root")
MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD", "password")
MARIADB_DATABASE = os.getenv("MARIADB_DATABASE", "nepse_db")

# Redis Settings
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# MLflow Settings
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

# Airflow Settings
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", str(PROJECT_ROOT / "airflow"))
