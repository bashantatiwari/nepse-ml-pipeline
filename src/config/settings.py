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

# ---------------------------------------------------------------------------
# Metadata DB — standard MariaDB (Airflow + MLflow metadata)
# ---------------------------------------------------------------------------
MARIADB_HOST = os.getenv("MARIADB_HOST", "localhost")
MARIADB_PORT = int(os.getenv("MARIADB_PORT", 3306))
MARIADB_USER = os.getenv("MARIADB_USER", "nepse_user")
MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD", "nepse_secure_password")
MARIADB_DATABASE = os.getenv("MARIADB_DATABASE", "nepse_mlops")

# ---------------------------------------------------------------------------
# ColumnStore — analytical warehouse (pipeline tables)
# ---------------------------------------------------------------------------
COLUMNSTORE_HOST = os.getenv("COLUMNSTORE_HOST", "localhost")
COLUMNSTORE_PORT = int(os.getenv("COLUMNSTORE_PORT", 3306))
COLUMNSTORE_USER = os.getenv("COLUMNSTORE_USER", "nepse_user")
COLUMNSTORE_PASSWORD = os.getenv("COLUMNSTORE_PASSWORD", "nepse_secure_password")
COLUMNSTORE_DATABASE = os.getenv("COLUMNSTORE_DATABASE", "nepse_columnstore")

# Redis Settings
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# MLflow Settings
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

# Airflow Settings
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", str(PROJECT_ROOT / "airflow"))
