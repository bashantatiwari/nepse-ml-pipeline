import json
import logging

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression

from src.config.settings import MODELS_DIR, PROCESSED_DATA_DIR
from src.registry.mlflow_utils import MLflowManager
from src.storage.mariadb_client import MariaDBClient
from src.storage.redis_client import RedisClient
from src.training.evaluate_model import evaluate_predictions, save_evaluation_report
from src.utils.logger import get_logger

logger = get_logger(__name__)


def fetch_processed_data() -> pd.DataFrame:
    """Fetches processed data reading from Redis -> MariaDB -> CSV sequentially."""
    
    # 1. Try Redis
    redis_client = RedisClient()
    df = redis_client.get_cached_dataframe("processed_nabil_features")
    if df is not None and not df.empty:
        logger.info("Loaded processed data from Redis Cache.")
        return df
        
    # 2. Try MariaDB
    logger.info("Data not found in Redis. Attempting to load from MariaDB...")
    db_client = MariaDBClient()
    try:
        with db_client.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM processed_nabil_features ORDER BY published_date ASC")
            rows = cursor.fetchall()
            if rows:
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                logger.info(f"Loaded {len(df)} rows from MariaDB.")
                return df
    except Exception as e:
        logger.warning(f"Failed to load from MariaDB: {e}")

    # 3. Fallback to CSV
    logger.info("Attempting to load from CSV fallback...")
    csv_path = PROCESSED_DATA_DIR / "nabil_features.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} rows from CSV fallback.")
        return df

    logger.error("Failed to load processed data from all sources.")
    return pd.DataFrame()


def split_data(df: pd.DataFrame, features: list, target: str, lookback_years: int = 3):
    """Chronological 80/20 train/test split. Does not shuffle."""
    # Ensure strict chronological order
    if "published_date" in df.columns:
        df["published_date"] = pd.to_datetime(df["published_date"])
        df = df.sort_values("published_date").reset_index(drop=True)
    
    cutoff = df["published_date"].max() - pd.DateOffset(years=lookback_years)
    df = df[df["published_date"] >= cutoff].reset_index(drop=True)
    split_index = int(len(df) * 0.8)
    
    train_df = df.iloc[:split_index]
    test_df = df.iloc[split_index:]
    
    X_train = train_df[features]
    y_train = train_df[target]
    
    X_test = test_df[features]
    y_test = test_df[target]
    
    logger.info(f"Train/Test split completed chronologically.")
    logger.info(f"Training rows: {len(X_train)} (Up to {train_df['published_date'].max().date()})")
    logger.info(f"Testing rows: {len(X_test)} (From {test_df['published_date'].min().date()})")
    
    return X_train, X_test, y_train, y_test


def train_and_evaluate():
    df = fetch_processed_data()
    if df.empty:
        logger.error("No data available to train.")
        return
        
    # Define features and target
    target = "next_close"
    
    # Exclude identifiers and future/target variables from features
    exclude_cols = ["id", "symbol", "published_date", "status", "next_close", "target_change", "target_pct_change", "percent_change"]
    features = [col for col in df.columns if col not in exclude_cols]
    
    # Handle possible NaN generated in DB
    df = df.dropna(subset=features + [target]).reset_index(drop=True)
    
    X_train, X_test, y_train, y_test = split_data(df, features, target)
    
    mlflow_manager = MLflowManager()
    
    # 1. Baseline Model (Predict tomorrow's close will be today's close)
    logger.info("--- Evaluating Baseline Model ---")
    y_pred_baseline = X_test["close"]
    baseline_metrics = evaluate_predictions(y_test, y_pred_baseline)
    logger.info(f"Baseline Metrics: {baseline_metrics}")
    save_evaluation_report("Baseline", baseline_metrics)
    
    # 2. Linear Regression
    logger.info("--- Training Linear Regression ---")
    lr_model = LinearRegression()
    lr_model.fit(X_train, y_train)
    y_pred_lr = lr_model.predict(X_test)
    lr_metrics = evaluate_predictions(y_test, y_pred_lr)
    logger.info(f"Linear Regression Metrics: {lr_metrics}")
    save_evaluation_report("LinearRegression", lr_metrics)
    
    mlflow_manager.log_run(
        run_name="Linear_Regression",
        model=lr_model,
        params={"fit_intercept": lr_model.fit_intercept},
        metrics=lr_metrics,
        features=features
    )
    
    # 3. Random Forest Regressor
    logger.info("--- Training Random Forest ---")
    rf_params = {"n_estimators": 100, "random_state": 42, "max_depth": 10}
    rf_model = RandomForestRegressor(**rf_params)
    rf_model.fit(X_train, y_train)
    y_pred_rf = rf_model.predict(X_test)
    rf_metrics = evaluate_predictions(y_test, y_pred_rf)
    logger.info(f"Random Forest Metrics: {rf_metrics}")
    save_evaluation_report("RandomForest", rf_metrics)
    
    mlflow_manager.log_run(
        run_name="Random_Forest",
        model=rf_model,
        params=rf_params,
        metrics=rf_metrics,
        features=features
    )
    
    # Model Selection (Lowest RMSE)
    best_model = None
    best_name = ""
    best_rmse = float("inf")
    
    models = {
        "Baseline": (None, baseline_metrics["RMSE"]),
        "LinearRegression": (lr_model, lr_metrics["RMSE"]),
        "RandomForest": (rf_model, rf_metrics["RMSE"])
    }
    
    for name, (model, rmse) in models.items():
        if rmse < best_rmse and model is not None:
            best_rmse = rmse
            best_model = model
            best_name = name
            
    logger.info(f"--- Best Model Selected: {best_name} with RMSE: {best_rmse} ---")
    
    # Save best model locally
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODELS_DIR / "best_model.joblib"
    joblib.dump(best_model, model_path)
    logger.info(f"Saved {best_name} model artifact locally to {model_path}")
    
    # Save feature list and metadata for FastAPI serving
    metadata_path = MODELS_DIR / "model_metadata.json"
    metadata = {
        "model_name": best_name,
        "features": features,
        "rmse": best_rmse
    }
    try:
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)
        logger.info(f"Saved model metadata and exact feature list to {metadata_path}")
    except Exception as e:
        logger.error(f"Failed to save model metadata: {e}")


if __name__ == "__main__":
    train_and_evaluate()
