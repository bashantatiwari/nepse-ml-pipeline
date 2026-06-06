import logging
from contextlib import contextmanager
from typing import Generator

import mariadb

from src.config.settings import (
    MARIADB_DATABASE,
    MARIADB_HOST,
    MARIADB_PASSWORD,
    MARIADB_PORT,
    MARIADB_USER,
)

logger = logging.getLogger(__name__)


class MariaDBClient:
    def __init__(self):
        self.host = MARIADB_HOST
        self.port = MARIADB_PORT
        self.user = MARIADB_USER
        self.password = MARIADB_PASSWORD
        self.database = MARIADB_DATABASE

    @contextmanager
    def get_connection(self) -> Generator[mariadb.connection, None, None]:
        """Provides a transactional scope around a series of operations."""
        conn = None
        try:
            conn = mariadb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            conn.database = self.database
            yield conn
        except mariadb.Error as e:
            logger.error(f"Error connecting to MariaDB: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def init_tables(self):
        """Creates required tables for the ML pipeline if they do not exist."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS raw_nabil_prices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                published_date DATE NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                percent_change DOUBLE,
                traded_quantity DOUBLE,
                traded_amount DOUBLE,
                status VARCHAR(20)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS processed_nabil_features (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                published_date DATE NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                percent_change DOUBLE,
                traded_quantity DOUBLE,
                traded_amount DOUBLE,
                status VARCHAR(20),
                price_range DOUBLE,
                daily_return DOUBLE,
                close_lag_1 DOUBLE,
                close_lag_3 DOUBLE,
                close_lag_7 DOUBLE,
                rolling_mean_7 DOUBLE,
                rolling_mean_14 DOUBLE,
                rolling_std_7 DOUBLE,
                next_close DOUBLE,
                target_change DOUBLE,
                target_pct_change DOUBLE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS predictions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                prediction_date DATE NOT NULL,
                latest_close DOUBLE,
                predicted_next_close DOUBLE,
                predicted_change DOUBLE,
                predicted_pct_change DOUBLE,
                model_version VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        ]
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for q in queries:
                cursor.execute(q)
            conn.commit()
            logger.info("Database tables initialized successfully.")
