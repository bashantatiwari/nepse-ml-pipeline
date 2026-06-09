import logging
from contextlib import contextmanager
from typing import Generator

import mariadb

from src.config.settings import (
    COLUMNSTORE_DATABASE,
    COLUMNSTORE_HOST,
    COLUMNSTORE_PASSWORD,
    COLUMNSTORE_PORT,
    COLUMNSTORE_USER,
)

logger = logging.getLogger(__name__)


class ColumnStoreClient:
    """
    Client for MariaDB ColumnStore — analytical warehouse.
    Handles warehouse tables: raw_nabil_prices, processed_nabil_features, predictions.
    NOTE: ColumnStore engine does not support UNIQUE KEY or AUTO_INCREMENT.
    Deduplication is handled at the application layer.
    """

    def __init__(self):
        self.host = COLUMNSTORE_HOST
        self.port = int(COLUMNSTORE_PORT)
        self.user = COLUMNSTORE_USER
        self.password = COLUMNSTORE_PASSWORD
        self.database = COLUMNSTORE_DATABASE

    @contextmanager
    def get_connection(self) -> Generator[mariadb.Connection, None, None]:
        conn = None
        try:
            conn = mariadb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            conn.autocommit = True
            yield conn
        except mariadb.Error as e:
            logger.error(f"ColumnStore connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def init_tables(self):
        """
        Creates warehouse tables using ENGINE=ColumnStore.
        ColumnStore limitations:
          - No UNIQUE KEY constraints
          - No AUTO_INCREMENT
          - No foreign keys
          - Deduplication handled at application layer
        """
        queries = [
            """
            CREATE TABLE IF NOT EXISTS raw_nabil_prices (
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
            ) ENGINE=ColumnStore
            """,
            """
            CREATE TABLE IF NOT EXISTS processed_nabil_features (
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
            ) ENGINE=ColumnStore
            """,
            """
            CREATE TABLE IF NOT EXISTS predictions (
                symbol VARCHAR(20) NOT NULL,
                prediction_date DATE NOT NULL,
                latest_close DOUBLE,
                predicted_next_close DOUBLE,
                predicted_change DOUBLE,
                predicted_pct_change DOUBLE,
                model_version VARCHAR(50),
                created_at DATE
            ) ENGINE=ColumnStore
            """
        ]
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for q in queries:
                cursor.execute(q)
            logger.info("✅ ColumnStore warehouse tables initialized successfully.")
