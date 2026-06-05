import logging
import pandas as pd
import redis

from src.config.settings import REDIS_HOST, REDIS_PORT

logger = logging.getLogger(__name__)


class RedisClient:
    def __init__(self):
        self.host = REDIS_HOST
        self.port = REDIS_PORT
        try:
            self.client = redis.Redis(host=self.host, port=self.port, db=0, decode_responses=False)
            # Ping to test connection
            self.client.ping()
        except Exception as e:
            logger.warning(f"Redis is unavailable: {e}. Caching will be skipped.")
            self.client = None

    def cache_dataframe(self, key: str, df: pd.DataFrame) -> bool:
        """Caches a Pandas DataFrame into Redis as JSON."""
        if not self.client:
            logger.warning(f"Redis not connected. Skipping cache for key: {key}")
            return False
            
        try:
            # Serialize DataFrame to JSON
            json_data = df.to_json(orient="records", date_format="iso")
            self.client.set(key, json_data)
            logger.info(f"Successfully cached dataframe under key '{key}' in Redis.")
            return True
        except Exception as e:
            logger.error(f"Failed to cache dataframe in Redis: {e}")
            return False

    def get_cached_dataframe(self, key: str) -> pd.DataFrame:
        """Retrieves a cached Pandas DataFrame from Redis."""
        if not self.client:
            logger.warning(f"Redis not connected. Cannot fetch key: {key}")
            return None
            
        try:
            json_data = self.client.get(key)
            if json_data:
                df = pd.read_json(json_data, orient="records")
                logger.info(f"Successfully retrieved dataframe '{key}' from Redis.")
                return df
            else:
                logger.info(f"No data found in Redis for key '{key}'.")
                return None
        except Exception as e:
            logger.error(f"Failed to retrieve dataframe from Redis: {e}")
            return None
