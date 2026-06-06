import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger with a consistent format for the NABIL MLOps pipeline.
    Usage: logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        # Prevent log messages from propagating to the root logger (avoids duplicates)
        logger.propagate = False

    return logger
