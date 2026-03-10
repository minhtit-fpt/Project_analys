"""
Centralized logging configuration.

Usage:
    from src.core.logger import get_logger

    logger = get_logger(__name__)
    logger.info("Hello from module X")
"""

import logging
import os
from datetime import datetime

from src.core.config import settings
from src.core.constants import LOG_DATE_FORMAT


def get_logger(name: str = __name__) -> logging.Logger:
    """
    Return a configured logger.

    - Writes to a daily log file under ``settings.log_dir``.
    - Also streams to the console.
    - Log level is controlled by ``settings.log_level``.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # ── File handler (daily rotation by filename) ─────────────────────────
    os.makedirs(settings.log_dir, exist_ok=True)
    today = datetime.now().strftime(LOG_DATE_FORMAT)
    file_handler = logging.FileHandler(
        os.path.join(settings.log_dir, f"binance_fetcher_{today}.log"),
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    # ── Console handler ───────────────────────────────────────────────────
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
