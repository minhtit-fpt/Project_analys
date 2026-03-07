"""
Centralized configuration management using pydantic-settings.

Loads values from environment variables / .env file and validates them.
"""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.core.constants import (
    DEFAULT_DATA_OUTPUT_DIR,
    DEFAULT_LOG_DIR,
    DEFAULT_PRICE_THRESHOLD,
    DEFAULT_QUOTE_CURRENCY,
    DEFAULT_TIMEFRAME,
)


class AppSettings(BaseSettings):
    """Application settings loaded from environment / .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Binance ───────────────────────────────────────────────────────────────
    binance_api_key: str = Field(default="", description="Binance API key")
    binance_secret_key: str = Field(default="", description="Binance secret key")

    # ── ETL ────────────────────────────────────────────────────────────────────
    price_threshold: float = Field(default=DEFAULT_PRICE_THRESHOLD)
    target_quote: str = Field(default=DEFAULT_QUOTE_CURRENCY)
    timeframe: str = Field(default=DEFAULT_TIMEFRAME)

    # ── Data Storage ──────────────────────────────────────────────────────────
    data_output_dir: str = Field(default=DEFAULT_DATA_OUTPUT_DIR)

    # ── Google Cloud Storage ──────────────────────────────────────────────────
    gcs_bucket_name: str = Field(default="")
    gcs_folder_prefix: str = Field(default="")
    google_application_credentials: Optional[str] = Field(default=None)

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = Field(default="INFO")
    log_dir: str = Field(default=DEFAULT_LOG_DIR)


# Singleton instance – import this wherever settings are needed.
settings = AppSettings()
