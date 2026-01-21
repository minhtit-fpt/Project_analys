"""
Configuration settings for Binance Future ETL System.
"""
from pathlib import Path
from datetime import datetime

# =============================================================================
# EXCHANGE SETTINGS
# =============================================================================
EXCHANGE_ID = "binance"
MARKET_TYPE = "future"  # USDT-Margined Futures (Linear)

# =============================================================================
# ASSET FILTERS
# =============================================================================
TARGET_QUOTE = "USDT"
MAX_PRICE = 10.0  # Maximum price constraint in USDT
REQUIRED_STATUS = "active"  # Only active/trading pairs

# =============================================================================
# DATA SETTINGS
# =============================================================================
TIMEFRAME = "1d"  # Default timeframe for OHLCV data
OHLCV_LIMIT = 1000  # Max candles per request

# =============================================================================
# PATH SETTINGS
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / f"{EXCHANGE_ID}_{MARKET_TYPE}"
LOG_DIR = BASE_DIR / "logs"

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# FILE NAMING CONVENTION
# =============================================================================
FILE_PREFIX = f"{EXCHANGE_ID}_{MARKET_TYPE}_coins"
FILE_EXTENSION = ".csv"

# =============================================================================
# RUNTIME SETTINGS
# =============================================================================
CURRENT_YEAR = datetime.now().year
RATE_LIMIT_DELAY = 0.1  # Seconds between API calls
MAX_RETRIES = 3
RETRY_DELAY = 5  # Seconds

# =============================================================================
# CSV COLUMNS
# =============================================================================
CSV_COLUMNS = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]

# =============================================================================
# LOGGING
# =============================================================================
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
