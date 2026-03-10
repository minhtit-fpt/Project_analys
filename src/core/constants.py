"""
Application-wide constants.
"""

# ── Binance Defaults ──────────────────────────────────────────────────────────
DEFAULT_TIMEFRAME = "1d"
DEFAULT_PRICE_THRESHOLD = 10.0
DEFAULT_QUOTE_CURRENCY = "USDT"

# ── Supported Timeframes ─────────────────────────────────────────────────────
VALID_TIMEFRAMES = [
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
]

# ── Data / Storage ────────────────────────────────────────────────────────────
DEFAULT_DATA_OUTPUT_DIR = "data/binance_future"
DEFAULT_LOG_DIR = "logs"

# ── OHLCV Column Names ───────────────────────────────────────────────────────
OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]

# ── Date Formats ──────────────────────────────────────────────────────────────
DATE_FORMAT = "%Y-%m-%d"
LOG_DATE_FORMAT = "%Y%m%d"
