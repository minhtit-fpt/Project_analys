"""
Binance Future ETL Source Package.

This package contains modules for:
- exchange: CCXT async wrapper for Binance Futures
- scanner: Market scanning and coin filtering
- file_manager: Dynamic file rotation and CSV management
- pipeline: Main ETL orchestration
"""

__version__ = "1.0.0"
__author__ = "Crypto Quant Team"

from .exchange import BinanceFutureExchange
from .scanner import FutureMarketScanner
from .file_manager import CohortFileManager
from .pipeline import ETLPipeline

__all__ = [
    "BinanceFutureExchange",
    "FutureMarketScanner",
    "CohortFileManager",
    "ETLPipeline",
]
