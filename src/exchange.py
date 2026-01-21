"""
CCXT Async Wrapper for Binance USDT-Margined Futures.

This module provides async interface for:
- Loading future markets
- Fetching OHLCV data
- Getting ticker prices
"""

import asyncio
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

import ccxt.async_support as ccxt

from config.settings import (
    EXCHANGE_ID,
    MARKET_TYPE,
    RATE_LIMIT_DELAY,
    MAX_RETRIES,
    RETRY_DELAY,
    OHLCV_LIMIT,
)

logger = logging.getLogger(__name__)


class BinanceFutureExchange:
    """
    Async wrapper for Binance USDT-Margined Futures exchange operations.
    
    Attributes:
        exchange: CCXT async exchange instance
        markets: Cached market data
    """
    
    def __init__(self):
        """Initialize the exchange wrapper."""
        self.exchange: Optional[ccxt.binance] = None
        self.markets: Dict[str, Any] = {}
        self._initialized = False
    
    async def initialize(self) -> None:
        """
        Initialize the CCXT exchange with future market settings.
        
        Sets defaultType to 'future' for USDT-Margined contracts.
        """
        if self._initialized:
            return
            
        logger.info(f"Initializing {EXCHANGE_ID} exchange for {MARKET_TYPE} markets...")
        
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',  # USDT-Margined Futures
                'adjustForTimeDifference': True,
            }
        })
        
        self._initialized = True
        logger.info("Exchange initialized successfully")
    
    async def load_markets(self) -> Dict[str, Any]:
        """
        Load all future markets from Binance.
        
        Returns:
            Dict containing all market information.
        """
        if not self._initialized:
            await self.initialize()
        
        logger.info("Loading future markets...")
        self.markets = await self.exchange.load_markets()
        logger.info(f"Loaded {len(self.markets)} markets")
        
        return self.markets
    
    async def fetch_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch current ticker for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            
        Returns:
            Ticker data dict or None if failed.
        """
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(RATE_LIMIT_DELAY)
                ticker = await self.exchange.fetch_ticker(symbol)
                return ticker
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for ticker {symbol}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY)
        
        logger.error(f"Failed to fetch ticker for {symbol} after {MAX_RETRIES} attempts")
        return None
    
    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1d",
        since: Optional[int] = None,
        limit: int = OHLCV_LIMIT,
    ) -> List[List]:
        """
        Fetch OHLCV (candlestick) data for a symbol.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe (default: '1d')
            since: Start timestamp in milliseconds
            limit: Maximum number of candles
            
        Returns:
            List of OHLCV data: [[timestamp, open, high, low, close, volume], ...]
        """
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(RATE_LIMIT_DELAY)
                ohlcv = await self.exchange.fetch_ohlcv(
                    symbol=symbol,
                    timeframe=timeframe,
                    since=since,
                    limit=limit,
                )
                return ohlcv
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for OHLCV {symbol}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY)
        
        logger.error(f"Failed to fetch OHLCV for {symbol} after {MAX_RETRIES} attempts")
        return []
    
    async def fetch_first_candle(self, symbol: str, timeframe: str = "1d") -> Optional[List]:
        """
        Fetch the very first historical candle to determine listing date.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe
            
        Returns:
            First candle data or None if not found.
        """
        try:
            # Fetch from a very early timestamp (2017-01-01)
            earliest_timestamp = int(datetime(2017, 1, 1).timestamp() * 1000)
            
            ohlcv = await self.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=earliest_timestamp,
                limit=1,
            )
            
            if ohlcv and len(ohlcv) > 0:
                return ohlcv[0]
            return None
            
        except Exception as e:
            logger.error(f"Error fetching first candle for {symbol}: {e}")
            return None
    
    async def fetch_ohlcv_since(
        self,
        symbol: str,
        timeframe: str,
        since_timestamp: int,
    ) -> List[List]:
        """
        Fetch all OHLCV data from a given timestamp to now.
        
        Handles pagination automatically.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe
            since_timestamp: Start timestamp in milliseconds
            
        Returns:
            Complete list of OHLCV data from since_timestamp to now.
        """
        all_ohlcv = []
        current_since = since_timestamp
        
        logger.info(f"Fetching OHLCV for {symbol} since {datetime.fromtimestamp(since_timestamp/1000)}")
        
        while True:
            ohlcv = await self.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=current_since,
                limit=OHLCV_LIMIT,
            )
            
            if not ohlcv:
                break
            
            all_ohlcv.extend(ohlcv)
            
            # Check if we've reached the end
            if len(ohlcv) < OHLCV_LIMIT:
                break
            
            # Update since to last candle timestamp + 1ms
            current_since = ohlcv[-1][0] + 1
        
        logger.info(f"Fetched {len(all_ohlcv)} candles for {symbol}")
        return all_ohlcv
    
    async def close(self) -> None:
        """Close the exchange connection."""
        if self.exchange:
            await self.exchange.close()
            self._initialized = False
            logger.info("Exchange connection closed")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
