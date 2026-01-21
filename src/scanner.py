"""
Future Market Scanner Module.

This module handles:
- Filtering USDT-Margined Future pairs
- Price constraint filtering (<=10 USDT)
- Determining listing year from first historical candle
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import dataclass, field

from config.settings import (
    TARGET_QUOTE,
    MAX_PRICE,
    REQUIRED_STATUS,
    RATE_LIMIT_DELAY,
)
from .exchange import BinanceFutureExchange

logger = logging.getLogger(__name__)


@dataclass
class CoinInfo:
    """
    Data class representing a filtered coin's information.
    
    Attributes:
        symbol: Trading pair symbol (e.g., 'DOGE/USDT')
        base: Base currency (e.g., 'DOGE')
        quote: Quote currency (e.g., 'USDT')
        current_price: Current market price
        listing_year: Year when the coin was listed
        first_timestamp: Timestamp of first candle (ms)
    """
    symbol: str
    base: str
    quote: str
    current_price: float
    listing_year: int = 0
    first_timestamp: int = 0


@dataclass
class ScanResult:
    """
    Result of market scanning operation.
    
    Attributes:
        coins_by_year: Dict mapping listing_year to list of CoinInfo
        total_scanned: Total markets scanned
        total_filtered: Total coins after filtering
        errors: List of symbols that failed during scanning
    """
    coins_by_year: Dict[int, List[CoinInfo]] = field(default_factory=dict)
    total_scanned: int = 0
    total_filtered: int = 0
    errors: List[str] = field(default_factory=list)


class FutureMarketScanner:
    """
    Scanner for Binance USDT-Margined Futures markets.
    
    Filters markets based on:
    - Market type: Linear (USDT-Margined) Futures only
    - Quote currency: USDT only
    - Status: Active/Trading only
    - Price: <= MAX_PRICE (10 USDT)
    
    Classifies coins by their listing year (year of first candle).
    """
    
    def __init__(self, exchange: BinanceFutureExchange):
        """
        Initialize the scanner.
        
        Args:
            exchange: Initialized BinanceFutureExchange instance
        """
        self.exchange = exchange
        self._price_cache: Dict[str, float] = {}
    
    async def scan_markets(self) -> ScanResult:
        """
        Scan and filter all future markets.
        
        Returns:
            ScanResult containing filtered coins grouped by listing year.
        """
        result = ScanResult()
        
        # Step 1: Load markets
        markets = await self.exchange.load_markets()
        result.total_scanned = len(markets)
        logger.info(f"Total markets loaded: {result.total_scanned}")
        
        # Step 2: Filter by basic criteria
        filtered_symbols = await self._filter_markets(markets)
        logger.info(f"Markets after basic filter: {len(filtered_symbols)}")
        
        # Step 3: Filter by price
        price_filtered = await self._filter_by_price(filtered_symbols)
        logger.info(f"Markets after price filter: {len(price_filtered)}")
        
        # Step 4: Determine listing year for each coin
        classified_coins = await self._classify_by_listing_year(price_filtered, markets)
        
        # Step 5: Group by listing year
        for coin in classified_coins:
            if coin.listing_year not in result.coins_by_year:
                result.coins_by_year[coin.listing_year] = []
            result.coins_by_year[coin.listing_year].append(coin)
        
        result.total_filtered = len(classified_coins)
        
        # Log summary
        logger.info("=== Scan Summary ===")
        logger.info(f"Total scanned: {result.total_scanned}")
        logger.info(f"Total filtered: {result.total_filtered}")
        for year, coins in sorted(result.coins_by_year.items()):
            logger.info(f"  Year {year}: {len(coins)} coins")
        
        return result
    
    async def _filter_markets(self, markets: Dict[str, Any]) -> List[str]:
        """
        Filter markets by basic criteria.
        
        Criteria:
        - Must be linear (USDT-Margined) future
        - Quote currency must be TARGET_QUOTE (USDT)
        - Must be active/trading
        
        Args:
            markets: Raw market data from exchange
            
        Returns:
            List of filtered symbol names
        """
        filtered = []
        
        for symbol, market in markets.items():
            # Check if it's a future market (not spot)
            if market.get('type') != 'swap' and market.get('type') != 'future':
                continue
            
            # Must be linear (USDT-Margined), not inverse (Coin-Margined)
            if not market.get('linear', False):
                continue
            
            # Quote currency must be USDT
            if market.get('quote') != TARGET_QUOTE:
                continue
            
            # Must be active
            if not market.get('active', False):
                continue
            
            filtered.append(symbol)
            
        return filtered
    
    async def _filter_by_price(self, symbols: List[str]) -> List[str]:
        """
        Filter symbols by current price constraint.
        
        Args:
            symbols: List of symbol names to check
            
        Returns:
            List of symbols with price <= MAX_PRICE
        """
        filtered = []
        
        for symbol in symbols:
            try:
                ticker = await self.exchange.fetch_ticker(symbol)
                
                if ticker and ticker.get('last'):
                    price = float(ticker['last'])
                    self._price_cache[symbol] = price
                    
                    if price <= MAX_PRICE:
                        filtered.append(symbol)
                        logger.debug(f"{symbol}: ${price:.4f} - PASSED")
                    else:
                        logger.debug(f"{symbol}: ${price:.4f} - SKIPPED (price > {MAX_PRICE})")
                        
            except Exception as e:
                logger.warning(f"Error checking price for {symbol}: {e}")
                
            await asyncio.sleep(RATE_LIMIT_DELAY)
        
        return filtered
    
    async def _classify_by_listing_year(
        self,
        symbols: List[str],
        markets: Dict[str, Any],
    ) -> List[CoinInfo]:
        """
        Determine listing year for each symbol by fetching first candle.
        
        Args:
            symbols: List of filtered symbols
            markets: Market data for additional info
            
        Returns:
            List of CoinInfo with listing_year populated
        """
        coins = []
        
        for symbol in symbols:
            market = markets.get(symbol, {})
            
            # Get first candle to determine listing date
            first_candle = await self.exchange.fetch_first_candle(symbol)
            
            if first_candle:
                first_timestamp = first_candle[0]
                listing_year = datetime.fromtimestamp(first_timestamp / 1000).year
                
                coin = CoinInfo(
                    symbol=symbol,
                    base=market.get('base', ''),
                    quote=market.get('quote', TARGET_QUOTE),
                    current_price=self._price_cache.get(symbol, 0.0),
                    listing_year=listing_year,
                    first_timestamp=first_timestamp,
                )
                coins.append(coin)
                
                logger.info(f"Classified {symbol}: Listed in {listing_year}, Price: ${coin.current_price:.4f}")
            else:
                logger.warning(f"Could not determine listing year for {symbol}")
            
            await asyncio.sleep(RATE_LIMIT_DELAY)
        
        return coins
    
    async def get_coin_info(self, symbol: str) -> Optional[CoinInfo]:
        """
        Get detailed information for a single coin.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            CoinInfo if successful, None otherwise
        """
        markets = self.exchange.markets or await self.exchange.load_markets()
        market = markets.get(symbol)
        
        if not market:
            return None
        
        ticker = await self.exchange.fetch_ticker(symbol)
        first_candle = await self.exchange.fetch_first_candle(symbol)
        
        if ticker and first_candle:
            return CoinInfo(
                symbol=symbol,
                base=market.get('base', ''),
                quote=market.get('quote', ''),
                current_price=float(ticker.get('last', 0)),
                listing_year=datetime.fromtimestamp(first_candle[0] / 1000).year,
                first_timestamp=first_candle[0],
            )
        
        return None
