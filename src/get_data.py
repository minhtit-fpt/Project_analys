"""
Binance Data Fetcher
Responsible for retrieving data from Binance Futures API.
"""

import ccxt
import pandas as pd
import time
import logging
from datetime import datetime
from typing import List, Optional
from collections import defaultdict
from tqdm import tqdm


class GetData:
    """
    Handles all data retrieval operations from Binance Futures.
    
    Responsibilities:
    - Initialize exchange connection
    - Fetch available markets
    - Filter symbols by price threshold
    - Detect listing dates
    - Fetch historical OHLCV data
    - Calculate moving averages
    """
    
    def __init__(self, price_threshold: float = 10.0, timeframe: str = '1d', logger: logging.Logger = None):
        """
        Initialize the Binance data fetcher.
        
        Args:
            price_threshold: Maximum price in USDT to filter coins (default: 10.0)
            timeframe: Candle timeframe (default: '1d' for daily)
            logger: Optional logger instance. If not provided, creates a new one.
        """
        self.price_threshold = price_threshold
        self.timeframe = timeframe
        self.logger = logger or logging.getLogger(__name__)
        self.data_store = defaultdict(list)  # Store data grouped by year
        
        # Initialize ccxt exchange with rate limit protection
        self.exchange = ccxt.binance({
            'enableRateLimit': True,  # Built-in rate limit handling
            'options': {
                'defaultType': 'future',  # Set to futures market
                'adjustForTimeDifference': True,
            },
            'timeout': 30000,
        })
    
    def get_markets(self) -> List[str]:
        """
        Fetch all available markets and filter for USDT futures with price < threshold.
        
        Returns:
            List of filtered symbol names
        """
        self.logger.info("Fetching market data from Binance Futures...")
        
        try:
            # Load all markets
            markets = self.exchange.load_markets()
            self.logger.info(f"Total markets loaded: {len(markets)}")
            
            # Fetch current tickers for price filtering
            tickers = self.exchange.fetch_tickers()
            self.logger.info(f"Total tickers fetched: {len(tickers)}")
            
            filtered_symbols = []
            usdt_futures = 0
            price_filtered = 0
            
            for symbol, market in markets.items():
                # More flexible filter for USDT-margined futures
                # Binance uses 'swap' type for perpetual futures
                if (market.get('quote') == 'USDT' and 
                    market.get('type') in ['future', 'swap'] and
                    market.get('linear') == True and
                    market.get('active') == True):
                    
                    usdt_futures += 1
                    
                    if symbol in tickers:
                        ticker = tickers[symbol]
                        last_price = ticker.get('last')
                        
                        if last_price:
                            if last_price < self.price_threshold:
                                filtered_symbols.append(symbol)
                                price_filtered += 1
            
            self.logger.info(f"USDT-margined futures found: {usdt_futures}")
            self.logger.info(f"Coins with price < {self.price_threshold} USDT: {price_filtered}")
            self.logger.info(f"Final filtered symbols: {len(filtered_symbols)}")
            
            # Debug: show first 10 symbols
            if filtered_symbols:
                self.logger.info(f"Sample symbols: {filtered_symbols[:10]}")
            
            return filtered_symbols
            
        except Exception as e:
            self.logger.error(f"Error fetching markets: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return []
    
    def _detect_listing_date(self, symbol: str) -> Optional[int]:
        """
        Detect the listing date of a coin by finding the first available candle.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            
        Returns:
            Timestamp in milliseconds of the first candle, or None if not found
        """
        try:
            # Start from a very early date (Binance Futures started around 2019)
            start_date = self.exchange.parse8601('2019-01-01T00:00:00Z')
            
            # Fetch the first available candle
            candles = self.exchange.fetch_ohlcv(
                symbol, 
                timeframe=self.timeframe,
                since=start_date,
                limit=1
            )
            
            if candles and len(candles) > 0:
                return candles[0][0]  # Return timestamp of first candle
            
            return None
            
        except ccxt.RateLimitExceeded:
            self.logger.warning(f"Rate limit exceeded while detecting listing date for {symbol}. Sleeping...")
            time.sleep(5)
            return self._detect_listing_date(symbol)
        except Exception as e:
            self.logger.error(f"Error detecting listing date for {symbol}: {e}")
            return None
    
    def fetch_candles(self, symbol: str, since: int) -> Optional[pd.DataFrame]:
        """
        Fetch all historical OHLCV data from listing date to present.
        
        Args:
            symbol: Trading pair symbol
            since: Start timestamp in milliseconds
            
        Returns:
            DataFrame with OHLCV data including MAs, or None if error
        """
        all_candles = []
        current_since = since
        now = self.exchange.milliseconds()
        
        try:
            while current_since < now:
                # Fetch batch of candles
                candles = self.exchange.fetch_ohlcv(
                    symbol,
                    timeframe=self.timeframe,
                    since=current_since,
                    limit=1000  # Max limit for most exchanges
                )
                
                if not candles:
                    break
                
                all_candles.extend(candles)
                
                # Update since to the last candle's timestamp + 1
                current_since = candles[-1][0] + 1
                
                # Break if we received fewer candles than requested (reached the end)
                if len(candles) < 1000:
                    break
                
                # Small sleep to respect rate limits (ccxt handles this, but extra safety)
                time.sleep(0.1)
            
            if not all_candles:
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(
                all_candles,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            
            # Add symbol column
            df['symbol'] = symbol
            
            # Convert timestamp to datetime
            df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Calculate Moving Averages
            df['MA_7'] = df['close'].rolling(window=7, min_periods=1).mean()
            df['MA_25'] = df['close'].rolling(window=25, min_periods=1).mean()
            df['MA_50'] = df['close'].rolling(window=50, min_periods=1).mean()
            df['MA_99'] = df['close'].rolling(window=99, min_periods=1).mean()
            df['MA_200'] = df['close'].rolling(window=200, min_periods=1).mean()
            
            # Reorder columns
            df = df[['symbol', 'date', 'timestamp', 'open', 'high', 'low', 'close', 
                    'volume', 'MA_7', 'MA_25', 'MA_50', 'MA_99', 'MA_200']]
            
            return df
            
        except ccxt.RateLimitExceeded:
            self.logger.warning(f"Rate limit exceeded for {symbol}. Sleeping for 60 seconds...")
            time.sleep(60)
            return self.fetch_candles(symbol, since)
        except ccxt.ExchangeNotAvailable as e:
            self.logger.error(f"Exchange not available for {symbol}: {e}")
            time.sleep(30)
            return self.fetch_candles(symbol, since)
        except Exception as e:
            self.logger.error(f"Error fetching candles for {symbol}: {e}")
            return None
    
    def get_year_range(self) -> tuple:
        """
        Get the range of years that have available data.
        
        Returns:
            Tuple of (min_year, max_year) or (None, None) if no data available
        """
        self.logger.info("Detecting year range for available data...")
        
        # Get filtered markets
        symbols = self.get_markets()
        
        if not symbols:
            self.logger.error("No symbols found.")
            return (None, None)
        
        min_year = None
        max_year = datetime.now().year
        
        # Sample symbols to find the earliest listing date
        sample_size = min(10, len(symbols))
        self.logger.info(f"Sampling {sample_size} symbols to detect year range...")
        
        for symbol in symbols[:sample_size]:
            try:
                listing_timestamp = self._detect_listing_date(symbol)
                if listing_timestamp:
                    listing_date = datetime.fromtimestamp(listing_timestamp / 1000)
                    listing_year = listing_date.year
                    
                    if min_year is None or listing_year < min_year:
                        min_year = listing_year
                        
            except Exception as e:
                self.logger.error(f"Error detecting year for {symbol}: {e}")
                continue
        
        if min_year is None:
            min_year = 2019  # Default to Binance Futures start year
        
        self.logger.info(f"Year range detected: {min_year} to {max_year}")
        return (min_year, max_year)
    
    def fetch_data_for_year(self, target_year: int) -> dict:
        """
        Fetch data for all symbols listed in a specific year.
        
        Args:
            target_year: The year to fetch data for
            
        Returns:
            Dictionary with the target year as key and list of DataFrames as value
        """
        self.logger.info("=" * 80)
        self.logger.info(f"Fetching data for year {target_year}")
        self.logger.info("=" * 80)
        
        # Get filtered markets
        symbols = self.get_markets()
        
        if not symbols:
            self.logger.error("No symbols found.")
            return {}
        
        year_data = []
        
        self.logger.info(f"Processing {len(symbols)} symbols for year {target_year}...")
        
        # Iterate through symbols with progress bar
        for symbol in tqdm(symbols, desc=f"Fetching data for {target_year}", unit="coin"):
            try:
                # Detect listing date
                listing_timestamp = self._detect_listing_date(symbol)
                
                if not listing_timestamp:
                    continue
                
                # Get listing year
                listing_date = datetime.fromtimestamp(listing_timestamp / 1000)
                listing_year = listing_date.year
                
                # Only process symbols listed in the target year
                if listing_year != target_year:
                    continue
                
                self.logger.info(f"{symbol} listed in {listing_year}")
                
                # Fetch all historical data
                df = self.fetch_candles(symbol, listing_timestamp)
                
                if df is None or df.empty:
                    self.logger.warning(f"No data fetched for {symbol}. Skipping...")
                    continue
                
                year_data.append(df)
                
                self.logger.info(f"Successfully fetched {len(df)} candles for {symbol}")
                
            except Exception as e:
                self.logger.error(f"Unexpected error processing {symbol}: {e}")
                continue
        
        if year_data:
            self.logger.info(f"Completed fetching data for year {target_year}: {len(year_data)} coins")
        else:
            self.logger.info(f"No coins found listed in year {target_year}")
        
        return {target_year: year_data} if year_data else {}
    
    
