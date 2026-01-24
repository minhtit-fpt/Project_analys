"""
Binance Futures Historical Data Fetcher
Author: Senior Python Backend Engineer
Date: 2026-01-23
Description: Production-ready OOP script to fetch historical OHLCV data from Binance Futures (USDT-M)
"""

import ccxt
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from tqdm import tqdm
import os
import logging
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows


class BinanceFutureFetcher:
    """
    High-Frequency Trading data fetcher for Binance Futures market.
    Fetches historical OHLCV data grouped by listing year.
    """
    
    def __init__(self, price_threshold: float = 10.0, timeframe: str = '1d'):
        """
        Initialize the Binance Futures fetcher.
        
        Args:
            price_threshold: Maximum price in USDT to filter coins (default: 10.0)
            timeframe: Candle timeframe (default: '1d' for daily)
        """
        self.price_threshold = price_threshold
        self.timeframe = timeframe
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
        
        # Setup logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Configure logging for the application."""
        log_dir = 'logs'
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{log_dir}/binance_fetcher_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
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
    
    def process_and_save(self):
        """
        Main execution flow: fetch markets, get data, group by year, and save to CSV.
        """
        self.logger.info("=" * 80)
        self.logger.info("Starting Binance Futures Historical Data Fetcher")
        self.logger.info("=" * 80)
        
        # Step 1: Get filtered markets
        symbols = self.get_markets()
        
        if not symbols:
            self.logger.error("No symbols found. Exiting...")
            return
        
        self.logger.info(f"Processing {len(symbols)} symbols...")
        
        # Step 2: Iterate through symbols with progress bar
        for symbol in tqdm(symbols, desc="Fetching historical data", unit="coin"):
            try:
                self.logger.info(f"Processing {symbol}...")
                
                # Detect listing date
                listing_timestamp = self._detect_listing_date(symbol)
                
                if not listing_timestamp:
                    self.logger.warning(f"Could not detect listing date for {symbol}. Skipping...")
                    continue
                
                # Get listing year
                listing_date = datetime.fromtimestamp(listing_timestamp / 1000)
                listing_year = listing_date.year
                
                self.logger.info(f"{symbol} listed in {listing_year}")
                
                # Fetch all historical data
                df = self.fetch_candles(symbol, listing_timestamp)
                
                if df is None or df.empty:
                    self.logger.warning(f"No data fetched for {symbol}. Skipping...")
                    continue
                
                # Group by listing year
                self.data_store[listing_year].append(df)
                
                self.logger.info(f"Successfully fetched {len(df)} candles for {symbol}")
                
            except Exception as e:
                self.logger.error(f"Unexpected error processing {symbol}: {e}")
                continue
        
        # Step 3: Save data grouped by year
        self._save_by_year()
    
    def _save_by_year(self):
        """
        Save grouped data to Excel files by listing year.
        Each coin gets its own sheet within the year's Excel file.
        """
        if not self.data_store:
            self.logger.warning("No data to save.")
            return
        
        output_dir = 'data/binance_future'
        os.makedirs(output_dir, exist_ok=True)
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        self.logger.info("=" * 80)
        self.logger.info("Saving data to Excel files (each coin = 1 sheet)...")
        self.logger.info("=" * 80)
        
        for year, dataframes in self.data_store.items():
            try:
                # Generate filename with current date
                filename = f"{output_dir}/Binance_{year}_to_{current_date}.csv"
                
                # Check and remove old files for this year if new date is greater
                import glob
                pattern = f"{output_dir}/Binance_{year}_to_*.csv"
                existing_files = glob.glob(pattern)
                
                for old_file in existing_files:
                    if old_file != filename:
                        # Extract date from old filename
                        try:
                            old_date = old_file.split('_to_')[1].replace('.csv', '')
                            # Compare dates
                            if current_date > old_date:
                                os.remove(old_file)
                                self.logger.info(f"  Removed old file: {old_file}")
                        except Exception as e:
                            self.logger.warning(f"  Could not process old file {old_file}: {e}")
                
                # Create Excel writer (will override if same filename exists)
                with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
                    total_records = 0
                    coins_saved = []
                    
                    for df in dataframes:
                        if df.empty:
                            continue
                        
                        # Get unique symbol for this dataframe
                        symbol = df['symbol'].iloc[0]
                        
                        # Clean sheet name (Excel has restrictions)
                        # Remove forward slash and limit to 31 characters
                        sheet_name = symbol.replace('/', '_').replace(':', '_')[:31]
                        
                        # Sort by date
                        df_sorted = df.sort_values('date')
                        
                        # Write to Excel sheet
                        df_sorted.to_excel(writer, sheet_name=sheet_name, index=False)
                        
                        total_records += len(df_sorted)
                        coins_saved.append(symbol)
                        
                        self.logger.info(f"  ✓ {symbol}: {len(df_sorted)} records → sheet '{sheet_name}'")
                
                self.logger.info(f"\n✓ Saved year {year} to Excel")
                self.logger.info(f"  File: {filename}")
                self.logger.info(f"  Total sheets (coins): {len(coins_saved)}")
                self.logger.info(f"  Total records: {total_records}")
                self.logger.info(f"  Coins: {', '.join(coins_saved[:5])}{'...' if len(coins_saved) > 5 else ''}\n")
                
            except Exception as e:
                self.logger.error(f"Error saving data for year {year}: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
        
        self.logger.info("=" * 80)
        self.logger.info("Data fetching and saving completed successfully!")
        self.logger.info("=" * 80)


def main():
    """
    Entry point for the application.
    """
    try:
        # Initialize fetcher with price threshold of 10 USDT
        fetcher = BinanceFutureFetcher(price_threshold=10.0, timeframe='1d')
        
        # Execute the full pipeline
        fetcher.process_and_save()
        
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Exiting gracefully...")
    except Exception as e:
        logging.error(f"Critical error in main execution: {e}")
        raise


if __name__ == "__main__":
    main()
