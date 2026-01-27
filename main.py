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
import glob
import tempfile
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Load environment variables from .env file
load_dotenv()


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
        
        # Initialize Google Drive service
        self._setup_google_drive()
        
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
    
    def _setup_google_drive(self):
        """
        Initialize Google Drive API service using OAuth 2.0 authentication.
        Reads OAuth credentials and tokens from environment variables.
        Validates existing refresh token and re-authenticates if necessary.
        """
        try:
            # Get configuration from environment variables
            self.drive_folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
            client_id = os.getenv('GOOGLE_CLIENT_ID')
            client_secret = os.getenv('GOOGLE_CLIENT_SECRET')
            refresh_token = os.getenv('GOOGLE_REFRESH_TOKEN')
            
            if not self.drive_folder_id:
                raise ValueError("GOOGLE_DRIVE_FOLDER_ID environment variable is not set")
            
            if not client_id or not client_secret:
                raise ValueError("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set in .env file")
            
            # Clean refresh token (remove quotes if present)
            if refresh_token:
                refresh_token = refresh_token.strip().strip("'\"")
                if not refresh_token:
                    refresh_token = None
            
            # Define the scopes required for Google Drive API
            SCOPES = ['https://www.googleapis.com/auth/drive.file']
            
            creds = None
            needs_reauth = False
            
            # Try to use existing refresh token
            if refresh_token:
                self.logger.info("Found existing refresh token, attempting to use it...")
                try:
                    creds = Credentials(
                        token=None,
                        refresh_token=refresh_token,
                        token_uri='https://oauth2.googleapis.com/token',
                        client_id=client_id,
                        client_secret=client_secret,
                        scopes=SCOPES
                    )
                    
                    # Force refresh to validate the token
                    creds.refresh(Request())
                    self.logger.info("Successfully refreshed credentials using existing refresh token")
                    
                    # Validate by making a test API call
                    test_service = build('drive', 'v3', credentials=creds)
                    test_service.files().list(pageSize=1).execute()
                    self.logger.info("Refresh token validated successfully")
                    
                except Exception as e:
                    self.logger.warning(f"Existing refresh token is invalid or expired: {e}")
                    self.logger.info("Will trigger re-authentication...")
                    creds = None
                    needs_reauth = True
            else:
                self.logger.info("No refresh token found, authentication required")
                needs_reauth = True
            
            # Perform OAuth flow if needed
            if creds is None or needs_reauth:
                self.logger.info("Starting OAuth 2.0 authentication flow...")
                print("\n" + "=" * 60)
                print("Google Drive Authentication Required")
                print("A browser window will open for you to log in.")
                print("=" * 60 + "\n")
                
                flow = InstalledAppFlow.from_client_config(
                    {
                        "installed": {
                            "client_id": client_id,
                            "client_secret": client_secret,
                            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                            "token_uri": "https://oauth2.googleapis.com/token",
                            "redirect_uris": ["http://localhost"]
                        }
                    },
                    SCOPES
                )
                creds = flow.run_local_server(port=0)
                
                # Save the new refresh token to .env file
                if creds.refresh_token:
                    self._save_refresh_token(creds.refresh_token)
                    self.logger.info("New refresh token saved to .env file")
                else:
                    self.logger.warning("No refresh token received from OAuth flow")
            
            # Build the Google Drive API service
            self.drive_service = build('drive', 'v3', credentials=creds)
            
            self.logger.info("Google Drive service initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing Google Drive service: {e}")
            raise
    
    def _save_refresh_token(self, refresh_token: str):
        """
        Save the refresh token to the .env file.
        
        Args:
            refresh_token: The OAuth refresh token to save
        """
        try:
            env_path = os.path.join(os.path.dirname(__file__), '.env')
            
            # Read the current .env file
            with open(env_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Find and update the GOOGLE_REFRESH_TOKEN line
            token_found = False
            for i, line in enumerate(lines):
                if line.startswith('GOOGLE_REFRESH_TOKEN='):
                    lines[i] = f'GOOGLE_REFRESH_TOKEN={refresh_token}\n'
                    token_found = True
                    break
            
            # If not found, append it
            if not token_found:
                lines.append(f'\nGOOGLE_REFRESH_TOKEN={refresh_token}\n')
            
            # Write back to .env file
            with open(env_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            # Also update the environment variable in current session
            os.environ['GOOGLE_REFRESH_TOKEN'] = refresh_token
            
        except Exception as e:
            self.logger.error(f"Failed to save refresh token to .env file: {e}")
            raise
    
    def _list_drive_files(self, name_pattern: str = None) -> List[Dict]:
        """
        List files in the Google Drive folder.
        
        Args:
            name_pattern: Optional pattern to filter files by name
            
        Returns:
            List of file metadata dictionaries with 'id' and 'name'
        """
        try:
            query = f"'{self.drive_folder_id}' in parents and trashed = false"
            
            if name_pattern:
                query += f" and name contains '{name_pattern}'"
            
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            return results.get('files', [])
            
        except Exception as e:
            self.logger.error(f"Error listing files from Google Drive: {e}")
            return []
    
    def _delete_drive_file(self, file_id: str) -> bool:
        """
        Delete a file from Google Drive.
        
        Args:
            file_id: The ID of the file to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.drive_service.files().delete(fileId=file_id).execute()
            return True
        except Exception as e:
            self.logger.error(f"Error deleting file {file_id} from Google Drive: {e}")
            return False
    
    def _upload_to_drive(self, file_path: str, filename: str) -> Optional[str]:
        """
        Upload a file to Google Drive.
        
        Args:
            file_path: Local path to the file to upload
            filename: Name to give the file in Google Drive
            
        Returns:
            File ID if successful, None otherwise
        """
        try:
            file_metadata = {
                'name': filename,
                'parents': [self.drive_folder_id]
            }
            
            # Determine MIME type based on file extension
            mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            
            media = MediaFileUpload(
                file_path,
                mimetype=mime_type,
                resumable=True
            )
            
            # Check if file with same name already exists
            existing_files = self._list_drive_files(filename)
            for existing_file in existing_files:
                if existing_file['name'] == filename:
                    # Update existing file
                    updated_file = self.drive_service.files().update(
                        fileId=existing_file['id'],
                        media_body=media
                    ).execute()
                    self.logger.info(f"Updated existing file: {filename}")
                    return updated_file.get('id')
            
            # Create new file
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            self.logger.info(f"Uploaded new file: {filename}")
            return file.get('id')
            
        except Exception as e:
            self.logger.error(f"Error uploading file to Google Drive: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
    
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
        Save grouped data to Excel files by listing year and upload to Google Drive.
        Each coin gets its own sheet within the year's Excel file.
        Old files with outdated dates are removed from Google Drive.
        """
        if not self.data_store:
            self.logger.warning("No data to save.")
            return
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        self.logger.info("=" * 80)
        self.logger.info("Saving data to Excel files and uploading to Google Drive (each coin = 1 sheet)...")
        self.logger.info("=" * 80)
        
        for year, dataframes in self.data_store.items():
            try:
                # Generate filename with current date
                filename = f"Binance_{year}_to_{current_date}.xlsx"
                
                # Check and remove old files for this year from Google Drive
                pattern = f"Binance_{year}_to_"
                existing_files = self._list_drive_files(pattern)
                
                for old_file in existing_files:
                    if old_file['name'] != filename:
                        # Extract date from old filename
                        try:
                            old_date = old_file['name'].split('_to_')[1].replace('.xlsx', '')
                            # Compare dates
                            if current_date > old_date:
                                if self._delete_drive_file(old_file['id']):
                                    self.logger.info(f"  Removed old file from Google Drive: {old_file['name']}")
                        except Exception as e:
                            self.logger.warning(f"  Could not process old file {old_file['name']}: {e}")
                
                # Create a temporary file to write Excel data
                with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
                    temp_path = tmp_file.name
                
                try:
                    # Create Excel writer using temporary file
                    with pd.ExcelWriter(temp_path, engine='openpyxl', mode='w') as writer:
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
                    
                    # Upload the Excel file to Google Drive
                    file_id = self._upload_to_drive(temp_path, filename)
                    
                    if file_id:
                        self.logger.info(f"\n✓ Saved year {year} to Google Drive")
                        self.logger.info(f"  File: {filename}")
                        self.logger.info(f"  Google Drive File ID: {file_id}")
                        self.logger.info(f"  Total sheets (coins): {len(coins_saved)}")
                        self.logger.info(f"  Total records: {total_records}")
                        self.logger.info(f"  Coins: {', '.join(coins_saved[:5])}{'...' if len(coins_saved) > 5 else ''}\n")
                    else:
                        self.logger.error(f"Failed to upload {filename} to Google Drive")
                        
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                
            except Exception as e:
                self.logger.error(f"Error saving data for year {year}: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
        
        self.logger.info("=" * 80)
        self.logger.info("Data fetching and uploading to Google Drive completed successfully!")
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
