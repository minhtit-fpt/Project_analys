"""
Data Saver
Responsible for handling all data-saving logic.
"""

import os
import logging
import tempfile
from datetime import datetime
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.core.logger import get_logger
from src.core.constants import DEFAULT_TIMEFRAME, DATE_FORMAT
from src.LOGIC.google_cloud_storage_api import GoogleCloudStorageAPI


class SaveData:
    """
    Handles all data-saving operations.
    
    Responsibilities:
    - Create Parquet files with proper structure
    - Handle file naming conventions
    - Manage year-based folder/file structure
    - Handle overwriting and deleting old files
    - Delegate upload operations to GoogleCloudStorageAPI
    """
    
    def __init__(self, storage_api: GoogleCloudStorageAPI, timeframe: str = DEFAULT_TIMEFRAME,
                 logger: logging.Logger = None):
        """
        Initialize the data saver.
        
        Args:
            storage_api: GoogleCloudStorageAPI instance for file uploads
            timeframe: Candle timeframe for filename
            logger: Optional logger instance. If not provided, creates a new one.
        """
        self.storage = storage_api
        self.timeframe = timeframe
        self.logger = logger or get_logger(__name__)
    
    def save_single_year(self, year: int, dataframes: List[pd.DataFrame]):
        """
        Save data for a single year to a Parquet file and upload to Google Cloud Storage immediately.
        All coins are stored in a single Parquet file with a 'symbol' column.
        Old files with outdated dates are removed from Google Cloud Storage.
        
        Args:
            year: The year to save data for
            dataframes: List of DataFrames for this year
        """
        if not dataframes:
            self.logger.warning(f"No data to save for year {year}.")
            return
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        self.logger.info("=" * 80)
        self.logger.info(f"Saving data for year {year} to Google Cloud Storage...")
        self.logger.info("=" * 80)
        
        try:
            # Generate filename with timeframe and current date
            filename = f"Binance_timeframe:{self.timeframe}_{year}_to_{current_date}.parquet"
            
            # Remove old files for this year from Google Drive
            self._cleanup_old_files(year, filename, current_date)
            
            # Create and upload the Parquet file
            self._create_and_upload_parquet(year, dataframes, filename)
            
            self.logger.info("=" * 80)
            self.logger.info(f"Year {year} data uploaded to Google Cloud Storage successfully!")
            self.logger.info("=" * 80)
            
        except Exception as e:
            self.logger.error(f"Error saving data for year {year}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def _cleanup_old_files(self, year: int, current_filename: str, current_date: str):
        """
        Remove old files for a specific year from Google Cloud Storage.
        
        Args:
            year: The year to clean up files for
            current_filename: The current filename (to avoid deleting)
            current_date: The current date string for comparison
        """
        self.logger.info(f"Checking for old files to clean up for year {year}...")
        
        # Search for files with this year and timeframe pattern
        # Pattern: Binance_timeframe:1d_2020_to_
        pattern = f"Binance_timeframe:{self.timeframe}_{year}_to_"
        
        try:
            existing_files = self.storage.list_files(pattern)
            
            if not existing_files:
                self.logger.info(f"  No existing files found for year {year}")
                return
            
            self.logger.info(f"  Found {len(existing_files)} existing file(s) for year {year}")
            
            for old_file in existing_files:
                old_filename = old_file['name']
                
                # Skip if it's the same filename we're about to upload
                if old_filename == current_filename:
                    self.logger.info(f"  Skipping current file: {old_filename}")
                    continue
                
                # Extract date from old filename for comparison
                try:
                    # Format: Binance_timeframe:1d_2020_to_2026-01-30.parquet
                    if '_to_' in old_filename:
                        old_date = old_filename.split('_to_')[1].replace('.parquet', '')
                        
                        self.logger.info(f"  Comparing dates: old={old_date}, current={current_date}")
                        
                        # Delete if old date is less than current date
                        if old_date < current_date:
                            self.logger.info(f"  Deleting old file: {old_filename} (ID: {old_file['id']})")
                            
                            if self.storage.delete_file(old_file['id']):
                                self.logger.info(f"  ✓ Successfully removed: {old_filename}")
                            else:
                                self.logger.error(f"  ✗ Failed to delete: {old_filename}")
                        else:
                            self.logger.info(f"  Keeping file: {old_filename} (not older)")
                    else:
                        self.logger.warning(f"  Cannot parse filename: {old_filename}")
                        
                except Exception as e:
                    self.logger.error(f"  Error processing file {old_filename}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def _create_and_upload_parquet(self, year: int, dataframes: List[pd.DataFrame], filename: str):
        """
        Create a Parquet file from dataframes and upload to Google Cloud Storage.
        
        Args:
            year: The year for the data
            dataframes: List of DataFrames to include
            filename: The filename to use
        """
        # Create a temporary file to write Parquet data
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            temp_path = tmp_file.name
        
        try:
            self.logger.info(f"Creating Parquet file for year {year}...")
            self.logger.info(f"  Temporary file: {temp_path}")
            
            total_records = 0
            coins_saved = []
            sorted_dfs = []
            
            for df in dataframes:
                if df.empty:
                    continue
                
                # Get unique symbol for this dataframe
                symbol = df['symbol'].iloc[0]
                
                # Sort by date
                df_sorted = df.sort_values('date')
                sorted_dfs.append(df_sorted)
                
                total_records += len(df_sorted)
                coins_saved.append(symbol)
                
                self.logger.info(f"  ✓ {symbol}: {len(df_sorted)} records")
            
            # Concatenate all DataFrames and write to Parquet
            if sorted_dfs:
                combined_df = pd.concat(sorted_dfs, ignore_index=True)
                
                # Optimize dtypes for Parquet columnar storage
                combined_df = self._optimize_dtypes(combined_df)
                
                # Sort by symbol then date for optimal compression
                # (groups similar values together in each column)
                combined_df.sort_values(['symbol', 'date'], inplace=True, ignore_index=True)
                
                # Convert to PyArrow Table for fine-grained write control
                table = pa.Table.from_pandas(combined_df, preserve_index=False)
                
                # Write with optimized settings
                pq.write_table(
                    table,
                    temp_path,
                    compression='zstd',           # Better ratio than snappy
                    compression_level=3,           # Good balance of speed vs ratio
                    use_dictionary=['symbol'],     # Dictionary-encode low-cardinality col
                    write_statistics=True,          # Enable column stats for predicate pushdown
                    row_group_size=100_000,         # Optimal row group size for read perf
                )
            
            self.logger.info(f"Parquet file created with {len(coins_saved)} coins, {total_records} total records")
            
            # Verify file was created
            if not os.path.exists(temp_path):
                self.logger.error(f"Parquet file was not created at {temp_path}")
                return
            
            file_size = os.path.getsize(temp_path)
            self.logger.info(f"File size: {file_size / 1024 / 1024:.2f} MB")
            
            # Upload the Parquet file to Google Cloud Storage
            self.logger.info(f"Uploading to GCS: {filename}...")
            file_id = self.storage.upload_file(temp_path, filename)
            
            if file_id:
                self.logger.info(f"\n✓ Successfully uploaded to Google Cloud Storage")
                self.logger.info(f"  File: {filename}")
                self.logger.info(f"  GCS Blob: {file_id}")
                self.logger.info(f"  Total coins: {len(coins_saved)}")
                self.logger.info(f"  Total records: {total_records}")
                self.logger.info(f"  Coins: {', '.join(coins_saved[:5])}{'...' if len(coins_saved) > 5 else ''}\n")
            else:
                self.logger.error(f"✗ Failed to upload {filename} to Google Cloud Storage")
                self.logger.error("  Check GCS credentials and bucket permissions")
                
        except Exception as e:
            self.logger.error(f"Error creating or uploading Parquet file: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            # Clean up temporary file
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                    self.logger.info(f"Cleaned up temporary file: {temp_path}")
                except Exception as e:
                    self.logger.warning(f"Could not delete temporary file {temp_path}: {e}")

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize DataFrame column dtypes for efficient Parquet storage.
        
        Optimizations:
        - symbol → category (low-cardinality string, massive savings with dictionary encoding)
        - date → datetime64[ms] (millisecond precision is sufficient; saves metadata overhead)
        - float64 columns → float32 where safe (halves numeric column storage)
        
        Args:
            df: Combined DataFrame to optimize
            
        Returns:
            DataFrame with optimized dtypes
        """
        optimized = df.copy()
        
        # 1. Symbol: category dtype (few unique values, repeated many times)
        optimized['symbol'] = optimized['symbol'].astype('category')
        
        # 2. Date: downcast to millisecond precision (matches original data granularity)
        optimized['date'] = optimized['date'].astype('datetime64[ms]')
        
        # 3. Float columns: downcast float64 → float32
        #    float32 gives ~7 decimal digits of precision, sufficient for price/volume data
        float_cols = ['open', 'high', 'low', 'close', 'volume',
                      'MA_7', 'MA_25', 'MA_99',
                      'ma_volume_7', 'ma_volume_25', 'ma_volume_99']
        for col in float_cols:
            if col in optimized.columns:
                optimized[col] = optimized[col].astype('float32')
        
        self.logger.info(f"  Dtype optimization applied: symbol→category, date→datetime64[ms], floats→float32")
        
        return optimized
