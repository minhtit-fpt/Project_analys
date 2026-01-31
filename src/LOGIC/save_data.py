"""
Data Saver
Responsible for handling all data-saving logic.
"""

import os
import logging
import tempfile
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from src.LOGIC.google_drive_api import GoogleDriveAPI


class SaveData:
    """
    Handles all data-saving operations.
    
    Responsibilities:
    - Create Excel files with proper structure
    - Handle file naming conventions
    - Manage year-based folder/file structure
    - Handle overwriting and deleting old files
    - Delegate upload operations to GoogleDriveAPI
    """
    
    def __init__(self, google_drive_api: GoogleDriveAPI, timeframe: str = '1d', logger: logging.Logger = None):
        """
        Initialize the data saver.
        
        Args:
            google_drive_api: GoogleDriveAPI instance for file uploads
            timeframe: Candle timeframe for filename (default: '1d')
            logger: Optional logger instance. If not provided, creates a new one.
        """
        self.google_drive = google_drive_api
        self.timeframe = timeframe
        self.logger = logger or logging.getLogger(__name__)
    
    def save_single_year(self, year: int, dataframes: List[pd.DataFrame]):
        """
        Save data for a single year to Excel file and upload to Google Drive immediately.
        Each coin gets its own sheet within the year's Excel file.
        Old files with outdated dates are removed from Google Drive.
        
        Args:
            year: The year to save data for
            dataframes: List of DataFrames for this year
        """
        if not dataframes:
            self.logger.warning(f"No data to save for year {year}.")
            return
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        self.logger.info("=" * 80)
        self.logger.info(f"Saving data for year {year} to Google Drive...")
        self.logger.info("=" * 80)
        
        try:
            # Generate filename with timeframe and current date
            filename = f"Binance_timeframe:{self.timeframe}_{year}_to_{current_date}.xlsx"
            
            # Remove old files for this year from Google Drive
            self._cleanup_old_files(year, filename, current_date)
            
            # Create and upload the Excel file
            self._create_and_upload_excel(year, dataframes, filename)
            
            self.logger.info("=" * 80)
            self.logger.info(f"Year {year} data uploaded to Google Drive successfully!")
            self.logger.info("=" * 80)
            
        except Exception as e:
            self.logger.error(f"Error saving data for year {year}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def _cleanup_old_files(self, year: int, current_filename: str, current_date: str):
        """
        Remove old files for a specific year from Google Drive.
        
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
            existing_files = self.google_drive.list_files(pattern)
            
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
                    # Format: Binance_timeframe:1d_2020_to_2026-01-30.xlsx
                    if '_to_' in old_filename:
                        old_date = old_filename.split('_to_')[1].replace('.xlsx', '')
                        
                        self.logger.info(f"  Comparing dates: old={old_date}, current={current_date}")
                        
                        # Delete if old date is less than current date
                        if old_date < current_date:
                            self.logger.info(f"  Deleting old file: {old_filename} (ID: {old_file['id']})")
                            
                            if self.google_drive.delete_file(old_file['id']):
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
    
    def _create_and_upload_excel(self, year: int, dataframes: List[pd.DataFrame], filename: str):
        """
        Create an Excel file from dataframes and upload to Google Drive.
        
        Args:
            year: The year for the data
            dataframes: List of DataFrames to include as sheets
            filename: The filename to use
        """
        # Create a temporary file to write Excel data
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            temp_path = tmp_file.name
        
        try:
            self.logger.info(f"Creating Excel file for year {year}...")
            self.logger.info(f"  Temporary file: {temp_path}")
            
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
            
            self.logger.info(f"Excel file created with {len(coins_saved)} sheets, {total_records} total records")
            
            # Verify file was created
            if not os.path.exists(temp_path):
                self.logger.error(f"Excel file was not created at {temp_path}")
                return
            
            file_size = os.path.getsize(temp_path)
            self.logger.info(f"File size: {file_size / 1024 / 1024:.2f} MB")
            
            # Upload the Excel file to Google Drive
            self.logger.info(f"Uploading to Google Drive: {filename}...")
            file_id = self.google_drive.upload_file(temp_path, filename)
            
            if file_id:
                self.logger.info(f"\n✓ Successfully uploaded to Google Drive")
                self.logger.info(f"  File: {filename}")
                self.logger.info(f"  Google Drive File ID: {file_id}")
                self.logger.info(f"  Total sheets (coins): {len(coins_saved)}")
                self.logger.info(f"  Total records: {total_records}")
                self.logger.info(f"  Coins: {', '.join(coins_saved[:5])}{'...' if len(coins_saved) > 5 else ''}\n")
            else:
                self.logger.error(f"✗ Failed to upload {filename} to Google Drive")
                self.logger.error("  Check Google Drive credentials and permissions")
                
        except Exception as e:
            self.logger.error(f"Error creating or uploading Excel file: {e}")
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
