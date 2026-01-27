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

from src.google_drive_api import GoogleDriveAPI


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
    
    def __init__(self, google_drive_api: GoogleDriveAPI, logger: logging.Logger = None):
        """
        Initialize the data saver.
        
        Args:
            google_drive_api: GoogleDriveAPI instance for file uploads
            logger: Optional logger instance. If not provided, creates a new one.
        """
        self.google_drive = google_drive_api
        self.logger = logger or logging.getLogger(__name__)
    
    def save_by_year(self, data_store: Dict[int, List[pd.DataFrame]]):
        """
        Save grouped data to Excel files by listing year and upload to Google Drive.
        Each coin gets its own sheet within the year's Excel file.
        Old files with outdated dates are removed from Google Drive.
        
        Args:
            data_store: Dictionary with years as keys and list of DataFrames as values
        """
        if not data_store:
            self.logger.warning("No data to save.")
            return
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        self.logger.info("=" * 80)
        self.logger.info("Saving data to Excel files and uploading to Google Drive (each coin = 1 sheet)...")
        self.logger.info("=" * 80)
        
        for year, dataframes in data_store.items():
            try:
                # Generate filename with current date
                filename = f"Binance_{year}_to_{current_date}.xlsx"
                
                # Remove old files for this year from Google Drive
                self._cleanup_old_files(year, filename, current_date)
                
                # Create and upload the Excel file
                self._create_and_upload_excel(year, dataframes, filename)
                
            except Exception as e:
                self.logger.error(f"Error saving data for year {year}: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
        
        self.logger.info("=" * 80)
        self.logger.info("Data fetching and uploading to Google Drive completed successfully!")
        self.logger.info("=" * 80)
    
    def _cleanup_old_files(self, year: int, current_filename: str, current_date: str):
        """
        Remove old files for a specific year from Google Drive.
        
        Args:
            year: The year to clean up files for
            current_filename: The current filename (to avoid deleting)
            current_date: The current date string for comparison
        """
        # Check and remove old files for this year from Google Drive
        pattern = f"Binance_{year}_to_"
        existing_files = self.google_drive.list_files(pattern)
        
        for old_file in existing_files:
            if old_file['name'] != current_filename:
                # Extract date from old filename
                try:
                    old_date = old_file['name'].split('_to_')[1].replace('.xlsx', '')
                    # Compare dates
                    if current_date > old_date:
                        if self.google_drive.delete_file(old_file['id']):
                            self.logger.info(f"  Removed old file from Google Drive: {old_file['name']}")
                except Exception as e:
                    self.logger.warning(f"  Could not process old file {old_file['name']}: {e}")
    
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
            file_id = self.google_drive.upload_file(temp_path, filename)
            
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
