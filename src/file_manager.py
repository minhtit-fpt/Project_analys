"""
Cohort File Manager Module.

Handles dynamic file management:
- File rotation (rename old files to current year)
- CSV append operations
- Last timestamp tracking per symbol
"""

import os
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

import pandas as pd

from config.settings import (
    DATA_DIR,
    FILE_PREFIX,
    FILE_EXTENSION,
    CURRENT_YEAR,
    CSV_COLUMNS,
    EXCHANGE_ID,
    MARKET_TYPE,
)

logger = logging.getLogger(__name__)


class CohortFileManager:
    """
    Manages CSV files organized by listing year cohorts.
    
    File naming convention:
        {exchange}_{market_type}_coins_{listing_year}_to_{current_year}.csv
        
    Example:
        binance_future_coins_2020_to_2026.csv
        
    Features:
    - Automatic file rotation when year changes
    - Track last timestamp per symbol for incremental updates
    - Append new data to existing files
    """
    
    def __init__(self, data_dir: Path = DATA_DIR):
        """
        Initialize the file manager.
        
        Args:
            data_dir: Directory path for data storage
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Pattern: binance_future_coins_2020_to_2025.csv
        self.file_pattern = re.compile(
            rf"{FILE_PREFIX}_(\d{{4}})_to_(\d{{4}}){FILE_EXTENSION}$"
        )
        
        # Cache for last timestamps: {listing_year: {symbol: last_timestamp}}
        self._last_timestamps: Dict[int, Dict[str, int]] = {}
    
    def get_file_path(self, listing_year: int) -> Path:
        """
        Get the file path for a given listing year.
        
        Args:
            listing_year: Year when coins were listed
            
        Returns:
            Path to the CSV file
        """
        filename = f"{FILE_PREFIX}_{listing_year}_to_{CURRENT_YEAR}{FILE_EXTENSION}"
        return self.data_dir / filename
    
    def find_existing_file(self, listing_year: int) -> Optional[Path]:
        """
        Find existing file for a listing year (may have old year suffix).
        
        Args:
            listing_year: Year when coins were listed
            
        Returns:
            Path to existing file if found, None otherwise
        """
        pattern = f"{FILE_PREFIX}_{listing_year}_to_*{FILE_EXTENSION}"
        
        for file_path in self.data_dir.glob(pattern):
            match = self.file_pattern.search(file_path.name)
            if match and int(match.group(1)) == listing_year:
                return file_path
        
        return None
    
    def rotate_file_if_needed(self, listing_year: int) -> Tuple[Path, bool]:
        """
        Check and rotate file if it has an old year suffix.
        
        Case A: Found file with old year (e.g., ...to_2025.csv in 2026)
                -> Rename to ...to_2026.csv
                
        Case B: No existing file
                -> Return new file path (will be created on first write)
        
        Args:
            listing_year: Year when coins were listed
            
        Returns:
            Tuple of (final_path, was_rotated)
        """
        existing_file = self.find_existing_file(listing_year)
        target_path = self.get_file_path(listing_year)
        
        if existing_file is None:
            # Case B: No existing file
            logger.info(f"No existing file for {listing_year}. Will create: {target_path.name}")
            return target_path, False
        
        if existing_file == target_path:
            # File already has current year suffix
            logger.info(f"File already up-to-date: {target_path.name}")
            return target_path, False
        
        # Case A: Need to rotate file
        logger.info(f"Rotating file: {existing_file.name} -> {target_path.name}")
        
        try:
            os.rename(existing_file, target_path)
            return target_path, True
        except OSError as e:
            logger.error(f"Failed to rotate file: {e}")
            raise
    
    def load_last_timestamps(self, listing_year: int) -> Dict[str, int]:
        """
        Load last timestamps for all symbols in a cohort file.
        
        Args:
            listing_year: Year when coins were listed
            
        Returns:
            Dict mapping symbol to its last timestamp (milliseconds)
        """
        if listing_year in self._last_timestamps:
            return self._last_timestamps[listing_year]
        
        file_path = self.get_file_path(listing_year)
        timestamps = {}
        
        if file_path.exists():
            try:
                # Read only necessary columns for efficiency
                df = pd.read_csv(file_path, usecols=['timestamp', 'symbol'])
                
                # Get last timestamp for each symbol
                for symbol in df['symbol'].unique():
                    symbol_data = df[df['symbol'] == symbol]
                    timestamps[symbol] = int(symbol_data['timestamp'].max())
                
                logger.info(f"Loaded last timestamps for {len(timestamps)} symbols from {file_path.name}")
                
            except Exception as e:
                logger.warning(f"Error loading timestamps from {file_path.name}: {e}")
        
        self._last_timestamps[listing_year] = timestamps
        return timestamps
    
    def get_last_timestamp(self, listing_year: int, symbol: str) -> Optional[int]:
        """
        Get last timestamp for a specific symbol.
        
        Args:
            listing_year: Year when coin was listed
            symbol: Trading pair symbol
            
        Returns:
            Last timestamp in milliseconds, or None if not found
        """
        timestamps = self.load_last_timestamps(listing_year)
        return timestamps.get(symbol)
    
    def prepare_cohort_file(self, listing_year: int) -> Tuple[Path, Dict[str, int]]:
        """
        Prepare file for a cohort: rotate if needed and load timestamps.
        
        Args:
            listing_year: Year when coins were listed
            
        Returns:
            Tuple of (file_path, last_timestamps_dict)
        """
        # Step 1: Rotate file if needed
        file_path, was_rotated = self.rotate_file_if_needed(listing_year)
        
        # Step 2: Load last timestamps
        last_timestamps = self.load_last_timestamps(listing_year)
        
        return file_path, last_timestamps
    
    def append_ohlcv_data(
        self,
        listing_year: int,
        symbol: str,
        ohlcv_data: List[List],
    ) -> int:
        """
        Append OHLCV data to the cohort file.
        
        Args:
            listing_year: Year when coin was listed
            symbol: Trading pair symbol
            ohlcv_data: List of [timestamp, open, high, low, close, volume]
            
        Returns:
            Number of records appended
        """
        if not ohlcv_data:
            return 0
        
        file_path = self.get_file_path(listing_year)
        
        # Convert OHLCV to DataFrame
        df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['symbol'] = symbol
        
        # Reorder columns to match CSV_COLUMNS
        df = df[CSV_COLUMNS]
        
        # Check if file exists (for header decision)
        file_exists = file_path.exists()
        
        # Append to CSV
        df.to_csv(
            file_path,
            mode='a',
            header=not file_exists,
            index=False,
        )
        
        # Update last timestamp cache
        if listing_year not in self._last_timestamps:
            self._last_timestamps[listing_year] = {}
        self._last_timestamps[listing_year][symbol] = int(df['timestamp'].max())
        
        logger.info(f"Appended {len(df)} records for {symbol} to {file_path.name}")
        return len(df)
    
    def write_batch_ohlcv(
        self,
        listing_year: int,
        data_batch: Dict[str, List[List]],
    ) -> int:
        """
        Write batch OHLCV data for multiple symbols to cohort file.
        
        Args:
            listing_year: Year when coins were listed
            data_batch: Dict mapping symbol to OHLCV data
            
        Returns:
            Total number of records written
        """
        if not data_batch:
            return 0
        
        file_path = self.get_file_path(listing_year)
        all_records = []
        
        for symbol, ohlcv_data in data_batch.items():
            for candle in ohlcv_data:
                record = {
                    'timestamp': candle[0],
                    'symbol': symbol,
                    'open': candle[1],
                    'high': candle[2],
                    'low': candle[3],
                    'close': candle[4],
                    'volume': candle[5],
                }
                all_records.append(record)
        
        if not all_records:
            return 0
        
        df = pd.DataFrame(all_records)
        df = df[CSV_COLUMNS]
        
        # Sort by timestamp then symbol for better organization
        df = df.sort_values(['timestamp', 'symbol'])
        
        file_exists = file_path.exists()
        
        df.to_csv(
            file_path,
            mode='a',
            header=not file_exists,
            index=False,
        )
        
        # Update cache
        if listing_year not in self._last_timestamps:
            self._last_timestamps[listing_year] = {}
        
        for symbol in data_batch.keys():
            symbol_df = df[df['symbol'] == symbol]
            if not symbol_df.empty:
                self._last_timestamps[listing_year][symbol] = int(symbol_df['timestamp'].max())
        
        logger.info(f"Batch wrote {len(df)} records to {file_path.name}")
        return len(df)
    
    def get_cohort_summary(self) -> Dict[int, Dict]:
        """
        Get summary of all cohort files in data directory.
        
        Returns:
            Dict mapping listing_year to file info
        """
        summary = {}
        
        for file_path in self.data_dir.glob(f"{FILE_PREFIX}_*{FILE_EXTENSION}"):
            match = self.file_pattern.search(file_path.name)
            if match:
                listing_year = int(match.group(1))
                to_year = int(match.group(2))
                
                try:
                    df = pd.read_csv(file_path)
                    summary[listing_year] = {
                        'file_name': file_path.name,
                        'to_year': to_year,
                        'total_records': len(df),
                        'symbols': df['symbol'].nunique() if 'symbol' in df.columns else 0,
                        'date_range': (
                            datetime.fromtimestamp(df['timestamp'].min() / 1000).strftime('%Y-%m-%d')
                            if 'timestamp' in df.columns else None,
                            datetime.fromtimestamp(df['timestamp'].max() / 1000).strftime('%Y-%m-%d')
                            if 'timestamp' in df.columns else None,
                        ),
                        'file_size_mb': file_path.stat().st_size / (1024 * 1024),
                    }
                except Exception as e:
                    logger.warning(f"Error reading {file_path.name}: {e}")
                    summary[listing_year] = {
                        'file_name': file_path.name,
                        'error': str(e),
                    }
        
        return summary
