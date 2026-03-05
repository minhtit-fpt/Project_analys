"""
Chart Generator
Retrieves data files from Google Drive, filters by user criteria,
and produces filtered datasets for chart display.
"""

import os
import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd
from dateutil.relativedelta import relativedelta

from src.LOGIC.google_cloud_storage_api import GoogleCloudStorageAPI
from src.GUI.progress_reporter import ProgressReporter, ExecutionStage


class ChartGenerator:
    """
    Generates filtered datasets from pre-existing Google Drive parquet files
    for chart display.

    Responsibilities:
    - Retrieve parquet data files from Google Drive by timeframe and year
    - Filter data by date range and coin scope
    - Return the filtered dataset for chart rendering
    """

    def __init__(self, storage_api: GoogleCloudStorageAPI,
                 progress_reporter: Optional[ProgressReporter] = None,
                 logger: logging.Logger = None):
        """
        Initialize the chart generator.

        Args:
            storage_api: Authenticated GoogleCloudStorageAPI instance
            progress_reporter: Optional progress reporter for UI feedback
            logger: Optional logger instance
        """
        self.storage = storage_api
        self.progress_reporter = progress_reporter
        self.logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_chart_data(
        self,
        timeframe: str,
        time_range: str,
        reference_year: str,
        coin_scope: str,
        single_coin: Optional[str] = None,
        is_running_check=None,
    ) -> Optional[pd.DataFrame]:
        """
        Main entry point – retrieve, filter, and return data for chart display.

        Args:
            timeframe: Candle timeframe (e.g. '1d', '1h')
            time_range: One of 'from_beginning', 'last_6m', 'last_1y',
                        'last_2y', 'last_5y'
            reference_year: End-boundary year as string (e.g. '2025')
            coin_scope: 'all' or 'single'
            single_coin: Coin symbol when coin_scope == 'single'
            is_running_check: Optional callable returning bool
                              (False → user cancelled)

        Returns:
            filtered_df on success, None on failure.
            filtered_df – full OHLCV rows that passed all filters
        """
        ref_year = int(reference_year)

        # 1. Compute date boundaries
        end_date = datetime(ref_year, 12, 31, 23, 59, 59)
        start_date = self._compute_start_date(time_range, ref_year, end_date)

        # 2. Which calendar years do we need files for?
        years_needed = list(range(start_date.year, end_date.year + 1))

        self.logger.info(f"Date range: {start_date} → {end_date}")
        self.logger.info(f"Years to retrieve: {years_needed}")

        # 3. Retrieve parquet files from Google Drive
        self._report(ExecutionStage.FETCHING_MARKETS, 0.0,
                     f"Searching GCS for {timeframe} data files "
                     f"({len(years_needed)} year(s))...")

        combined_df = self._retrieve_and_load(timeframe, years_needed,
                                              is_running_check)

        if combined_df is None or combined_df.empty:
            self.logger.warning("No data retrieved from GCS.")
            return None

        if is_running_check and not is_running_check():
            return None

        # 4. Filter by date range
        self._report(ExecutionStage.PROCESSING_SYMBOLS, 0.0,
                     "Filtering data by date range...")

        filtered_df = self._filter_by_date(combined_df, start_date, end_date)

        if filtered_df.empty:
            self.logger.warning("No data remaining after date filtering.")
            return None

        # 5. Filter by coin scope
        if coin_scope == "single" and single_coin:
            self._report(ExecutionStage.PROCESSING_SYMBOLS, 0.4,
                         f"Filtering for coin: {single_coin}...")
            filtered_df = self._filter_by_coin(filtered_df, single_coin)
            if filtered_df.empty:
                self.logger.warning(f"No data found for {single_coin}.")
                return None

        self._report(ExecutionStage.PROCESSING_SYMBOLS, 0.7,
                     f"Filtered dataset: {len(filtered_df)} rows, "
                     f"{filtered_df['symbol'].nunique()} coin(s)")

        if is_running_check and not is_running_check():
            return None

        self._report(ExecutionStage.SAVING_DATA, 1.0,
                     f"Data ready — {filtered_df['symbol'].nunique()} coin(s), "
                     f"{len(filtered_df)} rows")

        return filtered_df

    # ------------------------------------------------------------------
    # Date helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_start_date(time_range: str, ref_year: int,
                            end_date: datetime) -> datetime:
        """Compute the start date relative to the selected year.

        'from_beginning' returns Jan 1 of *ref_year* (selected year only).
        Other ranges go backwards from end_date and may span prior years.
        If the computed start predates any available data the retrieval
        layer will simply skip missing years — no error is raised.
        """
        if time_range == "last_6m":
            return end_date - relativedelta(months=6)
        elif time_range == "last_1y":
            return end_date - relativedelta(years=1)
        elif time_range == "last_2y":
            return end_date - relativedelta(years=2)
        elif time_range == "last_5y":
            return end_date - relativedelta(years=5)

        # 'from_beginning' → only the selected year
        return datetime(ref_year, 1, 1)

    # ------------------------------------------------------------------
    # Drive retrieval
    # ------------------------------------------------------------------

    def _retrieve_and_load(self, timeframe: str, years: List[int],
                           is_running_check=None) -> Optional[pd.DataFrame]:
        """
        Download relevant parquet files from GCS and load them
        into a single combined DataFrame.

        File naming convention (not modified):
            Binance_timeframe:{tf}_{year}_to_{date}.parquet
        """
        all_dfs: List[pd.DataFrame] = []
        total = len(years)

        for idx, year in enumerate(years):
            if is_running_check and not is_running_check():
                return None

            # Build the GCS search pattern matching the naming convention
            pattern = f"Binance_timeframe:{timeframe}_{year}_to_"
            self.logger.info(f"Searching GCS for pattern: {pattern}")

            files = self.storage.list_files(pattern)

            if not files:
                self.logger.info(f"  No file found for year {year} "
                                 f"(timeframe {timeframe})")
                self._report(
                    ExecutionStage.FETCHING_MARKETS,
                    (idx + 1) / total,
                    f"No file found for year {year} — skipping"
                )
                continue

            # Pick the most recent file (highest _to_ date in its name)
            target_file = sorted(files, key=lambda f: f['name'],
                                 reverse=True)[0]

            self.logger.info(f"  Downloading: {target_file['name']}")
            self._report(
                ExecutionStage.FETCHING_MARKETS,
                (idx + 0.5) / total,
                f"Downloading {target_file['name']}..."
            )

            local_path = self.storage.download_file(target_file['id'])

            if local_path is None:
                self.logger.error(f"  Failed to download {target_file['name']}")
                continue

            try:
                df = pd.read_parquet(local_path)
                all_dfs.append(df)
                self.logger.info(f"  Loaded {len(df)} rows from "
                                 f"{target_file['name']}")
                self._report(
                    ExecutionStage.FETCHING_MARKETS,
                    (idx + 1) / total,
                    f"Loaded {target_file['name']} — {len(df)} rows"
                )
            except Exception as e:
                self.logger.error(f"  Error reading parquet "
                                  f"{target_file['name']}: {e}")
            finally:
                # Clean up temp file
                try:
                    os.remove(local_path)
                except OSError:
                    pass

        if not all_dfs:
            return None

        combined = pd.concat(all_dfs, ignore_index=True)
        self.logger.info(f"Combined dataset: {len(combined)} rows")
        return combined

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    @staticmethod
    def _filter_by_date(df: pd.DataFrame, start_date: datetime,
                        end_date: datetime) -> pd.DataFrame:
        """Keep only rows whose date falls within [start_date, end_date]."""
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        mask = (df['date'] >= start_ts) & (df['date'] <= end_ts)
        return df.loc[mask].copy()

    @staticmethod
    def _filter_by_coin(df: pd.DataFrame,
                        coin_symbol: str) -> pd.DataFrame:
        """
        Keep only rows matching *coin_symbol*.
        Handles both 'BTC/USDT' and 'BTCUSDT' formats transparently.
        """
        upper = coin_symbol.upper()
        mask = df['symbol'].str.upper() == upper
        if mask.sum() == 0:
            # Try without the slash
            clean = upper.replace('/', '')
            mask = (df['symbol']
                    .str.replace('/', '', regex=False)
                    .str.upper() == clean)
        return df.loc[mask].copy()

    # ------------------------------------------------------------------
    # Progress helper
    # ------------------------------------------------------------------

    def _report(self, stage: ExecutionStage, stage_progress: float,
                message: str, **kwargs):
        """Report progress if a reporter is available."""
        if self.progress_reporter:
            self.progress_reporter.report(stage, stage_progress, message,
                                          **kwargs)
