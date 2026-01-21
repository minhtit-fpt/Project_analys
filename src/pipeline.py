"""
ETL Pipeline Module.

Main orchestration for the Future Market Data ETL:
1. Scan markets and filter coins
2. Classify by listing year
3. Manage file rotation
4. Fetch and append OHLCV data
"""

import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime

from config.settings import (
    TIMEFRAME,
    CURRENT_YEAR,
    RATE_LIMIT_DELAY,
)
from .exchange import BinanceFutureExchange
from .scanner import FutureMarketScanner, ScanResult, CoinInfo
from .file_manager import CohortFileManager

logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Main ETL Pipeline for Binance Future Market Data.
    
    Pipeline stages:
    1. Initialize exchange connection
    2. Scan and filter markets
    3. Classify coins by listing year
    4. Prepare cohort files (rotate if needed)
    5. Fetch incremental OHLCV data
    6. Append to CSV files
    """
    
    def __init__(self):
        """Initialize the ETL pipeline components."""
        self.exchange = BinanceFutureExchange()
        self.scanner = FutureMarketScanner(self.exchange)
        self.file_manager = CohortFileManager()
        
        self._scan_result: Optional[ScanResult] = None
        self._stats = {
            'total_coins': 0,
            'total_records': 0,
            'cohorts_processed': 0,
            'errors': [],
        }
    
    async def run(self) -> Dict:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            Dict containing pipeline execution statistics
        """
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info(f"Starting ETL Pipeline at {start_time}")
        logger.info("=" * 60)
        
        try:
            async with self.exchange:
                # Stage 1: Scan markets
                logger.info("\n[Stage 1] Scanning and filtering markets...")
                self._scan_result = await self.scanner.scan_markets()
                self._stats['total_coins'] = self._scan_result.total_filtered
                
                # Stage 2: Process each cohort
                logger.info("\n[Stage 2] Processing cohorts...")
                await self._process_cohorts()
                
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            self._stats['errors'].append(str(e))
            raise
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 60)
        logger.info("ETL Pipeline Completed")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Total coins processed: {self._stats['total_coins']}")
        logger.info(f"Total records written: {self._stats['total_records']}")
        logger.info(f"Cohorts processed: {self._stats['cohorts_processed']}")
        logger.info("=" * 60)
        
        return self._stats
    
    async def _process_cohorts(self) -> None:
        """Process each listing year cohort."""
        if not self._scan_result:
            return
        
        for listing_year, coins in sorted(self._scan_result.coins_by_year.items()):
            logger.info(f"\n--- Processing cohort: {listing_year} ({len(coins)} coins) ---")
            
            try:
                await self._process_single_cohort(listing_year, coins)
                self._stats['cohorts_processed'] += 1
            except Exception as e:
                logger.error(f"Error processing cohort {listing_year}: {e}")
                self._stats['errors'].append(f"Cohort {listing_year}: {e}")
    
    async def _process_single_cohort(
        self,
        listing_year: int,
        coins: List[CoinInfo],
    ) -> None:
        """
        Process a single listing year cohort.
        
        Args:
            listing_year: The cohort's listing year
            coins: List of CoinInfo in this cohort
        """
        # Step 1: Prepare file (rotate if needed)
        file_path, last_timestamps = self.file_manager.prepare_cohort_file(listing_year)
        logger.info(f"File prepared: {file_path.name}")
        logger.info(f"Existing symbols with data: {len(last_timestamps)}")
        
        # Step 2: Fetch OHLCV for each coin
        data_batch: Dict[str, List[List]] = {}
        
        for coin in coins:
            ohlcv = await self._fetch_coin_ohlcv(coin, last_timestamps)
            if ohlcv:
                data_batch[coin.symbol] = ohlcv
            
            await asyncio.sleep(RATE_LIMIT_DELAY)
        
        # Step 3: Write batch to file
        if data_batch:
            records_written = self.file_manager.write_batch_ohlcv(listing_year, data_batch)
            self._stats['total_records'] += records_written
    
    async def _fetch_coin_ohlcv(
        self,
        coin: CoinInfo,
        last_timestamps: Dict[str, int],
    ) -> List[List]:
        """
        Fetch OHLCV data for a single coin.
        
        Determines starting point based on:
        - If symbol exists in file: fetch from last_timestamp + 1
        - If new symbol: fetch from first_timestamp (full history)
        
        Args:
            coin: CoinInfo object
            last_timestamps: Dict of symbol -> last_timestamp
            
        Returns:
            List of OHLCV data
        """
        symbol = coin.symbol
        
        if symbol in last_timestamps:
            # Incremental fetch - start from last timestamp + 1 day
            since_ms = last_timestamps[symbol] + (24 * 60 * 60 * 1000)  # +1 day in ms
            logger.info(f"Incremental fetch for {symbol} from {datetime.fromtimestamp(since_ms/1000)}")
        else:
            # Full fetch - start from listing date
            since_ms = coin.first_timestamp
            logger.info(f"Full fetch for {symbol} from {datetime.fromtimestamp(since_ms/1000)}")
        
        try:
            ohlcv = await self.exchange.fetch_ohlcv_since(
                symbol=symbol,
                timeframe=TIMEFRAME,
                since_timestamp=since_ms,
            )
            return ohlcv
        except Exception as e:
            logger.error(f"Error fetching OHLCV for {symbol}: {e}")
            return []
    
    async def run_scan_only(self) -> ScanResult:
        """
        Run only the scanning stage (no data fetch).
        
        Useful for:
        - Testing filters
        - Previewing which coins will be processed
        
        Returns:
            ScanResult with filtered coins
        """
        logger.info("Running scan-only mode...")
        
        async with self.exchange:
            self._scan_result = await self.scanner.scan_markets()
        
        return self._scan_result
    
    async def run_for_cohort(self, listing_year: int) -> Dict:
        """
        Run pipeline for a specific cohort only.
        
        Args:
            listing_year: Target listing year
            
        Returns:
            Execution statistics
        """
        logger.info(f"Running pipeline for cohort {listing_year} only...")
        
        async with self.exchange:
            self._scan_result = await self.scanner.scan_markets()
            
            if listing_year in self._scan_result.coins_by_year:
                coins = self._scan_result.coins_by_year[listing_year]
                await self._process_single_cohort(listing_year, coins)
            else:
                logger.warning(f"No coins found for listing year {listing_year}")
        
        return self._stats
    
    def get_summary(self) -> Dict:
        """
        Get current pipeline summary.
        
        Returns:
            Dict with summary information
        """
        file_summary = self.file_manager.get_cohort_summary()
        
        return {
            'current_year': CURRENT_YEAR,
            'scan_result': {
                'total_scanned': self._scan_result.total_scanned if self._scan_result else 0,
                'total_filtered': self._scan_result.total_filtered if self._scan_result else 0,
                'cohorts': len(self._scan_result.coins_by_year) if self._scan_result else 0,
            },
            'files': file_summary,
            'stats': self._stats,
        }


async def main():
    """Main entry point for running the ETL pipeline."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    
    pipeline = ETLPipeline()
    
    try:
        stats = await pipeline.run()
        print("\n✅ Pipeline completed successfully!")
        print(f"   Total records: {stats['total_records']}")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
