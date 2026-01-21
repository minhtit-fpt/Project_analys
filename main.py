"""
Binance Future Market Data ETL - Main Entry Point

This script orchestrates the ETL pipeline for fetching USDT-Margined Futures
market data from Binance, organized by listing year cohorts.

Usage:
    python main.py                  # Run full ETL pipeline
    python main.py --scan-only      # Only scan and classify coins
    python main.py --cohort 2020    # Process specific cohort only
    python main.py --summary        # Show current data summary
"""

import sys
import asyncio
import argparse
import logging
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(__file__).rsplit('\\', 1)[0])
sys.path.insert(0, str(__file__).rsplit('/', 1)[0])

from config.settings import (
    EXCHANGE_ID,
    MARKET_TYPE,
    TARGET_QUOTE,
    MAX_PRICE,
    CURRENT_YEAR,
    DATA_DIR,
    LOG_DIR,
    LOG_LEVEL,
    LOG_FORMAT,
)
from src.pipeline import ETLPipeline


def setup_logging(verbose: bool = False) -> None:
    """
    Configure logging for the application.
    
    Args:
        verbose: Enable debug logging if True
    """
    level = logging.DEBUG if verbose else getattr(logging, LOG_LEVEL)
    
    # Create log file with timestamp
    log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ]
    )
    
    # Reduce noise from external libraries
    logging.getLogger('ccxt').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)


def print_banner() -> None:
    """Print application banner."""
    banner = f"""
╔══════════════════════════════════════════════════════════════╗
║         BINANCE FUTURE MARKET DATA ETL SYSTEM                ║
╠══════════════════════════════════════════════════════════════╣
║  Exchange:     {EXCHANGE_ID.upper():<45} ║
║  Market Type:  {MARKET_TYPE.upper():<45} ║
║  Quote Filter: {TARGET_QUOTE:<45} ║
║  Max Price:    ${MAX_PRICE:<44} ║
║  Current Year: {CURRENT_YEAR:<45} ║
║  Data Dir:     {str(DATA_DIR):<45} ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Binance Future Market Data ETL System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                    Run full ETL pipeline
  python main.py --scan-only        Only scan markets (no data fetch)
  python main.py --cohort 2020      Process only 2020 cohort
  python main.py --summary          Show data summary
  python main.py -v                 Verbose logging
        """
    )
    
    parser.add_argument(
        '--scan-only',
        action='store_true',
        help='Only scan and classify coins without fetching data'
    )
    
    parser.add_argument(
        '--cohort',
        type=int,
        metavar='YEAR',
        help='Process only a specific listing year cohort'
    )
    
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show summary of existing data files'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose (debug) logging'
    )
    
    return parser.parse_args()


async def run_full_pipeline() -> None:
    """Run the complete ETL pipeline."""
    pipeline = ETLPipeline()
    stats = await pipeline.run()
    
    print("\n" + "=" * 60)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("=" * 60)
    print(f"  Total coins processed: {stats['total_coins']}")
    print(f"  Total records written: {stats['total_records']}")
    print(f"  Cohorts processed:     {stats['cohorts_processed']}")
    if stats['errors']:
        print(f"  Errors encountered:    {len(stats['errors'])}")
        for err in stats['errors']:
            print(f"    - {err}")
    print("=" * 60)


async def run_scan_only() -> None:
    """Run scan-only mode."""
    pipeline = ETLPipeline()
    result = await pipeline.run_scan_only()
    
    print("\n" + "=" * 60)
    print("📋 SCAN RESULTS")
    print("=" * 60)
    print(f"  Total markets scanned: {result.total_scanned}")
    print(f"  Markets after filter:  {result.total_filtered}")
    print("\n  Coins by Listing Year:")
    print("  " + "-" * 40)
    
    for year, coins in sorted(result.coins_by_year.items()):
        print(f"\n  📅 {year} ({len(coins)} coins):")
        for coin in coins[:5]:  # Show first 5
            print(f"      • {coin.symbol:<15} ${coin.current_price:.4f}")
        if len(coins) > 5:
            print(f"      ... and {len(coins) - 5} more")
    
    print("\n" + "=" * 60)


async def run_cohort(year: int) -> None:
    """Run pipeline for a specific cohort."""
    pipeline = ETLPipeline()
    stats = await pipeline.run_for_cohort(year)
    
    print(f"\n✅ Cohort {year} processing completed")
    print(f"   Records written: {stats['total_records']}")


def show_summary() -> None:
    """Show summary of existing data."""
    from src.file_manager import CohortFileManager
    
    manager = CohortFileManager()
    summary = manager.get_cohort_summary()
    
    print("\n" + "=" * 60)
    print("📁 DATA FILES SUMMARY")
    print("=" * 60)
    
    if not summary:
        print("  No data files found.")
        print(f"  Data directory: {DATA_DIR}")
    else:
        total_records = 0
        total_size = 0
        
        for year, info in sorted(summary.items()):
            if 'error' in info:
                print(f"\n  ⚠️  {info['file_name']}: Error - {info['error']}")
                continue
            
            print(f"\n  📄 {info['file_name']}")
            print(f"      Records:    {info['total_records']:,}")
            print(f"      Symbols:    {info['symbols']}")
            print(f"      Date Range: {info['date_range'][0]} to {info['date_range'][1]}")
            print(f"      File Size:  {info['file_size_mb']:.2f} MB")
            
            total_records += info['total_records']
            total_size += info['file_size_mb']
        
        print("\n  " + "-" * 40)
        print(f"  Total Records: {total_records:,}")
        print(f"  Total Size:    {total_size:.2f} MB")
    
    print("\n" + "=" * 60)


def main() -> None:
    """Main entry point."""
    args = parse_arguments()
    
    # Setup logging
    setup_logging(verbose=args.verbose)
    logger = logging.getLogger(__name__)
    
    # Print banner
    print_banner()
    
    try:
        if args.summary:
            show_summary()
        elif args.scan_only:
            asyncio.run(run_scan_only())
        elif args.cohort:
            asyncio.run(run_cohort(args.cohort))
        else:
            asyncio.run(run_full_pipeline())
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception("Pipeline failed with error")
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
