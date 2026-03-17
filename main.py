"""
Binance Futures Historical Data Fetcher
Author: Senior Python Backend Engineer
Date: 2026-01-23
Description: Production-ready OOP script to fetch historical OHLCV data from Binance Futures (USDT-M)

Architecture:
    - Main: Entry point and orchestrator
    - GetData: Data retrieval from Binance
    - SaveData: Data saving and file management
    - GoogleCloudStorageAPI: Google Cloud Storage interactions
    - GUI: Optional graphical interface using CustomTkinter

Usage:
    - GUI Mode (default): python main.py
    - CLI Mode: python main.py --cli
"""

import sys
import argparse
from datetime import datetime
from dotenv import load_dotenv

from src.core.config import settings
from src.core.logger import get_logger
from src.LOGIC.google_cloud_storage_api import GoogleCloudStorageAPI
from src.LOGIC.get_data import GetData
from src.LOGIC.save_data import SaveData

# Load environment variables from .env file
load_dotenv()


class Main:
    """
    Main orchestrator class for the Binance Futures Data Fetcher.
    
    Acts as the central controller that initializes and coordinates
    all other components without containing business logic itself.
    
    Components:
        - GoogleCloudStorageAPI: Handles authentication and file operations
        - GetData: Handles data retrieval from Binance
        - SaveData: Handles data saving to Google Cloud Storage
    """
    
    def __init__(self, price_threshold: float = settings.price_threshold,
                 timeframe: str = settings.timeframe):
        """
        Initialize the main orchestrator.
        
        Args:
            price_threshold: Maximum price in USDT to filter coins
            timeframe: Candle timeframe (default from settings)
        """
        self.price_threshold = price_threshold
        self.timeframe = timeframe
        self.logger = get_logger(__name__)
        
        self.logger.info("=" * 80)
        self.logger.info("Initializing Binance Futures Historical Data Fetcher")
        self.logger.info("=" * 80)
        
        # Initialize components
        self._initialize_components()
    
    def _initialize_components(self):
        """Initialize all child components."""
        self.logger.info("Initializing components...")
        
        # Initialize Google Cloud Storage API handler
        self.logger.info("  → Initializing Google Cloud Storage API...")
        self.storage_api = GoogleCloudStorageAPI(logger=self.logger)
        
        # Initialize data fetcher
        self.logger.info("  → Initializing Data Fetcher...")
        self.data_fetcher = GetData(
            price_threshold=self.price_threshold,
            timeframe=self.timeframe,
            logger=self.logger
        )
        
        # Initialize data saver
        self.logger.info("  → Initializing Data Saver...")
        self.data_saver = SaveData(
            storage_api=self.storage_api,
            timeframe=self.timeframe,
            logger=self.logger
        )
        
        self.logger.info("All components initialized successfully")
    
    def run(self):
        """
        Execute the main data pipeline.
        
        Steps:
            1. Scan all symbols once and group by year
            2. For each year: fetch data → immediately save to Google Drive
        """
        self.logger.info("=" * 80)
        self.logger.info("Starting Binance Futures Historical Data Fetcher")
        self.logger.info("=" * 80)
        
        try:
            # Step 1: Scan all symbols once and group by year
            self.logger.info("Step 1: Scanning symbols and grouping by year...")
            symbols_by_year = self.data_fetcher.scan_and_group_symbols_by_year()
            
            if not symbols_by_year:
                self.logger.error("No symbols found or could not group by year. Exiting...")
                return
            
            min_year = min(symbols_by_year.keys())
            max_year = max(symbols_by_year.keys())
            
            self.logger.info(f"Will process years from {min_year} to {max_year}")
            
            # Step 2: Process each year sequentially (fetch → save → next year)
            total_years = max_year - min_year + 1
            total_coins_processed = 0
            
            self.logger.info(f"\nProcessing {total_years} year(s) sequentially...\n")
            
            for year in range(min_year, max_year + 1):
                self.logger.info("#" * 80)
                self.logger.info(f"Processing Year {year} ({year - min_year + 1}/{total_years})")
                self.logger.info("#" * 80)
                
                # Get symbols for this year (already grouped, no re-scan needed)
                year_symbols = symbols_by_year.get(year, [])
                
                if not year_symbols:
                    self.logger.info(f"No coins listed in year {year}. Moving to next year...")
                    continue
                
                # Fetch data for this year's symbols
                self.logger.info(f"Fetching data for {len(year_symbols)} coins...")
                year_data = self.data_fetcher.fetch_data_for_symbols(year_symbols)
                
                # Immediately save to Google Drive if data was fetched
                if year_data:
                    total_coins_processed += len(year_data)
                    self.logger.info(f"Saving data for year {year} to Google Cloud Storage...")
                    self.data_saver.save_single_year(year, year_data)
                else:
                    self.logger.info(f"No data fetched for year {year}. Moving to next year...")
                
                self.logger.info(f"\nCompleted processing year {year}\n")
            
            self.logger.info("=" * 80)
            self.logger.info("Pipeline completed successfully!")
            self.logger.info(f"Processed {total_coins_processed} coins across {len(symbols_by_year)} years.")
            self.logger.info("All years have been processed and uploaded to Google Cloud Storage.")
            self.logger.info("=" * 80)
            
        except Exception as e:
            self.logger.error(f"Error in pipeline execution: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise


def run_cli():
    """Run the application in CLI mode."""
    try:
        # Initialize and run the main orchestrator
        app = Main()
        app.run()
        
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Exiting gracefully...")
    except Exception as e:
        get_logger(__name__).error(f"Critical error in main execution: {e}")
        raise


def run_gui():
    """Run the application in GUI mode."""
    try:
        from src.GUI.main_window import BinanceFetcherGUI
        
        app = BinanceFetcherGUI()
        app.protocol("WM_DELETE_WINDOW", app.on_closing)
        app.mainloop()
        
    except ImportError as e:
        print(f"Error: Could not import GUI components. Make sure customtkinter is installed.")
        print(f"Install with: pip install customtkinter")
        print(f"Details: {e}")
        sys.exit(1)
    except Exception as e:
        get_logger(__name__).error(f"Critical error in GUI execution: {e}")
        raise


def main():
    """
    Entry point for the application.
    Supports both GUI and CLI modes.
    """
    parser = argparse.ArgumentParser(
        description="Binance Futures Historical Data Fetcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py          # Run with GUI (default)
  python main.py --gui    # Run with GUI
  python main.py --cli    # Run in command-line mode
        """
    )
    
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '--gui',
        action='store_true',
        default=True,
        help='Run with graphical user interface (default)'
    )
    mode_group.add_argument(
        '--cli',
        action='store_true',
        help='Run in command-line mode without GUI'
    )
    
    args = parser.parse_args()
    
    if args.cli:
        print("Running in CLI mode...")
        run_cli()
    else:
        print("Starting GUI...")
        run_gui()


if __name__ == "__main__":
    main()
