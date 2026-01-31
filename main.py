"""
Binance Futures Historical Data Fetcher
Author: Senior Python Backend Engineer
Date: 2026-01-23
Description: Production-ready OOP script to fetch historical OHLCV data from Binance Futures (USDT-M)

Architecture:
    - Main: Entry point and orchestrator
    - GetData: Data retrieval from Binance
    - SaveData: Data saving and file management
    - GoogleDriveAPI: Google Drive API interactions
    - GUI: Optional graphical interface using CustomTkinter

Usage:
    - GUI Mode (default): python main.py
    - CLI Mode: python main.py --cli
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from dotenv import load_dotenv

from src.google_drive_api import GoogleDriveAPI
from src.get_data import GetData
from src.save_data import SaveData

# Load environment variables from .env file
load_dotenv()


class Main:
    """
    Main orchestrator class for the Binance Futures Data Fetcher.
    
    Acts as the central controller that initializes and coordinates
    all other components without containing business logic itself.
    
    Components:
        - GoogleDriveAPI: Handles authentication and file operations
        - GetData: Handles data retrieval from Binance
        - SaveData: Handles data saving to Google Drive
    """
    
    def __init__(self, price_threshold: float = 10.0, timeframe: str = '1d'):
        """
        Initialize the main orchestrator.
        
        Args:
            price_threshold: Maximum price in USDT to filter coins (default: 10.0)
            timeframe: Candle timeframe (default: '1d' for daily)
        """
        self.price_threshold = price_threshold
        self.timeframe = timeframe
        
        # Setup logging first
        self._setup_logging()
        
        self.logger.info("=" * 80)
        self.logger.info("Initializing Binance Futures Historical Data Fetcher")
        self.logger.info("=" * 80)
        
        # Initialize components
        self._initialize_components()
    
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
    
    def _initialize_components(self):
        """Initialize all child components."""
        self.logger.info("Initializing components...")
        
        # Initialize Google Drive API handler
        self.logger.info("  → Initializing Google Drive API...")
        self.google_drive_api = GoogleDriveAPI(logger=self.logger)
        
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
            google_drive_api=self.google_drive_api,
            timeframe=self.timeframe,
            logger=self.logger
        )
        
        self.logger.info("All components initialized successfully")
    
    def run(self):
        """
        Execute the main data pipeline.
        
        Steps:
            1. Detect year range with available data
            2. For each year: fetch data → immediately save to Google Drive
        """
        self.logger.info("=" * 80)
        self.logger.info("Starting Binance Futures Historical Data Fetcher")
        self.logger.info("=" * 80)
        
        try:
            # Step 1: Get the range of years to process
            self.logger.info("Step 1: Detecting year range...")
            min_year, max_year = self.data_fetcher.get_year_range()
            
            if min_year is None or max_year is None:
                self.logger.error("Could not determine year range. Exiting...")
                return
            
            self.logger.info(f"Will process years from {min_year} to {max_year}")
            
            # Step 2: Process each year sequentially (fetch → save → next year)
            total_years = max_year - min_year + 1
            self.logger.info(f"\nProcessing {total_years} year(s) sequentially...\n")
            
            for year in range(min_year, max_year + 1):
                self.logger.info("#" * 80)
                self.logger.info(f"Processing Year {year} ({year - min_year + 1}/{total_years})")
                self.logger.info("#" * 80)
                
                # Fetch data for this specific year
                year_data = self.data_fetcher.fetch_data_for_year(year)
                
                # Immediately save to Google Drive if data was fetched
                if year_data and year in year_data and year_data[year]:
                    self.logger.info(f"Saving data for year {year} to Google Drive...")
                    self.data_saver.save_single_year(year, year_data[year])
                else:
                    self.logger.info(f"No data to save for year {year}. Moving to next year...")
                
                self.logger.info(f"\nCompleted processing year {year}\n")
            
            self.logger.info("=" * 80)
            self.logger.info("Pipeline completed successfully!")
            self.logger.info("All years have been processed and uploaded to Google Drive.")
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
        app = Main(price_threshold=10.0, timeframe='1d')
        app.run()
        
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Exiting gracefully...")
    except Exception as e:
        logging.error(f"Critical error in main execution: {e}")
        raise


def run_gui():
    """Run the application in GUI mode."""
    try:
        from src.gui import BinanceFetcherGUI
        
        app = BinanceFetcherGUI()
        app.protocol("WM_DELETE_WINDOW", app.on_closing)
        app.mainloop()
        
    except ImportError as e:
        print(f"Error: Could not import GUI components. Make sure customtkinter is installed.")
        print(f"Install with: pip install customtkinter")
        print(f"Details: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Critical error in GUI execution: {e}")
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
