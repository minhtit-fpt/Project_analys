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
            1. Fetch data from Binance (via GetData)
            2. Save data to Google Drive (via SaveData)
        """
        self.logger.info("=" * 80)
        self.logger.info("Starting Binance Futures Historical Data Fetcher")
        self.logger.info("=" * 80)
        
        try:
            # Step 1: Fetch all data from Binance
            self.logger.info("Step 1: Fetching data from Binance...")
            data_store = self.data_fetcher.fetch_all_data()
            
            if not data_store:
                self.logger.error("No data fetched. Exiting...")
                return
            
            # Step 2: Save data to Google Drive
            self.logger.info("Step 2: Saving data to Google Drive...")
            self.data_saver.save_by_year(data_store)
            
            self.logger.info("=" * 80)
            self.logger.info("Pipeline completed successfully!")
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
