"""
GUI Application using CustomTkinter
Provides visual feedback and control for the Binance Futures Data Fetcher.
"""

import customtkinter as ctk
from datetime import datetime, timedelta
from typing import Optional
import threading
from tkinter import messagebox

from src.progress_reporter import ProgressReporter, ProgressInfo, ExecutionStage


class BinanceFetcherGUI(ctk.CTk):
    """
    Main GUI window for the Binance Futures Data Fetcher.
    
    Displays:
    - Start button
    - Progress bar with percentage
    - Status messages
    - Time range selection
    - Completion notifications
    """
    
    def __init__(self):
        super().__init__()
        
        # Window configuration
        self.title("Binance Futures Data Fetcher")
        self.geometry("700x600")
        self.minsize(600, 550)
        
        # Set appearance
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        # Progress reporter for communication with core logic
        self.progress_reporter = ProgressReporter()
        self.progress_reporter.add_callback(self._on_progress_update)
        
        # State
        self._is_running = False
        self._worker_thread: Optional[threading.Thread] = None
        
        # Build UI
        self._create_widgets()
        
        # Center window
        self._center_window()
    
    def _center_window(self):
        """Center the window on screen."""
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")
    
    def _create_widgets(self):
        """Create all GUI widgets."""
        # Main container with padding
        self.main_frame = ctk.CTkFrame(self)
        self.main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Title
        self.title_label = ctk.CTkLabel(
            self.main_frame,
            text="📊 Binance Futures Data Fetcher",
            font=ctk.CTkFont(size=24, weight="bold")
        )
        self.title_label.pack(pady=(0, 20))
        
        # Configuration Frame
        self._create_config_frame()
        
        # Progress Frame
        self._create_progress_frame()
        
        # Status Frame
        self._create_status_frame()
        
        # Control Frame
        self._create_control_frame()
    
    def _create_config_frame(self):
        """Create the configuration section."""
        config_frame = ctk.CTkFrame(self.main_frame)
        config_frame.pack(fill="x", pady=(0, 15))
        
        # Title
        config_label = ctk.CTkLabel(
            config_frame,
            text="⚙️ Configuration",
            font=ctk.CTkFont(size=16, weight="bold")
        )
        config_label.pack(anchor="w", padx=15, pady=(10, 5))
        
        # Inner frame for inputs
        input_frame = ctk.CTkFrame(config_frame, fg_color="transparent")
        input_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        # Price threshold
        price_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        price_frame.pack(fill="x", pady=5)
        
        price_label = ctk.CTkLabel(price_frame, text="Price Threshold (USDT):", width=150, anchor="w")
        price_label.pack(side="left")
        
        self.price_threshold_var = ctk.StringVar(value="10.0")
        self.price_entry = ctk.CTkEntry(price_frame, textvariable=self.price_threshold_var, width=100)
        self.price_entry.pack(side="left", padx=(10, 0))
        
        # Timeframe selection
        timeframe_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        timeframe_frame.pack(fill="x", pady=5)
        
        timeframe_label = ctk.CTkLabel(timeframe_frame, text="Candle Timeframe:", width=150, anchor="w")
        timeframe_label.pack(side="left")
        
        self.timeframe_var = ctk.StringVar(value="1d")
        self.timeframe_combo = ctk.CTkComboBox(
            timeframe_frame,
            values=["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"],
            variable=self.timeframe_var,
            width=100
        )
        self.timeframe_combo.pack(side="left", padx=(10, 0))
        
        # Info label showing data range
        info_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        info_frame.pack(fill="x", pady=5)
        
        info_label = ctk.CTkLabel(
            info_frame,
            text=f"ℹ️ Will fetch all available historical data up to {datetime.now().strftime('%Y-%m-%d')}",
            text_color="gray",
            font=ctk.CTkFont(size=12)
        )
        info_label.pack(anchor="w")
    
    def _create_progress_frame(self):
        """Create the progress section."""
        progress_frame = ctk.CTkFrame(self.main_frame)
        progress_frame.pack(fill="x", pady=(0, 15))
        
        # Title
        progress_label = ctk.CTkLabel(
            progress_frame,
            text="📈 Progress",
            font=ctk.CTkFont(size=16, weight="bold")
        )
        progress_label.pack(anchor="w", padx=15, pady=(10, 5))
        
        # Overall progress
        overall_frame = ctk.CTkFrame(progress_frame, fg_color="transparent")
        overall_frame.pack(fill="x", padx=15, pady=5)
        
        overall_label = ctk.CTkLabel(overall_frame, text="Overall Progress:", width=120, anchor="w")
        overall_label.pack(side="left")
        
        self.overall_progress = ctk.CTkProgressBar(overall_frame, width=350)
        self.overall_progress.pack(side="left", padx=(10, 10))
        self.overall_progress.set(0)
        
        self.overall_percent_label = ctk.CTkLabel(overall_frame, text="0%", width=50)
        self.overall_percent_label.pack(side="left")
        
        # Stage progress
        stage_frame = ctk.CTkFrame(progress_frame, fg_color="transparent")
        stage_frame.pack(fill="x", padx=15, pady=5)
        
        stage_label = ctk.CTkLabel(stage_frame, text="Current Stage:", width=120, anchor="w")
        stage_label.pack(side="left")
        
        self.stage_progress = ctk.CTkProgressBar(stage_frame, width=350)
        self.stage_progress.pack(side="left", padx=(10, 10))
        self.stage_progress.set(0)
        
        self.stage_percent_label = ctk.CTkLabel(stage_frame, text="0%", width=50)
        self.stage_percent_label.pack(side="left")
        
        # Current stage name
        self.stage_name_label = ctk.CTkLabel(
            progress_frame,
            text="Stage: Waiting to start...",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        self.stage_name_label.pack(anchor="w", padx=15, pady=(5, 10))
    
    def _create_status_frame(self):
        """Create the status messages section."""
        status_frame = ctk.CTkFrame(self.main_frame)
        status_frame.pack(fill="both", expand=True, pady=(0, 15))
        
        # Title
        status_label = ctk.CTkLabel(
            status_frame,
            text="📋 Status Log",
            font=ctk.CTkFont(size=16, weight="bold")
        )
        status_label.pack(anchor="w", padx=15, pady=(10, 5))
        
        # Status text box
        self.status_text = ctk.CTkTextbox(status_frame, height=150)
        self.status_text.pack(fill="both", expand=True, padx=15, pady=(0, 10))
        self.status_text.configure(state="disabled")
        
        self._log_message("Ready to start. Configure settings and click 'Start Process'.")
    
    def _create_control_frame(self):
        """Create the control buttons section."""
        control_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        control_frame.pack(fill="x", pady=(0, 10))
        
        # Start button
        self.start_button = ctk.CTkButton(
            control_frame,
            text="▶️ Start Process",
            font=ctk.CTkFont(size=14, weight="bold"),
            height=40,
            width=200,
            command=self._on_start_click
        )
        self.start_button.pack(side="left", padx=(0, 10))
        
        # Stop button
        self.stop_button = ctk.CTkButton(
            control_frame,
            text="⏹️ Stop",
            font=ctk.CTkFont(size=14),
            height=40,
            width=100,
            fg_color="red",
            hover_color="darkred",
            command=self._on_stop_click,
            state="disabled"
        )
        self.stop_button.pack(side="left", padx=(0, 10))
        
        # Clear log button
        self.clear_button = ctk.CTkButton(
            control_frame,
            text="🗑️ Clear Log",
            font=ctk.CTkFont(size=14),
            height=40,
            width=100,
            fg_color="gray",
            hover_color="darkgray",
            command=self._on_clear_log
        )
        self.clear_button.pack(side="right")
    
    def _log_message(self, message: str, timestamp: bool = True):
        """Add a message to the status log."""
        self.status_text.configure(state="normal")
        if timestamp:
            time_str = datetime.now().strftime("%H:%M:%S")
            self.status_text.insert("end", f"[{time_str}] {message}\n")
        else:
            self.status_text.insert("end", f"{message}\n")
        self.status_text.see("end")
        self.status_text.configure(state="disabled")
    
    def _on_progress_update(self, info: ProgressInfo):
        """
        Handle progress updates from the core logic.
        This method is called from a worker thread, so we use after() for thread safety.
        """
        self.after(0, lambda: self._update_ui(info))
    
    def _update_ui(self, info: ProgressInfo):
        """Update UI elements with progress information (must be called from main thread)."""
        # Update progress bars
        self.overall_progress.set(info.overall_progress)
        self.overall_percent_label.configure(text=f"{int(info.overall_progress * 100)}%")
        
        self.stage_progress.set(info.stage_progress)
        self.stage_percent_label.configure(text=f"{int(info.stage_progress * 100)}%")
        
        # Update stage name
        self.stage_name_label.configure(text=f"Stage: {info.stage.value}")
        
        # Log message
        self._log_message(info.message)
        
        # Handle completion or error
        if info.stage == ExecutionStage.COMPLETED:
            self._on_process_complete(info.message)
        elif info.stage == ExecutionStage.ERROR:
            self._on_process_error(info.message)
    
    def _on_start_click(self):
        """Handle start button click."""
        if self._is_running:
            return
        
        # Validate inputs
        try:
            price_threshold = float(self.price_threshold_var.get())
            if price_threshold <= 0:
                raise ValueError("Price threshold must be positive")
        except ValueError as e:
            messagebox.showerror("Invalid Input", f"Invalid price threshold: {e}")
            return
        
        # Reset progress
        self.overall_progress.set(0)
        self.stage_progress.set(0)
        self.overall_percent_label.configure(text="0%")
        self.stage_percent_label.configure(text="0%")
        self.stage_name_label.configure(text="Stage: Starting...")
        
        # Update UI state
        self._is_running = True
        self.start_button.configure(state="disabled")
        self.stop_button.configure(state="normal")
        self.price_entry.configure(state="disabled")
        self.timeframe_combo.configure(state="disabled")
        
        self._log_message("=" * 50, timestamp=False)
        self._log_message("Starting Binance Futures Data Fetcher...")
        self._log_message(f"Price Threshold: {price_threshold} USDT")
        self._log_message(f"Timeframe: {self.timeframe_var.get()}")
        self._log_message("Fetching all available historical data...")
        
        # Start worker thread
        self._worker_thread = threading.Thread(
            target=self._run_process,
            args=(price_threshold, self.timeframe_var.get()),
            daemon=True
        )
        self._worker_thread.start()
    
    def _run_process(self, price_threshold: float, timeframe: str):
        """Run the data fetching process in a background thread."""
        try:
            # Import here to avoid circular imports
            from src.google_drive_api import GoogleDriveAPI
            from src.get_data import GetData
            from src.save_data import SaveData
            import logging
            
            # Setup a simple logger
            logger = logging.getLogger(__name__)
            
            # Report initialization
            self.progress_reporter.report(
                ExecutionStage.INITIALIZING,
                0.5,
                "Initializing components..."
            )
            
            # Initialize Google Drive API
            self.progress_reporter.report(
                ExecutionStage.AUTHENTICATING,
                0.0,
                "Authenticating with Google Drive..."
            )
            google_drive = GoogleDriveAPI(logger=logger)
            self.progress_reporter.report(
                ExecutionStage.AUTHENTICATING,
                1.0,
                "Google Drive authentication successful"
            )
            
            # Initialize data fetcher
            data_fetcher = GetData(
                price_threshold=price_threshold,
                timeframe=timeframe,
                logger=logger
            )
            
            # Initialize data saver with timeframe for filename
            data_saver = SaveData(
                google_drive_api=google_drive,
                timeframe=timeframe,
                logger=logger
            )
            
            # === STEP 1: SCAN ALL SYMBOLS ONCE AND GROUP BY YEAR ===
            self.progress_reporter.report(
                ExecutionStage.FETCHING_MARKETS,
                0.0,
                "Scanning all symbols and grouping by listing year (one-time scan)..."
            )
            
            # This scans all symbols ONCE and groups them by listing year
            symbols_by_year = data_fetcher.scan_and_group_symbols_by_year()
            
            if not symbols_by_year:
                self.progress_reporter.report_error("No symbols found or could not group by year")
                return
            
            # Get year range from the grouped data
            min_year = min(symbols_by_year.keys())
            max_year = max(symbols_by_year.keys())
            total_symbols = sum(len(syms) for syms in symbols_by_year.values())
            
            self.progress_reporter.report(
                ExecutionStage.FETCHING_MARKETS,
                1.0,
                f"Scan complete: {total_symbols} coins grouped into years {min_year}-{max_year}"
            )
            
            # === STEP 2: PROCESS YEAR BY YEAR (NO RE-SCANNING) ===
            total_years = max_year - min_year + 1
            total_coins_processed = 0
            
            for year_idx, year in enumerate(range(min_year, max_year + 1)):
                if not self._is_running:
                    self.progress_reporter.report_error("Process cancelled by user")
                    return
                
                # Get symbols for this specific year (already grouped, no re-scan needed)
                year_symbols = symbols_by_year.get(year, [])
                
                if not year_symbols:
                    self.progress_reporter.report(
                        ExecutionStage.PROCESSING_SYMBOLS,
                        (year_idx + 1) / total_years,
                        f"[Year {year}] No coins listed in this year, moving to next...",
                        total_items=total_years,
                        completed_items=year_idx + 1
                    )
                    continue
                
                # Update overall progress based on years
                overall_progress = year_idx / total_years
                
                # === FETCH DATA FOR THIS YEAR (using pre-scanned symbols) ===
                self.progress_reporter.report(
                    ExecutionStage.PROCESSING_SYMBOLS,
                    overall_progress,
                    f"[Year {year}] Fetching data for {len(year_symbols)} coins ({year_idx + 1}/{total_years})...",
                    total_items=total_years,
                    completed_items=year_idx
                )
                
                # Fetch data using the pre-scanned symbol list (no re-detection needed)
                year_data = data_fetcher.fetch_data_for_symbols(year_symbols)
                
                # === IMMEDIATELY SAVE THIS YEAR'S DATA ===
                if year_data:
                    coins_in_year = len(year_data)
                    total_coins_processed += coins_in_year
                    
                    self.progress_reporter.report(
                        ExecutionStage.UPLOADING,
                        (year_idx + 0.5) / total_years,
                        f"[Year {year}] Saving {coins_in_year} coins to Google Drive...",
                        total_items=total_years,
                        completed_items=year_idx
                    )
                    
                    # Save immediately after fetching this year's data
                    data_saver.save_single_year(year, year_data)
                    
                    self.progress_reporter.report(
                        ExecutionStage.UPLOADING,
                        (year_idx + 1) / total_years,
                        f"[Year {year}] ✓ Saved {coins_in_year} coins to Google Drive",
                        total_items=total_years,
                        completed_items=year_idx + 1
                    )
                else:
                    self.progress_reporter.report(
                        ExecutionStage.PROCESSING_SYMBOLS,
                        (year_idx + 1) / total_years,
                        f"[Year {year}] No data fetched for coins, moving to next...",
                        total_items=total_years,
                        completed_items=year_idx + 1
                    )
            
            if total_coins_processed == 0:
                self.progress_reporter.report_error("No data fetched for any symbol")
                return
            
            # Report completion
            self.progress_reporter.report_completion(
                f"Process completed! Fetched and saved data for {total_coins_processed} coins across {total_years} years"
            )
            
        except Exception as e:
            self.progress_reporter.report_error(f"Error: {str(e)}")
    
    def _on_stop_click(self):
        """Handle stop button click."""
        if not self._is_running:
            return
        
        self._is_running = False
        self._log_message("Stopping process... Please wait.")
        self.stop_button.configure(state="disabled")
    
    def _on_clear_log(self):
        """Clear the status log."""
        self.status_text.configure(state="normal")
        self.status_text.delete("1.0", "end")
        self.status_text.configure(state="disabled")
        self._log_message("Log cleared.")
    
    def _on_process_complete(self, message: str):
        """Handle process completion."""
        self._is_running = False
        self.start_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self.price_entry.configure(state="normal")
        self.timeframe_combo.configure(state="normal")
        
        self._log_message("=" * 50, timestamp=False)
        self._log_message("✅ " + message)
        
        # Show completion notification
        messagebox.showinfo("Process Complete", message)
    
    def _on_process_error(self, message: str):
        """Handle process error."""
        self._is_running = False
        self.start_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self.price_entry.configure(state="normal")
        self.timeframe_combo.configure(state="normal")
        
        self._log_message("=" * 50, timestamp=False)
        self._log_message("❌ " + message)
        
        # Show error notification
        messagebox.showerror("Error", message)
    
    def on_closing(self):
        """Handle window close event."""
        if self._is_running:
            if messagebox.askokcancel("Quit", "Process is running. Do you want to quit?"):
                self._is_running = False
                self.destroy()
        else:
            self.destroy()


def run_gui():
    """Launch the GUI application."""
    app = BinanceFetcherGUI()
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()


if __name__ == "__main__":
    run_gui()
