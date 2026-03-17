"""Main modular GUI window and application orchestration."""

from __future__ import annotations

import json
import os
import threading
from typing import Optional

import customtkinter as ctk
import pandas as pd
from tkinter import filedialog, messagebox

from src.GUI.progress_reporter import ExecutionStage, ProgressInfo, ProgressReporter

from .chart_bridge import ChartBridge
from .chart_page import ChartPage
from .components import percent_text
from .data_page import DataPage
from .home import HomePage


class BinanceFetcherGUI(ctk.CTk):
    """Main GUI controller that coordinates modular pages and background jobs."""

    def __init__(self):
        super().__init__()

        self.title("Binance Futures Data Fetcher")
        self.state("zoomed")
        self.minsize(900, 600)

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.progress_reporter = ProgressReporter()
        self.progress_reporter.add_callback(self._on_progress_update)

        self._is_running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._last_filtered_df: Optional[pd.DataFrame] = None
        self._chart_timeframe = "1d"

        assets_dir = os.path.join(os.path.dirname(__file__), "assets")
        self._chart_bridge = ChartBridge(assets_dir=assets_dir)

        self._build_layout()
        self._show_page("home")

    def _build_layout(self) -> None:
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.nav_frame = ctk.CTkFrame(self, width=220, corner_radius=0)
        self.nav_frame.grid(row=0, column=0, sticky="nsw")

        nav_title = ctk.CTkLabel(
            self.nav_frame,
            text="Binance Fetcher",
            font=ctk.CTkFont(size=20, weight="bold"),
        )
        nav_title.pack(padx=20, pady=(20, 10), anchor="w")

        self.home_nav_button = ctk.CTkButton(
            self.nav_frame,
            text="Home",
            command=lambda: self._show_page("home"),
            height=40,
        )
        self.home_nav_button.pack(fill="x", padx=12, pady=(6, 4))

        self.data_nav_button = ctk.CTkButton(
            self.nav_frame,
            text="Data",
            command=lambda: self._show_page("data"),
            height=40,
        )
        self.data_nav_button.pack(fill="x", padx=12, pady=4)

        self.chart_nav_button = ctk.CTkButton(
            self.nav_frame,
            text="Chart",
            command=lambda: self._show_page("chart"),
            height=40,
        )
        self.chart_nav_button.pack(fill="x", padx=12, pady=4)

        self.page_container = ctk.CTkFrame(self, corner_radius=0)
        self.page_container.grid(row=0, column=1, sticky="nsew")
        self.page_container.grid_rowconfigure(0, weight=1)
        self.page_container.grid_columnconfigure(0, weight=1)

        self.home_page = HomePage(
            self.page_container,
            on_open_data=lambda: self._show_page("data"),
            on_open_chart=lambda: self._show_page("chart"),
        )
        self.data_page = DataPage(
            self.page_container,
            on_start=self._on_start_click,
            on_stop=self._on_stop_click,
            on_clear_log=self._on_clear_log,
            on_open_chart=lambda: self._show_page("chart"),
        )
        self.chart_page = ChartPage(
            self.page_container,
            on_generate=self._on_generate_chart_click,
            on_reopen_chart=self._reopen_chart_window,
            on_export_csv=self._export_filtered_csv,
        )

        self.pages = {
            "home": self.home_page,
            "data": self.data_page,
            "chart": self.chart_page,
        }

        for page in self.pages.values():
            page.grid(row=0, column=0, sticky="nsew")

    def _show_page(self, page_name: str) -> None:
        page = self.pages[page_name]
        page.tkraise()

        active_color = "#2962ff"
        inactive_color = "#3a3a3a"

        self.home_nav_button.configure(fg_color=active_color if page_name == "home" else inactive_color)
        self.data_nav_button.configure(fg_color=active_color if page_name == "data" else inactive_color)
        self.chart_nav_button.configure(fg_color=active_color if page_name == "chart" else inactive_color)

    def _prepare_chart_data(self, df: pd.DataFrame) -> str:
        result = {"coins": [], "data": {}}
        coins = sorted(df["symbol"].unique().tolist())
        result["coins"] = coins

        for coin in coins:
            coin_df = df[df["symbol"] == coin].sort_values("date").copy()
            coin_df["time"] = (coin_df["date"].astype("int64") // 10 ** 9).astype(int)

            cols = ["time", "open", "high", "low", "close"]
            if "volume" in coin_df.columns:
                cols.append("volume")
            for ma_col in ("MA_7", "MA_25", "MA_99", "ma_volume_7", "ma_volume_25", "ma_volume_99"):
                if ma_col in coin_df.columns:
                    cols.append(ma_col)

            records = coin_df[cols].to_dict("records")
            for record in records:
                for ma_col in ("MA_7", "MA_25", "MA_99", "ma_volume_7", "ma_volume_25", "ma_volume_99"):
                    if ma_col in record and (record[ma_col] is None or record[ma_col] != record[ma_col]):
                        record[ma_col] = None

            result["data"][coin] = records

        return json.dumps(result)

    def _launch_chart_in_browser(self, chart_json: str, timeframe: str) -> None:
        def fetcher_factory():
            from src.LOGIC.get_data import GetData

            return GetData(timeframe=timeframe)

        self._chart_bridge.launch_chart(
            chart_json=chart_json,
            data_fetcher_factory=fetcher_factory,
        )

    def _reopen_chart_window(self) -> None:
        if self._last_filtered_df is None or self._last_filtered_df.empty:
            messagebox.showwarning("No Data", "Generate chart data first.")
            return

        chart_json = self._prepare_chart_data(self._last_filtered_df)
        self._launch_chart_in_browser(chart_json, self._chart_timeframe)

    def _on_generate_chart_click(self) -> None:
        if self._is_running:
            messagebox.showwarning("Busy", "A process is already running.")
            return

        params = self.chart_page.get_generation_params()

        if params["coin_scope"] == "single" and not params["single_coin"]:
            messagebox.showerror("Invalid Input", "Please enter a coin symbol (e.g. BTC/USDT).")
            return

        self._show_page("data")
        self.data_page.reset_progress("Stage: Starting chart generation...")

        self._is_running = True
        self.data_page.set_fetch_running_state(running=True, chart_generation=True)
        self.chart_page.set_generation_running(True)

        scope_desc = params["single_coin"] if params["single_coin"] else "all coins"

        self.data_page.log_message("=" * 50, timestamp=False)
        self.data_page.log_message("Starting Chart Generation...")
        self.data_page.log_message(f"Timeframe: {params['timeframe']}")
        self.data_page.log_message(f"Reference Year: {params['reference_year']}")
        self.data_page.log_message(f"Time Range: {params['time_range']}")
        self.data_page.log_message(f"Coin Scope: {scope_desc}")

        self._worker_thread = threading.Thread(
            target=self._run_chart_generation,
            args=(
                params["timeframe"],
                params["time_range"],
                params["reference_year"],
                params["coin_scope"],
                params["single_coin"],
            ),
            daemon=True,
        )
        self._worker_thread.start()

    def _run_chart_generation(
        self,
        timeframe: str,
        time_range: str,
        reference_year: str,
        coin_scope: str,
        single_coin: Optional[str],
    ) -> None:
        try:
            from src.LOGIC.chart_generator import ChartGenerator
            from src.LOGIC.google_cloud_storage_api import GoogleCloudStorageAPI
            from src.core.logger import get_logger

            logger = get_logger(__name__)

            self.progress_reporter.report(
                ExecutionStage.INITIALIZING,
                0.5,
                "Initializing chart generation...",
            )

            self.progress_reporter.report(
                ExecutionStage.AUTHENTICATING,
                0.0,
                "Authenticating with Google Cloud Storage...",
            )
            storage_api = GoogleCloudStorageAPI(logger=logger)
            self.progress_reporter.report(
                ExecutionStage.AUTHENTICATING,
                1.0,
                "Google Cloud Storage authentication successful",
            )

            if not self._is_running:
                self.progress_reporter.report_error("Process cancelled by user")
                return

            generator = ChartGenerator(
                storage_api=storage_api,
                progress_reporter=self.progress_reporter,
                logger=logger,
            )

            filtered_df = generator.generate_chart_data(
                timeframe=timeframe,
                time_range=time_range,
                reference_year=reference_year,
                coin_scope=coin_scope,
                single_coin=single_coin,
                is_running_check=lambda: self._is_running,
            )

            if filtered_df is None or filtered_df.empty:
                self.progress_reporter.report_error(
                    "No data found for the selected criteria. "
                    "Make sure data has been fetched first using 'Start Process'."
                )
                return

            self._last_filtered_df = filtered_df
            n_coins = filtered_df["symbol"].nunique()
            n_rows = len(filtered_df)

            self.after(0, lambda: self._on_chart_generation_complete(n_coins, n_rows, timeframe))

        except ImportError as error:
            self.progress_reporter.report_error(
                f"Missing dependency: {error}. Install with: pip install python-dateutil pyarrow"
            )
        except Exception as error:
            self.progress_reporter.report_error(f"Error: {str(error)}")

    def _on_chart_generation_complete(self, n_coins: int, n_rows: int, timeframe: str = "1d") -> None:
        self._is_running = False
        self.data_page.set_fetch_running_state(running=False)
        self.chart_page.set_generation_running(False)

        self.data_page.overall_progress.set(1.0)
        self.data_page.overall_percent_label.configure(text="100%")
        self.data_page.stage_progress.set(1.0)
        self.data_page.stage_percent_label.configure(text="100%")
        self.data_page.stage_name_label.configure(text="Stage: Completed")

        self.data_page.log_message("=" * 50, timestamp=False)
        self.data_page.log_message(f"Chart generated! {n_coins} coin(s), {n_rows} total candles")

        if self._last_filtered_df is not None:
            self._chart_timeframe = timeframe
            chart_json = self._prepare_chart_data(self._last_filtered_df)
            self._launch_chart_in_browser(chart_json, timeframe)

        self.chart_page.set_results_info(n_coins, n_rows)
        self.home_page.set_system_status("Chart generated")
        self._show_page("chart")

    def _export_filtered_csv(self) -> None:
        if self._last_filtered_df is None or self._last_filtered_df.empty:
            messagebox.showwarning("No Data", "No filtered data to export.")
            return

        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Full Filtered Data",
            initialfile="filtered_data.csv",
        )
        if filepath:
            self._last_filtered_df.to_csv(filepath, index=False)
            messagebox.showinfo("Exported", f"Full data exported to:\n{filepath}")

    def _on_start_click(self) -> None:
        if self._is_running:
            return

        try:
            price_threshold = float(self.data_page.price_threshold_var.get())
            if price_threshold <= 0:
                raise ValueError("Price threshold must be positive")
        except ValueError as error:
            messagebox.showerror("Invalid Input", f"Invalid price threshold: {error}")
            return

        self.data_page.reset_progress("Stage: Starting...")
        self._is_running = True
        self.data_page.set_fetch_running_state(running=True)

        timeframe = self.data_page.timeframe_var.get()
        self.data_page.log_message("=" * 50, timestamp=False)
        self.data_page.log_message("Starting Binance Futures Data Fetcher...")
        self.data_page.log_message(f"Price Threshold: {price_threshold} USDT")
        self.data_page.log_message(f"Timeframe: {timeframe}")
        self.data_page.log_message("Fetching all available historical data...")

        self._worker_thread = threading.Thread(
            target=self._run_process,
            args=(price_threshold, timeframe),
            daemon=True,
        )
        self._worker_thread.start()

    def _run_process(self, price_threshold: float, timeframe: str) -> None:
        try:
            from src.LOGIC.get_data import GetData
            from src.LOGIC.google_cloud_storage_api import GoogleCloudStorageAPI
            from src.LOGIC.save_data import SaveData
            from src.core.logger import get_logger

            logger = get_logger(__name__)

            self.progress_reporter.report(
                ExecutionStage.INITIALIZING,
                0.5,
                "Initializing components...",
            )

            self.progress_reporter.report(
                ExecutionStage.AUTHENTICATING,
                0.0,
                "Authenticating with Google Cloud Storage...",
            )
            storage_api = GoogleCloudStorageAPI(logger=logger)
            self.progress_reporter.report(
                ExecutionStage.AUTHENTICATING,
                1.0,
                "Google Cloud Storage authentication successful",
            )

            data_fetcher = GetData(
                price_threshold=price_threshold,
                timeframe=timeframe,
                logger=logger,
            )

            data_saver = SaveData(
                storage_api=storage_api,
                timeframe=timeframe,
                logger=logger,
            )

            self.progress_reporter.report(
                ExecutionStage.FETCHING_MARKETS,
                0.0,
                "Scanning all symbols and grouping by listing year (one-time scan)...",
            )

            symbols_by_year = data_fetcher.scan_and_group_symbols_by_year()
            if not symbols_by_year:
                self.progress_reporter.report_error("No symbols found or could not group by year")
                return

            min_year = min(symbols_by_year.keys())
            max_year = max(symbols_by_year.keys())
            total_symbols = sum(len(symbols) for symbols in symbols_by_year.values())

            self.progress_reporter.report(
                ExecutionStage.FETCHING_MARKETS,
                1.0,
                f"Scan complete: {total_symbols} coins grouped into years {min_year}-{max_year}",
            )

            total_years = max_year - min_year + 1
            total_coins_processed = 0

            for year_index, year in enumerate(range(min_year, max_year + 1)):
                if not self._is_running:
                    self.progress_reporter.report_error("Process cancelled by user")
                    return

                year_symbols = symbols_by_year.get(year, [])

                if not year_symbols:
                    self.progress_reporter.report(
                        ExecutionStage.PROCESSING_SYMBOLS,
                        (year_index + 1) / total_years,
                        f"[Year {year}] No coins listed in this year, moving to next...",
                        total_items=total_years,
                        completed_items=year_index + 1,
                    )
                    continue

                overall_progress = year_index / total_years
                self.progress_reporter.report(
                    ExecutionStage.PROCESSING_SYMBOLS,
                    overall_progress,
                    f"[Year {year}] Fetching data for {len(year_symbols)} coins ({year_index + 1}/{total_years})...",
                    total_items=total_years,
                    completed_items=year_index,
                )

                year_data = data_fetcher.fetch_data_for_symbols(year_symbols)

                if year_data:
                    coins_in_year = len(year_data)
                    total_coins_processed += coins_in_year

                    self.progress_reporter.report(
                        ExecutionStage.UPLOADING,
                        (year_index + 0.5) / total_years,
                        f"[Year {year}] Saving {coins_in_year} coins to Google Cloud Storage...",
                        total_items=total_years,
                        completed_items=year_index,
                    )

                    data_saver.save_single_year(year, year_data)

                    self.progress_reporter.report(
                        ExecutionStage.UPLOADING,
                        (year_index + 1) / total_years,
                        f"[Year {year}] Saved {coins_in_year} coins to Google Cloud Storage",
                        total_items=total_years,
                        completed_items=year_index + 1,
                    )
                else:
                    self.progress_reporter.report(
                        ExecutionStage.PROCESSING_SYMBOLS,
                        (year_index + 1) / total_years,
                        f"[Year {year}] No data fetched for coins, moving to next...",
                        total_items=total_years,
                        completed_items=year_index + 1,
                    )

            if total_coins_processed == 0:
                self.progress_reporter.report_error("No data fetched for any symbol")
                return

            self.progress_reporter.report_completion(
                f"Process completed! Fetched and saved data for {total_coins_processed} coins across {total_years} years"
            )

        except Exception as error:
            self.progress_reporter.report_error(f"Error: {str(error)}")

    def _on_stop_click(self) -> None:
        if not self._is_running:
            return

        self._is_running = False
        self.data_page.log_message("Stopping process... Please wait.")
        self.data_page.stop_button.configure(state="disabled")

    def _on_clear_log(self) -> None:
        self.data_page.clear_log()

    def _on_progress_update(self, info: ProgressInfo) -> None:
        self.after(0, lambda: self._update_ui(info))

    def _update_ui(self, info: ProgressInfo) -> None:
        self.data_page.overall_progress.set(info.overall_progress)
        self.data_page.overall_percent_label.configure(text=percent_text(info.overall_progress))
        self.data_page.stage_progress.set(info.stage_progress)
        self.data_page.stage_percent_label.configure(text=percent_text(info.stage_progress))
        self.data_page.stage_name_label.configure(text=f"Stage: {info.stage.value}")
        self.data_page.log_message(info.message)

        if info.stage == ExecutionStage.AUTHENTICATING:
            self.home_page.set_storage_status("Authenticated")
        elif info.stage == ExecutionStage.ERROR:
            self.home_page.set_system_status("Error")
        elif info.stage == ExecutionStage.COMPLETED:
            self.home_page.set_system_status("Completed")

        if info.stage == ExecutionStage.COMPLETED:
            self._on_process_complete(info.message)
        elif info.stage == ExecutionStage.ERROR:
            self._on_process_error(info.message)

    def _on_process_complete(self, message: str) -> None:
        self._is_running = False
        self.data_page.set_fetch_running_state(running=False)
        self.chart_page.set_generation_running(False)

        self.data_page.log_message("=" * 50, timestamp=False)
        self.data_page.log_message(f"{message}")
        messagebox.showinfo("Process Complete", message)

    def _on_process_error(self, message: str) -> None:
        self._is_running = False
        self.data_page.set_fetch_running_state(running=False)
        self.chart_page.set_generation_running(False)

        self.data_page.log_message("=" * 50, timestamp=False)
        self.data_page.log_message(f"{message}")
        messagebox.showerror("Error", message)

    def on_closing(self) -> None:
        self._chart_bridge.stop()
        if self._is_running:
            if messagebox.askokcancel("Quit", "Process is running. Do you want to quit?"):
                self._is_running = False
                self.destroy()
        else:
            self.destroy()


def run_gui() -> None:
    """Launch the modular GUI application."""
    app = BinanceFetcherGUI()
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()


if __name__ == "__main__":
    run_gui()
