"""
GUI Application using CustomTkinter
Provides visual feedback and control for the Binance Futures Data Fetcher.
"""

import customtkinter as ctk
from datetime import datetime, timedelta
from typing import Optional, List
import threading
import json
import os
import webbrowser
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from tkinter import messagebox, filedialog

import pandas as pd

from src.GUI.progress_reporter import ProgressReporter, ProgressInfo, ExecutionStage


# ---------------------------------------------------------------------------
#  Lightweight HTTP server for the chart bridge
# ---------------------------------------------------------------------------

class _ChartRequestHandler(BaseHTTPRequestHandler):
    """Serves the chart HTML page and handles API requests from JavaScript."""

    def log_message(self, format, *args):
        """Silence default stderr logging."""
        pass

    # ---- GET ---------------------------------------------------------------
    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path in ("/", "/chart"):
            self._serve_chart_html()
        elif parsed.path == "/api/latest_data":
            self._handle_latest_data(parsed)
        else:
            self.send_error(404)

    def _serve_chart_html(self):
        html_bytes = self.server.chart_html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html_bytes)))
        self.end_headers()
        self.wfile.write(html_bytes)

    def _handle_latest_data(self, parsed):
        """``/api/latest_data?symbol=X&since=Y`` → JSON candle array."""
        qs = parse_qs(parsed.query)
        symbol = qs.get("symbol", [""])[0]
        since_str = qs.get("since", ["0"])[0]

        records: list = []
        if symbol and self.server.data_fetcher_factory:
            try:
                fetcher = self.server.data_fetcher_factory()
                since_ms = int(since_str) * 1000 + 1
                df = fetcher.fetch_candles(symbol, since_ms)
                if df is not None and not df.empty:
                    df = df.copy()
                    df["time"] = (df["date"].astype("int64") // 10 ** 9).astype(int)
                    cols = ["time", "open", "high", "low", "close"]
                    if "volume" in df.columns:
                        cols.append("volume")
                    records = df[cols].to_dict("records")
            except Exception as e:
                print(f"[ChartServer] Error fetching latest data: {e}")

        body = json.dumps(records).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


class _ChartHTTPServer(HTTPServer):
    """HTTPServer subclass carrying extra state for the chart."""
    chart_html: str = ""
    data_fetcher_factory = None  # callable → GetData instance


def _find_free_port() -> int:
    """Return an available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class BinanceFetcherGUI(ctk.CTk):
    """
    Main GUI window for the Binance Futures Data Fetcher.
    
    Displays:
    - Start button
    - Progress bar with percentage
    - Status messages
    - Time range selection
    - Completion notifications
    - Create Chart button to switch to chart configuration view
    """
    
    def __init__(self):
        super().__init__()
        
        # Window configuration
        self.title("Binance Futures Data Fetcher")
        self.state("zoomed")  # Start maximized (fullscreen window)
        self.minsize(600, 600)
        
        # Set appearance
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        # Progress reporter for communication with core logic
        self.progress_reporter = ProgressReporter()
        self.progress_reporter.add_callback(self._on_progress_update)
        
        # State
        self._is_running = False
        self._worker_thread: Optional[threading.Thread] = None
        
        # Results storage
        self._last_filtered_df = None

        # Build UI
        self._create_widgets()
        self._create_chart_config_view()
        self._create_results_view()
        
        # Center window only if not maximized
        if self.state() != "zoomed":
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
        
        # Create Chart button
        self.create_chart_button = ctk.CTkButton(
            control_frame,
            text="📈 Create Chart",
            font=ctk.CTkFont(size=14, weight="bold"),
            height=40,
            width=140,
            fg_color="#2b8a3e",
            hover_color="#237032",
            command=self._show_chart_config_view
        )
        self.create_chart_button.pack(side="left", padx=(10, 0))
        
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
    
    # =========================================================================
    # Chart Configuration View
    # =========================================================================

    def _create_chart_config_view(self):
        """Create the chart configuration view (hidden by default)."""
        self.chart_config_frame = ctk.CTkFrame(self)
        # Not packed yet — shown only when user clicks "Create Chart"

        # --- Header ---
        header_frame = ctk.CTkFrame(self.chart_config_frame, fg_color="transparent")
        header_frame.pack(fill="x", padx=20, pady=(20, 10))

        back_button = ctk.CTkButton(
            header_frame,
            text="← Back",
            font=ctk.CTkFont(size=13),
            width=80,
            height=32,
            fg_color="gray",
            hover_color="darkgray",
            command=self._show_main_view
        )
        back_button.pack(side="left")

        title = ctk.CTkLabel(
            header_frame,
            text="📈 Create Chart",
            font=ctk.CTkFont(size=22, weight="bold")
        )
        title.pack(side="left", padx=(15, 0))

        # --- Scrollable content area ---
        content = ctk.CTkFrame(self.chart_config_frame)
        content.pack(fill="both", expand=True, padx=20, pady=(0, 10))

        # 1️⃣  Timeframe Selection
        self._build_timeframe_section(content)

        # 2️⃣  Time Range Selection
        self._build_time_range_section(content)

        # 3️⃣  Coin Selection
        self._build_coin_selection_section(content)

        # --- Generate button ---
        btn_frame = ctk.CTkFrame(self.chart_config_frame, fg_color="transparent")
        btn_frame.pack(fill="x", padx=20, pady=(0, 20))

        self.generate_button = ctk.CTkButton(
            btn_frame,
            text="🚀 Generate Chart",
            font=ctk.CTkFont(size=15, weight="bold"),
            height=45,
            width=250,
            fg_color="#2b8a3e",
            hover_color="#237032",
            command=self._on_generate_chart_click
        )
        self.generate_button.pack(pady=5)

        # Summary label (updated dynamically)
        self.chart_summary_label = ctk.CTkLabel(
            btn_frame,
            text="",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        self.chart_summary_label.pack(pady=(5, 0))

    # --- Section builders ---------------------------------------------------

    def _build_timeframe_section(self, parent):
        """Build the timeframe selection section."""
        section = ctk.CTkFrame(parent)
        section.pack(fill="x", pady=(15, 5), padx=15)

        label = ctk.CTkLabel(
            section,
            text="1️⃣  Candle Timeframe",
            font=ctk.CTkFont(size=15, weight="bold")
        )
        label.pack(anchor="w", padx=15, pady=(10, 5))

        desc = ctk.CTkLabel(
            section,
            text="Select the candle interval for the chart.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(anchor="w", padx=15)

        tf_frame = ctk.CTkFrame(section, fg_color="transparent")
        tf_frame.pack(fill="x", padx=15, pady=(5, 10))

        self.chart_timeframe_var = ctk.StringVar(value="1d")
        timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]

        for i, tf in enumerate(timeframes):
            btn = ctk.CTkRadioButton(
                tf_frame,
                text=tf,
                variable=self.chart_timeframe_var,
                value=tf,
                font=ctk.CTkFont(size=13),
                command=self._update_chart_summary
            )
            btn.grid(row=0, column=i, padx=(0, 18), pady=5)

    def _build_time_range_section(self, parent):
        """Build the year / time range selection section."""
        section = ctk.CTkFrame(parent)
        section.pack(fill="x", pady=(10, 5), padx=15)

        label = ctk.CTkLabel(
            section,
            text="2️⃣  Year & Time Range",
            font=ctk.CTkFont(size=15, weight="bold")
        )
        label.pack(anchor="w", padx=15, pady=(10, 5))

        desc = ctk.CTkLabel(
            section,
            text="Select a year as the end boundary, then choose how far back to retrieve data.",
            font=ctk.CTkFont(size=12),
            text_color="gray",
            wraplength=550,
            justify="left"
        )
        desc.pack(anchor="w", padx=15)

        # --- Year picker (always visible, mandatory) ---
        year_frame = ctk.CTkFrame(section, fg_color="transparent")
        year_frame.pack(fill="x", padx=15, pady=(8, 2))

        year_label = ctk.CTkLabel(
            year_frame,
            text="Year:",
            font=ctk.CTkFont(size=13, weight="bold"),
            width=120,
            anchor="w"
        )
        year_label.pack(side="left")

        current_year = datetime.now().year
        year_values = [str(y) for y in range(2019, current_year + 1)]

        self.specific_year_var = ctk.StringVar(value=str(current_year))
        self.specific_year_combo = ctk.CTkComboBox(
            year_frame,
            values=year_values,
            variable=self.specific_year_var,
            width=100,
            command=lambda _: self._update_chart_summary()
        )
        self.specific_year_combo.pack(side="left", padx=(10, 0))

        year_hint = ctk.CTkLabel(
            year_frame,
            text="(end boundary for retrieved data)",
            font=ctk.CTkFont(size=11),
            text_color="gray"
        )
        year_hint.pack(side="left", padx=(10, 0))

        # --- Time range relative to the selected year ---
        range_label = ctk.CTkLabel(
            section,
            text="How far back from the selected year:",
            font=ctk.CTkFont(size=13),
            anchor="w"
        )
        range_label.pack(anchor="w", padx=15, pady=(10, 2))

        range_frame = ctk.CTkFrame(section, fg_color="transparent")
        range_frame.pack(fill="x", padx=15, pady=(0, 10))

        self.time_range_var = ctk.StringVar(value="from_beginning")

        ranges = [
            ("from_beginning", "From beginning of year"),
            ("last_6m", "Last 6 months"),
            ("last_1y", "Last 1 year"),
            ("last_2y", "Last 2 years"),
            ("last_5y", "Last 5 years"),
        ]

        for i, (value, text) in enumerate(ranges):
            rb = ctk.CTkRadioButton(
                range_frame,
                text=text,
                variable=self.time_range_var,
                value=value,
                font=ctk.CTkFont(size=13),
                command=self._on_time_range_changed
            )
            rb.grid(row=i // 3, column=i % 3, sticky="w", padx=(0, 25), pady=4)

    def _build_coin_selection_section(self, parent):
        """Build the coin scope selection section."""
        section = ctk.CTkFrame(parent)
        section.pack(fill="x", pady=(10, 5), padx=15)

        label = ctk.CTkLabel(
            section,
            text="3️⃣  Coin Selection",
            font=ctk.CTkFont(size=15, weight="bold")
        )
        label.pack(anchor="w", padx=15, pady=(10, 5))

        desc = ctk.CTkLabel(
            section,
            text="Generate the chart for a single coin or all available coins.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(anchor="w", padx=15)

        coin_frame = ctk.CTkFrame(section, fg_color="transparent")
        coin_frame.pack(fill="x", padx=15, pady=(5, 5))

        self.coin_scope_var = ctk.StringVar(value="all")

        all_rb = ctk.CTkRadioButton(
            coin_frame,
            text="All available coins",
            variable=self.coin_scope_var,
            value="all",
            font=ctk.CTkFont(size=13),
            command=self._on_coin_scope_changed
        )
        all_rb.grid(row=0, column=0, sticky="w", padx=(0, 30), pady=4)

        single_rb = ctk.CTkRadioButton(
            coin_frame,
            text="Single coin",
            variable=self.coin_scope_var,
            value="single",
            font=ctk.CTkFont(size=13),
            command=self._on_coin_scope_changed
        )
        single_rb.grid(row=0, column=1, sticky="w", padx=(0, 30), pady=4)

        # Single coin entry (shown/hidden dynamically)
        self.single_coin_frame = ctk.CTkFrame(section, fg_color="transparent")
        self.single_coin_frame.pack(fill="x", padx=15, pady=(0, 10))

        coin_label = ctk.CTkLabel(
            self.single_coin_frame,
            text="Symbol:",
            font=ctk.CTkFont(size=13),
            width=55
        )
        coin_label.pack(side="left")

        self.single_coin_var = ctk.StringVar(value="BTC/USDT")
        self.single_coin_entry = ctk.CTkEntry(
            self.single_coin_frame,
            textvariable=self.single_coin_var,
            width=160,
            placeholder_text="e.g. BTC/USDT"
        )
        self.single_coin_entry.pack(side="left", padx=(10, 0))

        coin_hint = ctk.CTkLabel(
            self.single_coin_frame,
            text="(Binance Futures USDT-M symbol)",
            font=ctk.CTkFont(size=11),
            text_color="gray"
        )
        coin_hint.pack(side="left", padx=(10, 0))

        # Hide single coin input by default
        self.single_coin_frame.pack_forget()

    # --- Dynamic UI callbacks -----------------------------------------------

    def _on_time_range_changed(self):
        """Update summary when time range selection changes."""
        self._update_chart_summary()

    def _on_coin_scope_changed(self):
        """Show/hide the single coin entry based on coin scope selection."""
        if self.coin_scope_var.get() == "single":
            self.single_coin_frame.pack(fill="x", padx=15, pady=(0, 10))
        else:
            self.single_coin_frame.pack_forget()
        self._update_chart_summary()

    def _update_chart_summary(self):
        """Update the dynamic summary label with current selections."""
        tf = self.chart_timeframe_var.get()
        tr = self.time_range_var.get()
        ref_year = self.specific_year_var.get()
        scope = self.coin_scope_var.get()

        range_labels = {
            "from_beginning": f"Jan–Dec {ref_year}",
            "last_6m": f"last 6 months up to end of {ref_year}",
            "last_1y": f"last 1 year up to end of {ref_year}",
            "last_2y": f"last 2 years up to end of {ref_year}",
            "last_5y": f"last 5 years up to end of {ref_year}",
        }
        range_text = range_labels.get(tr, tr)
        coin_text = "all coins" if scope == "all" else f"{self.single_coin_var.get()}"

        summary = f"ℹ️  Will generate {tf} candles for {range_text} — {coin_text}"
        self.chart_summary_label.configure(text=summary)

    # --- View switching -----------------------------------------------------

    def _show_chart_config_view(self):
        """Switch from the main view to the chart configuration view."""
        self.main_frame.pack_forget()
        self.chart_config_frame.pack(fill="both", expand=True, padx=20, pady=20)
        self._update_chart_summary()

    def _show_main_view(self):
        """Switch back to the main view from any other view."""
        self.chart_config_frame.pack_forget()
        if hasattr(self, 'results_frame'):
            self.results_frame.pack_forget()
        self.main_frame.pack(fill="both", expand=True, padx=20, pady=20)

    # =========================================================================
    # Results View (Chart only)
    # =========================================================================

    def _create_results_view(self):
        """Create the results view (simplified – chart is in a WebView window)."""
        self.results_frame = ctk.CTkFrame(self)
        # Not packed — shown only after chart generation completes

        # --- Header ---
        header_frame = ctk.CTkFrame(self.results_frame, fg_color="transparent")
        header_frame.pack(fill="x", padx=20, pady=(20, 10))

        back_btn = ctk.CTkButton(
            header_frame,
            text="← Back",
            font=ctk.CTkFont(size=13),
            width=80,
            height=32,
            fg_color="gray",
            hover_color="darkgray",
            command=self._show_main_view
        )
        back_btn.pack(side="left")

        title = ctk.CTkLabel(
            header_frame,
            text="📈 Chart View",
            font=ctk.CTkFont(size=22, weight="bold")
        )
        title.pack(side="left", padx=(15, 0))

        # --- Info label ---
        self.results_info_label = ctk.CTkLabel(
            self.results_frame,
            text="",
            font=ctk.CTkFont(size=13),
            text_color="gray"
        )
        self.results_info_label.pack(anchor="w", padx=20, pady=(0, 5))

        # --- Chart status area ---
        status_area = ctk.CTkFrame(self.results_frame)
        status_area.pack(fill="both", expand=True, padx=20, pady=(0, 10))

        chart_msg = ctk.CTkLabel(
            status_area,
            text="📊 The interactive TradingView chart is displayed\n"
                 "in its own window.  Use the button below to\n"
                 "reopen it if you closed it.",
            font=ctk.CTkFont(size=15),
            justify="center",
        )
        chart_msg.pack(expand=True)

        self._reopen_chart_btn = ctk.CTkButton(
            status_area,
            text="🔄  Reopen Chart Window",
            font=ctk.CTkFont(size=14, weight="bold"),
            height=42,
            width=240,
            fg_color="#2962ff",
            hover_color="#1e53e5",
            command=self._reopen_chart_window,
        )
        self._reopen_chart_btn.pack(pady=(0, 20))

        # === Bottom buttons ===
        btn_frame = ctk.CTkFrame(self.results_frame, fg_color="transparent")
        btn_frame.pack(fill="x", padx=20, pady=(0, 20))

        self.export_filtered_btn = ctk.CTkButton(
            btn_frame,
            text="💾 Export Data CSV",
            font=ctk.CTkFont(size=14),
            height=40,
            width=200,
            fg_color="#555555",
            hover_color="#444444",
            command=self._export_filtered_csv,
        )
        self.export_filtered_btn.pack(side="left", padx=(0, 10))

        # Chart HTTP server handle (managed externally)
        self._chart_server = None
        self._chart_timeframe = "1d"

    # --- WebView chart helpers -----------------------------------------------

    def _prepare_chart_data(self, df: pd.DataFrame) -> str:
        """
        Convert a filtered DataFrame into a JSON string consumable by
        the TradingView Lightweight Charts HTML page.

        Returns JSON of the form::

            {
                "coins": ["BTC/USDT", ...],
                "data": {
                    "BTC/USDT": [
                        {"time": <unix-sec>, "open": …, "high": …, …},
                        …
                    ]
                }
            }
        """
        result: dict = {"coins": [], "data": {}}
        coins = sorted(df["symbol"].unique().tolist())
        result["coins"] = coins

        for coin in coins:
            coin_df = df[df["symbol"] == coin].sort_values("date").copy()
            coin_df["time"] = (
                coin_df["date"].astype("int64") // 10 ** 9
            ).astype(int)

            cols = ["time", "open", "high", "low", "close"]
            if "volume" in coin_df.columns:
                cols.append("volume")

            result["data"][coin] = coin_df[cols].to_dict("records")

        return json.dumps(result)

    def _launch_chart_in_browser(self, chart_json: str, timeframe: str):
        """
        Start a local HTTP server and open the chart in the default browser.

        The HTML template is loaded from ``chart.html`` next to this module.
        ``INITIAL_DATA`` is replaced with *chart_json* so the chart renders
        immediately, and ``API_BASE`` is set so JS can call back into Python
        via ``fetch()``.
        """
        # Read the HTML template packaged alongside this module
        html_path = os.path.join(os.path.dirname(__file__), "chart.html")
        with open(html_path, "r", encoding="utf-8") as fh:
            html_template = fh.read()

        # Pick a free port for this server instance
        port = _find_free_port()

        # Inject the dataset + API base URL into the page
        html_content = html_template.replace(
            "const initialData = INITIAL_DATA;",
            f"const initialData = {chart_json};",
        ).replace(
            "const API_BASE = null;",
            f'const API_BASE = "http://127.0.0.1:{port}";',
        )

        # Shut down a previously running chart server
        self._stop_chart_server()

        # Build the data-fetcher factory (lazy import)
        def _fetcher_factory():
            from src.LOGIC.get_data import GetData
            return GetData(timeframe=timeframe)

        server = _ChartHTTPServer(("127.0.0.1", port), _ChartRequestHandler)
        server.chart_html = html_content
        server.data_fetcher_factory = _fetcher_factory
        self._chart_server = server

        # Run the server in a daemon thread
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()

        # Open in the default browser
        webbrowser.open(f"http://127.0.0.1:{port}/chart")

    def _stop_chart_server(self):
        """Shut down the chart HTTP server if it is running."""
        if self._chart_server is not None:
            try:
                self._chart_server.shutdown()
            except Exception:
                pass
            self._chart_server = None

    def _reopen_chart_window(self):
        """Reopen the chart in the browser using the last generated dataset."""
        if self._last_filtered_df is None or self._last_filtered_df.empty:
            messagebox.showwarning("No Data", "Generate chart data first.")
            return
        chart_json = self._prepare_chart_data(self._last_filtered_df)
        self._launch_chart_in_browser(chart_json, self._chart_timeframe)

    def _show_results_view(self, filtered_df, timeframe: str = "1d"):
        """Switch to the results view and open the interactive chart window."""
        self.main_frame.pack_forget()
        self.chart_config_frame.pack_forget()

        n_coins = filtered_df["symbol"].nunique() if filtered_df is not None else 0
        n_rows = len(filtered_df) if filtered_df is not None else 0
        self.results_info_label.configure(
            text=f"  {n_coins} coin(s)  ·  {n_rows} total candles"
        )

        self.results_frame.pack(fill="both", expand=True, padx=20, pady=20)

        # Launch the TradingView WebView chart
        if filtered_df is not None and not filtered_df.empty:
            self._chart_timeframe = timeframe
            chart_json = self._prepare_chart_data(filtered_df)
            self._launch_chart_in_browser(chart_json, timeframe)

    def _on_chart_generation_complete(self, n_coins: int, n_rows: int,
                                      timeframe: str = "1d"):
        """Handle chart generation completion on the main thread."""
        self._is_running = False
        self.start_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self.create_chart_button.configure(state="normal")
        self.price_entry.configure(state="normal")
        self.timeframe_combo.configure(state="normal")

        self.overall_progress.set(1.0)
        self.overall_percent_label.configure(text="100%")
        self.stage_progress.set(1.0)
        self.stage_percent_label.configure(text="100%")
        self.stage_name_label.configure(text="Stage: Completed")

        self._log_message("=" * 50, timestamp=False)
        self._log_message(
            f"✅ Chart generated! {n_coins} coin(s), {n_rows} total candles"
        )

        # Show results in the chart view
        if self._last_filtered_df is not None:
            self._show_results_view(self._last_filtered_df, timeframe)

    def _export_filtered_csv(self):
        """Export the full filtered dataset to a CSV file."""
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
            messagebox.showinfo(
                "Exported", f"Full data exported to:\n{filepath}"
            )

    # --- Generate chart handler -----------------------------------------------

    def _on_generate_chart_click(self):
        """Handle Generate Chart button click — collect params and start process."""
        if self._is_running:
            messagebox.showwarning("Busy", "A process is already running.")
            return

        # Collect parameters
        timeframe = self.chart_timeframe_var.get()
        time_range = self.time_range_var.get()
        coin_scope = self.coin_scope_var.get()
        reference_year = self.specific_year_var.get()  # always required
        single_coin = self.single_coin_var.get().strip() if coin_scope == "single" else None

        # Validate single coin
        if coin_scope == "single" and not single_coin:
            messagebox.showerror("Invalid Input", "Please enter a coin symbol (e.g. BTC/USDT).")
            return

        # Switch back to main view to show progress
        self._show_main_view()

        # Reset progress
        self.overall_progress.set(0)
        self.stage_progress.set(0)
        self.overall_percent_label.configure(text="0%")
        self.stage_percent_label.configure(text="0%")
        self.stage_name_label.configure(text="Stage: Starting chart generation...")

        # Update UI state
        self._is_running = True
        self.start_button.configure(state="disabled")
        self.stop_button.configure(state="normal")
        self.create_chart_button.configure(state="disabled")
        self.price_entry.configure(state="disabled")
        self.timeframe_combo.configure(state="disabled")

        scope_desc = single_coin if single_coin else "all coins"

        self._log_message("=" * 50, timestamp=False)
        self._log_message("Starting Chart Generation...")
        self._log_message(f"Timeframe: {timeframe}")
        self._log_message(f"Reference Year: {reference_year}")
        self._log_message(f"Time Range: {time_range}")
        self._log_message(f"Coin Scope: {scope_desc}")

        # Start worker thread
        self._worker_thread = threading.Thread(
            target=self._run_chart_generation,
            args=(timeframe, time_range, reference_year, coin_scope, single_coin),
            daemon=True
        )
        self._worker_thread.start()

    def _run_chart_generation(self, timeframe: str, time_range: str,
                              reference_year: str, coin_scope: str,
                              single_coin: Optional[str]):
        """
        Run the chart data generation process in a background thread.

        Retrieves pre-existing parquet files from Google Cloud Storage,
        filters by date range and coin scope, and prepares data
        for chart display.
        """
        try:
            from src.LOGIC.google_cloud_storage_api import GoogleCloudStorageAPI
            from src.LOGIC.chart_generator import ChartGenerator
            import logging

            logger = logging.getLogger(__name__)

            # --- Initialization ---
            self.progress_reporter.report(
                ExecutionStage.INITIALIZING, 0.5,
                "Initializing chart generation..."
            )

            # --- Authenticate with Google Cloud Storage ---
            self.progress_reporter.report(
                ExecutionStage.AUTHENTICATING, 0.0,
                "Authenticating with Google Cloud Storage..."
            )
            storage_api = GoogleCloudStorageAPI(logger=logger)
            self.progress_reporter.report(
                ExecutionStage.AUTHENTICATING, 1.0,
                "Google Cloud Storage authentication successful"
            )

            if not self._is_running:
                self.progress_reporter.report_error("Process cancelled by user")
                return

            # --- Generate chart data from GCS ---
            generator = ChartGenerator(
                storage_api=storage_api,
                progress_reporter=self.progress_reporter,
                logger=logger
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

            # Store results for display / export
            self._last_filtered_df = filtered_df

            n_coins = filtered_df['symbol'].nunique()
            n_rows = len(filtered_df)

            # Schedule UI update on the main thread
            self.after(0, lambda: self._on_chart_generation_complete(
                n_coins, n_rows, timeframe))

        except ImportError as e:
            self.progress_reporter.report_error(
                f"Missing dependency: {e}. "
                "Install with: pip install python-dateutil pyarrow"
            )
        except Exception as e:
            self.progress_reporter.report_error(f"Error: {str(e)}")

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
            from src.LOGIC.google_cloud_storage_api import GoogleCloudStorageAPI
            from src.LOGIC.get_data import GetData
            from src.LOGIC.save_data import SaveData
            import logging
            
            # Setup a simple logger
            logger = logging.getLogger(__name__)
            
            # Report initialization
            self.progress_reporter.report(
                ExecutionStage.INITIALIZING,
                0.5,
                "Initializing components..."
            )
            
            # Initialize Google Cloud Storage API
            self.progress_reporter.report(
                ExecutionStage.AUTHENTICATING,
                0.0,
                "Authenticating with Google Cloud Storage..."
            )
            storage_api = GoogleCloudStorageAPI(logger=logger)
            self.progress_reporter.report(
                ExecutionStage.AUTHENTICATING,
                1.0,
                "Google Cloud Storage authentication successful"
            )
            
            # Initialize data fetcher
            data_fetcher = GetData(
                price_threshold=price_threshold,
                timeframe=timeframe,
                logger=logger
            )
            
            # Initialize data saver with timeframe for filename
            data_saver = SaveData(
                storage_api=storage_api,
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
                        f"[Year {year}] Saving {coins_in_year} coins to Google Cloud Storage...",
                        total_items=total_years,
                        completed_items=year_idx
                    )
                    
                    # Save immediately after fetching this year's data
                    data_saver.save_single_year(year, year_data)
                    
                    self.progress_reporter.report(
                        ExecutionStage.UPLOADING,
                        (year_idx + 1) / total_years,
                        f"[Year {year}] ✓ Saved {coins_in_year} coins to Google Cloud Storage",
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
        self.create_chart_button.configure(state="normal")
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
        self.create_chart_button.configure(state="normal")
        self.price_entry.configure(state="normal")
        self.timeframe_combo.configure(state="normal")
        
        self._log_message("=" * 50, timestamp=False)
        self._log_message("❌ " + message)
        
        # Show error notification
        messagebox.showerror("Error", message)
    
    def on_closing(self):
        """Handle window close event."""
        # Shut down the chart server if it's still running
        self._stop_chart_server()

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
