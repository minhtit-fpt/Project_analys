"""Data management and fetch page."""

from __future__ import annotations

from datetime import datetime

import customtkinter as ctk

from .components import append_log_message


class DataPage(ctk.CTkFrame):
    """Page that hosts fetch configuration, progress, and run controls."""

    def __init__(self, parent, on_start, on_stop, on_clear_log, on_open_chart):
        super().__init__(parent)
        self._on_start = on_start
        self._on_stop = on_stop
        self._on_clear_log = on_clear_log
        self._on_open_chart = on_open_chart

        self._build_ui()

    def _build_ui(self) -> None:
        title_label = ctk.CTkLabel(
            self,
            text="Data Management",
            font=ctk.CTkFont(size=26, weight="bold"),
        )
        title_label.pack(anchor="w", padx=20, pady=(20, 12))

        self.main_frame = ctk.CTkFrame(self)
        self.main_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))

        self._create_config_frame()
        self._create_progress_frame()
        self._create_status_frame()
        self._create_control_frame()

    def _create_config_frame(self) -> None:
        config_frame = ctk.CTkFrame(self.main_frame)
        config_frame.pack(fill="x", pady=(0, 15))

        config_label = ctk.CTkLabel(
            config_frame,
            text="Configuration",
            font=ctk.CTkFont(size=16, weight="bold"),
        )
        config_label.pack(anchor="w", padx=15, pady=(10, 5))

        input_frame = ctk.CTkFrame(config_frame, fg_color="transparent")
        input_frame.pack(fill="x", padx=15, pady=(0, 10))

        price_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        price_frame.pack(fill="x", pady=5)

        price_label = ctk.CTkLabel(price_frame, text="Price Threshold (USDT):", width=150, anchor="w")
        price_label.pack(side="left")

        self.price_threshold_var = ctk.StringVar(value="10.0")
        self.price_entry = ctk.CTkEntry(price_frame, textvariable=self.price_threshold_var, width=100)
        self.price_entry.pack(side="left", padx=(10, 0))

        timeframe_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        timeframe_frame.pack(fill="x", pady=5)

        timeframe_label = ctk.CTkLabel(timeframe_frame, text="Candle Timeframe:", width=150, anchor="w")
        timeframe_label.pack(side="left")

        self.timeframe_var = ctk.StringVar(value="1d")
        self.timeframe_combo = ctk.CTkComboBox(
            timeframe_frame,
            values=["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"],
            variable=self.timeframe_var,
            width=100,
        )
        self.timeframe_combo.pack(side="left", padx=(10, 0))

        info_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        info_frame.pack(fill="x", pady=5)

        info_label = ctk.CTkLabel(
            info_frame,
            text=f"Will fetch all available historical data up to {datetime.now().strftime('%Y-%m-%d')}",
            text_color="gray",
            font=ctk.CTkFont(size=12),
        )
        info_label.pack(anchor="w")

    def _create_progress_frame(self) -> None:
        progress_frame = ctk.CTkFrame(self.main_frame)
        progress_frame.pack(fill="x", pady=(0, 15))

        progress_label = ctk.CTkLabel(
            progress_frame,
            text="Progress",
            font=ctk.CTkFont(size=16, weight="bold"),
        )
        progress_label.pack(anchor="w", padx=15, pady=(10, 5))

        overall_frame = ctk.CTkFrame(progress_frame, fg_color="transparent")
        overall_frame.pack(fill="x", padx=15, pady=5)

        overall_label = ctk.CTkLabel(overall_frame, text="Overall Progress:", width=120, anchor="w")
        overall_label.pack(side="left")

        self.overall_progress = ctk.CTkProgressBar(overall_frame, width=350)
        self.overall_progress.pack(side="left", padx=(10, 10))
        self.overall_progress.set(0)

        self.overall_percent_label = ctk.CTkLabel(overall_frame, text="0%", width=50)
        self.overall_percent_label.pack(side="left")

        stage_frame = ctk.CTkFrame(progress_frame, fg_color="transparent")
        stage_frame.pack(fill="x", padx=15, pady=5)

        stage_label = ctk.CTkLabel(stage_frame, text="Current Stage:", width=120, anchor="w")
        stage_label.pack(side="left")

        self.stage_progress = ctk.CTkProgressBar(stage_frame, width=350)
        self.stage_progress.pack(side="left", padx=(10, 10))
        self.stage_progress.set(0)

        self.stage_percent_label = ctk.CTkLabel(stage_frame, text="0%", width=50)
        self.stage_percent_label.pack(side="left")

        self.stage_name_label = ctk.CTkLabel(
            progress_frame,
            text="Stage: Waiting to start...",
            font=ctk.CTkFont(size=12),
            text_color="gray",
        )
        self.stage_name_label.pack(anchor="w", padx=15, pady=(5, 10))

    def _create_status_frame(self) -> None:
        status_frame = ctk.CTkFrame(self.main_frame)
        status_frame.pack(fill="both", expand=True, pady=(0, 15))

        status_label = ctk.CTkLabel(
            status_frame,
            text="Status Log",
            font=ctk.CTkFont(size=16, weight="bold"),
        )
        status_label.pack(anchor="w", padx=15, pady=(10, 5))

        self.status_text = ctk.CTkTextbox(status_frame, height=150)
        self.status_text.pack(fill="both", expand=True, padx=15, pady=(0, 10))
        self.status_text.configure(state="disabled")

        self.log_message("Ready to start. Configure settings and click 'Start Process'.")

    def _create_control_frame(self) -> None:
        control_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        control_frame.pack(fill="x", pady=(0, 10))

        self.start_button = ctk.CTkButton(
            control_frame,
            text="Start Process",
            font=ctk.CTkFont(size=14, weight="bold"),
            height=40,
            width=200,
            command=self._on_start,
        )
        self.start_button.pack(side="left", padx=(0, 10))

        self.stop_button = ctk.CTkButton(
            control_frame,
            text="Stop",
            font=ctk.CTkFont(size=14),
            height=40,
            width=100,
            fg_color="red",
            hover_color="darkred",
            command=self._on_stop,
            state="disabled",
        )
        self.stop_button.pack(side="left", padx=(0, 10))

        self.create_chart_button = ctk.CTkButton(
            control_frame,
            text="Create Chart",
            font=ctk.CTkFont(size=14, weight="bold"),
            height=40,
            width=140,
            fg_color="#2b8a3e",
            hover_color="#237032",
            command=self._on_open_chart,
        )
        self.create_chart_button.pack(side="left", padx=(10, 0))

        self.clear_button = ctk.CTkButton(
            control_frame,
            text="Clear Log",
            font=ctk.CTkFont(size=14),
            height=40,
            width=100,
            fg_color="gray",
            hover_color="darkgray",
            command=self._on_clear_log,
        )
        self.clear_button.pack(side="right")

    def log_message(self, message: str, timestamp: bool = True) -> None:
        append_log_message(self.status_text, message, timestamp)

    def clear_log(self) -> None:
        self.status_text.configure(state="normal")
        self.status_text.delete("1.0", "end")
        self.status_text.configure(state="disabled")
        self.log_message("Log cleared.")

    def reset_progress(self, stage_text: str = "Stage: Starting...") -> None:
        self.overall_progress.set(0)
        self.stage_progress.set(0)
        self.overall_percent_label.configure(text="0%")
        self.stage_percent_label.configure(text="0%")
        self.stage_name_label.configure(text=stage_text)

    def set_fetch_running_state(self, running: bool, chart_generation: bool = False) -> None:
        self.start_button.configure(state="disabled" if running else "normal")
        self.stop_button.configure(state="normal" if running else "disabled")
        self.create_chart_button.configure(state="disabled" if running else "normal")
        self.price_entry.configure(state="disabled" if running else "normal")
        self.timeframe_combo.configure(state="disabled" if running else "normal")
        if chart_generation:
            self.start_button.configure(state="disabled")
