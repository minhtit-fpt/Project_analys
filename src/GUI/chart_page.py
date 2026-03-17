"""Chart generation page and chart result controls."""

from __future__ import annotations

from datetime import datetime

import customtkinter as ctk


class ChartPage(ctk.CTkFrame):
    """Page responsible for chart configuration and chart result actions."""

    def __init__(self, parent, on_generate, on_reopen_chart, on_export_csv):
        super().__init__(parent)
        self._on_generate = on_generate
        self._on_reopen_chart = on_reopen_chart
        self._on_export_csv = on_export_csv

        self._build_ui()

    def _build_ui(self) -> None:
        title_label = ctk.CTkLabel(
            self,
            text="Chart Generation",
            font=ctk.CTkFont(size=26, weight="bold"),
        )
        title_label.pack(anchor="w", padx=20, pady=(20, 12))

        self.content_frame = ctk.CTkFrame(self)
        self.content_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))

        self._build_timeframe_section(self.content_frame)
        self._build_time_range_section(self.content_frame)
        self._build_coin_selection_section(self.content_frame)

        action_frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
        action_frame.pack(fill="x", padx=15, pady=(10, 10))

        self.generate_button = ctk.CTkButton(
            action_frame,
            text="Generate Chart",
            font=ctk.CTkFont(size=15, weight="bold"),
            height=45,
            width=250,
            fg_color="#2b8a3e",
            hover_color="#237032",
            command=self._on_generate,
        )
        self.generate_button.pack(side="left")

        self.chart_summary_label = ctk.CTkLabel(
            action_frame,
            text="",
            font=ctk.CTkFont(size=12),
            text_color="gray",
        )
        self.chart_summary_label.pack(side="left", padx=(15, 0))

        self.results_frame = ctk.CTkFrame(self.content_frame)
        self.results_frame.pack(fill="x", padx=15, pady=(5, 10))

        self.results_info_label = ctk.CTkLabel(
            self.results_frame,
            text="No chart generated yet.",
            font=ctk.CTkFont(size=13),
            text_color="gray",
        )
        self.results_info_label.pack(anchor="w", padx=15, pady=(12, 8))

        buttons = ctk.CTkFrame(self.results_frame, fg_color="transparent")
        buttons.pack(fill="x", padx=15, pady=(0, 12))

        self.reopen_chart_button = ctk.CTkButton(
            buttons,
            text="Reopen Chart Window",
            font=ctk.CTkFont(size=14, weight="bold"),
            height=40,
            width=220,
            fg_color="#2962ff",
            hover_color="#1e53e5",
            command=self._on_reopen_chart,
        )
        self.reopen_chart_button.pack(side="left", padx=(0, 10))

        self.export_filtered_button = ctk.CTkButton(
            buttons,
            text="Export Data CSV",
            font=ctk.CTkFont(size=14),
            height=40,
            width=180,
            fg_color="#555555",
            hover_color="#444444",
            command=self._on_export_csv,
        )
        self.export_filtered_button.pack(side="left")

        self._update_chart_summary()

    def _build_timeframe_section(self, parent) -> None:
        section = ctk.CTkFrame(parent)
        section.pack(fill="x", pady=(15, 5), padx=15)

        label = ctk.CTkLabel(
            section,
            text="1. Candle Timeframe",
            font=ctk.CTkFont(size=15, weight="bold"),
        )
        label.pack(anchor="w", padx=15, pady=(10, 5))

        desc = ctk.CTkLabel(
            section,
            text="Select the candle interval for the chart.",
            font=ctk.CTkFont(size=12),
            text_color="gray",
        )
        desc.pack(anchor="w", padx=15)

        tf_frame = ctk.CTkFrame(section, fg_color="transparent")
        tf_frame.pack(fill="x", padx=15, pady=(5, 10))

        self.chart_timeframe_var = ctk.StringVar(value="1d")
        timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]

        for index, timeframe in enumerate(timeframes):
            button = ctk.CTkRadioButton(
                tf_frame,
                text=timeframe,
                variable=self.chart_timeframe_var,
                value=timeframe,
                font=ctk.CTkFont(size=13),
                command=self._update_chart_summary,
            )
            button.grid(row=0, column=index, padx=(0, 18), pady=5)

    def _build_time_range_section(self, parent) -> None:
        section = ctk.CTkFrame(parent)
        section.pack(fill="x", pady=(10, 5), padx=15)

        label = ctk.CTkLabel(
            section,
            text="2. Year and Time Range",
            font=ctk.CTkFont(size=15, weight="bold"),
        )
        label.pack(anchor="w", padx=15, pady=(10, 5))

        desc = ctk.CTkLabel(
            section,
            text="Select a year as the end boundary, then choose how far back to retrieve data.",
            font=ctk.CTkFont(size=12),
            text_color="gray",
            wraplength=550,
            justify="left",
        )
        desc.pack(anchor="w", padx=15)

        year_frame = ctk.CTkFrame(section, fg_color="transparent")
        year_frame.pack(fill="x", padx=15, pady=(8, 2))

        year_label = ctk.CTkLabel(
            year_frame,
            text="Year:",
            font=ctk.CTkFont(size=13, weight="bold"),
            width=120,
            anchor="w",
        )
        year_label.pack(side="left")

        current_year = datetime.now().year
        year_values = [str(year) for year in range(2020, current_year + 1)]

        self.specific_year_var = ctk.StringVar(value=str(current_year))
        self.specific_year_combo = ctk.CTkComboBox(
            year_frame,
            values=year_values,
            variable=self.specific_year_var,
            width=100,
            command=lambda _: self._update_chart_summary(),
        )
        self.specific_year_combo.pack(side="left", padx=(10, 0))

        year_hint = ctk.CTkLabel(
            year_frame,
            text="(end boundary for retrieved data)",
            font=ctk.CTkFont(size=11),
            text_color="gray",
        )
        year_hint.pack(side="left", padx=(10, 0))

        range_label = ctk.CTkLabel(
            section,
            text="How far back from the selected year:",
            font=ctk.CTkFont(size=13),
            anchor="w",
        )
        range_label.pack(anchor="w", padx=15, pady=(10, 2))

        range_frame = ctk.CTkFrame(section, fg_color="transparent")
        range_frame.pack(fill="x", padx=15, pady=(0, 10))

        self.time_range_var = ctk.StringVar(value="from_beginning")

        options = [
            ("from_beginning", "From beginning of year"),
            ("last_6m", "Last 6 months"),
            ("last_1y", "Last 1 year"),
            ("last_2y", "Last 2 years"),
            ("last_5y", "Last 5 years"),
        ]

        for index, (value, text) in enumerate(options):
            radio = ctk.CTkRadioButton(
                range_frame,
                text=text,
                variable=self.time_range_var,
                value=value,
                font=ctk.CTkFont(size=13),
                command=self._update_chart_summary,
            )
            radio.grid(row=index // 3, column=index % 3, sticky="w", padx=(0, 25), pady=4)

    def _build_coin_selection_section(self, parent) -> None:
        section = ctk.CTkFrame(parent)
        section.pack(fill="x", pady=(10, 5), padx=15)

        label = ctk.CTkLabel(
            section,
            text="3. Coin Selection",
            font=ctk.CTkFont(size=15, weight="bold"),
        )
        label.pack(anchor="w", padx=15, pady=(10, 5))

        desc = ctk.CTkLabel(
            section,
            text="Generate the chart for a single coin or all available coins.",
            font=ctk.CTkFont(size=12),
            text_color="gray",
        )
        desc.pack(anchor="w", padx=15)

        coin_frame = ctk.CTkFrame(section, fg_color="transparent")
        coin_frame.pack(fill="x", padx=15, pady=(5, 5))

        self.coin_scope_var = ctk.StringVar(value="all")

        all_button = ctk.CTkRadioButton(
            coin_frame,
            text="All available coins",
            variable=self.coin_scope_var,
            value="all",
            font=ctk.CTkFont(size=13),
            command=self._on_coin_scope_changed,
        )
        all_button.grid(row=0, column=0, sticky="w", padx=(0, 30), pady=4)

        single_button = ctk.CTkRadioButton(
            coin_frame,
            text="Single coin",
            variable=self.coin_scope_var,
            value="single",
            font=ctk.CTkFont(size=13),
            command=self._on_coin_scope_changed,
        )
        single_button.grid(row=0, column=1, sticky="w", padx=(0, 30), pady=4)

        self.single_coin_frame = ctk.CTkFrame(section, fg_color="transparent")
        self.single_coin_frame.pack(fill="x", padx=15, pady=(0, 10))

        coin_label = ctk.CTkLabel(
            self.single_coin_frame,
            text="Symbol:",
            font=ctk.CTkFont(size=13),
            width=55,
        )
        coin_label.pack(side="left")

        self.single_coin_var = ctk.StringVar(value="BTC/USDT")
        self.single_coin_entry = ctk.CTkEntry(
            self.single_coin_frame,
            textvariable=self.single_coin_var,
            width=160,
            placeholder_text="e.g. BTC/USDT",
        )
        self.single_coin_entry.pack(side="left", padx=(10, 0))

        coin_hint = ctk.CTkLabel(
            self.single_coin_frame,
            text="(Binance Futures USDT-M symbol)",
            font=ctk.CTkFont(size=11),
            text_color="gray",
        )
        coin_hint.pack(side="left", padx=(10, 0))

        self.single_coin_frame.pack_forget()

    def _on_coin_scope_changed(self) -> None:
        if self.coin_scope_var.get() == "single":
            self.single_coin_frame.pack(fill="x", padx=15, pady=(0, 10))
        else:
            self.single_coin_frame.pack_forget()
        self._update_chart_summary()

    def _update_chart_summary(self) -> None:
        timeframe = self.chart_timeframe_var.get()
        time_range = self.time_range_var.get()
        reference_year = self.specific_year_var.get()
        scope = self.coin_scope_var.get()

        range_labels = {
            "from_beginning": f"Jan-Dec {reference_year}",
            "last_6m": f"last 6 months up to end of {reference_year}",
            "last_1y": f"last 1 year up to end of {reference_year}",
            "last_2y": f"last 2 years up to end of {reference_year}",
            "last_5y": f"last 5 years up to end of {reference_year}",
        }
        range_text = range_labels.get(time_range, time_range)
        coin_text = "all coins" if scope == "all" else self.single_coin_var.get()
        self.chart_summary_label.configure(
            text=f"Will generate {timeframe} candles for {range_text} - {coin_text}"
        )

    def get_generation_params(self) -> dict:
        coin_scope = self.coin_scope_var.get()
        single_coin = self.single_coin_var.get().strip() if coin_scope == "single" else None
        return {
            "timeframe": self.chart_timeframe_var.get(),
            "time_range": self.time_range_var.get(),
            "reference_year": self.specific_year_var.get(),
            "coin_scope": coin_scope,
            "single_coin": single_coin,
        }

    def set_generation_running(self, running: bool) -> None:
        self.generate_button.configure(state="disabled" if running else "normal")

    def set_results_info(self, n_coins: int, n_rows: int) -> None:
        self.results_info_label.configure(
            text=f"{n_coins} coin(s) - {n_rows} total candles"
        )
