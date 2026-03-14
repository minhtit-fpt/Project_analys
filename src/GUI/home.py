"""Home dashboard page."""

from __future__ import annotations

import customtkinter as ctk


class HomePage(ctk.CTkFrame):
    """Landing page with quick navigation actions and status summary."""

    def __init__(self, parent, on_open_data, on_open_chart):
        super().__init__(parent)
        self._on_open_data = on_open_data
        self._on_open_chart = on_open_chart

        self._build_ui()

    def _build_ui(self) -> None:
        container = ctk.CTkFrame(self)
        container.pack(fill="both", expand=True, padx=20, pady=20)

        title = ctk.CTkLabel(
            container,
            text="Binance Futures Dashboard",
            font=ctk.CTkFont(size=30, weight="bold"),
        )
        title.pack(anchor="w", pady=(15, 8), padx=20)

        subtitle = ctk.CTkLabel(
            container,
            text="Manage data collection and chart generation from one place.",
            font=ctk.CTkFont(size=14),
            text_color="gray",
        )
        subtitle.pack(anchor="w", padx=20, pady=(0, 20))

        summary = ctk.CTkFrame(container)
        summary.pack(fill="x", padx=20, pady=(0, 20))

        self.system_status_label = ctk.CTkLabel(
            summary,
            text="System status: Ready",
            font=ctk.CTkFont(size=14, weight="bold"),
        )
        self.system_status_label.pack(anchor="w", padx=15, pady=(12, 4))

        self.storage_status_label = ctk.CTkLabel(
            summary,
            text="Storage status: Idle",
            font=ctk.CTkFont(size=13),
            text_color="gray",
        )
        self.storage_status_label.pack(anchor="w", padx=15, pady=(0, 12))

        actions = ctk.CTkFrame(container, fg_color="transparent")
        actions.pack(fill="x", padx=20, pady=(0, 10))

        data_button = ctk.CTkButton(
            actions,
            text="Open Data Page",
            width=220,
            height=44,
            font=ctk.CTkFont(size=14, weight="bold"),
            command=self._on_open_data,
        )
        data_button.pack(side="left", padx=(0, 10))

        chart_button = ctk.CTkButton(
            actions,
            text="Open Chart Page",
            width=220,
            height=44,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="#2b8a3e",
            hover_color="#237032",
            command=self._on_open_chart,
        )
        chart_button.pack(side="left")

    def set_system_status(self, text: str) -> None:
        self.system_status_label.configure(text=f"System status: {text}")

    def set_storage_status(self, text: str) -> None:
        self.storage_status_label.configure(text=f"Storage status: {text}")
