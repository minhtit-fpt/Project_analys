"""Reusable GUI components and helpers."""

from __future__ import annotations

from datetime import datetime

import customtkinter as ctk


def percent_text(value: float) -> str:
    """Return a progress value in percentage text."""
    return f"{int(value * 100)}%"


def append_log_message(textbox: ctk.CTkTextbox, message: str, timestamp: bool = True) -> None:
    """Append a timestamped message to a read-only text widget."""
    textbox.configure(state="normal")
    if timestamp:
        time_str = datetime.now().strftime("%H:%M:%S")
        textbox.insert("end", f"[{time_str}] {message}\n")
    else:
        textbox.insert("end", f"{message}\n")
    textbox.see("end")
    textbox.configure(state="disabled")
