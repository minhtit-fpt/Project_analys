"""HTTP bridge for rendering and refreshing TradingView Lightweight Charts."""

from __future__ import annotations

import json
import os
import socket
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable, Optional
from urllib.parse import parse_qs, urlparse


class _ChartRequestHandler(BaseHTTPRequestHandler):
    """Serve chart assets and lightweight data API for incremental updates."""

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path in ("/", "/chart"):
            self._serve_chart_html()
        elif parsed.path == "/api/latest_data":
            self._handle_latest_data(parsed)
        elif parsed.path == "/lightweight-charts.standalone.production.js":
            self._serve_static_asset("lightweight-charts.standalone.production.js", "application/javascript")
        elif parsed.path == "/chart.css":
            self._serve_static_asset("chart.css", "text/css")
        else:
            self.send_error(404)

    def _serve_static_asset(self, filename: str, content_type: str) -> None:
        assets_dir = self.server.assets_dir
        filepath = os.path.join(assets_dir, filename)
        if not os.path.isfile(filepath):
            self.send_error(404)
            return

        with open(filepath, "rb") as file_handle:
            data = file_handle.read()

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_chart_html(self) -> None:
        html_bytes = self.server.chart_html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html_bytes)))
        self.end_headers()
        self.wfile.write(html_bytes)

    def _handle_latest_data(self, parsed) -> None:
        qs = parse_qs(parsed.query)
        symbol = qs.get("symbol", [""])[0]
        since_str = qs.get("since", ["0"])[0]

        records = []
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
            except Exception as error:
                print(f"[ChartServer] Error fetching latest data: {error}")

        body = json.dumps(records).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


class _ChartHTTPServer(HTTPServer):
    """HTTP server that stores runtime chart state."""

    chart_html: str = ""
    assets_dir: str = ""
    data_fetcher_factory = None


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


class ChartBridge:
    """Owns chart server lifecycle and browser launch behavior."""

    def __init__(self, assets_dir: str):
        self.assets_dir = assets_dir
        self._chart_server: Optional[_ChartHTTPServer] = None
        self._chart_url: Optional[str] = None

    def launch_chart(
        self,
        chart_json: str,
        data_fetcher_factory: Callable[[], object],
    ) -> str:
        """Serve chart HTML and open it in the system browser."""
        html_path = os.path.join(self.assets_dir, "chart.html")
        with open(html_path, "r", encoding="utf-8") as file_handle:
            html_template = file_handle.read()

        port = _find_free_port()
        safe_chart_json = chart_json.replace("</", r"<\\/")

        html_content = html_template.replace(
            "const initialData = INITIAL_DATA;",
            f"const initialData = {safe_chart_json};",
        ).replace(
            "const API_BASE = null;",
            f'const API_BASE = "http://127.0.0.1:{port}";',
        )

        self.stop()

        server = _ChartHTTPServer(("127.0.0.1", port), _ChartRequestHandler)
        server.chart_html = html_content
        server.assets_dir = self.assets_dir
        server.data_fetcher_factory = data_fetcher_factory
        self._chart_server = server
        self._chart_url = f"http://127.0.0.1:{port}/chart"

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        webbrowser.open(self._chart_url)
        return self._chart_url

    def reopen(self) -> None:
        """Reopen existing chart browser tab if bridge is already active."""
        if self._chart_url:
            webbrowser.open(self._chart_url)

    def stop(self) -> None:
        """Shutdown active chart bridge server if present."""
        if self._chart_server is None:
            return

        server = self._chart_server
        self._chart_server = None
        self._chart_url = None
        try:
            server.shutdown()
        except Exception:
            pass
        finally:
            try:
                server.server_close()
            except Exception:
                pass
