# -*- coding: utf-8 -*-
"""
Config Package Initialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Provides centralized configuration management for Binance Future ETL.
"""

from config.settings import Settings, get_settings

__all__ = ["Settings", "get_settings"]
