# Binance Futures Historical Data Fetcher

## Overview
Production-ready Python script for fetching historical OHLCV data from Binance Futures (USDT-M) with advanced filtering and grouping capabilities.

## Features
- ✅ **OOP Design**: Clean class-based architecture
- ✅ **Rate Limit Protection**: Built-in `ccxt` rate limiting + retry logic
- ✅ **Smart Filtering**: Only fetches coins with price < 10 USDT
- ✅ **Historical Data**: Fetches from listing date to present
- ✅ **Grouping by Year**: Saves data grouped by listing year (not by coin)
- ✅ **Moving Averages**: Calculates MA7, MA25, MA50, MA99, MA200
- ✅ **Progress Tracking**: TQDM progress bar + detailed logging
- ✅ **Error Handling**: Robust exception handling for API failures

## Requirements
```bash
pip install -r requirements.txt
```

## Usage
```bash
python main.py
```

## Output Structure
```
data/binance_future/
├── Binance_2019_to_2026-01-23.csv
├── Binance_2020_to_2026-01-23.csv
├── Binance_2021_to_2026-01-23.csv
└── ...
```

## CSV Columns
- `symbol`: Trading pair (e.g., DOGE/USDT)
- `date`: Human-readable datetime
- `timestamp`: Unix timestamp (ms)
- `open`, `high`, `low`, `close`: OHLC prices
- `volume`: Trading volume
- `MA_7`, `MA_25`, `MA_50`, `MA_99`, `MA_200`: Moving averages

## Configuration
Modify in `main()` function:
```python
fetcher = BinanceFutureFetcher(
    price_threshold=10.0,  # Max price in USDT
    timeframe='1d'         # Candle timeframe
)
```

## Logging
Logs are saved to: `logs/binance_fetcher_YYYYMMDD.log`

## Architecture
```
BinanceFutureFetcher
├── get_markets()           → Filter coins by price
├── _detect_listing_date()  → Find first available candle
├── fetch_candles()         → Fetch historical OHLCV data
├── process_and_save()      → Main execution flow
└── _save_by_year()         → Group & save by listing year
```

## Grouping Logic
Instead of saving one file per coin, data is grouped by **listing year**:
- All coins listed in 2019 → `Binance_2019_to_YYYY-MM-DD.csv`
- All coins listed in 2020 → `Binance_2020_to_YYYY-MM-DD.csv`
- etc.

## Error Handling
- **Rate Limit Exceeded**: Auto-retry with exponential backoff
- **Exchange Unavailable**: 30-second cooldown before retry
- **Missing Data**: Logged and skipped gracefully

## Notes
- Binance Futures launched around 2019, so data starts from 2019-01-01
- Daily timeframe (`1d`) is recommended for historical analysis
- API calls are rate-limited to prevent bans
- Progress is logged to both console and file

## License
MIT License - Use at your own risk for trading decisions.
