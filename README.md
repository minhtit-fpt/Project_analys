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
├── Binance_2019_to_2026-01-23.xlsx  (Each coin = 1 sheet)
├── Binance_2020_to_2026-01-23.xlsx  (Each coin = 1 sheet)
├── Binance_2021_to_2026-01-23.xlsx  (Each coin = 1 sheet)
└── ...
```

**Excel Format:**
- Each file represents one listing year
- Each **sheet** inside = one coin (e.g., sheet "DOGE_USDT", "SHIB_USDT")
- All historical data for that coin in its dedicated sheet

## CSV Columns
- `symbol`: Trading pair (e.g., DOGE/USDT)
- `date`: Human-readable datetime
- `timestamp`: Unix timestamp (ms)
- `open`, `high`, `low`, `close`: OHLC prices
- `volume`: Trading volume
- `MA_7`, `MA_25`, `MA_99`: Moving averages

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
Instead of saving one file per coin, data is grouped by **listing year** into Excel files:
- All coins listed in 2019 → `Binance_2019_to_YYYY-MM-DD.xlsx`
  - Sheet 1: BTC_USDT
  - Sheet 2: ETH_USDT
  - ...
- All coins listed in 2020 → `Binance_2020_to_YYYY-MM-DD.xlsx`
  - Sheet 1: DOGE_USDT
  - Sheet 2: SHIB_USDT
  - ...

**Each coin gets its own sheet** within the year's Excel file.

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
