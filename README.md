# qlab-quant-research

Personal research environment for systematic factor strategies and backtesting.

## Current stage (Feb 2026)
- Robust daily OHLCV data pipeline using yfinance (incremental fetch, batch processing)
- Aggressive quality scanning & cleanup (zero-volume filtering, bad price removal, short-history junk)
- Parquet-based storage + basic reporting
- Preparing clean, filtered universe for backtesting

## Next priorities
1. Production-grade vectorized backtester (momentum/value baselines, slippage/costs, OOS splits)
2. Feature library (technicals, simple combos, low-turnover variants)
3. Regime detection & adaptive blending experiments

## Tech stack
- Python 3.11+ • pandas 2.x • polars • yfinance • pyarrow • tqdm
- DuckDB (exploratory) • vectorbt / custom engine (planned)
- Scripts + modular src/ layout (in progress)

## How to run (current pipeline)
```bash
# Quality scan + aggressive cleanup for daily data
python scripts/quality_scan_and_cleanup.py 1d

# Or for intraday (recent only)
python scripts/quality_scan_and_cleanup.py 1m