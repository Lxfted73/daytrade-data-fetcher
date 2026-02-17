# QLab – U.S. Equities Data Pipeline & Intraday Behavior Explorer

**Status (February 2026):**  
Working end-to-end pipeline from ticker ingestion → daily + recent intraday data collection → DuckDB analytical layer → early intraday directional bias visualization (up-bar % heatmaps across timeframes)

QLab is a lightweight, modular research environment focused on **U.S. equities** with the following goals:

- Collect and maintain clean OHLCV history (daily + short lookback intraday)
- Enable fast DuckDB-based exploration and feature generation
- Identify intraday patterns suitable for scalping/day-trading
- Build toward a production-grade **factor / signal research & backtesting stack**

Current emphasis:  
robust daily backtesting foundation + quick univariate signal testing + intuition-building around intraday bar directionality

## Project Roadmap – Recommended Order (2026)

1. **Robust Backtesting & Single-Factor Validation** ← **active priority**  
   Goal: trustworthy vectorized/event-driven backtest engine  
   - realistic transaction costs (spread + commission + slippage)  
   - proper out-of-sample evaluation  
   - standard metrics: IC, IR, SR, turnover, Calmar, max drawdown, etc.  
   → Quick wins: classic momentum (12-1), value (EP/BP), low-vol, quality

2. **Feature Engineering & Signal Exploration**  
   Goal: rapidly test dozens → hundreds of candidate alphas  
   - technicals (momentum, mean-reversion, volatility, volume anomalies)  
   - simple combinations / ratios  
   - proxy signals from alternative data (when available)  
   → Learn what actually looks "alpha-like": predictability, low turnover, persistence, cross-sectional rank stability

3. **Regime-Adaptive Multi-Signal Blending**  
   Goal: dynamic signal selection / weighting + final multi-factor portfolio  
   - detect regimes (volatility, trend, liquidity, macro proxy, etc.)  
   - combine 3–5 strongest alphas with regime-conditioned weights  
   → Demonstrate edge that survives regime changes

## Current Capabilities

- **Ticker ingestion & batching**  
  NASDAQ + NYSE lists → cleaned, deduplicated, sorted → split into ~400-ticker batches

- **Data fetching (Yahoo Finance)**  
  - Daily / weekly: full history or incremental append  
  - Intraday (1m–240m): recent snapshot only (~7–8 day lookback limit)  
  - Duplicate removal + timestamp conflict resolution (highest volume wins)  
  - Gap / continuity warnings for daily+ data

- **Storage layout**


## Fodler Structure
├── 01_pipeline_to_duckdb.py           # main orchestrator
├── data/
│   ├── yfinance/
│   │   ├── 1m/ *.parquet
│   │   ├── 1d/ *.parquet
│   │   └── batches/ *.txt
│   └── qlab/data/qlab.duckdb
├── heatmaps/                          # generated PNG heatmaps
├── qlab/
│   └── resources/                     # source CSV ticker lists
└── supporting scripts:
    ├── data_batch_downloader.py
    ├── data_duckdb.py
    ├── data_fetcher.py
    ├── data_standardize_parquets.py
    ├── processing_EV_duckDB.py
    ├── processing_EV.py               # pandas-only fallback version
    └── ...

    