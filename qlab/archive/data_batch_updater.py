"""
ETF/Stock Data Fetcher - Professional Incremental Updater for Yahoo Finance
─────────────────────────────────────────────────────────────────────────────

Full Summary (January 2026 version):

This script provides a robust, incremental way to download and maintain historical OHLCV bar data
from Yahoo Finance using the yfinance library. It is designed for long-term data collection, backtesting,
and monitoring, with special care for reliability and data integrity.

Core Features:
• Incremental updates: Only fetches new bars since the last saved date (avoids redundant downloads)
• Stable filenames: One Parquet file per ticker + interval (e.g. data/1d/SPY.parquet)
• Automatic duplicate removal: Cleans exact duplicates and timestamp conflicts on append
  - Keeps the row with highest volume when timestamps overlap
• Smart 1-minute handling: When interval="1m" and no period given → auto-uses period="7d"
  - Ignores old start dates to prevent Yahoo's ~7-8 day limit errors
  - Still respects explicit period="3d"/"5d"/etc. if provided
• Weekly caching: Limits repeated API calls within the same ISO week
• Rate limiting: Gentle 2 calls/second via ratelimit decorator
• Continuity warnings: Flags possible gaps in daily/weekly data
• Auto folder creation: Organizes data by interval (1m/, 1d/, 1wk/, etc.)
• Error resilience: Graceful handling of empty responses, read/write failures

Main Components:
• DataSource (ABC) → YahooFinanceSource: Core fetch logic with ratelimit & fallback
• DataFetcher: High-level manager with caching, incremental logic, append+clean, saving

Usage Patterns:
1. First run: full history (e.g. start="2000-01-01" or period="max")
2. Daily/weekly runs: fetches only missing bars → appends to existing files
3. 1m monitoring: auto short recent snapshot (7 days default) → appends recent activity

Dependencies:
• yfinance
• pandas
• ratelimit
• pathlib

Best Practices / Notes:
• Run frequently (daily) for continuous data
• Delete .parquet file to force full re-fetch for a ticker/interval
• 1m is limited to ~7-8 days max by Yahoo — for deeper history use Polygon.io/Databento
• Parquet format preserves datetime index and is efficient for large datasets
• Continuity warnings help spot Yahoo skips — can add ffill later if desired

Last modified: January 2026
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod
from ratelimit import limits, sleep_and_retry


class DataSource(ABC):
    @abstractmethod
    def get(self, ticker, start, period, interval, use_earliest_if_unavailable=False):
        pass


class YahooFinanceSource(DataSource):
    INTRADAY_INTERVALS = {"1m", "2m", "5m", "15m", "30m", "60m", "90m"}

    @sleep_and_retry
    @limits(calls=2, period=1)
    def get(self, ticker, start, period, interval, use_earliest_if_unavailable=False):
        try:
            kwargs = {
                "tickers": ticker,
                "interval": interval,
                "progress": False,
                "auto_adjust": False
            }

            if period:
                kwargs["period"] = period
            else:
                kwargs["start"] = start

            data = yf.download(**kwargs)

            if data.empty and use_earliest_if_unavailable:
                print(f"Falling back to period='max' for {ticker} ({interval})")
                data = yf.download(period="max", **kwargs)

            if data.empty:
                print(f"No data returned for {ticker} ({interval})")
                return None

            if isinstance(data.columns, pd.MultiIndex):
                data.columns = [col[0] for col in data.columns]

            first = data.index.min().strftime('%Y-%m-%d %H:%M:%S%z') if not data.empty else "N/A"
            last  = data.index.max().strftime('%Y-%m-%d %H:%M:%S%z') if not data.empty else "N/A"
            print(f"{ticker} ({interval}) data: {first} → {last}  |  rows: {len(data)}")
            return data

        except Exception as e:
            print(f"Yahoo failed for {ticker} ({interval}): {e}")
            return None


class DataFetcher:
    def __init__(self, save_dir: str = "./data"):
        self.source = YahooFinanceSource()
        self.cache = {}
        self.last_fetch_week = None
        self.save_root = Path(save_dir)
        self.save_root.mkdir(parents=True, exist_ok=True)

    def _get_current_week(self):
        return datetime.now().isocalendar()[:2]

    def _should_fetch_this_week(self):
        current_week = self._get_current_week()
        return self.last_fetch_week is None or current_week != self.last_fetch_week

    def get(self, ticker: str, start='2000-01-01', period=None, interval="1wk",
            use_earliest_if_unavailable=True, save=True):

        cache_key_part = f"{ticker}_{interval}_{period or start}"
        cache_key = f"etf/{cache_key_part}"

        if cache_key in self.cache and not self._should_fetch_this_week():
            print(f"Cache hit ({interval}, same week): {ticker}")
            return self.cache[cache_key]

        print(f"Fetching {interval} data: {ticker} "
              f"({'period=' + period if period else 'start=' + start})")

        # ── Smart 1m handling: auto short period if none given ──────────────────
        effective_start = start
        effective_period = period

        if interval == "1m":
            if period is None:
                effective_period = "7d"
                effective_start = None  # ignore start for auto mode
                print(f"1m → auto-using period='7d' (Yahoo-safe recent window)")
            else:
                # User specified period → use it, warn if long
                if "d" in str(period) and int(str(period)[:-1]) > 10:
                    print(f"Note: period='{period}' for 1m → may be limited by Yahoo (~7-8d max)")

        # ── Incremental fetch for non-1m (or if period not forced) ──────────────
        existing_last_date = None
        path = self._get_file_path(ticker, interval)

        if save and path.exists() and interval != "1m":  # skip incremental for 1m auto mode
            try:
                old_df = pd.read_parquet(path)
                if not old_df.empty:
                    existing_last_date = old_df.index.max()
                    next_day = (existing_last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    if effective_period is None:  # only override start if not using period
                        effective_start = next_day
                        print(f"Existing ends {existing_last_date.date()} → fetching from {next_day}")
            except Exception as e:
                print(f"Could not read {path}: {e} → full fetch")

        data = self.source.get(
            ticker,
            start=effective_start,
            period=effective_period,
            interval=interval,
            use_earliest_if_unavailable=use_earliest_if_unavailable
        )

        if data is None or data.empty:
            print(f"No new data for {ticker} ({interval})")
            return None

        self.cache[cache_key] = data
        self.last_fetch_week = self._get_current_week()

        if save:
            self._append_and_save(ticker, data, interval, existing_last_date)

        return data

    def _get_file_path(self, ticker: str, interval: str) -> Path:
        interval_clean = interval.replace(" ", "").lower()
        folder = self.save_root / interval_clean
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{ticker.upper()}.parquet"

    def _append_and_save(self, ticker: str, new_df: pd.DataFrame, interval: str, existing_last_date=None):
        path = self._get_file_path(ticker, interval)

        old_df = pd.DataFrame()
        if path.exists():
            try:
                old_df = pd.read_parquet(path)
                print(f"Loaded existing: {len(old_df)} rows (last: {old_df.index.max().date()})")
            except Exception as e:
                print(f"Failed to read {path}: {e} → treating as new")

        if old_df.empty:
            print(f"Creating new file for {ticker} ({interval})")
            new_df.sort_index().to_parquet(path, index=True)
            print(f"Saved: {path} ({len(new_df)} rows)")
            return

        # ── Append + clean duplicates ──────────────────────────────────────────
        print(f"Appending {len(new_df)} new rows")

        combined = pd.concat([old_df, new_df])
        combined = combined.sort_index()

        # Remove exact duplicates
        before = len(combined)
        combined = combined.drop_duplicates(keep='last')
        exact_dupes = before - len(combined)

        # Resolve timestamp conflicts (keep highest volume row)
        if combined.index.duplicated().any():
            print(f"Resolving timestamp conflicts for {ticker} (highest volume)")
            combined = combined.loc[combined.groupby(combined.index)['Volume'].idxmax()]

        after = len(combined)

        if exact_dupes > 0:
            print(f"Removed {exact_dupes} exact duplicate rows")
        if after < before - exact_dupes:
            print(f"Resolved {before - exact_dupes - after} timestamp conflicts")

        # Continuity warning for daily/weekly
        if interval in ["1d", "1wk"] and len(combined) > 20:
            span_days = (combined.index.max() - combined.index.min()).days
            rough_expected = span_days / (7 if interval == "1wk" else 1) * 0.7
            if len(combined) < rough_expected * 0.6:
                print(f"Warning: possible gaps in {ticker} ({interval}) "
                      f"({len(combined)} bars over ~{span_days} days)")

        combined.to_parquet(path, index=True)
        print(f"Updated: {path} (now {len(combined)} rows)")

    def update_all(self):
        current = self._get_current_week()
        if self.last_fetch_week != current:
            print("New week detected → fresh fetches allowed")
            self.last_fetch_week = current


# ────────────────────────────────────────────────
if __name__ == "__main__":
    fetcher = DataFetcher(save_dir="./data/yfinance")

    tickers = ["SPY", "QQQ", "VTI", "BND"]

    print("\n=== Recent 1-minute update (auto 7d) ===")
    for t in tickers:
        df = fetcher.get(t, interval="1m", save=True)
        if df is not None:
            print(f"\n{t} last few rows:")
            print(df.tail(3))
            print("-" * 70)

    print("\n=== Weekly long history or update ===")
    for t in tickers:
        df = fetcher.get(t, start="2010-01-01", interval="1wk", save=True)
        if df is not None:
            print(f"\n{t} last 5 weeks:")
            print(df.tail(5))
            print("-" * 70)

    print("\n=== Daily last year or incremental ===")
    df_spy = fetcher.get("SPY", period="1y", interval="1d", save=True)
    if df_spy is not None:
        print("SPY last 5 days:")
        print(df_spy.tail(5))