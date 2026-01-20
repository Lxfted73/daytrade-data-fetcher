"""
ETF/Stock Data Fetcher - Incremental Updater with Duplicate Removal & Stable Files
─────────────────────────────────────────────────────────────────────────────────

Purpose:
    Fetches OHLCV bars from Yahoo Finance using yfinance.
    - Incremental: only fetches/appends new data since last saved date
    - Stable filenames: one Parquet file per ticker + interval
    - Removes exact duplicates and resolves timestamp conflicts (keeps highest volume)
    - Smart 1m handling: auto-uses period="7d" when no period/start is appropriate
    - Weekly fetch caching to reduce redundant API calls
    - Rate limiting (2 calls/sec)
    - Continuity warnings for obvious gaps in daily/weekly data

Dependencies:
    - yfinance
    - pandas
    - ratelimit
    - pathlib

Usage examples are at the bottom of the file under if __name__ == "__main__":

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
            print(f"{ticker} ({interval}) → {first} to {last}  |  {len(data)} rows")
            return data

        except Exception as e:
            print(f"Yahoo failed for {ticker} ({interval}): {e}")
            return None


class DataFetcher:
    def __init__(self, save_dir: str = "./data/yfinance"):
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
        """
        Fetch OHLCV data for one ticker.
        If save=True and file exists → incremental fetch + append + clean.
        """
        cache_key = f"{ticker}_{interval}_{period or start}"

        if cache_key in self.cache and not self._should_fetch_this_week():
            print(f"Cache hit ({interval}): {ticker}")
            return self.cache[cache_key]

        print(f"Fetching {interval} data: {ticker} "
              f"({'period=' + period if period else 'start=' + start})")

        # ── Incremental logic ───────────────────────────────────────────────
        effective_start = start
        existing_last_date = None
        path = self._get_file_path(ticker, interval)

        if save and path.exists():
            try:
                old_df = pd.read_parquet(path)
                if not old_df.empty:
                    existing_last_date = old_df.index.max()
                    next_day = (existing_last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    effective_start = next_day
                    print(f"Existing ends {existing_last_date.date()} → fetching from {next_day}")
            except Exception as e:
                print(f"Could not read {path}: {e} → doing full fetch")

        # Smart period for intraday
        period_for_fetch = period
        if period is None and interval in self.source.INTRADAY_INTERVALS:
            if interval == "1m":
                period_for_fetch = "7d"
                effective_start = None
                print("1m → using period='7d' (Yahoo safe limit)")
            elif effective_start and (datetime.now() - pd.to_datetime(effective_start)).days > 60:
                period_for_fetch = "60d"
                effective_start = None
                print(f"Intraday → using period='60d' for older requested start")

        data = self.source.get(
            ticker, effective_start, period_for_fetch, interval, use_earliest_if_unavailable
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
                print(f"Loaded existing: {len(old_df)} rows (last: {old_df.index.max().date() if not old_df.empty else 'empty'})")
            except Exception as e:
                print(f"Failed to read {path}: {e} → treating as new")

        if old_df.empty:
            print(f"Creating new file: {path}")
            new_df.sort_index().to_parquet(path, index=True)
            print(f"Saved {len(new_df)} rows")
            return

        print(f"Appending {len(new_df)} rows")

        combined = pd.concat([old_df, new_df]).sort_index()

        before = len(combined)
        combined = combined.drop_duplicates(keep='last')
        exact_dupes = before - len(combined)

        if combined.index.duplicated().any():
            print(f"Resolving timestamp conflicts ({ticker}) → keeping highest volume")
            combined = combined.loc[combined.groupby(combined.index)['Volume'].idxmax()]

        after = len(combined)

        if exact_dupes > 0:
            print(f"Removed {exact_dupes} exact duplicates")
        if after < before - exact_dupes:
            print(f"Resolved {before - exact_dupes - after} timestamp conflicts")

        # Gap warning
        if interval in ["1d", "1wk"] and len(combined) > 20:
            span_days = (combined.index.max() - combined.index.min()).days
            rough_expected = span_days / (7 if interval == "1wk" else 1) * 0.7
            if len(combined) < rough_expected * 0.6:
                print(f"Warning: possible gaps in {ticker} ({interval}) "
                      f"({len(combined)} bars over ~{span_days} days)")

        combined.to_parquet(path, index=True)
        print(f"Updated: {path} ({len(combined)} rows total)")


# ────────────────────────────────────────────────
# Example usage (copy-paste these snippets wherever you need them)
# ────────────────────────────────────────────────
if __name__ == "__main__":
    # Quick one-off examples — feel free to delete or comment out

    fetcher = DataFetcher(save_dir="./data/yfinance")

    # Example 1: Get/update daily data (incremental if file exists)
    print("\nDaily SPY (will append only new days if already saved)")
    df_daily = fetcher.get("SPY", interval="1d", save=True)
    if df_daily is not None:
        print(df_daily.tail(3))

    # Example 2: Recent 1-minute bars (always uses ~last 7 days)
    print("\nRecent 1-minute data for QQQ")
    df_1m = fetcher.get("QQQ", interval="1m", save=True)
    if df_1m is not None:
        print(df_1m.tail(5))

    # Example 3: Long weekly history (or update)
    print("\nWeekly VTI from 2010 onwards")
    df_weekly = fetcher.get("VTI", start="2010-01-01", interval="1wk", save=True)
    if df_weekly is not None:
        print(df_weekly.tail(4))