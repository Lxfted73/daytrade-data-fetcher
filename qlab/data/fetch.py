# src/qlab/data/fetch.py
"""
Unified Yahoo Finance data fetcher module – SPEED-OPTIMIZED VERSION.

Key improvements:
- Bulk multi-ticker downloads (5–20× faster for batches)
- Skip fetch if file exists and recently modified
- Polite delays only between bulk calls (not per ticker)
- Optimized duplicate/timestamp conflict handling
- Zstandard compression for Parquet

Usage:
    from qlab.data.fetch import DataFetcher

    fetcher = DataFetcher(save_dir="data/yfinance")
    fetcher.get("SPY", interval="1d", save=True)           # incremental
    fetcher.fetch_batch(1, 20, interval="1d")             # batch mode – now much faster
"""

from __future__ import annotations

import yfinance as yf
import pandas as pd
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod
from ratelimit import limits, sleep_and_retry
from tqdm.auto import tqdm
import time
import random
import os


class DataSource(ABC):
    """Abstract base for data providers."""

    @abstractmethod
    def get(
        self,
        tickers: str | list[str],
        start: str | None = None,
        period: str | None = None,
        interval: str = "1d",
        use_earliest_if_unavailable: bool = False,
    ) -> pd.DataFrame | None:
        pass


class YahooFinanceSource(DataSource):
    """Yahoo Finance data source with rate limiting."""

    INTRADAY_INTERVALS = {"1m", "2m", "3m", "5m", "15m", "30m", "60m", "90m", "1h"}

    @sleep_and_retry
    @limits(calls=2, period=1)
    def get(
        self,
        tickers: str | list[str],
        start: str | None = None,
        period: str | None = None,
        interval: str = "1d",
        use_earliest_if_unavailable: bool = False,
    ) -> pd.DataFrame | None:
        try:
            if isinstance(tickers, list):
                tickers = " ".join(tickers)

            kwargs = {
                "tickers": tickers,
                "interval": interval,
                "progress": False,
                "auto_adjust": False,
                "threads": True,
                "timeout": 20,
            }

            if period:
                kwargs["period"] = period
            elif start:
                kwargs["start"] = start
            else:
                kwargs["period"] = "max"

            data = yf.download(**kwargs)

            if data.empty and use_earliest_if_unavailable:
                kwargs["period"] = "max"
                data = yf.download(**kwargs)

            if data.empty:
                return None

            return data

        except Exception as e:
            print(f"Fetch failed ({tickers[:60]}...): {e}")
            return None


class DataFetcher:
    """Main fetcher: incremental + bulk batch support – optimized for speed."""

    def __init__(self, save_dir: str = "data/yfinance"):
        self.source = YahooFinanceSource()
        self.save_root = Path(save_dir)
        self.save_root.mkdir(parents=True, exist_ok=True)
        self.last_fetch_week: tuple[int, int] | None = None

    def _get_current_week(self) -> tuple[int, int]:
        return datetime.now().isocalendar()[:2]

    def _should_fetch_this_week(self) -> bool:
        current = self._get_current_week()
        if self.last_fetch_week != current:
            self.last_fetch_week = current
            return True
        return False

    def _get_file_path(self, ticker: str, interval: str) -> Path:
        folder = self.save_root / interval.lower().replace(" ", "")
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{ticker.upper()}.parquet"

    def get(
        self,
        ticker: str,
        interval: str = "1d",
        start: str | None = None,
        period: str | None = None,
        save: bool = True,
        force_full: bool = False,
        skip_existing_days: int = 1,           # NEW: configurable skip
    ) -> pd.DataFrame | None:
        """Fetch data for one ticker (incremental by default)."""

        path = self._get_file_path(ticker, interval)

        # Quick skip if file is recent
        if path.exists() and not force_full:
            mtime = path.stat().st_mtime
            age_days = (time.time() - mtime) / 86400
            if age_days < skip_existing_days:
                try:
                    return pd.read_parquet(path)
                except Exception:
                    pass  # fall through to fetch if read fails

        # Smart intraday handling
        if interval in self.source.INTRADAY_INTERVALS and not period and not start:
            period = "7d" if interval == "1m" else "60d"

        # Load existing if incremental
        old_df = pd.DataFrame()
        if path.exists() and not force_full:
            try:
                old_df = pd.read_parquet(path)
            except Exception as e:
                print(f"Failed reading {path}: {e}")

        last_date = old_df.index.max() if not old_df.empty else None

        # Skip fetch if very recent and same week
        if last_date and not self._should_fetch_this_week() and not force_full:
            if (datetime.now() - last_date).days < 2:
                return old_df

        # Fetch new data
        new_df = self.source.get(
            ticker,
            start=start,
            period=period,
            interval=interval,
            use_earliest_if_unavailable=True,
        )

        if new_df is None or new_df.empty:
            return old_df if not old_df.empty else None

        # Append + clean
        if not old_df.empty:
            combined = pd.concat([old_df, new_df]).sort_index()
            before = len(combined)

            # Fast path: drop exact duplicates
            combined = combined[~combined.index.duplicated(keep="last")]
            after_dedup = len(combined)
            exact_dupes = before - after_dedup

            # Only resolve conflicts if duplicates still exist
            if combined.index.duplicated().any():
                combined = combined.loc[combined.groupby(combined.index)["Volume"].idxmax()]
                after_resolve = len(combined)
                conflicts = after_dedup - after_resolve
            else:
                conflicts = 0

            if exact_dupes > 0 or conflicts > 0:
                print(f"{ticker}: removed {exact_dupes} dupes + resolved {conflicts} conflicts")

            # Gap warning (daily/weekly only)
            if interval in ["1d", "1wk"] and len(combined) > 20:
                span = (combined.index.max() - combined.index.min()).days
                expected = span * 0.72
                if len(combined) < expected * 0.6:
                    print(f"Warning: possible gaps in {ticker} ({len(combined)} bars over ~{span} days)")
        else:
            combined = new_df.sort_index()

        if save:
            combined.to_parquet(path, index=True, compression="zstd")
            print(f"Saved/updated: {path} ({len(combined)} rows)")

        return combined

    def fetch_batch(
        self,
        start_batch: int,
        end_batch: int,
        interval: str = "1d",
        batch_dir: str = "data/yfinance/batches",
        sub_batch_size: int = 80,               # sweet spot: 50–150
        skip_existing_days: int = 1,            # skip if file modified < N days ago
    ):
        """Process multiple batch files – now with bulk fetching."""

        batch_dir = Path(batch_dir)
        total_success = total_skipped = total_failed = 0

        for batch_num in range(start_batch, end_batch + 1):
            batch_file = batch_dir / f"batch_{batch_num:03d}.txt"
            if not batch_file.exists():
                print(f"Batch file missing: {batch_file}")
                continue

            print(f"\n{'═'*40} Batch {batch_num:03d} {'═'*40}")

            with open(batch_file) as f:
                tickers = [line.strip().upper() for line in f if line.strip()]

            # Filter tickers that actually need fetching
            to_fetch = []
            for t in tickers:
                p = self._get_file_path(t, interval)
                if p.exists():
                    age_days = (time.time() - p.stat().st_mtime) / 86400
                    if age_days < skip_existing_days:
                        total_skipped += 1
                        continue
                to_fetch.append(t)

            if not to_fetch:
                print("All files recent → skipping batch")
                continue

            print(f"Fetching {len(to_fetch)} / {len(tickers)} tickers...")

            success = failed = 0

            for i in tqdm(range(0, len(to_fetch), sub_batch_size), desc=f"Batch {batch_num:03d}"):
                sub = to_fetch[i : i + sub_batch_size]

                try:
                    # BULK FETCH – this is the main speedup
                    data = self.source.get(
                        tickers=sub,
                        interval=interval,
                        period="max" if interval not in self.source.INTRADAY_INTERVALS else "60d",
                    )

                    if data is None or data.empty:
                        failed += len(sub)
                        continue

                    for ticker in sub:
                        try:
                            if ticker not in data.columns.levels[0]:
                                failed += 1
                                continue

                            df = data[ticker].dropna(how="all")
                            if df.empty:
                                failed += 1
                                continue

                            path = self._get_file_path(ticker, interval)
                            self._save_incremental(path, df)  # reuse clean/save logic
                            success += 1

                        except Exception as e:
                            print(f"  {ticker} save error: {e}")
                            failed += 1

                except Exception as e:
                    print(f"Sub-batch {i//sub_batch_size + 1} failed ({len(sub)} tickers): {e}")
                    failed += len(sub)

                time.sleep(1.5 + random.uniform(0, 2.5))  # polite delay BETWEEN bulk calls

            total_success += success
            total_failed += failed
            print(f"Batch {batch_num:03d}: success {success}, skipped {total_skipped}, failed/empty {failed}")

        print(f"\nAll batches done → Success: {total_success} | Skipped: {total_skipped} | Failed/empty: {total_failed}")

    def _save_incremental(self, path: Path, new_df: pd.DataFrame):
        """Shared append + clean + save logic."""
        if path.exists():
            try:
                old_df = pd.read_parquet(path)
                combined = pd.concat([old_df, new_df]).sort_index()
                before = len(combined)
                combined = combined[~combined.index.duplicated(keep="last")]
                after_dedup = len(combined)

                if combined.index.duplicated().any():
                    combined = combined.loc[combined.groupby(combined.index)["Volume"].idxmax()]

                if len(combined) < before:
                    print(f"  Cleaned {before - len(combined)} rows on save")
            except Exception:
                combined = new_df
        else:
            combined = new_df

        combined.to_parquet(path, index=True, compression="zstd")


# ── Command-line batch mode ──────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Batch fetch from yfinance (optimized)")
    parser.add_argument("start", type=int, nargs="?", default=1, help="Start batch")
    parser.add_argument("end", type=int, nargs="?", default=20, help="End batch")
    parser.add_argument("--interval", default="1d", help="Data interval")
    parser.add_argument("--skip-days", type=int, default=1, help="Skip if file modified < N days ago")
    args = parser.parse_args()

    fetcher = DataFetcher()
    fetcher.fetch_batch(args.start, args.end, interval=args.interval, skip_existing_days=args.skip_days)