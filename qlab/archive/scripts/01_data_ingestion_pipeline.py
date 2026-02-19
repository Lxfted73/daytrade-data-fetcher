#!/usr/bin/env python3
# 01_pipeline_to_parquet.py
"""
Pipeline Orchestrator: Ticker Extraction → Batch Parquet Download → Standardization
────────────────────────────────────────────────────────────────────────────────────

Single-file launcher that prepares U.S. equities OHLCV data (from yfinance) 
and saves everything as clean Parquet files — no DuckDB involved.

Supported stages (toggle via RUN dict):
1. Extract unique tickers from source CSVs → create sorted batch files
2. Download daily bars (batches 001–0XX) → save as parquet
3. (Optional) Download recent intraday bars (1min, respects Yahoo ~7d limit)
4. (Optional) Standardize/repair existing parquet files (index, timezone, etc.)

Typical usage:
    python 01_pipeline_to_parquet.py

Last modified: February 2026
"""

import sys
from pathlib import Path
from datetime import datetime
import time
import importlib.util

# 01_data_ingestion_pipeline.py
# (put this near the top, after your imports)


# ── Make qlab/ discoverable ──────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]          # go up 1 level from scripts/
QLAB_DIR     = PROJECT_ROOT / "archive"                        # adjust if it's qlab/data → / "qlab" / "data"

if str(QLAB_DIR) not in sys.path:
    sys.path.insert(0, str(QLAB_DIR))

print(f"Added to sys.path: {QLAB_DIR.resolve()}")           # helpful for debugging

# ── Safe dynamic imports ─────────────────────────────────────────────────────

def safe_import(module_name: str, function_name: str = "main"):
    """Try to import module.function or return None"""
    try:
        # Now we can just use the module name (no path needed)
        module = __import__(module_name)
        return getattr(module, function_name, None)
    except (ImportError, AttributeError) as e:
        print(f"  Failed to import {module_name}.{function_name}  →  {e}")
        return None


# Usage stays almost identical
extract_main     = safe_import("data_extract_all_tickers", "extract_all_unique_tickers")
download_main    = safe_import("data_batch_downloader",   "main")
standardize_main = safe_import("data_standardize_parquets", "main")

# ── Configuration ─────────────────────────────────────────────────────────────

RUN = {
    "extract":       True,      # usually only once or when ticker list changes
    "download_daily": True,
    "download_1min": False,
    "standardize":   False,
}

# ── Batch & interval settings ─────────────────────────────────────────────────

BATCH_START = 1
BATCH_END   = 20                # inclusive

# Only used when download_1min = True
INTRADAY_INTERVAL = "1m"        # "1m", "2m", "5m", etc.

BASE_DIR = Path(__file__).parent.resolve()
BATCH_DIR = BASE_DIR / "data" / "yfinance" / "batches"


# ── Helpers ───────────────────────────────────────────────────────────────────

def run_step(name: str, func, *args, required: bool = True):
    if func is None:
        print(f"→ SKIPPED: {name}  (module/function not found)")
        if required:
            print("Critical step missing → stopping.")
            sys.exit(1)
        return

    print(f"\n{'═' * 80}")
    print(f" {name.upper()}")
    print(f"{'═' * 80}\n")

    start = time.time()
    try:
        func(*args)
        duration = time.time() - start
        print(f"\n→ SUCCESS  ({duration:.1f}s)\n")
    except Exception as e:
        duration = time.time() - start
        print(f"\n→ FAILED: {type(e).__name__}: {e}  ({duration:.1f}s)\n")
        if required:
            print("Critical step failed → stopping.")
            sys.exit(1)


def download_batches(interval: str, start_batch: int, end_batch: int):
    """
    Run the batch downloader for given interval and batch range.
    Simulates CLI argument passing (most reliable for existing scripts).
    """
    if download_main is None:
        raise ImportError("data_batch_downloader.main could not be imported")

    old_argv = sys.argv[:]
    try:
        # Simulate command-line call: script.py START END
        sys.argv = [str(Path(download_main.__code__.co_filename)), str(start_batch), str(end_batch)]

        # Set interval (most batch downloaders use a global INTERVAL)
        import data_batch_downloader
        original_interval = getattr(data_batch_downloader, 'INTERVAL', '1d')
        data_batch_downloader.INTERVAL = interval

        download_main()
    finally:
        sys.argv = old_argv
        if 'data_batch_downloader' in sys.modules:
            data_batch_downloader.INTERVAL = original_interval


# ── Main Pipeline ─────────────────────────────────────────────────────────────

def main():
    ts = datetime.now().strftime('%Y-%m-%d %H:%M')
    print(f"Parquet Pipeline • {ts}\n")
    print(f"Batch range: {BATCH_START:03d} – {BATCH_END:03d}\n")

    # 1. Extract tickers & create batch files (usually one-time)
    if RUN["extract"] and extract_main is not None:
        run_step("Extracting tickers & creating batches", extract_main)

    # 2. Download daily data (most common / repeated step)
    if RUN["download_daily"]:
        run_step(
            f"Downloading daily data (batches {BATCH_START:03d}–{BATCH_END:03d})",
            download_batches,
            "1d", BATCH_START, BATCH_END
        )

    # 3. Optional: recent intraday snapshot (~last 7 days)
    if RUN["download_1min"]:
        run_step(
            f"Downloading recent {INTRADAY_INTERVAL} data",
            download_batches,
            INTRADAY_INTERVAL, BATCH_START, BATCH_END
        )

    # 4. Optional: fix index names, timezones, columns, etc.
    if RUN["standardize"] and standardize_main is not None:
        run_step("Standardizing / repairing parquet files", standardize_main)

    print("\n" + "═" * 80)
    print("Pipeline finished.")
    print(f"Data should now be in: data/yfinance/1d/*.parquet   (and other intervals if enabled)")
    print("Next recommended steps:")
    print("  • Inspect files:     dir data\\yfinance\\1d\\   (or ls -lh on unix)")
    print("  • Run quick quality check (see code below)")
    print("  • Start EV analysis: python processing_EV.py")
    print("  • Build simple momentum backtest")
    print("═" * 80 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\nUnexpected error: {type(e).__name__}: {e}")
        sys.exit(1)