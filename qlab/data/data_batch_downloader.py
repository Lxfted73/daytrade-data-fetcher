"""
main.py - Download data for one batch of tickers from data/yfinance/batch_XXX.txt

Change BATCH_NUMBER or BATCH_FILE to process different batches.
"""

from pathlib import Path
from tqdm import tqdm
from data_fetcher import DataFetcher


# ── GLOBAL CONFIGURATION ─────────────────────────────────────────────────────
INTERVAL = "1d"                         # "1d", "1wk", "1m", etc.
LOOKBACK_PERIOD = "2010-01-01"          # "max", "10y", "5y", "2015-01-01", etc.

BASE_DIR = Path("data/yfinance")
BATCH_NUMBER = 1                        # ← change this to select different batch
BATCH_FILE = BASE_DIR / f"batch_{BATCH_NUMBER:03d}.txt"

# Alternative: directly specify file
# BATCH_FILE = BASE_DIR / "batch_001.txt"

SAVE_DIRECTORY = "./data/yfinance"      # where parquet files are stored
# ─────────────────────────────────────────────────────────────────────────────


def load_batch_tickers(path: Path) -> list[str]:
    if not path.exists():
        print(f"Batch file not found: {path}")
        return []

    with open(path, encoding="utf-8") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]

    print(f"Loaded {len(tickers)} tickers from {path.name}")
    return tickers


def main():
    print(f"Batch file: {BATCH_FILE}")

    tickers = load_batch_tickers(BATCH_FILE)
    if not tickers:
        print("No tickers loaded. Exiting.")
        return

    # Optional: skip known problematic tickers
    skip = {"AACB", "AACBU", "AAM", "ZIP", "ZOOZW"}
    tickers = [t for t in tickers if t not in skip]

    fetch_kwargs = (
        {"start": LOOKBACK_PERIOD}
        if LOOKBACK_PERIOD.count("-") == 2 and len(LOOKBACK_PERIOD) == 10
        else {"period": LOOKBACK_PERIOD}
    )

    fetcher = DataFetcher(save_dir=SAVE_DIRECTORY)

    print(f"\nStarting download: interval={INTERVAL}, {len(tickers)} tickers")
    print(f"Data will be saved in: {Path(SAVE_DIRECTORY).resolve()}/{INTERVAL}/")
    print("-" * 70)

    success = 0
    failed = 0

    for ticker in tqdm(tickers, desc="Downloading", unit="ticker"):
        try:
            df = fetcher.get(
                ticker=ticker,
                interval=INTERVAL,
                save=True,
                use_earliest_if_unavailable=True,
                **fetch_kwargs
            )
            if df is not None and not df.empty:
                success += 1
            else:
                failed += 1
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as e:
            print(f"  {ticker:<10} → {str(e)}")
            failed += 1

    print("\n" + "="*70)
    print("Batch finished")
    print(f"Successfully processed : {success}")
    print(f"Failed / no new data    : {failed}")
    print("="*70)


if __name__ == "__main__":
    main()