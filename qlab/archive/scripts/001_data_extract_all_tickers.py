"""
extract_all_tickers.py - One-time script to extract all valid tickers from both CSVs
Creates: data/yfinance/all_tickers.txt + data/yfinance/batch_001.txt, batch_002.txt, etc.
"""

import pandas as pd
from pathlib import Path

# ── Configuration ────────────────────────────────────────────────────────────
BATCH_SIZE = 50
BASE_DIR = Path("market_data/yfinance/batches")
ALL_TICKERS_FILE = BASE_DIR / "all_tickers.txt"
# ─────────────────────────────────────────────────────────────────────────────

def clean_ticker(t: str) -> str | None:
    t = str(t).strip().upper()
    if not (1 <= len(t) <= 10):
        return None
    if any(c in t for c in r'^~`!@#$%&*+={}[]|\:;"\'<>,.?/'):
        return None
    if t.startswith(('-', '.', '$', '=', '+')):
        return None
    return t


def extract_all_unique_tickers():
    alpha_vantage = Path(r"qlab-quant-research\qlab\resources\listing_status.csv")

    tickers = set()

    for path in [alpha_vantage]:
        if not path.exists():
            print(f"Missing  file: {path}")
            continue

        try:
            df = pd.read_csv(path, dtype=str, on_bad_lines='skip')
            symbol_col = next((c for c in ['Symbol', 'symbol', 'Ticker', 'ticker'] 
                              if c in df.columns), None)
            if not symbol_col:
                print(f"No symbol column in {path.name}")
                continue

            for val in df[symbol_col].dropna():
                cleaned = clean_ticker(val)
                if cleaned:
                    tickers.add(cleaned)

        except Exception as e:
            print(f"Error reading {path}: {e}")

    all_tickers = sorted(tickers)
    print(f"Extracted {len(all_tickers):,} valid tickers")

    # Create base directory if it doesn't exist
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    # Save full list
    with open(ALL_TICKERS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(all_tickers) + "\n")
    print(f"Saved full list → {ALL_TICKERS_FILE}")

    # Split into batches inside data/yfinance
    for i in range(0, len(all_tickers), BATCH_SIZE):
        batch = all_tickers[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        batch_path = BASE_DIR / f"batch_{batch_num:03d}.txt"

        with open(batch_path, "w", encoding="utf-8") as f:
            f.write("\n".join(batch) + "\n")

        print(f"Created {batch_path} ({len(batch)} tickers)")

    print("\nDone. Batch files are now saved in data/yfinance/")


if __name__ == "__main__":
    extract_all_unique_tickers()