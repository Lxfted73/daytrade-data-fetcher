import pandas as pd
from pathlib import Path
import numpy as np

folder = Path("data/yfinance/1d")          # or /1m, etc.
parquets = list(folder.glob("*.parquet"))

print(f"Found {len(parquets)} files\n")

# Quick stats on a few random tickers
sample_files = np.random.choice(parquets, size=min(8, len(parquets)), replace=False)

for p in sample_files:
    df = pd.read_parquet(p)
    if df.empty:
        print(f"EMPTY: {p.name}")
        continue
        
    print(f"{p.stem:>8}  |  rows: {len(df):>6,}  |  date range: {df.index.min().date()} – {df.index.max().date()}")
    print(f"         |  NaN Close: {df['Close'].isna().sum():3d}  |  zero/neg Close: {(df['Close'] <= 0).sum():3d}")
    print(f"         |  zero Volume days: {(df['Volume'] <= 0).sum():3d}")
    print()