"""
Stock Timeframe Performance Analyzer – Single File Version with Progress Bar
──────────────────────────────────────────────────────────────────────────────

Analyzes pre-downloaded 1-minute OHLCV parquet files.
Shows % of up bars (Close > Open) across intraday timeframes for a specified or previous trading day.
Shows only top 15 stocks per "best timeframe" group + separate heatmaps per group.
Filters out low-volume tickers (<5M shares daily) for scalping suitability.
Saves heatmaps: group ones in day subfolder, overall one directly in HEATMAP_OUTPUT_DIR.

Last modified: January 2026
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from pathlib import Path
from pandas_market_calendars import get_calendar
from tqdm import tqdm

# ── Configuration ───────────────────────────────────────────────────────────
DATA_DIR             = Path("data/yfinance/1m")
BASE_DIR             = Path("data/yfinance/batches")
MIN_THRESHOLD        = 60                                # % up bars threshold to include
HIGH_THRESHOLD       = 80                                # "strong"
MED_THRESHOLD        = 70                                # "good"
MAX_SYMBOLS          = 7000                              # safety limit
SAVE_INTERMEDIATE_CSV = True
CSV_OUTPUT_SUFFIX    = "strong_stocks"
HEATMAP_MAX_TICKERS  = 200                               # max rows in overall heatmap
TOP_PER_GROUP        = 15                                # max stocks shown per group

MIN_DAILY_VOLUME     = 5_000_000                         # 5M shares

TARGET_DATE          = "2026-01-12"                      # or None for previous trading day

DEBUG_BARS           = True                              # Set False to quiet output

HEATMAP_SAVE_MODE    = "overall_only"                    # "all", "overall_only", "groups_only", "none"

HEATMAP_OUTPUT_DIR   = Path("heatmaps")

INTERVALS = ['1min', '2min', '3min', '5min', '7min', '15min', '30min', '60min', '120min', '240min']
INTERVAL_LABELS = ['1min', '2min', '3min', '5min', '7min', '15min', '30min', '1h', '2h', '4h']

MARKET_OPEN  = '09:30:00'
MARKET_CLOSE = '16:00:00'

plt.rcParams['figure.max_open_warning'] = 0


# ── Helpers ─────────────────────────────────────────────────────────────────
def get_trading_day(target_date_str: str | None = None):
    nyse = get_calendar('NYSE')
    today = pd.Timestamp.now(tz='America/New_York').normalize()

    if target_date_str:
        try:
            target = pd.Timestamp(target_date_str, tz='America/New_York').normalize()
            schedule = nyse.schedule(start_date=target - timedelta(days=10), end_date=target + timedelta(days=1))
            if target.date() in schedule.index.date:
                print(f"Using specified trading day: {target.date()}")
                return target.date()
            else:
                print(f"Warning: {target_date_str} is not a trading day. Falling back...")
        except Exception as e:
            print(f"Invalid date '{target_date_str}': {e}. Falling back...")

    schedule = nyse.schedule(start_date=today - timedelta(days=40), end_date=today)
    if len(schedule) < 2:
        return None
    return schedule.index[-2].date()


def load_ticker_1min(ticker: str) -> pd.DataFrame | None:
    path = DATA_DIR / f"{ticker.upper()}.parquet"
    if not path.exists():
        if DEBUG_BARS:
            print(f"{ticker}: no parquet file at {path}")
        return None
    try:
        df = pd.read_parquet(path)
        if not isinstance(df.index, pd.DatetimeIndex):
            if 'index' in df.columns:
                df = df.set_index('index')
            df.index = pd.to_datetime(df.index)
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        else:
            df.index = df.index.tz_convert('UTC')
        df = df.sort_index()
        if DEBUG_BARS:
            print(f"{ticker}: loaded {len(df)} total rows | date range {df.index.min().date()} → {df.index.max().date()}")
        return df
    except Exception as e:
        print(f"{ticker}: load error {e}")
        return None


def analyze_day(ticker: str, df_1min: pd.DataFrame, target_date: datetime.date) -> dict | None:
    """Filter to ALL bars on the target date (full day, including extended hours), resample, compute % up bars."""
    if df_1min is None or df_1min.empty:
        if DEBUG_BARS:
            print(f"{ticker}: skipped - no data loaded or empty")
        return None

    # Convert to New York time for consistent date slicing
    df_et = df_1min.tz_convert('America/New_York')

    # Slice ALL bars on the target date (no time-of-day filter)
    target_date_str = target_date.strftime('%Y-%m-%d')
    df_day = df_et[df_et.index.date == target_date]

    if DEBUG_BARS:
        print(f"{ticker} - total rows on {target_date}: {len(df_day)}")
        if not df_day.empty:
            print(f"  First bar: {df_day.index.min()}")
            print(f"  Last bar:  {df_day.index.max()}")

    if df_day.empty:
        if DEBUG_BARS:
            print(f"{ticker}: skipped - no rows on {target_date} at all")
        return None

    # Optional: still enforce a minimum number of bars (now much higher possible)
    if len(df_day) < 300:  # keep this or raise to 1000 if you want only very active stocks
        if DEBUG_BARS:
            print(f"{ticker}: skipped - only {len(df_day)} total bars on date (need ≥300)")
        return None

    daily_volume = df_day['Volume'].sum()
    if daily_volume < MIN_DAILY_VOLUME:
        if DEBUG_BARS:
            print(f"{ticker}: skipped - volume {daily_volume:,.0f} < {MIN_DAILY_VOLUME:,}")
        return None

    # Resample and compute % up bars (now using full-day bars)
    results = {}
    for intv, label in zip(INTERVALS, INTERVAL_LABELS):
        resampled = df_day.resample(intv).agg({
            'Open':   'first',
            'High':   'max',
            'Low':    'min',
            'Close':  'last',
            'Volume': 'sum'
        }).dropna(how='all')

        if resampled.empty:
            results[label] = 0.0
            if DEBUG_BARS:
                print(f"{ticker} {label}: 0 bars (empty resample)")
            continue

        up_count = (resampled['Close'] > resampled['Open']).sum()
        total = len(resampled)
        pct_up = (up_count / total) * 100 if total > 0 else 0.0
        results[label] = round(pct_up, 1)

        if DEBUG_BARS:
            print(f"{ticker} {label}: {total} bars, {up_count} up → {pct_up:.1f}%")

    return results

def main():
    target_date = get_trading_day(TARGET_DATE)
    if not target_date:
        print("No valid trading day found.")
        return
    date_str = target_date.strftime('%Y-%m-%d')
    print(f"\nAnalyzing {date_str} (vol ≥{MIN_DAILY_VOLUME:,} | up bars ≥{MIN_THRESHOLD}%)\n")
    if not BASE_DIR.exists():
        print(f"Missing {BASE_DIR} — run data_extract_all_tickers.py first!")
        return
    ticker_files = sorted(BASE_DIR.glob("batch_*.txt"))
    all_tickers = []
    for file in ticker_files:
        with open(file, encoding='utf-8') as f:
            all_tickers.extend([t.strip().upper() for t in f if t.strip() and not t.startswith('#')])
    all_tickers = sorted(set(all_tickers))[:MAX_SYMBOLS]
    print(f"Loaded {len(all_tickers):,} tickers from {len(ticker_files)} batches")
    results = {}
    with tqdm(total=len(all_tickers), desc="Analyzing tickers") as pbar:
        for ticker in all_tickers:
            df = load_ticker_1min(ticker)
            if df is None:
                pbar.update()
                continue
            result = analyze_day(ticker, df, target_date)
            if result:
                results[ticker] = result
            pbar.update()
    if not results:
        print("No usable data found.")
        print("Check:")
        print("  • Are parquets in data/yfinance/1m/ ?")
        print("  • Do they contain data for the target date?")
        print("  • Try TARGET_DATE = None or a recent date")
        return
    print(f"\n✓ Analyzed {len(results)} stocks with usable data\n")
    df_results = pd.DataFrame.from_dict(results, orient='index')[INTERVAL_LABELS]
    good_mask = df_results.max(axis=1) >= MIN_THRESHOLD
    df_filtered = df_results[good_mask].copy()
    if df_filtered.empty:
        print(f"No stocks hit ≥{MIN_THRESHOLD}% up bars on {date_str}.")
        return
    df_filtered['Max_%'] = df_filtered.max(axis=1).round(1)
    df_filtered['Strong_count'] = (df_filtered >= HIGH_THRESHOLD).sum(axis=1)
    df_filtered['Good_count'] = (df_filtered >= MED_THRESHOLD).sum(axis=1)
    df_filtered = df_filtered.sort_values('Max_%', ascending=False)
    if SAVE_INTERMEDIATE_CSV:
        HEATMAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = HEATMAP_OUTPUT_DIR / f"{CSV_OUTPUT_SUFFIX}_{date_str}.csv"
        save_df = df_filtered.copy()
        save_df.insert(0, 'Ticker', save_df.index)
        save_df.to_csv(csv_path, index=False)
        print(f"✓ CSV saved: {csv_path} ({len(save_df)} rows)")
    output_subdir = HEATMAP_OUTPUT_DIR / date_str
    output_subdir.mkdir(parents=True, exist_ok=True)
    print("\n" + "═"*80)
    print(f"TOP STOCKS ≥{MIN_THRESHOLD}% UP BARS | {date_str}".center(80))
    print("═"*80)
    print(df_filtered.round(1))
    # ── Group heatmaps ───────────────────────────────────────────────────────
    if HEATMAP_SAVE_MODE in ["all", "groups_only"]:
        best_tf = df_filtered.drop(columns=['Max_%','Strong_count','Good_count']).idxmax(axis=1)
        for tf, group in df_filtered.groupby(best_tf):
            if len(group) == 0: continue
            display_group = group.sort_values('Max_%', ascending=False).head(TOP_PER_GROUP)
            print(f"\nBest {tf}: {len(group)} stocks (top {TOP_PER_GROUP} shown)")
            print(display_group[['Max_%'] + INTERVAL_LABELS].round(1))
            heatmap_data = display_group.drop(columns=['Max_%', 'Strong_count', 'Good_count'])
            if not heatmap_data.empty:
                fig_height = max(5, min(18, len(heatmap_data)*0.45))  # (unchanged for groups, as they are small)
                plt.figure(figsize=(14, fig_height))
                sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap="YlGnBu", vmin=50, vmax=100,
                            linewidths=0.4, annot_kws={"size":10}, cbar_kws={'label': '% Up Bars'})
                plt.title(f"{tf} Top {date_str} (≥{MIN_THRESHOLD}%)", fontsize=13, pad=12)
                plt.ylabel("Ticker"); plt.xlabel("Timeframe")
                plt.xticks(rotation=45, ha='right'); plt.tight_layout()
                safe_tf = tf.replace(" ", "").replace(":", "")
                heatmap_file = output_subdir / f"heatmap_{date_str}_{safe_tf}_top{len(heatmap_data)}.png"
                plt.savefig(heatmap_file, dpi=140, bbox_inches='tight'); plt.close()
                print(f"  → {heatmap_file}")
    # ── Overall heatmap ──────────────────────────────────────────────────────
    if HEATMAP_SAVE_MODE in ["all", "overall_only"]:
        heatmap_data_all = df_filtered.drop(columns=['Max_%', 'Strong_count', 'Good_count'])
        if len(heatmap_data_all) > HEATMAP_MAX_TICKERS:
            max_vals = heatmap_data_all.max(axis=1)
            heatmap_data_all = heatmap_data_all.loc[max_vals.nlargest(HEATMAP_MAX_TICKERS).index]
        if not heatmap_data_all.empty:
            # NEW: Increase max height cap to 48 inches for taller plots (scale downward with more rows)
            # Adjust the multiplier (0.38) down to 0.3 for denser rows if needed.
            fig_height = max(6, min(48, len(heatmap_data_all) * 0.38))  # Increased cap from 24 to 48
            # NEW: Wider figure for better label visibility with many columns
            plt.figure(figsize=(18, fig_height))  # Increased width from 16 to 18
            # NEW: Smaller annotation font for dense rows (adjust to 8 or 7 if >300 rows)
            sns.heatmap(heatmap_data_all, annot=True, fmt=".1f", cmap="YlGnBu", vmin=50, vmax=100,
                        linewidths=0.3, annot_kws={"size":8}, cbar_kws={'label': '% Up Bars'})
            plt.title(f"Overall Top {date_str}\n(top {len(heatmap_data_all)})", fontsize=14, pad=16)
            plt.ylabel("Ticker"); plt.xlabel("Timeframe")
            plt.xticks(rotation=45, ha='right'); plt.tight_layout()
            overall_file = HEATMAP_OUTPUT_DIR / f"heatmap_{date_str}_overall_top{len(heatmap_data_all)}.png"
            # NEW: Higher DPI (200) for sharper large images; consider PDF for vector zoom if needed
            plt.savefig(overall_file, dpi=200, bbox_inches='tight'); plt.close()
            print(f"\n✓ Overall heatmap: {overall_file}")
    print("\nDone!")


if __name__ == "__main__":
    main()