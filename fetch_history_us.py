#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch US stock historical data and save as CSV with schema:
timestamps,open,high,low,close,volume,amount

Source: Yahoo Finance via yfinance. Use polite rate limiting.
Default output dir: data_us (use --out-dir to change).

Usage example:
    python fetch_history_us.py --ticker AAPL --start 2010-01-01 --end 2025-08-15 --out-dir data_us
"""
import argparse
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
import yfinance as yf

from utils import setup_logger, ensure_dir, write_history_csv, polite_sleep


def df_to_rows(df: pd.DataFrame) -> List[Tuple[str, float, float, float, float, float, float]]:
    rows: List[Tuple[str, float, float, float, float, float, float]] = []
    for idx, r in df.iterrows():
        ts = pd.Timestamp(idx).tz_localize(None).strftime("%Y-%m-%d")
        open_ = float(r["Open"]) if pd.notna(r["Open"]) else None
        close_ = float(r["Close"]) if pd.notna(r["Close"]) else None
        high_ = float(r["High"]) if pd.notna(r["High"]) else None
        low_ = float(r["Low"]) if pd.notna(r["Low"]) else None
        vol_ = float(r["Volume"]) if "Volume" in r and pd.notna(r["Volume"]) else 0.0
        amount_ = (vol_ * close_) if (vol_ is not None and close_ is not None) else 0.0
        if None in (open_, close_, high_, low_):
            continue
        rows.append((ts, open_, high_, low_, close_, vol_, float(amount_)))
    return rows


def fetch_history(ticker: str, start: str = None, end: str = None, interval: str = "1d") -> pd.DataFrame:
    polite_sleep(1.0)
    df = yf.download(
        tickers=ticker,
        start=start,
        end=end,
        interval=interval,
        progress=False,
        auto_adjust=False,
        group_by="column",
    )
    if df.empty:
        return df
    if isinstance(df.columns, pd.MultiIndex):
        try:
            df = df.xs(ticker, axis=1, level=1)
        except Exception:
            df = df.copy()
            df.columns = [c[0] for c in df.columns]
    if not isinstance(df.index, pd.DatetimeIndex):
        raise RuntimeError("Unexpected data format: index is not DatetimeIndex")
    if df.index.tz is not None:
        df.index = df.index.tz_convert("UTC").tz_localize(None)
    needed = {"Open", "High", "Low", "Close", "Volume"}
    missing = needed - set(df.columns)
    if missing and missing != {"Volume"}:  # 没有 Volume 也可以写 0
        raise RuntimeError(f"Missing columns in response: {df.columns}")
    cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    df = df[cols].dropna(subset=["Open", "High", "Low", "Close"], how="any")
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch US stock historical data to CSV via Yahoo Finance")
    parser.add_argument("--ticker", required=True, help="US ticker, e.g. AAPL")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD (exclusive), default today")
    parser.add_argument("--out-dir", default="data_us", help="Output directory, default data_us")
    args = parser.parse_args()

    logger = setup_logger()

    # Make end inclusive for user convenience: yfinance expects end-exclusive.
    if args.end:
        try:
            end_eff = (datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        except ValueError:
            end_eff = args.end
    else:
        end_eff = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")

    df = fetch_history(args.ticker, start=args.start, end=end_eff)

    rows = df_to_rows(df)
    out_dir = args.out_dir
    ensure_dir(out_dir)
    out_path = f"{out_dir.rstrip('/')}/{args.ticker.upper()}.csv"
    write_history_csv(out_path, rows)
    logger.info(f"已保存 {args.ticker.upper()} 历史数据，共 {len(rows)} 行 -> {out_path}")


if __name__ == "__main__":
    main()
