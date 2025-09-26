#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Incrementally update US stock historical data CSV.

Source: Yahoo Finance via yfinance. Use polite rate limiting.

Usage example:
  python update_history_us.py --ticker AAPL --csv data_us/AAPL.csv
"""
import argparse
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
import yfinance as yf

from utils import setup_logger, read_last_date_from_csv, append_history_csv, polite_sleep


def df_to_rows(df: pd.DataFrame, interval: str = "1d") -> List[Tuple[str, float, float, float, float, float, float]]:
    """Convert DataFrame to rows with appropriate timestamp formatting based on interval."""
    rows: List[Tuple[str, float, float, float, float, float, float]] = []
    
    # Define timestamp format based on interval
    if interval == "1d":
        ts_format = "%Y-%m-%d"  # Daily: YYYY-MM-DD
    else:
        ts_format = "%Y-%m-%d %H:%M:%S"  # Intraday: YYYY-MM-DD HH:MM:SS
    
    for idx, r in df.iterrows():
        ts = pd.Timestamp(idx).tz_localize(None).strftime(ts_format)
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
    if missing and missing != {"Volume"}:
        raise RuntimeError(f"Missing columns in response: {df.columns}")
    cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    df = df[cols].dropna(subset=["Open", "High", "Low", "Close"], how="any")
    return df


def main():
    parser = argparse.ArgumentParser(description="Incrementally update US stock history CSV via Yahoo Finance")
    parser.add_argument("--ticker", required=True, help="US ticker, e.g. AAPL")
    parser.add_argument("--csv", required=True, help="Existing CSV path to append to")
    parser.add_argument("--interval", default="1d", 
                       choices=["1d", "1h", "30m", "15m", "5m"],
                       help="Data interval: 1d (daily), 1h (hourly), 30m (30min), 15m (15min), 5m (5min)")
    args = parser.parse_args()

    logger = setup_logger()
    last_date = read_last_date_from_csv(args.csv)
    if last_date is None:
        logger.info("未找到有效历史数据，建议先运行 fetch_history_us.py 生成全量数据。")
        return

    # For intraday data, we need different start calculation
    if args.interval == "1d":
        start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.utcnow().strftime("%Y-%m-%d")
    else:
        # For intraday data, start from the last date and get more recent data
        start_date = last_date.strftime("%Y-%m-%d")
        end_date = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")

    if args.interval == "1d" and start_date >= end_date:
        logger.info("CSV 已是最新，无需更新。")
        return

    df = fetch_history(args.ticker, start=start_date, end=end_date, interval=args.interval)
    if df.empty:
        logger.info("没有新数据可追加。")
        return

    rows = df_to_rows(df, interval=args.interval)
    append_history_csv(args.csv, rows)
    logger.info(f"已更新 {args.ticker.upper()} 历史数据 ({args.interval})，追加 {len(rows)} 行 -> {args.csv}")


if __name__ == "__main__":
    main()
