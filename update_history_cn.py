#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据已有 CSV（历史数据）自动增量抓取并追加 A 股最近数据。

数据来源：AkShare（免 Key）。
输出 CSV 列：timestamps,open,high,low,close,volume,amount（逗号分隔）

用法示例：
  python update_history_cn.py --ticker 600519 --csv data_cn/600519.csv
"""
import argparse
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
import akshare as ak

from utils import setup_logger, read_last_date_from_csv, append_history_csv, polite_sleep


def normalize_cn_hist_symbol(ticker: str) -> str:
    t = ticker.upper().strip()
    t = t.replace("SZ", "").replace("SH", "")
    t = t.replace(".", "")
    return t


def df_to_rows(df: pd.DataFrame, interval: str = "1d") -> List[Tuple[str, float, float, float, float, float, float]]:
    """Convert DataFrame to rows with appropriate timestamp formatting based on interval."""
    rows: List[Tuple[str, float, float, float, float, float, float]] = []
    
    # Define timestamp format based on interval
    if interval == "1d":
        ts_format = "%Y-%m-%d"  # Daily: YYYY-MM-DD
    else:
        ts_format = "%Y-%m-%d %H:%M:%S"  # Intraday: YYYY-MM-DD HH:MM:SS
    
    for _, r in df.iterrows():
        if interval == "1d":
            # For daily data, use date column and format as YYYY-MM-DD
            time_col = "时间" if "时间" in df.columns else "日期"
            ts = pd.Timestamp(r[time_col]).strftime(ts_format)
        else:
            # For intraday data, assume the column might be named differently
            time_col = "时间" if "时间" in df.columns else "日期"
            ts = pd.Timestamp(r[time_col]).strftime(ts_format)
        
        open_ = float(r["开盘"]) if pd.notna(r["开盘"]) else None
        close_ = float(r["收盘"]) if pd.notna(r["收盘"]) else None
        high_ = float(r["最高"]) if pd.notna(r["最高"]) else None
        low_ = float(r["最低"]) if pd.notna(r["最低"]) else None
        vol_ = float(r["成交量"]) if "成交量" in df.columns and pd.notna(r["成交量"]) else 0.0
        amt_ = float(r["成交额"]) if "成交额" in df.columns and pd.notna(r["成交额"]) else 0.0
        if None in (open_, close_, high_, low_):
            continue
        rows.append((ts, open_, high_, low_, close_, vol_, amt_))
    return rows


def fetch_history_cn(ticker: str, start: str = None, end: str = None, interval: str = "1d") -> pd.DataFrame:
    """使用 AkShare 获取股票数据，支持日线和分钟线。优先 qfq，失败则 hfq，再失败则不复权。"""
    polite_sleep(1.0)
    symbol = normalize_cn_hist_symbol(ticker)
    start_date = (start or "19900101").replace("-", "")
    end_date = (end or datetime.utcnow().strftime("%Y%m%d")).replace("-", "")

    def _try_daily(adjust: str | None):
        return ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust=(adjust or ""))
    
    def _try_minute(period: str):
        """Try to fetch minute data using akshare intraday APIs"""
        try:
            # For intraday data, we use current day's data as akshare may not support historical intraday
            if period in ["1", "5", "15", "30", "60"]:
                # Use stock_zh_a_minute API for minute data
                return ak.stock_zh_a_minute(symbol=symbol, period=period, adjust="qfq")
            return pd.DataFrame()
        except Exception as e:
            # If minute API fails, return empty DataFrame
            return pd.DataFrame()

    # Handle different intervals
    if interval == "1d":
        df = _try_daily("qfq")
        if df is None or df.empty:
            df = _try_daily("hfq")
        if df is None or df.empty:
            df = _try_daily("")
        if df is None or df.empty:
            return pd.DataFrame(columns=["日期", "开盘", "收盘", "最高", "最低"])
    else:
        # For intraday data, map intervals to akshare periods
        period_map = {"5m": "5", "15m": "15", "30m": "30", "1h": "60"}
        period = period_map.get(interval, "5")
        
        df = _try_minute(period)
        if df is None or df.empty:
            # Fallback to daily data if minute data is not available
            print(f"警告：分钟数据不可用，回退到日线数据")
            return fetch_history_cn(ticker, start, end, "1d")

    rename_map = {
        "date": "日期", 
        "time": "时间", 
        "datetime": "时间",
        "open": "开盘", 
        "close": "收盘", 
        "high": "最高", 
        "low": "最低", 
        "volume": "成交量", 
        "amount": "成交额"
    }
    for k, v in rename_map.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})
    
    # Determine required columns based on interval
    if interval == "1d":
        required = ["日期", "开盘", "收盘", "最高", "最低"]
        time_col = "日期"
    else:
        # For minute data, check if we have time column
        if "时间" in df.columns:
            required = ["时间", "开盘", "收盘", "最高", "最低"]
            time_col = "时间"
        else:
            required = ["日期", "开盘", "收盘", "最高", "最低"]
            time_col = "日期"
    
    alt_map = {"开盘价": "开盘", "收盘价": "收盘", "最高价": "最高", "最低价": "最低"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        for src, dst in alt_map.items():
            if src in df.columns and dst not in df.columns:
                df = df.rename(columns={src: dst})
        missing = [c for c in required if c not in df.columns]
        if missing:
            return pd.DataFrame(columns=required)
    keep_cols = required + [c for c in ["成交量", "成交额"] if c in df.columns]
    df = df[keep_cols].copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[time_col, "开盘", "收盘", "最高", "最低"]).drop_duplicates(subset=[time_col]).sort_values(time_col)
    return df


essential_msg = "未找到有效历史数据，建议先运行 fetch_history_cn.py 生成全量数据。"


def main():
    parser = argparse.ArgumentParser(description="Incrementally update CN A-share history CSV via AkShare")
    parser.add_argument("--ticker", required=True, help="A 股股票代码，例如 600519 或 600519.SH")
    parser.add_argument("--csv", required=True, help="已存在的 CSV 路径")
    parser.add_argument("--interval", default="1d", 
                       choices=["1d", "1h", "30m", "15m", "5m"],
                       help="数据间隔: 1d (日线), 1h (小时线), 30m (30分钟), 15m (15分钟), 5m (5分钟)")
    args = parser.parse_args()

    logger = setup_logger()
    last_date = read_last_date_from_csv(args.csv)
    if last_date is None:
        logger.info(essential_msg)
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

    df = fetch_history_cn(args.ticker, start=start_date, end=end_date, interval=args.interval)
    if df.empty:
        logger.info("没有新数据可追加。")
        return

    rows = df_to_rows(df, interval=args.interval)
    append_history_csv(args.csv, rows)
    logger.info(f"已更新 {args.ticker.upper()} 历史数据 ({args.interval})，追加 {len(rows)} 行 -> {args.csv}")


if __name__ == "__main__":
    main()
