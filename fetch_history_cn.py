#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
抓取 A 股某只股票的历史数据并保存为 CSV。

数据来源：AkShare（聚合各公开数据源），免 Key。
输出 CSV 列：timestamps,open,high,low,close,volume,amount（逗号分隔）
文件名：<TICKER>.csv（默认保存到 data_cn 目录）。

用法示例：
    python fetch_history_cn.py --ticker 600519 --start 2010-01-01 --out-dir data_cn

注意：A 股代码需带市场前缀时，按 akshare 规范处理（如 600519.SH、000001.SZ）。本脚本自动根据数值开头推断：
- 以 6 开头默认 .SH
- 以 0 或 3 开头默认 .SZ
如传入已带后缀的，将按原样使用。
"""
import argparse
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import akshare as ak

from utils import setup_logger, ensure_dir, write_history_csv, polite_sleep


def normalize_cn_hist_symbol(ticker: str) -> str:
    """AkShare stock_zh_a_hist 需要不带后缀的纯数字代码，例如 600519 或 000001。
    兼容传入 600519.SH / SH600519 / 000001.SZ / SZ000001 / 纯数字。
    """
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

        # 有些环境会返回英文字段，统一重命名为中文
    rename_map = {
        "date": "日期",
        "time": "时间", 
        "datetime": "时间",
        "open": "开盘",
        "close": "收盘",
        "high": "最高",
        "low": "最低",
        "volume": "成交量",
        "amount": "成交额",
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
    
    missing = [c for c in required if c not in df.columns]
    if missing:
        # 有些版本列名为带"价"后缀
        alt_map = {"开盘价": "开盘", "收盘价": "收盘", "最高价": "最高", "最低价": "最低"}
        for src, dst in alt_map.items():
            if src in df.columns and dst not in df.columns:
                df = df.rename(columns={src: dst})
        missing = [c for c in required if c not in df.columns]
        if missing:
            return pd.DataFrame(columns=required)

    # 只保留所需列（若有成交量/成交额则一并保留）
    keep_cols = required + [c for c in ["成交量", "成交额"] if c in df.columns]
    df = df[keep_cols].copy()
    
    # 规范时间格式并排序
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    
    # 转为数值
    for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    df = df.dropna(subset=[time_col, "开盘", "收盘", "最高", "最低"]).drop_duplicates(subset=[time_col]).sort_values(time_col)
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch CN A-share historical data to CSV via AkShare")
    parser.add_argument("--ticker", required=True, help="A 股股票代码，例如 600519 或 600519.SH")
    parser.add_argument("--start", help="开始日期 YYYY-MM-DD，不填则尽可能早")
    parser.add_argument("--end", help="结束日期 YYYY-MM-DD（不包含当天），默认为今天")
    parser.add_argument("--interval", default="1d", 
                       choices=["1d", "1h", "30m", "15m", "5m"],
                       help="数据间隔: 1d (日线), 1h (小时线), 30m (30分钟), 15m (15分钟), 5m (5分钟)")
    parser.add_argument("--out-dir", default="data_cn", help="输出目录，默认 data_cn")
    args = parser.parse_args()

    logger = setup_logger()

    end = args.end or datetime.utcnow().strftime("%Y-%m-%d")
    df = fetch_history_cn(args.ticker, start=args.start, end=end, interval=args.interval)

    rows = df_to_rows(df, interval=args.interval)
    out_dir = args.out_dir
    ensure_dir(out_dir)
    
    # Add interval suffix to filename for non-daily data
    if args.interval == "1d":
        out_path = f"{out_dir.rstrip('/')}/{args.ticker.upper().strip()}.csv"
    else:
        out_path = f"{out_dir.rstrip('/')}/{args.ticker.upper().strip()}_{args.interval}.csv"
    
    write_history_csv(out_path, rows)
    logger.info(f"已保存 {args.ticker.upper()} 历史数据 ({args.interval})，共 {len(rows)} 行 -> {out_path}")


if __name__ == "__main__":
    main()
