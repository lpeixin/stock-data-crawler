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


def df_to_rows(df: pd.DataFrame) -> List[Tuple[str, float, float, float, float, float, float]]:
    rows: List[Tuple[str, float, float, float, float, float, float]] = []
    for _, r in df.iterrows():
        ts = pd.Timestamp(r["日期"]).tz_localize(None).strftime("%Y-%m-%d")
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


def fetch_history_cn(ticker: str, start: str = None, end: str = None) -> pd.DataFrame:
    polite_sleep(1.0)
    symbol = normalize_cn_hist_symbol(ticker)
    start_date = (start or "19900101").replace("-", "")
    end_date = (end or datetime.utcnow().strftime("%Y%m%d")).replace("-", "")

    def _try(adjust: str | None):
        return ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust=(adjust or ""))

    df = _try("qfq")
    if df is None or df.empty:
        df = _try("hfq")
    if df is None or df.empty:
        df = _try("")
    if df is None or df.empty:
        return pd.DataFrame(columns=["日期", "开盘", "收盘", "最高", "最低"])  # 空结构

    rename_map = {"date": "日期", "open": "开盘", "close": "收盘", "high": "最高", "low": "最低", "volume": "成交量", "amount": "成交额"}
    for k, v in rename_map.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})
    required = ["日期", "开盘", "收盘", "最高", "最低"]
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
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["日期", "开盘", "收盘", "最高", "最低"]).drop_duplicates(subset=["日期"]).sort_values("日期")
    return df


essential_msg = "未找到有效历史数据，建议先运行 fetch_history_cn.py 生成全量数据。"


def main():
    parser = argparse.ArgumentParser(description="Incrementally update CN A-share history CSV via AkShare")
    parser.add_argument("--ticker", required=True, help="A 股股票代码，例如 600519 或 600519.SH")
    parser.add_argument("--csv", required=True, help="已存在的 CSV 路径")
    args = parser.parse_args()

    logger = setup_logger()
    last_date = read_last_date_from_csv(args.csv)
    if last_date is None:
        logger.info(essential_msg)
        return

    start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = datetime.utcnow().strftime("%Y-%m-%d")

    if start_date >= end_date:
        logger.info("CSV 已是最新，无需更新。")
        return

    df = fetch_history_cn(args.ticker, start=start_date, end=end_date)
    if df.empty:
        logger.info("没有新数据可追加。")
        return

    rows = df_to_rows(df)
    append_history_csv(args.csv, rows)
    logger.info(f"已更新 {args.ticker.upper()} 历史数据，追加 {len(rows)} 行 -> {args.csv}")


if __name__ == "__main__":
    main()
