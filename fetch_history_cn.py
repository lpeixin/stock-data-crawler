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
    """使用 AkShare 获取日线，优先 qfq，失败则 hfq，再失败则不复权。标准化列为：日期 开盘 收盘 最高 最低。"""
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

    # 有些环境会返回英文字段，统一重命名为中文
    rename_map = {
        "date": "日期",
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

    required = ["日期", "开盘", "收盘", "最高", "最低"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        # 有些版本列名为 带“价”后缀
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
    # 规范日期格式并排序
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    # 转为数值
    for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["日期", "开盘", "收盘", "最高", "最低"]).drop_duplicates(subset=["日期"]).sort_values("日期")
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch CN A-share historical data to CSV via AkShare")
    parser.add_argument("--ticker", required=True, help="A 股股票代码，例如 600519 或 600519.SH")
    parser.add_argument("--start", help="开始日期 YYYY-MM-DD，不填则尽可能早")
    parser.add_argument("--end", help="结束日期 YYYY-MM-DD（不包含当天），默认为今天")
    parser.add_argument("--out-dir", default="data_cn", help="输出目录，默认 data_cn")
    args = parser.parse_args()

    logger = setup_logger()

    end = args.end or datetime.utcnow().strftime("%Y-%m-%d")
    df = fetch_history_cn(args.ticker, start=args.start, end=end)

    rows = df_to_rows(df)
    out_dir = args.out_dir
    ensure_dir(out_dir)
    # CSV 文件名使用原始传入 ticker 的大写去空格（不强制加后缀）
    out_path = f"{out_dir.rstrip('/')}/{args.ticker.upper().strip()}.csv"
    write_history_csv(out_path, rows)
    logger.info(f"已保存 {args.ticker.upper()} 历史数据，共 {len(rows)} 行 -> {out_path}")


if __name__ == "__main__":
    main()
