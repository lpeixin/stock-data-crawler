import os
import sys
import time
import csv
import logging
from datetime import datetime
from typing import Optional, Iterable


def setup_logger(name: str = "stock_crawler", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def polite_sleep(seconds: float, jitter: float = 0.3) -> None:
    """Sleep with a small jitter to avoid strict patterns."""
    if seconds <= 0:
        return
    import random
    jitter_amt = random.uniform(-jitter, jitter)
    time.sleep(max(0.0, seconds + jitter_amt))


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def csv_exists(path: str) -> bool:
    return os.path.isfile(path)


def read_last_date_from_csv(path: str) -> Optional[datetime]:
    """Read last date from CSV, supporting legacy and new schema.

    - Legacy schema: comma-separated, header in Chinese, first column is YYYY-MM-DD
    - New schema: pipe-separated ("|"), header: timestamps|open|hight|low|close|volume|amount;
      first column is epoch seconds (UTC) integer.
    - Intraday schema: comma-separated, first column is YYYY-MM-DD HH:MM:SS
    """
    if not csv_exists(path):
        return None
    sample = None
    with open(path, "r", encoding="utf-8") as f:
        sample = f.read(2048)
    if not sample:
        return None
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",|")
        delimiter = dialect.delimiter
    except Exception:
        # Fallback: detect by presence of pipe in header
        delimiter = "|" if "|" in sample.splitlines()[0] else ","

    last_date: Optional[datetime] = None
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=delimiter)
        _ = next(reader, None)  # header
        for row in reader:
            if not row:
                continue
            cell0 = row[0].strip()
            
            # Try intraday format YYYY-MM-DD HH:MM:SS first
            try:
                d = datetime.strptime(cell0, "%Y-%m-%d %H:%M:%S")
                last_date = d
                continue
            except Exception:
                pass
                
            # Try legacy YYYY-MM-DD
            try:
                d = datetime.strptime(cell0, "%Y-%m-%d")
                last_date = d
                continue
            except Exception:
                pass
            # Try epoch seconds
            try:
                ts = int(float(cell0))
                d = datetime.utcfromtimestamp(ts)
                d = datetime.strptime(d.strftime("%Y-%m-%d"), "%Y-%m-%d")
                last_date = d
            except Exception:
                continue
    return last_date


def write_history_csv(path: str, rows: Iterable[tuple]) -> None:
    """Overwrite CSV with header and given rows.

    New schema header (comma-separated):
    timestamps,open,high,low,close,volume,amount
    Each row: (date: YYYY-MM-DD, open:float, high:float, low:float, close:float, volume:float, amount:float)
    """
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(["timestamps", "open", "high", "low", "close", "volume", "amount"])
        writer.writerows(rows)


def append_history_csv(path: str, rows: Iterable[tuple]) -> None:
    file_exists = csv_exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",")
        if not file_exists:
            writer.writerow(["timestamps", "open", "high", "low", "close", "volume", "amount"])
        writer.writerows(rows)
