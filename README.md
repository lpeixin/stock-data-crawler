# Stock Crawler / 股票数据抓取脚本

## English

Lightweight command-line scripts to fetch and maintain daily historical data (CSV) for US stocks and Chinese A-shares.

- **US**: Yahoo Finance via `yfinance`
- **CN**: AkShare (aggregating public data sources like Eastmoney)

**Unified CSV columns** (comma-separated): `timestamps,open,high,low,close,volume,amount`

**Notes**:
- `timestamps`: Date string in YYYY-MM-DD format (daily) or YYYY-MM-DD HH:MM:SS format (intraday)
- **US source** (Yahoo): Does not directly provide transaction amount, script estimates using `amount = close * volume`
- **CN source**: Uses actual transaction amount when available

## 中文

轻量级命令行脚本，抓取并维护 美股/中国A股 的历史数据（CSV），支持日线和分钟线。

- **美股**: Yahoo Finance via `yfinance`
- **A股**: AkShare（聚合东方财富等公开数据源）

**CSV 统一列**（逗号分隔）：`timestamps,open,high,low,close,volume,amount`

**说明**：
- `timestamps`: 日线为日期字符串 YYYY-MM-DD，分钟线为 YYYY-MM-DD HH:MM:SS
- **美股数据源**（Yahoo）：不直接提供成交额，脚本使用 `amount = close * volume` 估算
- **A股数据源**：若提供实际成交额则直接写入

## 1) Installation

We recommend using a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 1) 安装

建议使用虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Usage

Data is saved to:
- **US stocks** -> `data_us/<TICKER>.csv` (daily) or `data_us/<TICKER>_<INTERVAL>.csv` (intraday)
- **CN stocks** -> `data_cn/<TICKER>.csv` (daily) or `data_cn/<TICKER>_<INTERVAL>.csv` (intraday)

You can customize output directory using `--out-dir` or `--csv` parameters.

## 2) 使用

数据默认保存到：
- **美股** -> `data_us/<TICKER>.csv`（日线）或 `data_us/<TICKER>_<INTERVAL>.csv`（分钟线）
- **A股** -> `data_cn/<TICKER>.csv`（日线）或 `data_cn/<TICKER>_<INTERVAL>.csv`（分钟线）

你也可以用 `--out-dir` 或 `--csv` 自定义输出目录。

### Time Intervals Support

Supported time intervals:
- `1d`: Daily data (default)
- `1h`: 1-hour data
- `30m`: 30-minute data
- `15m`: 15-minute data
- `5m`: 5-minute data

Timestamp formats:
- **Daily**: `YYYY-MM-DD` (e.g., 2024-01-15)
- **Intraday**: `YYYY-MM-DD HH:MM:SS` (e.g., 2024-01-15 09:30:00)

### 时间粒度支持

支持以下时间粒度：
- `1d`: 日线数据（默认）
- `1h`: 1小时线数据
- `30m`: 30分钟线数据
- `15m`: 15分钟线数据
- `5m`: 5分钟线数据

时间戳格式：
- **日线**: `YYYY-MM-DD`（如：2024-01-15）
- **分钟线**: `YYYY-MM-DD HH:MM:SS`（如：2024-01-15 09:30:00）

### US Stocks

**Full history download:**

```bash
# Daily data
python fetch_history_us.py --ticker AAPL --start 2010-01-01 --end 2025-12-31 --out-dir data_us

# 5-minute data
python fetch_history_us.py --ticker AAPL --start 2024-01-01 --interval 5m --out-dir data_us

# 1-hour data
python fetch_history_us.py --ticker AAPL --start 2024-01-01 --interval 1h --out-dir data_us
```

**Incremental update:**

```bash
# Daily incremental update
python update_history_us.py --ticker AAPL --csv data_us/AAPL.csv

# 5-minute incremental update
python update_history_us.py --ticker AAPL --csv data_us/AAPL_5m.csv --interval 5m
```

**Notes:**
- Ticker examples: AAPL, MSFT, TSLA, BRK-B (following yfinance conventions)
- `--end` is the exclusive end date, defaults to today
- Intraday data files automatically have interval suffix, e.g., `AAPL_5m.csv`

### 美股

**全量下载:**

```bash
# 日线数据
python fetch_history_us.py --ticker AAPL --start 2010-01-01 --end 2025-12-31 --out-dir data_us

# 5分钟线数据
python fetch_history_us.py --ticker AAPL --start 2024-01-01 --interval 5m --out-dir data_us

# 1小时线数据
python fetch_history_us.py --ticker AAPL --start 2024-01-01 --interval 1h --out-dir data_us
```

**增量更新:**

```bash
# 日线增量更新
python update_history_us.py --ticker AAPL --csv data_us/AAPL.csv

# 5分钟线增量更新
python update_history_us.py --ticker AAPL --csv data_us/AAPL_5m.csv --interval 5m
```

**说明:**
- 股票代码示例：AAPL, MSFT, TSLA, BRK-B（遵循 yfinance 规范）
- `--end` 为不包含当日的截止日期，默认为今天
- 分钟线数据文件名会自动添加间隔后缀，如 `AAPL_5m.csv`

### Chinese A-Shares

**Full history download:**

```bash
# Daily data
python fetch_history_cn.py --ticker 600519 --start 2010-01-01 --out-dir data_cn

# 5-minute data
python fetch_history_cn.py --ticker 600519 --start 2024-01-01 --interval 5m --out-dir data_cn

# 30-minute data
python fetch_history_cn.py --ticker 600519 --start 2024-01-01 --interval 30m --out-dir data_cn
```

**Incremental update:**

```bash
# Daily incremental update
python update_history_cn.py --ticker 600519 --csv data_cn/600519.csv

# 15-minute incremental update
python update_history_cn.py --ticker 600519 --csv data_cn/600519_15m.csv --interval 15m
```

**Notes:**
- Supports various formats: pure numbers, with suffixes, with market prefixes: `600519` / `600519.SH` / `SH600519` / `000001.SZ`, etc. Script automatically normalizes to AkShare format.
- Prioritizes forward-adjusted (qfq), falls back to backward-adjusted (hfq), then to unadjusted data.
- **Important**: A-share intraday data may be limited by data sources, may fallback to daily data in some cases.

### 中国A股

**全量下载:**

```bash
# 日线数据
python fetch_history_cn.py --ticker 600519 --start 2010-01-01 --out-dir data_cn

# 5分钟线数据
python fetch_history_cn.py --ticker 600519 --start 2024-01-01 --interval 5m --out-dir data_cn

# 30分钟线数据
python fetch_history_cn.py --ticker 600519 --start 2024-01-01 --interval 30m --out-dir data_cn
```

**增量更新:**

```bash
# 日线增量更新
python update_history_cn.py --ticker 600519 --csv data_cn/600519.csv

# 15分钟线增量更新
python update_history_cn.py --ticker 600519 --csv data_cn/600519_15m.csv --interval 15m
```

**说明:**
- 支持多种格式：纯数字/带后缀/带市场前缀：`600519` / `600519.SH` / `SH600519` / `000001.SZ` 等，脚本会自动标准化为 AkShare 需要的格式。
- 默认会优先使用前复权（qfq），失败则尝试后复权（hfq），最后无复权。
- **注意**: A股分钟线数据可能受限于数据源，某些情况下会回退到日线数据。

## 3) Data Format

CSV examples:

**Daily data (1d):**
```
timestamps,open,high,low,close,volume,amount
2024-06-18,11.27,11.28,11.26,11.27,379.0,427161.0
2024-06-19,11.28,11.30,11.25,11.29,425.0,479325.0
...
```

**Intraday data (5m, 15m, 30m, 1h):**
```
timestamps,open,high,low,close,volume,amount
2024-06-18 09:30:00,11.27,11.28,11.26,11.27,379.0,427161.0
2024-06-18 09:35:00,11.28,11.30,11.25,11.29,425.0,479325.0
...
```

## 3) 数据格式

CSV 示例：

**日线数据 (1d):**
```
timestamps,open,high,low,close,volume,amount
2024-06-18,11.27,11.28,11.26,11.27,379.0,427161.0
2024-06-19,11.28,11.30,11.25,11.29,425.0,479325.0
...
```

**分钟线数据 (5m, 15m, 30m, 1h):**
```
timestamps,open,high,low,close,volume,amount
2024-06-18 09:30:00,11.27,11.28,11.26,11.27,379.0,427161.0
2024-06-18 09:35:00,11.28,11.30,11.25,11.29,425.0,479325.0
...
```

## 4) Rate Limiting

- Each request includes `polite_sleep(≈1s with jitter)` to reduce rate limiting risk.
- **US** uses `yfinance`, which includes built-in segmentation and retry logic; **CN** uses AkShare, which has good stability.
- If you encounter 0 rows or intermittent failures: retry later, or increase delay (to 2-3 seconds).

## 4) 反爬友好

- 每次请求前会 `polite_sleep(≈1s, 含抖动)`，降低被限流风险。
- **美股**使用 `yfinance`，自带分段与重试；**A股**使用 AkShare，稳定性较好。
- 如果遇到 0 行或间歇性失败：稍后重试、调大延迟（到 2-3 秒）。

## 5) Troubleshooting

- **ImportError**: Some packages not installed
  - Ensure virtual environment is activated and run `pip install -r requirements.txt`
- **US** returns empty data or missing columns
  - Try a different time range; or check if ticker is correct (e.g., BRK-B with hyphen follows yfinance conventions).
- **CN** returns 0 rows
  - Use pure numeric codes for better stability (e.g., `600519`); or retry later.

## 5) 故障排查

- **ImportError**: 某些包未安装
  - 确认已激活虚拟环境，并执行 `pip install -r requirements.txt`
- **美股**返回空数据或缺列
  - 换个时间段；或检查股票代码是否正确（BRK-B 这种带短杠的按 yfinance 规范）。
- **A股**返回 0 行
  - 传入纯数字代码更稳（如 `600519`）；或稍后重试。

## 6) Project Structure

```
fetch_history_us.py      # US stocks full history fetch
update_history_us.py     # US stocks incremental update
fetch_history_cn.py      # CN A-shares full history fetch
update_history_cn.py     # CN A-shares incremental update
utils.py                 # Logging, CSV I/O, delays
requirements.txt         # Dependencies
example_usage.py         # Usage example script
data_us/                 # Suggested US data directory (auto-created)
data_cn/                 # Suggested CN data directory (auto-created)
```

### Quick Start

Run the example script to experience different time interval data fetching:

```bash
python example_usage.py
```

## 6) 项目结构

```
fetch_history_us.py      # 美股全量抓取
update_history_us.py     # 美股增量更新
fetch_history_cn.py      # A股全量抓取
update_history_cn.py     # A股增量更新
utils.py                 # 日志、CSV IO、延时
requirements.txt         # 依赖
example_usage.py         # 使用示例脚本
data_us/                 # 建议的 US 数据目录（脚本自动创建）
data_cn/                 # 建议的 CN 数据目录（脚本自动创建）
```

### 快速开始

运行示例脚本体验不同时间粒度的数据抓取：

```bash
python example_usage.py
```

## 7) Data Sources

- **US**: Yahoo Finance (free, stable, wide coverage). Recommended as a zero-barrier source.
- **CN**: AkShare (aggregating public sources; recommends `stock_zh_a_hist` for A-share daily data).

For stricter SLA or commercial use, consider integrating paid data sources with proper rate limiting and caching strategies.

## 7) 数据源说明

- **美股**: Yahoo Finance（免费、稳定、覆盖广）。推荐作为零门槛来源。
- **A股**: AkShare（聚合公开源；A 股日线推荐 `stock_zh_a_hist`）。

如需更严格的 SLA 或商业用途，建议接入付费数据源并按其限流策略实现重试与缓存。
