# Stock Crawler / 股票数据抓取脚本

轻量级命令行脚本，抓取并维护 美股/中国A股 的日线历史数据（CSV）。

- US: Yahoo Finance via `yfinance`
- CN: AkShare（Eastmoney 等公开源）

CSV 统一列（逗号分隔）：`timestamps,open,high,low,close,volume,amount`
说明：
- `timestamps` 为日期字符串 YYYY-MM-DD
- US 源（Yahoo）不直接提供成交额，脚本使用 `amount = close * volume` 估算；CN 源若提供则直接写入

## 1) 安装 / Install

建议使用虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) 使用 / Usage

数据默认保存到：
- 美股 US -> `data_us/<TICKER>.csv`
- A股 CN -> `data_cn/<TICKER>.csv`

你也可以用 `--out-dir` 或 `--csv` 自定义。

### US 美股

- 全量下载 / Full history

```bash
python fetch_history_us.py --ticker AAPL --start 2010-01-01 --end 2025-12-31 --out-dir data_us
```

- 增量更新 / Incremental update

```bash
python update_history_us.py --ticker AAPL --csv data_us/AAPL.csv
```

说明 / Notes:
- Ticker 示例：AAPL, MSFT, TSLA, BRK-B（yfinance 规范）
- `--end` 为不包含当日的截止日期，默认今天。

### CN 中国A股

- 全量下载 / Full history

```bash
python fetch_history_cn.py --ticker 600519 --start 2010-01-01 --out-dir data_cn
```

- 增量更新 / Incremental update

```bash
python update_history_cn.py --ticker 600519 --csv data_cn/600519.csv
```

说明 / Notes:
- 支持 纯数字/带后缀/带市场前缀：`600519` / `600519.SH` / `SH600519` / `000001.SZ` 等，脚本会自动标准化为 AkShare 需要的格式。
- 默认会优先使用前复权（qfq），失败则尝试后复权（hfq），最后无复权。

## 3) 数据格式 / Data format

CSV 示例：

```
timestamps,open,high,low,close,volume,amount
2024-06-18,11.27,11.28,11.26,11.27,379.0,427161.0
...
```

## 4) 反爬友好 / Rate limiting

- 每次请求前会 `polite_sleep(≈1s, 含抖动)`，降低被限流风险。
- US 使用 `yfinance`，自带分段与重试；CN 使用 AkShare，稳定性较好。
- 如果遇到 0 行或间歇性失败：稍后重试、调大延迟（到 2-3 秒）。

## 5) 故障排查 / Troubleshooting

- ImportError: 某些包未安装
	- 确认已激活虚拟环境，并执行 `pip install -r requirements.txt`
- US 返回空数据或缺列
	- 换个时间段；或检查 ticker 是否正确（BRK-B 这种带短杠的按 yfinance 规范）。
- CN 返回 0 行
	- 传入纯数字代码更稳（如 `600519`）；或稍后重试。

## 6) 项目结构 / Project structure

```
fetch_history_us.py      # 美股全量抓取
update_history_us.py     # 美股增量更新
fetch_history_cn.py      # A股全量抓取
update_history_cn.py     # A股增量更新
utils.py                 # 日志、CSV IO、延时
requirements.txt         # 依赖
data_us/                 # 建议的 US 数据目录（脚本自动创建）
data_cn/                 # 建议的 CN 数据目录（脚本自动创建）
```

## 7) 数据源说明 / Data sources

- US: Yahoo Finance（免费、稳定、覆盖广）。推荐作为零门槛来源。
- CN: AkShare（聚合公开源；A 股日线推荐 `stock_zh_a_hist`）。

如需更严格的 SLA 或商业用途，建议接入付费数据源并按其限流策略实现重试与缓存。
