#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据爬虫使用示例 - 展示不同时间粒度的数据抓取

运行前请确保已安装依赖：
pip install -r requirements.txt
"""

import os
import subprocess
from datetime import datetime, timedelta

def run_command(cmd):
    """运行命令并打印输出"""
    print(f"\n执行命令: {cmd}")
    print("-" * 50)
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd="/Users/peixinliu/Develop/projects/stock-data-crawler")
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print("错误信息:", result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"执行失败: {e}")
        return False

def main():
    print("股票数据爬虫 - 时间粒度示例")
    print("=" * 60)
    
    # 设置测试日期范围（最近几天）
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    print(f"测试日期范围: {start_date} 到 {end_date}")
    
    # 1. 美股示例
    print("\n🇺🇸 美股数据示例:")
    
    # 日线数据
    print("\n1.1 下载 AAPL 日线数据:")
    run_command(f"python fetch_history_us.py --ticker AAPL --start {start_date} --end {end_date} --interval 1d")
    
    # 1小时线数据 (注意：Yahoo Finance分钟线数据有时间限制)
    print("\n1.2 下载 AAPL 1小时线数据:")
    recent_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    run_command(f"python fetch_history_us.py --ticker AAPL --start {recent_date} --interval 1h")
    
    # 2. 中国A股示例
    print("\n\n🇨🇳 中国A股数据示例:")
    
    # 日线数据
    print("\n2.1 下载贵州茅台(600519)日线数据:")
    run_command(f"python fetch_history_cn.py --ticker 600519 --start {start_date} --end {end_date} --interval 1d")
    
    # 分钟线数据 (可能回退到日线)
    print("\n2.2 尝试下载贵州茅台(600519) 5分钟线数据:")
    run_command(f"python fetch_history_cn.py --ticker 600519 --start {start_date} --interval 5m")
    
    # 3. 显示生成的文件
    print("\n📁 生成的数据文件:")
    print("-" * 30)
    
    data_dirs = ["data_us", "data_cn"]
    for data_dir in data_dirs:
        if os.path.exists(data_dir):
            print(f"\n{data_dir}/:")
            for file in os.listdir(data_dir):
                if file.endswith(".csv"):
                    filepath = os.path.join(data_dir, file)
                    size = os.path.getsize(filepath)
                    print(f"  {file} ({size} bytes)")
                    
                    # 显示前几行内容
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            lines = f.readlines()[:3]  # 显示前3行
                            for line in lines:
                                print(f"    {line.strip()}")
                            if len(lines) > 3:
                                print("    ...")
                    except Exception as e:
                        print(f"    读取文件失败: {e}")
    
    print("\n✅ 示例完成！")
    print("\n💡 提示:")
    print("- 日线数据时间格式: YYYY-MM-DD")
    print("- 分钟线数据时间格式: YYYY-MM-DD HH:MM:SS")
    print("- 美股分钟线数据受Yahoo Finance限制，只能获取最近60天")
    print("- A股分钟线数据受AkShare限制，可能回退到日线数据")
    print("- 使用 --interval 参数选择时间粒度: 1d, 1h, 30m, 15m, 5m")

if __name__ == "__main__":
    main()