import os
import time
import random
import pandas as pd
import yfinance as yf
import numpy as np
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path
import arrow
from enum import Enum, auto
from curl_cffi import requests as cffi_requests


# ==================== 1. 定义环境与代理枚举 ====================
class EnvType(Enum):
    HOME = auto()
    WORK = auto()


# ==================== 2. 配置获取函数 ====================
def get_env_config(env: EnvType) -> dict:
    if env == EnvType.HOME:
        base_path = Path("/Users/evaseemefly/03data/05-spiders")
        proxy_url = 'http://127.0.0.1:1087'
    elif env == EnvType.WORK:
        base_path = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
        proxy_url = 'http://127.0.0.1:1087'  # Mac V2Ray 端口
    else:
        raise ValueError(f"未知的环境类型: {env}")

    config = {
        'csv_file': base_path / "broad_market_history/historical_broad_market_master.csv",
        'output_dir': base_path / "output/trade_msg",
        'figures_dir': base_path / "output/trade_msg/figures",
        'ind_stock_dir': base_path / "individual_stocks",  # 个股专用目录
        'proxy_url': proxy_url
    }

    # 创建目录
    config['output_dir'].mkdir(parents=True, exist_ok=True)
    config['figures_dir'].mkdir(parents=True, exist_ok=True)
    config['ind_stock_dir'].mkdir(parents=True, exist_ok=True)

    return config


# ==================== 3. 顶层配置加载 ====================
CURRENT_ENV = EnvType.WORK
CONFIG = get_env_config(CURRENT_ENV)

PROXY_URL = CONFIG['proxy_url']

os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL
if 'NO_PROXY' in os.environ:
    del os.environ['NO_PROXY']

print(f"⚙️ 运行环境: [{CURRENT_ENV.name}]")
print(f"🌐 系统代理已设置为: {PROXY_URL}")
print(f"📂 个股数据将分别保存至: {CONFIG['ind_stock_dir']}\n")


# ==================== 4. 防封禁 Session ====================
def get_custom_session():
    proxies_dict = {"http": PROXY_URL, "https": PROXY_URL}
    return cffi_requests.Session(proxies=proxies_dict, impersonate="chrome110")


# ==================== 5. 个股每日数据下载（按个股单独保存） ====================
def fetch_daily_stock_data(tickers: list, start_date: str = "2020-01-01"):
    print(f"📥 正在通过防封禁通道下载 {tickers} 的每日交易数据...")

    custom_session = get_custom_session()

    for symbol in tickers:
        max_retries = 4
        success = False
        for attempt in range(max_retries):
            try:
                print(f"   📥 {symbol} 第 {attempt + 1}/{max_retries} 次尝试...")
                ticker_obj = yf.Ticker(symbol, session=custom_session)
                hist = ticker_obj.history(start=start_date, end=arrow.now().shift(days=1).format('YYYY-MM-DD'),
                                          interval="1d")

                if not hist.empty:
                    hist.index = hist.index.tz_localize(None).normalize()

                    # 提取数据
                    df_single = pd.DataFrame({
                        f'{symbol}_open': hist['Open'].round(4),
                        f'{symbol}_high': hist['High'].round(4),
                        f'{symbol}_low': hist['Low'].round(4),
                        f'{symbol}_close': hist['Close'].round(4),
                        f'{symbol}_volume': hist['Volume']
                    })
                    df_single.index.name = 'trade_date_utc'
                    df_single = df_single.ffill()

                    # === 关键修改：按个股单独保存 ===
                    save_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{symbol}.csv"
                    df_single.to_csv(save_path)

                    print(f"   ✅ {symbol} 下载成功: {len(df_single)} 条记录 → 已保存 {save_path.name}")
                    success = True
                    break

            except Exception as e:
                if attempt < max_retries - 1:
                    sleep_time = random.uniform(4.0, 12.0)
                    print(f"   ⚠️ {symbol} 触发限流，等待 {sleep_time:.1f} 秒后重试...")
                    time.sleep(sleep_time)
                else:
                    print(f"   ❌ {symbol} 最终失败: {e}")

        if not success:
            print(f"   ⚠️ {symbol} 本次未成功获取数据")

        time.sleep(random.uniform(2.0, 5.0))  # 防止被封

    print(f"\n✅ 所有个股处理完成！数据已分别保存到 {CONFIG['ind_stock_dir']} 目录")


# ==================== 6. 主程序入口 ====================
def main():
    print("=== 🚀 启动个股历史数据防封禁下载引擎 ===")
    # target_stocks = ['NVDA', 'TSLA']
    target_stocks = ['META', 'MSFT']
    fetch_daily_stock_data(target_stocks, start_date="2023-01-01")


if __name__ == "__main__":
    main()
