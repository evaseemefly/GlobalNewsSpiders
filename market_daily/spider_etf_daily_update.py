import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime
from pathlib import Path
from enum import Enum, auto
from curl_cffi import requests as cffi_requests   # ← 关键防封禁库

# ==================== 1. V2Ray 代理配置 ====================
# PROXY_URL = 'http://127.0.0.1:7890'   # ← 根据你当前环境修改（Mac 通常是 1087，Ubuntu 是 7890）
PROXY_URL = 'http://127.0.0.1:1087'   # ← 根据你当前环境修改（Mac 通常是 1087，Ubuntu 是 7890）

os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL
if 'NO_PROXY' in os.environ:
    del os.environ['NO_PROXY']

# ==================== 2. 环境配置 ====================
class EnvType(Enum):
    HOME = auto()
    WORK = auto()

def get_env_config(env: EnvType) -> dict:
    if env == EnvType.HOME:
        base_path = Path("/Users/evaseemefly/03data/05-spiders")
    elif env == EnvType.WORK:
        base_path = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
    else:
        raise ValueError(f"未知的环境类型: {env}")

    config = {
        'csv_file': base_path / "broad_market_history/historical_broad_market_master.csv",
        'output_dir': base_path / "output/trade_msg",
        'figures_dir': base_path / "output/trade_msg/figures",
        'individual_stocks_dir': base_path / "output/trade_msg/individual_stocks"
    }

    for key in ['output_dir', 'figures_dir', 'individual_stocks_dir']:
        config[key].mkdir(parents=True, exist_ok=True)

    return config

CURRENT_ENV = EnvType.WORK
CONFIG = get_env_config(CURRENT_ENV)

print(f"⚙️ 运行环境: [{CURRENT_ENV.name}]")
print(f"📂 个股数据将保存至: {CONFIG['individual_stocks_dir']}")

# ==================== 3. 防封禁 Session ====================
def get_custom_session():
    proxies_dict = {"http": PROXY_URL, "https": PROXY_URL}
    return cffi_requests.Session(proxies=proxies_dict, impersonate="chrome110")

# ==================== 4. 个股每日数据下载（带防封禁 + 重试） ====================
def fetch_daily_stock_data(tickers: list, start_date: str = "2023-01-01"):
    print(f"📥 正在通过代理 + curl_cffi 下载 {tickers} 的每日交易数据...")

    custom_session = get_custom_session()
    final_df = pd.DataFrame()

    for ticker in tickers:
        max_retries = 5
        for attempt in range(max_retries):
            try:
                print(f"   📥 {ticker} 第 {attempt+1}/{max_retries} 次尝试...")
                stock = yf.Ticker(ticker, session=custom_session)
                df = stock.history(start=start_date, end=datetime.today().strftime('%Y-%m-%d'), interval="1d")

                if df.empty:
                    print(f"   ⚠️ {ticker} 返回空数据")
                    break

                # 清洗列名
                temp_df = pd.DataFrame({
                    f"{ticker}_open":   df['Open'].round(4),
                    f"{ticker}_high":   df['High'].round(4),
                    f"{ticker}_low":    df['Low'].round(4),
                    f"{ticker}_close":  df['Close'].round(4),
                    f"{ticker}_volume": df['Volume']
                })
                temp_df.index.name = 'trade_date_utc'

                final_df = pd.concat([final_df, temp_df], axis=1) if not final_df.empty else temp_df

                print(f"   ✅ {ticker} 下载成功: {len(df)} 条记录")
                break

            except Exception as e:
                if attempt < max_retries - 1:
                    sleep_time = random.uniform(8, 25)
                    print(f"   ⚠️ {ticker} 触发限流，等待 {sleep_time:.1f} 秒后重试...")
                    time.sleep(sleep_time)
                else:
                    print(f"   ❌ {ticker} 连续失败: {e}")

        time.sleep(random.uniform(2, 5))   # 每次 ticker 间随机间隔

    if final_df.empty:
        print("❌ 所有股票下载失败")
        return None

    final_df = final_df.ffill()
    print(f"✅ 下载完成！共 {len(final_df)} 个交易日的数据。")

    # 保存
    save_path = CONFIG['individual_stocks_dir'] / "nvda_tsla_daily_master.csv"
    final_df.to_csv(save_path)
    print(f"💾 数据已保存至: {save_path}")

    print("\n📊 最新 3 天快照：")
    print(final_df.tail(3))

    return final_df


# ==================== 执行 ====================
if __name__ == "__main__":
    target_stocks = ['NVDA', 'TSLA']
    fetch_daily_stock_data(target_stocks, start_date="2023-01-01")