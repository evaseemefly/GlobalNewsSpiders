import os
import time
import random
import pandas as pd
import yfinance as yf
import platform
from pathlib import Path
from datetime import datetime, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
# 【核心引入】：引入 curl_cffi 来构建满足雅虎胃口的底层会话
from curl_cffi import requests as cffi_requests

# ==========================================
# 🌐 V2Ray 代理配置 (Mac 环境)
# ==========================================
PROXY_URL = 'http://127.0.0.1:1087'  # 确保这是你 V2Ray 的 http 端口

os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL
if 'NO_PROXY' in os.environ:
    del os.environ['NO_PROXY']

# ==========================================
# 配置目录与资产
# ==========================================
DATA_DIR = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history")
MASTER_FILE = DATA_DIR / "historical_broad_market_master.csv"

TICKERS = {
    'VOO': 'VOO',
    'QQQ': 'QQQ',
    'SMH': 'SMH',
    'HYG': 'HYG',
    'BTC': 'BTC-USD',
    'US10Y': '^TNX',
    'VIX': '^VIX',
}


def download_latest_data(last_date: pd.Timestamp) -> pd.DataFrame:
    """自动下载带有安全重叠期的增量数据"""

    # 💡 核心优化：往前多推 3 天作为起点，形成数据重叠区，防止节假日断层
    start_date = (last_date - timedelta(days=3)).strftime('%Y-%m-%d')
    # 结束日期往后推一天，确保能拉到今天的最新数据
    end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"📡 正在通过防封禁通道下载 {start_date} 至 {end_date} 的增量数据...")

    # 🛡️ 构建防弹 Session
    proxies_dict = {"http": PROXY_URL, "https": PROXY_URL}
    custom_session = cffi_requests.Session(proxies=proxies_dict, impersonate="chrome110")

    data_list = []

    for name, symbol in TICKERS.items():
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 放弃 yf.download，改回我们最稳健的 Ticker + Session 模式
                ticker_obj = yf.Ticker(symbol, session=custom_session)
                hist = ticker_obj.history(start=start_date, end=end_date, interval="1d")

                if not hist.empty:
                    # 时区清洗对齐
                    hist.index = hist.index.tz_localize(None).normalize()

                    # 提取特征并规范命名
                    temp_df = pd.DataFrame({
                        f'{name}_open': hist['Open'].round(4),
                        f'{name}_high': hist['High'].round(4),
                        f'{name}_low': hist['Low'].round(4),
                        f'{name}_close': hist['Close'].round(4),
                        f'{name}_volume': hist['Volume']
                    })
                    data_list.append(temp_df)
                    print(f"   ✅ {name} ({symbol}) 获取成功: {len(temp_df)} 条")
                    break  # 成功，跳出重试循环
                else:
                    print(f"   ⚠️ {name} 返回数据为空 (尝试 {attempt + 1}/{max_retries})")

            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    sleep_time = random.uniform(5.0, 15.0)
                    print(f"   ⚠️ 触发风控拦截 ({name})! 冷冻 {sleep_time:.1f} 秒后重试...")
                    time.sleep(sleep_time)
                else:
                    print(f"   ❌ 获取 {name} 失败: {error_msg}")

        # 随机停顿，防止并发封锁
        time.sleep(random.uniform(2.0, 4.0))

    if not data_list:
        print("❌ 未下载到任何新数据")
        return pd.DataFrame()

    # 合并所有资产数据（利用 index 日期自动对齐）
    merged = pd.concat(data_list, axis=1)
    merged.index.name = 'trade_date_utc'

    # 向前填充缺失值（解决美股周末停盘但比特币交易的问题），并去掉全空的行
    merged = merged.ffill().dropna()

    # 把 index 释放为普通列，供后续的 update_master 去重使用
    merged = merged.reset_index()
    return merged


def update_master():
    print("=== 🚀 每日全自动数据更新开始 ===")

    # 1. 读取主文件，确定最后日期
    if MASTER_FILE.exists():
        master = pd.read_csv(MASTER_FILE)
        master['trade_date_utc'] = pd.to_datetime(master['trade_date_utc'])
        last_date = master['trade_date_utc'].max()
        print(f"当前主文件最后日期: {last_date.date()}  (共 {len(master)} 条记录)")
    else:
        print("⚠️ 主文件不存在，将拉取极长历史数据（可能需要较长时间）")
        last_date = pd.Timestamp('2014-09-16')
        master = pd.DataFrame()

    # 2. 自动下载最新增量数据
    new_data = download_latest_data(last_date)

    if new_data.empty:
        print("✅ 没有新数据需要更新")
        return

    # 3. 合并 + 无缝去重缝合
    combined = pd.concat([master, new_data], ignore_index=True)

    # 💡 核心：因为我们的增量拉取包含了重叠的旧日期，这里必须用 keep='last'
    # 这样如果有日期冲突，系统会保留刚刚拉下来的最新数据（修正过的前复权或最新收盘价）
    combined = combined.drop_duplicates(subset=['trade_date_utc'], keep='last')
    combined = combined.sort_values('trade_date_utc').reset_index(drop=True)

    # 4. 保存落盘
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_csv(MASTER_FILE, index=False)

    print(f"\n🎉 更新完成！")
    print(f"   主文件当前共 {len(combined)} 条记录")
    print(f"   最新交易日已更新至: {combined['trade_date_utc'].max().date()}")


def main():
    print(f"=== 🚀 宽基指数与大类资产 (日线) 全自动增量更新服务启动 ===")
    print(f"🖥️ 当前运行环境: {platform.system()}")

    # 1. 启动服务时，先立刻跑一次，确保系统当前数据是最新的
    update_master()

    # 2. 注册定时任务引擎
    scheduler = BlockingScheduler(timezone="UTC")

    # 设定在每天 UTC 时间 22:00 运行一次
    # (美股正常交易时段在美东 16:00 收盘，夏令时对应 UTC 20:00，冬令时对应 UTC 21:00)
    # 设定在 22:00 可以完美避开盘后数据的微调，拿到最稳固的“最终结算价”
    scheduler.add_job(update_master, 'cron', hour='22', minute='0', id='broad_market_daily_job')

    print("⏳ 宽基日线定时任务已注册 (每日 UTC 22:00 执行一次)，挂机中...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n⏹️ 宽基全自动增量更新服务已安全停止。")


if __name__ == "__main__":
    main()
