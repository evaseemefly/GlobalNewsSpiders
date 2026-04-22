import os
import csv
import arrow
import random
import time
import yfinance as yf
import platform
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler

# ==========================================
# 🌐 代理配置 (请根据部署环境修改端口：Mac通常1087，Ubuntu通常7890)
# ==========================================
# mac
# PROXY_URL = 'http://127.0.0.1:7890'
# ubuntu
PROXY_URL = 'http://127.0.0.1:1087'

os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL
if 'NO_PROXY' in os.environ:
    del os.environ['NO_PROXY']


# ==========================================
# 多端环境自适应路径配置
# ==========================================
def get_save_path() -> Path:
    sys_name = platform.system()
    if sys_name == "Darwin":
        base_path = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
    elif sys_name == "Linux":
        base_path = Path("/home/evaseemefly/01data/05-spiders")
    else:
        base_path = Path("./")
    # 使用推荐的宽基日线目录名
    return base_path / "broad_market_daily"


SAVE_ROOT_PATH = get_save_path()


def fetch_daily_broad_market():
    """每日拉取一次宽基与跨资产日线收盘数据"""
    now_utc = arrow.utcnow()

    # 因为是日线数据，我们以“交易日期”作为主键
    fetch_time_str = now_utc.format('YYYY-MM-DD HH:mm:ss')

    # 注意：为了防止跨时区导致日期错乱，我们统一以 UTC 日期为基准
    date_str = now_utc.format('YYYY-MM-DD')

    SAVE_ROOT_PATH.mkdir(parents=True, exist_ok=True)
    # 按月或按年存储均可，这里按月存储方便管理 (例如: broad_market_daily_2026-04.csv)
    month_str = now_utc.format('YYYY-MM')
    csv_file_path = SAVE_ROOT_PATH / f"broad_market_daily_{month_str}.csv"

    # 初始化记录字典
    record = {
        'trade_date_utc': date_str,
        'fetch_time_utc': fetch_time_str,
        'VOO': None, 'QQQ': None, 'SMH': None,
        'HYG': None, 'BTC': None
    }

    print(f"\n[{fetch_time_str} UTC] 🚀 正在执行宽基指数日线收盘数据抓取...")

    tickers = {
        'VOO': 'VOO',  # 标普500 ETF
        'QQQ': 'QQQ',  # 纳斯达克100 ETF
        'SMH': 'SMH',  # 半导体 ETF (先行指标)
        'HYG': 'HYG',  # 高收益债 ETF (风险偏好指标)
        'BTC': 'BTC-USD'  # 比特币 (流动性指标)
    }

    for key, symbol in tickers.items():
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 依赖 yf 底层 curl_cffi 防御机制
                ticker_obj = yf.Ticker(symbol)
                # 获取最近 1 天的日线数据
                hist = ticker_obj.history(period="1d", interval="1d")

                if not hist.empty:
                    record[key] = round(float(hist['Close'].iloc[-1]), 4)
                    print(f"✅ {key} ({symbol}): {record[key]}")
                else:
                    record[key] = round(float(ticker_obj.fast_info['lastPrice']), 4)
                    print(f"✅ {key} ({symbol}) [静态缓存]: {record[key]}")

                break  # 成功，跳出重试循环

            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    if "Too Many Requests" in error_msg or "Rate limited" in error_msg:
                        sleep_time = random.uniform(30.0, 60.0)
                        print(f"⚠️ 触发风控墙 ({key})! 深度冷冻 {sleep_time:.1f} 秒后重试...")
                        time.sleep(sleep_time)
                    else:
                        print(f"⚠️ {key} 网络异常，等待 5 秒后重试... ({error_msg})")
                        time.sleep(5)
                else:
                    print(f"❌ {key} ({symbol}) 3次尝试全部失败: {error_msg}")

        # 随机休眠打乱节奏
        time.sleep(random.uniform(2.0, 4.0))

    # ==========================================
    # 持久化追加写入 CSV
    # ==========================================
    file_exists = csv_file_path.exists()
    fieldnames = ['trade_date_utc', 'fetch_time_utc', 'VOO', 'QQQ', 'SMH', 'HYG', 'BTC']

    try:
        with open(csv_file_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)
        print(f"💾 宽基日线数据已保存至: {csv_file_path.name}")
    except Exception as e:
        print(f"❌ 写入 CSV 失败: {e}")
    print("-" * 50)


def main():
    print(f"=== 宽基指数与大类资产 (日线) 独立采集服务启动 ===")
    print(f"🖥️ 当前运行环境: {platform.system()}")

    # 启动时先测试拉取一次
    fetch_daily_broad_market()

    scheduler = BlockingScheduler(timezone="UTC")
    # 设定在每天 UTC 时间 22:00 运行一次
    # (美股正常交易时段在美东 16:00 收盘，夏令时对应 UTC 20:00，冬令时对应 UTC 21:00)
    # 设定在 22:00 可以确保拿到极其稳固的“盘后最终结算价”
    scheduler.add_job(fetch_daily_broad_market, 'cron', hour='22', minute='0', id='broad_market_daily_job')

    print("⏳ 宽基日线定时任务已注册 (每日 UTC 22:00 执行一次)，挂机中...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n⏹️ 宽基采集服务已停止。")


if __name__ == "__main__":
    main()
