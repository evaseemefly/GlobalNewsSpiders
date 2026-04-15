import os
import csv
import arrow
import yfinance as yf
import platform
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler

# ==========================================
# 🌐 V2Ray 代理配置
# ==========================================
PROXY_URL = 'http://127.0.0.1:1087'  # <-- 确保这是你 V2Ray 的端口

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
    return base_path / "macro_realtime"


SAVE_ROOT_PATH = get_save_path()


def fetch_realtime_macro():
    """实时拉取 yfinance 高频宏观数据并执行三重时间对齐"""
    now_utc = arrow.utcnow()

    # 1. 获取时间 (Fetch Time): 爬虫真实的物理运行时间
    fetch_time_str = now_utc.format('YYYY-MM-DD HH:mm:ss')
    date_str = now_utc.format('YYYY-MM-DD')

    # todo 26-04-15: 2. 网格时间 (Timestamp): 强制向下取整到最近的 5 分钟
    # 例如: 07:30:10 -> 07:30:00, 07:34:22 -> 07:30:00
    minute_floor = now_utc.minute - (now_utc.minute % 5)
    aligned_time = now_utc.replace(minute=minute_floor, second=0, microsecond=0)
    timestamp_utc_str = aligned_time.format('YYYY-MM-DD HH:mm:ss')

    SAVE_ROOT_PATH.mkdir(parents=True, exist_ok=True)
    csv_file_path = SAVE_ROOT_PATH / f"macro_realtime_{date_str}.csv"

    # 初始化记录字典，包含了三种时间维度
    record = {
        'timestamp_utc': timestamp_utc_str,
        'fetch_time_utc': fetch_time_str,
        'DXY': None, 'DXY_time': None,
        'US10Y': None, 'US10Y_time': None,
        'VIX': None, 'VIX_time': None
    }

    print(f"\n[{fetch_time_str} UTC] 🚀 正在拉取宏观指标 (网格基准: {timestamp_utc_str})...")

    tickers = {
        'DXY': 'DX-Y.NYB',
        'US10Y': '^TNX',
        'VIX': '^VIX'
    }

    for key, symbol in tickers.items():
        try:
            ticker_obj = yf.Ticker(symbol)
            hist = ticker_obj.history(period="1d", interval="5m")

            if not hist.empty:
                # 提取最新数值
                record[key] = round(float(hist['Close'].iloc[-1]), 4)

                # todo 26-04-15: 3. 数据时间 (Data Time): 提取雅虎财经返回的真实时间并转为 UTC
                # yfinance 返回的 index 带有纽约时区，我们强制将其统一转换为 UTC
                dt_val = hist.index[-1]
                if dt_val.tzinfo is None:
                    dt_val = dt_val.tz_localize('UTC')  # 防御性转换
                data_time_utc = dt_val.tz_convert('UTC').strftime('%Y-%m-%d %H:%M:%S')
                record[f'{key}_time'] = data_time_utc

                print(f"✅ {key}: {record[key]} (数据时间: {data_time_utc})")
            else:
                record[key] = round(float(ticker_obj.fast_info['lastPrice']), 4)
                record[f'{key}_time'] = "Static/Closed"
                print(f"✅ {key}: {record[key]} [静态缓存]")
        except Exception as e:
            print(f"❌ {key} ({symbol}) 获取异常: {e}")

    # ==========================================
    # 持久化追加写入 CSV
    # ==========================================
    file_exists = csv_file_path.exists()
    # 动态定义列名顺序
    fieldnames = [
        'timestamp_utc', 'fetch_time_utc',
        'DXY', 'DXY_time',
        'US10Y', 'US10Y_time',
        'VIX', 'VIX_time'
    ]

    try:
        with open(csv_file_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)
        print(f"💾 高频数据已保存至: {csv_file_path.name}")
    except Exception as e:
        print(f"❌ 写入 CSV 失败: {e}")
    print("-" * 50)


def main():
    print(f"=== 全球宏观高频数据独立采集服务启动 ===")
    print(f"🖥️ 当前运行环境: {platform.system()}")
    print(f"🌐 当前配置代理: {PROXY_URL}")

    fetch_realtime_macro()

    scheduler = BlockingScheduler(timezone="UTC")
    # 定时器设置在每 5 分钟的第 10 秒运行，确保它抓取的“网格时间”向下取整后极其工整
    scheduler.add_job(fetch_realtime_macro, 'cron', minute='*/5', second='10', id='macro_realtime_job')

    print("⏳ 高频定时任务已注册 (每 5 分钟运行一次)，挂机中...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n⏹️ 高频采集服务已停止。")


if __name__ == "__main__":
    main()
