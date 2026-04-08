import requests
import csv
import arrow
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler

# --- 配置区域 ---
TWELVE_DATA_API_KEY = "fc2d83de92264dd38668ae19f44a806d"
# home
# SAVE_ROOT_PATH = Path("/home/evaseemefly/01data/05-spiders/market_prices")
# workplace
SAVE_ROOT_PATH = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/market_prices")

SYMBOLS = "XAU/USD"

# 用于内存去重
last_saved_ts = {"XAU/USD": None, "XAG/USD": None}


def fetch_market_data():
    now_utc = arrow.utcnow()
    date_str = now_utc.format('YYYY-MM-DD')
    print(f"[{now_utc.format('YYYY-MM-DD HH:mm:ss')} UTC] 正在同步全球行情...")

    symbols_to_fetch = ["XAU/USD", "XAG/USD"]
    latest_records = []

    for sym in symbols_to_fetch:
        # todo 26-04-08: 修复核心漏洞。
        # 1. interval 改为 5min，与你的调度器 5 分钟频次严格对齐，获取标准的 5 分钟 OHLC K 线。
        # 2. 增加 &timezone=UTC 参数，确保 API 返回的时间戳是绝对的 UTC 时间，消除 10 小时时差。
        url = f"https://api.twelvedata.com/time_series?symbol={sym}&interval=5min&apikey={TWELVE_DATA_API_KEY}&outputsize=1&timezone=UTC"

        try:
            response = requests.get(url, timeout=15)
            data = response.json()

            if data.get("status") == "ok":
                val = data["values"][0]
                current_ts = arrow.get(val["datetime"]).format('YYYY-MM-DD HH:mm:ss')

                if current_ts != last_saved_ts[sym]:
                    record = {
                        "timestamp_utc": current_ts,
                        "symbol": sym.replace("/", ""),
                        "open": val["open"],
                        "high": val["high"],
                        "low": val["low"],
                        "close": val["close"],
                        "fetch_time_utc": now_utc.format('YYYY-MM-DD HH:mm:ss')
                    }
                    latest_records.append(record)
                    last_saved_ts[sym] = current_ts
            else:
                print(f"警告: {sym} 获取失败 - {data.get('message', '权限受限')}")

        except Exception as e:
            print(f"请求 {sym} 异常: {e}")

    if latest_records:
        save_to_csv(latest_records, date_str)


def save_to_csv(records, date_str):
    SAVE_ROOT_PATH.mkdir(parents=True, exist_ok=True)
    daily_file = SAVE_ROOT_PATH / f"precious_metals_{date_str}.csv"

    file_exists = daily_file.exists()
    keys = records[0].keys()

    with daily_file.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if not file_exists:
            writer.writeheader()
        writer.writerows(records)
    print(f"成功保存 {len(records)} 条新行情。")


def main():
    print(f"=== 稳定版金银价行情同步启动 ===")
    fetch_market_data()  # 立即执行一次

    scheduler = BlockingScheduler(timezone="UTC")

    # todo 26-04-08: 调度器配置保持不变，每 5 分钟的第 5 秒执行。
    # 现在它会去抓取上一根刚刚闭合的 5 分钟 K 线。
    scheduler.add_job(
        fetch_market_data,
        'cron',
        minute='*/5',
        second='05',
        id='market_sync'
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n同步服务已停止。")


if __name__ == "__main__":
    main()
