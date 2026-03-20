import requests
import csv
import arrow
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler

# --- 配置区域 ---
# 替换为你申请到的免费 API Key
TWELVE_DATA_API_KEY = ""
SAVE_ROOT_PATH = Path("news_data/market_prices")
# Twelve Data 的代码格式略有不同
# SYMBOLS = "XAU/USD,XAG/USD"
SYMBOLS = "XAU/USD"

# 用于内存去重
last_saved_ts = {"XAU/USD": None, "XAG/USD": None}


def fetch_market_data():
    now_utc = arrow.utcnow()
    date_str = now_utc.format('YYYY-MM-DD')
    print(f"[{now_utc.format('YYYY-MM-DD HH:mm:ss')} UTC] 正在同步全球行情...")

    # 为了稳定，我们对每个品种发起独立请求
    symbols_to_fetch = ["XAU/USD", "XAG/USD"]
    latest_records = []

    for sym in symbols_to_fetch:
        # 单独请求每一个 symbol
        url = f"https://api.twelvedata.com/time_series?symbol={sym}&interval=1min&apikey={TWELVE_DATA_API_KEY}&outputsize=1"

        try:
            response = requests.get(url, timeout=15)
            data = response.json()

            # 对应你截图中显示的结构：直接从 data 中获取 status 和 values
            if data.get("status") == "ok":
                val = data["values"][0]
                current_ts = arrow.get(val["datetime"]).format('YYYY-MM-DD HH:mm:ss')

                # 去重校验
                if current_ts != last_saved_ts[sym]:
                    record = {
                        "timestamp_utc": current_ts,
                        "symbol": sym.replace("/", ""),  # 存储为 XAUUSD
                        "open": val["open"],
                        "high": val["high"],
                        "low": val["low"],
                        "close": val["close"],
                        "fetch_time_utc": now_utc.format('YYYY-MM-DD HH:mm:ss')
                    }
                    latest_records.append(record)
                    last_saved_ts[sym] = current_ts
            else:
                # 如果 XAG/USD 依然报 403，这里会打印提示，但不会影响 XAU/USD 的抓取
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
    # 每分钟的第 5 秒执行，给 API 一点数据更新的缓冲时间
    scheduler.add_job(fetch_market_data, 'cron', minute='*', second='05', id='market_sync')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n同步服务已停止。")


if __name__ == "__main__":
    main()
