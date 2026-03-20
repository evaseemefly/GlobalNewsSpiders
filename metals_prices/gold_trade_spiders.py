import yfinance as yf
import pandas as pd
import arrow
from pathlib import Path
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

# --- 配置区域 ---
SAVE_ROOT_PATH = Path("news_data/market_prices")
TICKERS = ["XAUUSD=X", "XAGUSD=X"]

# 用于去重：记录上一次保存的数据时间戳
last_saved_timestamps = {ticker: None for ticker in TICKERS}

API_KEY = ""
SYMBOL = "XAUUSD"  # 国际金价


def fetch_from_alpha_vantage():
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={SYMBOL}&interval=1min&apikey={API_KEY}'
    r = requests.get(url)
    '''
        'Thank you for using Alpha Vantage! This is a premium endpoint. 
        You may subscribe to any of the premium plans at https://www.alphavantage.co/premium/ to instantly unlock all premium endpoints'
    '''
    data = r.json()

    # 提取最后一条分钟数据
    last_refreshed = data['Meta Data']['3. Last Refreshed']
    latest_price = data['Time Series (1min)'][last_refreshed]

    print(f"最新金价 ({last_refreshed}): {latest_price['4. close']}")

# --- 核心函数 ---
def fetch_and_save_prices():
    """获取分钟级金银价并保存"""
    now_utc = arrow.utcnow()
    date_str = now_utc.format('YYYY-MM-DD')

    print(f"[{now_utc.format('YYYY-MM-DD HH:mm:ss')} UTC] 正在拉取实时行情...")

    try:
        # 核心变动：不传 session，让 yfinance 自动调用内部集成的 curl_cffi
        df = yf.download(
            tickers=TICKERS,
            period="1d",
            interval="5m",  # 你当前代码使用的是 5 分钟线
            progress=False,
            group_by='ticker'
        )

        if df.empty:
            print("未能获取到行情数据，请检查网络（或代理）状态。")
            return

        SAVE_ROOT_PATH.mkdir(parents=True, exist_ok=True)
        csv_file = SAVE_ROOT_PATH / f"precious_metals_{date_str}.csv"

        latest_records = []
        for ticker in TICKERS:
            # 提取最新的一行
            ticker_data = df[ticker].dropna().iloc[-1:]
            if ticker_data.empty:
                continue

            current_ts = arrow.get(ticker_data.index[0]).to('UTC').format('YYYY-MM-DD HH:mm:ss')

            # --- 去重逻辑 ---
            # 如果这一分钟的数据已经存过了，就跳过，防止 CSV 膨胀
            if current_ts == last_saved_timestamps[ticker]:
                continue

            record = {
                "timestamp_utc": current_ts,
                "symbol": ticker,
                "open": float(ticker_data['Open'].iloc[0]),
                "high": float(ticker_data['High'].iloc[0]),
                "low": float(ticker_data['Low'].iloc[0]),
                "close": float(ticker_data['Close'].iloc[0]),
                "adj_close": float(ticker_data['Adj Close'].iloc[0]),
                "volume": int(ticker_data['Volume'].iloc[0]),
                "fetch_time_utc": now_utc.format('YYYY-MM-DD HH:mm:ss')
            }
            latest_records.append(record)
            last_saved_timestamps[ticker] = current_ts  # 更新最后保存记录

        if latest_records:
            save_records(latest_records, csv_file)
        else:
            print("当前时间点无新行情更新。")

    except Exception as e:
        print(f"行情抓取异常: {e}")


def save_records(records, file_path):
    file_exists = file_path.exists()
    keys = records[0].keys()
    with file_path.open("a", newline="", encoding="utf-8") as f:
        import csv
        writer = csv.DictWriter(f, fieldnames=keys)
        if not file_exists:
            writer.writeheader()
        writer.writerows(records)
    print(f"成功写入 {len(records)} 条新行情。")


def main():
    print(f"=== 金银价定时爬虫启动 (UTC: {arrow.utcnow().format('YYYY-MM-DD HH:mm:ss')}) ===")

    # 首次加载时，可以先读取一下 CSV 填充 last_saved_timestamps
    # (篇幅原因省略，若脚本长期运行，内存中的变量即可胜任)

    fetch_and_save_prices()

    scheduler = BlockingScheduler(timezone="UTC")
    # 维持每分钟运行一次的频率，以便在 5 分钟线更新的瞬间捕捉到数据
    scheduler.add_job(fetch_and_save_prices, 'cron', minute='*', id='market_price_job')

    print("-" * 50)
    print("定时任务已注册，自动运行中...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n爬虫已停止。")


if __name__ == "__main__":
    main()
    # fetch_from_alpha_vantage()