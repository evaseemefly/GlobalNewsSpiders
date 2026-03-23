import os
import csv
import arrow
import yfinance as yf
from pathlib import Path

# ==========================================
# 1. 网络与代理配置 (针对 Mac 的 V2Ray)
# ==========================================
PROXY_URL = "http://127.0.0.1:1087"
os.environ["http_proxy"] = PROXY_URL
os.environ["https_proxy"] = PROXY_URL

# ==========================================
# 2. 核心指标与存储路径
# ==========================================
TICKERS = {
    'VIX': '^VIX',  # 恐慌指数
    'US10Y': '^TNX',  # 10年期美债收益率
    'DXY': 'DX-Y.NYB',  # 美元指数
    'WTI': 'CL=F'  # WTI 原油连续合约
}

CSV_FILE_PATH = Path("macro_data.csv")


# ==========================================
# 3. 核心执行逻辑
# ==========================================
def fetch_and_save_to_csv():
    now_utc = arrow.utcnow()
    datetime_str = now_utc.format('YYYY-MM-DD HH:mm:ss')
    timestamp_sec = now_utc.int_timestamp

    record = {
        'datetime_utc': datetime_str,
        'timestamp': timestamp_sec,
        'VIX': None,
        'US10Y': None,
        'DXY': None,
        'WTI': None
    }

    print(f"[{datetime_str} UTC] 🚀 启动宏观数据采集通道...")

    for name, symbol in TICKERS.items():
        try:
            # 【核心修改】：不传任何 session，让 yfinance 内部的 curl_cffi 自动发力
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d")

            if not hist.empty:
                record[name] = round(float(hist['Close'].iloc[-1]), 4)
                print(f"✅ {name:.<8} 成功获取: {record[name]}")
            else:
                print(f"⚠️ {name:.<8} 返回空数据，可能是非交易时段。")
        except Exception as e:
            print(f"❌ {name:.<8} 获取失败: {e}")

    # ==========================================
    # 4. 持久化写入 CSV
    # ==========================================
    file_exists = CSV_FILE_PATH.exists()
    fieldnames = ['datetime_utc', 'timestamp', 'VIX', 'US10Y', 'DXY', 'WTI']

    try:
        with open(CSV_FILE_PATH, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)

        print("-" * 50)
        print(f"💾 数据已成功追加至 {CSV_FILE_PATH.name}")
        print(f"当前记录: {record}")
    except Exception as e:
        print(f"写入 CSV 失败: {e}")


if __name__ == "__main__":
    fetch_and_save_to_csv()