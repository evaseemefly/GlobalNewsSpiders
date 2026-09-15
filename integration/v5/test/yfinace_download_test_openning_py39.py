import os
import pandas as pd
import yfinance as yf
from curl_cffi import requests as cffi_requests

PROXY = "http://127.0.0.1:1087"

os.environ["HTTP_PROXY"] = PROXY
os.environ["HTTPS_PROXY"] = PROXY

session = cffi_requests.Session(
    proxies={
        "http": PROXY,
        "https": PROXY,
    },
    impersonate="chrome110",
)

symbols = ["SPY", "MSFT", "NVDA"]

for symbol in symbols:

    print("\n" + "=" * 90)
    print(symbol)

    for interval in ["1m", "5m"]:

        print("\n--- interval =", interval, "---")

        try:
            df = yf.Ticker(symbol, session=session).history(
                start="2026-09-14",
                end="2026-09-15",
                interval=interval,
                auto_adjust=False,
                actions=False,
                prepost=False,
                keepna=True,
            )

            if df.empty:
                print("EMPTY")
                continue

            print("rows:", len(df))
            print("first:")
            print(df.head(3).to_string())

            print("\nlast:")
            print(df.tail(5).to_string())

            # 只取正常交易时段
            intraday = df.copy()

            if intraday.index.tz is not None:
                intraday = intraday.tz_convert("America/New_York")

            intraday = intraday.between_time("09:30", "16:00")

            intraday = intraday.dropna(
                subset=["Open", "High", "Low", "Close"]
            )

            if intraday.empty:
                print("\n没有有效正常交易时段数据")
                continue

            day_open = float(intraday["Open"].iloc[0])
            day_high = float(intraday["High"].max())
            day_low = float(intraday["Low"].min())
            day_close = float(intraday["Close"].iloc[-1])
            day_volume = float(intraday["Volume"].sum())

            print("\n重建日K:")
            print("Open   =", day_open)
            print("High   =", day_high)
            print("Low    =", day_low)
            print("Close  =", day_close)
            print("Volume =", day_volume)

        except Exception as exc:
            print("ERROR:", repr(exc))