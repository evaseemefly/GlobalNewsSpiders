import os
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

symbol = "SPY"

for auto_adjust in [True, False]:
    print("\n" + "=" * 70)
    print("auto_adjust =", auto_adjust)

    try:
        ticker = yf.Ticker(symbol, session=session)

        df = ticker.history(
            start="2026-09-10",
            end="2026-09-15",
            interval="1d",
            auto_adjust=auto_adjust,
            actions=True,
            prepost=False,
            keepna=True,
        )

        print(df.tail())

    except Exception as e:
        print("ERROR:", repr(e))