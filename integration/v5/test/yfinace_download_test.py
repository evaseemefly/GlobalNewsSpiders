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

for symbol in ["SPY", "MSFT", "NVDA"]:
    print("\n" + "=" * 80)
    print(symbol)

    df = yf.Ticker(symbol, session=session).history(
        start="2026-09-10",
        end="2026-09-15",
        interval="1d",
        auto_adjust=False,
        actions=True,
        prepost=False,
        keepna=True,
    )

    cols = [
        "Open", "High", "Low",
        "Close", "Adj Close",
        "Volume", "Dividends", "Stock Splits"
    ]

    cols = [c for c in cols if c in df.columns]

    print(df[cols].to_string())