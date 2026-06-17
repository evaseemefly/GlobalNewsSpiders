import os
import time
import random
import pandas as pd
import yfinance as yf
import platform
from pathlib import Path
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None
    import pytz

# 【核心新增】：引入 curl_cffi 来构建满足雅虎胃口的底层会话
from curl_cffi import requests as cffi_requests

# ==========================================
# V2Ray 代理配置 (Mac 环境)
# ==========================================
# PROXY_URL = 'http://127.0.0.1:1087'  # 确保你的 V2Ray 端口是 1087
PROXY_URL = 'http://127.0.0.1:7890'  # ubuntu

os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL
if 'NO_PROXY' in os.environ:
    del os.environ['NO_PROXY']

TICKERS = {
    'VOO': 'VOO',
    'QQQ': 'QQQ',
    'SMH': 'SMH',
    'HYG': 'HYG',
    'BTC': 'BTC-USD',
    'US10Y': '^TNX',
    'VIX': '^VIX',
}

START_DATE = "2014-09-16"
INTERVAL = "1d"
US_MARKET_ANCHORS = ('VOO_close', 'QQQ_close', 'SMH_close', 'HYG_close')
US_MARKET_SAFE_CLOSE_HOUR = 18


# ==========================================
# 多端环境自适应路径配置
# ==========================================
def get_save_path() -> Path:
    sys_name = platform.system()
    if sys_name == "Darwin":
        candidates = [
            Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders"),
            Path("/Users/evaseemefly/03data/05-spiders"),
        ]
        base_path = next((path for path in candidates if path.exists()), candidates[0])
    elif sys_name == "Linux":
        base_path = Path("/home/evaseemefly/01data/05-spiders")
    else:
        base_path = Path("./")
    return base_path / "broad_market_history"


def get_new_york_now() -> datetime:
    """返回当前美东时间，避免服务器本地时区影响下载窗口。"""
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/New_York"))
    return datetime.now(pytz.timezone("US/Eastern"))


def get_last_safe_us_market_date() -> pd.Timestamp:
    """返回最后一个可以安全使用的美股日期。"""
    ny_time = get_new_york_now()
    safe_date = ny_time.date()
    if ny_time.hour < US_MARKET_SAFE_CLOSE_HOUR:
        safe_date -= timedelta(days=1)
    return pd.Timestamp(safe_date)


def get_us_market_end_date() -> str:
    """返回 yfinance history(end=...) 使用的 exclusive end date。"""
    end_date = get_last_safe_us_market_date() + pd.Timedelta(days=1)
    return end_date.strftime('%Y-%m-%d')


def fetch_historical_broad_market():
    """批量拉取大盘宽基与跨资产的日线历史数据并融合成宽表"""
    save_dir = get_save_path()
    save_dir.mkdir(parents=True, exist_ok=True)

    # todo:26-06-15 暂时不做现金管理（优先继续优化量化模型）
    # tickers = {
    #     # 风险资产
    #     'VOO': 'VOO',
    #     'QQQ': 'QQQ',
    #     'SMH': 'SMH',
    #
    #     # 信用风险观察
    #     'HYG': 'HYG',
    #
    #     # 其他资产
    #     'BTC': 'BTC-USD',
    #
    #     # 利率与波动率
    #     'US10Y': '^TNX',
    #     'US3M': '^IRX',
    #     'VIX': '^VIX',
    #
    #     # Risk-Off 资金效率模块候选
    #     'SHV': 'SHV',
    #     'BIL': 'BIL',
    #     'SGOV': 'SGOV',
    # }

    print("=== 🚀 开始拉取大类资产历史日线数据 ===")

    end_date = get_us_market_end_date()
    safe_us_market_date = get_last_safe_us_market_date()
    print(f"📅 下载窗口: {START_DATE} 至 {end_date} (Exclusive)")
    print(f"🛡️ 安全美股日期上限: {safe_us_market_date.date()}")

    # ==========================================
    # 构建绑定 V2Ray 的 curl_cffi 会话
    # ==========================================
    proxies_dict = {
        "http": PROXY_URL,
        "https": PROXY_URL
    }

    custom_session = cffi_requests.Session(
        proxies=proxies_dict,
        impersonate="chrome110"
    )

    data_map = {}

    for name, symbol in TICKERS.items():
        max_retries = 3
        for attempt in range(max_retries):
            print(f"📥 正在拉取 {name} ({symbol}) 的历史日线数据 (第 {attempt + 1} 次尝试)...")
            try:
                ticker_obj = yf.Ticker(symbol, session=custom_session)
                hist = ticker_obj.history(start=START_DATE, end=end_date, interval=INTERVAL)

                if not hist.empty:
                    hist.index = hist.index.tz_localize(None).normalize()
                    temp_df = pd.DataFrame({
                        f'{name}_open': hist['Open'].round(4),
                        f'{name}_high': hist['High'].round(4),
                        f'{name}_low': hist['Low'].round(4),
                        f'{name}_close': hist['Close'].round(4),
                        f'{name}_volume': hist['Volume']
                    })
                    data_map[name] = temp_df

                    print(f"✅ {name} 获取成功！共 {len(temp_df)} 条")
                    break
                else:
                    print(f"⚠️ {name} 返回数据为空。")
                    break

            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    if "Too Many Requests" in error_msg or "NoneType" in error_msg:
                        sleep_time = random.uniform(20.0, 40.0)
                        print(f"⚠️ 触发风控拦截 ({name})! 深度冷冻 {sleep_time:.1f} 秒后重试...")
                        time.sleep(sleep_time)
                    else:
                        print(f"⚠️ {name} 网络异常，等待 5 秒后重试... ({error_msg})")
                        time.sleep(5)
                else:
                    print(f"❌ 获取 {name} 3次尝试全部失败: {error_msg}")

        time.sleep(random.uniform(2.0, 5.0))

    missing_tickers = [name for name in TICKERS if name not in data_map]
    if missing_tickers:
        print(f"❌ 以下资产下载失败或为空，已放弃生成历史主表: {missing_tickers}")
        return

    # ==========================================
    # 数据清洗：以美股交易日为锚点，避免 BTC 周末/未来日污染
    # ==========================================
    df_merged = pd.concat([data_map[name] for name in TICKERS], axis=1)
    if not df_merged.empty:
        print("\n🧹 正在处理跨资产日期对齐与清洗...")
        df_merged.index.name = 'trade_date_utc'

        df_merged = df_merged.loc[df_merged.index <= safe_us_market_date]

        anchor_cols = [col for col in US_MARKET_ANCHORS if col in df_merged.columns]
        if not anchor_cols:
            print("❌ 没有任何美股锚点列，已放弃生成历史主表。")
            return

        before_anchor_filter = len(df_merged)
        df_merged = df_merged[df_merged[anchor_cols].notna().any(axis=1)]
        anchor_dropped = before_anchor_filter - len(df_merged)
        if anchor_dropped:
            print(f"🧹 已剔除 {anchor_dropped} 条 BTC-only / 非美股交易日记录")

        before_dropna = len(df_merged)
        df_merged = df_merged.dropna()
        missing_dropped = before_dropna - len(df_merged)
        if missing_dropped:
            print(f"🧹 已剔除 {missing_dropped} 条存在缺失资产数据的记录")

        if not df_merged.empty:
            start_date = df_merged.index.min().strftime('%Y%m%d')
            end_date = df_merged.index.max().strftime('%Y%m%d')
            csv_path = save_dir / f"historical_broad_market_clean_{start_date}_to_{end_date}.csv"
            master_path = save_dir / "historical_broad_market_master.csv"

            df_merged.to_csv(csv_path)
            df_merged.to_csv(master_path)
            print("-" * 50)
            print(f"✅ 历史宽表已落盘: {csv_path.name} (共 {len(df_merged)} 个交易日)")
            print(f"✅ 主表已同步更新: {master_path.name}")
            print(f"📁 存储路径: {save_dir}")
        else:
            print("❌ 数据清洗后全为空。")
    else:
        print("❌ 数据合并失败，DataFrame 为空。")


if __name__ == "__main__":
    fetch_historical_broad_market()
