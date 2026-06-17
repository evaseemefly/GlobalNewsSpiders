import os
import time
import random
import pandas as pd
import yfinance as yf
import platform
from pathlib import Path
from datetime import datetime, timedelta

# todo 26-06-17: 优先使用标准库 zoneinfo，避免生产环境额外依赖；旧 Python 再回退 pytz。
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None
    import pytz

from apscheduler.schedulers.blocking import BlockingScheduler
from curl_cffi import requests as cffi_requests

# ==========================================
# V2Ray 代理配置 (Ubuntu 环境)
# ==========================================
PROXY_URL = 'http://127.0.0.1:7890'

os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL
if 'NO_PROXY' in os.environ:
    del os.environ['NO_PROXY']

# ==========================================
# 配置目录与资产
# ==========================================
DATA_DIR = Path("/home/evaseemefly/01data/05-spiders") / 'broad_market_history'
MASTER_FILE = DATA_DIR / "historical_broad_market_master.csv"

TICKERS = {
    'VOO': 'VOO',
    'RSP': 'RSP',
    'QQQ': 'QQQ',
    'SMH': 'SMH',
    'HYG': 'HYG',
    'BTC': 'BTC-USD',
    'US10Y': '^TNX',
    'VIX': '^VIX',
}

# todo 26-06-17: 用多个美股资产作为交易日锚点，避免单一 VOO 缺失时误删真实交易日。
US_MARKET_ANCHORS = ('VOO_close', 'RSP_close', 'QQQ_close', 'SMH_close', 'HYG_close')

# todo 26-06-17: 用这些美国市场列识别历史主表里由 ffill 生成的周末/未来脏行。
US_MARKET_PREFIXES = ('VOO', 'QQQ', 'SMH', 'RSP', 'HYG', 'US10Y', 'VIX')

# todo 26-06-17: 16:00 刚收盘时 Yahoo 数据可能仍抖动，18:00 美东后再视为安全收盘日。
US_MARKET_SAFE_CLOSE_HOUR = 18


def get_new_york_now() -> datetime:
    """返回当前美东时间，避免服务器本地时区影响下载窗口。"""
    # todo 26-06-17: 所有日线截止判断都锚定 America/New_York。
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/New_York"))
    return datetime.now(pytz.timezone("US/Eastern"))


def get_last_safe_us_market_date() -> pd.Timestamp:
    """返回最后一个可以安全使用的美股日期。"""
    # todo 26-06-17: 美东 18:00 前不拉取/保留当天美股日线，避免盘中或盘后未稳定数据。
    ny_time = get_new_york_now()
    safe_date = ny_time.date()
    if ny_time.hour < US_MARKET_SAFE_CLOSE_HOUR:
        safe_date -= timedelta(days=1)
    return pd.Timestamp(safe_date)


def get_us_market_end_date() -> str:
    """返回 yfinance history(end=...) 使用的 exclusive end date。"""
    # todo 26-06-17: yfinance 的 end 是不包含当天，所以在安全美股日期基础上加一天。
    end_date = get_last_safe_us_market_date() + pd.Timedelta(days=1)
    return end_date.strftime('%Y-%m-%d')


def normalize_trade_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """统一 trade_date_utc 为无时区的日期时间。"""
    # todo 26-06-17: 合并主表和新数据前统一日期类型，避免字符串/Timestamp 混合导致去重异常。
    df = df.copy()
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc']).dt.normalize()
    return df


def drop_stale_us_market_rows(df: pd.DataFrame) -> pd.DataFrame:
    """删除美国市场列完全沿用上一行的脏行，典型来源是周末/BTC-only 日期被 ffill。"""
    # todo 26-06-17: 旧主表中已经存在的周末/未来 ffill 脏行，不能只靠新数据去重自然消失。
    if df.empty:
        return df

    us_cols = [
        col for col in df.columns
        if any(col.startswith(f'{prefix}_') for prefix in US_MARKET_PREFIXES)
    ]
    if not us_cols:
        return df

    repeated_us_market = df[us_cols].eq(df[us_cols].shift(1)).all(axis=1)
    dropped = int(repeated_us_market.sum())
    if dropped:
        print(f"🧹 已剔除 {dropped} 条美国市场数据完全重复的无效交易日记录")

    return df.loc[~repeated_us_market].copy()


def download_latest_data(last_date: pd.Timestamp) -> pd.DataFrame:
    """自动下载带有安全重叠期的增量数据。"""
    start_date = (last_date - timedelta(days=3)).strftime('%Y-%m-%d')
    end_date = get_us_market_end_date()

    print(f"📡 正在通过防封禁通道下载 {start_date} 至 {end_date} (Exclusive) 的增量数据...")

    proxies_dict = {"http": PROXY_URL, "https": PROXY_URL}
    custom_session = cffi_requests.Session(proxies=proxies_dict, impersonate="chrome110")

    # todo 26-06-17: 用 dict 记录每个资产是否成功，防止缺列新数据覆盖主表。
    data_map = {}

    for name, symbol in TICKERS.items():
        max_retries = 3
        for attempt in range(max_retries):
            try:
                ticker_obj = yf.Ticker(symbol, session=custom_session)
                hist = ticker_obj.history(start=start_date, end=end_date, interval="1d")

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
                    print(f"   ✅ {name} ({symbol}) 获取成功: {len(temp_df)} 条")
                    break

                print(f"   ⚠️ {name} 返回数据为空 (尝试 {attempt + 1}/{max_retries})")

            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    sleep_time = random.uniform(5.0, 15.0)
                    print(f"   ⚠️ 触发风控拦截 ({name})! 冷冻 {sleep_time:.1f} 秒后重试...")
                    time.sleep(sleep_time)
                else:
                    print(f"   ❌ 获取 {name} 失败: {error_msg}")

        time.sleep(random.uniform(2.0, 4.0))

    # todo 26-06-17: 只要任一资产整段下载失败，就放弃本次更新，避免 keep='last' 写入缺列/NaN。
    missing_tickers = [name for name in TICKERS if name not in data_map]
    if missing_tickers:
        print(f"❌ 本次更新缺少资产数据: {missing_tickers}，已放弃写入主表")
        return pd.DataFrame()

    merged = pd.concat([data_map[name] for name in TICKERS], axis=1)
    merged.index.name = 'trade_date_utc'

    # todo 26-06-17: 双保险，下载窗口内也不保留超过美东安全收盘日的日期。
    safe_us_market_date = get_last_safe_us_market_date()
    merged = merged.loc[merged.index <= safe_us_market_date]

    # todo 26-06-17: 先剔除 BTC-only 日期，再 ffill；否则 BTC 会把周末/未来日拖进宽表。
    anchor_cols = [col for col in US_MARKET_ANCHORS if col in merged.columns]
    if not anchor_cols:
        print("❌ 本次更新没有任何美股锚点数据，已放弃写入主表")
        return pd.DataFrame()

    before_drop = len(merged)
    merged = merged[merged[anchor_cols].notna().any(axis=1)]
    dropped = before_drop - len(merged)
    if dropped:
        print(f"🧹 已剔除 {dropped} 条 BTC-only / 非美股交易日记录")

    merged = merged.ffill().dropna()
    merged = merged.reset_index()
    return merged


def update_master():
    print("=== 🚀 每日全自动数据更新开始 ===")

    if MASTER_FILE.exists():
        master = pd.read_csv(MASTER_FILE)
        master = normalize_trade_date_column(master)
        last_date = master['trade_date_utc'].max()
        print(f"当前主文件最后日期: {last_date.date()}  (共 {len(master)} 条记录)")
    else:
        print("⚠️ 主文件不存在，将拉取极长历史数据（可能需要较长时间）")
        last_date = pd.Timestamp('2014-09-16')
        master = pd.DataFrame()

    new_data = download_latest_data(last_date)

    if new_data.empty:
        print("✅ 没有新数据需要更新")
        return

    new_data = normalize_trade_date_column(new_data)
    combined = pd.concat([master, new_data], ignore_index=True)
    combined = normalize_trade_date_column(combined)

    combined = combined.drop_duplicates(subset=['trade_date_utc'], keep='last')
    combined = combined.sort_values('trade_date_utc').reset_index(drop=True)

    # todo 26-06-17: 合并后再次裁剪，删除主表里已经存在的未来/未安全收盘日期。
    safe_us_market_date = get_last_safe_us_market_date()
    before_cutoff = len(combined)
    combined = combined[combined['trade_date_utc'] <= safe_us_market_date]
    cutoff_dropped = before_cutoff - len(combined)
    if cutoff_dropped:
        print(f"🧹 已剔除 {cutoff_dropped} 条晚于安全美股日期 {safe_us_market_date.date()} 的记录")

    combined = drop_stale_us_market_rows(combined)
    combined = combined.sort_values('trade_date_utc').reset_index(drop=True)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_csv(MASTER_FILE, index=False)

    print(f"\n🎉 更新完成！")
    print(f"   主文件当前共 {len(combined)} 条记录")
    print(f"   最新交易日已更新至: {combined['trade_date_utc'].max().date()}")


def main():
    print(f"=== 🚀 宽基指数与大类资产 (日线) 全自动增量更新服务启动 ===")
    print(f"🖥️ 当前运行环境: {platform.system()}")

    update_master()

    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(update_master, 'cron', hour='22', minute='0', id='broad_market_daily_job')

    print("⏳ 宽基日线定时任务已注册 (每日 UTC 22:00 执行一次)，挂机中...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n⏹️ 宽基全自动增量更新服务已安全停止。")


if __name__ == "__main__":
    main()
