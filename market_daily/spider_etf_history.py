import os
import time
import random
import pandas as pd
import yfinance as yf
import platform
from pathlib import Path

# 【核心新增】：引入 curl_cffi 来构建满足雅虎胃口的底层会话
from curl_cffi import requests as cffi_requests

# ==========================================
# 🌐 V2Ray 代理配置 (Mac 环境)
# ==========================================
PROXY_URL = 'http://127.0.0.1:1087'  # 确保你的 V2Ray 端口是 1087

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
    return base_path / "broad_market_history"


def fetch_historical_broad_market():
    """批量拉取大盘宽基与跨资产的日线历史数据并融合成宽表"""
    save_dir = get_save_path()
    save_dir.mkdir(parents=True, exist_ok=True)

    tickers = {
        'VOO': 'VOO',
        'QQQ': 'QQQ',
        'SMH': 'SMH',
        'HYG': 'HYG',
        'BTC': 'BTC-USD',
        'US10Y': '^TNX',  # <-- 新增：十年期美债收益率
        'VIX': '^VIX'  # <-- 新增：恐慌指数
    }

    print("=== 🚀 开始拉取大类资产历史日线数据 ===")

    """获取的过去多久的数据："1d" (1天), "5d", "1mo", "3mo", "6mo", "1y" (1年), "5y", "10y", "ytd" (年初至今), "max" """
    PERIOD = "max"
    INTERVAL = "1d"
    df_merged = pd.DataFrame()

    # ==========================================
    # 🛡️ 构建终极防弹盾牌：绑定了 V2Ray 的 curl_cffi 会话
    # ==========================================
    proxies_dict = {
        "http": PROXY_URL,
        "https": PROXY_URL
    }

    # impersonate="chrome110" 会在底层模拟 Chrome 110 的完整 TLS 指纹
    # 这是目前爬虫界绕过 Cloudflare 和雅虎 WAF 的顶级写法规！
    custom_session = cffi_requests.Session(
        proxies=proxies_dict,
        impersonate="chrome110"
    )

    for name, symbol in tickers.items():
        max_retries = 3
        for attempt in range(max_retries):
            print(f"📥 正在拉取 {name} ({symbol}) 过去 {PERIOD} 的日线数据 (第 {attempt + 1} 次尝试)...")
            try:
                # 【修改点 1】：把我们特制的 session 传进去
                ticker_obj = yf.Ticker(symbol, session=custom_session)

                # 【修改点 2】：去掉报错的 proxy 参数，老老实实调用 history
                hist = ticker_obj.history(period=PERIOD, interval=INTERVAL)

                if not hist.empty:
                    # 清洗时区，将精确到秒的时间戳统一格式化为只保留日期 (YYYY-MM-DD)
                    hist.index = hist.index.tz_localize(None).normalize()
                    # 提取收盘价
                    # temp_df = pd.DataFrame({name: hist['Close'].round(4)})
                    # 提取 OHLC，并统一命名格式 (例如: VOO_open, VOO_close)
                    temp_df = pd.DataFrame({
                        f'{name}_open': hist['Open'].round(4),
                        f'{name}_high': hist['High'].round(4),
                        f'{name}_low': hist['Low'].round(4),
                        f'{name}_close': hist['Close'].round(4),
                        # 顺手把成交量也带上，对于股票大盘来说，放量大跌和缩量大跌的意义完全不同
                        f'{name}_volume': hist['Volume']
                    })
                    if df_merged.empty:
                        df_merged = temp_df
                    else:
                        df_merged = df_merged.join(temp_df, how='outer')

                    print(f"✅ {name} 获取成功！")
                    break  # 成功拿回数据，跳出当前的重试循环
                else:
                    print(f"⚠️ {name} 返回数据为空。")
                    break

            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    # 如果遇到 NoneType 报错，说明还是被弹验证码了
                    if "Too Many Requests" in error_msg or "NoneType" in error_msg:
                        sleep_time = random.uniform(20.0, 40.0)
                        print(f"⚠️ 触发风控拦截 ({name})! 深度冷冻 {sleep_time:.1f} 秒后重试...")
                        time.sleep(sleep_time)
                    else:
                        print(f"⚠️ {name} 网络异常，等待 5 秒后重试... ({error_msg})")
                        time.sleep(5)
                else:
                    print(f"❌ 获取 {name} 3次尝试全部失败: {error_msg}")

        # 每次成功抓完一个指标，随机停顿几秒，防范并发封锁
        time.sleep(random.uniform(2.0, 5.0))

    # ==========================================
    # 数据清洗：处理比特币周末交易导致的股市缺失值
    # ==========================================
    if not df_merged.empty:
        print("\n🧹 正在处理跨资产日期对齐与清洗...")
        df_merged.index.name = 'trade_date_utc'

        # ffill() 前向填充处理周末休市
        df_merged = df_merged.ffill()
        df_merged = df_merged.dropna()

        if not df_merged.empty:
            start_date = df_merged.index.min().strftime('%Y%m%d')
            end_date = df_merged.index.max().strftime('%Y%m%d')
            csv_path = save_dir / f"historical_broad_market_{PERIOD}_{start_date}_to_{end_date}.csv"

            df_merged.to_csv(csv_path)
            print("-" * 50)
            print(f"✅ 历史宽表已完美落盘: {csv_path.name} (共 {len(df_merged)} 个交易日)")
            print(f"📁 存储路径: {save_dir}")
        else:
            print("❌ 数据清洗后全为空。")
    else:
        print("❌ 数据合并失败，DataFrame 为空。")


if __name__ == "__main__":
    fetch_historical_broad_market()
