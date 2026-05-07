import os
import time
import random
import requests
import pandas as pd
import yfinance as yf
from pathlib import Path
from enum import Enum, auto
from curl_cffi import requests as cffi_requests

# ==================== 1. 定义环境与代理 ====================
class EnvType(Enum):
    HOME = auto()
    WORK = auto()

def get_env_config(env: EnvType) -> dict:
    if env == EnvType.HOME:
        base_path = Path("/Users/evaseemefly/03data/05-spiders")
        proxy_url = 'http://127.0.0.1:1087'
    elif env == EnvType.WORK:
        base_path = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
        proxy_url = 'http://127.0.0.1:1087'
    else:
        raise ValueError(f"未知的环境类型: {env}")

    config = {
        'ind_stock_dir': base_path / "individual_stocks",
        'fundamental_dir': base_path / "output/trade_msg/fundamental",
        'proxy_url': proxy_url
    }

    config['fundamental_dir'].mkdir(parents=True, exist_ok=True)
    return config

CURRENT_ENV = EnvType.WORK
CONFIG = get_env_config(CURRENT_ENV)

PROXY_URL = CONFIG['proxy_url']
os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL
if 'NO_PROXY' in os.environ:
    del os.environ['NO_PROXY']

print(f"⚙️ 运行环境: [{CURRENT_ENV.name}]")
print(f"🌐 代理: {PROXY_URL}")
print(f"📂 基本面结果保存目录: {CONFIG['fundamental_dir']}\n")

# ==================== 2. 防封禁 Session ====================
def get_custom_session(use_cffi=True):
    """
    获取网络请求 Session。
    use_cffi: 默认尝试使用 curl_cffi 伪装。如果报错，可设为 False 退回标准 requests。
    """
    proxies_dict = {"http": PROXY_URL, "https": PROXY_URL}

    if use_cffi:
        try:
            # 尝试使用 curl_cffi 伪装浏览器
            return cffi_requests.Session(proxies=proxies_dict, impersonate="chrome116")  # 升级一下模拟的浏览器版本
        except Exception as e:
            print(f"⚠️ curl_cffi 初始化失败，降级为标准 requests: {e}")

    # 优雅降级：使用标准 requests 库
    session = requests.Session()
    session.proxies.update(proxies_dict)
    # 添加基础的伪装请求头
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    return session

# ==================== 3. 单个个股基本面打分（修复版） ====================
def get_fundamental_score(ticker_symbol: str) -> dict:
    max_retries = 3

    for attempt in range(max_retries):
        try:
            # 策略：第一次尝试用 cffi 伪装，如果失败，后续重试使用标准 requests
            use_cffi_flag = True if attempt == 0 else False
            session = get_custom_session(use_cffi=use_cffi_flag)

            ticker = yf.Ticker(ticker_symbol, session=session)
            info = ticker.info

            # 如果能成功取到 info，说明网络通了，跳出重试循环，继续执行后面的打分逻辑
            if not info or len(info) == 0:
                raise ValueError("获取到的 info 为空字典")

            # ======== 下面继续接你之前的完美打分逻辑 ========
            # 1. 估值 (PE 越低越好)
            pe = info.get('trailingPE') or info.get('forwardPE')
            valuation_score = max(0, 20 - (pe / 2.5)) if pe is not None and pe > 0 else 0

            # 2. 盈利能力 (ROE)
            roe = info.get('returnOnEquity')
            roe_score = min(20, roe * 100) if roe is not None and roe > 0 else 0

            # 3. 成长性
            growth = info.get('earningsGrowth')
            growth_score = min(20, growth * 200) if growth is not None and growth > 0 else 0

            # 4. 财务健康 (debtToEquity) —— 关键修复
            debt_ratio = info.get('debtToEquity')
            if debt_ratio is not None and debt_ratio >= 0:
                actual_debt_percent = debt_ratio if debt_ratio > 10 else debt_ratio * 100
                health_score = max(0, 20 - (actual_debt_percent / 10))
            else:
                health_score = 0

            fundamental_score = valuation_score + roe_score + growth_score + health_score

            result = {
                'ticker': ticker_symbol,
                'fundamental_score': round(fundamental_score, 1),
                'pe': round(pe, 2) if pe is not None else "N/A",
                'roe_%': round(roe * 100, 2) if roe is not None else "N/A",
                'earnings_growth_%': round(growth * 100, 2) if growth is not None else "N/A",
                'debt_to_equity': round(debt_ratio, 2) if debt_ratio is not None else "N/A",
                'valuation_score': round(valuation_score, 1),
                'roe_score': round(roe_score, 1),
                'growth_score': round(growth_score, 1),
                'health_score': round(health_score, 1)
            }

            # === 保存逻辑 ===
            save_path = CONFIG['fundamental_dir'] / f"fundamental_score_{ticker_symbol}.csv"
            pd.DataFrame([result]).to_csv(save_path, index=False)
            print(f"   ✅ {ticker_symbol} 基本面得分计算完成 → {save_path.name}")

            return result

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"   🔄 {ticker_symbol} 获取失败 (第{attempt + 1}次)，等待2秒后重试... 错误: {e}")
                time.sleep(2)
            else:
                print(f"⚠️ ❌ {ticker_symbol} 彻底获取失败 (已重试{max_retries}次): {e}")
                return {'ticker': ticker_symbol, 'fundamental_score': 0.0}


# ==================== 4. 主程序：手动指定股票列表 ====================
def main():
    print("=== 🚀 个股基本面打分引擎启动 ===")

    # ==================== 在这里手动添加你要计算的股票 ====================
    target_stocks = ['NVDA', 'TSLA', 'META', 'MSFT', 'AAPL']   # ← 你想算哪只就加哪只

    results = []
    for ticker in target_stocks:
        score = get_fundamental_score(ticker)
        results.append(score)
        time.sleep(random.uniform(1.5, 3.5))   # 避免被 Yahoo 限流

    # 生成汇总表
    df_summary = pd.DataFrame(results)
    df_summary = df_summary.sort_values('fundamental_score', ascending=False)

    summary_path = CONFIG['fundamental_dir'] / "fundamental_scores_summary.csv"
    df_summary.to_csv(summary_path, index=False)

    print("\n=== 🎯 所有个股基本面打分汇总 ===")
    print(df_summary.to_string(index=False))
    print(f"\n💾 汇总表已保存: {summary_path}")

    print(f"\n✅ 每只股票的独立文件已分别保存在 {CONFIG['fundamental_dir']} 目录下")


if __name__ == "__main__":
    main()