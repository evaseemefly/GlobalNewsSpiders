import os
import time
import random
import numpy as np
import pandas as pd
import yfinance as yf
import requests
from pathlib import Path
from datetime import datetime
from enum import Enum, auto
from curl_cffi import requests as cffi_requests
import matplotlib.pyplot as plt


# ==================== 1. 环境与代理配置 ====================
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
        'master_macro': base_path / "broad_market_history/historical_broad_market_master.csv",
        'trade_msg_dir': base_path / "output/trade_msg",
        'proxy_url': proxy_url
    }
    config['fundamental_dir'].mkdir(parents=True, exist_ok=True)
    config['trade_msg_dir'].mkdir(parents=True, exist_ok=True)
    return config


CURRENT_ENV = EnvType.WORK
CONFIG = get_env_config(CURRENT_ENV)

PROXY_URL = CONFIG['proxy_url']
os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL
if 'NO_PROXY' in os.environ:
    del os.environ['NO_PROXY']

print(f"⚙️ 运行环境: [{CURRENT_ENV.name}]")
print(f"🌐 代理配置: {PROXY_URL}")


# ==================== 2. 防封禁 Session ====================
def get_custom_session(use_cffi=True):
    proxies_dict = {"http": PROXY_URL, "https": PROXY_URL}
    if use_cffi:
        try:
            return cffi_requests.Session(proxies=proxies_dict, impersonate="chrome116")
        except Exception as e:
            print(f"⚠️ curl_cffi 初始化失败，降级为 requests: {e}")

    session = requests.Session()
    session.proxies.update(proxies_dict)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    return session


# ==================== 3. 核心数学模型 ====================
def calculate_rsi(series: pd.Series, period: int = 14) -> float:
    """计算真实 RSI"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]


def calculate_sigmoid_position(score, midpoint=62, k=0.12):
    """S型仓位曲线：低分严控敞口，高分稳健重仓"""
    pos = 1 / (1 + np.exp(-k * (score - midpoint)))
    return round(float(pos), 4)


def calculate_vix_discount(vix):
    """宏观风险折扣"""
    if vix > 40: return 0.6
    if vix > 35: return 0.8
    return 1.0


# ==================== 4. 动态基本面打分 (带网络重试与 PEG 逻辑) ====================
def fetch_and_score_fundamentals(ticker_symbol: str) -> dict:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            use_cffi_flag = True if attempt == 0 else False
            session = get_custom_session(use_cffi=use_cffi_flag)

            ticker = yf.Ticker(ticker_symbol, session=session)
            info = ticker.info

            if not info or len(info) == 0:
                raise ValueError("获取到的 info 为空字典")

            # --- 1. 估值项：PEG (权重 20分) ---
            pe = info.get('trailingPE') or info.get('forwardPE')
            growth = info.get('earningsGrowth') or 0
            safe_growth = max(0.01, min(0.6, growth))  # 限制增速防止分母异常或数据溢出

            if pe and pe > 0:
                peg = pe / (safe_growth * 100)
                if peg <= 1.0:
                    val_score = 20
                elif peg <= 2.0:
                    val_score = 20 - (peg - 1.0) * 15
                else:
                    val_score = 0
            else:
                val_score = 0;
                peg = 999

            # --- 2. 盈利能力：ROE (权重 30分) ---
            roe = info.get('returnOnEquity') or 0
            roe_score = min(30, roe * 150)

            # --- 3. 成长性 (权重 30分) ---
            growth_score = min(30, safe_growth * 100)

            # --- 4. 财务健康：Debt/Equity (权重 20分) ---
            debt = info.get('debtToEquity') or 200
            actual_debt = debt if debt > 10 else debt * 100
            health_score = max(0, 20 - (actual_debt / 10))

            total_score = val_score + roe_score + growth_score + health_score

            result = {
                'ticker': ticker_symbol,
                'fundamental_score': round(total_score, 1),
                'peg': round(peg, 2) if peg != 999 else "N/A",
                'pe': round(pe, 2) if pe is not None else "N/A",
                'roe_%': round(roe * 100, 2),
                'earnings_growth_%': round(growth * 100, 2),
                'debt_to_equity': round(actual_debt, 2),
                'details': f"Val:{val_score:.1f}/ROE:{roe_score:.1f}/Gro:{growth_score:.1f}/Hea:{health_score:.1f}"
            }

            # 自动保存基本面历史到 CSV
            save_path = CONFIG['fundamental_dir'] / f"fundamental_score_{ticker_symbol}.csv"
            pd.DataFrame([result]).to_csv(save_path, index=False)
            print(f"   ✅ {ticker_symbol} 基本面获取并计算完成")

            return result

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"   🔄 {ticker_symbol} 网络波动 (第{attempt + 1}次)，等待2秒重试... {e}")
                time.sleep(2)
            else:
                print(f"⚠️ ❌ {ticker_symbol} 彻底获取失败: {e}")
                return {'ticker': ticker_symbol, 'fundamental_score': 0, 'peg': 'N/A', 'roe_%': 0,
                        'details': 'Network Error'}


# ==================== 5. 信号生成引擎 ====================
def generate_daily_signal(ticker: str):
    price_file = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"

    if not price_file.exists():
        print(f"⏩ 跳过 {ticker}：找不到本地量价文件 ({price_file.name})")
        return None  # 👈 确保缺失文件时返回 None，不影响画图收集

    # 1. 执行网络请求，获取最新基本面并打分
    fund = fetch_and_score_fundamentals(ticker)

    # 2. 读取宏观数据获取 VIX
    macro_df = pd.read_csv(CONFIG['master_macro'])
    vix = macro_df['VIX_close'].iloc[-1]

    # 3. 读取本地个股历史，计算技术面 Trigger
    df = pd.read_csv(price_file)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.sort_values('trade_date_utc')

    latest_close = df[f'{ticker}_close'].iloc[-1]
    ma200 = df[f'{ticker}_close'].rolling(200).mean().iloc[-1]
    rsi14 = calculate_rsi(df[f'{ticker}_close'])

    # 扳机逻辑：价格在 200日均线之上，或 RSI 出现极度超卖 (<32)
    is_trend_up = latest_close > ma200
    is_oversold = rsi14 < 32
    trigger_pulled = is_trend_up or is_oversold

    # 4. 风控数学计算：基础仓位 * 宏观折扣
    base_pos_ratio = calculate_sigmoid_position(fund['fundamental_score'])
    macro_discount = calculate_vix_discount(vix)

    # 若技术面不满足开枪条件，最终仓位归零 (但在图表上，我们依然会画出它的基础理论仓位)
    final_pos = (base_pos_ratio * macro_discount * 100) if trigger_pulled else 0

    # 5. 输出精美分析报告
    status = "🟢 允许操作" if trigger_pulled else "🔴 趋势不佳/观望"
    report = f"""
【{ticker} 智能信号卡片 | {datetime.now().strftime('%Y-%m-%d')}】
-------------------------------------------
[1] 决策扳机: {status}
    收盘价: {latest_close:.2f} (MA200: {ma200:.2f})
    RSI(14): {rsi14:.1f}

[2] 仓位指引 (核心风控)
    基本面得分: {fund['fundamental_score']} / 100
    PEG指标: {fund['peg']} | ROE: {fund['roe_%']}%
    S型基础仓位: {base_pos_ratio * 100:.1f}%
    宏观折扣(VIX={vix}): {macro_discount}x

🎯 最终建议持仓: {final_pos:.1f}%
-------------------------------------------
策略详情: {fund['details']}
"""
    # 写入 TXT 文件供日常审阅
    save_path = CONFIG['trade_msg_dir'] / f"{ticker}_signal_v2.txt"
    save_path.write_text(report, encoding='utf-8')
    print(report)

    # 👈 核心修改：将数据返回给主程序用于画图
    return {
        'ticker': ticker,
        'score': fund['fundamental_score'],
        'pos': base_pos_ratio * 100  # 传递给图表的是基础仓位，用于直观展示系统评估
    }


# ==================== 6. 可视化绘图模块 ====================
def plot_sigmoid_positions(stock_results, midpoint=62, k=0.12):
    """绘制 S型仓位曲线及个股位置"""
    plt.figure(figsize=(10, 6))

    # 生成曲线数据
    x = np.linspace(0, 100, 200)
    y = 100 / (1 + np.exp(-k * (x - midpoint)))

    plt.plot(x, y, color='blue', alpha=0.5, label=f'Sigmoid Curve (Mid={midpoint}, k={k})')
    plt.axvline(x=midpoint, color='gray', linestyle='--', alpha=0.5, label='Midpoint (50% Pos)')

    colors = ['red', 'green', 'orange', 'purple', 'cyan']
    for i, stock in enumerate(stock_results):
        score = stock['score']
        pos = stock['pos']
        # 即使个股得分为 0 (网络错误等)，也会画在最左下角
        plt.scatter(score, pos, color=colors[i % len(colors)], s=100, zorder=5)
        plt.annotate(f"{stock['ticker']} ({score}, {pos:.1f}%)",
                     (score, pos),
                     textcoords="offset points",
                     xytext=(0, 10),
                     ha='center')

    plt.title("Fundamental Score vs Target Position (Sigmoid Model)")
    plt.xlabel("Fundamental Score (0-100)")
    plt.ylabel("Target Position (%)")
    plt.grid(True, alpha=0.3)
    plt.legend()

    # 保存图片
    save_path = CONFIG['trade_msg_dir'] / f"sigmoid_curve_{datetime.now().strftime('%Y-%m-%d')}.png"
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"\n📊 S型曲线图表已成功保存至: {save_path.name}")
    # plt.show() # 如需直接弹窗查看可取消注释


# ==================== 7. 主程序入口 ====================
if __name__ == "__main__":
    print("=== 🚀 专业级量化信号引擎启动 ===")
    target_stocks = ['META', 'MSFT', 'NVDA', 'TSLA', 'MU', 'ASML', 'AMZN', 'MAGS']

    # 用于收集画图所需数据的列表
    results_for_plot = []

    for t in target_stocks:
        signal_data = generate_daily_signal(t)
        # 如果获取成功（未被跳过），则加入画图数据列表
        if signal_data:
            results_for_plot.append(signal_data)
        time.sleep(random.uniform(1.5, 3.5))  # 礼貌延迟，防止被封

    # 当所有股票处理完毕后，执行绘图
    if results_for_plot:
        plot_sigmoid_positions(results_for_plot)

    print("\n✅ 所有信号生成及绘图完毕！请查看 trade_msg 目录。")
