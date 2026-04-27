import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# ==================== 【Mac 中文字体修复】 ====================
plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Heiti TC', 'STHeiti']  # Mac 原生中文字体
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# ==========================================
# 路径配置
# ==========================================
CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_max_20140917_to_20260424.csv")

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_metrics(nav_series: pd.Series, name: str, position: pd.Series = None):
    total_days = len(nav_series)
    cagr = (nav_series.iloc[-1] ** (252 / total_days)) - 1
    roll_max = nav_series.cummax()
    drawdown = nav_series / roll_max - 1.0
    max_dd = drawdown.min()
    calmar = cagr / abs(max_dd) if max_dd != 0 else 0
    time_in_market = (position.mean() * 100) if position is not None else 100.0

    print(f"[{name}] 年化: {cagr*100:>5.2f}%  最大回撤: {max_dd*100:>6.2f}%  "
          f"Calmar: {calmar:>5.2f}  持仓时间: {time_in_market:>5.1f}%")
    return cagr, max_dd, calmar, time_in_market

def main():
    print("=== 🚀 QQQ Hybrid 激进优化版（已修复字体） ===")
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index().ffill()

    # 核心指标
    df['QQQ_MA120'] = df['QQQ_close'].rolling(window=120, min_periods=1).mean()
    df['US10Y_MA60'] = df['US10Y_close'].rolling(60, min_periods=1).mean()
    df['HYG_MA60'] = df['HYG_close'].rolling(60, min_periods=1).mean()
    df['VIX_MA60'] = df['VIX_close'].rolling(60, min_periods=1).mean()
    df['RSI_14'] = calculate_rsi(df['QQQ_close'])
    df['QQQ_ret'] = df['QQQ_close'].pct_change().fillna(0)

    df['NAV_Baseline'] = (1 + df['QQQ_ret']).cumprod()

    # ==================== 激进优化版信号 ====================
    base_trend = df['QQQ_close'] > df['QQQ_MA120']

    us10y_rising = df['US10Y_close'].diff(20) > 0.12
    hyg_divergence = ((df['HYG_close'] < df['HYG_MA60']).rolling(3).sum() == 3) & base_trend
    vix_risk = (df['VIX_close'] > 40) | (df['VIX_close'] > df['VIX_MA60'] * 1.8)

    risk_off = us10y_rising | hyg_divergence | vix_risk

    dip_buy = (df['RSI_14'] < 35) & (df['QQQ_close'] > df['QQQ_open']) & (df['VIX_close'] < df['VIX_close'].shift(1))

    conditions = [dip_buy, risk_off, base_trend]
    choices = [1.0, 0.5, 1.0]
    df['position'] = np.select(conditions, choices, default=0.5)
    df['position'] = df['position'].shift(1).fillna(0.5)

    df['NAV_Hybrid'] = (1 + df['position'] * df['QQQ_ret']).cumprod()

    # 绩效
    print("\n📊 激进优化版回测结果:")
    print("-" * 95)
    calculate_metrics(df['NAV_Baseline'], "大盘基准", pd.Series(1.0, index=df.index))
    calculate_metrics(df['NAV_Hybrid'], "Hybrid 激进优化版", df['position'])
    print("-" * 95)

    # ==================== 绘图 ====================
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.05)

    ax1.plot(df.index, df['NAV_Baseline'], label='Baseline (Buy & Hold)', color='gray', alpha=0.7, linewidth=1.5)
    ax1.plot(df.index, df['NAV_Hybrid'], label='Hybrid 激进优化版', color='#e74c3c', linewidth=2.5)
    ax1.set_title('QQQ Hybrid v2 (MA120 + 宏观 + RSI抄底)——26-04-26', fontsize=16, fontweight='bold')
    ax1.set_ylabel('Net Asset Value (log)', fontsize=12)
    ax1.set_yscale('log')
    ax1.legend()
    ax1.grid(True, alpha=0.5)

    ax2.plot(df.index, df['QQQ_close'], label='QQQ Price', color='black')
    ax2.plot(df.index, df['QQQ_MA120'], label='QQQ MA120', color='orange', linestyle='--')
    ax2.fill_between(df.index, ax2.get_ylim()[0], ax2.get_ylim()[1],
                     where=(df['position'] < 0.6), color='red', alpha=0.15, label='Risk-Off (50%仓位)')
    ax2.set_ylabel('QQQ Price')
    ax2.legend()
    ax2.grid(True, alpha=0.5)

    output_pic_path = Path(__file__).parent / "08_hybrid_fixed.png"
    plt.savefig(output_pic_path, dpi=200, bbox_inches='tight')
    plt.close()
    print(f"🖼️ 图表已保存: {output_pic_path.name}")

if __name__ == "__main__":
    main()