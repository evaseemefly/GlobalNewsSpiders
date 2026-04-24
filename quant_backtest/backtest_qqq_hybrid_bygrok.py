import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# ==========================================
# 1. 路径配置
# ==========================================
CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_max_20140917_to_20260424.csv")

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """计算 RSI (14日)"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_metrics(nav_series: pd.Series, name: str, position: pd.Series = None):
    """计算年化收益率、最大回撤、持仓时间占比"""
    total_days = len(nav_series)
    cagr = (nav_series.iloc[-1] ** (252 / total_days)) - 1

    roll_max = nav_series.cummax()
    drawdown = nav_series / roll_max - 1.0
    max_dd = drawdown.min()

    time_in_market = (position.mean() * 100) if position is not None else 100.0

    print(f"[{name}] 年化收益率: {cagr*100:>5.2f}%  最大回撤: {max_dd*100:>6.2f}%  持仓时间: {time_in_market:>5.1f}%")
    return cagr, max_dd, time_in_market

def main():
    print("=== 🚀 QQQ Hybrid 终极优化版回测引擎 ===")

    if not CSV_FILE_PATH.exists():
        print(f"❌ 找不到数据文件: {CSV_FILE_PATH}")
        return

    # 读取数据
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index().ffill()

    # ==================== 计算核心指标 ====================
    df['QQQ_MA120'] = df['QQQ_close'].rolling(window=120, min_periods=1).mean()
    df['US10Y_MA60'] = df['US10Y_close'].rolling(window=60, min_periods=1).mean()
    df['HYG_MA60'] = df['HYG_close'].rolling(window=60, min_periods=1).mean()
    df['VIX_MA60'] = df['VIX_close'].rolling(window=60, min_periods=1).mean()
    df['RSI_14'] = calculate_rsi(df['QQQ_close'])
    df['QQQ_ret'] = df['QQQ_close'].pct_change().fillna(0)

    # ==================== Baseline ====================
    df['NAV_Baseline'] = (1 + df['QQQ_ret']).cumprod()

    # ==================== Hybrid 终极优化版 ====================
    base_trend = df['QQQ_close'] > df['QQQ_MA120']

    # 风险-off 条件（已大幅松绑）
    us10y_rising = df['US10Y_close'].diff(20) > 0.08                    # 20天收益率上升超过0.08个百分点
    hyg_divergence = ((df['HYG_close'] < df['HYG_MA60']).rolling(3).sum() == 3) & base_trend
    vix_risk = (df['VIX_close'] > 35) | (df['VIX_close'] > df['VIX_MA60'] * 1.8)

    risk_off = us10y_rising | hyg_divergence | vix_risk

    # 抄底条件（加强版）
    dip_buy = (df['RSI_14'] < 32) & (df['QQQ_close'] > df['QQQ_open']) & (df['VIX_close'] < df['VIX_close'].shift(1))

    # 优先级：抄底 > 风险-off > 趋势
    conditions = [dip_buy, risk_off, base_trend]
    choices = [1.0, 0.3, 1.0]                     # 风险-off 时保留30%仓位
    df['position'] = np.select(conditions, choices, default=0.3)
    df['position'] = df['position'].shift(1).fillna(0.3)   # 移位1天

    df['NAV_Hybrid'] = (1 + df['position'] * df['QQQ_ret']).cumprod()

    # ==================== 打印绩效 ====================
    print("\n📊 Hybrid 优化版回测结果 (2014-2026):")
    print("-" * 80)
    calculate_metrics(df['NAV_Baseline'], "大盘基准 (死拿QQQ)", pd.Series(1.0, index=df.index))
    calculate_metrics(df['NAV_Hybrid'], "Hybrid 终极优化版 (MA120+宏观+抄底)", df['position'])
    print("-" * 80)

    # ==================== 绘图 ====================
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.05)

    # 上图：净值曲线
    ax1.plot(df.index, df['NAV_Baseline'], label='Baseline (Buy & Hold QQQ)', color='gray', alpha=0.7, linewidth=1.5)
    ax1.plot(df.index, df['NAV_Hybrid'], label='Hybrid 终极优化版', color='#e74c3c', linewidth=2.5)

    ax1.set_title('QQQ Hybrid 终极优化版回测 (MA120 + 宏观滤网 + RSI抄底)', fontsize=16, fontweight='bold')
    ax1.set_ylabel('Net Asset Value (Initial = 1)', fontsize=12, fontweight='bold')
    ax1.set_yscale('log')
    ax1.legend(loc='upper left', fontsize=11)
    ax1.grid(True, linestyle='--', alpha=0.5)

    # 下图：价格 + MA120 + 风险-off区域
    ax2.plot(df.index, df['QQQ_close'], label='QQQ Price', color='black', linewidth=1)
    ax2.plot(df.index, df['QQQ_MA120'], label='QQQ MA120', color='orange', linewidth=1.5, linestyle='--')

    ax2.fill_between(df.index, ax2.get_ylim()[0], ax2.get_ylim()[1],
                     where=(df['position'] < 0.4), color='red', alpha=0.15, label='Risk-Off (30%仓位)')

    ax2.set_ylabel('QQQ Price', fontsize=12, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=10)
    ax2.grid(True, linestyle='--', alpha=0.5)

    # 保存图表
    output_pic_path = Path(__file__).parent / "06_hybrid_optimized.png"
    plt.savefig(output_pic_path, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"\n🖼️ 回测图表已保存至: {output_pic_path.name}")

if __name__ == "__main__":
    main()