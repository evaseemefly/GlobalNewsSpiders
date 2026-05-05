import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime

# ==================== 配置 ====================
# home mac
CSV_FILE_PATH = Path(
    "/Users/evaseemefly/03data/05-spiders/broad_market_history/historical_broad_market_master.csv")
# workplace mac
# CSV_FILE_PATH = Path(
#     "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv")

# home - mac
OUTPUT_PATH = Path(
    "/Users/evaseemefly/03data/05-spiders/output//trade_msg")
# workplace mac
# OUTPUT_PATH = Path(
#     "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/output/trade_msg")

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


FIGURES_PATH = OUTPUT_PATH / "figures"
FIGURES_PATH.mkdir(parents=True, exist_ok=True)

# ==================== VOO 最优参数 ====================
PARAMS = {
    'ma_len': 100,
    'us10y_th': 0.15,
    'vix_th': 45,
    'rsi_th': 30,
    'risk_pos': 0.3
}

plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Heiti TC', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def generate_daily_report():
    print("=== 🚀 VOO 终极量化每日信号生成器（升级执行版）===")

    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index().ffill()

    p = PARAMS
    df['VOO_MA'] = df['VOO_close'].rolling(p['ma_len'], min_periods=1).mean()
    df['US10Y_diff_20'] = df['US10Y_close'].diff(20)
    df['HYG_MA60'] = df['HYG_close'].rolling(60, min_periods=1).mean()
    df['VIX_MA60'] = df['VIX_close'].rolling(60, min_periods=1).mean()
    df['RSI_14'] = calculate_rsi(df['VOO_close'])

    today = df.iloc[-1]
    yesterday = df.iloc[-2]
    date_str = today.name.strftime('%Y-%m-%d')
    file_date = date_str.replace('-', '_')

    # ==================== 核心信号逻辑 ====================
    base_trend = today['VOO_close'] > today['VOO_MA']
    us10y_rising = today['US10Y_diff_20'] > p['us10y_th']
    hyg_divergence = (len(df) >= 3 and (df['HYG_close'].iloc[-3:] < df['HYG_MA60'].iloc[-3:]).sum() == 3) and base_trend
    vix_risk = (today['VIX_close'] > p['vix_th']) | (today['VIX_close'] > today['VIX_MA60'] * 1.8)
    risk_off = us10y_rising | hyg_divergence | vix_risk
    dip_buy = (today['RSI_14'] < p['rsi_th']) and (today['VOO_close'] > today['VOO_open']) and (
            today['VIX_close'] < yesterday['VIX_close'])

    if dip_buy:
        position = 1.0
        action = "🟢 左侧抄底：满仓加仓"
    elif risk_off:
        position = p['risk_pos']
        action = "🔴 风险-Off：减仓至 30% 防守"
    elif base_trend:
        position = 1.0
        action = "🟢 多头趋势：维持 100% 满仓"
    else:
        position = p['risk_pos']
        action = "⚪ 防御状态：保持 30% 底仓"

    # ==================== 新增：箱体区间 + 执行建议 ====================
    recent = df.tail(60)
    support = recent['VOO_close'].min()
    resistance = recent['VOO_close'].max()
    atr = (recent['VOO_high'] - recent['VOO_low']).mean()
    mid_price = (support + resistance) / 2

    # 执行建议（仅在允许满仓时给出）
    if position == 1.0:
        exec_suggestion = f"""
📍 当前箱体区间: {support:.2f} — {resistance:.2f}（中轴 {mid_price:.2f}）
💡 执行建议:
   • 初始建仓: 当前价或 {mid_price:.2f} 附近（建议占总仓位 40%）
   • 第1次加仓: 回落至 MA100 附近 或 -6%（+20%）
   • 第2次加仓: 回落至箱体支撑 {support:.2f} 附近 或 -11%（+20%）
   • 第3次加仓: 极端恐慌（RSI<30）（+20%）
"""
    else:
        exec_suggestion = "\n📍 当前为风险-Off 状态，暂不建议新增仓位。"

    # ==================== Grok 风格报告 ====================
    grok_text = f"""📅 【{date_str} VOO 中长期量化信号】
{'=' * 55}
趋势框架：{'✅ 多头' if base_trend else '❌ 空头'} (VOO vs MA{p['ma_len']})
宏观利率：{'⚠️ 上升' if us10y_rising else '✅ 安全'} (20日变化 {today['US10Y_diff_20']:+.2f})
信用背离：{'⚠️ 触发' if hyg_divergence else '✅ 正常'}
恐慌指数：{'⚠️ 高位' if vix_risk else '✅ 低位'} (VIX={today['VIX_close']:.2f})
抄底机会：{'🟢 触发' if dip_buy else '❌ 未触发'} (RSI={today['RSI_14']:.1f})
{'-' * 55}
📢 操作建议：{action}
🎯 推荐仓位：{int(position * 100)}%
{exec_suggestion}
"""
    grok_file = OUTPUT_PATH / f"grok_trade_tick_{file_date}.txt"
    grok_file.write_text(grok_text, encoding='utf-8')
    print(f"✅ Grok 风格报告已保存 → {grok_file.name}")

    # ==================== 正确的历史净值曲线图 ====================
    # （保持原有净值图逻辑，代码略长，这里省略以节省篇幅，但实际代码中保留）
    # ...（与之前版本完全一致的净值图代码）...

    # 计算净值（完整历史）
    df['base_trend'] = df['VOO_close'] > df['VOO_MA']
    df['us10y_rising'] = df['US10Y_diff_20'] > p['us10y_th']
    df['hyg_divergence'] = False
    if len(df) >= 3:
        df['hyg_divergence'] = (df['HYG_close'] < df['HYG_MA60']).rolling(3).sum() == 3
    df['vix_risk'] = (df['VIX_close'] > p['vix_th']) | (df['VIX_close'] > df['VIX_MA60'] * 1.8)
    df['risk_off'] = df['us10y_rising'] | df['hyg_divergence'] | df['vix_risk']
    df['dip_buy'] = (df['RSI_14'] < p['rsi_th']) & (df['VOO_close'] > df['VOO_open']) & (df['VIX_close'] < df['VIX_close'].shift(1))

    df['position'] = np.where(df['dip_buy'], 1.0,
                    np.where(df['risk_off'], p['risk_pos'],
                    np.where(df['base_trend'], 1.0, p['risk_pos'])))
    df['position'] = df['position'].shift(1).fillna(p['risk_pos'])

    baseline = (1 + df['VOO_close'].pct_change().fillna(0)).cumprod()
    strategy = (1 + df['position'] * df['VOO_close'].pct_change().fillna(0)).cumprod()

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(baseline.index, baseline, label='Baseline (死拿VOO)', color='gray', alpha=0.7, linewidth=1.5)
    ax.plot(strategy.index, strategy, label='Hybrid Strategy (VOO)', color='#e74c3c', linewidth=2.5)
    ax.set_title(f'VOO Hybrid 策略净值曲线（截至 {date_str}）')
    ax.set_ylabel('净值 (初始 = 1)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    chart_path = FIGURES_PATH / f"hybrid_equity_voo_{file_date}.png"
    plt.tight_layout()
    plt.savefig(chart_path, dpi=200, bbox_inches='tight')
    plt.close()

    print(f"✅ 净值曲线图已保存 → {chart_path.name}")
    print(f"\n🎉 今日 VOO 报告生成完成！所有文件均保存在：{OUTPUT_PATH}")


if __name__ == "__main__":
    generate_daily_report()