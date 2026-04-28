import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime

# ==================== 配置 ====================
CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv")

OUTPUT_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/output/trade_msg")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

FIGURES_PATH = OUTPUT_PATH / "figures"
FIGURES_PATH.mkdir(parents=True, exist_ok=True)

# 最优参数
PARAMS = {
    'ma_len': 200,
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
    print("=== 🚀 QQQ 终极量化每日信号生成器（Grok 版）===")

    # 读取数据
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index().ffill()

    p = PARAMS
    df['QQQ_MA'] = df['QQQ_close'].rolling(p['ma_len'], min_periods=1).mean()
    df['US10Y_diff_20'] = df['US10Y_close'].diff(20)
    df['HYG_MA60'] = df['HYG_close'].rolling(60, min_periods=1).mean()
    df['VIX_MA60'] = df['VIX_close'].rolling(60, min_periods=1).mean()
    df['RSI_14'] = calculate_rsi(df['QQQ_close'])

    today = df.iloc[-1]
    yesterday = df.iloc[-2]
    date_str = today.name.strftime('%Y-%m-%d')
    file_date = date_str.replace('-', '_')

    # ==================== 核心信号逻辑 ====================
    base_trend = today['QQQ_close'] > today['QQQ_MA']
    us10y_rising = today['US10Y_diff_20'] > p['us10y_th']
    hyg_divergence = (len(df) >= 3 and (df['HYG_close'].iloc[-3:] < df['HYG_MA60'].iloc[-3:]).sum() == 3) and base_trend
    vix_risk = (today['VIX_close'] > p['vix_th']) | (today['VIX_close'] > today['VIX_MA60'] * 1.8)
    risk_off = us10y_rising | hyg_divergence | vix_risk
    dip_buy = (today['RSI_14'] < p['rsi_th']) and (today['QQQ_close'] > today['QQQ_open']) and (
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

    # ==================== Grok 风格报告 ====================
    grok_text = f"""📅 【{date_str} QQQ 中长期量化信号】
{'=' * 55}
趋势框架：{'✅ 多头' if base_trend else '❌ 空头'} (QQQ vs MA200)
宏观利率：{'⚠️ 上升' if us10y_rising else '✅ 安全'} (20日变化 {today['US10Y_diff_20']:+.2f})
信用背离：{'⚠️ 触发' if hyg_divergence else '✅ 正常'}
恐慌指数：{'⚠️ 高位' if vix_risk else '✅ 低位'} (VIX={today['VIX_close']:.2f})
抄底机会：{'🟢 触发' if dip_buy else '❌ 未触发'} (RSI={today['RSI_14']:.1f})
{'-' * 55}
📢 操作建议：{action}
🎯 推荐仓位：{int(position * 100)}%
"""
    grok_file = OUTPUT_PATH / f"grok_trade_tick_{file_date}.txt"
    grok_file.write_text(grok_text, encoding='utf-8')
    print(f"✅ Grok 风格报告已保存 → {grok_file.name}")

    # ==================== 正确的历史净值曲线图 ====================
    # 计算完整历史仓位
    df['base_trend'] = df['QQQ_close'] > df['QQQ_MA']
    df['us10y_rising'] = df['US10Y_diff_20'] > p['us10y_th']
    df['hyg_divergence'] = False
    if len(df) >= 3:
        df['hyg_divergence'] = (df['HYG_close'] < df['HYG_MA60']).rolling(3).sum() == 3
    df['vix_risk'] = (df['VIX_close'] > p['vix_th']) | (df['VIX_close'] > df['VIX_MA60'] * 1.8)
    df['risk_off'] = df['us10y_rising'] | df['hyg_divergence'] | df['vix_risk']
    df['dip_buy'] = (df['RSI_14'] < p['rsi_th']) & (df['QQQ_close'] > df['QQQ_open']) & (df['VIX_close'] < df['VIX_close'].shift(1))

    df['position'] = np.where(df['dip_buy'], 1.0,
                    np.where(df['risk_off'], p['risk_pos'],
                    np.where(df['base_trend'], 1.0, p['risk_pos'])))
    df['position'] = df['position'].shift(1).fillna(p['risk_pos'])

    # 计算净值
    baseline = (1 + df['QQQ_close'].pct_change().fillna(0)).cumprod()
    strategy = (1 + df['position'] * df['QQQ_close'].pct_change().fillna(0)).cumprod()

    # 绘图
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(baseline.index, baseline, label='Baseline (死拿QQQ)', color='gray', alpha=0.7, linewidth=1.5)
    ax.plot(strategy.index, strategy, label='Hybrid Strategy', color='#e74c3c', linewidth=2.5)
    ax.set_title(f'QQQ Hybrid 策略净值曲线（截至 {date_str}）')
    ax.set_ylabel('净值 (初始 = 1)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    chart_path = FIGURES_PATH / f"hybrid_equity_{file_date}.png"
    plt.tight_layout()
    plt.savefig(chart_path, dpi=200, bbox_inches='tight')
    plt.close()

    print(f"✅ 净值曲线图已保存 → {chart_path.name}")
    print(f"\n🎉 今日报告生成完成！所有文件均保存在：{OUTPUT_PATH}")


if __name__ == "__main__":
    generate_daily_report()