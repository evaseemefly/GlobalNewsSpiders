import pandas as pd
import numpy as np
import matplotlib

matplotlib.use('Agg')  # 无头服务器兼容
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

# ==================== VOO 最优参数 ====================
BEST_PARAMS = {
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
    print("=== 📡 VOO 终极量化交易雷达启动 (Gemini 极客版) ===")

    if not CSV_FILE_PATH.exists():
        print("❌ 找不到主数据文件，请先运行每日更新脚本！")
        return

    # 读取数据
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index().ffill()
    df = df.tail(400).copy()  # 足够计算 MA100 和 diff(20)

    p = BEST_PARAMS
    df['VOO_MA'] = df['VOO_close'].rolling(window=p['ma_len'], min_periods=1).mean()
    df['US10Y_MA60'] = df['US10Y_close'].rolling(60, min_periods=1).mean()
    df['HYG_MA60'] = df['HYG_close'].rolling(60, min_periods=1).mean()
    df['VIX_MA60'] = df['VIX_close'].rolling(60, min_periods=1).mean()
    df['RSI_14'] = calculate_rsi(df['VOO_close'])
    df['US10Y_diff_20'] = df['US10Y_close'].diff(20)

    today = df.iloc[-1]
    yesterday = df.iloc[-2]
    trade_date = today.name.strftime('%Y-%m-%d')
    file_date_str = today.name.strftime('%Y_%m_%d')

    # ==================== 核心逻辑判定 ====================
    base_trend = today['VOO_close'] > today['VOO_MA']
    us10y_rising = today['US10Y_diff_20'] > p['us10y_th']

    hyg_divergence = False
    if len(df) >= 3:
        hyg_last_3 = df['HYG_close'].iloc[-3:] < df['HYG_MA60'].iloc[-3:]
        hyg_divergence = (hyg_last_3.sum() == 3) and base_trend

    vix_risk = (today['VIX_close'] > p['vix_th']) | (today['VIX_close'] > today['VIX_MA60'] * 1.8)
    risk_off = us10y_rising | hyg_divergence | vix_risk
    dip_buy = (today['RSI_14'] < p['rsi_th']) and (today['VOO_close'] > today['VOO_open']) and (
            today['VIX_close'] < yesterday['VIX_close'])

    # ==================== 判定明日交易指令 ====================
    if dip_buy:
        target_position = 1.0
        action_reason = f"🚨【绝地反击】RSI跌破{p['rsi_th']}，VIX回落，全仓抄底！"
    elif risk_off:
        target_position = p['risk_pos']
        action_reason = f"🛡️【宏观避险】触发警报，降低至 {int(p['risk_pos'] * 100)}% 底仓防守。"
    elif base_trend:
        target_position = 1.0
        action_reason = f"📈【顺势做多】稳站 MA{p['ma_len']} 之上，维持满仓。"
    else:
        target_position = p['risk_pos']
        action_reason = f"📉【趋势走弱】跌破 MA{p['ma_len']}，保持 {int(p['risk_pos'] * 100)}% 底仓。"

    # ==================== Gemini 极客风格报告 ====================
    trend_dist = (today['VOO_close'] / today['VOO_MA'] - 1) * 100
    report_content = (
        f"{'=' * 60}\n"
        f"📊 VOO 极客量化交易日报 | 结算日: {trade_date}\n"
        f"{'=' * 60}\n\n"
        f"🎯【明日实盘交易指令】\n"
        f"   执行目标仓位 : {target_position * 100:.0f}%\n"
        f"   逻辑触发说明 : {action_reason}\n\n"
        f"🔍【关键指标快照】\n"
        f"   • VOO 价格  : {today['VOO_close']:.2f} (MA{p['ma_len']}: {today['VOO_MA']:.2f})\n"
        f"   • 均线偏离度: {trend_dist:+.2f}%\n"
        f"   • RSI (14)  : {today['RSI_14']:.2f}\n"
        f"   • 美债 20日动量: {today['US10Y_diff_20']:+.2f}\n"
        f"   • HYG 信用   : {today['HYG_close']:.2f} (MA60: {today['HYG_MA60']:.2f})\n"
        f"   • VIX 恐慌   : {today['VIX_close']:.2f}\n"
        f"{'=' * 60}\n"
    )

    print("\n" + report_content)

    # 保存 TXT 报告
    txt_name = f"gemini_trade_tick_{file_date_str}_voo.txt"
    save_txt_path = OUTPUT_PATH / txt_name
    save_txt_path.write_text(report_content, encoding="utf-8")
    print(f"💾 Gemini 风格报告已保存: {save_txt_path.name}")

    # ==================== 生成视觉快照 ====================
    print("🎨 正在渲染 VOO K线视觉雷达快照...")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.08)

    # 上图：价格 + MA
    ax1.plot(df.index, df['VOO_close'], label='VOO Price', color='#2c3e50', linewidth=1.5)
    ax1.plot(df.index, df['VOO_MA'], label=f"MA{p['ma_len']}", color='#e67e22', linewidth=2, linestyle='--')
    dot_color = '#27ae60' if target_position == 1.0 else '#c0392b'
    ax1.scatter(today.name, today['VOO_close'], color=dot_color, s=180, zorder=5, edgecolors='white', linewidth=2)

    ax1.set_title(f"VOO Daily Radar Snapshot | {trade_date} | Target Pos: {int(target_position * 100)}%",
                  fontsize=15, fontweight='bold')
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle='--', alpha=0.4)

    # 下图：RSI 雷达
    ax2.plot(df.index, df['RSI_14'], label='RSI (14)', color='#8e44ad', linewidth=1.5)
    ax2.axhline(p['rsi_th'], color='red', linestyle='--', alpha=0.7, label=f'Oversold ({p["rsi_th"]})')
    ax2.fill_between(df.index, df['RSI_14'], p['rsi_th'],
                     where=(df['RSI_14'] < p['rsi_th']), color='red', alpha=0.25)
    ax2.legend(loc='upper left')
    ax2.grid(True, linestyle='--', alpha=0.4)

    # 保存图片
    pic_name = f"gemini_trade_tick_{file_date_str}_voo.png"
    save_pic_path = FIGURES_PATH / pic_name
    plt.tight_layout()
    fig.savefig(save_pic_path, dpi=160, bbox_inches='tight')
    plt.close(fig)

    print(f"🖼️ 视觉快照已保存: {save_pic_path.name}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    generate_daily_report()
