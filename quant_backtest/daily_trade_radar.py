import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# ==================== 服务器无头绘图配置 ====================
import matplotlib

# 强制使用 Agg 后端，确保在无桌面的 Linux/Mac 服务器挂机时不会崩溃
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# 修复中文字体显示问题
plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Heiti TC', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

# ==================== 1. 核心路径与最优参数配置 ====================
CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv")
# 文本消息存储路径
OUTPUT_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/output/trade_msg")
# 图片快照专用存储路径
FIGURES_PATH = OUTPUT_PATH / "figures"

# 🏆 殿堂级最优参数 (Grid Search Top 1 | Calmar: 1.10)
BEST_PARAMS = {
    'ma_len': 200,  # 趋势均线周期: MA200
    'us10y_th': 0.15,  # 美债飙升阈值: 20日内 > 0.15
    'vix_th': 45,  # 恐慌熔断阈值: VIX > 45
    'rsi_th': 30,  # 抄底极值: RSI < 30
    'risk_pos': 0.3  # 避险底仓比例: 30%
}


# ==================== 2. 指标计算引擎 ====================
def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def generate_daily_report():
    print("=== 📡 QQQ 终极量化交易雷达启动 (Calmar 1.10 典藏版) ===")

    if not CSV_FILE_PATH.exists():
        print("❌ 找不到主数据文件，请先运行每日更新脚本！")
        return

    # 数据读取与截取 (过去 300 天)
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index().ffill()
    df = df.tail(300).copy()

    # 指标计算
    p = BEST_PARAMS
    df['QQQ_MA'] = df['QQQ_close'].rolling(window=p['ma_len'], min_periods=1).mean()
    df['US10Y_MA60'] = df['US10Y_close'].rolling(60, min_periods=1).mean()
    df['HYG_MA60'] = df['HYG_close'].rolling(60, min_periods=1).mean()
    df['VIX_MA60'] = df['VIX_close'].rolling(60, min_periods=1).mean()
    df['RSI_14'] = calculate_rsi(df['QQQ_close'])
    df['US10Y_diff_20'] = df['US10Y_close'].diff(20)

    today = df.iloc[-1]
    yesterday = df.iloc[-2]
    trade_date = today.name.strftime('%Y-%m-%d')
    file_date_str = today.name.strftime('%Y_%m_%d')

    # ==================== 3. 核心逻辑判定 ====================
    base_trend = today['QQQ_close'] > today['QQQ_MA']
    us10y_rising = today['US10Y_diff_20'] > p['us10y_th']

    hyg_divergence = False
    if len(df) >= 3:
        hyg_last_3 = df['HYG_close'].iloc[-3:] < df['HYG_MA60'].iloc[-3:]
        hyg_divergence = (hyg_last_3.sum() == 3) and base_trend

    vix_risk = (today['VIX_close'] > p['vix_th']) | (today['VIX_close'] > today['VIX_MA60'] * 1.8)
    risk_off = us10y_rising | hyg_divergence | vix_risk
    dip_buy = (today['RSI_14'] < p['rsi_th']) and (today['QQQ_close'] > today['QQQ_open']) and (
            today['VIX_close'] < yesterday['VIX_close'])

    # ==================== 4. 判定下个交易日指令 ====================
    target_position = p['risk_pos']
    action_reason = ""

    if dip_buy:
        target_position = 1.0
        action_reason = f"🚨【绝地反击】RSI跌破{p['rsi_th']}，全仓抄底！"
    elif risk_off:
        target_position = p['risk_pos']
        action_reason = f"🛡️【宏观避险】触发警报，降低至 {int(p['risk_pos'] * 100)}% 底仓防守。"
    elif base_trend:
        target_position = 1.0
        action_reason = "📈【顺势做多】稳站 MA200 之上，维持满仓。"
    else:
        target_position = p['risk_pos']
        action_reason = f"📉【趋势走弱】跌破 MA200，维持 {int(p['risk_pos'] * 100)}% 底仓。"

    # ==================== 5. 构建文本报告并保存 ====================
    trend_dist = (today['QQQ_close'] / today['QQQ_MA'] - 1) * 100
    report_content = (
        f"{'=' * 55}\n"
        f"📊 QQQ 极客量化交易日报 | 结算日: {trade_date}\n"
        f"{'=' * 55}\n\n"
        f"🎯【明日实盘交易指令】\n"
        f"   执行目标仓位 : {target_position * 100:.0f}%\n"
        f"   逻辑触发说明 : {action_reason}\n\n"
        f"🔍【关键指标快照】\n"
        f"   • QQQ 价格  : {today['QQQ_close']:.2f} (MA200: {today['QQQ_MA']:.2f})\n"
        f"   • 均线偏离度: {trend_dist:+.2f}%\n"
        f"   • RSI (14)  : {today['RSI_14']:.2f}\n"
        f"   • 美债 20日动量: {today['US10Y_diff_20']:+.2f}\n"
        f"   • VIX 恐慌值 : {today['VIX_close']:.2f}\n"
        f"{'=' * 55}\n"
    )

    print("\n" + report_content)

    # 保存 TXT 消息
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    txt_name = f"grok_trade_tick_{file_date_str}.txt"
    save_txt_path = OUTPUT_PATH / txt_name
    with open(save_txt_path, "w", encoding="utf-8") as f:
        f.write(report_content)
    print(f"💾 文本指令已保存: {save_txt_path.name}")

    # ==================== 6. 生成视觉快照并存入 figures 目录 ====================
    print("🎨 正在渲染 K 线视觉雷达快照...")
    FIGURES_PATH.mkdir(parents=True, exist_ok=True)  # 自动创建 figures 目录

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.08)

    # 上图：主图 K 线
    ax1.plot(df.index, df['QQQ_close'], label='QQQ Price', color='#2c3e50', linewidth=1.5)
    ax1.plot(df.index, df['QQQ_MA'], label=f"MA{p['ma_len']}", color='#e67e22', linewidth=2, linestyle='--')

    # 指令点标记
    dot_color = '#27ae60' if target_position == 1.0 else '#c0392b'
    ax1.scatter(today.name, today['QQQ_close'], color=dot_color, s=200, zorder=5, edgecolors='white')

    ax1.set_title(f"QQQ Daily Radar Snapshot | {trade_date} | Target Pos: {int(target_position * 100)}%", fontsize=16,
                  fontweight='bold')
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle='--', alpha=0.4)

    # 下图：RSI 抄底雷达
    ax2.plot(df.index, df['RSI_14'], label='RSI (14)', color='#8e44ad', linewidth=1.5)
    ax2.axhline(p['rsi_th'], color='red', linestyle='--', alpha=0.6, label='Oversold Line')
    ax2.fill_between(df.index, df['RSI_14'], p['rsi_th'], where=(df['RSI_14'] < p['rsi_th']), color='red', alpha=0.2)
    ax2.legend(loc='upper left')
    ax2.grid(True, linestyle='--', alpha=0.4)

    # 保存图片至 figures 文件夹
    pic_name = f"grok_trade_tick_{file_date_str}.png"
    save_pic_path = FIGURES_PATH / pic_name
    plt.tight_layout()
    fig.savefig(save_pic_path, dpi=150, bbox_inches='tight')
    plt.close(fig)

    print(f"🖼️ 视觉快照已保存至: {save_pic_path}")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    generate_daily_report()