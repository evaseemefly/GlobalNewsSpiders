import pandas as pd
import numpy as np
import matplotlib

matplotlib.use('Agg')  # 无头服务器兼容
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from enum import Enum, auto


# ==================== 1. 定义环境枚举 ====================
class EnvType(Enum):
    """
        使用 auto() 的好处：
            省心且优雅：不需要去思考下一个数字该填什么。
            绝对防错：如果有新同事在代码里加了一个 OFFICE 环境，如果是手动填数字，他可能会不小心也填成
            2，导致严重的逻辑冲突。而用 auto() 则永远不会出现重复值。
            意图清晰：向阅读代码的人传达一个明确的信号——“这些枚举的具体数值不重要，它们只是用来作为独立标识的”。
    """
    HOME = auto()
    WORK = auto()


# ==================== 2. 配置获取函数 (无副作用) ====================
def get_env_config(env: EnvType) -> dict:
    """根据运行环境返回对应的路径配置字典"""
    if env == EnvType.HOME:
        base_path = Path("/Users/evaseemefly/03data/05-spiders")
    elif env == EnvType.WORK:
        base_path = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
    else:
        raise ValueError(f"未知的环境类型: {env}")

    # 组装配置字典
    config = {
        'csv_file': base_path / "broad_market_history/historical_broad_market_master.csv",
        'output_dir': base_path / "output/trade_msg",
        'figures_dir': base_path / "output/trade_msg/figures"
    }

    # 在这里创建必要的目录是合理的，因为这是在初始化配置时必须保证的环境状态
    config['output_dir'].mkdir(parents=True, exist_ok=True)
    config['figures_dir'].mkdir(parents=True, exist_ok=True)

    return config


# ==================== 3. 顶层配置加载 ====================
# 在这里手动切换环境
CURRENT_ENV = EnvType.HOME

# 获取配置字典
CONFIG = get_env_config(CURRENT_ENV)

# 显式地赋值给常量，方便后续直接使用
CSV_FILE_PATH = CONFIG['csv_file']
OUTPUT_PATH = CONFIG['output_dir']
FIGURES_PATH = CONFIG['figures_dir']

print(f"⚙️ 运行环境: [{CURRENT_ENV.name}]")
print(f"📂 数据路径: {CSV_FILE_PATH}")

# todo 26-05-06: 整合参数配置，支持 QQQ 和 VOO 多资产循环处理
ASSET_CONFIG = {
    'QQQ': {
        'ma_len': 200,
        'us10y_th': 0.15,
        'vix_th': 45,
        'rsi_th': 30,
        'risk_pos': 0.3
    },
    'VOO': {
        'ma_len': 100,
        'us10y_th': 0.15,
        'vix_th': 45,
        'rsi_th': 30,
        'risk_pos': 0.3
    }
}

# 修复字体，不包含 Emoji
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


def process_asset_indicators(df: pd.DataFrame, asset: str, p: dict) -> pd.DataFrame:
    df_calc = df.copy()
    # 基础指标
    df_calc[f'{asset}_MA'] = df_calc[f'{asset}_close'].rolling(window=p['ma_len'], min_periods=1).mean()
    df_calc['US10Y_MA60'] = df_calc['US10Y_close'].rolling(60, min_periods=1).mean()
    df_calc['HYG_MA60'] = df_calc['HYG_close'].rolling(60, min_periods=1).mean()
    df_calc['VIX_MA60'] = df_calc['VIX_close'].rolling(60, min_periods=1).mean()
    df_calc[f'RSI_14_{asset}'] = calculate_rsi(df_calc[f'{asset}_close'])
    df_calc['US10Y_diff_20'] = df_calc['US10Y_close'].diff(20)
    return df_calc


def plot_snapshot_with_levels(df: pd.DataFrame, asset: str, p: dict, target_position: float,
                              support: float, resistance: float, mid_price: float,
                              trade_date: str, file_date_str: str) -> Path:
    # 创建三行子图：全局价格+MA, 近期箱体细节, RSI雷达
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 15), gridspec_kw={'height_ratios': [2.5, 2, 1]},
                                        sharex=False)
    plt.subplots_adjust(hspace=0.2)
    today = df.iloc[-1]

    # --- 上图：主图 K 线 ---
    ax1.plot(df.index, df[f'{asset}_close'], label=f'{asset} Price', color='#2c3e50', linewidth=1.5)
    ax1.plot(df.index, df[f'{asset}_MA'], label=f"MA{p['ma_len']}", color='#e67e22', linewidth=2, linestyle='--')

    dot_color = '#27ae60' if target_position == 1.0 else '#c0392b'
    ax1.scatter(today.name, today[f'{asset}_close'], color=dot_color, s=200, zorder=5, edgecolors='white', linewidth=2)

    ax1.set_title(f"{asset} Daily Radar Snapshot | {trade_date} | Target Pos: {int(target_position * 100)}%",
                  fontsize=16, fontweight='bold')
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle='--', alpha=0.4)

    # --- 中图：近期箱体与加仓计划 ---
    recent_df = df.tail(120)
    ax2.plot(recent_df.index, recent_df[f'{asset}_close'], label=f'{asset} Close Price', color='#2980b9', linewidth=2)
    ax2.plot(recent_df.index, recent_df[f'{asset}_MA'], label=f'MA{p["ma_len"]}', color='#f39c12', linestyle='-',
             linewidth=1.5)

    box_start = df.tail(60).index[0]
    box_end = df.tail(60).index[-1]
    ax2.axvspan(box_start, box_end, color='lightgray', alpha=0.2, label='Box Horizon (60d)')

    ax2.axhline(resistance, color='red', linestyle='--', alpha=0.6, label=f'Resistance ({resistance:.2f})')
    ax2.axhline(support, color='green', linestyle='--', alpha=0.6, label=f'Support / Buy 2 ({support:.2f})')
    ax2.axhline(mid_price, color='blue', linestyle='-.', alpha=0.8, label=f'Initial Buy (Mid: {mid_price:.2f})')

    buy1_price = resistance * 0.94
    buy2_price = resistance * 0.89
    ax2.axhline(buy1_price, color='purple', linestyle=':', linewidth=2, label=f'Buy 1 (-6%: {buy1_price:.2f})')
    ax2.axhline(buy2_price, color='brown', linestyle=':', linewidth=2, label=f'Buy 2 (-11%: {buy2_price:.2f})')

    ax2.text(0.02, 0.05, "【注意】第3次加仓点位: 极端恐慌 (RSI < 30) 时触发",
             transform=ax2.transAxes, color='red', fontsize=11, fontweight='bold',
             bbox=dict(facecolor='white', alpha=0.8, edgecolor='lightgray'))

    ax2.set_title(f'{asset} 近期箱体空间与加仓位置可视化', fontsize=14)
    ax2.set_ylabel('Price')
    ax2.legend(loc='lower left', fontsize=9, ncol=2)
    ax2.grid(True, alpha=0.3)

    # --- 下图：RSI 抄底雷达 ---
    ax3.plot(df.index, df[f'RSI_14_{asset}'], label=f'RSI (14)', color='#8e44ad', linewidth=1.5)
    ax3.axhline(p['rsi_th'], color='red', linestyle='--', alpha=0.6, label=f'Oversold ({p["rsi_th"]})')
    ax3.fill_between(df.index, df[f'RSI_14_{asset}'], p['rsi_th'], where=(df[f'RSI_14_{asset}'] < p['rsi_th']),
                     color='red', alpha=0.25)
    ax3.legend(loc='upper left')
    ax3.grid(True, linestyle='--', alpha=0.4)

    # 保存图片
    pic_name = f"gemini_trade_tick_{file_date_str}_{asset.lower()}.png"
    save_pic_path = FIGURES_PATH / pic_name
    plt.tight_layout()
    fig.savefig(save_pic_path, dpi=160, bbox_inches='tight')
    plt.close(fig)

    return save_pic_path


def generate_daily_report():
    print("=== 📡 终极量化交易雷达启动 (多资产合并执行版) ===")

    if not CSV_FILE_PATH or not CSV_FILE_PATH.exists():
        print(f"❌ 找不到主数据文件: {CSV_FILE_PATH}")
        print("请确认环境配置是否正确，且数据文件已同步。")
        return

    # 读取全量数据
    master_df = pd.read_csv(CSV_FILE_PATH)
    master_df['trade_date_utc'] = pd.to_datetime(master_df['trade_date_utc'])
    master_df = master_df.set_index('trade_date_utc').sort_index().ffill()

    # 截取足够长的数据用于绘图和指标计算
    master_df = master_df.tail(400).copy()

    for asset, p in ASSET_CONFIG.items():
        print(f"\n⏳ 正在生成 {asset} 的信号及雷达图...")

        # 获取单资产指标数据
        df = process_asset_indicators(master_df, asset, p)

        today = df.iloc[-1]
        yesterday = df.iloc[-2]
        trade_date = today.name.strftime('%Y-%m-%d')
        file_date_str = today.name.strftime('%Y_%m_%d')

        # ==================== 核心逻辑判定 ====================
        base_trend = today[f'{asset}_close'] > today[f'{asset}_MA']
        us10y_rising = today['US10Y_diff_20'] > p['us10y_th']

        hyg_divergence = False
        if len(df) >= 3:
            hyg_last_3 = df['HYG_close'].iloc[-3:] < df['HYG_MA60'].iloc[-3:]
            hyg_divergence = (hyg_last_3.sum() == 3) and base_trend

        vix_risk = (today['VIX_close'] > p['vix_th']) | (today['VIX_close'] > today['VIX_MA60'] * 1.8)
        risk_off = us10y_rising | hyg_divergence | vix_risk
        dip_buy = (today[f'RSI_14_{asset}'] < p['rsi_th']) and (today[f'{asset}_close'] > today[f'{asset}_open']) and (
                today['VIX_close'] < yesterday['VIX_close'])

        # ==================== 判定交易指令 ====================
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

        # 计算箱体区间与执行建议
        recent = df.tail(60)
        support = recent[f'{asset}_close'].min()
        resistance = recent[f'{asset}_close'].max()
        mid_price = (support + resistance) / 2

        if target_position == 1.0:
            exec_suggestion = f"""
📍 当前箱体区间: {support:.2f} — {resistance:.2f}（中轴 {mid_price:.2f}）
💡 执行建议:
   • 初始建仓: 当前价或 {mid_price:.2f} 附近（建议占总仓位 40%）
   • 第1次加仓: 回落至 MA{p['ma_len']} 附近 或 -6%（+20%）
   • 第2次加仓: 回落至箱体支撑 {support:.2f} 附近 或 -11%（+20%）
   • 第3次加仓: 极端恐慌（RSI<{p['rsi_th']}）（+20%）
"""
        else:
            exec_suggestion = "\n📍 当前为风险-Off 状态，暂不建议新增仓位。"

        # ==================== 构建雷达文本报告 ====================
        trend_dist = (today[f'{asset}_close'] / today[f'{asset}_MA'] - 1) * 100
        report_content = (
            f"{'=' * 60}\n"
            f"📊 {asset} 极客量化交易日报 | 结算日: {trade_date}\n"
            f"{'=' * 60}\n\n"
            f"🎯【明日实盘交易指令】\n"
            f"   执行目标仓位 : {target_position * 100:.0f}%\n"
            f"   逻辑触发说明 : {action_reason}\n"
            f"{exec_suggestion}\n"
            f"🔍【关键指标快照】\n"
            f"   • {asset} 价格  : {today[f'{asset}_close']:.2f} (MA{p['ma_len']}: {today[f'{asset}_MA']:.2f})\n"
            f"   • 均线偏离度: {trend_dist:+.2f}%\n"
            f"   • RSI (14)  : {today[f'RSI_14_{asset}']:.2f}\n"
            f"   • 美债 20日动量: {today['US10Y_diff_20']:+.2f}\n"
            f"   • HYG 信用   : {today['HYG_close']:.2f} (MA60: {today['HYG_MA60']:.2f})\n"
            f"   • VIX 恐慌   : {today['VIX_close']:.2f}\n"
            f"{'=' * 60}\n"
        )

        print(report_content)

        # 保存 TXT
        txt_name = f"gemini_trade_tick_{file_date_str}_{asset.lower()}.txt"
        save_txt_path = OUTPUT_PATH / txt_name
        save_txt_path.write_text(report_content, encoding="utf-8")
        print(f"💾 雷达文本指令已保存: {save_txt_path.name}")

        # 生成视觉快照
        pic_path = plot_snapshot_with_levels(df, asset, p, target_position, support, resistance, mid_price, trade_date,
                                             file_date_str)
        print(f"🖼️ 视觉雷达快照已保存: {pic_path.name}")
        print("-" * 60)

    print(f"\n🎉 完毕！所有资产雷达扫描完成！")


if __name__ == "__main__":
    # todo 26-05-06: 根据实际运行环境，在此处手动修改枚举即可！
    # 选项：EnvType.HOME 或 EnvType.WORK
    CURRENT_ENV = EnvType.HOME

    # 动态初始化所有环境路径
    init_environment(CURRENT_ENV)

    # 执行主程序
    generate_daily_report()
