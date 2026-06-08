import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from enum import Enum, auto  # todo: 26-05-06 新增：导入枚举相关库


# ==================== 1. 定义环境枚举 ====================
class EnvType(Enum):
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
CURRENT_ENV = EnvType.WORK

# 获取配置字典
CONFIG = get_env_config(CURRENT_ENV)

# 显式地赋值给常量，方便后续直接使用
CSV_FILE_PATH = CONFIG['csv_file']
OUTPUT_PATH = CONFIG['output_dir']
FIGURES_PATH = CONFIG['figures_dir']

print(f"⚙️ 运行环境: [{CURRENT_ENV.name}]")
print(f"📂 数据路径: {CSV_FILE_PATH}")

# todo: 26-05-06 新增：动态初始化环境路径的方法
# def init_environment(env: EnvType):
#     """根据传入的枚举类型，动态加载并创建相关的全局路径配置"""
#     global CSV_FILE_PATH, OUTPUT_PATH, FIGURES_PATH
#
#     root_path = ENV_CONFIG[env]
#
#     CSV_FILE_PATH = root_path / "broad_market_history/historical_broad_market_master.csv"
#     OUTPUT_PATH = root_path / "output/trade_msg"
#
#     # 确保输出目录存在
#     OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
#     FIGURES_PATH = OUTPUT_PATH / "figures"
#     FIGURES_PATH.mkdir(parents=True, exist_ok=True)
#
#     print(f"⚙️ 当前手动配置运行环境: [{env.name}]")
#     print(f"📂 数据源读取路径: {CSV_FILE_PATH}")
#     print(f"💾 报告输出主目录: {OUTPUT_PATH}\n")


# todo: 26-05-06 整合配置：将 QQQ 和 VOO 的参数合并为字典格式以便循环调用
# todo:26-06-08: 将 Risk-Off 一刀切模型升级为 Risk-On / Risk-Warning / Risk-Off / Panic-Reversal / Crash 五层仓位模型
ASSET_CONFIG = {
    'QQQ': {
        'ma_len': 200,
        'crash_ma_len': 200,
        'us10y_th': 0.15,

        # 风险阈值
        'vix_warning_low': 15,
        'vix_warning_high': 20,
        'vix_risk_th': 20,
        'vix_crash_th': 30,
        'vix_extreme_th': 45,

        # RSI / 恐慌反转参数
        'rsi_th': 30,
        'panic_rsi_low': 35,
        'panic_rsi_high': 45,
        'panic_drop_pct': 0.045,

        # 五层仓位
        'risk_on_pos': 1.0,
        'risk_warning_pos': 0.60,
        'risk_pos': 0.30,
        'panic_reversal_pos': 0.40,
        'crash_pos': 0.25,

        # todo:26-06-08: 可选，填入真实持仓后自动计算明日应买/卖金额与股数
        # 'portfolio_value': 19890,
        # 'current_shares': 13,
    },
    'VOO': {
        'ma_len': 100,
        'crash_ma_len': 200,
        'us10y_th': 0.15,

        # 风险阈值
        'vix_warning_low': 15,
        'vix_warning_high': 20,
        'vix_risk_th': 20,
        'vix_crash_th': 30,
        'vix_extreme_th': 45,

        # RSI / 恐慌反转参数
        'rsi_th': 30,
        'panic_rsi_low': 35,
        'panic_rsi_high': 45,
        'panic_drop_pct': 0.025,

        # 五层仓位
        'risk_on_pos': 1.0,
        'risk_warning_pos': 0.70,
        'risk_pos': 0.40,
        'panic_reversal_pos': 0.50,
        'crash_pos': 0.30,

        # todo:26-06-08: 可选，填入真实持仓后自动计算明日应买/卖金额与股数
        # 'portfolio_value': 25590,
        # 'current_shares': 25,
    }
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


# todo: 26-05-06 新增：提取复用逻辑为生成信号特征的独立方法
def process_asset_signals(df: pd.DataFrame, asset: str, p: dict) -> pd.DataFrame:
    """为特定标的计算指标和生成量化交易历史信号"""
    df = df.copy()

    # 指标计算
    df[f'{asset}_MA'] = df[f'{asset}_close'].rolling(p['ma_len'], min_periods=1).mean()
    # todo:26-06-08: 额外计算 MA100 / MA200 与单日涨跌幅，用于 Crash 与 Panic-Reversal 判断
    df[f'{asset}_MA100'] = df[f'{asset}_close'].rolling(100, min_periods=1).mean()
    df[f'{asset}_MA200'] = df[f'{asset}_close'].rolling(200, min_periods=1).mean()
    df[f'{asset}_daily_return'] = df[f'{asset}_close'].pct_change()
    df['US10Y_diff_20'] = df['US10Y_close'].diff(20)
    df['HYG_MA60'] = df['HYG_close'].rolling(60, min_periods=1).mean()
    df['VIX_MA60'] = df['VIX_close'].rolling(60, min_periods=1).mean()
    df[f'RSI_14_{asset}'] = calculate_rsi(df[f'{asset}_close'])

    # 历史向量化信号计算 (用于绘制净值曲线)
    df['base_trend'] = df[f'{asset}_close'] > df[f'{asset}_MA']
    df['us10y_rising'] = df['US10Y_diff_20'] > p['us10y_th']
    df['credit_weak'] = df['HYG_close'] < df['HYG_MA60']
    df['hyg_divergence'] = False
    if len(df) >= 3:
        df['hyg_divergence'] = ((df['HYG_close'] < df['HYG_MA60']).rolling(3).sum() == 3) & df['base_trend']

    # todo:26-06-08: 新增 Risk-Warning / Risk-Off / Panic-Reversal / Crash 分层
    df['vix_warning'] = (df['VIX_close'] >= p['vix_warning_low']) & (df['VIX_close'] <= p['vix_warning_high'])
    df['vix_risk'] = df['VIX_close'] > p['vix_risk_th']
    df['vix_extreme'] = df['VIX_close'] > p['vix_extreme_th']
    df['vix_crash'] = df['VIX_close'] > p['vix_crash_th']

    df['risk_warning'] = (df['credit_weak'] | df['vix_warning']) & (~df['vix_risk'])
    df['risk_off'] = df['us10y_rising'] | df['hyg_divergence'] | (df['credit_weak'] & df['vix_risk']) | df['vix_extreme']

    df['panic_reversal'] = (
        df['risk_off']
        & (df[f'{asset}_daily_return'] <= -p['panic_drop_pct'])
        & (df[f'RSI_14_{asset}'].between(p['panic_rsi_low'], p['panic_rsi_high']))
    )

    df['crash'] = (
        ((df[f'{asset}_close'] < df[f'{asset}_MA200']) & df['vix_crash'])
        | ((df[f'{asset}_close'] < df[f'{asset}_MA100']) & (df['VIX_close'] > p['vix_crash_th']))
    )

    df['dip_buy'] = (df[f'RSI_14_{asset}'] < p['rsi_th']) & (df[f'{asset}_close'] > df[f'{asset}_open']) & (
            df['VIX_close'] < df['VIX_close'].shift(1))

    # 历史仓位计算：Crash > Panic-Reversal > Risk-Off > Risk-Warning > Dip-Buy/Risk-On
    df['position'] = np.select(
        [
            df['crash'],
            df['panic_reversal'],
            df['risk_off'],
            df['risk_warning'],
            df['dip_buy'],
            df['base_trend'],
        ],
        [
            p['crash_pos'],
            p['panic_reversal_pos'],
            p['risk_pos'],
            p['risk_warning_pos'],
            p['risk_on_pos'],
            p['risk_on_pos'],
        ],
        default=p['risk_pos'],
    )
    df['position'] = df['position'].shift(1).fillna(p['risk_pos'])

    return df


def classify_latest_market_state(df: pd.DataFrame, asset: str, p: dict) -> dict:
    # todo:26-06-08: 提取最新交易日的五层状态，供文本报告使用
    today = df.iloc[-1]

    if today['crash']:
        state = 'Crash'
        position = p['crash_pos']
        action = f"🧊 Crash 防守：降至 {int(position * 100)}% 防守仓"
    elif today['panic_reversal']:
        state = 'Panic-Reversal'
        position = p['panic_reversal_pos']
        action = f"🟡 Risk-Off 恐慌反弹试探：允许小比例提高至 {int(position * 100)}%"
    elif today['risk_off']:
        state = 'Risk-Off'
        position = p['risk_pos']
        action = f"🔴 风险-Off：减仓至 {int(position * 100)}% 防守"
    elif today['risk_warning']:
        state = 'Risk-Warning'
        position = p['risk_warning_pos']
        action = f"⚠️ 风险预警：降至 {int(position * 100)}% 观察仓"
    elif today['dip_buy']:
        state = 'Dip-Buy'
        position = p['risk_on_pos']
        action = f"🟢 非 Risk-Off 左侧抄底：恢复至 {int(position * 100)}%"
    elif today['base_trend']:
        state = 'Risk-On'
        position = p['risk_on_pos']
        action = f"🟢 多头趋势：维持 {int(position * 100)}% 满仓"
    else:
        state = 'Trend-Weak'
        position = p['risk_pos']
        action = f"⚪ 趋势走弱：保持 {int(position * 100)}% 防守仓"

    return {'state': state, 'position': position, 'action': action}


def build_execution_amount_plan(asset: str, p: dict, current_price: float, target_position: float) -> str:
    # todo:26-06-08: 若配置真实持仓，则输出明日应买/卖金额与股数
    portfolio_value = p.get('portfolio_value')
    current_shares = p.get('current_shares')

    if portfolio_value is None or current_shares is None:
        return (
            "\n💰【实盘金额估算】\n"
            "   • 尚未配置 portfolio_value / current_shares，因此只输出目标仓位，不计算具体买卖股数。\n"
            "   • 可在 ASSET_CONFIG 中加入：'portfolio_value': 19890, 'current_shares': 13。\n"
        )

    current_value = current_shares * current_price
    target_value = portfolio_value * target_position
    diff_value = target_value - current_value
    trade_shares = int(abs(diff_value) / current_price)

    if abs(diff_value) < current_price:
        action = "持有不动"
        detail = "差额不足 1 股，暂不需要交易。"
    elif diff_value > 0:
        action = f"买入 {trade_shares} 股"
        detail = f"预计增加约 ${trade_shares * current_price:,.2f}。"
    else:
        action = f"卖出 {trade_shares} 股"
        detail = f"预计回收约 ${trade_shares * current_price:,.2f}。"

    return (
        "\n💰【实盘金额估算】\n"
        f"   • 账户/资金池规模 : ${portfolio_value:,.2f}\n"
        f"   • 当前持仓       : {current_shares} 股，市值约 ${current_value:,.2f}\n"
        f"   • 目标仓位       : {target_position * 100:.0f}%，目标市值约 ${target_value:,.2f}\n"
        f"   • 明日动作       : {action}，{detail}\n"
    )


# todo: 26-05-06 新增：提取复用的图表生成逻辑，并在下方增加箱体与加仓买点可视化
def plot_equity_and_levels(df: pd.DataFrame, asset: str, p: dict, support: float, resistance: float, mid_price: float,
                           date_str: str, file_date: str) -> Path:
    """生成包含策略净值及近期箱体/买入计划的综合图表"""
    baseline = (1 + df[f'{asset}_close'].pct_change().fillna(0)).cumprod()
    strategy = (1 + df['position'] * df[f'{asset}_close'].pct_change().fillna(0)).cumprod()

    # 创建上下两行子图 (高度比 2:1.5)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12), gridspec_kw={'height_ratios': [2, 1.5]}, sharex=False)

    # --- 上图：净值曲线 ---
    ax1.plot(baseline.index, baseline, label=f'Baseline (死拿{asset})', color='gray', alpha=0.7, linewidth=1.5)
    ax1.plot(strategy.index, strategy, label=f'Hybrid Strategy ({asset})', color='#e74c3c', linewidth=2.5)
    ax1.set_title(f'{asset} Hybrid 策略净值曲线及执行计划（截至 {date_str}）', fontsize=16, fontweight='bold')
    ax1.set_ylabel('净值 (初始 = 1)')
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)

    # --- 下图：近期价格、箱体及三次加仓计划标记 ---
    # 取最近约半年(120个交易日)展示清晰细节
    recent_df = df.tail(120)
    ax2.plot(recent_df.index, recent_df[f'{asset}_close'], label=f'{asset} Close Price', color='#2980b9', linewidth=2)
    ax2.plot(recent_df.index, recent_df[f'{asset}_MA'], label=f'MA{p["ma_len"]}', color='#f39c12', linestyle='-',
             linewidth=1.5)

    # 用阴影标记近 60 天的箱体计算范围
    box_start = df.tail(60).index[0]
    box_end = df.tail(60).index[-1]
    ax2.axvspan(box_start, box_end, color='lightgray', alpha=0.2, label='Box Horizon (60d)')

    # 标记关键位置
    ax2.axhline(resistance, color='red', linestyle='--', alpha=0.6, label=f'Resistance ({resistance:.2f})')
    ax2.axhline(support, color='green', linestyle='--', alpha=0.6, label=f'Support / Buy 2 ({support:.2f})')
    ax2.axhline(mid_price, color='blue', linestyle='-.', alpha=0.8, label=f'Initial Buy (Mid: {mid_price:.2f})')

    # 标记按比例回撤的买入点位
    buy1_price = resistance * 0.94  # -6%
    buy2_price = resistance * 0.89  # -11%
    ax2.axhline(buy1_price, color='purple', linestyle=':', linewidth=2, label=f'Buy 1 (-6%: {buy1_price:.2f})')
    ax2.axhline(buy2_price, color='brown', linestyle=':', linewidth=2, label=f'Buy 2 (-11%: {buy2_price:.2f})')

    # 左下角增加 RSI 买3 文本提示框
    ax2.text(0.02, 0.05, "【注意】第3次加仓点位: 极端恐慌 (RSI < 30) 时触发",
             transform=ax2.transAxes, color='red', fontsize=11, fontweight='bold',
             bbox=dict(facecolor='white', alpha=0.8, edgecolor='lightgray'))

    ax2.set_title(f'{asset} 近期箱体空间与加仓位置可视化', fontsize=14)
    ax2.set_ylabel('Price')
    ax2.legend(loc='lower left', fontsize=9, ncol=2)
    ax2.grid(True, alpha=0.3)

    chart_path = FIGURES_PATH / f"hybrid_equity_{file_date}_{asset.lower()}.png"
    plt.tight_layout()
    plt.savefig(chart_path, dpi=200, bbox_inches='tight')
    plt.close()

    return chart_path


def generate_daily_report():
    print("=== 🚀 终极量化每日信号生成器（多资产合并版）===")

    if not CSV_FILE_PATH or not CSV_FILE_PATH.exists():
        print(f"❌ 找不到主数据文件: {CSV_FILE_PATH}")
        print("请确认环境配置枚举是否正确，且数据文件已同步。")
        return

    # 读取并处理全量公共数据
    master_df = pd.read_csv(CSV_FILE_PATH)
    master_df['trade_date_utc'] = pd.to_datetime(master_df['trade_date_utc'])
    master_df = master_df.set_index('trade_date_utc').sort_index().ffill()

    # todo: 26-05-06 循环处理所有在配置中的标的资产
    for asset, p in ASSET_CONFIG.items():
        print(f"\n⏳ 正在生成 {asset} 的信号及报告...")

        # 获取包含指标的 DataFrame
        df = process_asset_signals(master_df, asset, p)

        today = df.iloc[-1]
        yesterday = df.iloc[-2]
        date_str = today.name.strftime('%Y-%m-%d')
        file_date = date_str.replace('-', '_')

        # ==================== 核心单日判定 ====================
        # todo:26-06-08: 使用五层仓位状态替代 Risk-Off 一刀切
        latest_signal = classify_latest_market_state(df, asset, p)
        market_state = latest_signal['state']
        position = latest_signal['position']
        action = latest_signal['action']

        base_trend = bool(today['base_trend'])
        us10y_rising = bool(today['us10y_rising'])
        hyg_divergence = bool(today['hyg_divergence'])
        vix_risk = bool(today['vix_risk'])
        risk_off = bool(today['risk_off'])
        dip_buy = bool(today['dip_buy'])

        # ==================== 箱体区间 + 执行建议 ====================
        recent = df.tail(60)
        support = recent[f'{asset}_close'].min()
        resistance = recent[f'{asset}_close'].max()
        mid_price = (support + resistance) / 2

        # todo:26-06-08: 按不同市场状态给出执行建议，不再 Risk-Off 下完全禁止试探仓
        amount_plan = build_execution_amount_plan(asset, p, today[f'{asset}_close'], position)

        if market_state in ['Risk-On', 'Dip-Buy']:
            exec_suggestion = f"""
📍 当前箱体区间: {support:.2f} — {resistance:.2f}（中轴 {mid_price:.2f}）
💡 执行建议:
   • 初始建仓: 当前价或 {mid_price:.2f} 附近（建议占总仓位 40%）
   • 第1次加仓: 回落至 MA{p['ma_len']} 附近 或 -6%（+20%）
   • 第2次加仓: 回落至箱体支撑 {support:.2f} 附近 或 -11%（+20%）
   • 第3次加仓: 极端恐慌（RSI<30）（+20%）
{amount_plan}"""
        elif market_state == 'Panic-Reversal':
            exec_suggestion = f"""
📍 当前箱体区间: {support:.2f} — {resistance:.2f}（中轴 {mid_price:.2f}）
💡 Panic-Reversal 执行建议:
   • 当前仍是 Risk-Off，禁止恢复满仓。
   • 允许从防守仓小幅提高至 {position * 100:.0f}% 试探仓。
   • 若连续 2 日不创新低，或盘中跌破后收回，可考虑下一笔 10%。
   • 若 HYG 重新站上 MA60 且 VIX 回落至 18 以下，再恢复到 60%—70%。
   • 若跌破 MA100/MA200 或 VIX > {p['vix_crash_th']}，停止加仓并切入 Crash 防守。
{amount_plan}"""
        elif market_state == 'Risk-Warning':
            exec_suggestion = f"""
📍 当前为 Risk-Warning 状态：降低到 {position * 100:.0f}% 观察仓。
💡 执行建议:
   • 不追涨，不满仓。
   • 若 HYG 修复且 VIX 回落，可恢复趋势仓。
   • 若 HYG 跌破 MA60 且 VIX > {p['vix_risk_th']}，切换至 Risk-Off。
{amount_plan}"""
        elif market_state == 'Risk-Off':
            exec_suggestion = f"""
📍 当前为 Risk-Off 状态：保持 {position * 100:.0f}% 防守仓。
💡 执行建议:
   • 暂不新增趋势仓。
   • 只有出现单日恐慌跌幅且 RSI 落入 {p['panic_rsi_low']}—{p['panic_rsi_high']}，才允许小仓试探。
{amount_plan}"""
        else:
            exec_suggestion = f"""
📍 当前为 {market_state} 状态：目标仓位 {position * 100:.0f}%。
💡 执行建议:
   • 优先控制回撤，不做主动加仓。
{amount_plan}"""

        # ==================== 写入文本报告 ====================
        grok_text = f"""📅 【{date_str} {asset} 中长期量化信号】
{'=' * 55}
市场状态：{market_state}
趋势框架：{'✅ 多头' if base_trend else '❌ 空头'} ({asset} vs MA{p['ma_len']})
宏观利率：{'⚠️ 上升' if us10y_rising else '✅ 安全'} (20日变化 {today['US10Y_diff_20']:+.2f})
信用背离：{'⚠️ 触发' if hyg_divergence else '✅ 正常'}
恐慌指数：{'⚠️ 高位' if vix_risk else '✅ 低位'} (VIX={today['VIX_close']:.2f})
单日涨跌：{today[f'{asset}_daily_return'] * 100:+.2f}%
抄底机会：{'🟢 触发' if dip_buy else '❌ 未触发'} (RSI={today[f'RSI_14_{asset}']:.1f})
{'-' * 55}
📢 操作建议：{action}
🎯 推荐仓位：{int(position * 100)}%
{exec_suggestion}
"""
        grok_file = OUTPUT_PATH / f"grok_trade_tick_{file_date}_{asset.lower()}.txt"
        grok_file.write_text(grok_text, encoding='utf-8')
        print(f"✅ {asset} 文本报告已保存 → {grok_file.name}")

        # ==================== 生成最终图表 ====================
        chart_path = plot_equity_and_levels(df, asset, p, support, resistance, mid_price, date_str, file_date)
        print(f"✅ {asset} 净值及箱体路线图已保存 → {chart_path.name}")

    print(f"\n🎉 今日所有资产报告生成完成！所有文件均保存在：{OUTPUT_PATH}")


if __name__ == "__main__":
    # todo: 26-05-06 手动环境切换区域！
    # 如果在家中运行，请设置为 EnvType.HOME；在单位运行请设置为 EnvType.WORK。
    # CURRENT_ENV = EnvType.HOME

    # 在主程序运行前，动态挂载所有路径配置
    # init_environment(CURRENT_ENV)

    generate_daily_report()