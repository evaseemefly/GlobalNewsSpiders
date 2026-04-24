import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import platform
from pathlib import Path

# ==========================================
# 1. 路径配置与数据加载
# ==========================================
# ⚠️ 注意：运行此代码前，请务必先重新运行你的历史爬虫脚本，确保拿到了包含 US10Y 和 VIX 的新数据！
CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_max_20140917_to_20260424.csv")


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """计算 RSI (相对强弱指数) 的标准量化实现"""
    delta = series.diff()
    # 区分上涨和下跌
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    # 使用指数移动平均 (EMA) 平滑，这比简单移动平均 (SMA) 更贴近华尔街真实指标
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_metrics(nav_series: pd.Series, name: str):
    """计算年化收益率和最大回撤"""
    total_days = len(nav_series)
    cagr = (nav_series.iloc[-1] ** (252 / total_days)) - 1

    roll_max = nav_series.cummax()
    drawdown = nav_series / roll_max - 1.0
    max_dd = drawdown.min()

    print(f"[{name}] \t 年化收益率: {cagr * 100:>5.2f}% \t 最大回撤: {max_dd * 100:>7.2f}%")
    return cagr, max_dd


def main():
    print("=== 🚀 QQQ 终极多因子量化擂台赛 (Gemini vs Grok) ===")

    if not CSV_FILE_PATH.exists():
        print(f"❌ 找不到数据文件: {CSV_FILE_PATH}")
        print("💡 提示：请确保你已经修改了爬虫脚本，加入了 US10Y 和 VIX，并重新拉取了数据。")
        return

    # 读取并清洗数据
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()

    # 如果有缺失值，进行前向填充
    df = df.ffill()

    # ==========================================
    # 2. 基础指标引擎计算 (Indicators)
    # ==========================================
    df['QQQ_MA120'] = df['QQQ_close'].rolling(window=120, min_periods=1).mean()
    df['US10Y_MA60'] = df['US10Y_close'].rolling(window=60, min_periods=1).mean()
    df['HYG_MA60'] = df['HYG_close'].rolling(window=60, min_periods=1).mean()
    df['RSI_14'] = calculate_rsi(df['QQQ_close'], 14)

    # 每日底层收益率
    df['QQQ_ret'] = df['QQQ_close'].pct_change().fillna(0)

    # 1. 大盘基准 (死拿)
    df['NAV_Baseline'] = (1 + df['QQQ_ret']).cumprod()

    # ==========================================
    # 🛡️ 方案 A: Gemini 终极方案 (宏观利率滤网)
    # ==========================================
    # 条件1 (逃顶)：US10Y 连续3天突破均线，或 VIX 飙升突破 25# 【旧代码】
    #     # us10y_above_ma = df['US10Y_close'] > df['US10Y_MA60']
    #     # us10y_risk = us10y_above_ma.rolling(window=3).sum() == 3
    #
    # 【新代码：利率休克 (Rate Shock) 过滤】
    # 只有当 US10Y 不仅突破均线，而且比均线高出 10% (极端飙升) 时，才认为是危险的拔估值
    us10y_shock = df['US10Y_close'] > (df['US10Y_MA60'] * 1.10)
    us10y_risk = us10y_shock.rolling(window=3).sum() == 3
    # gemini_risk_off = us10y_risk | (df['VIX_close'] > 25)
    gemini_risk_off = us10y_risk | (df['VIX_close'] > 28)  # VIX 阈值从 25 稍微放宽到 28

    # 条件2 (抄底)：QQQ 跌破均线 + RSI极度超卖(<30) + VIX今日低于昨日(恐慌情绪见顶回落)
    gemini_dip_buy = (df['QQQ_close'] < df['QQQ_MA120']) & (df['RSI_14'] < 30) & (
            df['VIX_close'] < df['VIX_close'].shift(1))

    # 条件3 (顺势)：常规多头趋势
    base_trend = df['QQQ_close'] > df['QQQ_MA120']

    # 优先级逻辑融合 (np.select: 列表越靠前，优先级越高)
    # 逻辑顺序：先判断是否极度恐慌需要抄底 -> 如果不抄底，判断是否需要逃顶避险 -> 否则跟随趋势
    gemini_conditions = [gemini_dip_buy, gemini_risk_off, base_trend]
    gemini_choices = [1, 0, 1]  # 1 为满仓，0 为空仓

    # shift(1) 是量化铁律：今天收盘产生的信号，决定明天怎么操作
    df['Signal_Gemini'] = np.select(gemini_conditions, gemini_choices, default=0)
    df['Signal_Gemini'] = pd.Series(df['Signal_Gemini'], index=df.index).shift(1).fillna(0)
    df['NAV_Gemini'] = (1 + df['Signal_Gemini'] * df['QQQ_ret']).cumprod()

    # ==========================================
    # 🛡️ 方案 B: Grok 终极方案 (信用分歧预警)
    # ==========================================
    # 条件1 (逃顶)：US10Y突破均线，或 (HYG连续3天破位 且 QQQ还在涨)，或 VIX>30
    # 【旧代码】
    # hyg_below_ma = df['HYG_close'] < df['HYG_MA60']
    # hyg_divergence = (hyg_below_ma.rolling(window=3).sum() == 3) & (df['QQQ_close'] > df['QQQ_MA120'])
    # grok_risk_off = (df['US10Y_close'] > df['US10Y_MA60']) | hyg_divergence | (df['VIX_close'] > 30)

    # 【新代码：严格信用背离】
    # HYG不仅要破均线，而且必须是连续暴跌(比均线低2%)，才能阻断 QQQ 的多头
    hyg_divergence = (df['HYG_close'] < (df['HYG_MA60'] * 0.98)) & (df['QQQ_close'] > df['QQQ_MA120'])
    # 去掉绝对的 US10Y 压制，引入和上面一样的 Rate Shock
    grok_risk_off = us10y_risk | hyg_divergence | (df['VIX_close'] > 30)
    # 条件2 (抄底)：QQQ 跌破均线 + RSI<30 + 当天收出阳线(拒绝下跌)
    grok_dip_buy = (df['QQQ_close'] < df['QQQ_MA120']) & (df['RSI_14'] < 30) & (df['QQQ_close'] > df['QQQ_open'])

    # 优先级逻辑融合
    grok_conditions = [grok_dip_buy, grok_risk_off, base_trend]
    grok_choices = [1, 0, 1]

    df['Signal_Grok'] = np.select(grok_conditions, grok_choices, default=0)
    df['Signal_Grok'] = pd.Series(df['Signal_Grok'], index=df.index).shift(1).fillna(0)
    df['NAV_Grok'] = (1 + df['Signal_Grok'] * df['QQQ_ret']).cumprod()

    # ==========================================
    # 打印擂台赛结果
    # ==========================================
    print("\n📊 回测结果 (以实际拉取年份为准):")
    print("-" * 65)
    calculate_metrics(df['NAV_Baseline'], "大盘基准 (死拿 QQQ)")
    calculate_metrics(df['NAV_Gemini'], "Gemini (宏观利率滤网)")
    calculate_metrics(df['NAV_Grok'], "Grok (信用分歧预警)")
    print("-" * 65)

    # ==========================================
    # 绘制高阶可视化对比图表
    # ==========================================
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 14), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.08)

    # 上图：净值曲线对决
    ax1.plot(df.index, df['NAV_Baseline'], label='Baseline (Buy & Hold QQQ)', color='gray', alpha=0.5, linewidth=1.5)
    ax1.plot(df.index, df['NAV_Gemini'], label='Gemini Ultimate (Macro Rate Filter)', color='#e74c3c', linewidth=2)
    ax1.plot(df.index, df['NAV_Grok'], label='Grok Ultimate (Credit Divergence)', color='#2980b9', linewidth=2)

    ax1.set_title('Quantitative Arena: Gemini vs Grok Strategy (Log Scale)', fontsize=18, fontweight='bold')
    ax1.set_ylabel('Net Asset Value (Initial = 1)', fontsize=14, fontweight='bold')
    ax1.set_yscale('log')
    ax1.legend(loc='upper left', fontsize=12)
    ax1.grid(True, linestyle='--', alpha=0.5)

    # 下图：大盘走势与两大策略的“空仓(避险)雷达”
    ax2.plot(df.index, df['QQQ_close'], label='QQQ Price', color='black', linewidth=1.2)
    ax2.plot(df.index, df['QQQ_MA120'], label='QQQ MA120', color='orange', linewidth=1.5, linestyle='--')

    # 绘制底色带，直观对比两个 AI 的逃顶时机
    # 红色区域代表 Gemini 判定危险空仓；蓝色区域代表 Grok 判定危险空仓；重合区域会变成紫色
    ax2.fill_between(df.index, ax2.get_ylim()[0], ax2.get_ylim()[1],
                     where=(df['Signal_Gemini'] == 0), color='red', alpha=0.15, label='Gemini Risk-Off (Cash)')
    ax2.fill_between(df.index, ax2.get_ylim()[0], ax2.get_ylim()[1],
                     where=(df['Signal_Grok'] == 0), color='blue', alpha=0.15, label='Grok Risk-Off (Cash)')

    ax2.set_ylabel('QQQ Price', fontsize=14, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=11)
    ax2.grid(True, linestyle='--', alpha=0.5)

    output_pic_path = Path(__file__).parent / "ultimate_arena_result.png"
    plt.savefig(output_pic_path, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"\n🖼️ 擂台赛高阶图表已保存至: {output_pic_path.name}")


if __name__ == "__main__":
    main()
