import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from itertools import product

# ==================== Mac 中文字体修复 ====================
plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Heiti TC', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

# ⚠️ 注意：运行此代码前，请确保您的 CSV 文件中已包含 'RSP_close' 列
CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv")
OUTPUT_DIR = Path(__file__).parent
PROCESSED_CSV_PATH = OUTPUT_DIR / "historical_broad_market_master_v3_1_processed.csv"


def calc_log_linreg_slope(y_array):
    """
    计算给定一维数组的对数线性回归斜率
    使用 numpy 底层以提升网格搜索时的运算速度
    """
    if np.isnan(y_array).any():
        return np.nan
    x = np.arange(len(y_array))
    # y_array 传入前已经在主函数中取过对数
    slope, _ = np.polyfit(x, y_array, 1)
    return slope


def run_strategy_v3_1(df, ma_len, n_period, epsilon):
    """
    V3.1 核心逻辑：VOO 趋势斜率 + RSP 市场广度二维矩阵
    """
    df = df.copy()

    # 1. 计算均线与收益率
    df['VOO_MA'] = df['VOO_close'].rolling(window=ma_len, min_periods=ma_len).mean()
    df['RSP_MA'] = df['RSP_close'].rolling(window=ma_len, min_periods=ma_len).mean()
    df['VOO_ret'] = df['VOO_close'].pct_change().fillna(0)

    # 2. 计算 VOO 均线的对数斜率
    # 提前取对数再进行 rolling apply，大幅优化性能
    df['Log_VOO_MA'] = np.log(df['VOO_MA'])
    df['VOO_MA_Slope'] = df['Log_VOO_MA'].rolling(window=n_period).apply(
        calc_log_linreg_slope, raw=True
    )

    # 3. 提取各个维度的布尔状态
    # 优先级最高：绝对防御判定
    voo_below_ma = df['VOO_close'] < df['VOO_MA']
    voo_above_ma = df['VOO_close'] >= df['VOO_MA']

    # 斜率判定
    trend_up = df['VOO_MA_Slope'] > epsilon
    trend_flat_or_down = df['VOO_MA_Slope'] <= epsilon

    # 广度判定
    breadth_strong = df['RSP_close'] >= df['RSP_MA']
    breadth_weak = df['RSP_close'] < df['RSP_MA']

    # 4. 构建二维状态机矩阵 (State Machine)
    # 顺序决定优先级，条件满足即停止向下匹配
    conditions = [
        voo_below_ma,  # 绝对防御
        voo_above_ma & trend_up & breadth_strong,  # 共振多头
        voo_above_ma & trend_flat_or_down & breadth_strong,  # 积极建仓
        voo_above_ma & trend_up & breadth_weak,  # 巨头抱团
        voo_above_ma & trend_flat_or_down & breadth_weak  # 弱势震荡
    ]

    # 对应的目标仓位映射
    choices = [
        0.3,  # 绝对防御 -> 30%
        1.0,  # 共振多头 -> 100%
        0.7,  # 积极建仓 -> 70%
        0.6,  # 巨头抱团 -> 60%
        0.3  # 弱势震荡 -> 30%
    ]

    # 生成当日信号，默认给出最低防守仓位 0.3 作为保底
    signal = np.select(conditions, choices, default=0.3)

    # ⚠️ 关键步骤：消除未来函数。当日收盘价产生的信号，次日执行
    position = pd.Series(signal, index=df.index).shift(1).fillna(0.3)
    df['Position'] = position

    # 5. 计算资金曲线及核心指标
    nav = (1 + df['Position'] * df['VOO_ret']).cumprod()

    total_ret = nav.iloc[-1] - 1
    days = (nav.index[-1] - nav.index[0]).days
    years = days / 365.25
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0

    roll_max = nav.cummax()
    drawdown = (nav - roll_max) / roll_max
    max_dd = drawdown.min()

    calmar = cagr / abs(max_dd) if max_dd != 0 else 0
    time_in_market = (df['Position'].mean() * 100)

    return {
        'ma_len': ma_len, 'n_period': n_period, 'epsilon': epsilon,
        'cagr': cagr, 'max_dd': max_dd, 'calmar': calmar,
        'time_in_market': time_in_market, 'nav': nav, 'position': df['Position']
    }


def main():
    print("=== 🚀 VOO v3.1 趋势与等权广度软过滤参数搜索 ===")

    # 模拟数据加载防御机制 (如果你测试时缺少 RSP 数据，可以用这段代码临时补齐防止报错)
    try:
        df = pd.read_csv(CSV_FILE_PATH)
    except FileNotFoundError:
        print(f"⚠️ 未找到文件 {CSV_FILE_PATH}，请检查路径。")
        return

    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index().ffill()

    # 检查 RSP 数据列是否存在
    if 'RSP_close' not in df.columns:
        print("⚠️ 警告：数据源缺少 'RSP_close'，正在使用 VOO_close 加噪模拟 RSP 数据以供代码测试通过。")
        print("请在正式回测前抓取真实的 RSP 历史数据入库！")
        np.random.seed(42)
        df['RSP_close'] = df['VOO_close'] * (1 + np.random.normal(0, 0.005, len(df)))

    # 不回写原始 CSV：只在程序内生成 v3.1 需要的收益率列，并另存加工后的数据快照。
    df['VOO_ret'] = df['VOO_close'].pct_change().fillna(0)

    processed_cols = ['VOO_close', 'RSP_close', 'VOO_ret']
    processed_df = df[processed_cols].copy()
    processed_df.to_csv(PROCESSED_CSV_PATH, index_label='trade_date_utc')
    print(f"📄 v3.1 加工后数据已另存为: {PROCESSED_CSV_PATH}")

    # V3.1 参数网格空间设定
    # 均线周期：测试 MA90 到 MA110 的平原稳健性
    ma_lens = [90, 100, 110]
    # 斜率回溯视窗：短（5）、中（10）、长（15）
    n_periods = [5, 10, 15]
    # 死区阈值：0(无死区), 0.0002(每日0.02%), 0.0005(每日0.05%), 0.001(每日0.1%)
    epsilons = [0.0, 0.0002, 0.0005, 0.001]

    results = []
    total_combinations = len(ma_lens) * len(n_periods) * len(epsilons)
    print(f"开始搜索... 共 {total_combinations} 种组合\n")

    for ma, n, eps in product(ma_lens, n_periods, epsilons):
        res = run_strategy_v3_1(df, ma, n, eps)
        results.append(res)

    results = sorted(results, key=lambda x: x['calmar'], reverse=True)

    print("🏆 Top 10 最优参数组合 (按 Calmar 排序):")
    print("-" * 100)
    print(
        f"{'排名':<4} {'MA均线':<8} {'视窗(N)':<8} {'死区(Eps)':<10} {'年化':<8} {'最大回撤':<10} {'Calmar':<8} {'平均持仓':<6}")
    print("-" * 100)
    for i, r in enumerate(results[:10], 1):
        print(f"{i:<4} {r['ma_len']:<8} {r['n_period']:<8} {r['epsilon']:<10.4f} "
              f"{r['cagr'] * 100:>6.2f}% {r['max_dd'] * 100:>8.2f}% "
              f"{r['calmar']:>7.2f} {r['time_in_market']:>7.1f}%")

    # ==================== 最优参数可视化（Top 10 曲线叠加） ====================
    best = results[0]
    print(f"\n🎯 V3.1 最优参数：MA{best['ma_len']} | 视窗 N={best['n_period']} | 死区 ε={best['epsilon']}")
    print(f"年化: {best['cagr'] * 100:.2f}% | 最大回撤: {best['max_dd'] * 100:.2f}% | Calmar: {best['calmar']:.2f}")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 12), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.05)

    # 上图：Baseline + Top 10 曲线
    baseline_nav = (1 + df['VOO_ret']).cumprod()
    ax1.plot(df.index, baseline_nav, label='Baseline (Buy & Hold VOO)', color='gray', alpha=0.7, linewidth=2)

    colors = plt.cm.tab10(np.linspace(0, 1, 10))
    for i, r in enumerate(results[:10]):
        label = f"Top{i + 1} (MA{r['ma_len']}|N{r['n_period']}|ε{r['epsilon']}) Calmar {r['calmar']:.2f}"
        linewidth = 3.0 if i == 0 else 1.5
        alpha = 1.0 if i == 0 else 0.85
        ax1.plot(df.index, r['nav'], label=label, color=colors[i], linewidth=linewidth, alpha=alpha)

    ax1.set_title('VOO V3.1 趋势广度软过滤模型（Top 10 参数对比）', fontsize=16, fontweight='bold')
    ax1.set_ylabel('Net Asset Value (log scale)')
    ax1.set_yscale('log')
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.5)

    # 下图：标的价格 + 仓位变动分布
    ax2.plot(df.index, df['VOO_close'], label='VOO Price', color='black')
    ax2.plot(df.index, df['VOO_close'].rolling(best['ma_len']).mean(),
             label=f'VOO MA{best["ma_len"]}', color='orange', linestyle='--')

    # 使用底色标记不同的仓位状态区间 (针对 Top1 最优策略)
    ax2.fill_between(df.index, ax2.get_ylim()[0], ax2.get_ylim()[1],
                     where=(best['position'] == 1.0), color='green', alpha=0.1,
                     label='100% 满仓 (多头共振)')
    ax2.fill_between(df.index, ax2.get_ylim()[0], ax2.get_ylim()[1],
                     where=(best['position'] == 0.3), color='red', alpha=0.1,
                     label='30% 底仓 (防守/空头)')

    ax2.set_ylabel('VOO Price & Positions')
    ax2.legend(loc='upper left', fontsize=9)
    ax2.grid(True, alpha=0.5)

    output_pic_path = Path(__file__).parent / "10_best_v3_1_trend_breadth.png"
    plt.savefig(output_pic_path, dpi=200, bbox_inches='tight')
    plt.close()
    print(f"\n🖼️ V3.1 策略图表已保存至当前目录: {output_pic_path.name}")


if __name__ == "__main__":
    main()
