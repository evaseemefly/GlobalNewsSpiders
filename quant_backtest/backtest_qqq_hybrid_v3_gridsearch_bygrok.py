import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from itertools import product

# ==================== Mac 中文字体修复 ====================
# 确保在 Mac 环境下绘图时，图表标题和图例中的中文字符能正常显示，不会变成方块
plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Heiti TC', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

# ⚠️ 数据底座：请确保这是包含了 QQQ, US10Y, HYG, VIX 等所有核心数据的最新宽表
CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_max_20140917_to_20260424.csv")


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    计算 RSI (相对强弱指数)
    金融逻辑：用于衡量资产近期的超买/超卖程度。RSI < 30 通常被视为极度恐慌的超卖区。
    """
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    # 使用指数移动平均 (EWM) 平滑，这比简单移动平均更贴近华尔街看盘软件的真实指标
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def run_strategy(df, ma_len, us10y_th, vix_th, rsi_th, risk_pos):
    """
    核心策略引擎 (无状态化封装)
    隔离原则：该函数内部独立完成所有特征计算与信号生成，绝不污染全局变量。
    这保证了我们在跑 864 次循环时，每次测试都是一个干净的“平行宇宙”。
    """
    df = df.copy()

    # 1. 基础特征计算
    df['QQQ_MA'] = df['QQQ_close'].rolling(window=ma_len, min_periods=1).mean()
    df['US10Y_MA60'] = df['US10Y_close'].rolling(60, min_periods=1).mean()
    df['HYG_MA60'] = df['HYG_close'].rolling(60, min_periods=1).mean()
    df['VIX_MA60'] = df['VIX_close'].rolling(60, min_periods=1).mean()
    df['RSI_14'] = calculate_rsi(df['QQQ_close'])
    df['QQQ_ret'] = df['QQQ_close'].pct_change().fillna(0)  # 每日底层收益率

    # 2. 信号模块构建 (三层逻辑)
    # 模块 A: 顺势底层 (天下太平时，只要大盘在均线之上，就看多)
    base_trend = df['QQQ_close'] > df['QQQ_MA']

    # 模块 B: 宏观防守 (多因子交叉验证)
    #   - 美债动量: 过去20天涨幅超过阈值 (过滤掉了高息震荡的噪音，只抓“利率休克”)
    us10y_rising = df['US10Y_close'].diff(20) > us10y_th
    #   - 信用背离: HYG 连续3天破位，且此时大盘还在涨 (极度危险的诱多信号)
    hyg_divergence = ((df['HYG_close'] < df['HYG_MA60']).rolling(3).sum() == 3) & base_trend
    #   - 波动率炸裂: VIX 突破绝对高位，或者突然飙升至均线的 1.8 倍
    vix_risk = (df['VIX_close'] > vix_th) | (df['VIX_close'] > df['VIX_MA60'] * 1.8)
    #   只要触发以上任意一个宏观警报，立刻进入 Risk-Off (避险) 状态
    risk_off = us10y_rising | hyg_divergence | vix_risk

    # 模块 C: 绝地反击 (左侧抄底)
    # 条件：极度超卖(RSI低) + 当天收阳线(拒绝下跌) + VIX比昨天低(恐慌情绪见顶回落)
    dip_buy = (df['RSI_14'] < rsi_th) & (df['QQQ_close'] > df['QQQ_open']) & (
            df['VIX_close'] < df['VIX_close'].shift(1))

    # 3. 仓位优先级裁决 (极度重要！)
    # 逻辑顺序：最高指令(抄底) -> 第二指令(防守) -> 兜底指令(顺势满仓)
    conditions = [dip_buy, risk_off, base_trend]
    # 如果防守，保留风险底仓(risk_pos, 比如0.3) 作为“趋势的彩票”，拒绝100%空仓带来的踏空磨损
    choices = [1.0, risk_pos, 1.0]
    position = np.select(conditions, choices, default=risk_pos)

    # 🛡️ 量化铁律：防未来函数
    # 今天收盘才确定的信号，只能以“明天”的收益率来结算！所以必须整体 shift(1)
    position = pd.Series(position, index=df.index).shift(1).fillna(risk_pos)

    # 4. 绩效核算
    nav = (1 + position * df['QQQ_ret']).cumprod()  # 累乘计算资金净值

    # 计算年化收益率 (CAGR)
    total_ret = nav.iloc[-1] - 1
    days = (nav.index[-1] - nav.index[0]).days
    years = days / 365.25
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0

    # 计算最大回撤 (Max Drawdown - MDD)
    roll_max = nav.cummax()
    drawdown = (nav - roll_max) / roll_max
    max_dd = drawdown.min()

    # 计算卡玛比率 (Calmar Ratio)
    # 衡量“每承担1份风险，能换取多少收益”，实盘中大于 0.5 即为极度优秀的策略
    calmar = cagr / abs(max_dd) if max_dd != 0 else 0
    time_in_market = (position.mean() * 100)  # 资金利用率

    return {
        'ma_len': ma_len, 'us10y_th': us10y_th, 'vix_th': vix_th, 'rsi_th': rsi_th,
        'risk_pos': risk_pos, 'cagr': cagr, 'max_dd': max_dd, 'calmar': calmar,
        'time_in_market': time_in_market, 'nav': nav, 'position': position
    }


def main():
    print("=== 🚀 QQQ Hybrid 参数自动搜索（Grid Search）===")
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index().ffill()

    # ==================== 参数网格 (Parameter Grid) ====================
    # 构建多维参数池，探寻科技股底层的最优基因组合
    ma_lens = [80, 100, 120, 150, 180, 200]  # 趋势敏感度
    us10y_ths = [0.08, 0.10, 0.12, 0.15]  # 利率休克容忍度
    vix_ths = [35, 38, 40, 45]  # 恐慌熔断阈值
    rsi_ths = [30, 32, 35]  # 抄底激进程度
    risk_poss = [0.3, 0.4, 0.5]  # 避险时的底仓留存率

    results = []
    # product 将所有参数进行笛卡尔积组合，生成 6*4*4*3*3 = 864 种平行宇宙策略
    print(f"开始搜索... 共 {len(ma_lens) * len(us10y_ths) * len(vix_ths) * len(rsi_ths) * len(risk_poss)} 种组合\n")

    for ma, us_th, vix_th, rsi_th, risk_pos in product(ma_lens, us10y_ths, vix_ths, rsi_ths, risk_poss):
        res = run_strategy(df, ma, us_th, vix_th, rsi_th, risk_pos)
        results.append(res)

    # 信仰确立：坚决抛弃“唯收益论”，按 Calmar 比率（风险收益比）降序排列
    # 只有低回撤的稳健系统，才能拿得住、睡得着
    results = sorted(results, key=lambda x: x['calmar'], reverse=True)

    print("🏆 Top 10 最优参数组合 (按 Calmar 排序):")
    print("-" * 120)
    print(
        f"{'排名':<4} {'MA':<4} {'US10Y↑':<7} {'VIX>':<5} {'RSI<':<5} {'风险仓位':<8} {'年化':<8} {'回撤':<8} {'Calmar':<8} {'持仓%':<6}")
    print("-" * 120)
    for i, r in enumerate(results[:10], 1):
        print(f"{i:<4} {r['ma_len']:<4} {r['us10y_th']:<7.2f} {r['vix_th']:<5} {r['rsi_th']:<5} "
              f"{r['risk_pos']:<8.1f} {r['cagr'] * 100:>6.2f}% {r['max_dd'] * 100:>7.2f}% "
              f"{r['calmar']:>7.2f} {r['time_in_market']:>6.1f}%")

    # ==================== 最优参数回测与可视化 ====================
    best = results[0]  # 取出 864 种组合中的绝对王者
    print(
        f"\n🎯 最优参数：MA{best['ma_len']} | US10Y>{best['us10y_th']} | VIX>{best['vix_th']} | RSI<{best['rsi_th']} | 风险仓位 {best['risk_pos']}")
    print(f"年化: {best['cagr'] * 100:.2f}% | 最大回撤: {best['max_dd'] * 100:.2f}% | Calmar: {best['calmar']:.2f}")

    # 绘图逻辑 (上图为对数坐标系下的净值对比，下图为避险雷达图)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.05)

    ax1.plot(df.index, (1 + df['QQQ_close'].pct_change().fillna(0)).cumprod(), label='Baseline (死拿QQQ)', color='gray',
             alpha=0.7)
    ax1.plot(df.index, best['nav'], label=f'Best Hybrid (Calmar {best["calmar"]:.2f})', color='#e74c3c', linewidth=2.5)
    ax1.set_title('QQQ 最优 Hybrid 参数回测结果 (网格搜索寻优)', fontsize=16, fontweight='bold')
    ax1.set_ylabel('Net Asset Value (log scale)')
    ax1.set_yscale('log')
    ax1.legend()
    ax1.grid(True, alpha=0.5)

    ax2.plot(df.index, df['QQQ_close'], label='QQQ Price', color='black')
    ax2.plot(df.index, df['QQQ_close'].rolling(best['ma_len']).mean(), label=f'QQQ MA{best["ma_len"]}', color='orange',
             linestyle='--')
    # 绘制红色阴影区，直观展示机器识别出并执行降低仓位(避险)的残酷岁月
    ax2.fill_between(df.index, ax2.get_ylim()[0], ax2.get_ylim()[1],
                     where=(best['position'] < 0.6), color='red', alpha=0.15,
                     label=f'Risk-Off (保留 {best["risk_pos"]} 仓位)')
    ax2.set_ylabel('QQQ Price')
    ax2.legend()
    ax2.grid(True, alpha=0.5)

    output_pic_path = Path(__file__).parent / "09_best_hybrid.png"
    plt.savefig(output_pic_path, dpi=200, bbox_inches='tight')
    plt.close()
    print(f"\n🖼️ 最优策略图表已保存: {output_pic_path.name}")


if __name__ == "__main__":
    main()
