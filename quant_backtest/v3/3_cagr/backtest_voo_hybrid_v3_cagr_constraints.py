import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from itertools import product
from typing import Optional

# ==================== Mac 中文字体修复 ====================
plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Heiti TC', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv"
)
OUTPUT_DIR = Path(__file__).parent
CALMAR_FLOOR = 0.60
MAX_DD_LIMITS = [-0.18, -0.20, -0.22]


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_metrics(nav: pd.Series, position: pd.Series) -> dict:
    if nav.empty:
        return {
            'cagr': 0.0,
            'max_dd': 0.0,
            'calmar': 0.0,
            'avg_exposure': 0.0,
            'dd_peak': pd.NaT,
            'dd_trough': pd.NaT,
        }

    total_ret = nav.iloc[-1] - 1
    days = (nav.index[-1] - nav.index[0]).days
    years = days / 365.25
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 and total_ret > -1 else 0.0

    roll_max = nav.cummax()
    drawdown = nav / roll_max - 1
    max_dd = drawdown.min()
    dd_trough = drawdown.idxmin()
    dd_peak = nav.loc[:dd_trough].idxmax()

    calmar = cagr / abs(max_dd) if max_dd < 0 else 0.0
    avg_exposure = position.mean() * 100

    return {
        'cagr': cagr,
        'max_dd': max_dd,
        'calmar': calmar,
        'avg_exposure': avg_exposure,
        'dd_peak': dd_peak,
        'dd_trough': dd_trough,
    }


def run_strategy(df: pd.DataFrame, ma_len: int, us10y_th: float, vix_th: float, rsi_th: int, risk_pos: float) -> dict:
    df = df.copy()
    df['VOO_MA'] = df['VOO_close'].rolling(window=ma_len, min_periods=1).mean()
    df['US10Y_MA60'] = df['US10Y_close'].rolling(60, min_periods=1).mean()
    df['HYG_MA60'] = df['HYG_close'].rolling(60, min_periods=1).mean()
    df['VIX_MA60'] = df['VIX_close'].rolling(60, min_periods=1).mean()
    df['RSI_14'] = calculate_rsi(df['VOO_close'])
    df['VOO_ret'] = df['VOO_close'].pct_change().fillna(0)

    base_trend = df['VOO_close'] > df['VOO_MA']
    us10y_rising = df['US10Y_close'].diff(20) > us10y_th
    hyg_divergence = ((df['HYG_close'] < df['HYG_MA60']).rolling(3).sum() == 3) & base_trend
    vix_risk = (df['VIX_close'] > vix_th) | (df['VIX_close'] > df['VIX_MA60'] * 1.8)
    risk_off = us10y_rising | hyg_divergence | vix_risk

    raw_dip_buy = (
        (df['RSI_14'] < rsi_th)
        & (df['VOO_close'] > df['VOO_open'])
        & (df['VIX_close'] < df['VIX_close'].shift(1))
    )

    # Revised: 系统性避险优先。risk_off 期间不允许 dip_buy 把仓位重新拉满。
    dip_buy = raw_dip_buy & (~risk_off)

    conditions = [risk_off, dip_buy, base_trend]
    choices = [risk_pos, 1.0, 1.0]
    position = np.select(conditions, choices, default=risk_pos)
    position = pd.Series(position, index=df.index).shift(1).fillna(risk_pos)

    nav = (1 + position * df['VOO_ret']).cumprod()
    metrics = calc_metrics(nav, position)

    return {
        'ma_len': ma_len,
        'us10y_th': us10y_th,
        'vix_th': vix_th,
        'rsi_th': rsi_th,
        'risk_pos': risk_pos,
        'nav': nav,
        'position': position,
        'raw_dip_buy': raw_dip_buy,
        'dip_buy': dip_buy,
        'risk_off': risk_off,
        **metrics,
    }


def calc_period_metrics(df: pd.DataFrame, result: dict, start: Optional[str], end: Optional[str]) -> dict:
    period_ret = result['position'] * df['VOO_close'].pct_change().fillna(0)
    if start:
        period_ret = period_ret.loc[period_ret.index >= pd.Timestamp(start)]
    if end:
        period_ret = period_ret.loc[period_ret.index <= pd.Timestamp(end)]

    if period_ret.empty:
        return calc_metrics(pd.Series(dtype=float), pd.Series(dtype=float))

    period_ret = period_ret.copy()
    period_ret.iloc[0] = 0.0
    nav = (1 + period_ret).cumprod()
    position = result['position'].reindex(nav.index)
    return calc_metrics(nav, position)


def print_top_results(results: list[dict], top_n: int = 10, title: Optional[str] = None) -> None:
    if title is None:
        title = f"🏆 Top {top_n} 最优参数组合 (按 Calmar、年化、平均仓位排序):"
    print(title)
    print("-" * 150)
    print(
        f"{'排名':<4} {'MA':<4} {'US10Y↑':<7} {'VIX>':<5} {'RSI<':<5} "
        f"{'风险仓位':<8} {'年化':<8} {'回撤':<9} {'Calmar':<8} {'平均仓位':<8} "
        f"{'回撤起点':<12} {'回撤终点':<12}"
    )
    print("-" * 150)

    for i, r in enumerate(results[:top_n], 1):
        dd_peak = r['dd_peak'].strftime('%Y-%m-%d') if pd.notna(r['dd_peak']) else 'N/A'
        dd_trough = r['dd_trough'].strftime('%Y-%m-%d') if pd.notna(r['dd_trough']) else 'N/A'
        print(
            f"{i:<4} {r['ma_len']:<4} {r['us10y_th']:<7.2f} {r['vix_th']:<5} {r['rsi_th']:<5} "
            f"{r['risk_pos']:<8.1f} {r['cagr'] * 100:>6.2f}% {r['max_dd'] * 100:>8.2f}% "
            f"{r['calmar']:>7.2f} {r['avg_exposure']:>7.1f}% "
            f"{dd_peak:<12} {dd_trough:<12}"
        )


def print_regime_checks(df: pd.DataFrame, best: dict) -> None:
    periods = [
        ('全样本', None, None),
        ('训练段 2014-2021', '2014-01-01', '2021-12-31'),
        ('样本外 2022-至今', '2022-01-01', None),
        ('COVID 2020', '2020-01-01', '2020-12-31'),
        ('加息熊市 2022', '2022-01-01', '2022-12-31'),
        ('近三年 2023-至今', '2023-01-01', None),
    ]

    print("\n🧪 最优参数分阶段稳定性检查:")
    print("-" * 95)
    print(f"{'阶段':<18} {'年化':<9} {'回撤':<9} {'Calmar':<8} {'平均仓位':<8} {'回撤区间':<25}")
    print("-" * 95)

    for label, start, end in periods:
        m = calc_period_metrics(df, best, start, end)
        dd_peak = m['dd_peak'].strftime('%Y-%m-%d') if pd.notna(m['dd_peak']) else 'N/A'
        dd_trough = m['dd_trough'].strftime('%Y-%m-%d') if pd.notna(m['dd_trough']) else 'N/A'
        dd_range = f"{dd_peak} -> {dd_trough}"
        print(
            f"{label:<18} {m['cagr'] * 100:>7.2f}% {m['max_dd'] * 100:>8.2f}% "
            f"{m['calmar']:>7.2f} {m['avg_exposure']:>7.1f}% {dd_range:<25}"
        )


def print_oos_top10(df: pd.DataFrame, results: list[dict], top_n: int = 10) -> None:
    print(f"\n🔎 全样本 Top {top_n} 在样本外 2022-至今的表现:")
    print("-" * 120)
    print(f"{'排名':<4} {'MA':<4} {'US10Y↑':<7} {'VIX>':<5} {'RSI<':<5} {'风险仓位':<8} {'年化':<9} {'回撤':<9} {'Calmar':<8}")
    print("-" * 120)

    for i, r in enumerate(results[:top_n], 1):
        m = calc_period_metrics(df, r, '2022-01-01', None)
        print(
            f"{i:<4} {r['ma_len']:<4} {r['us10y_th']:<7.2f} {r['vix_th']:<5} {r['rsi_th']:<5} "
            f"{r['risk_pos']:<8.1f} {m['cagr'] * 100:>7.2f}% {m['max_dd'] * 100:>8.2f}% {m['calmar']:>7.2f}"
        )


def result_row(r: dict) -> dict:
    dd_peak = r['dd_peak'].strftime('%Y-%m-%d') if pd.notna(r['dd_peak']) else ''
    dd_trough = r['dd_trough'].strftime('%Y-%m-%d') if pd.notna(r['dd_trough']) else ''
    return {
        'ma_len': r['ma_len'],
        'us10y_th': r['us10y_th'],
        'vix_th': r['vix_th'],
        'rsi_th': r['rsi_th'],
        'risk_pos': r['risk_pos'],
        'cagr': r['cagr'],
        'max_dd': r['max_dd'],
        'calmar': r['calmar'],
        'avg_exposure': r['avg_exposure'],
        'dd_peak': dd_peak,
        'dd_trough': dd_trough,
    }


def sort_for_cagr_constraint(results: list[dict]) -> list[dict]:
    return sorted(
        results,
        key=lambda x: (x['cagr'], x['calmar'], x['max_dd'], -x['avg_exposure']),
        reverse=True,
    )


def filter_by_constraints(results: list[dict], max_dd_limit: float, calmar_floor: float) -> list[dict]:
    return [
        r for r in results
        if r['max_dd'] >= max_dd_limit and r['calmar'] >= calmar_floor
    ]


def print_cagr_constraint_table(results: list[dict], max_dd_limit: float, calmar_floor: float, top_n: int = 10) -> None:
    constrained = sort_for_cagr_constraint(filter_by_constraints(results, max_dd_limit, calmar_floor))

    print(f"\n🏁 CAGR 最大化 | 约束: MaxDD >= {max_dd_limit * 100:.0f}% 且 Calmar >= {calmar_floor:.2f}")
    print("-" * 150)
    print(
        f"{'排名':<4} {'MA':<4} {'US10Y↑':<7} {'VIX>':<5} {'RSI<':<5} "
        f"{'风险仓位':<8} {'年化':<8} {'回撤':<9} {'Calmar':<8} {'平均仓位':<8} "
        f"{'回撤起点':<12} {'回撤终点':<12}"
    )
    print("-" * 150)

    if not constrained:
        print("无满足约束的参数组合。")
        return

    for i, r in enumerate(constrained[:top_n], 1):
        dd_peak = r['dd_peak'].strftime('%Y-%m-%d') if pd.notna(r['dd_peak']) else 'N/A'
        dd_trough = r['dd_trough'].strftime('%Y-%m-%d') if pd.notna(r['dd_trough']) else 'N/A'
        print(
            f"{i:<4} {r['ma_len']:<4} {r['us10y_th']:<7.2f} {r['vix_th']:<5} {r['rsi_th']:<5} "
            f"{r['risk_pos']:<8.1f} {r['cagr'] * 100:>6.2f}% {r['max_dd'] * 100:>8.2f}% "
            f"{r['calmar']:>7.2f} {r['avg_exposure']:>7.1f}% "
            f"{dd_peak:<12} {dd_trough:<12}"
        )


def select_constraint_winners(results: list[dict], calmar_floor: float) -> list[dict]:
    winners = []
    for max_dd_limit in MAX_DD_LIMITS:
        constrained = sort_for_cagr_constraint(filter_by_constraints(results, max_dd_limit, calmar_floor))
        if constrained:
            winner = constrained[0].copy()
            winner['constraint_label'] = f"DD>={max_dd_limit * 100:.0f}%"
            winner['max_dd_limit'] = max_dd_limit
            winners.append(winner)
    return winners


def print_constraint_winner_checks(df: pd.DataFrame, winners: list[dict]) -> None:
    periods = [
        ('全样本', None, None),
        ('训练段 2014-2021', '2014-01-01', '2021-12-31'),
        ('样本外 2022-至今', '2022-01-01', None),
        ('COVID 2020', '2020-01-01', '2020-12-31'),
        ('加息熊市 2022', '2022-01-01', '2022-12-31'),
        ('近三年 2023-至今', '2023-01-01', None),
    ]

    print("\n🧪 三档约束赢家的分阶段稳定性检查:")
    for winner in winners:
        print(
            f"\n[{winner['constraint_label']}] MA{winner['ma_len']} | US10Y>{winner['us10y_th']} | "
            f"VIX>{winner['vix_th']} | RSI<{winner['rsi_th']} | 风险仓位 {winner['risk_pos']}"
        )
        print("-" * 100)
        print(f"{'阶段':<18} {'年化':<9} {'回撤':<9} {'Calmar':<8} {'平均仓位':<8} {'回撤区间':<25}")
        print("-" * 100)

        for label, start, end in periods:
            m = calc_period_metrics(df, winner, start, end)
            dd_peak = m['dd_peak'].strftime('%Y-%m-%d') if pd.notna(m['dd_peak']) else 'N/A'
            dd_trough = m['dd_trough'].strftime('%Y-%m-%d') if pd.notna(m['dd_trough']) else 'N/A'
            dd_range = f"{dd_peak} -> {dd_trough}"
            print(
                f"{label:<18} {m['cagr'] * 100:>7.2f}% {m['max_dd'] * 100:>8.2f}% "
                f"{m['calmar']:>7.2f} {m['avg_exposure']:>7.1f}% {dd_range:<25}"
            )


def plot_constraint_winners(df: pd.DataFrame, winners: list[dict], output_pic_path: Path) -> None:
    if not winners:
        return

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 12), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.05)

    baseline_nav = (1 + df['VOO_close'].pct_change().fillna(0)).cumprod()
    ax1.plot(df.index, baseline_nav, label='Baseline (死拿 VOO)', color='gray', alpha=0.7, linewidth=2)

    colors = ['tab:blue', 'tab:green', 'tab:red']
    for i, r in enumerate(winners):
        label = (
            f"{r['constraint_label']} | CAGR {r['cagr'] * 100:.2f}% | "
            f"DD {r['max_dd'] * 100:.2f}% | Calmar {r['calmar']:.2f}"
        )
        ax1.plot(df.index, r['nav'], label=label, color=colors[i % len(colors)], linewidth=2.5)

    ax1.set_title('VOO v3 CAGR 约束搜索：三档最大回撤约束赢家', fontsize=16, fontweight='bold')
    ax1.set_ylabel('Net Asset Value (log scale)')
    ax1.set_yscale('log')
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.5)

    best = winners[-1]
    ax2.plot(df.index, df['VOO_close'], label='VOO Price', color='black')
    ax2.plot(
        df.index,
        df['VOO_close'].rolling(best['ma_len']).mean(),
        label=f'VOO MA{best["ma_len"]} ({best["constraint_label"]})',
        color='orange',
        linestyle='--',
    )
    ax2.fill_between(
        df.index,
        ax2.get_ylim()[0],
        ax2.get_ylim()[1],
        where=(best['position'] < 0.8),
        color='red',
        alpha=0.12,
        label='Reduced Exposure',
    )
    ax2.set_ylabel('VOO Price')
    ax2.legend(loc='upper left', fontsize=9)
    ax2.grid(True, alpha=0.5)

    plt.savefig(output_pic_path, dpi=200, bbox_inches='tight')
    plt.close()


def plot_top10(df: pd.DataFrame, results: list[dict], output_pic_path: Path) -> None:
    best = results[0]
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 12), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.05)

    baseline_nav = (1 + df['VOO_close'].pct_change().fillna(0)).cumprod()
    ax1.plot(df.index, baseline_nav, label='Baseline (死拿 VOO)', color='gray', alpha=0.7, linewidth=2)

    colors = plt.cm.tab10(np.linspace(0, 1, 10))
    for i, r in enumerate(results[:10]):
        label = f"Top{i + 1} (MA{r['ma_len']} Calmar {r['calmar']:.2f})"
        linewidth = 3.0 if i == 0 else 1.5
        alpha = 1.0 if i == 0 else 0.85
        ax1.plot(df.index, r['nav'], label=label, color=colors[i], linewidth=linewidth, alpha=alpha)

    ax1.set_title('VOO Revised Hybrid 参数回测结果（Top 10 曲线对比）', fontsize=16, fontweight='bold')
    ax1.set_ylabel('Net Asset Value (log scale)')
    ax1.set_yscale('log')
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.5)

    ax2.plot(df.index, df['VOO_close'], label='VOO Price', color='black')
    ax2.plot(
        df.index,
        df['VOO_close'].rolling(best['ma_len']).mean(),
        label=f'VOO MA{best["ma_len"]}',
        color='orange',
        linestyle='--',
    )
    ax2.fill_between(
        df.index,
        ax2.get_ylim()[0],
        ax2.get_ylim()[1],
        where=(best['position'] < 0.6),
        color='red',
        alpha=0.15,
        label=f'Risk-Off (保留 {best["risk_pos"]} 仓位)',
    )

    ax2.set_ylabel('VOO Price')
    ax2.legend()
    ax2.grid(True, alpha=0.5)

    plt.savefig(output_pic_path, dpi=200, bbox_inches='tight')
    plt.close()


def main() -> None:
    print("=== 🚀 VOO v3 CAGR 最大化约束搜索 ===")
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index().ffill()

    ma_lens = [80, 100, 120, 150, 180, 200]
    us10y_ths = [0.08, 0.10, 0.12, 0.15]
    vix_ths = [35, 38, 40, 45]
    rsi_ths = [30, 32, 35]
    risk_poss = [0.3, 0.4, 0.5, 0.6, 0.7]

    total_combinations = len(ma_lens) * len(us10y_ths) * len(vix_ths) * len(rsi_ths) * len(risk_poss)
    results = []
    print(f"开始搜索... 共 {total_combinations} 种组合\n")

    for ma, us_th, vix_th, rsi_th, risk_pos in product(ma_lens, us10y_ths, vix_ths, rsi_ths, risk_poss):
        res = run_strategy(df, ma, us_th, vix_th, rsi_th, risk_pos)
        results.append(res)

    calmar_sorted = sorted(
        results,
        key=lambda x: (x['calmar'], x['cagr'], -x['avg_exposure']),
        reverse=True,
    )
    cagr_sorted = sort_for_cagr_constraint(results)

    print("📌 参考：原 v3 口径 Calmar 第一名")
    print_top_results(calmar_sorted, top_n=5)

    print("\n📌 参考：不加回撤约束，仅按 CAGR 排序的前 5 名")
    print_top_results(cagr_sorted, top_n=5, title="🏆 Top 5 参数组合 (仅按 CAGR 优先排序):")

    for max_dd_limit in MAX_DD_LIMITS:
        print_cagr_constraint_table(results, max_dd_limit, CALMAR_FLOOR, top_n=10)

    winners = select_constraint_winners(results, CALMAR_FLOOR)
    print_constraint_winner_checks(df, winners)

    all_results_path = OUTPUT_DIR / "v3_cagr_constraint_all_results.csv"
    winners_path = OUTPUT_DIR / "v3_cagr_constraint_winners.csv"
    pd.DataFrame([result_row(r) for r in sort_for_cagr_constraint(results)]).to_csv(all_results_path, index=False)
    pd.DataFrame([result_row(r) | {'constraint_label': r['constraint_label'], 'max_dd_limit': r['max_dd_limit']} for r in winners]).to_csv(
        winners_path,
        index=False,
    )

    output_pic_path = OUTPUT_DIR / "v3_cagr_constraint_winners.png"
    plot_constraint_winners(df, winners, output_pic_path)

    print(f"\n📄 全部搜索结果已保存: {all_results_path}")
    print(f"📄 三档约束赢家已保存: {winners_path}")
    print(f"🖼️ 三档约束赢家图表已保存: {output_pic_path}")


if __name__ == "__main__":
    main()
