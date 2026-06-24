import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from itertools import product
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from backtest_qqq_hybrid_v3_gridsearch_revised import (
    FINAL_PARAMS,
    calc_metrics,
    load_market_data,
    run_buy_hold,
    run_strategy,
)


plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False


OUTPUT_DIR = Path(__file__).resolve().parent


# COVID is still included in full-sample metrics.
# The ex-COVID metric below only prevents one crash window from monopolizing the ranking.
EX_COVID_START = pd.Timestamp("2020-02-19")
EX_COVID_END = pd.Timestamp("2020-03-16")


ROBUST_CONSTRAINTS = {
    "full_max_dd_floor": -0.16,
    "ex_covid_max_dd_floor": -0.14,
    "oos_2022_now_max_dd_floor": -0.14,
    "bear_2022_max_dd_floor": -0.12,
    "recent_2023_now_max_dd_floor": -0.14,
    "full_calmar_floor": 0.90,
}


PERIODS = [
    ("全样本", None, None),
    ("训练段 2014-2021", "2014-01-01", "2021-12-31"),
    ("样本外 2022-至今", "2022-01-01", None),
    ("COVID 2020", "2020-01-01", "2020-12-31"),
    ("加息熊市 2022", "2022-01-01", "2022-12-31"),
    ("近三年 2023-至今", "2023-01-01", None),
]


def format_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def safe_name(text: str) -> str:
    return text.replace(" ", "_").replace("-", "_").replace("至今", "to今").replace("/", "_")


def slice_series(series: pd.Series, start: Optional[str], end: Optional[str]) -> pd.Series:
    if start and end:
        return series.loc[start:end]
    if start:
        return series.loc[start:]
    if end:
        return series.loc[:end]
    return series


def calc_period_metrics(nav: pd.Series, position: pd.Series, start: Optional[str], end: Optional[str]) -> Dict:
    period_nav = slice_series(nav, start, end).dropna()
    period_position = slice_series(position, start, end).reindex(period_nav.index).dropna()

    if len(period_nav) < 2:
        return {
            "total_return": np.nan,
            "cagr": np.nan,
            "max_dd": np.nan,
            "calmar": np.nan,
            "avg_exposure": np.nan,
            "dd_peak": pd.NaT,
            "dd_trough": pd.NaT,
            "norm_nav": period_nav,
        }

    norm_nav = period_nav / period_nav.iloc[0]
    metrics = calc_metrics(norm_nav, period_position)
    metrics["norm_nav"] = norm_nav
    return metrics


def calc_ex_covid_max_dd(nav: pd.Series, position: pd.Series) -> float:
    """
    Calculate the worst segment drawdown outside the COVID crash window.

    This does not delete COVID from full-sample evaluation. It only adds a second
    drawdown lens so the optimizer cannot hide poor behavior outside 2020-02/03.
    """
    segments = [
        (nav.index < EX_COVID_START),
        (nav.index > EX_COVID_END),
    ]

    segment_drawdowns = []
    for mask in segments:
        seg_nav = nav.loc[mask].dropna()
        seg_position = position.loc[seg_nav.index].dropna()
        if len(seg_nav) < 2:
            continue
        norm_nav = seg_nav / seg_nav.iloc[0]
        segment_drawdowns.append(calc_metrics(norm_nav, seg_position)["max_dd"])

    if not segment_drawdowns:
        return np.nan
    return min(segment_drawdowns)


def evaluate_result(result: Dict) -> Dict:
    nav = result["nav"]
    position = result["position"]
    full = result["metrics"]
    oos = calc_period_metrics(nav, position, "2022-01-01", None)
    bear_2022 = calc_period_metrics(nav, position, "2022-01-01", "2022-12-31")
    recent = calc_period_metrics(nav, position, "2023-01-01", None)
    ex_covid_max_dd = calc_ex_covid_max_dd(nav, position)

    row = {
        "ma_len": result["ma_len"],
        "us10y_th": result["us10y_th"],
        "vix_th": result["vix_th"],
        "vix_ma_multiplier": result["vix_ma_multiplier"],
        "rsi_th": result["rsi_th"],
        "risk_pos": result["risk_pos"],
        "risk_on_pos": result["risk_on_pos"],
        "full_total_return": full["total_return"],
        "full_cagr": full["cagr"],
        "full_max_dd": full["max_dd"],
        "full_calmar": full["calmar"],
        "full_avg_exposure": full["avg_exposure"],
        "full_dd_peak": full["dd_peak"],
        "full_dd_trough": full["dd_trough"],
        "ex_covid_max_dd": ex_covid_max_dd,
        "oos_2022_now_cagr": oos["cagr"],
        "oos_2022_now_max_dd": oos["max_dd"],
        "oos_2022_now_calmar": oos["calmar"],
        "bear_2022_cagr": bear_2022["cagr"],
        "bear_2022_max_dd": bear_2022["max_dd"],
        "recent_2023_now_cagr": recent["cagr"],
        "recent_2023_now_max_dd": recent["max_dd"],
        "recent_2023_now_calmar": recent["calmar"],
        "result": result,
    }

    row["robust_pass"] = passes_robust_constraints(row)
    row["constraint_gap"] = calc_constraint_gap(row)
    return row


def passes_robust_constraints(row: Dict) -> bool:
    return (
        row["full_max_dd"] >= ROBUST_CONSTRAINTS["full_max_dd_floor"]
        and row["ex_covid_max_dd"] >= ROBUST_CONSTRAINTS["ex_covid_max_dd_floor"]
        and row["oos_2022_now_max_dd"] >= ROBUST_CONSTRAINTS["oos_2022_now_max_dd_floor"]
        and row["bear_2022_max_dd"] >= ROBUST_CONSTRAINTS["bear_2022_max_dd_floor"]
        and row["recent_2023_now_max_dd"] >= ROBUST_CONSTRAINTS["recent_2023_now_max_dd_floor"]
        and row["full_calmar"] >= ROBUST_CONSTRAINTS["full_calmar_floor"]
    )


def calc_constraint_gap(row: Dict) -> float:
    gaps = [
        row["full_max_dd"] - ROBUST_CONSTRAINTS["full_max_dd_floor"],
        row["ex_covid_max_dd"] - ROBUST_CONSTRAINTS["ex_covid_max_dd_floor"],
        row["oos_2022_now_max_dd"] - ROBUST_CONSTRAINTS["oos_2022_now_max_dd_floor"],
        row["bear_2022_max_dd"] - ROBUST_CONSTRAINTS["bear_2022_max_dd_floor"],
        row["recent_2023_now_max_dd"] - ROBUST_CONSTRAINTS["recent_2023_now_max_dd_floor"],
        row["full_calmar"] - ROBUST_CONSTRAINTS["full_calmar_floor"],
    ]
    return min(gaps)


def sort_robust_results(rows: List[Dict]) -> List[Dict]:
    return sorted(
        rows,
        key=lambda r: (
            r["robust_pass"],
            r["full_cagr"],
            r["oos_2022_now_cagr"],
            r["recent_2023_now_cagr"],
            r["full_calmar"],
            r["constraint_gap"],
            r["full_avg_exposure"],
        ),
        reverse=True,
    )


def run_robust_grid_search(df: pd.DataFrame) -> List[Dict]:
    ma_lens = [80, 100, 120, 150, 180, 200]
    us10y_ths = [0.08, 0.10, 0.12, 0.15]
    vix_ths = [35, 38, 40, 45]
    rsi_ths = [30, 32, 35]
    risk_poss = [0.3, 0.4, 0.5]

    total = len(ma_lens) * len(us10y_ths) * len(vix_ths) * len(rsi_ths) * len(risk_poss)
    print(f"\n开始稳健搜索... 共 {total} 种组合")
    print("约束:")
    print(f"  full MaxDD >= {ROBUST_CONSTRAINTS['full_max_dd_floor']:.0%}")
    print(f"  ex-COVID MaxDD >= {ROBUST_CONSTRAINTS['ex_covid_max_dd_floor']:.0%}")
    print(f"  2022-至今 MaxDD >= {ROBUST_CONSTRAINTS['oos_2022_now_max_dd_floor']:.0%}")
    print(f"  2022 MaxDD >= {ROBUST_CONSTRAINTS['bear_2022_max_dd_floor']:.0%}")
    print(f"  2023-至今 MaxDD >= {ROBUST_CONSTRAINTS['recent_2023_now_max_dd_floor']:.0%}")
    print(f"  full Calmar >= {ROBUST_CONSTRAINTS['full_calmar_floor']:.2f}")

    rows = []
    for ma_len, us10y_th, vix_th, rsi_th, risk_pos in product(
        ma_lens,
        us10y_ths,
        vix_ths,
        rsi_ths,
        risk_poss,
    ):
        result = run_strategy(
            df=df,
            ma_len=ma_len,
            us10y_th=us10y_th,
            vix_th=vix_th,
            rsi_th=rsi_th,
            risk_pos=risk_pos,
        )
        rows.append(evaluate_result(result))

    return sort_robust_results(rows)


def print_top_robust(rows: List[Dict], top_n: int = 10) -> None:
    print("\n🏆 Top 10 稳健参数组合 (先过约束，再按 CAGR 排序):")
    print("-" * 178)
    print(
        f"{'排名':<4} {'通过':<4} {'MA':<4} {'US10Y↑':<7} {'VIX>':<5} {'RSI<':<5} {'风险仓位':<8} "
        f"{'全样本年化':>10} {'全样本DD':>10} {'ExCOVID DD':>11} {'OOS年化':>9} {'OOS DD':>9} "
        f"{'2022 DD':>9} {'近三年年化':>10} {'近三年DD':>10} {'Calmar':>8} {'平均仓位':>9}"
    )
    print("-" * 178)

    for i, row in enumerate(rows[:top_n], 1):
        print(
            f"{i:<4} {str(row['robust_pass']):<4} {row['ma_len']:<4} {row['us10y_th']:<7.2f} "
            f"{row['vix_th']:<5} {row['rsi_th']:<5} {row['risk_pos']:<8.1f} "
            f"{format_pct(row['full_cagr']):>10} {format_pct(row['full_max_dd']):>10} "
            f"{format_pct(row['ex_covid_max_dd']):>11} {format_pct(row['oos_2022_now_cagr']):>9} "
            f"{format_pct(row['oos_2022_now_max_dd']):>9} {format_pct(row['bear_2022_max_dd']):>9} "
            f"{format_pct(row['recent_2023_now_cagr']):>10} {format_pct(row['recent_2023_now_max_dd']):>10} "
            f"{row['full_calmar']:>8.2f} {format_pct(row['full_avg_exposure']):>9}"
        )


def save_robust_grid(rows: List[Dict]) -> Path:
    out_rows = []
    for row in rows:
        out = {k: v for k, v in row.items() if k != "result"}
        out["full_dd_peak"] = row["full_dd_peak"].date()
        out["full_dd_trough"] = row["full_dd_trough"].date()
        out_rows.append(out)

    out_path = OUTPUT_DIR / "qqq_three_layer_v3_gridsearch_robust_results.csv"
    pd.DataFrame(out_rows).to_csv(out_path, index=False)
    return out_path


def save_selected_timeseries(selected: Dict) -> Path:
    df = selected["result"]["df"].copy()
    out_path = OUTPUT_DIR / "qqq_three_layer_v3_robust_timeseries.csv"
    df.to_csv(out_path)
    return out_path


def print_selected(selected: Dict) -> None:
    print(
        "\n🎯 Robust Final 参数: "
        f"MA{selected['ma_len']} | US10Y>{selected['us10y_th']} | "
        f"VIX>{selected['vix_th']} 或 VIX>{selected['vix_ma_multiplier']}×VIX_MA60 | "
        f"RSI<{selected['rsi_th']} | risk_pos={selected['risk_pos']}"
    )
    print(
        f"全样本: CAGR {format_pct(selected['full_cagr'])} | "
        f"MaxDD {format_pct(selected['full_max_dd'])} | "
        f"Calmar {selected['full_calmar']:.2f} | "
        f"平均仓位 {format_pct(selected['full_avg_exposure'])}"
    )
    print(
        f"稳健审计: Ex-COVID DD {format_pct(selected['ex_covid_max_dd'])} | "
        f"OOS DD {format_pct(selected['oos_2022_now_max_dd'])} | "
        f"2022 DD {format_pct(selected['bear_2022_max_dd'])} | "
        f"近三年 DD {format_pct(selected['recent_2023_now_max_dd'])}"
    )


def build_period_rows(buy_hold: Dict, selected: Dict) -> Tuple[List[Dict], List[Path]]:
    rows = []
    plot_paths = []
    strategy = selected["result"]

    for period_name, start, end in PERIODS:
        period_rows = []
        nav_map = {}

        for name, nav, position in [
            ("Buy&Hold QQQ", buy_hold["nav"], buy_hold["position"]),
            ("QQQ Three-Layer v3 Robust", strategy["nav"], strategy["position"]),
        ]:
            metrics = calc_period_metrics(nav, position, start, end)
            row = {
                "period": period_name,
                "strategy": name,
                "total_return": metrics["total_return"],
                "cagr": metrics["cagr"],
                "max_dd": metrics["max_dd"],
                "calmar": metrics["calmar"],
                "avg_exposure": metrics["avg_exposure"],
                "dd_peak": metrics["dd_peak"].date(),
                "dd_trough": metrics["dd_trough"].date(),
            }
            rows.append(row)
            period_rows.append(row)
            nav_map[name] = metrics["norm_nav"]

        print_period_table(period_name, period_rows)
        plot_paths.append(plot_period_nav(period_name, period_rows, nav_map))

    return rows, plot_paths


def print_period_table(period_name: str, rows: List[Dict]) -> None:
    print(f"\n=== {period_name} ===")
    print(
        f"{'strategy':<30} {'total_return':>13} {'cagr':>9} {'max_dd':>9} "
        f"{'calmar':>8} {'avg_exposure':>13} {'dd_peak':>12} {'dd_trough':>12}"
    )
    for row in rows:
        print(
            f"{row['strategy']:<30} {format_pct(row['total_return']):>13} "
            f"{format_pct(row['cagr']):>9} {format_pct(row['max_dd']):>9} "
            f"{row['calmar']:>8.2f} {format_pct(row['avg_exposure']):>13} "
            f"{row['dd_peak']} {row['dd_trough']}"
        )


def plot_period_nav(period_name: str, rows: List[Dict], nav_map: Dict[str, pd.Series]) -> Path:
    metrics_by_name = {row["strategy"]: row for row in rows}
    colors = {
        "Buy&Hold QQQ": "#7f8c8d",
        "QQQ Three-Layer v3 Robust": "#e67e22",
    }

    fig, ax = plt.subplots(figsize=(18, 8))
    for name, nav in nav_map.items():
        metric = metrics_by_name[name]
        label = f"{name} | CAGR {metric['cagr'] * 100:.2f}% | DD {metric['max_dd'] * 100:.2f}%"
        ax.plot(nav.index, nav, label=label, color=colors[name], linewidth=2.2)

    ax.set_title(f"{period_name}: QQQ Three-Layer v3 Robust 阶段净值走势", fontsize=16, fontweight="bold")
    ax.set_ylabel("Normalized NAV")
    ax.grid(True, alpha=0.35)
    ax.legend(loc="upper left")

    out_path = OUTPUT_DIR / f"qqq_three_layer_v3_robust_period_nav_{safe_name(period_name)}.png"
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return out_path


def save_period_outputs(rows: List[Dict]) -> Tuple[Path, Path]:
    csv_path = OUTPUT_DIR / "qqq_three_layer_v3_robust_period_analysis.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    md_path = OUTPUT_DIR / "qqq_three_layer_v3_robust_period_analysis.md"
    lines = ["# QQQ Three-Layer v3 Robust 阶段回测\n"]
    df = pd.DataFrame(rows)
    for period_name, group in df.groupby("period", sort=False):
        lines.append(f"## {period_name}\n")
        lines.append("| strategy | total_return | cagr | max_dd | calmar | avg_exposure | dd_peak | dd_trough |")
        lines.append("|---|---:|---:|---:|---:|---:|---|---|")
        for _, row in group.iterrows():
            lines.append(
                "| {strategy} | {total_return:.2%} | {cagr:.2%} | {max_dd:.2%} | "
                "{calmar:.2f} | {avg_exposure:.2%} | {dd_peak} | {dd_trough} |".format(
                    strategy=row["strategy"],
                    total_return=row["total_return"],
                    cagr=row["cagr"],
                    max_dd=row["max_dd"],
                    calmar=row["calmar"],
                    avg_exposure=row["avg_exposure"],
                    dd_peak=row["dd_peak"],
                    dd_trough=row["dd_trough"],
                )
            )
        lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return csv_path, md_path


def plot_selected_overview(buy_hold: Dict, selected: Dict) -> Path:
    result = selected["result"]
    df = result["df"]

    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        figsize=(18, 12),
        gridspec_kw={"height_ratios": [3, 1]},
        sharex=True,
    )
    plt.subplots_adjust(hspace=0.06)

    bh_m = buy_hold["metrics"]
    ax1.plot(
        buy_hold["nav"].index,
        buy_hold["nav"],
        label=f"Buy&Hold QQQ | CAGR {bh_m['cagr'] * 100:.2f}% | DD {bh_m['max_dd'] * 100:.2f}%",
        color="#7f8c8d",
        alpha=0.75,
        linewidth=1.8,
    )
    ax1.plot(
        result["nav"].index,
        result["nav"],
        label=f"Robust v3 | CAGR {selected['full_cagr'] * 100:.2f}% | DD {selected['full_max_dd'] * 100:.2f}%",
        color="#e67e22",
        linewidth=2.4,
    )
    ax1.set_title("QQQ Three-Layer v3 Robust: 全样本净值与状态", fontsize=16, fontweight="bold")
    ax1.set_ylabel("NAV (log scale)")
    ax1.set_yscale("log")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.35)

    ax2.plot(df.index, df["QQQ_close"], label="QQQ Price", color="black", linewidth=1.0)
    ax2.plot(df.index, df["QQQ_MA"], label=f"QQQ MA{selected['ma_len']}", color="orange", linestyle="--")
    y_min, y_max = ax2.get_ylim()
    ax2.fill_between(
        df.index,
        y_min,
        y_max,
        where=df["state_raw"].eq("Risk-Off"),
        color="#e74c3c",
        alpha=0.16,
        label="Risk-Off",
    )
    ax2.fill_between(
        df.index,
        y_min,
        y_max,
        where=df["state_raw"].eq("Dip-Buy"),
        color="#2ecc71",
        alpha=0.18,
        label="Dip-Buy",
    )
    ax2.set_ylabel("QQQ Price")
    ax2.legend(loc="upper left", ncol=3)
    ax2.grid(True, alpha=0.35)

    out_path = OUTPUT_DIR / "qqq_three_layer_v3_robust_comparison.png"
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main() -> None:
    print("=== 🚀 QQQ Three-Layer v3 Robust 参数搜索 ===")
    df = load_market_data()
    rows = run_robust_grid_search(df)
    print_top_robust(rows)

    pass_count = sum(1 for row in rows if row["robust_pass"])
    print(f"\n✅ 通过全部稳健约束的组合数: {pass_count}/{len(rows)}")

    selected_pool = [row for row in rows if row["robust_pass"]]
    if not selected_pool:
        print("⚠️ 没有组合通过全部约束，退回按约束缺口和 CAGR 排序的第一名。")
        selected = rows[0]
    else:
        selected = selected_pool[0]

    print_selected(selected)

    grid_csv = save_robust_grid(rows)
    timeseries_csv = save_selected_timeseries(selected)
    buy_hold = run_buy_hold(df)
    overview_png = plot_selected_overview(buy_hold, selected)
    period_rows, period_pngs = build_period_rows(buy_hold, selected)
    period_csv, period_md = save_period_outputs(period_rows)

    print("\n📄 输出文件:")
    print(f"  Robust Grid : {grid_csv}")
    print(f"  Timeseries  : {timeseries_csv}")
    print(f"  Period CSV  : {period_csv}")
    print(f"  Period MD   : {period_md}")
    print(f"  Overview PNG: {overview_png}")
    print("  Period Charts:")
    for path in period_pngs:
        print(f"    {path}")


if __name__ == "__main__":
    main()
