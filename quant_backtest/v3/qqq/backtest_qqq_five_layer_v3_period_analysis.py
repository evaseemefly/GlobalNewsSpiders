import pandas as pd
import matplotlib.pyplot as plt

from pathlib import Path
from typing import Dict, List, Optional, Tuple

from backtest_qqq_five_layer_v3 import (
    DAILY_QQQ_FIVE_LAYER_PARAMS,
    THREE_LAYER_BEST_PARAMS,
    load_and_filter_data,
    run_buy_hold,
    run_three_layer_strategy,
    run_five_layer_strategy,
)


plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False


OUTPUT_DIR = Path(__file__).resolve().parent


PERIODS: List[Tuple[str, Optional[str], Optional[str]]] = [
    ("全样本", None, None),
    ("训练段 2014-2021", "2014-01-01", "2021-12-31"),
    ("样本外 2022-至今", "2022-01-01", None),
    ("COVID 2020", "2020-01-01", "2020-12-31"),
    ("加息熊市 2022", "2022-01-01", "2022-12-31"),
    ("近三年 2023-至今", "2023-01-01", None),
]


def safe_name(text: str) -> str:
    return (
        text.replace(" ", "_")
        .replace("-", "_")
        .replace("至今", "to今")
        .replace("/", "_")
    )


def calc_period_metrics(nav: pd.Series, position: pd.Series) -> Dict:
    nav = nav.dropna()
    position = position.reindex(nav.index).dropna()

    if len(nav) < 2:
        raise ValueError("阶段数据不足，无法计算绩效。")

    norm_nav = nav / nav.iloc[0]
    total_ret = norm_nav.iloc[-1] - 1
    years = (norm_nav.index[-1] - norm_nav.index[0]).days / 365.25
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0.0

    roll_max = norm_nav.cummax()
    drawdown = norm_nav / roll_max - 1.0
    max_dd = drawdown.min()
    dd_trough = drawdown.idxmin()
    dd_peak = norm_nav.loc[:dd_trough].idxmax()
    calmar = cagr / abs(max_dd) if max_dd != 0 else float("nan")

    return {
        "total_return": total_ret,
        "cagr": cagr,
        "max_dd": max_dd,
        "calmar": calmar,
        "avg_exposure": position.mean(),
        "dd_peak": dd_peak,
        "dd_trough": dd_trough,
        "norm_nav": norm_nav,
    }


def slice_series(series: pd.Series, start: Optional[str], end: Optional[str]) -> pd.Series:
    if start and end:
        return series.loc[start:end]
    if start:
        return series.loc[start:]
    if end:
        return series.loc[:end]
    return series


def build_strategy_results(raw_df: pd.DataFrame) -> List[Dict]:
    daily_five_params = DAILY_QQQ_FIVE_LAYER_PARAMS.copy()
    daily_five_params["strategy_name"] = "Five-Layer 日常固定"

    grid_best_params = DAILY_QQQ_FIVE_LAYER_PARAMS.copy()
    grid_best_params.update(
        {
            "strategy_name": "Five-Layer 核心最优",
            "ma_len": 120,
            "us10y_th": 0.08,
            "vix_extreme_th": 45,
            "rsi_th": 30,
        }
    )

    results = [
        run_buy_hold(raw_df),
        run_three_layer_strategy(raw_df, THREE_LAYER_BEST_PARAMS),
        run_five_layer_strategy(raw_df, daily_five_params),
        run_five_layer_strategy(raw_df, grid_best_params),
    ]

    results[0]["name"] = "Buy&Hold QQQ"
    results[1]["name"] = "Three-Layer 最优"
    results[2]["name"] = "Five-Layer 日常固定"
    results[3]["name"] = "Five-Layer 核心最优"

    return results


def analyze_period(
    label: str,
    start: Optional[str],
    end: Optional[str],
    strategy_results: List[Dict],
) -> Tuple[List[Dict], Path]:
    rows = []
    nav_by_strategy = {}

    for result in strategy_results:
        nav = slice_series(result["nav"], start, end)
        position = slice_series(result["position"], start, end)
        metrics = calc_period_metrics(nav, position)

        rows.append(
            {
                "period": label,
                "strategy": result["name"],
                "total_return": metrics["total_return"],
                "cagr": metrics["cagr"],
                "max_dd": metrics["max_dd"],
                "calmar": metrics["calmar"],
                "avg_exposure": metrics["avg_exposure"],
                "dd_peak": metrics["dd_peak"].date(),
                "dd_trough": metrics["dd_trough"].date(),
            }
        )
        nav_by_strategy[result["name"]] = metrics["norm_nav"]

    plot_path = plot_period_nav(label, nav_by_strategy, rows)
    return rows, plot_path


def plot_period_nav(label: str, nav_by_strategy: Dict[str, pd.Series], rows: List[Dict]) -> Path:
    metrics_by_name = {row["strategy"]: row for row in rows}
    colors = {
        "Buy&Hold QQQ": "#7f8c8d",
        "Three-Layer 最优": "#e67e22",
        "Five-Layer 日常固定": "#2ca02c",
        "Five-Layer 核心最优": "#1f77b4",
    }

    fig, ax = plt.subplots(figsize=(18, 8))

    for name, nav in nav_by_strategy.items():
        metric = metrics_by_name[name]
        label_text = (
            f"{name} | CAGR {metric['cagr'] * 100:.2f}% | "
            f"DD {metric['max_dd'] * 100:.2f}%"
        )
        ax.plot(
            nav.index,
            nav,
            label=label_text,
            linewidth=2.2 if "Five-Layer" in name else 1.8,
            color=colors.get(name),
            alpha=0.95,
        )

    ax.set_title(f"{label}: QQQ 阶段净值走势对比", fontsize=16, fontweight="bold")
    ax.set_ylabel("Normalized NAV")
    ax.grid(True, alpha=0.35)
    ax.legend(loc="upper left")

    out_path = OUTPUT_DIR / f"qqq_five_layer_period_nav_{safe_name(label)}.png"
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return out_path


def format_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def print_period_table(label: str, rows: List[Dict]) -> None:
    print(f"\n=== {label} ===")
    print(
        f"{'strategy':<22} {'total_return':>13} {'cagr':>9} {'max_dd':>9} "
        f"{'calmar':>8} {'avg_exposure':>13} {'dd_peak':>12} {'dd_trough':>12}"
    )
    for row in rows:
        print(
            f"{row['strategy']:<22} {format_pct(row['total_return']):>13} "
            f"{format_pct(row['cagr']):>9} {format_pct(row['max_dd']):>9} "
            f"{row['calmar']:>8.2f} {format_pct(row['avg_exposure']):>13} "
            f"{row['dd_peak']} {row['dd_trough']}"
        )


def save_markdown(all_rows: List[Dict]) -> Path:
    out_path = OUTPUT_DIR / "qqq_five_layer_v3_period_analysis.md"
    df = pd.DataFrame(all_rows)

    lines = ["# QQQ Five-Layer V3 阶段回测\n"]
    for period, group in df.groupby("period", sort=False):
        lines.append(f"## {period}\n")
        lines.append("| strategy | total_return | cagr | max_dd | calmar | avg_exposure | dd_peak | dd_trough |")
        lines.append("|---|---:|---:|---:|---:|---:|---|---|")
        for _, row in group.iterrows():
            lines.append(
                "| {strategy} | {total_return} | {cagr} | {max_dd} | {calmar:.2f} | "
                "{avg_exposure} | {dd_peak} | {dd_trough} |".format(
                    strategy=row["strategy"],
                    total_return=format_pct(row["total_return"]),
                    cagr=format_pct(row["cagr"]),
                    max_dd=format_pct(row["max_dd"]),
                    calmar=row["calmar"],
                    avg_exposure=format_pct(row["avg_exposure"]),
                    dd_peak=row["dd_peak"],
                    dd_trough=row["dd_trough"],
                )
            )
        lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path


def main() -> None:
    print("=== 🚀 QQQ Five-Layer V3 分阶段回测 ===")
    raw_df = load_and_filter_data()
    strategy_results = build_strategy_results(raw_df)

    all_rows = []
    plot_paths = []

    for label, start, end in PERIODS:
        rows, plot_path = analyze_period(label, start, end, strategy_results)
        all_rows.extend(rows)
        plot_paths.append(plot_path)
        print_period_table(label, rows)

    csv_path = OUTPUT_DIR / "qqq_five_layer_v3_period_analysis.csv"
    pd.DataFrame(all_rows).to_csv(csv_path, index=False)
    md_path = save_markdown(all_rows)

    print(f"\n📄 阶段回测 CSV 已保存: {csv_path}")
    print(f"📄 阶段回测 Markdown 已保存: {md_path}")
    print("🖼️ 阶段净值走势对比图已保存:")
    for path in plot_paths:
        print(f"  {path}")


if __name__ == "__main__":
    main()
