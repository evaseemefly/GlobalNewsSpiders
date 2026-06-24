import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from itertools import product
from pathlib import Path
from typing import Dict, List, Optional

from backtest_qqq_hybrid_v3_gridsearch_revised import (
    calc_metrics,
    load_market_data,
    run_strategy,
)


plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False


OUTPUT_DIR = Path(__file__).resolve().parent


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


def format_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


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
        return {"cagr": np.nan, "max_dd": np.nan, "calmar": np.nan}

    norm_nav = period_nav / period_nav.iloc[0]
    return calc_metrics(norm_nav, period_position)


def calc_ex_covid_max_dd(nav: pd.Series, position: pd.Series) -> float:
    segments = [
        nav.index < EX_COVID_START,
        nav.index > EX_COVID_END,
    ]
    max_dds = []

    for mask in segments:
        seg_nav = nav.loc[mask].dropna()
        seg_position = position.loc[seg_nav.index].dropna()
        if len(seg_nav) < 2:
            continue
        norm_nav = seg_nav / seg_nav.iloc[0]
        max_dds.append(calc_metrics(norm_nav, seg_position)["max_dd"])

    return min(max_dds) if max_dds else np.nan


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
        "full_cagr": full["cagr"],
        "full_max_dd": full["max_dd"],
        "full_calmar": full["calmar"],
        "full_avg_exposure": full["avg_exposure"],
        "full_dd_peak": full["dd_peak"],
        "full_dd_trough": full["dd_trough"],
        "ex_covid_max_dd": ex_covid_max_dd,
        "oos_2022_now_cagr": oos["cagr"],
        "oos_2022_now_max_dd": oos["max_dd"],
        "bear_2022_max_dd": bear_2022["max_dd"],
        "recent_2023_now_cagr": recent["cagr"],
        "recent_2023_now_max_dd": recent["max_dd"],
    }
    row["robust_pass"] = passes_constraints(row)
    row["constraint_gap"] = calc_constraint_gap(row)
    return row


def passes_constraints(row: Dict) -> bool:
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


def sort_rows(rows: List[Dict]) -> List[Dict]:
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


def run_vix_audit(df: pd.DataFrame) -> List[Dict]:
    ma_lens = [80, 100, 120, 150, 180, 200]
    us10y_ths = [0.08, 0.10, 0.12, 0.15]
    vix_ths = [35, 38, 40, 45]
    vix_ma_multipliers = [1.5, 1.6, 1.7, 1.8, 2.0]
    rsi_ths = [30, 32, 35]
    risk_poss = [0.3, 0.4, 0.5]

    total = (
        len(ma_lens)
        * len(us10y_ths)
        * len(vix_ths)
        * len(vix_ma_multipliers)
        * len(rsi_ths)
        * len(risk_poss)
    )
    print(f"\n开始 VIX 稳健性审计... 共 {total} 种组合")

    rows = []
    for ma_len, us10y_th, vix_th, vix_mult, rsi_th, risk_pos in product(
        ma_lens,
        us10y_ths,
        vix_ths,
        vix_ma_multipliers,
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
            vix_ma_multiplier=vix_mult,
        )
        rows.append(evaluate_result(result))

    return sort_rows(rows)


def print_top(rows: List[Dict], top_n: int = 15) -> None:
    print("\n🏆 Top VIX 稳健参数组合:")
    print("-" * 188)
    print(
        f"{'排名':<4} {'通过':<4} {'MA':<4} {'US10Y↑':<7} {'VIX>':<5} {'倍数':<5} {'RSI<':<5} {'风险仓位':<8} "
        f"{'全样本年化':>10} {'全样本DD':>10} {'ExCOVID DD':>11} {'OOS年化':>9} {'OOS DD':>9} "
        f"{'2022 DD':>9} {'近三年年化':>10} {'近三年DD':>10} {'Calmar':>8} {'平均仓位':>9}"
    )
    print("-" * 188)

    for i, row in enumerate(rows[:top_n], 1):
        print(
            f"{i:<4} {str(row['robust_pass']):<4} {row['ma_len']:<4} {row['us10y_th']:<7.2f} "
            f"{row['vix_th']:<5} {row['vix_ma_multiplier']:<5.1f} {row['rsi_th']:<5} {row['risk_pos']:<8.1f} "
            f"{format_pct(row['full_cagr']):>10} {format_pct(row['full_max_dd']):>10} "
            f"{format_pct(row['ex_covid_max_dd']):>11} {format_pct(row['oos_2022_now_cagr']):>9} "
            f"{format_pct(row['oos_2022_now_max_dd']):>9} {format_pct(row['bear_2022_max_dd']):>9} "
            f"{format_pct(row['recent_2023_now_cagr']):>10} {format_pct(row['recent_2023_now_max_dd']):>10} "
            f"{row['full_calmar']:>8.2f} {format_pct(row['full_avg_exposure']):>9}"
        )


def save_results(rows: List[Dict]) -> Path:
    out_rows = []
    for row in rows:
        out = row.copy()
        out["full_dd_peak"] = row["full_dd_peak"].date()
        out["full_dd_trough"] = row["full_dd_trough"].date()
        out_rows.append(out)

    out_path = OUTPUT_DIR / "qqq_three_layer_v3_vix_robustness_audit_results.csv"
    pd.DataFrame(out_rows).to_csv(out_path, index=False)
    return out_path


def print_multiplier_summary(rows: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    summary = (
        df.groupby("vix_ma_multiplier")
        .agg(
            pass_count=("robust_pass", "sum"),
            best_full_cagr=("full_cagr", "max"),
            median_full_cagr=("full_cagr", "median"),
            best_calmar=("full_calmar", "max"),
            best_oos_cagr=("oos_2022_now_cagr", "max"),
            best_recent_cagr=("recent_2023_now_cagr", "max"),
        )
        .reset_index()
    )
    summary_path = OUTPUT_DIR / "qqq_three_layer_v3_vix_multiplier_summary.csv"
    summary.to_csv(summary_path, index=False)

    print("\n📌 vix_ma_multiplier 分组摘要:")
    print(summary.to_string(index=False, formatters={
        "best_full_cagr": lambda x: f"{x * 100:.2f}%",
        "median_full_cagr": lambda x: f"{x * 100:.2f}%",
        "best_calmar": lambda x: f"{x:.2f}",
        "best_oos_cagr": lambda x: f"{x * 100:.2f}%",
        "best_recent_cagr": lambda x: f"{x * 100:.2f}%",
    }))
    print(f"📄 倍数摘要已保存: {summary_path}")
    return summary


def print_final_param_pivot(rows: List[Dict]) -> None:
    df = pd.DataFrame(rows)
    focus = df[
        (df["ma_len"] == 200)
        & (df["us10y_th"] == 0.15)
        & (df["rsi_th"] == 30)
        & (df["risk_pos"] == 0.3)
    ].copy()

    pivot = focus.pivot_table(
        index="vix_ma_multiplier",
        columns="vix_th",
        values="full_cagr",
        aggfunc="max",
    )
    pivot_path = OUTPUT_DIR / "qqq_three_layer_v3_vix_final_param_pivot.csv"
    pivot.to_csv(pivot_path)

    print("\n🔎 固定 MA200 / US10Y>0.15 / RSI<30 / risk_pos=0.3 时的 CAGR 透视:")
    print((pivot * 100).round(2).to_string())
    print(f"📄 固定参数 VIX 透视已保存: {pivot_path}")


def plot_heatmap(rows: List[Dict]) -> Path:
    df = pd.DataFrame(rows)
    focus = df[
        (df["ma_len"] == 200)
        & (df["us10y_th"] == 0.15)
        & (df["rsi_th"] == 30)
        & (df["risk_pos"] == 0.3)
    ].copy()
    pivot = focus.pivot_table(
        index="vix_ma_multiplier",
        columns="vix_th",
        values="full_cagr",
        aggfunc="max",
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    im = ax.imshow(pivot.values * 100, cmap="YlGnBu", aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([f"{v:.1f}" for v in pivot.index])
    ax.set_xlabel("VIX absolute threshold")
    ax.set_ylabel("VIX MA multiplier")
    ax.set_title("QQQ v3 VIX 参数稳健性: 固定核心参数 CAGR(%)", fontweight="bold")

    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            ax.text(j, i, f"{pivot.values[i, j] * 100:.2f}", ha="center", va="center", color="black", fontsize=9)

    fig.colorbar(im, ax=ax, label="CAGR (%)")
    out_path = OUTPUT_DIR / "qqq_three_layer_v3_vix_param_heatmap.png"
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main() -> None:
    print("=== 🚀 QQQ Three-Layer v3 VIX 稳健性审计 ===")
    df = load_market_data()
    rows = run_vix_audit(df)
    print_top(rows)

    pass_count = sum(1 for row in rows if row["robust_pass"])
    print(f"\n✅ 通过全部稳健约束的组合数: {pass_count}/{len(rows)}")

    best = rows[0]
    print(
        "\n🎯 VIX Audit 最优参数: "
        f"MA{best['ma_len']} | US10Y>{best['us10y_th']} | "
        f"VIX>{best['vix_th']} 或 VIX>{best['vix_ma_multiplier']}×VIX_MA60 | "
        f"RSI<{best['rsi_th']} | risk_pos={best['risk_pos']}"
    )
    print(
        f"全样本 CAGR {format_pct(best['full_cagr'])} | "
        f"MaxDD {format_pct(best['full_max_dd'])} | "
        f"Calmar {best['full_calmar']:.2f} | "
        f"OOS CAGR {format_pct(best['oos_2022_now_cagr'])}"
    )

    results_path = save_results(rows)
    summary = print_multiplier_summary(rows)
    print_final_param_pivot(rows)
    heatmap_path = plot_heatmap(rows)

    print("\n📄 输出文件:")
    print(f"  Results : {results_path}")
    print(f"  Heatmap : {heatmap_path}")


if __name__ == "__main__":
    main()
