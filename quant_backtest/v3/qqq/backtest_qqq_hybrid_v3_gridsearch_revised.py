import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from itertools import product
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ==================== Plot font support ====================
plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False


# ==================== Paths ====================
CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_max_20140917_to_20260424.csv"
)
OUTPUT_DIR = Path(__file__).resolve().parent


# Final QQQ v3 three-layer parameters from the revised grid-search baseline.
FINAL_PARAMS = {
    "ma_len": 200,
    "us10y_th": 0.15,
    "vix_th": 45,
    "vix_ma_multiplier": 1.8,
    "rsi_th": 30,
    "risk_pos": 0.30,
    "risk_on_pos": 1.00,
}


PERIODS = [
    ("全样本", None, None),
    ("训练段 2014-2021", "2014-01-01", "2021-12-31"),
    ("样本外 2022-至今", "2022-01-01", None),
    ("COVID 2020", "2020-01-01", "2020-12-31"),
    ("加息熊市 2022", "2022-01-01", "2022-12-31"),
    ("近三年 2023-至今", "2023-01-01", None),
]


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def load_market_data() -> pd.DataFrame:
    if not CSV_FILE_PATH.exists():
        raise FileNotFoundError(f"找不到历史数据文件: {CSV_FILE_PATH}")

    df = pd.read_csv(CSV_FILE_PATH)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()

    required_cols = ["QQQ_close", "QQQ_open", "HYG_close", "US10Y_close", "VIX_close"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"缺少必要字段: {missing_cols}")

    raw_rows = len(df)
    weekend_rows = int((df.index.dayofweek >= 5).sum())

    # Only keep real QQQ trading days before filling macro data.
    df = df[df["QQQ_close"].notna()]
    df = df[df.index.dayofweek < 5]
    if "QQQ_volume" in df.columns:
        df = df[df["QQQ_volume"] > 0]

    df = df.ffill()

    print(f"📂 数据路径: {CSV_FILE_PATH}")
    print(f"🧹 数据清洗: 原始 {raw_rows} 行，过滤周末 {weekend_rows} 行，保留交易日 {len(df)} 行")

    return df


def prepare_features(df: pd.DataFrame, ma_len: int) -> pd.DataFrame:
    df = df.copy()

    df["QQQ_MA"] = df["QQQ_close"].rolling(ma_len, min_periods=1).mean()
    df["HYG_MA60"] = df["HYG_close"].rolling(60, min_periods=1).mean()
    df["VIX_MA60"] = df["VIX_close"].rolling(60, min_periods=1).mean()
    df["RSI_14"] = calculate_rsi(df["QQQ_close"])
    df["QQQ_ret"] = df["QQQ_close"].pct_change().fillna(0.0)

    return df


def calc_metrics(nav: pd.Series, position: pd.Series) -> Dict:
    nav = nav.dropna()
    position = position.reindex(nav.index).dropna()

    total_ret = nav.iloc[-1] - 1
    years = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0.0

    roll_max = nav.cummax()
    drawdown = nav / roll_max - 1.0
    max_dd = drawdown.min()
    dd_trough = drawdown.idxmin()
    dd_peak = nav.loc[:dd_trough].idxmax()
    calmar = cagr / abs(max_dd) if max_dd != 0 else np.nan

    return {
        "total_return": total_ret,
        "cagr": cagr,
        "max_dd": max_dd,
        "calmar": calmar,
        "avg_exposure": position.mean(),
        "dd_peak": dd_peak,
        "dd_trough": dd_trough,
    }


def run_strategy(
    df: pd.DataFrame,
    ma_len: int,
    us10y_th: float,
    vix_th: float,
    rsi_th: float,
    risk_pos: float,
    risk_on_pos: float = 1.0,
    vix_ma_multiplier: float = 1.8,
) -> Dict:
    """
    QQQ v3 three-layer state machine.

    Priority is intentionally kept aligned with the original v3 grid-search:
    Dip-Buy -> Risk-Off -> Risk-On -> Trend-Weak.
    """
    df = prepare_features(df, ma_len)

    base_trend = df["QQQ_close"] > df["QQQ_MA"]
    us10y_rising = df["US10Y_close"].diff(20) > us10y_th
    hyg_divergence = (
        (df["HYG_close"] < df["HYG_MA60"]).rolling(3).sum() == 3
    ) & base_trend
    vix_abs_risk = df["VIX_close"] > vix_th
    vix_dynamic_risk = df["VIX_close"] > df["VIX_MA60"] * vix_ma_multiplier
    vix_risk = vix_abs_risk | vix_dynamic_risk
    risk_off = us10y_rising | hyg_divergence | vix_risk

    dip_buy = (
        (df["RSI_14"] < rsi_th)
        & (df["QQQ_close"] > df["QQQ_open"])
        & (df["VIX_close"] < df["VIX_close"].shift(1))
    )

    state_raw = np.select(
        [dip_buy, risk_off, base_trend],
        ["Dip-Buy", "Risk-Off", "Risk-On"],
        default="Trend-Weak",
    )
    position_raw = np.select(
        [dip_buy, risk_off, base_trend],
        [risk_on_pos, risk_pos, risk_on_pos],
        default=risk_pos,
    )

    df["base_trend"] = base_trend
    df["us10y_rising"] = us10y_rising
    df["hyg_divergence"] = hyg_divergence
    df["vix_abs_risk"] = vix_abs_risk
    df["vix_dynamic_risk"] = vix_dynamic_risk
    df["vix_risk"] = vix_risk
    df["risk_off"] = risk_off
    df["dip_buy"] = dip_buy
    df["state_raw"] = state_raw
    df["position_raw"] = position_raw

    # signal[t] is only tradable from t+1.
    df["state"] = df["state_raw"].shift(1).fillna("Initial")
    df["position"] = df["position_raw"].shift(1).fillna(risk_pos)
    df["strategy_ret"] = df["position"] * df["QQQ_ret"]
    df["nav"] = (1 + df["strategy_ret"]).cumprod()
    df["buy_hold_nav"] = (1 + df["QQQ_ret"]).cumprod()

    metrics = calc_metrics(df["nav"], df["position"])

    return {
        "ma_len": ma_len,
        "us10y_th": us10y_th,
        "vix_th": vix_th,
        "vix_ma_multiplier": vix_ma_multiplier,
        "rsi_th": rsi_th,
        "risk_pos": risk_pos,
        "risk_on_pos": risk_on_pos,
        "metrics": metrics,
        "df": df,
        "nav": df["nav"],
        "position": df["position"],
    }


def run_buy_hold(df: pd.DataFrame) -> Dict:
    prepared = prepare_features(df, FINAL_PARAMS["ma_len"])
    position = pd.Series(1.0, index=prepared.index)
    nav = (1 + prepared["QQQ_ret"]).cumprod()
    metrics = calc_metrics(nav, position)

    return {
        "name": "Buy&Hold QQQ",
        "nav": nav,
        "position": position,
        "metrics": metrics,
    }


def run_grid_search(df: pd.DataFrame) -> List[Dict]:
    ma_lens = [80, 100, 120, 150, 180, 200]
    us10y_ths = [0.08, 0.10, 0.12, 0.15]
    vix_ths = [35, 38, 40, 45]
    rsi_ths = [30, 32, 35]
    risk_poss = [0.3, 0.4, 0.5]

    total = len(ma_lens) * len(us10y_ths) * len(vix_ths) * len(rsi_ths) * len(risk_poss)
    print(f"\n开始搜索... 共 {total} 种组合")

    results = []
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
        results.append(result)

    return sorted(
        results,
        key=lambda x: (
            x["metrics"]["calmar"],
            x["metrics"]["cagr"],
            x["metrics"]["avg_exposure"],
        ),
        reverse=True,
    )


def save_grid_results(results: List[Dict]) -> Path:
    rows = []
    for result in results:
        m = result["metrics"]
        rows.append(
            {
                "ma_len": result["ma_len"],
                "us10y_th": result["us10y_th"],
                "vix_th": result["vix_th"],
                "vix_ma_multiplier": result["vix_ma_multiplier"],
                "rsi_th": result["rsi_th"],
                "risk_pos": result["risk_pos"],
                "risk_on_pos": result["risk_on_pos"],
                "total_return": m["total_return"],
                "cagr": m["cagr"],
                "max_dd": m["max_dd"],
                "calmar": m["calmar"],
                "avg_exposure": m["avg_exposure"],
                "dd_peak": m["dd_peak"].date(),
                "dd_trough": m["dd_trough"].date(),
            }
        )

    out_path = OUTPUT_DIR / "qqq_three_layer_v3_gridsearch_revised_results.csv"
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return out_path


def save_timeseries(strategy_result: Dict) -> Path:
    df = strategy_result["df"].copy()
    out_cols = [
        "QQQ_open",
        "QQQ_close",
        "QQQ_ret",
        "QQQ_MA",
        "US10Y_close",
        "HYG_close",
        "HYG_MA60",
        "VIX_close",
        "VIX_MA60",
        "RSI_14",
        "base_trend",
        "us10y_rising",
        "hyg_divergence",
        "vix_abs_risk",
        "vix_dynamic_risk",
        "vix_risk",
        "risk_off",
        "dip_buy",
        "state_raw",
        "state",
        "position_raw",
        "position",
        "strategy_ret",
        "nav",
        "buy_hold_nav",
    ]
    out_path = OUTPUT_DIR / "qqq_three_layer_v3_revised_timeseries.csv"
    df[out_cols].to_csv(out_path)
    return out_path


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


def calc_period_metrics(nav: pd.Series, position: pd.Series) -> Dict:
    nav = nav.dropna()
    position = position.reindex(nav.index).dropna()

    if len(nav) < 2:
        raise ValueError("阶段数据不足，无法计算绩效。")

    norm_nav = nav / nav.iloc[0]
    metrics = calc_metrics(norm_nav, position)
    metrics["norm_nav"] = norm_nav
    return metrics


def build_period_analysis(buy_hold: Dict, strategy_result: Dict) -> Tuple[List[Dict], List[Path]]:
    rows = []
    plot_paths = []

    for period_name, start, end in PERIODS:
        period_rows = []
        nav_map = {}

        for name, nav, position in [
            ("Buy&Hold QQQ", buy_hold["nav"], buy_hold["position"]),
            ("QQQ Three-Layer v3 Final", strategy_result["nav"], strategy_result["position"]),
        ]:
            period_nav = slice_series(nav, start, end)
            period_pos = slice_series(position, start, end)
            m = calc_period_metrics(period_nav, period_pos)

            row = {
                "period": period_name,
                "strategy": name,
                "total_return": m["total_return"],
                "cagr": m["cagr"],
                "max_dd": m["max_dd"],
                "calmar": m["calmar"],
                "avg_exposure": m["avg_exposure"],
                "dd_peak": m["dd_peak"].date(),
                "dd_trough": m["dd_trough"].date(),
            }
            rows.append(row)
            period_rows.append(row)
            nav_map[name] = m["norm_nav"]

        print_period_table(period_name, period_rows)
        plot_paths.append(plot_period_nav(period_name, period_rows, nav_map))

    return rows, plot_paths


def plot_period_nav(period_name: str, rows: List[Dict], nav_map: Dict[str, pd.Series]) -> Path:
    metric_by_name = {row["strategy"]: row for row in rows}
    colors = {
        "Buy&Hold QQQ": "#7f8c8d",
        "QQQ Three-Layer v3 Final": "#e67e22",
    }

    fig, ax = plt.subplots(figsize=(18, 8))
    for name, nav in nav_map.items():
        m = metric_by_name[name]
        label = f"{name} | CAGR {m['cagr'] * 100:.2f}% | DD {m['max_dd'] * 100:.2f}%"
        ax.plot(nav.index, nav, label=label, color=colors[name], linewidth=2.2)

    ax.set_title(f"{period_name}: QQQ Three-Layer v3 净值走势对比", fontsize=16, fontweight="bold")
    ax.set_ylabel("Normalized NAV")
    ax.grid(True, alpha=0.35)
    ax.legend(loc="upper left")

    out_path = OUTPUT_DIR / f"qqq_three_layer_v3_period_nav_{safe_name(period_name)}.png"
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return out_path


def plot_full_comparison(buy_hold: Dict, strategy_result: Dict) -> Path:
    df = strategy_result["df"]

    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        figsize=(18, 12),
        gridspec_kw={"height_ratios": [3, 1]},
        sharex=True,
    )
    plt.subplots_adjust(hspace=0.06)

    bh_m = buy_hold["metrics"]
    st_m = strategy_result["metrics"]
    ax1.plot(
        buy_hold["nav"].index,
        buy_hold["nav"],
        label=f"Buy&Hold QQQ | CAGR {bh_m['cagr'] * 100:.2f}% | DD {bh_m['max_dd'] * 100:.2f}%",
        color="#7f8c8d",
        alpha=0.75,
        linewidth=1.8,
    )
    ax1.plot(
        strategy_result["nav"].index,
        strategy_result["nav"],
        label=f"QQQ Three-Layer v3 Final | CAGR {st_m['cagr'] * 100:.2f}% | DD {st_m['max_dd'] * 100:.2f}%",
        color="#e67e22",
        linewidth=2.4,
    )
    ax1.set_title("QQQ Three-Layer v3 Revised: 净值与状态归因", fontsize=16, fontweight="bold")
    ax1.set_ylabel("NAV (log scale)")
    ax1.set_yscale("log")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.35)

    ax2.plot(df.index, df["QQQ_close"], label="QQQ Price", color="black", linewidth=1.0)
    ax2.plot(df.index, df["QQQ_MA"], label=f"QQQ MA{strategy_result['ma_len']}", color="orange", linestyle="--")
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

    out_path = OUTPUT_DIR / "qqq_three_layer_v3_revised_comparison.png"
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return out_path


def save_period_outputs(rows: List[Dict]) -> Tuple[Path, Path]:
    csv_path = OUTPUT_DIR / "qqq_three_layer_v3_period_analysis.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    md_path = OUTPUT_DIR / "qqq_three_layer_v3_period_analysis.md"
    lines = ["# QQQ Three-Layer v3 Revised 阶段回测\n"]
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


def print_top_results(results: List[Dict], top_n: int = 10) -> None:
    print("\n🏆 Top 10 最优参数组合 (按 Calmar、年化、平均仓位排序):")
    print("-" * 132)
    print(
        f"{'排名':<4} {'MA':<4} {'US10Y↑':<7} {'VIX>':<5} {'RSI<':<5} "
        f"{'风险仓位':<8} {'年化':>8} {'回撤':>9} {'Calmar':>8} {'平均仓位':>9} "
        f"{'回撤起点':>12} {'回撤终点':>12}"
    )
    print("-" * 132)

    for i, result in enumerate(results[:top_n], 1):
        m = result["metrics"]
        print(
            f"{i:<4} {result['ma_len']:<4} {result['us10y_th']:<7.2f} {result['vix_th']:<5} "
            f"{result['rsi_th']:<5} {result['risk_pos']:<8.1f} "
            f"{m['cagr'] * 100:>7.2f}% {m['max_dd'] * 100:>8.2f}% "
            f"{m['calmar']:>8.2f} {m['avg_exposure'] * 100:>8.1f}% "
            f"{m['dd_peak'].date()} {m['dd_trough'].date()}"
        )


def print_period_table(period_name: str, rows: List[Dict]) -> None:
    print(f"\n=== {period_name} ===")
    print(
        f"{'strategy':<28} {'total_return':>13} {'cagr':>9} {'max_dd':>9} "
        f"{'calmar':>8} {'avg_exposure':>13} {'dd_peak':>12} {'dd_trough':>12}"
    )
    for row in rows:
        print(
            f"{row['strategy']:<28} {row['total_return'] * 100:>12.2f}% "
            f"{row['cagr'] * 100:>8.2f}% {row['max_dd'] * 100:>8.2f}% "
            f"{row['calmar']:>8.2f} {row['avg_exposure'] * 100:>12.1f}% "
            f"{row['dd_peak']} {row['dd_trough']}"
        )


def print_state_distribution(strategy_result: Dict) -> None:
    state_counts = (
        strategy_result["df"]["state_raw"]
        .value_counts(normalize=True)
        .reindex(["Risk-On", "Risk-Off", "Dip-Buy", "Trend-Weak"], fill_value=0.0)
    )
    print("\n🧭 三层状态机原始信号状态分布:")
    for state, value in state_counts.items():
        print(f"  {state:<12}: {value * 100:>6.2f}%")


def main() -> None:
    print("=== 🚀 QQQ Three-Layer v3 Revised 参数搜索与阶段回测 ===")
    df = load_market_data()

    results = run_grid_search(df)
    print_top_results(results)
    grid_csv = save_grid_results(results)

    best = results[0]
    best_m = best["metrics"]
    print(
        "\n🎯 Revised 最优参数: "
        f"MA{best['ma_len']} | US10Y>{best['us10y_th']} | "
        f"VIX>{best['vix_th']} 或 VIX>{best['vix_ma_multiplier']}×VIX_MA60 | "
        f"RSI<{best['rsi_th']} | risk_pos={best['risk_pos']}"
    )
    print(
        f"年化: {best_m['cagr'] * 100:.2f}% | "
        f"最大回撤: {best_m['max_dd'] * 100:.2f}% "
        f"({best_m['dd_peak'].date()} -> {best_m['dd_trough'].date()}) | "
        f"Calmar: {best_m['calmar']:.2f} | 平均仓位: {best_m['avg_exposure'] * 100:.1f}%"
    )

    final_result = run_strategy(
        df=df,
        ma_len=FINAL_PARAMS["ma_len"],
        us10y_th=FINAL_PARAMS["us10y_th"],
        vix_th=FINAL_PARAMS["vix_th"],
        rsi_th=FINAL_PARAMS["rsi_th"],
        risk_pos=FINAL_PARAMS["risk_pos"],
        risk_on_pos=FINAL_PARAMS["risk_on_pos"],
        vix_ma_multiplier=FINAL_PARAMS["vix_ma_multiplier"],
    )
    final_m = final_result["metrics"]
    print(
        "\n✅ QQQ Three-Layer v3 Final 固定参数复核: "
        f"MA{FINAL_PARAMS['ma_len']} | US10Y>{FINAL_PARAMS['us10y_th']} | "
        f"VIX>{FINAL_PARAMS['vix_th']} 或 VIX>{FINAL_PARAMS['vix_ma_multiplier']}×VIX_MA60 | "
        f"RSI<{FINAL_PARAMS['rsi_th']} | risk_pos={FINAL_PARAMS['risk_pos']}"
    )
    print(
        f"年化: {final_m['cagr'] * 100:.2f}% | "
        f"最大回撤: {final_m['max_dd'] * 100:.2f}% "
        f"({final_m['dd_peak'].date()} -> {final_m['dd_trough'].date()}) | "
        f"Calmar: {final_m['calmar']:.2f} | 平均仓位: {final_m['avg_exposure'] * 100:.1f}%"
    )

    buy_hold = run_buy_hold(df)
    print_state_distribution(final_result)

    timeseries_csv = save_timeseries(final_result)
    comparison_png = plot_full_comparison(buy_hold, final_result)
    period_rows, period_pngs = build_period_analysis(buy_hold, final_result)
    period_csv, period_md = save_period_outputs(period_rows)

    print("\n📄 输出文件:")
    print(f"  Grid Search: {grid_csv}")
    print(f"  Timeseries : {timeseries_csv}")
    print(f"  Period CSV : {period_csv}")
    print(f"  Period MD  : {period_md}")
    print(f"  Full Chart : {comparison_png}")
    print("  Period Charts:")
    for path in period_pngs:
        print(f"    {path}")


if __name__ == "__main__":
    main()
