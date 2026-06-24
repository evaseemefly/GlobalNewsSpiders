import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from itertools import product
from pathlib import Path
from typing import Dict, List, Tuple


# ==================== Mac Chinese font support ====================
plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False


# ==================== Data path ====================
DATA_CANDIDATES = [
    Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_max_20140917_to_20260424.csv"),
    Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv"),
    Path("/Users/evaseemefly/03data/05-spiders/broad_market_history/historical_broad_market_master.csv"),
]

OUTPUT_DIR = Path(__file__).resolve().parent


# QQQ parameters copied from daily_index_trade_signal_v3.py.
DAILY_QQQ_FIVE_LAYER_PARAMS = {
    "strategy_name": "QQQ Five-Layer V3",
    "ma_len": 200,
    "crash_ma_len": 200,
    "us10y_th": 0.15,
    "vix_warning_low": 15,
    "vix_warning_high": 20,
    "vix_risk_th": 20,
    "vix_crash_th": 30,
    "vix_extreme_th": 45,
    "rsi_th": 30,
    "panic_rsi_low": 35,
    "panic_rsi_high": 45,
    "panic_drop_pct": 0.045,
    "risk_on_pos": 1.00,
    "risk_warning_pos": 0.60,
    "risk_pos": 0.30,
    "panic_reversal_pos": 0.40,
    "crash_pos": 0.25,
}


# Best result from backtest_qqq_hybrid_v3_gridsearch_bygrok.py.
THREE_LAYER_BEST_PARAMS = {
    "ma_len": 200,
    "us10y_th": 0.15,
    "vix_th": 45,
    "vix_ma_multiplier": 1.8,
    "rsi_th": 30,
    "risk_pos": 0.30,
}


RUN_CORE_GRID_SEARCH = True


def resolve_data_path() -> Path:
    for path in DATA_CANDIDATES:
        if path.exists():
            return path
    raise FileNotFoundError(
        "找不到历史宽表数据，请检查 DATA_CANDIDATES 中的路径。"
    )


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def load_and_filter_data() -> pd.DataFrame:
    csv_path = resolve_data_path()
    print(f"📂 数据路径: {csv_path}")

    df = pd.read_csv(csv_path)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()

    required = ["QQQ_close", "HYG_close", "US10Y_close", "VIX_close"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"缺少必要字段: {missing}")

    df = df[df["QQQ_close"].notna()]
    df = df[df.index.dayofweek < 5]

    if "QQQ_volume" in df.columns:
        df = df[df["QQQ_volume"] > 0]

    if "QQQ_open" not in df.columns:
        df["QQQ_open"] = df["QQQ_close"]

    df = df.ffill()
    return df


def prepare_indicators(df: pd.DataFrame, ma_len: int) -> pd.DataFrame:
    df = df.copy()

    df["QQQ_MA"] = df["QQQ_close"].rolling(ma_len, min_periods=1).mean()
    df["QQQ_MA100"] = df["QQQ_close"].rolling(100, min_periods=1).mean()
    df["QQQ_MA200"] = df["QQQ_close"].rolling(200, min_periods=1).mean()
    df["US10Y_diff_20"] = df["US10Y_close"].diff(20)
    df["HYG_MA60"] = df["HYG_close"].rolling(60, min_periods=1).mean()
    df["VIX_MA60"] = df["VIX_close"].rolling(60, min_periods=1).mean()
    df["RSI_14_QQQ"] = calculate_rsi(df["QQQ_close"])
    df["QQQ_ret"] = df["QQQ_close"].pct_change().fillna(0.0)

    return df


def calc_metrics(nav: pd.Series, position: pd.Series) -> Dict:
    total_ret = nav.iloc[-1] - 1
    years = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0.0

    roll_max = nav.cummax()
    drawdown = nav / roll_max - 1.0
    max_dd = drawdown.min()
    trough = drawdown.idxmin()
    peak = nav.loc[:trough].idxmax()
    calmar = cagr / abs(max_dd) if max_dd != 0 else np.nan

    return {
        "total_return": total_ret,
        "cagr": cagr,
        "max_dd": max_dd,
        "calmar": calmar,
        "avg_exposure": position.mean(),
        "dd_peak": peak,
        "dd_trough": trough,
    }


def run_buy_hold(df: pd.DataFrame) -> Dict:
    prepared = prepare_indicators(df, DAILY_QQQ_FIVE_LAYER_PARAMS["ma_len"])
    position = pd.Series(1.0, index=prepared.index)
    nav = (1 + prepared["QQQ_ret"]).cumprod()
    metrics = calc_metrics(nav, position)

    return {
        "name": "Buy&Hold QQQ",
        "df": prepared,
        "nav": nav,
        "position": position,
        "metrics": metrics,
    }


def run_three_layer_strategy(df: pd.DataFrame, p: Dict) -> Dict:
    prepared = prepare_indicators(df, p["ma_len"])

    base_trend = prepared["QQQ_close"] > prepared["QQQ_MA"]
    us10y_rising = prepared["US10Y_diff_20"] > p["us10y_th"]
    hyg_divergence = (
        (prepared["HYG_close"] < prepared["HYG_MA60"]).rolling(3).sum() == 3
    ) & base_trend
    vix_risk = (
        (prepared["VIX_close"] > p["vix_th"])
        | (prepared["VIX_close"] > prepared["VIX_MA60"] * p["vix_ma_multiplier"])
    )
    risk_off = us10y_rising | hyg_divergence | vix_risk

    dip_buy = (
        (prepared["RSI_14_QQQ"] < p["rsi_th"])
        & (prepared["QQQ_close"] > prepared["QQQ_open"])
        & (prepared["VIX_close"] < prepared["VIX_close"].shift(1))
    )

    position_raw = np.select(
        [dip_buy, risk_off, base_trend],
        [1.0, p["risk_pos"], 1.0],
        default=p["risk_pos"],
    )
    prepared["position_raw"] = position_raw
    prepared["position"] = prepared["position_raw"].shift(1).fillna(p["risk_pos"])
    prepared["state_raw"] = np.select(
        [dip_buy, risk_off, base_trend],
        ["Dip-Buy", "Risk-Off", "Risk-On"],
        default="Trend-Weak",
    )
    prepared["state"] = prepared["state_raw"].shift(1).fillna("Initial")
    prepared["nav"] = (1 + prepared["position"] * prepared["QQQ_ret"]).cumprod()

    metrics = calc_metrics(prepared["nav"], prepared["position"])

    return {
        "name": "Three-Layer Hybrid Best",
        "df": prepared,
        "nav": prepared["nav"],
        "position": prepared["position"],
        "metrics": metrics,
    }


def run_five_layer_strategy(df: pd.DataFrame, p: Dict) -> Dict:
    prepared = prepare_indicators(df, p["ma_len"])

    close = prepared["QQQ_close"]
    ma = prepared["QQQ_MA"]
    ma100 = prepared["QQQ_MA100"]
    ma200 = prepared["QQQ_MA200"]
    rsi = prepared["RSI_14_QQQ"]
    vix = prepared["VIX_close"]

    base_trend = close > ma
    us10y_rising = prepared["US10Y_diff_20"] > p["us10y_th"]
    credit_weak = prepared["HYG_close"] < prepared["HYG_MA60"]
    hyg_divergence = (
        (prepared["HYG_close"] < prepared["HYG_MA60"]).rolling(3).sum() == 3
    ) & base_trend

    vix_warning = (vix >= p["vix_warning_low"]) & (vix <= p["vix_warning_high"])
    vix_risk = vix > p["vix_risk_th"]
    vix_crash = vix > p["vix_crash_th"]
    vix_extreme = vix > p["vix_extreme_th"]

    risk_warning = (credit_weak | vix_warning) & (~vix_risk)
    risk_off = us10y_rising | hyg_divergence | (credit_weak & vix_risk) | vix_extreme

    panic_reversal = (
        risk_off
        & (prepared["QQQ_ret"] <= -p["panic_drop_pct"])
        & rsi.between(p["panic_rsi_low"], p["panic_rsi_high"])
    )

    crash = ((close < ma200) & vix_crash) | ((close < ma100) & (vix > p["vix_crash_th"]))

    dip_buy = (
        (rsi < p["rsi_th"])
        & (close > prepared["QQQ_open"])
        & (vix < vix.shift(1))
        & (~risk_off)
    )

    state_conditions = [
        crash,
        panic_reversal,
        risk_off,
        risk_warning,
        dip_buy,
        base_trend,
    ]
    state_choices = [
        "Crash",
        "Panic-Reversal",
        "Risk-Off",
        "Risk-Warning",
        "Dip-Buy",
        "Risk-On",
    ]
    position_choices = [
        p["crash_pos"],
        p["panic_reversal_pos"],
        p["risk_pos"],
        p["risk_warning_pos"],
        p["risk_on_pos"],
        p["risk_on_pos"],
    ]

    prepared["state_raw"] = np.select(
        state_conditions,
        state_choices,
        default="Trend-Weak",
    )
    prepared["position_raw"] = np.select(
        state_conditions,
        position_choices,
        default=p["risk_pos"],
    )

    # signal[t] drives position[t+1]
    prepared["state"] = prepared["state_raw"].shift(1).fillna("Initial")
    prepared["position"] = prepared["position_raw"].shift(1).fillna(p["risk_pos"])
    prepared["nav"] = (1 + prepared["position"] * prepared["QQQ_ret"]).cumprod()

    prepared["base_trend"] = base_trend
    prepared["us10y_rising"] = us10y_rising
    prepared["credit_weak"] = credit_weak
    prepared["hyg_divergence"] = hyg_divergence
    prepared["vix_warning"] = vix_warning
    prepared["vix_risk"] = vix_risk
    prepared["vix_extreme"] = vix_extreme
    prepared["risk_warning"] = risk_warning
    prepared["risk_off"] = risk_off
    prepared["panic_reversal"] = panic_reversal
    prepared["crash"] = crash
    prepared["dip_buy"] = dip_buy

    metrics = calc_metrics(prepared["nav"], prepared["position"])

    return {
        "name": p.get("strategy_name", "QQQ Five-Layer V3"),
        "params": p,
        "df": prepared,
        "nav": prepared["nav"],
        "position": prepared["position"],
        "metrics": metrics,
    }


def format_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def print_result_table(results: List[Dict]) -> None:
    print("\n📊 固定参数回测对比:")
    print("-" * 118)
    print(
        f"{'策略':<24} {'总收益':>10} {'年化':>9} {'最大回撤':>10} "
        f"{'Calmar':>8} {'平均仓位':>10} {'回撤起点':>12} {'回撤终点':>12}"
    )
    print("-" * 118)
    for res in results:
        m = res["metrics"]
        print(
            f"{res['name']:<24} {format_pct(m['total_return']):>10} "
            f"{format_pct(m['cagr']):>9} {format_pct(m['max_dd']):>10} "
            f"{m['calmar']:>8.2f} {format_pct(m['avg_exposure']):>10} "
            f"{m['dd_peak'].date()} {m['dd_trough'].date()}"
        )


def run_five_layer_core_grid(df: pd.DataFrame) -> List[Dict]:
    ma_lens = [80, 100, 120, 150, 180, 200]
    us10y_ths = [0.08, 0.10, 0.12, 0.15]
    vix_extreme_ths = [35, 38, 40, 45]
    rsi_ths = [30, 32, 35]

    results = []
    total = len(ma_lens) * len(us10y_ths) * len(vix_extreme_ths) * len(rsi_ths)
    print(f"\n🔎 五层状态机核心阈值搜索... 共 {total} 种组合")
    print("   仓位层固定为每日 QQQ Five-Layer V3: 100% / 60% / 30% / 40% / 25%")

    for ma_len, us10y_th, vix_extreme_th, rsi_th in product(
        ma_lens,
        us10y_ths,
        vix_extreme_ths,
        rsi_ths,
    ):
        p = DAILY_QQQ_FIVE_LAYER_PARAMS.copy()
        p.update(
            {
                "ma_len": ma_len,
                "us10y_th": us10y_th,
                "vix_extreme_th": vix_extreme_th,
                "rsi_th": rsi_th,
            }
        )
        res = run_five_layer_strategy(df, p)
        m = res["metrics"]
        results.append(
            {
                "ma_len": ma_len,
                "us10y_th": us10y_th,
                "vix_extreme_th": vix_extreme_th,
                "rsi_th": rsi_th,
                "cagr": m["cagr"],
                "max_dd": m["max_dd"],
                "calmar": m["calmar"],
                "avg_exposure": m["avg_exposure"],
                "nav": res["nav"],
                "position": res["position"],
                "df": res["df"],
            }
        )

    results = sorted(results, key=lambda x: (x["calmar"], x["cagr"]), reverse=True)
    return results


def print_grid_top(results: List[Dict], top_n: int = 10) -> None:
    print("\n🏆 Five-Layer V3 Top 10 核心阈值组合 (按 Calmar、CAGR 排序):")
    print("-" * 112)
    print(
        f"{'排名':<4} {'MA':<4} {'US10Y↑':<7} {'VIX极端>':<9} "
        f"{'RSI<':<5} {'年化':>8} {'回撤':>9} {'Calmar':>8} {'平均仓位':>9}"
    )
    print("-" * 112)
    for i, r in enumerate(results[:top_n], 1):
        print(
            f"{i:<4} {r['ma_len']:<4} {r['us10y_th']:<7.2f} {r['vix_extreme_th']:<9} "
            f"{r['rsi_th']:<5} {format_pct(r['cagr']):>8} {format_pct(r['max_dd']):>9} "
            f"{r['calmar']:>8.2f} {format_pct(r['avg_exposure']):>9}"
        )


def save_grid_results(results: List[Dict]) -> Path:
    rows = []
    for r in results:
        rows.append(
            {
                "ma_len": r["ma_len"],
                "us10y_th": r["us10y_th"],
                "vix_extreme_th": r["vix_extreme_th"],
                "rsi_th": r["rsi_th"],
                "cagr": r["cagr"],
                "max_dd": r["max_dd"],
                "calmar": r["calmar"],
                "avg_exposure": r["avg_exposure"],
            }
        )
    out_path = OUTPUT_DIR / "qqq_five_layer_v3_core_grid_results.csv"
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return out_path


def save_timeseries(
    buy_hold: Dict,
    three_layer: Dict,
    five_layer: Dict,
) -> Path:
    df = five_layer["df"].copy()
    out = pd.DataFrame(index=df.index)
    out["QQQ_close"] = df["QQQ_close"]
    out["QQQ_ret"] = df["QQQ_ret"]
    out["BuyHold_NAV"] = buy_hold["nav"]
    out["ThreeLayer_NAV"] = three_layer["nav"]
    out["FiveLayer_NAV"] = five_layer["nav"]
    out["ThreeLayer_Position"] = three_layer["position"]
    out["FiveLayer_Position"] = five_layer["position"]
    out["FiveLayer_State_Raw"] = df["state_raw"]
    out["FiveLayer_State"] = df["state"]
    out["Base_Trend"] = df["base_trend"]
    out["Risk_Warning"] = df["risk_warning"]
    out["Risk_Off"] = df["risk_off"]
    out["Panic_Reversal"] = df["panic_reversal"]
    out["Crash"] = df["crash"]
    out["Dip_Buy"] = df["dip_buy"]

    out_path = OUTPUT_DIR / "qqq_five_layer_v3_timeseries.csv"
    out.to_csv(out_path)
    return out_path


def print_state_distribution(five_layer: Dict) -> None:
    state_counts = (
        five_layer["df"]["state_raw"]
        .value_counts(normalize=True)
        .reindex(
            [
                "Risk-On",
                "Risk-Warning",
                "Risk-Off",
                "Panic-Reversal",
                "Crash",
                "Dip-Buy",
                "Trend-Weak",
            ],
            fill_value=0.0,
        )
    )

    print("\n🧭 Five-Layer V3 原始信号状态分布:")
    for state, value in state_counts.items():
        print(f"  {state:<15}: {value * 100:>6.2f}%")


def plot_comparison(
    buy_hold: Dict,
    three_layer: Dict,
    five_layer: Dict,
) -> Path:
    df = five_layer["df"]

    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        figsize=(18, 12),
        gridspec_kw={"height_ratios": [3, 1]},
        sharex=True,
    )
    plt.subplots_adjust(hspace=0.05)

    ax1.plot(
        buy_hold["nav"].index,
        buy_hold["nav"],
        label=(
            "Buy&Hold QQQ | "
            f"CAGR {buy_hold['metrics']['cagr'] * 100:.2f}% | "
            f"DD {buy_hold['metrics']['max_dd'] * 100:.2f}%"
        ),
        color="gray",
        alpha=0.7,
        linewidth=1.5,
    )
    ax1.plot(
        three_layer["nav"].index,
        three_layer["nav"],
        label=(
            "Three-Layer Best | "
            f"CAGR {three_layer['metrics']['cagr'] * 100:.2f}% | "
            f"DD {three_layer['metrics']['max_dd'] * 100:.2f}%"
        ),
        color="#e67e22",
        linewidth=2.0,
    )
    ax1.plot(
        five_layer["nav"].index,
        five_layer["nav"],
        label=(
            "QQQ Five-Layer V3 | "
            f"CAGR {five_layer['metrics']['cagr'] * 100:.2f}% | "
            f"DD {five_layer['metrics']['max_dd'] * 100:.2f}%"
        ),
        color="#2ca02c",
        linewidth=2.3,
    )
    ax1.set_title("QQQ Five-Layer V3 回测: 与三层 Hybrid / 死拿对比", fontsize=16, fontweight="bold")
    ax1.set_ylabel("NAV (log scale)")
    ax1.set_yscale("log")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.35)

    ax2.plot(df.index, df["QQQ_close"], label="QQQ Price", color="black", linewidth=1.0)
    ax2.plot(df.index, df["QQQ_MA"], label=f"QQQ MA{DAILY_QQQ_FIVE_LAYER_PARAMS['ma_len']}", color="orange", linestyle="--")

    y_min, y_max = ax2.get_ylim()
    state_colors: List[Tuple[str, str, float]] = [
        ("Risk-Warning", "#f1c40f", 0.16),
        ("Risk-Off", "#e74c3c", 0.16),
        ("Panic-Reversal", "#8e44ad", 0.18),
        ("Crash", "#2c3e50", 0.20),
    ]
    for state, color, alpha in state_colors:
        mask = df["state_raw"].eq(state)
        ax2.fill_between(
            df.index,
            y_min,
            y_max,
            where=mask,
            color=color,
            alpha=alpha,
            label=state,
        )

    ax2.set_ylabel("QQQ Price")
    ax2.legend(loc="upper left", ncol=3)
    ax2.grid(True, alpha=0.35)

    out_path = OUTPUT_DIR / "qqq_five_layer_v3_comparison.png"
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main() -> None:
    print("=== 🚀 QQQ Five-Layer V3 状态机回测 ===")
    raw_df = load_and_filter_data()

    buy_hold = run_buy_hold(raw_df)
    three_layer = run_three_layer_strategy(raw_df, THREE_LAYER_BEST_PARAMS)
    five_layer = run_five_layer_strategy(raw_df, DAILY_QQQ_FIVE_LAYER_PARAMS)

    print_result_table([buy_hold, three_layer, five_layer])
    print_state_distribution(five_layer)

    ts_path = save_timeseries(buy_hold, three_layer, five_layer)
    plot_path = plot_comparison(buy_hold, three_layer, five_layer)

    print(f"\n📄 五层状态机逐日序列已保存: {ts_path}")
    print(f"🖼️ 五层状态机对比图已保存: {plot_path}")

    if RUN_CORE_GRID_SEARCH:
        grid_results = run_five_layer_core_grid(raw_df)
        print_grid_top(grid_results)
        grid_path = save_grid_results(grid_results)
        best = grid_results[0]
        print(
            "\n🎯 Five-Layer 核心阈值搜索最优: "
            f"MA{best['ma_len']} | US10Y>{best['us10y_th']} | "
            f"VIX极端>{best['vix_extreme_th']} | RSI<{best['rsi_th']}"
        )
        print(
            f"年化: {best['cagr'] * 100:.2f}% | "
            f"最大回撤: {best['max_dd'] * 100:.2f}% | "
            f"Calmar: {best['calmar']:.2f} | "
            f"平均仓位: {best['avg_exposure'] * 100:.1f}%"
        )
        print(f"📄 核心阈值搜索结果已保存: {grid_path}")


if __name__ == "__main__":
    main()
