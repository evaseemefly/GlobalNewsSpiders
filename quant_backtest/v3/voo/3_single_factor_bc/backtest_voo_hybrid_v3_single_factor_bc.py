import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Optional


# ==================== Mac 中文字体修复 ====================
plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False


CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv"
)
OUTPUT_DIR = Path(__file__).parent

# A: 当前 VOO v3_cagr Final，不重新搜索核心参数。
FINAL_PARAMS = {
    "ma_len": 150,
    "us10y_th": 0.08,
    "vix_th": 38,
    "vix_ma_multiplier": 1.8,
    "rsi_th": 32,
    "risk_pos": 0.30,
    "risk_on_pos": 1.00,
}

# B: MA 斜率软过滤。使用 log(MA) 的 N 日平均日斜率，避免价格尺度影响。
SLOPE_LOOKBACK = 10
SLOPE_EPSILON = 0.0

# C: RSP/VOO 相对广度软过滤。使用等权 ETF 相对市值权重 ETF 的 N 日平均日动量。
BREADTH_LOOKBACK = 20
BREADTH_EPSILON = 0.0

# 单因子软过滤只把原本 100% 的非 Risk-Off 满仓信号降到 70%，不改变 Risk-Off 底仓。
SOFT_FILTER_POS = 0.70

PERIODS = [
    ("全样本", None, None),
    ("训练段 2014-2021", "2014-01-01", "2021-12-31"),
    ("样本外 2022-至今", "2022-01-01", None),
    ("COVID 2020", "2020-01-01", "2020-12-31"),
    ("加息熊市 2022", "2022-01-01", "2022-12-31"),
    ("近三年 2023-至今", "2023-01-01", None),
]

FORWARD_HORIZONS = [5, 20, 60]


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def filter_real_trading_days(df: pd.DataFrame, asset: str = "VOO") -> pd.DataFrame:
    """
    过滤真实交易日，避免周末 ffill 复制价格导致日涨跌幅和信号滞后失真。
    """
    df = df.copy()
    close_col = f"{asset}_close"
    volume_col = f"{asset}_volume"

    if close_col not in df.columns:
        raise KeyError(f"缺少字段: {close_col}")

    df = df[df[close_col].notna()]
    df = df[df.index.dayofweek < 5]

    if volume_col in df.columns:
        df = df[df[volume_col] > 0]

    return df.ffill()


def prepare_indicators(df: pd.DataFrame, p: dict) -> pd.DataFrame:
    required_cols = [
        "VOO_open",
        "VOO_close",
        "RSP_close",
        "HYG_close",
        "US10Y_close",
        "VIX_close",
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise KeyError(f"缺少字段: {missing}")

    df = df.copy()
    df["VOO_MA"] = df["VOO_close"].rolling(p["ma_len"], min_periods=1).mean()
    df["HYG_MA60"] = df["HYG_close"].rolling(60, min_periods=1).mean()
    df["VIX_MA60"] = df["VIX_close"].rolling(60, min_periods=1).mean()
    df["RSI_14"] = calculate_rsi(df["VOO_close"])
    df["VOO_ret"] = df["VOO_close"].pct_change().fillna(0)

    log_ma = np.log(df["VOO_MA"])
    df["ma_log_daily_slope"] = (log_ma - log_ma.shift(SLOPE_LOOKBACK)) / SLOPE_LOOKBACK
    df["ma_slope_weak"] = df["ma_log_daily_slope"] <= SLOPE_EPSILON

    df["rsp_voo_ratio"] = df["RSP_close"] / df["VOO_close"]
    log_ratio = np.log(df["rsp_voo_ratio"])
    df["rsp_voo_rel_daily_mom"] = (log_ratio - log_ratio.shift(BREADTH_LOOKBACK)) / BREADTH_LOOKBACK
    df["breadth_weak"] = df["rsp_voo_rel_daily_mom"] <= BREADTH_EPSILON

    return df


def build_v3_baseline_signal(df: pd.DataFrame, p: dict) -> dict:
    base_trend = df["VOO_close"] > df["VOO_MA"]
    us10y_rising = df["US10Y_close"].diff(20) > p["us10y_th"]
    hyg_divergence = ((df["HYG_close"] < df["HYG_MA60"]).rolling(3).sum() == 3) & base_trend
    vix_risk = (
        (df["VIX_close"] > p["vix_th"])
        | (df["VIX_close"] > df["VIX_MA60"] * p["vix_ma_multiplier"])
    )
    risk_off = us10y_rising | hyg_divergence | vix_risk

    raw_dip_buy = (
        (df["RSI_14"] < p["rsi_th"])
        & (df["VOO_close"] > df["VOO_open"])
        & (df["VIX_close"] < df["VIX_close"].shift(1))
    )

    # v3_cagr Final: 系统性 Risk-Off 优先，Risk-Off 期间不允许 Dip-Buy 拉满仓。
    dip_buy = raw_dip_buy & (~risk_off)

    conditions = [risk_off, dip_buy, base_trend]
    choices = [p["risk_pos"], p["risk_on_pos"], p["risk_on_pos"]]
    signal_position = pd.Series(
        np.select(conditions, choices, default=p["risk_pos"]),
        index=df.index,
        name="signal_position",
    )
    position = signal_position.shift(1).fillna(p["risk_pos"])

    return {
        "base_trend": base_trend,
        "us10y_rising": us10y_rising,
        "hyg_divergence": hyg_divergence,
        "vix_risk": vix_risk,
        "risk_off": risk_off,
        "raw_dip_buy": raw_dip_buy,
        "dip_buy": dip_buy,
        "signal_position": signal_position,
        "position": position,
    }


def apply_soft_filter(
    baseline_signal_position: pd.Series,
    weak_mask: pd.Series,
    p: dict,
    soft_pos: float,
) -> pd.Series:
    """
    单因子软过滤：只降低原本 100% 的信号日，Risk-Off 的 30% 底仓不再二次调整。
    """
    filtered_signal = baseline_signal_position.copy()
    full_signal = baseline_signal_position >= p["risk_on_pos"] - 1e-12
    filtered_signal.loc[full_signal & weak_mask.fillna(False)] = soft_pos
    return filtered_signal.shift(1).fillna(p["risk_pos"])


def max_drawdown_recovery_days(nav: pd.Series) -> Optional[int]:
    if nav.empty:
        return None

    roll_max = nav.cummax()
    drawdown = nav / roll_max - 1
    dd_trough = drawdown.idxmin()
    dd_peak = nav.loc[:dd_trough].idxmax()
    peak_nav = nav.loc[dd_peak]
    recovered = nav.loc[dd_trough:][nav.loc[dd_trough:] >= peak_nav]

    if recovered.empty:
        return None

    return int((recovered.index[0] - dd_peak).days)


def calc_metrics_from_position(df: pd.DataFrame, position: pd.Series) -> dict:
    returns = (position * df["VOO_ret"]).dropna()
    if returns.empty:
        return {
            "total_return": 0.0,
            "cagr": 0.0,
            "max_dd": 0.0,
            "calmar": 0.0,
            "avg_exposure": 0.0,
            "annual_turnover": 0.0,
            "worst_year": "",
            "worst_year_return": 0.0,
            "recovery_days": None,
            "dd_peak": pd.NaT,
            "dd_trough": pd.NaT,
            "nav": pd.Series(dtype=float),
        }

    returns = returns.copy()
    returns.iloc[0] = 0.0
    nav = (1 + returns).cumprod()
    total_return = nav.iloc[-1] - 1
    days = (nav.index[-1] - nav.index[0]).days
    years = days / 365.25
    cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 and total_return > -1 else 0.0

    roll_max = nav.cummax()
    drawdown = nav / roll_max - 1
    max_dd = drawdown.min()
    dd_trough = drawdown.idxmin()
    dd_peak = nav.loc[:dd_trough].idxmax()
    calmar = cagr / abs(max_dd) if max_dd < 0 else 0.0

    exposure = position.reindex(returns.index).dropna()
    avg_exposure = exposure.mean() * 100 if not exposure.empty else 0.0
    annual_turnover = exposure.diff().abs().sum() / years if years > 0 else 0.0

    yearly_returns = (1 + returns).groupby(returns.index.year).prod() - 1
    if yearly_returns.empty:
        worst_year = ""
        worst_year_return = 0.0
    else:
        worst_year = str(int(yearly_returns.idxmin()))
        worst_year_return = yearly_returns.min()

    return {
        "total_return": total_return,
        "cagr": cagr,
        "max_dd": max_dd,
        "calmar": calmar,
        "avg_exposure": avg_exposure,
        "annual_turnover": annual_turnover,
        "worst_year": worst_year,
        "worst_year_return": worst_year_return,
        "recovery_days": max_drawdown_recovery_days(nav),
        "dd_peak": dd_peak,
        "dd_trough": dd_trough,
        "nav": nav,
    }


def calc_period_metrics(df: pd.DataFrame, position: pd.Series, start: Optional[str], end: Optional[str]) -> dict:
    period_df = df.copy()
    period_position = position.copy()
    if start:
        start_ts = pd.Timestamp(start)
        period_df = period_df.loc[period_df.index >= start_ts]
        period_position = period_position.loc[period_position.index >= start_ts]
    if end:
        end_ts = pd.Timestamp(end)
        period_df = period_df.loc[period_df.index <= end_ts]
        period_position = period_position.loc[period_position.index <= end_ts]

    if period_df.empty:
        return calc_metrics_from_position(period_df, period_position)

    period_df = period_df.copy()
    period_df["VOO_ret"] = period_df["VOO_close"].pct_change().fillna(0)
    return calc_metrics_from_position(period_df, period_position)


def calc_future_return_stats(df: pd.DataFrame, state_masks: dict) -> pd.DataFrame:
    rows = []
    close = df["VOO_close"]

    for state_name, mask in state_masks.items():
        mask = mask.reindex(df.index).fillna(False)
        valid_mask_count = int(mask.sum())

        for horizon in FORWARD_HORIZONS:
            future_ret = close.shift(-horizon) / close - 1
            sample = future_ret[mask].dropna()
            if sample.empty:
                row = {
                    "state": state_name,
                    "horizon_days": horizon,
                    "occurrences": valid_mask_count,
                    "valid_forward_samples": 0,
                    "avg_return": np.nan,
                    "median_return": np.nan,
                    "win_rate": np.nan,
                    "p10_return": np.nan,
                    "p25_return": np.nan,
                }
            else:
                row = {
                    "state": state_name,
                    "horizon_days": horizon,
                    "occurrences": valid_mask_count,
                    "valid_forward_samples": len(sample),
                    "avg_return": sample.mean(),
                    "median_return": sample.median(),
                    "win_rate": (sample > 0).mean(),
                    "p10_return": sample.quantile(0.10),
                    "p25_return": sample.quantile(0.25),
                }
            rows.append(row)

    return pd.DataFrame(rows)


def format_pct(value: float) -> str:
    if pd.isna(value):
        return ""
    return f"{value * 100:.2f}%"


def format_date(value) -> str:
    return value.strftime("%Y-%m-%d") if pd.notna(value) else ""


def build_strategy_table(df: pd.DataFrame, strategies: list) -> pd.DataFrame:
    rows = []
    for period_name, start, end in PERIODS:
        for strategy in strategies:
            metrics = calc_period_metrics(df, strategy["position"], start, end)
            rows.append(
                {
                    "period": period_name,
                    "strategy": strategy["name"],
                    "params": strategy["params"],
                    "total_return": metrics["total_return"],
                    "cagr": metrics["cagr"],
                    "max_dd": metrics["max_dd"],
                    "calmar": metrics["calmar"],
                    "avg_exposure": metrics["avg_exposure"],
                    "annual_turnover": metrics["annual_turnover"],
                    "worst_year": metrics["worst_year"],
                    "worst_year_return": metrics["worst_year_return"],
                    "recovery_days": metrics["recovery_days"],
                    "dd_peak": format_date(metrics["dd_peak"]),
                    "dd_trough": format_date(metrics["dd_trough"]),
                }
            )

    return pd.DataFrame(rows)


def plot_nav_compare(df: pd.DataFrame, strategies: list, output_path: Path, start: Optional[str] = None) -> None:
    fig, ax = plt.subplots(figsize=(15, 8))

    plot_df = df.copy()
    if start:
        plot_df = plot_df.loc[plot_df.index >= pd.Timestamp(start)]

    for strategy in strategies:
        position = strategy["position"].reindex(plot_df.index)
        metrics = calc_period_metrics(plot_df, position, None, None)
        nav = metrics["nav"]
        if nav.empty:
            continue
        label = (
            f"{strategy['name']} | CAGR {metrics['cagr'] * 100:.2f}% | "
            f"DD {metrics['max_dd'] * 100:.2f}%"
        )
        ax.plot(nav.index, nav, label=label, linewidth=strategy.get("linewidth", 2.2), alpha=strategy.get("alpha", 1.0))

    title_suffix = "2022-至今" if start else "全样本"
    ax.set_title(f"VOO v3_cagr B/C 单因子实验净值对比 - {title_suffix}", fontsize=15, fontweight="bold")
    ax.set_ylabel("Normalized NAV")
    ax.set_yscale("log")
    ax.grid(True, alpha=0.35)
    ax.legend(loc="upper left", fontsize=9)

    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def print_summary_table(report: pd.DataFrame, period: str = "全样本") -> None:
    display = report[report["period"] == period].copy()
    for col in ["total_return", "cagr", "max_dd", "worst_year_return"]:
        display[col] = display[col].map(format_pct)
    display["calmar"] = display["calmar"].map(lambda x: f"{x:.2f}")
    display["avg_exposure"] = display["avg_exposure"].map(lambda x: f"{x:.1f}%")
    display["annual_turnover"] = display["annual_turnover"].map(lambda x: f"{x:.2f}x")
    cols = [
        "strategy",
        "cagr",
        "max_dd",
        "calmar",
        "avg_exposure",
        "annual_turnover",
        "worst_year",
        "worst_year_return",
        "recovery_days",
    ]
    print(f"\n=== {period} 核心结果 ===")
    print(display[cols].to_string(index=False))


def main() -> None:
    print("=== 🚀 VOO v3_cagr 单因子实验：B=MA斜率 | C=RSP/VOO广度 ===")
    print(
        "说明：A 为当前 v3_cagr Final；B/C 不重新搜索核心参数，只在原本 100% 仓位日触发弱信号时降至 70%。"
    )

    df = pd.read_csv(CSV_FILE_PATH)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()
    df = filter_real_trading_days(df, asset="VOO")
    df = prepare_indicators(df, FINAL_PARAMS)

    baseline = build_v3_baseline_signal(df, FINAL_PARAMS)

    position_a = baseline["position"]
    position_b = apply_soft_filter(
        baseline["signal_position"],
        df["ma_slope_weak"],
        FINAL_PARAMS,
        SOFT_FILTER_POS,
    )
    position_c = apply_soft_filter(
        baseline["signal_position"],
        df["breadth_weak"],
        FINAL_PARAMS,
        SOFT_FILTER_POS,
    )

    strategies = [
        {
            "name": "A 当前VOO v3_cagr Final",
            "params": (
                "MA150 | US10Y>0.08 | VIX>38 或 1.8×VIX_MA60 | "
                "RSI<32 | risk_pos=0.30"
            ),
            "position": position_a,
            "linewidth": 2.8,
        },
        {
            "name": "B v3 + MA斜率软过滤",
            "params": (
                f"A + log(MA{FINAL_PARAMS['ma_len']}) {SLOPE_LOOKBACK}日平均日斜率"
                f"<={SLOPE_EPSILON:.4f} 时满仓信号降至 {SOFT_FILTER_POS:.0%}"
            ),
            "position": position_b,
            "linewidth": 2.4,
        },
        {
            "name": "C v3 + RSP/VOO广度软过滤",
            "params": (
                f"A + log(RSP/VOO) {BREADTH_LOOKBACK}日平均日动量"
                f"<={BREADTH_EPSILON:.4f} 时满仓信号降至 {SOFT_FILTER_POS:.0%}"
            ),
            "position": position_c,
            "linewidth": 2.4,
        },
        {
            "name": "Buy&Hold VOO",
            "params": "100% VOO",
            "position": pd.Series(1.0, index=df.index),
            "linewidth": 1.8,
            "alpha": 0.75,
        },
    ]

    report = build_strategy_table(df, strategies)
    report_path = OUTPUT_DIR / "v3_single_factor_bc_metrics.csv"
    report.to_csv(report_path, index=False)

    full_signal = baseline["signal_position"] >= FINAL_PARAMS["risk_on_pos"] - 1e-12
    state_masks = {
        "A_risk_off": baseline["risk_off"],
        "A_dip_buy_non_risk_off": baseline["dip_buy"],
        "A_full_signal_all": full_signal,
        "B_full_signal_and_ma_slope_weak": full_signal & df["ma_slope_weak"],
        "B_full_signal_and_ma_slope_ok": full_signal & (~df["ma_slope_weak"]),
        "C_full_signal_and_breadth_weak": full_signal & df["breadth_weak"],
        "C_full_signal_and_breadth_ok": full_signal & (~df["breadth_weak"]),
        "B_and_C_both_weak_reference": full_signal & df["ma_slope_weak"] & df["breadth_weak"],
        "B_or_C_any_weak_reference": full_signal & (df["ma_slope_weak"] | df["breadth_weak"]),
    }
    future_stats = calc_future_return_stats(df, state_masks)
    future_stats_path = OUTPUT_DIR / "v3_single_factor_bc_future_return_stats.csv"
    future_stats.to_csv(future_stats_path, index=False)

    indicator_export_cols = [
        "VOO_close",
        "VOO_MA",
        "ma_log_daily_slope",
        "ma_slope_weak",
        "RSP_close",
        "rsp_voo_ratio",
        "rsp_voo_rel_daily_mom",
        "breadth_weak",
    ]
    indicator_path = OUTPUT_DIR / "v3_single_factor_bc_daily_indicators.csv"
    df[indicator_export_cols].to_csv(indicator_path)

    plot_nav_compare(df, strategies, OUTPUT_DIR / "v3_single_factor_bc_nav_full.png")
    plot_nav_compare(df, strategies, OUTPUT_DIR / "v3_single_factor_bc_nav_2022_to_now.png", start="2022-01-01")

    print_summary_table(report, "全样本")
    print_summary_table(report, "样本外 2022-至今")
    print_summary_table(report, "近三年 2023-至今")

    print("\n=== 状态后未来收益统计预览 ===")
    preview = future_stats.copy()
    for col in ["avg_return", "median_return", "win_rate", "p10_return", "p25_return"]:
        preview[col] = preview[col].map(format_pct)
    print(preview.to_string(index=False))

    print(f"\n📄 策略指标已保存: {report_path}")
    print(f"📄 状态后未来收益统计已保存: {future_stats_path}")
    print(f"📄 每日斜率/广度指标已保存: {indicator_path}")
    print(f"🖼️ 全样本净值图已保存: {OUTPUT_DIR / 'v3_single_factor_bc_nav_full.png'}")
    print(f"🖼️ 2022至今净值图已保存: {OUTPUT_DIR / 'v3_single_factor_bc_nav_2022_to_now.png'}")


if __name__ == "__main__":
    main()
