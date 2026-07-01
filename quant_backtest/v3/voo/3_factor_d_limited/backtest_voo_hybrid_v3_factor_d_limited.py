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

# B/C 指标沿用上一轮单因子实验口径。
SLOPE_LOOKBACK = 10
SLOPE_EPSILON = 0.0
BREADTH_LOOKBACK = 20
BREADTH_EPSILON = 0.0

# D 的有限实验，不做大规模搜索。
PRIMARY_D_POS = 0.80
SENSITIVITY_D_POS = 0.70
CONFIRM_DAYS = 3
RECOVERY_DAYS = 3

PERIODS = [
    ("全样本", None, None),
    ("训练段 2014-2021", "2014-01-01", "2021-12-31"),
    ("验证期 2022-至今", "2022-01-01", None),
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

    df["dual_weak_score"] = df["ma_slope_weak"].astype(int) + df["breadth_weak"].astype(int)
    df["dual_weak_raw"] = df["dual_weak_score"] == 2

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


def build_confirmed_dual_weak_state(
    ma_slope_weak: pd.Series,
    breadth_weak: pd.Series,
    confirm_days: int,
    recovery_days: int,
) -> pd.Series:
    """
    D 的严格版状态机：
    - B 和 C 连续 confirm_days 同时弱，进入 dual weak active；
    - 任一指标连续 recovery_days 恢复，退出 active。
    """
    ma_slope_weak = ma_slope_weak.fillna(False)
    breadth_weak = breadth_weak.fillna(False)

    active_values = []
    active = False
    both_count = 0
    slope_ok_count = 0
    breadth_ok_count = 0

    for dt in ma_slope_weak.index:
        slope_weak = bool(ma_slope_weak.loc[dt])
        breadth_is_weak = bool(breadth_weak.loc[dt])
        both_weak = slope_weak and breadth_is_weak

        both_count = both_count + 1 if both_weak else 0
        slope_ok_count = 0 if slope_weak else slope_ok_count + 1
        breadth_ok_count = 0 if breadth_is_weak else breadth_ok_count + 1

        if not active and both_count >= confirm_days:
            active = True
        elif active and (slope_ok_count >= recovery_days or breadth_ok_count >= recovery_days):
            active = False

        active_values.append(active)

    return pd.Series(active_values, index=ma_slope_weak.index, name="dual_weak_confirmed")


def apply_d_filter(
    baseline_signal_position: pd.Series,
    d_state: pd.Series,
    p: dict,
    soft_pos: float,
) -> pd.Series:
    """
    D 有限软过滤：只降低 A 原本 100% 的信号日，不改变 Risk-Off 30% 底仓。
    """
    filtered_signal = baseline_signal_position.copy()
    full_signal = baseline_signal_position >= p["risk_on_pos"] - 1e-12
    filtered_signal.loc[full_signal & d_state.fillna(False)] = soft_pos
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


def build_scaled_control(position_a: pd.Series, position_target: pd.Series) -> tuple[pd.Series, float]:
    scale = position_target.mean() / position_a.mean()
    return (position_a * scale).clip(lower=0.0, upper=1.0), scale


def extract_episodes(df: pd.DataFrame, mask: pd.Series, state_name: str) -> pd.DataFrame:
    mask = mask.reindex(df.index).fillna(False)
    starts = mask & (~mask.shift(1, fill_value=False))
    episode_id = starts.cumsum()

    rows = []
    for eid in episode_id[mask].dropna().unique():
        episode_dates = episode_id[(episode_id == eid) & mask].index
        if episode_dates.empty:
            continue

        start = episode_dates[0]
        end = episode_dates[-1]
        close_start = df.loc[start, "VOO_close"]

        row = {
            "state": state_name,
            "episode_id": int(eid),
            "start": start,
            "end": end,
            "year": int(start.year),
            "duration_trading_days": len(episode_dates),
        }

        for horizon in FORWARD_HORIZONS:
            loc = df.index.get_loc(start)
            fwd_loc = loc + horizon
            if fwd_loc < len(df):
                close_end = df["VOO_close"].iloc[fwd_loc]
                row[f"fwd_{horizon}d"] = close_end / close_start - 1
            else:
                row[f"fwd_{horizon}d"] = np.nan

        rows.append(row)

    return pd.DataFrame(rows)


def summarize_episode_returns(episodes: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if episodes.empty:
        return pd.DataFrame(rows)

    for state_name, group in episodes.groupby("state"):
        for horizon in FORWARD_HORIZONS:
            col = f"fwd_{horizon}d"
            sample = group[col].dropna()
            if sample.empty:
                rows.append(
                    {
                        "state": state_name,
                        "horizon_days": horizon,
                        "episodes": len(group),
                        "valid_forward_samples": 0,
                        "avg_return": np.nan,
                        "median_return": np.nan,
                        "win_rate": np.nan,
                        "p10_return": np.nan,
                        "p25_return": np.nan,
                    }
                )
                continue

            rows.append(
                {
                    "state": state_name,
                    "horizon_days": horizon,
                    "episodes": len(group),
                    "valid_forward_samples": len(sample),
                    "avg_return": sample.mean(),
                    "median_return": sample.median(),
                    "win_rate": (sample > 0).mean(),
                    "p10_return": sample.quantile(0.10),
                    "p25_return": sample.quantile(0.25),
                }
            )

    return pd.DataFrame(rows)


def build_contribution_by_year(df: pd.DataFrame, position_a: pd.Series, strategies: list) -> pd.DataFrame:
    rows = []
    base = position_a.reindex(df.index)

    for strategy in strategies:
        if strategy.get("skip_contribution"):
            continue

        pos = strategy["position"].reindex(df.index)
        diff = pos - base
        contribution = diff * df["VOO_ret"]
        changed = diff.abs() > 1e-12

        for year, year_contrib in contribution.groupby(contribution.index.year):
            idx = year_contrib.index
            year_diff = diff.loc[idx]
            year_changed = changed.loc[idx]
            rows.append(
                {
                    "strategy": strategy["name"],
                    "year": int(year),
                    "contribution_sum": year_contrib.sum(),
                    "changed_days": int(year_changed.sum()),
                    "avg_position_diff": year_diff.mean(),
                    "abs_position_diff_sum": year_diff.abs().sum(),
                    "saved_down_days": int(((year_contrib > 0) & year_changed).sum()),
                    "missed_up_days": int(((year_contrib < 0) & year_changed).sum()),
                }
            )

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
        if strategy.get("hide_in_plot"):
            continue
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
    ax.set_title(f"VOO v3_cagr D 有限实验净值对比 - {title_suffix}", fontsize=15, fontweight="bold")
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


def print_episode_summary(summary: pd.DataFrame) -> None:
    if summary.empty:
        print("\n没有识别到 D episode。")
        return

    display = summary.copy()
    for col in ["avg_return", "median_return", "win_rate", "p10_return", "p25_return"]:
        display[col] = display[col].map(format_pct)
    print("\n=== D 独立 episode 后未来收益统计 ===")
    print(display.to_string(index=False))


def main() -> None:
    print("=== 🚀 VOO v3_cagr 有限实验 D：MA斜率弱 + RSP/VOO广度弱 ===")
    print(
        "说明：A 继续作为正式基准；D 只在 A 原本 100% 仓位且双弱共振时做有限软过滤。"
    )

    df = pd.read_csv(CSV_FILE_PATH)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()
    df = filter_real_trading_days(df, asset="VOO")
    df = prepare_indicators(df, FINAL_PARAMS)

    baseline = build_v3_baseline_signal(df, FINAL_PARAMS)
    full_signal = baseline["signal_position"] >= FINAL_PARAMS["risk_on_pos"] - 1e-12

    df["a_signal_position"] = baseline["signal_position"]
    df["a_position"] = baseline["position"]
    df["a_full_signal"] = full_signal
    df["d_raw_state"] = full_signal & df["dual_weak_raw"]
    df["dual_weak_confirmed"] = build_confirmed_dual_weak_state(
        df["ma_slope_weak"],
        df["breadth_weak"],
        CONFIRM_DAYS,
        RECOVERY_DAYS,
    )
    df["d_confirmed_state"] = full_signal & df["dual_weak_confirmed"]

    position_a = baseline["position"]
    position_d_raw_80 = apply_d_filter(baseline["signal_position"], df["d_raw_state"], FINAL_PARAMS, PRIMARY_D_POS)
    position_d_raw_70 = apply_d_filter(
        baseline["signal_position"],
        df["d_raw_state"],
        FINAL_PARAMS,
        SENSITIVITY_D_POS,
    )
    position_d_confirmed_80 = apply_d_filter(
        baseline["signal_position"],
        df["d_confirmed_state"],
        FINAL_PARAMS,
        PRIMARY_D_POS,
    )
    position_d_confirmed_70 = apply_d_filter(
        baseline["signal_position"],
        df["d_confirmed_state"],
        FINAL_PARAMS,
        SENSITIVITY_D_POS,
    )

    scaled_raw_80, scale_raw_80 = build_scaled_control(position_a, position_d_raw_80)
    scaled_confirmed_80, scale_confirmed_80 = build_scaled_control(position_a, position_d_confirmed_80)

    strategies = [
        {
            "name": "A 当前VOO v3_cagr Final",
            "params": "MA150 | US10Y>0.08 | VIX>38 或 1.8×VIX_MA60 | RSI<32 | risk_pos=0.30",
            "position": position_a,
            "linewidth": 2.8,
            "skip_contribution": True,
        },
        {
            "name": "D_raw80 双弱日降至80%",
            "params": "A满仓信号 & MA斜率弱 & 广度弱 -> 80%",
            "position": position_d_raw_80,
            "linewidth": 2.5,
        },
        {
            "name": "D_confirmed80 双弱3日确认降至80%",
            "params": f"双弱连续{CONFIRM_DAYS}日确认，任一指标连续{RECOVERY_DAYS}日恢复后退出 -> 80%",
            "position": position_d_confirmed_80,
            "linewidth": 2.5,
        },
        {
            "name": "D_raw70 敏感性：双弱日降至70%",
            "params": "A满仓信号 & MA斜率弱 & 广度弱 -> 70%",
            "position": position_d_raw_70,
            "linewidth": 1.8,
            "alpha": 0.7,
        },
        {
            "name": "D_confirmed70 敏感性：双弱3日确认降至70%",
            "params": f"双弱连续{CONFIRM_DAYS}日确认，任一指标连续{RECOVERY_DAYS}日恢复后退出 -> 70%",
            "position": position_d_confirmed_70,
            "linewidth": 1.8,
            "alpha": 0.7,
        },
        {
            "name": "A_scaled_to_D_raw80_avg_exposure",
            "params": f"A 等比例缩放至 D_raw80 平均仓位，scale={scale_raw_80:.4f}",
            "position": scaled_raw_80,
            "hide_in_plot": True,
            "skip_contribution": True,
        },
        {
            "name": "A_scaled_to_D_confirmed80_avg_exposure",
            "params": f"A 等比例缩放至 D_confirmed80 平均仓位，scale={scale_confirmed_80:.4f}",
            "position": scaled_confirmed_80,
            "hide_in_plot": True,
            "skip_contribution": True,
        },
        {
            "name": "Buy&Hold VOO",
            "params": "100% VOO",
            "position": pd.Series(1.0, index=df.index),
            "linewidth": 1.8,
            "alpha": 0.75,
            "skip_contribution": True,
        },
    ]

    report = build_strategy_table(df, strategies)
    report_path = OUTPUT_DIR / "v3_factor_d_limited_metrics.csv"
    report.to_csv(report_path, index=False)

    episodes = pd.concat(
        [
            extract_episodes(df, df["d_raw_state"], "D_raw_full_signal_and_dual_weak"),
            extract_episodes(df, df["d_confirmed_state"], "D_confirmed_full_signal_and_dual_weak"),
        ],
        ignore_index=True,
    )
    episode_path = OUTPUT_DIR / "v3_factor_d_limited_episodes.csv"
    episodes.to_csv(episode_path, index=False)

    episode_summary = summarize_episode_returns(episodes)
    episode_summary_path = OUTPUT_DIR / "v3_factor_d_limited_episode_summary.csv"
    episode_summary.to_csv(episode_summary_path, index=False)

    contribution = build_contribution_by_year(df, position_a, strategies)
    contribution_path = OUTPUT_DIR / "v3_factor_d_limited_contribution_by_year.csv"
    contribution.to_csv(contribution_path, index=False)

    df["d_raw80_position"] = position_d_raw_80
    df["d_raw70_position"] = position_d_raw_70
    df["d_confirmed80_position"] = position_d_confirmed_80
    df["d_confirmed70_position"] = position_d_confirmed_70
    daily_cols = [
        "VOO_close",
        "VOO_MA",
        "a_signal_position",
        "a_position",
        "a_full_signal",
        "ma_log_daily_slope",
        "ma_slope_weak",
        "rsp_voo_ratio",
        "rsp_voo_rel_daily_mom",
        "breadth_weak",
        "dual_weak_score",
        "dual_weak_raw",
        "dual_weak_confirmed",
        "d_raw_state",
        "d_confirmed_state",
        "d_raw80_position",
        "d_raw70_position",
        "d_confirmed80_position",
        "d_confirmed70_position",
    ]
    daily_path = OUTPUT_DIR / "v3_factor_d_limited_daily_indicator.csv"
    df[daily_cols].to_csv(daily_path)

    plot_nav_compare(df, strategies, OUTPUT_DIR / "v3_factor_d_limited_nav_full.png")
    plot_nav_compare(df, strategies, OUTPUT_DIR / "v3_factor_d_limited_nav_2022_to_now.png", start="2022-01-01")

    print_summary_table(report, "全样本")
    print_summary_table(report, "验证期 2022-至今")
    print_summary_table(report, "近三年 2023-至今")
    print_episode_summary(episode_summary)

    print("\n=== D 状态触发概览 ===")
    print(f"Raw D 状态日数: {int(df['d_raw_state'].sum())}")
    print(f"Confirmed D 状态日数: {int(df['d_confirmed_state'].sum())}")
    print(f"Raw episode 数: {len(episodes[episodes['state'] == 'D_raw_full_signal_and_dual_weak'])}")
    print(f"Confirmed episode 数: {len(episodes[episodes['state'] == 'D_confirmed_full_signal_and_dual_weak'])}")

    print(f"\n📄 D 指标回测表已保存: {report_path}")
    print(f"📄 D 独立 episode 明细已保存: {episode_path}")
    print(f"📄 D episode 未来收益统计已保存: {episode_summary_path}")
    print(f"📄 D 收益贡献归因已保存: {contribution_path}")
    print(f"📄 D 每日量化指标已保存: {daily_path}")
    print(f"🖼️ 全样本净值图已保存: {OUTPUT_DIR / 'v3_factor_d_limited_nav_full.png'}")
    print(f"🖼️ 2022至今净值图已保存: {OUTPUT_DIR / 'v3_factor_d_limited_nav_2022_to_now.png'}")


if __name__ == "__main__":
    main()
