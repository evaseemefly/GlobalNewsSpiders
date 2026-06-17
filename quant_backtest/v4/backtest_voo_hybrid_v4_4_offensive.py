#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VOO Hybrid V4.4 Offensive 参数搜索

定位：
- V3 是信号层：沿用已确定的核心参数 MA100 / US10Y>0.15 / VIX>45 / RSI<30。
- V4.4 是偏进攻版本：目标是在受控风险下，寻找 CAGR 高于 V3 的组合。
- 新增 Sharpe / Sortino / Trade Count / Annual Turnover 作为质量与可执行性指标。
- 采用“硬过滤 + 双排序”：
  1) 先剔除收益、回撤、Calmar、交易次数不合格的组合；
  2) 再分别按 CAGR 和综合 Score 排序。

硬过滤：
1. CAGR 必须高于 V3；
2. MaxDD 不得超过 -18%；
3. Calmar 不低于 V3 的 90%；
4. Trade Count 不超过 V3 的 1.5 倍。

综合 Score：
Score = 0.35 * CAGR_rank
      + 0.20 * Calmar_rank
      + 0.15 * Sharpe_rank
      + 0.15 * Sortino_rank
      + 0.10 * LowTurnover_rank
      + 0.05 * LowTradeCount_rank

# todo:26-06-08: V4.4 Offensive，加入 Sharpe / Sortino / Trade Count 约束与偏进攻参数搜索。
"""

from pathlib import Path
from itertools import product
import json

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False


# ============================================================
# 1. 路径与基础配置
# ============================================================

CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv"
)

OUTPUT_DIR = Path(__file__).parent
RESULT_CSV_PATH = OUTPUT_DIR / "voo_v4_4_offensive_gridsearch_results.csv"
PASSED_CSV_PATH = OUTPUT_DIR / "voo_v4_4_offensive_passed_results.csv"
BEST_JSON_PATH = OUTPUT_DIR / "voo_v4_4_offensive_best_params.json"
TOP_CAGR_PIC_PATH = OUTPUT_DIR / "voo_v4_4_offensive_top_cagr.png"
TOP_SCORE_PIC_PATH = OUTPUT_DIR / "voo_v4_4_offensive_top_score.png"
BEST_COMPARE_PIC_PATH = OUTPUT_DIR / "voo_v4_4_offensive_best_compare.png"

TRANSACTION_COST_RATE = 0.0002
FILTER_REAL_TRADING_DAYS = True
SKIP_WARMUP_DAYS = 220


# ============================================================
# 2. V3 已确认核心参数
# ============================================================

V3_FIXED_PARAMS = {
    "asset": "VOO",
    "ma_len": 100,
    "us10y_th": 0.15,
    "vix_th": 45,
    "rsi_th": 30,
    "risk_pos": 0.30,
}


# ============================================================
# 3. V4.4 Offensive 搜索空间
# ============================================================

V4_4_GRID = {
    # Risk-Off 底仓进攻化
    "risk_pos": [0.30, 0.40, 0.50, 0.60],

    # 核心仓底座：始终持有一部分 VOO
    # final_position = core_floor + (1 - core_floor) * signal_position
    "core_floor": [0.00, 0.20, 0.30, 0.40, 0.50],

    # Panic 插件：Risk-Off 中若出现单日恐慌跌幅，允许提高仓位
    "panic_drop_pct": [0.015, 0.020, 0.025],
    "panic_reversal_pos": [0.50, 0.60, 0.70, 0.80],
    "panic_rsi_low": [35],
    "panic_rsi_high": [45],

    # 从低仓切回高仓时，买回目标缺口的比例
    "recovery_gap_buy_pct": [0.75, 1.00],
    "dip_buy_gap_buy_pct": [0.75, 1.00],
}


# ============================================================
# 4. 工具函数
# ============================================================

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def load_data() -> pd.DataFrame:
    if not CSV_FILE_PATH.exists():
        raise FileNotFoundError(f"找不到数据文件: {CSV_FILE_PATH}")

    df = pd.read_csv(CSV_FILE_PATH)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()

    return df


def filter_real_trading_days(df: pd.DataFrame, asset: str) -> pd.DataFrame:
    df = df.copy()

    close_col = f"{asset}_close"
    volume_col = f"{asset}_volume"

    if close_col not in df.columns:
        raise KeyError(f"缺少字段: {close_col}")

    df = df[df[close_col].notna()]

    if FILTER_REAL_TRADING_DAYS:
        df = df[df.index.dayofweek < 5]
        if volume_col in df.columns:
            df = df[df[volume_col] > 0]

    df = df.ffill()
    return df


def prepare_indicators(df: pd.DataFrame, p: dict) -> pd.DataFrame:
    asset = p["asset"]
    close_col = f"{asset}_close"
    open_col = f"{asset}_open"

    df = df.copy()

    if open_col not in df.columns:
        df[open_col] = df[close_col]

    df[f"{asset}_MA"] = df[close_col].rolling(window=p["ma_len"], min_periods=1).mean()
    df["US10Y_MA60"] = df["US10Y_close"].rolling(60, min_periods=1).mean()
    df["HYG_MA60"] = df["HYG_close"].rolling(60, min_periods=1).mean()
    df["VIX_MA60"] = df["VIX_close"].rolling(60, min_periods=1).mean()

    df["US10Y_diff_20"] = df["US10Y_close"].diff(20)
    df["RSI_14"] = calculate_rsi(df[close_col])

    df[f"{asset}_ret"] = df[close_col].pct_change().fillna(0.0)
    df[f"{asset}_daily_return"] = df[close_col].pct_change()

    return df


def compute_v3_signal_flags(df: pd.DataFrame, p: dict) -> pd.DataFrame:
    asset = p["asset"]

    base_trend = df[f"{asset}_close"] > df[f"{asset}_MA"]
    us10y_rising = df["US10Y_diff_20"] > p["us10y_th"]

    hyg_divergence = (
        ((df["HYG_close"] < df["HYG_MA60"]).rolling(3).sum() == 3)
        & base_trend
    )

    vix_risk = (
        (df["VIX_close"] > p["vix_th"])
        | (df["VIX_close"] > df["VIX_MA60"] * 1.8)
    )

    risk_off = us10y_rising | hyg_divergence | vix_risk

    dip_buy = (
        (df["RSI_14"] < p["rsi_th"])
        & (df[f"{asset}_close"] > df[f"{asset}_open"])
        & (df["VIX_close"] < df["VIX_close"].shift(1))
    )

    return pd.DataFrame(
        {
            "base_trend": base_trend,
            "us10y_rising": us10y_rising,
            "hyg_divergence": hyg_divergence,
            "vix_risk": vix_risk,
            "risk_off": risk_off,
            "dip_buy": dip_buy,
        },
        index=df.index,
    )


def compute_nav_with_cost(
    position: pd.Series,
    asset_ret: pd.Series,
    cost_rate: float = TRANSACTION_COST_RATE,
):
    position = position.fillna(0.0)
    turnover_daily = position.diff().abs().fillna(0.0)

    daily_net_ret = position * asset_ret.fillna(0.0) - turnover_daily * cost_rate
    nav = (1 + daily_net_ret).cumprod()

    return nav, daily_net_ret, turnover_daily


def move_towards(current_pos: float, target_pos: float, gap_pct: float) -> float:
    if target_pos < current_pos:
        return target_pos
    if target_pos > current_pos:
        return current_pos + (target_pos - current_pos) * gap_pct
    return current_pos


# ============================================================
# 5. 回测函数
# ============================================================

def run_v3_baseline(df: pd.DataFrame, p: dict) -> dict:
    signal_df = compute_v3_signal_flags(df, p)

    raw_position = np.select(
        [
            signal_df["dip_buy"],
            signal_df["risk_off"],
            signal_df["base_trend"],
        ],
        [
            1.0,
            p["risk_pos"],
            1.0,
        ],
        default=p["risk_pos"],
    )

    position = pd.Series(raw_position, index=df.index).shift(1).fillna(p["risk_pos"])

    nav, daily_net_ret, turnover_daily = compute_nav_with_cost(
        position=position,
        asset_ret=df[f"{p['asset']}_ret"],
    )

    return {
        "nav": nav,
        "position": position,
        "daily_net_ret": daily_net_ret,
        "turnover_daily": turnover_daily,
        "signal_df": signal_df,
    }


def run_v4_4_offensive(df: pd.DataFrame, base_params: dict, v44_params: dict) -> dict:
    p = dict(base_params)
    p.update(v44_params)

    asset = p["asset"]
    signal_df = compute_v3_signal_flags(df, p)

    current_pos = p["core_floor"] + (1 - p["core_floor"]) * p["risk_pos"]

    position_for_return = []
    target_position_series = []
    raw_signal_position_series = []
    notes = []

    for dt, row in signal_df.iterrows():
        position_for_return.append(current_pos)

        daily_return = df.at[dt, f"{asset}_daily_return"]
        rsi = df.at[dt, "RSI_14"]

        if row["dip_buy"]:
            raw_signal_pos = 1.0
            note = "V3 Dip-Buy"
            gap_pct = p["dip_buy_gap_buy_pct"]

        elif row["risk_off"]:
            raw_signal_pos = p["risk_pos"]
            note = "V3 Risk-Off"
            gap_pct = p["recovery_gap_buy_pct"]

            panic_triggered = (
                pd.notna(daily_return)
                and daily_return <= -p["panic_drop_pct"]
                and p["panic_rsi_low"] <= rsi <= p["panic_rsi_high"]
            )

            if panic_triggered:
                raw_signal_pos = max(raw_signal_pos, p["panic_reversal_pos"])
                note = "Risk-Off + Panic-Reversal"

        elif row["base_trend"]:
            raw_signal_pos = 1.0
            note = "V3 Risk-On"
            gap_pct = p["recovery_gap_buy_pct"]

        else:
            raw_signal_pos = p["risk_pos"]
            note = "V3 Trend-Weak"
            gap_pct = p["recovery_gap_buy_pct"]

        target_pos = p["core_floor"] + (1 - p["core_floor"]) * raw_signal_pos
        target_pos = float(np.clip(target_pos, 0.0, 1.0))

        if target_pos < current_pos:
            new_pos = target_pos
        else:
            new_pos = move_towards(current_pos, target_pos, gap_pct)

        new_pos = float(np.clip(new_pos, 0.0, 1.0))

        raw_signal_position_series.append(raw_signal_pos)
        target_position_series.append(target_pos)
        notes.append(note)

        current_pos = new_pos

    position = pd.Series(position_for_return, index=df.index).fillna(
        p["core_floor"] + (1 - p["core_floor"]) * p["risk_pos"]
    )

    nav, daily_net_ret, turnover_daily = compute_nav_with_cost(
        position=position,
        asset_ret=df[f"{asset}_ret"],
    )

    state_df = signal_df.copy()
    state_df["raw_signal_position"] = pd.Series(raw_signal_position_series, index=df.index)
    state_df["target_position"] = pd.Series(target_position_series, index=df.index)
    state_df["position"] = position
    state_df["note"] = notes
    state_df["daily_net_ret"] = daily_net_ret
    state_df["turnover_daily"] = turnover_daily

    return {
        "nav": nav,
        "position": position,
        "daily_net_ret": daily_net_ret,
        "turnover_daily": turnover_daily,
        "state_df": state_df,
        "params": p,
    }


# ============================================================
# 6. 绩效指标
# ============================================================

def max_recovery_days(nav: pd.Series) -> int:
    roll_max = nav.cummax()
    in_drawdown = nav < roll_max

    max_days = 0
    current_start = None

    for dt, is_dd in in_drawdown.items():
        if is_dd and current_start is None:
            current_start = dt
        elif (not is_dd) and current_start is not None:
            max_days = max(max_days, (dt - current_start).days)
            current_start = None

    if current_start is not None:
        max_days = max(max_days, (nav.index[-1] - current_start).days)

    return int(max_days)


def calc_metrics(
    nav: pd.Series,
    position: pd.Series,
    daily_net_ret: pd.Series,
    turnover_daily: pd.Series,
) -> dict:
    total_ret = nav.iloc[-1] - 1

    days = (nav.index[-1] - nav.index[0]).days
    years = days / 365.25 if days > 0 else 0

    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else np.nan

    roll_max = nav.cummax()
    drawdown = (nav - roll_max) / roll_max
    max_dd = drawdown.min()

    calmar = cagr / abs(max_dd) if max_dd != 0 else np.nan

    ann_vol = daily_net_ret.std() * np.sqrt(252)
    sharpe = cagr / ann_vol if ann_vol and ann_vol > 0 else np.nan

    downside = daily_net_ret[daily_net_ret < 0]
    downside_vol = downside.std() * np.sqrt(252) if len(downside) > 1 else np.nan
    sortino = cagr / downside_vol if downside_vol and downside_vol > 0 else np.nan

    total_turnover = turnover_daily.sum()
    annual_turnover = total_turnover / years if years > 0 else np.nan

    trade_count = int((turnover_daily > 1e-8).sum())
    avg_position = position.mean()
    time_in_market = avg_position * 100

    try:
        yearly_ret = (1 + daily_net_ret).resample("YE").prod() - 1
    except ValueError:
        yearly_ret = (1 + daily_net_ret).resample("Y").prod() - 1

    worst_year = yearly_ret.min() if len(yearly_ret) > 0 else np.nan

    recovery_days = max_recovery_days(nav)

    return {
        "total_ret": total_ret,
        "cagr": cagr,
        "max_dd": max_dd,
        "calmar": calmar,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "total_turnover": total_turnover,
        "annual_turnover": annual_turnover,
        "trade_count": trade_count,
        "avg_position": avg_position,
        "time_in_market": time_in_market,
        "worst_year": worst_year,
        "recovery_days": recovery_days,
    }


def add_rank_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if df.empty:
        return df

    df["cagr_rank"] = df["cagr"].rank(pct=True, ascending=True)
    df["calmar_rank"] = df["calmar"].rank(pct=True, ascending=True)
    df["sharpe_rank"] = df["sharpe"].rank(pct=True, ascending=True)
    df["sortino_rank"] = df["sortino"].rank(pct=True, ascending=True)

    df["turnover_rank_low_better"] = df["annual_turnover"].rank(pct=True, ascending=False)
    df["trade_count_rank_low_better"] = df["trade_count"].rank(pct=True, ascending=False)

    df["score"] = (
        0.35 * df["cagr_rank"]
        + 0.20 * df["calmar_rank"]
        + 0.15 * df["sharpe_rank"]
        + 0.15 * df["sortino_rank"]
        + 0.10 * df["turnover_rank_low_better"]
        + 0.05 * df["trade_count_rank_low_better"]
    )

    return df


# ============================================================
# 7. 绘图函数
# ============================================================

def plot_top_results(df: pd.DataFrame, baseline: dict, results: list, title: str, path: Path, sort_key: str):
    asset = V3_FIXED_PARAMS["asset"]

    if not results:
        print(f"⚠️ 无法绘制 {title}，因为没有通过过滤的结果。")
        return

    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        figsize=(18, 12),
        gridspec_kw={"height_ratios": [3, 1]},
        sharex=True,
    )
    plt.subplots_adjust(hspace=0.05)

    buy_hold_position = pd.Series(1.0, index=df.index)
    buy_hold_nav, _, _ = compute_nav_with_cost(
        position=buy_hold_position,
        asset_ret=df[f"{asset}_ret"],
    )

    ax1.plot(df.index, buy_hold_nav, label="Buy & Hold VOO", color="gray", alpha=0.65, linewidth=2)
    ax1.plot(df.index, baseline["nav"], label="V3 Baseline", color="black", linewidth=2.5)

    colors = plt.cm.tab10(np.linspace(0, 1, min(10, len(results))))

    for i, r in enumerate(results[:10]):
        label = (
            f"Top{i + 1} "
            f"CAGR {r['cagr'] * 100:.2f}% "
            f"Calmar {r['calmar']:.2f} "
            f"Score {r.get('score', np.nan):.2f}"
        )
        lw = 3.0 if i == 0 else 1.5
        alpha = 1.0 if i == 0 else 0.85

        ax1.plot(
            df.index,
            r["nav"],
            label=label,
            color=colors[i],
            linewidth=lw,
            alpha=alpha,
        )

    ax1.set_title(title, fontsize=16, fontweight="bold")
    ax1.set_ylabel("Net Asset Value")
    ax1.set_yscale("log")
    ax1.legend(loc="upper left", fontsize=8)
    ax1.grid(True, alpha=0.4)

    best = results[0]
    ax2.plot(df.index, baseline["position"], label="V3 Position", color="black", linewidth=1.5)
    ax2.plot(df.index, best["position"], label=f"Best V4.4 Position ({sort_key})", color="#e74c3c", linewidth=1.8)
    ax2.set_ylim(0, 1.05)
    ax2.set_ylabel("Position")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.4)

    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_best_compare(df: pd.DataFrame, baseline: dict, best: dict):
    asset = V3_FIXED_PARAMS["asset"]

    fig, axes = plt.subplots(
        4,
        1,
        figsize=(18, 16),
        gridspec_kw={"height_ratios": [3, 1, 1, 1]},
        sharex=True,
    )
    ax1, ax2, ax3, ax4 = axes

    buy_hold_position = pd.Series(1.0, index=df.index)
    buy_hold_nav, _, _ = compute_nav_with_cost(
        position=buy_hold_position,
        asset_ret=df[f"{asset}_ret"],
    )

    ax1.plot(df.index, buy_hold_nav, label="Buy & Hold VOO", color="gray", alpha=0.65, linewidth=2)
    ax1.plot(df.index, baseline["nav"], label="V3 Baseline", color="black", linewidth=2.5)
    ax1.plot(df.index, best["nav"], label="Best V4.4 Offensive", color="#e74c3c", linewidth=2.5)
    ax1.set_yscale("log")
    ax1.set_title("VOO V4.4 Offensive Best vs V3 Baseline", fontsize=16, fontweight="bold")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.4)

    ax2.plot(df.index, baseline["position"], label="V3 Position", color="black", linewidth=1.5)
    ax2.plot(df.index, best["position"], label="V4.4 Position", color="#e74c3c", linewidth=1.8)
    ax2.set_ylim(0, 1.05)
    ax2.set_ylabel("Position")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.4)

    ax3.plot(df.index, df[f"{asset}_close"], label="VOO Price", color="#2c3e50")
    ax3.plot(df.index, df[f"{asset}_MA"], label=f"MA{V3_FIXED_PARAMS['ma_len']}", color="#f39c12", linestyle="--")
    ax3.legend(loc="upper left")
    ax3.grid(True, alpha=0.4)

    ax4.plot(df.index, df["HYG_close"], label="HYG", color="#2980b9")
    ax4.plot(df.index, df["HYG_MA60"], label="HYG MA60", color="#16a085", linestyle="--")
    ax4_twin = ax4.twinx()
    ax4_twin.plot(df.index, df["VIX_close"], label="VIX", color="#c0392b", alpha=0.65)
    ax4.set_title("Risk Reference: HYG vs MA60 + VIX")
    ax4.legend(loc="upper left")
    ax4_twin.legend(loc="upper right")
    ax4.grid(True, alpha=0.4)

    fig.savefig(BEST_COMPARE_PIC_PATH, dpi=200, bbox_inches="tight")
    plt.close(fig)


# ============================================================
# 8. 主程序
# ============================================================

def main():
    print("=== 🚀 VOO Hybrid V4.4 Offensive 参数搜索 ===")
    print(f"交易摩擦成本: {TRANSACTION_COST_RATE:.4%}")

    raw_df = load_data()
    raw_df = filter_real_trading_days(raw_df, V3_FIXED_PARAMS["asset"])
    df = prepare_indicators(raw_df, V3_FIXED_PARAMS)

    if SKIP_WARMUP_DAYS > 0 and len(df) > SKIP_WARMUP_DAYS:
        df = df.iloc[SKIP_WARMUP_DAYS:].copy()

    baseline = run_v3_baseline(df, V3_FIXED_PARAMS)
    baseline_metrics = calc_metrics(
        nav=baseline["nav"],
        position=baseline["position"],
        daily_net_ret=baseline["daily_net_ret"],
        turnover_daily=baseline["turnover_daily"],
    )

    print("\n📌 V3 基准模型:")
    print(
        f"   MA{V3_FIXED_PARAMS['ma_len']} | "
        f"US10Y>{V3_FIXED_PARAMS['us10y_th']} | "
        f"VIX>{V3_FIXED_PARAMS['vix_th']} | "
        f"RSI<{V3_FIXED_PARAMS['rsi_th']} | "
        f"RiskPos {V3_FIXED_PARAMS['risk_pos']}"
    )
    print(
        f"   CAGR {baseline_metrics['cagr'] * 100:.2f}% | "
        f"MaxDD {baseline_metrics['max_dd'] * 100:.2f}% | "
        f"Calmar {baseline_metrics['calmar']:.2f} | "
        f"Sharpe {baseline_metrics['sharpe']:.2f} | "
        f"Sortino {baseline_metrics['sortino']:.2f} | "
        f"AnnualTurnover {baseline_metrics['annual_turnover']:.2f} | "
        f"Trades {baseline_metrics['trade_count']}"
    )

    min_cagr = baseline_metrics["cagr"]
    max_allowed_dd = -0.18
    min_calmar = baseline_metrics["calmar"] * 0.90
    max_trade_count = int(baseline_metrics["trade_count"] * 1.5)

    print("\n🔒 V4.4 Offensive 硬过滤条件:")
    print(f"   1. CAGR > V3 CAGR，即 > {min_cagr * 100:.2f}%")
    print(f"   2. MaxDD >= {max_allowed_dd * 100:.2f}%")
    print(f"   3. Calmar >= V3 Calmar × 90%，即 >= {min_calmar:.2f}")
    print(f"   4. Trade Count <= V3 × 1.5，即 <= {max_trade_count} 次")

    keys = list(V4_4_GRID.keys())
    values = [V4_4_GRID[k] for k in keys]
    total = int(np.prod([len(v) for v in values]))
    print(f"\n开始搜索 V4.4 Offensive 参数... 共 {total} 种组合\n")

    all_results = []

    for combo in product(*values):
        v44_params = dict(zip(keys, combo))

        res = run_v4_4_offensive(
            df=df,
            base_params=V3_FIXED_PARAMS,
            v44_params=v44_params,
        )

        metrics = calc_metrics(
            nav=res["nav"],
            position=res["position"],
            daily_net_ret=res["daily_net_ret"],
            turnover_daily=res["turnover_daily"],
        )

        row = dict(v44_params)
        row.update(metrics)
        row["nav"] = res["nav"]
        row["position"] = res["position"]
        row["state_df"] = res["state_df"]
        row["params"] = res["params"]

        row["pass_filter"] = (
            (row["cagr"] > min_cagr)
            and (row["max_dd"] >= max_allowed_dd)
            and (row["calmar"] >= min_calmar)
            and (row["trade_count"] <= max_trade_count)
        )

        all_results.append(row)

    result_df = pd.DataFrame([
        {k: v for k, v in r.items() if k not in ["nav", "position", "state_df", "params"]}
        for r in all_results
    ])

    result_df = add_rank_score(result_df)
    result_df.to_csv(RESULT_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"💾 全部搜索结果已保存: {RESULT_CSV_PATH.name}")

    passed_df = result_df[result_df["pass_filter"]].copy()
    passed_df = add_rank_score(passed_df)
    passed_df.to_csv(PASSED_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"💾 通过硬过滤的结果已保存: {PASSED_CSV_PATH.name}")

    passed_indices = set(passed_df.index.tolist())
    passed_results = []
    for idx, r in enumerate(all_results):
        if idx in passed_indices:
            r["score"] = float(passed_df.loc[idx, "score"])
            passed_results.append(r)

    if not passed_results:
        print("\n⚠️ 没有任何 V4.4 Offensive 组合通过硬过滤。")
        print("   可以考虑：放宽 MaxDD 到 -19%，或放宽 Trade Count 到 V3 × 2.0。")
        return

    top_by_cagr = sorted(
        passed_results,
        key=lambda x: (x["cagr"], x["calmar"], x["sharpe"], x["sortino"], -x["annual_turnover"]),
        reverse=True,
    )

    top_by_score = sorted(
        passed_results,
        key=lambda x: (x.get("score", -999), x["cagr"], x["calmar"]),
        reverse=True,
    )

    print("\n🏆 Top 10 V4.4 Offensive 参数组合（通过过滤后，按 CAGR 排序）:")
    print("-" * 190)
    print(
        f"{'排名':<4} {'RiskPos':<8} {'Core':<6} {'PanicDrop':<10} {'PanicPos':<9} "
        f"{'Recovery':<9} {'DipGap':<8} | "
        f"{'CAGR':<8} {'MaxDD':<8} {'Calmar':<8} {'Sharpe':<8} {'Sortino':<8} "
        f"{'年换手':<8} {'交易':<6} {'持仓%':<7} {'Score':<7}"
    )
    print("-" * 190)

    for i, r in enumerate(top_by_cagr[:10], 1):
        print(
            f"{i:<4} "
            f"{r['risk_pos']:<8.2f} "
            f"{r['core_floor']:<6.2f} "
            f"{r['panic_drop_pct']:<10.3f} "
            f"{r['panic_reversal_pos']:<9.2f} "
            f"{r['recovery_gap_buy_pct']:<9.2f} "
            f"{r['dip_buy_gap_buy_pct']:<8.2f} | "
            f"{r['cagr'] * 100:>6.2f}% "
            f"{r['max_dd'] * 100:>7.2f}% "
            f"{r['calmar']:>7.2f} "
            f"{r['sharpe']:>7.2f} "
            f"{r['sortino']:>7.2f} "
            f"{r['annual_turnover']:>7.2f} "
            f"{r['trade_count']:>6} "
            f"{r['time_in_market']:>6.1f}% "
            f"{r.get('score', np.nan):>6.2f}"
        )

    print("\n🏅 Top 10 V4.4 Offensive 参数组合（通过过滤后，按综合 Score 排序）:")
    print("-" * 190)
    print(
        f"{'排名':<4} {'RiskPos':<8} {'Core':<6} {'PanicDrop':<10} {'PanicPos':<9} "
        f"{'Recovery':<9} {'DipGap':<8} | "
        f"{'CAGR':<8} {'MaxDD':<8} {'Calmar':<8} {'Sharpe':<8} {'Sortino':<8} "
        f"{'年换手':<8} {'交易':<6} {'持仓%':<7} {'Score':<7}"
    )
    print("-" * 190)

    for i, r in enumerate(top_by_score[:10], 1):
        print(
            f"{i:<4} "
            f"{r['risk_pos']:<8.2f} "
            f"{r['core_floor']:<6.2f} "
            f"{r['panic_drop_pct']:<10.3f} "
            f"{r['panic_reversal_pos']:<9.2f} "
            f"{r['recovery_gap_buy_pct']:<9.2f} "
            f"{r['dip_buy_gap_buy_pct']:<8.2f} | "
            f"{r['cagr'] * 100:>6.2f}% "
            f"{r['max_dd'] * 100:>7.2f}% "
            f"{r['calmar']:>7.2f} "
            f"{r['sharpe']:>7.2f} "
            f"{r['sortino']:>7.2f} "
            f"{r['annual_turnover']:>7.2f} "
            f"{r['trade_count']:>6} "
            f"{r['time_in_market']:>6.1f}% "
            f"{r.get('score', np.nan):>6.2f}"
        )

    best_cagr = top_by_cagr[0]
    best_score = top_by_score[0]

    print("\n🎯 V4.4 生产候选建议:")
    print("   - 若你追求最高收益：参考 CAGR Top 1。")
    print("   - 若你追求综合质量：参考 Score Top 1。")
    print("   - 若两者参数接近，则该组合稳定性更强。")

    print("\n📈 CAGR Top 1:")
    for k in keys:
        print(f"   {k}: {best_cagr[k]}")
    print(
        f"   CAGR {best_cagr['cagr'] * 100:.2f}% | "
        f"MaxDD {best_cagr['max_dd'] * 100:.2f}% | "
        f"Calmar {best_cagr['calmar']:.2f} | "
        f"Sharpe {best_cagr['sharpe']:.2f} | "
        f"Sortino {best_cagr['sortino']:.2f} | "
        f"Trades {best_cagr['trade_count']}"
    )

    print("\n🏅 Score Top 1:")
    for k in keys:
        print(f"   {k}: {best_score[k]}")
    print(
        f"   CAGR {best_score['cagr'] * 100:.2f}% | "
        f"MaxDD {best_score['max_dd'] * 100:.2f}% | "
        f"Calmar {best_score['calmar']:.2f} | "
        f"Sharpe {best_score['sharpe']:.2f} | "
        f"Sortino {best_score['sortino']:.2f} | "
        f"Trades {best_score['trade_count']} | "
        f"Score {best_score.get('score', np.nan):.2f}"
    )

    print(
        "\n📊 V3 Baseline: "
        f"CAGR {baseline_metrics['cagr'] * 100:.2f}% | "
        f"MaxDD {baseline_metrics['max_dd'] * 100:.2f}% | "
        f"Calmar {baseline_metrics['calmar']:.2f} | "
        f"Sharpe {baseline_metrics['sharpe']:.2f} | "
        f"Sortino {baseline_metrics['sortino']:.2f} | "
        f"Trades {baseline_metrics['trade_count']}"
    )

    best_json = {
        "transaction_cost_rate": TRANSACTION_COST_RATE,
        "v3_fixed_params": V3_FIXED_PARAMS,
        "v3_metrics": {
            k: float(v) if isinstance(v, (float, np.floating)) else int(v) if isinstance(v, (int, np.integer)) else v
            for k, v in baseline_metrics.items()
        },
        "hard_filters": {
            "min_cagr": float(min_cagr),
            "max_allowed_dd": float(max_allowed_dd),
            "min_calmar": float(min_calmar),
            "max_trade_count": int(max_trade_count),
        },
        "cagr_top1_params": {k: best_cagr[k] for k in keys},
        "cagr_top1_metrics": {
            k: float(best_cagr[k]) if isinstance(best_cagr[k], (float, np.floating)) else int(best_cagr[k]) if isinstance(best_cagr[k], (int, np.integer)) else best_cagr[k]
            for k in [
                "total_ret", "cagr", "max_dd", "calmar", "ann_vol", "sharpe", "sortino",
                "annual_turnover", "trade_count", "avg_position", "time_in_market",
                "worst_year", "recovery_days", "score"
            ]
        },
        "score_top1_params": {k: best_score[k] for k in keys},
        "score_top1_metrics": {
            k: float(best_score[k]) if isinstance(best_score[k], (float, np.floating)) else int(best_score[k]) if isinstance(best_score[k], (int, np.integer)) else best_score[k]
            for k in [
                "total_ret", "cagr", "max_dd", "calmar", "ann_vol", "sharpe", "sortino",
                "annual_turnover", "trade_count", "avg_position", "time_in_market",
                "worst_year", "recovery_days", "score"
            ]
        },
        "notes": [
            "V4.4 Offensive uses V3 as signal layer.",
            "Hard filters are applied before ranking.",
            "Two rankings are provided: CAGR-first and Score-first.",
            "Score includes CAGR, Calmar, Sharpe, Sortino, turnover, and trade count.",
        ],
    }

    BEST_JSON_PATH.write_text(json.dumps(best_json, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n💾 最优参数 JSON 已保存: {BEST_JSON_PATH.name}")

    plot_top_results(
        df=df,
        baseline=baseline,
        results=top_by_cagr,
        title="VOO V4.4 Offensive Top 10 by CAGR",
        path=TOP_CAGR_PIC_PATH,
        sort_key="CAGR",
    )
    print(f"🖼️ CAGR Top10 图已保存: {TOP_CAGR_PIC_PATH.name}")

    plot_top_results(
        df=df,
        baseline=baseline,
        results=top_by_score,
        title="VOO V4.4 Offensive Top 10 by Score",
        path=TOP_SCORE_PIC_PATH,
        sort_key="Score",
    )
    print(f"🖼️ Score Top10 图已保存: {TOP_SCORE_PIC_PATH.name}")

    plot_best_compare(df, baseline, best_score)
    print(f"🖼️ Best Compare 图已保存: {BEST_COMPARE_PIC_PATH.name}")

    print("\n✅ V4.4 Offensive 参数搜索完成。")


if __name__ == "__main__":
    main()
