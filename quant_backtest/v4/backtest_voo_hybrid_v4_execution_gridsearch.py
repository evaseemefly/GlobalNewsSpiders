#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VOO Hybrid V4 执行层参数回测 / Grid Search

用途：
1. 固定你已经通过 V3 回测确定的核心参数：
   - MA100
   - US10Y > 0.15
   - VIX > 45
   - RSI < 30
   - Risk-Off 仓位 0.3

2. 在此基础上，只搜索 V4 的“执行节奏参数”：
   - Risk-Warning 理论目标仓位
   - Risk-Warning 连续几天后允许买
   - Risk-Warning 买回目标缺口的比例
   - HYG 修复 + VIX 回落后买回目标缺口的比例
   - Risk-On 恢复速度
   - Panic-Reversal 目标仓位

核心思想：
V3 解决“市场状态 -> 理论目标仓位”；
V4 解决“从当前仓位 -> 理论目标仓位，要不要一步到位”。

# todo:26-06-08: V4 分阶段执行层回测，用于确定 Risk-Warning / Risk-Repair 的最终参数。
"""

import json
from pathlib import Path
from itertools import product

import numpy as np
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ==================== 1. 基础配置 ====================

plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False

CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv"
)

OUTPUT_DIR = Path(__file__).parent
RESULT_CSV_PATH = OUTPUT_DIR / 'data' / "voo_v4_execution_gridsearch_results.csv"
BEST_JSON_PATH = OUTPUT_DIR / 'data' / "voo_v4_best_params.json"
TOP10_PIC_PATH = OUTPUT_DIR / 'data' / "voo_v4_top10_nav_compare.png"
BEST_PIC_PATH = OUTPUT_DIR / 'data' / "voo_v4_best_vs_v3_baseline.png"

# ==================== 2. V3 已确认核心参数 ====================
# 你提供的 V3 最优参数：
# MA100 | US10Y>0.15 | VIX>45 | RSI<30 | Risk-Off=0.3
# 年化 12.91% | 最大回撤 -14.08% | Calmar 0.92

V3_FIXED_PARAMS = {
    "asset": "VOO",

    # V3 已回测确认参数
    "ma_len": 100,
    "us10y_th": 0.15,
    "vix_th": 45,
    "rsi_th": 30,
    "risk_pos": 0.30,

    # V4 使用的补充状态参数
    # 注意：这里不推翻 V3 核心参数，只是在执行层增加 Risk-Warning / Crash / Panic-Reversal。
    "vix_warning_low": 15,
    "vix_warning_high": 20,
    "vix_crash_th": 30,

    "panic_drop_pct": 0.025,  # VOO 单日跌幅达到 2.5% 视为恐慌观察
    "panic_rsi_low": 35,
    "panic_rsi_high": 45,

    "risk_on_pos": 1.00,
    "crash_pos": 0.30,
}

# ==================== 3. V4 执行层参数搜索空间 ====================
# todo:26-06-08: 只搜索执行层，不重新搜索 V3 核心状态识别参数，避免过拟合。

V4_PARAM_GRID = {
    # Risk-Warning 的理论目标仓位。
    # VOO 原建议为 70%，这里允许 60/70/80 做鲁棒性检验。
    "risk_warning_pos": [0.60, 0.70, 0.80],

    # Panic-Reversal 的目标仓位。
    # VOO 原建议为 50%，这里允许 40/50/60 做检验。
    "panic_reversal_pos": [0.40, 0.50, 0.60],

    # Risk-Warning 连续多少天后才允许补仓。
    # 2 = 第 1 天观察，第 2 天开始买回缺口。
    "risk_warning_confirm_days": [2, 3],

    # Risk-Warning 确认后，买回“当前仓位到理论目标仓位”缺口的比例。
    "risk_warning_gap_buy_pct": [0.20, 0.25, 0.33, 0.50],

    # Risk-Repair 条件中的 VIX 阈值。
    # 当 HYG > MA60 且 VIX < 该阈值，认为风险环境修复。
    "risk_repair_vix_th": [16, 18, 20],

    # Risk-Repair 后，买回目标缺口的比例。
    "risk_repair_gap_buy_pct": [0.33, 0.50, 0.67],

    # Risk-On 后，恢复到完整目标仓位的速度。
    # 1.0 = 一次性恢复；0.75 = 先恢复 75% 缺口。
    "risk_on_gap_buy_pct": [0.75, 1.00],
}


# ==================== 4. 工具函数 ====================

def calculate_rsi(series, period=14):
    """计算 RSI。"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def load_master_data():
    if not CSV_FILE_PATH.exists():
        raise FileNotFoundError(f"找不到主数据文件: {CSV_FILE_PATH}")

    df = pd.read_csv(CSV_FILE_PATH)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()
    return df


def filter_real_trading_days(df, asset):
    """
    过滤真实交易日，避免周末 ffill 导致单日涨跌幅失真。

    # todo:26-06-08: V4 回测沿用生产版 V3 的真实交易日过滤逻辑。
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

    df = df.ffill()
    return df


def prepare_indicators(df, p):
    """计算 VOO / 宏观指标。"""
    asset = p["asset"]
    close_col = f"{asset}_close"
    open_col = f"{asset}_open"

    df = df.copy()

    if open_col not in df.columns:
        df[open_col] = df[close_col]

    df[f"{asset}_MA"] = df[close_col].rolling(p["ma_len"], min_periods=1).mean()
    df[f"{asset}_MA100"] = df[close_col].rolling(100, min_periods=1).mean()
    df[f"{asset}_MA200"] = df[close_col].rolling(200, min_periods=1).mean()

    df["US10Y_diff_20"] = df["US10Y_close"].diff(20)
    df["HYG_MA60"] = df["HYG_close"].rolling(60, min_periods=1).mean()
    df["VIX_MA60"] = df["VIX_close"].rolling(60, min_periods=1).mean()

    df[f"RSI_14_{asset}"] = calculate_rsi(df[close_col])
    df[f"{asset}_ret"] = df[close_col].pct_change().fillna(0.0)
    df[f"{asset}_daily_return"] = df[close_col].pct_change()

    return df


def build_daily_state_table(df, p):
    """
    生成每日市场状态表。
    注意：
    - 这里负责“状态识别”。
    - V4 的“执行节奏”在 run_v4_execution_strategy() 中处理。
    """
    asset = p["asset"]
    close = df[f"{asset}_close"]
    open_price = df[f"{asset}_open"]
    ma = df[f"{asset}_MA"]
    ma100 = df[f"{asset}_MA100"]
    ma200 = df[f"{asset}_MA200"]
    rsi = df[f"RSI_14_{asset}"]
    daily_return = df[f"{asset}_daily_return"]

    base_trend = close > ma
    us10y_rising = df["US10Y_diff_20"] > p["us10y_th"]

    credit_weak = df["HYG_close"] < df["HYG_MA60"]
    hyg_divergence = ((df["HYG_close"] < df["HYG_MA60"]).rolling(3).sum() == 3) & base_trend

    vix = df["VIX_close"]
    vix_warning = (vix >= p["vix_warning_low"]) & (vix <= p["vix_warning_high"])
    vix_risk = (vix > p["vix_th"]) | (vix > df["VIX_MA60"] * 1.8)
    vix_crash = vix > p["vix_crash_th"]

    # 与生产 V3 的 Risk-Warning 逻辑接近：
    # HYG 弱或 VIX 位于 15-20，但没有进入 V3 的 Risk-Off。
    risk_warning_raw = credit_weak | vix_warning

    risk_off = us10y_rising | hyg_divergence | vix_risk
    risk_warning = risk_warning_raw & (~risk_off)

    panic_reversal = (
            risk_off
            & (daily_return <= -p["panic_drop_pct"])
            & rsi.between(p["panic_rsi_low"], p["panic_rsi_high"])
    )

    crash = (
            ((close < ma200) & vix_crash)
            | ((close < ma100) & (vix > p["vix_crash_th"]))
    )

    dip_buy = (
            (rsi < p["rsi_th"])
            & (close > open_price)
            & (df["VIX_close"] < df["VIX_close"].shift(1))
            & (~risk_off)
    )

    risk_repair = (
            (df["HYG_close"] > df["HYG_MA60"])
            & (df["VIX_close"] < p.get("risk_repair_vix_th", 18))
    )

    states = np.select(
        [
            crash,
            panic_reversal,
            risk_off,
            risk_warning,
            dip_buy,
            base_trend,
        ],
        [
            "Crash",
            "Panic-Reversal",
            "Risk-Off",
            "Risk-Warning",
            "Dip-Buy",
            "Risk-On",
        ],
        default="Trend-Weak",
    )

    state_df = pd.DataFrame(
        {
            "state": states,
            "base_trend": base_trend,
            "us10y_rising": us10y_rising,
            "credit_weak": credit_weak,
            "hyg_divergence": hyg_divergence,
            "vix_warning": vix_warning,
            "vix_risk": vix_risk,
            "risk_warning": risk_warning,
            "risk_off": risk_off,
            "panic_reversal": panic_reversal,
            "crash": crash,
            "dip_buy": dip_buy,
            "risk_repair": risk_repair,
            "daily_return": daily_return,
            "rsi": rsi,
        },
        index=df.index,
    )

    # Risk-Warning 连续天数，用于 V4 执行层。
    streak = []
    count = 0
    for is_warning in state_df["state"].eq("Risk-Warning"):
        if is_warning:
            count += 1
        else:
            count = 0
        streak.append(count)

    state_df["risk_warning_streak"] = streak

    return state_df


def model_target_position_for_state(state, p):
    """
    市场状态对应的理论目标仓位。
    这是“模型目标”，不是 V4 明日实际执行目标。
    """
    if state == "Crash":
        return p["crash_pos"]
    if state == "Panic-Reversal":
        return p["panic_reversal_pos"]
    if state == "Risk-Off":
        return p["risk_pos"]
    if state == "Risk-Warning":
        return p["risk_warning_pos"]
    if state == "Dip-Buy":
        return p["risk_on_pos"]
    if state == "Risk-On":
        return p["risk_on_pos"]
    return p["risk_pos"]


def move_towards(current_pos, target_pos, gap_pct):
    """
    从当前仓位向目标仓位移动 gap_pct 的缺口。
    gap_pct=1.0 代表一次性移动到目标。
    """
    if target_pos > current_pos:
        return current_pos + (target_pos - current_pos) * gap_pct
    if target_pos < current_pos:
        # 防守/降仓方向默认一步降到目标，避免风险状态下拖延减仓。
        return target_pos
    return current_pos


# ==================== 5. V3 / V4 回测函数 ====================

def run_v3_fixed_baseline(df, p):
    """
    V3 固定参数基线：
    MA100 | US10Y>0.15 | VIX>45 | RSI<30 | Risk-Off=0.3

    逻辑与用户现有 V3 参数搜索脚本一致：
    dip_buy -> 100%
    risk_off -> risk_pos
    base_trend -> 100%
    default -> risk_pos
    """
    asset = p["asset"]

    base_trend = df[f"{asset}_close"] > df[f"{asset}_MA"]
    us10y_rising = df["US10Y_diff_20"] > p["us10y_th"]
    hyg_divergence = ((df["HYG_close"] < df["HYG_MA60"]).rolling(3).sum() == 3) & base_trend
    vix_risk = (df["VIX_close"] > p["vix_th"]) | (df["VIX_close"] > df["VIX_MA60"] * 1.8)
    risk_off = us10y_rising | hyg_divergence | vix_risk

    dip_buy = (
            (df[f"RSI_14_{asset}"] < p["rsi_th"])
            & (df[f"{asset}_close"] > df[f"{asset}_open"])
            & (df["VIX_close"] < df["VIX_close"].shift(1))
    )

    raw_position = np.select(
        [dip_buy, risk_off, base_trend],
        [1.0, p["risk_pos"], 1.0],
        default=p["risk_pos"],
    )

    position = pd.Series(raw_position, index=df.index).shift(1).fillna(p["risk_pos"])
    nav = (1 + position * df[f"{asset}_ret"]).cumprod()

    return {
        "nav": nav,
        "position": position,
    }


def run_v4_execution_strategy(df, base_params, v4_params):
    """
    V4 执行层回测。

    # todo:26-06-08:
    V4 不是把 Risk-Warning 直接买满理论目标仓，而是：
    1) Risk-Off / Crash：只减仓，不补仓。
    2) Risk-Warning 第 1 天：不买，只观察。
    3) Risk-Warning 连续 N 天：买回目标缺口的一部分。
    4) HYG > MA60 且 VIX < 指定阈值：买回目标缺口的较大一部分。
    5) Risk-On：按 risk_on_gap_buy_pct 恢复目标仓位。
    """
    p = dict(base_params)
    p.update(v4_params)

    asset = p["asset"]

    state_df = build_daily_state_table(df, p)

    current_pos = p["risk_pos"]
    position_for_return = []
    execution_target_signal = []
    model_target_signal = []
    execution_notes = []

    # 逐日循环，避免未来函数：
    # 当天持仓用于当天收益；
    # 当天收盘后的状态决定下一交易日持仓。
    for dt, row in state_df.iterrows():
        position_for_return.append(current_pos)

        state = row["state"]
        model_target = model_target_position_for_state(state, p)
        new_pos = current_pos
        note = ""

        if state == "Crash":
            # 只减仓，不补仓
            new_pos = min(current_pos, model_target)
            note = "Crash: only reduce / no buy"

        elif state == "Risk-Off":
            # 只减仓，不补仓
            new_pos = min(current_pos, model_target)
            note = "Risk-Off: only reduce / no buy"

        elif state == "Panic-Reversal":
            # Risk-Off 中的恐慌反弹，只允许提高到 panic_reversal_pos，不恢复满仓。
            if model_target > current_pos:
                new_pos = move_towards(
                    current_pos=current_pos,
                    target_pos=model_target,
                    gap_pct=p.get("panic_gap_buy_pct", 1.0),
                )
            else:
                new_pos = model_target
            note = "Panic-Reversal: controlled counter-trend buy"

        elif state == "Risk-Warning":
            # 如果当前仓位高于 Risk-Warning 理论目标，直接降到目标；
            # 如果当前仓位低于目标，则按确认天数 / 修复条件分步买回。
            if current_pos > model_target:
                new_pos = model_target
                note = "Risk-Warning: reduce to warning target"
            else:
                if bool(row["risk_repair"]):
                    new_pos = move_towards(
                        current_pos=current_pos,
                        target_pos=model_target,
                        gap_pct=p["risk_repair_gap_buy_pct"],
                    )
                    note = "Risk-Warning + repair: buy repair gap"
                elif row["risk_warning_streak"] >= p["risk_warning_confirm_days"]:
                    new_pos = move_towards(
                        current_pos=current_pos,
                        target_pos=model_target,
                        gap_pct=p["risk_warning_gap_buy_pct"],
                    )
                    note = "Risk-Warning confirmed: buy warning gap"
                else:
                    new_pos = current_pos
                    note = "Risk-Warning day 1: observe / no buy"

        elif state == "Dip-Buy":
            new_pos = move_towards(
                current_pos=current_pos,
                target_pos=model_target,
                gap_pct=p.get("risk_on_gap_buy_pct", 1.0),
            )
            note = "Dip-Buy: recover risk-on target"

        elif state == "Risk-On":
            new_pos = move_towards(
                current_pos=current_pos,
                target_pos=model_target,
                gap_pct=p["risk_on_gap_buy_pct"],
            )
            note = "Risk-On: recover risk-on target"

        else:
            # Trend-Weak：只减仓到 risk_pos，不主动补仓
            new_pos = min(current_pos, model_target)
            note = "Trend-Weak: defensive / no buy"

        # 防止浮点越界
        new_pos = float(np.clip(new_pos, 0.0, 1.0))

        model_target_signal.append(model_target)
        execution_target_signal.append(new_pos)
        execution_notes.append(note)

        # 当日收盘信号，下一交易日使用
        current_pos = new_pos

    position = pd.Series(position_for_return, index=df.index).fillna(p["risk_pos"])
    model_target_series = pd.Series(model_target_signal, index=df.index)
    execution_target_series = pd.Series(execution_target_signal, index=df.index)

    nav = (1 + position * df[f"{asset}_ret"]).cumprod()

    state_df = state_df.copy()
    state_df["position"] = position
    state_df["model_target_position"] = model_target_series
    state_df["execution_target_position"] = execution_target_series
    state_df["execution_note"] = execution_notes

    return {
        "nav": nav,
        "position": position,
        "state_df": state_df,
        "params": p,
    }


# ==================== 6. 绩效指标 ====================

def max_recovery_days(nav):
    """估算最大回撤修复天数。"""
    roll_max = nav.cummax()
    in_drawdown = nav < roll_max

    max_days = 0
    current_start = None

    for dt, is_dd in in_drawdown.items():
        if is_dd and current_start is None:
            current_start = dt
        elif (not is_dd) and current_start is not None:
            days = (dt - current_start).days
            max_days = max(max_days, days)
            current_start = None

    # 如果最后仍在回撤中，计算到最后一天
    if current_start is not None:
        days = (nav.index[-1] - current_start).days
        max_days = max(max_days, days)

    return max_days


def calc_metrics(nav, position, asset_ret):
    total_ret = nav.iloc[-1] - 1
    days = (nav.index[-1] - nav.index[0]).days
    years = days / 365.25

    if years > 0:
        cagr = (1 + total_ret) ** (1 / years) - 1
    else:
        cagr = 0.0

    roll_max = nav.cummax()
    drawdown = (nav - roll_max) / roll_max
    max_dd = drawdown.min()

    calmar = cagr / abs(max_dd) if max_dd != 0 else np.nan

    strat_daily_ret = position * asset_ret
    vol = strat_daily_ret.std() * np.sqrt(252)
    sharpe = cagr / vol if vol and vol > 0 else np.nan

    turnover = position.diff().abs().sum()
    trade_count = int((position.diff().abs() > 1e-6).sum())
    avg_position = position.mean()
    time_in_market = avg_position * 100

    yearly_ret = (1 + strat_daily_ret).resample("Y").prod() - 1
    worst_year = yearly_ret.min() if len(yearly_ret) > 0 else np.nan

    recovery_days = max_recovery_days(nav)

    return {
        "cagr": cagr,
        "max_dd": max_dd,
        "calmar": calmar,
        "vol": vol,
        "sharpe": sharpe,
        "turnover": turnover,
        "trade_count": trade_count,
        "avg_position": avg_position,
        "time_in_market": time_in_market,
        "worst_year": worst_year,
        "recovery_days": recovery_days,
        "total_ret": total_ret,
    }


def score_v4(metrics, baseline_metrics):
    """
    综合评分：
    - 主指标仍以 Calmar 为核心。
    - 惩罚过高换手与过长修复周期。
    - 奖励最大回撤改善。
    """
    calmar = metrics["calmar"] if pd.notna(metrics["calmar"]) else -999
    dd_improvement = abs(baseline_metrics["max_dd"]) - abs(metrics["max_dd"])

    # 换手和修复期惩罚系数不要太大，避免“为了低换手牺牲太多收益”。
    turnover_penalty = 0.015 * metrics["turnover"]
    recovery_penalty = 0.0002 * metrics["recovery_days"]

    return calmar + dd_improvement * 2.0 - turnover_penalty - recovery_penalty


# ==================== 7. 绘图与输出 ====================

def plot_top10(df, baseline_nav, results):
    asset = V3_FIXED_PARAMS["asset"]

    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        figsize=(18, 12),
        gridspec_kw={"height_ratios": [3, 1]},
        sharex=True,
    )
    plt.subplots_adjust(hspace=0.05)

    buy_hold_nav = (1 + df[f"{asset}_ret"]).cumprod()

    ax1.plot(df.index, buy_hold_nav, label="Buy & Hold VOO", color="gray", alpha=0.55, linewidth=2)
    ax1.plot(df.index, baseline_nav, label="V3 Fixed Baseline", color="black", linewidth=2.5)

    colors = plt.cm.tab10(np.linspace(0, 1, 10))
    for i, r in enumerate(results[:10]):
        label = (
            f"V4 Top{i + 1} "
            f"Calmar {r['calmar']:.2f} "
            f"RWpos {r['risk_warning_pos']:.1f} "
            f"RWgap {r['risk_warning_gap_buy_pct']:.2f}"
        )
        lw = 3.0 if i == 0 else 1.4
        alpha = 1.0 if i == 0 else 0.80
        ax1.plot(df.index, r["nav"], label=label, color=colors[i], linewidth=lw, alpha=alpha)

    ax1.set_title("VOO V4 执行层参数搜索 Top10 vs V3 Baseline", fontsize=16, fontweight="bold")
    ax1.set_ylabel("Net Asset Value")
    ax1.set_yscale("log")
    ax1.legend(loc="upper left", fontsize=8)
    ax1.grid(True, alpha=0.4)

    best = results[0]
    ax2.plot(df.index, best["position"], label="Best V4 Position", color="#34495e", linewidth=1.8)
    ax2.set_ylim(0, 1.05)
    ax2.set_ylabel("Position")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.4)

    fig.savefig(TOP10_PIC_PATH, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_best_vs_baseline(df, baseline, best_result):
    asset = V3_FIXED_PARAMS["asset"]

    baseline_nav = baseline["nav"]
    baseline_pos = baseline["position"]

    best_nav = best_result["nav"]
    best_pos = best_result["position"]

    buy_hold_nav = (1 + df[f"{asset}_ret"]).cumprod()

    fig, axes = plt.subplots(
        4,
        1,
        figsize=(18, 16),
        gridspec_kw={"height_ratios": [3, 1, 1, 1]},
        sharex=True,
    )
    ax1, ax2, ax3, ax4 = axes

    ax1.plot(df.index, buy_hold_nav, label="Buy & Hold VOO", color="gray", alpha=0.55, linewidth=2)
    ax1.plot(df.index, baseline_nav, label="V3 Fixed Baseline", color="black", linewidth=2.5)
    ax1.plot(df.index, best_nav, label="Best V4 Execution", color="#e74c3c", linewidth=2.5)
    ax1.set_yscale("log")
    ax1.set_title("Best V4 Execution vs V3 Baseline", fontsize=16, fontweight="bold")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.4)

    ax2.plot(df.index, baseline_pos, label="V3 Position", color="black", linewidth=1.5)
    ax2.plot(df.index, best_pos, label="V4 Position", color="#e74c3c", linewidth=1.8)
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
    ax4.set_title("Risk Repair Reference: HYG vs MA60 + VIX")
    ax4.legend(loc="upper left")
    ax4_twin.legend(loc="upper right")
    ax4.grid(True, alpha=0.4)

    fig.savefig(BEST_PIC_PATH, dpi=200, bbox_inches="tight")
    plt.close(fig)


# ==================== 8. 主程序 ====================

def main():
    print("=== 🚀 VOO Hybrid V4 执行层参数搜索（Grid Search）===")

    raw_df = load_master_data()
    raw_df = filter_real_trading_days(raw_df, V3_FIXED_PARAMS["asset"])
    df = prepare_indicators(raw_df, V3_FIXED_PARAMS)

    # 为了避免早期指标不稳定，保留 MA200 形成后的数据。
    # 如果你希望完全复刻旧 V3，可以注释掉这行。
    df = df.iloc[220:].copy()

    baseline = run_v3_fixed_baseline(df, V3_FIXED_PARAMS)
    baseline_metrics = calc_metrics(
        nav=baseline["nav"],
        position=baseline["position"],
        asset_ret=df[f"{V3_FIXED_PARAMS['asset']}_ret"],
    )

    print("\n📌 V3 固定参数基线：")
    print(
        f"   MA{V3_FIXED_PARAMS['ma_len']} | "
        f"US10Y>{V3_FIXED_PARAMS['us10y_th']} | "
        f"VIX>{V3_FIXED_PARAMS['vix_th']} | "
        f"RSI<{V3_FIXED_PARAMS['rsi_th']} | "
        f"RiskPos {V3_FIXED_PARAMS['risk_pos']}"
    )
    print(
        f"   CAGR: {baseline_metrics['cagr'] * 100:.2f}% | "
        f"MaxDD: {baseline_metrics['max_dd'] * 100:.2f}% | "
        f"Calmar: {baseline_metrics['calmar']:.2f} | "
        f"Turnover: {baseline_metrics['turnover']:.2f} | "
        f"Trades: {baseline_metrics['trade_count']} | "
        f"AvgPos: {baseline_metrics['time_in_market']:.1f}%"
    )

    keys = list(V4_PARAM_GRID.keys())
    values = [V4_PARAM_GRID[k] for k in keys]
    total = int(np.prod([len(v) for v in values]))

    print(f"\n开始搜索 V4 执行层参数... 共 {total} 种组合\n")

    results = []

    for combo in product(*values):
        v4_params = dict(zip(keys, combo))

        res = run_v4_execution_strategy(df, V3_FIXED_PARAMS, v4_params)

        metrics = calc_metrics(
            nav=res["nav"],
            position=res["position"],
            asset_ret=df[f"{V3_FIXED_PARAMS['asset']}_ret"],
        )

        score = score_v4(metrics, baseline_metrics)

        row = dict(v4_params)
        row.update(metrics)
        row["score"] = score
        row["nav"] = res["nav"]
        row["position"] = res["position"]
        row["state_df"] = res["state_df"]

        results.append(row)

    # 主排序：综合 score；辅助看 Calmar。
    results = sorted(results, key=lambda x: (x["score"], x["calmar"]), reverse=True)

    print("🏆 Top 20 V4 执行层参数组合（按综合 Score 排序）:")
    print("-" * 180)
    print(
        f"{'排名':<4} "
        f"{'RWpos':<6} {'Panic':<6} {'RW天':<5} {'RWgap':<7} "
        f"{'RepairVIX':<10} {'RepairGap':<10} {'RiskOnGap':<10} "
        f"{'年化':<8} {'回撤':<8} {'Calmar':<8} {'Score':<8} "
        f"{'换手':<8} {'交易':<6} {'持仓%':<7} {'最差年':<8} {'修复天':<8}"
    )
    print("-" * 180)

    for i, r in enumerate(results[:20], 1):
        print(
            f"{i:<4} "
            f"{r['risk_warning_pos']:<6.2f} "
            f"{r['panic_reversal_pos']:<6.2f} "
            f"{r['risk_warning_confirm_days']:<5} "
            f"{r['risk_warning_gap_buy_pct']:<7.2f} "
            f"{r['risk_repair_vix_th']:<10} "
            f"{r['risk_repair_gap_buy_pct']:<10.2f} "
            f"{r['risk_on_gap_buy_pct']:<10.2f} "
            f"{r['cagr'] * 100:>6.2f}% "
            f"{r['max_dd'] * 100:>7.2f}% "
            f"{r['calmar']:>7.2f} "
            f"{r['score']:>7.2f} "
            f"{r['turnover']:>7.2f} "
            f"{r['trade_count']:>6} "
            f"{r['time_in_market']:>6.1f}% "
            f"{r['worst_year'] * 100:>7.2f}% "
            f"{r['recovery_days']:>8}"
        )

    best = results[0]

    print("\n🎯 V4 最优执行层参数：")
    for k in keys:
        print(f"   {k}: {best[k]}")

    print(
        f"\n📈 V4 Best: CAGR {best['cagr'] * 100:.2f}% | "
        f"MaxDD {best['max_dd'] * 100:.2f}% | "
        f"Calmar {best['calmar']:.2f} | "
        f"Score {best['score']:.2f} | "
        f"Turnover {best['turnover']:.2f} | "
        f"Trades {best['trade_count']} | "
        f"AvgPos {best['time_in_market']:.1f}%"
    )

    print(
        f"📊 V3 Base: CAGR {baseline_metrics['cagr'] * 100:.2f}% | "
        f"MaxDD {baseline_metrics['max_dd'] * 100:.2f}% | "
        f"Calmar {baseline_metrics['calmar']:.2f} | "
        f"Turnover {baseline_metrics['turnover']:.2f} | "
        f"Trades {baseline_metrics['trade_count']} | "
        f"AvgPos {baseline_metrics['time_in_market']:.1f}%"
    )

    # 保存 CSV：不把 nav / position / state_df 这些大对象写入 CSV。
    csv_rows = []
    for r in results:
        csv_row = {k: v for k, v in r.items() if k not in ["nav", "position", "state_df"]}
        csv_rows.append(csv_row)

    result_df = pd.DataFrame(csv_rows)
    result_df.to_csv(RESULT_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"\n💾 V4 全部搜索结果已保存: {RESULT_CSV_PATH.name}")

    best_json = {
        "v3_fixed_params": V3_FIXED_PARAMS,
        "v3_baseline_metrics": {
            k: float(v) if isinstance(v, (np.floating, float)) else int(v) if isinstance(v, (np.integer, int)) else v
            for k, v in baseline_metrics.items()
        },
        "v4_best_params": {k: best[k] for k in keys},
        "v4_best_metrics": {
            k: float(best[k]) if isinstance(best[k], (np.floating, float)) else int(best[k]) if isinstance(best[k], (
            np.integer, int)) else best[k]
            for k in [
                "cagr",
                "max_dd",
                "calmar",
                "vol",
                "sharpe",
                "turnover",
                "trade_count",
                "avg_position",
                "time_in_market",
                "worst_year",
                "recovery_days",
                "total_ret",
                "score",
            ]
        },
        "notes": "V4 fixes V3 core params and grid-searches execution-layer parameters only.",
    }

    BEST_JSON_PATH.write_text(json.dumps(best_json, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"💾 V4 最优参数 JSON 已保存: {BEST_JSON_PATH.name}")

    plot_top10(df, baseline["nav"], results)
    print(f"🖼️ Top10 对比图已保存: {TOP10_PIC_PATH.name}")

    best_result = {
        "nav": best["nav"],
        "position": best["position"],
        "state_df": best["state_df"],
    }
    plot_best_vs_baseline(df, baseline, best_result)
    print(f"🖼️ Best vs V3 对比图已保存: {BEST_PIC_PATH.name}")

    print("\n✅ V4 执行层参数搜索完成。")


if __name__ == "__main__":
    main()
