#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VOO Hybrid V4.2.1.1 执行层参数回测 / Grid Search

本版本基于 VIX regime 验证结果，对 V4.1 做三项核心修正：

1) 收敛 VIX 状态阈值
   - 使用 15 / 20 / 30 / 45 作为 VIX 分层边界。
   - VIX 15–20 只作为轻度预警，且必须 VIX > VIX_MA60 才触发 Risk-Warning。
   - VIX 20–30 视为尾部风险较高区域。
   - VIX 30 以上不单独触发 Crash，只有价格跌破关键均线 + VIX>30 才进入 Crash。
   - VOO Panic Drop 固定为 -2.0%，用于验证 V4.2.1 收敛参数的稳定性。

2) 加入交易摩擦成本
   - 默认 TRANSACTION_COST_RATE = 0.0002，即每 1.0 仓位变动扣万分之二。
   - V4 是分批执行模型，如果不扣摩擦成本，会高估高频小步调仓的参数。

3) 修复 Risk-Warning 连续天数逻辑，并调整排序标准
   - Risk-Warning 累加连续天数。
   - Risk-Off / Panic-Reversal / Crash 期间挂起计数，不立即清零。
   - Risk-On / Dip-Buy / Trend-Weak 才清零。
   - 主排序使用 Calmar，Score 仅作为辅助参考；换手率使用年化换手率。


# todo:26-06-08: V4.2.1 在 V4.2 基础上进一步固定 panic_drop_pct=0.020，并收窄执行层搜索空间。
"""

"""
    本方案更偏向防守，回测后的最终参数化方案如下：
    
    参数名 (Parameter),参数值 (Value),说明 (Description)
    1. 状态识别边界 (风控雷达),,决定何时拉响警报的阈值
    vix_warning_low,15,进入预警观察区的下限
    vix_warning_high,20,预警转实质风险的上限边界
    vix_risk_low,20,尾部风险激增区下限
    vix_risk_high,30,恐慌极值区上限
    vix_crash_th,30,配合均线破位判定崩盘的 VIX 阈值
    panic_drop_pct,0.02,恐慌跌幅阈值：单日跌幅 2.0% 触发独立狙击抄底
    2. 目标仓位设定 (理论底线),,在特定状态下应保持的理论仓位目标
    risk_warning_pos,0.60,预警缓冲仓：VIX 15-20 时的防御仓位
    panic_reversal_pos,0.40,暴跌狙击仓：触发高胜率暴跌抄底时的试探仓位
    risk_pos,0.30,绝对底仓：全面 Risk-Off 和 Crash 时的最低防守仓位
    risk_on_pos,1.00,满仓进攻：趋势完好、警报解除时的目标仓位
    3. 执行节奏控制 (调仓微操),,控制实际买卖步长，熨平波动
    risk_warning_confirm_days,3,预警确认期：连续处于 Risk-Warning 状态 3 天才允许操作，过滤短噪音
    risk_warning_gap_buy_pct,0.25,预警区补仓速度：在预警区补仓时，单次只买回目标缺口的 1/4 (25%)，极其谨慎
    risk_repair_vix_th,20,风险解除阈值：VIX 降至 20 以下且叠加 HYG 修复，才视为风险初步解除
    risk_repair_gap_buy_pct,0.67,风险修复补仓速度：风险初步解除后，单次买回目标缺口的 2/3 (67%)
    risk_on_gap_buy_pct,1.00,满仓恢复速度：彻底重回 Risk-On 时，一步到位 (100%) 买满
    panic_gap_buy_pct,0.50,暴跌狙击补仓速度：执行暴跌狙击时，单次推入目标缺口的 1/2 (50%)
"""

import json
from pathlib import Path
from itertools import product

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ============================================================
# 1. 基础配置
# ============================================================

plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False

CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv"
)

OUTPUT_DIR = Path(__file__).parent
RESULT_CSV_PATH = OUTPUT_DIR / "voo_v4_2_1_gridsearch_results.csv"
STATE_SUMMARY_CSV_PATH = OUTPUT_DIR / "voo_v4_2_1_state_convergence_summary.csv"
BEST_JSON_PATH = OUTPUT_DIR / "voo_v4_2_1_best_params.json"
TOP10_PIC_PATH = OUTPUT_DIR / "voo_v4_2_1_top10_nav_compare.png"
BEST_PIC_PATH = OUTPUT_DIR / "voo_v4_2_1_best_vs_v3_baseline.png"

# todo:26-06-08: 加入交易摩擦成本，默认万分之二。
TRANSACTION_COST_RATE = 0.0002

# 主排序指标：建议用 Calmar；Score 仅辅助。
PRIMARY_SORT = "calmar"

# todo:26-06-08: V4.2.1 仅做收敛验证，不强制过滤；用于观察是否接近 V3 收益。
# 后续 V4.3 可启用硬性门槛：CAGR >= V3_CAGR - 0.5%。
CAGR_TOLERANCE_TO_V3 = 0.005


# ============================================================
# 2. V3 已确认核心参数
# ============================================================
# 你已经通过 V3 回测确认：
# MA100 | US10Y>0.15 | VIX>45 | RSI<30 | Risk-Off=0.3
# CAGR 12.91% | MaxDD -14.08% | Calmar 0.92

V3_FIXED_PARAMS = {
    "asset": "VOO",

    # V3 已验证核心参数，不在 V4.2.1 中重新搜索，避免过拟合。
    "ma_len": 100,
    "us10y_th": 0.15,
    "vix_th": 45,
    "rsi_th": 30,
    "risk_pos": 0.30,

    # V4 / V4.2.1 固定基础参数
    "risk_on_pos": 1.00,
    "crash_pos": 0.30,

    # RSI 区间
    "panic_rsi_low": 35,
    "panic_rsi_high": 45,
}


# ============================================================
# 3. V4.2.1 状态参数搜索空间
# ============================================================
# todo:26-06-08:
# 根据 VIX 分桶验证结果，状态参数不再大范围搜索：
# - 15 / 20 / 30 / 45 保持固定解释框架。
# - Panic Drop 固定为 -2.0%，不再测试 -1.5% / -2.5%。

STATE_PARAM_GRID = {
    "vix_warning_low": [15],
    "vix_warning_high": [20],
    "vix_risk_low": [20],
    "vix_risk_high": [30],
    "vix_crash_th": [30],
    "vix_extreme_th": [45],

    # todo:26-06-08: V4.2.1 收敛测试
    # 根据 VIX regime 与 Panic Drop 验证结果，固定 VOO Panic Drop 为 -2.0%。
    # V4.2.1 不再搜索 0.015 / 0.025，用来验证 0.020 作为中心参数是否稳定。
    "panic_drop_pct": [0.020],
}


# ============================================================
# 4. V4.2.1 执行层参数搜索空间
# ============================================================

V4_EXECUTION_GRID = {
    # VOO Risk-Warning 理论观察仓位。
    # todo:26-06-08: 15-20 区间极端风险介于平静与风险之间，保留 0.60 / 0.70，放弃 0.80。
    "risk_warning_pos": [0.60, 0.70],

    # Panic-Reversal 目标仓位。
    "panic_reversal_pos": [0.40, 0.50, 0.60],

    # Risk-Warning 连续几天后才允许补仓。
    "risk_warning_confirm_days": [2, 3],

    # Risk-Warning 确认后，只买回目标缺口的一部分。
    # todo:26-06-08: 去掉 0.20，避免恢复太慢、长期低仓位拖累收益。
    "risk_warning_gap_buy_pct": [0.25, 0.33, 0.50],

    # 风险修复条件：HYG > MA60 且 VIX < 20。
    # todo:26-06-08: 根据 VIX 分桶，20 是预警与风险的边界，因此固定为 20。
    "risk_repair_vix_th": [20],

    # Risk-Repair 后买回目标缺口的比例。
    # todo:26-06-08: 去掉 0.33，保留较快修复的 0.50 / 0.67。
    "risk_repair_gap_buy_pct": [0.50, 0.67],

    # Risk-On 后恢复目标仓位的速度。
    # todo:26-06-08: 去掉 0.75，Risk-On 确认后直接恢复理论目标。
    "risk_on_gap_buy_pct": [1.00],

    # Panic-Reversal 下恢复目标仓位的速度。
    "panic_gap_buy_pct": [0.50, 1.00],
}


# ============================================================
# 5. 工具函数
# ============================================================

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
    """读取主数据。"""
    if not CSV_FILE_PATH.exists():
        raise FileNotFoundError(f"找不到主数据文件: {CSV_FILE_PATH}")

    df = pd.read_csv(CSV_FILE_PATH)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()

    return df


def filter_real_trading_days(df, asset):
    """
    过滤真实交易日，避免周末 ffill 导致单日涨跌幅失真。
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


def compute_nav_with_cost(position, asset_ret, cost_rate=TRANSACTION_COST_RATE):
    """
    计算扣除交易摩擦成本后的净值。
    turnover_daily = 仓位变化绝对值。
    daily_net_ret = position * asset_ret - turnover_daily * cost_rate。
    """
    position = position.fillna(0.0)
    turnover_daily = position.diff().abs().fillna(0.0)
    daily_net_ret = position * asset_ret.fillna(0.0) - turnover_daily * cost_rate
    nav = (1 + daily_net_ret).cumprod()

    return nav, daily_net_ret, turnover_daily


def move_towards(current_pos, target_pos, gap_pct):
    """
    从 current_pos 向 target_pos 移动 gap_pct 的缺口。
    - 加仓方向：按 gap_pct 分批靠近。
    - 降仓方向：默认一步降到目标，避免防守拖延。
    """
    if target_pos > current_pos:
        return current_pos + (target_pos - current_pos) * gap_pct
    if target_pos < current_pos:
        return target_pos
    return current_pos


# ============================================================
# 6. 状态识别
# ============================================================

def build_daily_state_table(df, p):
    """
    生成每日市场状态表。

    V4.2.1 状态解释：
    - VIX 15–20：轻度预警；必须 VIX > VIX_MA60 才触发 Risk-Warning。
    - VIX 20–30：尾部风险较高，进入 Risk-Off 候选。
    - VIX 30–45：恐慌观察区，不单独触发 Crash。
    - VIX >45：极端恐慌，样本少，不单独触发满仓；配合价格/RSI/跌幅判断。
    - Crash：必须价格跌破关键均线 + VIX>30。
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
    vix_above_ma60 = vix > df["VIX_MA60"]

    vix_warning_zone = (
        (vix >= p["vix_warning_low"])
        & (vix < p["vix_warning_high"])
    )

    # todo:26-06-08: VIX 15-20 只有在 VIX > VIX_MA60 时才作为 Risk-Warning。
    vix_warning = vix_warning_zone & vix_above_ma60

    # todo:26-06-08: VIX 20-30 是尾部风险最高区域，作为 Risk-Off 候选。
    vix_tail_risk_zone = (
        (vix >= p["vix_risk_low"])
        & (vix < p["vix_risk_high"])
    )

    # VIX 30 以上作为 panic 观察区，不直接 Crash。
    vix_panic_zone = (
        (vix >= p["vix_risk_high"])
        & (vix < p["vix_extreme_th"])
    )

    vix_extreme = vix >= p["vix_extreme_th"]

    # V3 旧风险信号仍保留，避免完全推翻已验证的 V3 框架。
    vix_legacy_risk = (vix > p["vix_th"]) | (vix > df["VIX_MA60"] * 1.8)

    # Risk-Off：VIX 20-30 + VIX 上行 / HYG 弱 / V3 旧风险 / 利率风险。
    risk_off = (
        us10y_rising
        | hyg_divergence
        | (vix_tail_risk_zone & vix_above_ma60)
        | (credit_weak & (vix >= p["vix_risk_low"]))
        | vix_legacy_risk
    )

    # Risk-Warning：HYG 弱，或 VIX 15-20 且高于 MA60；但不能已进入 Risk-Off。
    risk_warning = (credit_weak | vix_warning) & (~risk_off)

    # Panic-Reversal：可以来自 Risk-Off，也可以来自 VIX 30+ 的恐慌区；
    # 但必须同时满足单日跌幅与 RSI 区间。
    panic_reversal = (
        (risk_off | vix_panic_zone | vix_extreme)
        & (daily_return <= -p["panic_drop_pct"])
        & rsi.between(p["panic_rsi_low"], p["panic_rsi_high"])
    )

    # Crash：不能由 VIX 单独触发，必须叠加价格跌破关键均线。
    crash = (
        ((close < ma200) & (vix >= p["vix_crash_th"]))
        | ((close < ma100) & (vix >= p["vix_crash_th"]))
    )

    dip_buy = (
        (rsi < p["rsi_th"])
        & (close > open_price)
        & (df["VIX_close"] < df["VIX_close"].shift(1))
        & (~risk_off)
    )

    risk_repair = (
        (df["HYG_close"] > df["HYG_MA60"])
        & (df["VIX_close"] < p.get("risk_repair_vix_th", 20))
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
            "vix_tail_risk_zone": vix_tail_risk_zone,
            "vix_panic_zone": vix_panic_zone,
            "vix_extreme": vix_extreme,
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

    # todo:26-06-08:
    # Risk-Warning 累加；
    # Risk-Off / Panic-Reversal / Crash 挂起，不清零；
    # Risk-On / Dip-Buy / Trend-Weak 清零。
    streak = []
    count = 0
    for state in state_df["state"]:
        if state == "Risk-Warning":
            count += 1
        elif state in ["Risk-Off", "Panic-Reversal", "Crash"]:
            count = count
        else:
            count = 0
        streak.append(count)

    state_df["risk_warning_streak"] = streak

    return state_df


def model_target_position_for_state(state, p):
    """市场状态对应的理论目标仓位。"""
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


# ============================================================
# 7. V3 / V4.2.1 策略回测
# ============================================================

def run_v3_fixed_baseline(df, p):
    """
    V3 固定参数基线。
    逻辑复刻 V3 回测核心：
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

    nav, daily_net_ret, turnover_daily = compute_nav_with_cost(
        position=position,
        asset_ret=df[f"{asset}_ret"],
        cost_rate=TRANSACTION_COST_RATE,
    )

    return {
        "nav": nav,
        "position": position,
        "daily_net_ret": daily_net_ret,
        "turnover_daily": turnover_daily,
    }


def run_v4_2_1_execution_strategy(df, base_params, state_params, execution_params):
    """
    V4.2.1 执行层回测。
    """
    p = dict(base_params)
    p.update(state_params)
    p.update(execution_params)

    asset = p["asset"]
    state_df = build_daily_state_table(df, p)

    current_pos = p["risk_pos"]
    position_for_return = []
    model_target_signal = []
    execution_target_signal = []
    execution_notes = []

    # 逐日循环处理路径依赖：
    # 当天持仓用于当天收益；
    # 当天收盘后的状态决定下一交易日持仓。
    for _, row in state_df.iterrows():
        position_for_return.append(current_pos)

        state = row["state"]
        model_target = model_target_position_for_state(state, p)
        new_pos = current_pos
        note = ""

        if state == "Crash":
            new_pos = min(current_pos, model_target)
            note = "Crash: only reduce / no buy"

        elif state == "Risk-Off":
            new_pos = min(current_pos, model_target)
            note = "Risk-Off: only reduce / no buy"

        elif state == "Panic-Reversal":
            if model_target > current_pos:
                new_pos = move_towards(
                    current_pos=current_pos,
                    target_pos=model_target,
                    gap_pct=p["panic_gap_buy_pct"],
                )
            else:
                new_pos = model_target
            note = "Panic-Reversal: controlled counter-trend buy"

        elif state == "Risk-Warning":
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
                    note = "Risk-Warning observe: no buy"

        elif state == "Dip-Buy":
            new_pos = move_towards(
                current_pos=current_pos,
                target_pos=model_target,
                gap_pct=p["risk_on_gap_buy_pct"],
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
            new_pos = min(current_pos, model_target)
            note = "Trend-Weak: defensive / no buy"

        new_pos = float(np.clip(new_pos, 0.0, 1.0))

        model_target_signal.append(model_target)
        execution_target_signal.append(new_pos)
        execution_notes.append(note)

        current_pos = new_pos

    position = pd.Series(position_for_return, index=df.index).fillna(p["risk_pos"])

    nav, daily_net_ret, turnover_daily = compute_nav_with_cost(
        position=position,
        asset_ret=df[f"{asset}_ret"],
        cost_rate=TRANSACTION_COST_RATE,
    )

    state_df = state_df.copy()
    state_df["position"] = position
    state_df["model_target_position"] = pd.Series(model_target_signal, index=df.index)
    state_df["execution_target_position"] = pd.Series(execution_target_signal, index=df.index)
    state_df["execution_note"] = execution_notes
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
# 8. 绩效指标
# ============================================================

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
            max_days = max(max_days, (dt - current_start).days)
            current_start = None

    if current_start is not None:
        max_days = max(max_days, (nav.index[-1] - current_start).days)

    return max_days


def calc_metrics(nav, position, daily_net_ret, turnover_daily):
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

    vol = daily_net_ret.std() * np.sqrt(252)
    sharpe = cagr / vol if vol and vol > 0 else np.nan

    total_turnover = turnover_daily.sum()
    annual_turnover = total_turnover / years if years > 0 else np.nan

    trade_count = int((turnover_daily > 1e-8).sum())
    avg_position = position.mean()
    time_in_market = avg_position * 100

    yearly_ret = (1 + daily_net_ret).resample("YE").prod() - 1
    worst_year = yearly_ret.min() if len(yearly_ret) > 0 else np.nan

    recovery_days = max_recovery_days(nav)

    return {
        "cagr": cagr,
        "max_dd": max_dd,
        "calmar": calmar,
        "vol": vol,
        "sharpe": sharpe,
        "total_turnover": total_turnover,
        "annual_turnover": annual_turnover,
        "trade_count": trade_count,
        "avg_position": avg_position,
        "time_in_market": time_in_market,
        "worst_year": worst_year,
        "recovery_days": recovery_days,
        "total_ret": total_ret,
    }


def score_v4_2_1_1(metrics, baseline_metrics):
    """
    辅助评分：主排序仍看 Calmar。
    使用年化换手率而不是总换手，避免回测区间长度影响惩罚强度。
    """
    calmar = metrics["calmar"] if pd.notna(metrics["calmar"]) else -999
    dd_improvement = abs(baseline_metrics["max_dd"]) - abs(metrics["max_dd"])

    annual_turnover = metrics["annual_turnover"] if pd.notna(metrics["annual_turnover"]) else 999
    recovery_years = metrics["recovery_days"] / 365.25

    return calmar + dd_improvement * 2.0 - 0.03 * annual_turnover - 0.02 * recovery_years


# ============================================================
# 9. 绘图与保存
# ============================================================

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

    buy_hold_nav, _, _ = compute_nav_with_cost(
        position=pd.Series(1.0, index=df.index),
        asset_ret=df[f"{asset}_ret"],
        cost_rate=TRANSACTION_COST_RATE,
    )

    ax1.plot(df.index, buy_hold_nav, label="Buy & Hold VOO", color="gray", alpha=0.55, linewidth=2)
    ax1.plot(df.index, baseline_nav, label="V3 Fixed Baseline", color="black", linewidth=2.5)

    colors = plt.cm.tab10(np.linspace(0, 1, 10))
    for i, r in enumerate(results[:10]):
        label = (
            f"V4.2.1 Top{i + 1} "
            f"Calmar {r['calmar']:.2f} "
            f"PanicDrop {r['panic_drop_pct']:.3f} "
            f"RWgap {r['risk_warning_gap_buy_pct']:.2f}"
        )
        lw = 3.0 if i == 0 else 1.4
        alpha = 1.0 if i == 0 else 0.80
        ax1.plot(df.index, r["nav"], label=label, color=colors[i], linewidth=lw, alpha=alpha)

    ax1.set_title("VOO V4.2.1 执行层参数搜索 Top10 vs V3 Baseline", fontsize=16, fontweight="bold")
    ax1.set_ylabel("Net Asset Value")
    ax1.set_yscale("log")
    ax1.legend(loc="upper left", fontsize=8)
    ax1.grid(True, alpha=0.4)

    best = results[0]
    ax2.plot(df.index, best["position"], label="Best V4.2.1 Position", color="#34495e", linewidth=1.8)
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
    state_df = best_result["state_df"]

    buy_hold_nav, _, _ = compute_nav_with_cost(
        position=pd.Series(1.0, index=df.index),
        asset_ret=df[f"{asset}_ret"],
        cost_rate=TRANSACTION_COST_RATE,
    )

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
    ax1.plot(df.index, best_nav, label="Best V4.2.1 Execution", color="#e74c3c", linewidth=2.5)
    ax1.set_yscale("log")
    ax1.set_title("Best V4.2.1 Execution vs V3 Baseline", fontsize=16, fontweight="bold")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.4)

    ax2.plot(df.index, baseline_pos, label="V3 Position", color="black", linewidth=1.5)
    ax2.plot(df.index, best_pos, label="V4.2.1 Position", color="#e74c3c", linewidth=1.8)
    ax2.set_ylim(0, 1.05)
    ax2.set_ylabel("Position")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.4)

    ax3.plot(df.index, df[f"{asset}_close"], label="VOO Price", color="#2c3e50")
    ax3.plot(df.index, df[f"{asset}_MA"], label=f"MA{V3_FIXED_PARAMS['ma_len']}", color="#f39c12", linestyle="--")
    ax3.plot(df.index, df[f"{asset}_MA200"], label="MA200", color="#c0392b", linestyle=":")
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


# ============================================================
# 10. 主程序
# ============================================================

def main():
    print("=== 🚀 VOO Hybrid V4.2.1 执行层参数搜索（Grid Search）===")
    print(f"交易摩擦成本: {TRANSACTION_COST_RATE:.4%}")

    raw_df = load_master_data()
    raw_df = filter_real_trading_days(raw_df, V3_FIXED_PARAMS["asset"])
    df = prepare_indicators(raw_df, V3_FIXED_PARAMS)

    # 保留 MA200 形成后的数据，避免早期指标不稳定。
    # 如果你希望完全复刻旧 V3，可注释掉这一行。
    df = df.iloc[220:].copy()

    baseline = run_v3_fixed_baseline(df, V3_FIXED_PARAMS)
    baseline_metrics = calc_metrics(
        nav=baseline["nav"],
        position=baseline["position"],
        daily_net_ret=baseline["daily_net_ret"],
        turnover_daily=baseline["turnover_daily"],
    )

    print("\n📌 V3 固定参数基线（已扣交易摩擦成本）:")
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
        f"AnnualTurnover: {baseline_metrics['annual_turnover']:.2f} | "
        f"Trades: {baseline_metrics['trade_count']} | "
        f"AvgPos: {baseline_metrics['time_in_market']:.1f}%"
    )

    state_keys = list(STATE_PARAM_GRID.keys())
    state_values = [STATE_PARAM_GRID[k] for k in state_keys]

    execution_keys = list(V4_EXECUTION_GRID.keys())
    execution_values = [V4_EXECUTION_GRID[k] for k in execution_keys]

    total = int(np.prod([len(v) for v in state_values]) * np.prod([len(v) for v in execution_values]))
    print(f"\n开始搜索 V4.2.1 状态 + 执行层参数... 共 {total} 种组合\n")

    results = []

    for state_combo in product(*state_values):
        state_params = dict(zip(state_keys, state_combo))

        for execution_combo in product(*execution_values):
            execution_params = dict(zip(execution_keys, execution_combo))

            res = run_v4_2_1_execution_strategy(
                df=df,
                base_params=V3_FIXED_PARAMS,
                state_params=state_params,
                execution_params=execution_params,
            )

            metrics = calc_metrics(
                nav=res["nav"],
                position=res["position"],
                daily_net_ret=res["daily_net_ret"],
                turnover_daily=res["turnover_daily"],
            )

            score = score_v4_2_1_1(metrics, baseline_metrics)

            row = {}
            row.update(state_params)
            row.update(execution_params)
            row.update(metrics)
            row["score"] = score
            row["nav"] = res["nav"]
            row["position"] = res["position"]
            row["state_df"] = res["state_df"]

            results.append(row)

    if PRIMARY_SORT == "calmar":
        results = sorted(results, key=lambda x: (x["calmar"], x["score"], x["cagr"]), reverse=True)
    else:
        results = sorted(results, key=lambda x: (x["score"], x["calmar"], x["cagr"]), reverse=True)

    print("🏆 Top 20 V4.2.1 参数组合（主排序：Calmar；Score 辅助）:")
    print("-" * 210)
    print(
        f"{'排名':<4} "
        f"{'PanicDrop':<10} {'RWpos':<6} {'PanicPos':<9} "
        f"{'RW天':<5} {'RWgap':<7} {'RepairVIX':<10} {'RepairGap':<10} "
        f"{'RiskOnGap':<10} {'PanicGap':<10} "
        f"{'年化':<8} {'回撤':<8} {'Calmar':<8} {'Score':<8} "
        f"{'年换手':<8} {'交易':<6} {'持仓%':<7} {'最差年':<8} {'修复天':<8}"
    )
    print("-" * 210)

    for i, r in enumerate(results[:20], 1):
        print(
            f"{i:<4} "
            f"{r['panic_drop_pct']:<10.3f} "
            f"{r['risk_warning_pos']:<6.2f} "
            f"{r['panic_reversal_pos']:<9.2f} "
            f"{r['risk_warning_confirm_days']:<5} "
            f"{r['risk_warning_gap_buy_pct']:<7.2f} "
            f"{r['risk_repair_vix_th']:<10} "
            f"{r['risk_repair_gap_buy_pct']:<10.2f} "
            f"{r['risk_on_gap_buy_pct']:<10.2f} "
            f"{r['panic_gap_buy_pct']:<10.2f} "
            f"{r['cagr'] * 100:>6.2f}% "
            f"{r['max_dd'] * 100:>7.2f}% "
            f"{r['calmar']:>7.2f} "
            f"{r['score']:>7.2f} "
            f"{r['annual_turnover']:>7.2f} "
            f"{r['trade_count']:>6} "
            f"{r['time_in_market']:>6.1f}% "
            f"{r['worst_year'] * 100:>7.2f}% "
            f"{r['recovery_days']:>8}"
        )

    best = results[0]

    print("\n🎯 V4.2.1 最优参数：")
    for k in state_keys + execution_keys:
        print(f"   {k}: {best[k]}")

    print(
        f"\n📈 V4.2.1 Best: CAGR {best['cagr'] * 100:.2f}% | "
        f"MaxDD {best['max_dd'] * 100:.2f}% | "
        f"Calmar {best['calmar']:.2f} | "
        f"Score {best['score']:.2f} | "
        f"AnnualTurnover {best['annual_turnover']:.2f} | "
        f"Trades {best['trade_count']} | "
        f"AvgPos {best['time_in_market']:.1f}%"
    )

    print(
        f"📊 V3 Base: CAGR {baseline_metrics['cagr'] * 100:.2f}% | "
        f"MaxDD {baseline_metrics['max_dd'] * 100:.2f}% | "
        f"Calmar {baseline_metrics['calmar']:.2f} | "
        f"AnnualTurnover {baseline_metrics['annual_turnover']:.2f} | "
        f"Trades {baseline_metrics['trade_count']} | "
        f"AvgPos {baseline_metrics['time_in_market']:.1f}%"
    )

    cagr_floor = baseline_metrics["cagr"] - CAGR_TOLERANCE_TO_V3
    print(
        f"🧪 V4.2.1 收益观察线: CAGR >= {cagr_floor * 100:.2f}% "
        f"即 V3 CAGR - {CAGR_TOLERANCE_TO_V3 * 100:.2f}%"
    )
    if best["cagr"] >= cagr_floor:
        print("✅ Best V4.2.1 满足收益观察线。")
    else:
        print("⚠️ Best V4.2.1 未满足收益观察线，说明该分支仍偏防守，不宜直接替代 V3。")

    # 保存全部结果 CSV，不写入大对象。
    csv_rows = []
    for r in results:
        csv_row = {
            k: v for k, v in r.items()
            if k not in ["nav", "position", "state_df"]
        }
        csv_rows.append(csv_row)

    result_df = pd.DataFrame(csv_rows)
    result_df.to_csv(RESULT_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"\n💾 V4.2.1 全部搜索结果已保存: {RESULT_CSV_PATH.name}")

    # 状态参数敏感性摘要
    group_cols = state_keys
    summary_df = (
        result_df
        .groupby(group_cols, dropna=False)
        .agg(
            count=("calmar", "count"),
            median_calmar=("calmar", "median"),
            mean_calmar=("calmar", "mean"),
            max_calmar=("calmar", "max"),
            median_cagr=("cagr", "median"),
            median_max_dd=("max_dd", "median"),
            median_annual_turnover=("annual_turnover", "median"),
        )
        .reset_index()
        .sort_values(["median_calmar", "max_calmar"], ascending=False)
    )
    summary_df.to_csv(STATE_SUMMARY_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"💾 状态参数收敛摘要已保存: {STATE_SUMMARY_CSV_PATH.name}")

    best_json = {
        "transaction_cost_rate": TRANSACTION_COST_RATE,
        "v3_fixed_params": V3_FIXED_PARAMS,
        "v3_baseline_metrics": {
            k: float(v) if isinstance(v, (np.floating, float)) else int(v) if isinstance(v, (np.integer, int)) else v
            for k, v in baseline_metrics.items()
        },
        "v4_2_1_best_params": {k: best[k] for k in state_keys + execution_keys},
        "v4_2_1_best_metrics": {
            k: float(best[k]) if isinstance(best[k], (np.floating, float)) else int(best[k]) if isinstance(best[k], (np.integer, int)) else best[k]
            for k in [
                "cagr",
                "max_dd",
                "calmar",
                "vol",
                "sharpe",
                "total_turnover",
                "annual_turnover",
                "trade_count",
                "avg_position",
                "time_in_market",
                "worst_year",
                "recovery_days",
                "total_ret",
                "score",
            ]
        },
        "notes": [
            "V4.2.1 keeps V3 core parameters fixed.",
            "V4.2.1 narrows VIX state thresholds according to regime validation.",
            "V4.2.1 uses transaction costs and annualized turnover.",
            "Primary sort is Calmar; score is auxiliary.",
        ],
    }

    BEST_JSON_PATH.write_text(json.dumps(best_json, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"💾 V4.2.1 最优参数 JSON 已保存: {BEST_JSON_PATH.name}")

    plot_top10(df, baseline["nav"], results)
    print(f"🖼️ Top10 对比图已保存: {TOP10_PIC_PATH.name}")

    best_result = {
        "nav": best["nav"],
        "position": best["position"],
        "state_df": best["state_df"],
    }
    plot_best_vs_baseline(df, baseline, best_result)
    print(f"🖼️ Best vs V3 对比图已保存: {BEST_PIC_PATH.name}")

    print("\n✅ V4.2.1 执行层参数搜索完成。")


if __name__ == "__main__":
    main()
