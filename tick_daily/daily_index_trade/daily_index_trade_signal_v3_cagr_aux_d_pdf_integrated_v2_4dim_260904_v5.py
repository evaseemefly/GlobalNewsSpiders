"""
VOO / QQQ 四维风险确认执行层（260904 v5）。

设计原则：
1. 原 v3 / v3_cagr 模型继续作为风险雷达，不修改原文件和原始分类函数；
2. 价格趋势、宏观利率、信用 HYG、波动/广度四个维度共同决定实盘动作；
3. 显示关键指标的前值、现值与单日变化，并识别连续边际改善/恶化；
4. Level 2 需要连续两日确认，降级也需要连续两日，防止阈值附近反复交易；
5. Level 1 只暂停加仓，Level 2 首次仅执行一档减仓，Level 3 才快速防守。

本文件通过组合方式复用旧版的行情清洗、原始信号和绘图能力。旧版文件不会被修改，
新版产物写入 output/trade_msg/4dim_260904_v5，避免覆盖旧报告。
"""

from __future__ import annotations

import importlib.util
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages


VERSION_TAG = "4dim_260904_v5"
LEGACY_PATH = Path(__file__).with_name(
    "daily_index_trade_signal_v3_cagr_aux_d_pdf_integrated_v2.py"
)


def _load_legacy_module():
    spec = importlib.util.spec_from_file_location("daily_index_trade_v3_legacy", LEGACY_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载旧版量化脚本: {LEGACY_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


legacy = _load_legacy_module()

# 新版所有产物进入独立目录，不覆盖旧版同日文件。
OUTPUT_PATH = legacy.OUTPUT_PATH / VERSION_TAG
FIGURES_PATH = OUTPUT_PATH / "figures"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
FIGURES_PATH.mkdir(parents=True, exist_ok=True)
legacy.OUTPUT_PATH = OUTPUT_PATH
legacy.FIGURES_PATH = FIGURES_PATH

# 复用原资产参数，并只在当前进程的副本中增加执行层参数。
ASSET_CONFIG = {asset: dict(params) for asset, params in legacy.ASSET_CONFIG.items()}
for _asset, _params in ASSET_CONFIG.items():
    _params.update(
        {
            "four_dim_enabled": True,
            "four_dim_confirm_days": 2,
            "four_dim_recovery_days": 2,
            "four_dim_rate_delta_epsilon": 0.005,
            "four_dim_rate_red_margin": 0.10,
            "four_dim_hyg_light_pct": -0.01,
            "four_dim_hyg_red_pct": -0.02,
            "four_dim_hyg_slope_days": 5,
            "four_dim_vix_green_max": 20.0,
            "four_dim_vix_confirm": 22.0,
            "four_dim_vix_red": 25.0,
            "four_dim_vix_hard": 30.0,
            "four_dim_breadth_lookback": 20,
            "four_dim_first_trim_shares": 2 if _asset == "QQQ" else 3,
        }
    )


LEVEL_NAMES = {
    0: "Level 0 · Risk-On",
    1: "Level 1 · Macro Warning",
    2: "Level 2 · Confirmed Risk-Off",
    3: "Level 3 · Hard Risk-Off",
}
LIGHTS = {"green": "🟢", "yellow": "🟡", "red": "🔴"}
LIGHT_CN = {"green": "绿", "yellow": "黄", "red": "红"}


def _finite(value: Any, default: float = float("nan")) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _json_value(value: Any) -> Any:
    """把 numpy/pandas 标量转成严格 JSON 可序列化值。"""
    if value is None:
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return value


def _status_rank(status: str) -> int:
    return {"green": 0, "yellow": 1, "red": 2}[status]


def _safe_streak(values: pd.Series, positive: bool, epsilon: float) -> int:
    """计算序列末端连续改善或恶化天数；NaN 会终止连续计数。"""
    count = 0
    for value in values.iloc[::-1]:
        if pd.isna(value):
            break
        matched = value < -epsilon if positive else value > epsilon
        if not bool(matched):
            break
        count += 1
    return count


def process_asset_indicators(df: pd.DataFrame, asset: str, p: dict) -> pd.DataFrame:
    """在旧版指标之上增加四维执行层需要的指标和明确的环比字段。"""
    df = legacy.process_asset_indicators(df, asset, p)
    close_col = f"{asset}_close"

    df[f"{asset}_MA150"] = df[close_col].rolling(150, min_periods=1).mean()
    df[f"{asset}_close_change_1d"] = df[close_col].diff()
    df[f"{asset}_return_1d"] = df[close_col].pct_change()

    # 用户要求的核心比较字段：例如 +0.18 -> +0.09，变化明确显示为 -0.09。
    df["US10Y_diff_20_change_1d"] = df["US10Y_diff_20"].diff()
    df["HYG_deviation_MA60"] = df["HYG_close"] / df["HYG_MA60"] - 1.0
    df["HYG_deviation_change_1d"] = df["HYG_deviation_MA60"].diff()
    slope_days = int(p.get("four_dim_hyg_slope_days", 5))
    df["HYG_slope_short"] = df["HYG_close"].pct_change(slope_days)
    df["VIX_change_1d"] = df["VIX_close"].diff()

    breadth_days = int(p.get("four_dim_breadth_lookback", 20))
    if {"RSP_close", "VOO_close"}.issubset(df.columns):
        ratio = df["RSP_close"] / df["VOO_close"]
        log_ratio = np.log(ratio)
        df["four_dim_breadth_mom"] = (log_ratio - log_ratio.shift(breadth_days)) / breadth_days
        df["four_dim_breadth_change_1d"] = df["four_dim_breadth_mom"].diff()
    else:
        df["four_dim_breadth_mom"] = np.nan
        df["four_dim_breadth_change_1d"] = np.nan
    return df


def _price_vote(df: pd.DataFrame, pos: int, asset: str, p: dict) -> Dict[str, Any]:
    row = df.iloc[pos]
    close = _finite(row[f"{asset}_close"])
    ma100 = _finite(row[f"{asset}_MA100"])
    ma150 = _finite(row[f"{asset}_MA150"])
    ma200 = _finite(row[f"{asset}_MA200"])
    prev_below_ma100 = False
    if pos > 0:
        prev = df.iloc[pos - 1]
        prev_below_ma100 = _finite(prev[f"{asset}_close"]) < _finite(prev[f"{asset}_MA100"])

    ma100_dist = close / ma100 - 1.0
    ma150_dist = close / ma150 - 1.0
    ma200_dist = close / ma200 - 1.0
    below_ma100_confirmed = close < ma100 and prev_below_ma100

    if asset == "VOO":
        hard = close < ma150
    else:
        # QQQ 在 MA100 下方且距离 MA200 不足 3%，视为“明显向 MA200 靠近”。
        hard = close < ma200 or (close < ma100 and ma200_dist <= 0.03)

    if hard:
        status = "red"
        detail = "跌破/逼近长期防守均线"
    elif close < ma100:
        status = "yellow"
        detail = "MA100 连续两日未收回" if below_ma100_confirmed else "首次跌破 MA100，等待防抖确认"
    elif close < _finite(row[f"{asset}_MA"]):
        status = "yellow"
        detail = "原模型核心趋势均线失守，等待跨维度确认"
    else:
        status = "green"
        detail = "价格仍处于 MA100 与核心趋势均线上方"

    return {
        "status": status,
        "detail": detail,
        "close": close,
        "ma100": ma100,
        "ma150": ma150,
        "ma200": ma200,
        "ma100_distance": ma100_dist,
        "ma150_distance": ma150_dist,
        "ma200_distance": ma200_dist,
        "below_ma100_confirmed": below_ma100_confirmed,
        "hard": hard,
    }


def _macro_vote(df: pd.DataFrame, pos: int, p: dict) -> Dict[str, Any]:
    row = df.iloc[pos]
    momentum = _finite(row["US10Y_diff_20"])
    change = _finite(row["US10Y_diff_20_change_1d"])
    threshold = float(p["us10y_th"])
    epsilon = float(p.get("four_dim_rate_delta_epsilon", 0.005))
    red_margin = float(p.get("four_dim_rate_red_margin", 0.10))
    changes = df["US10Y_diff_20_change_1d"].iloc[: pos + 1]
    improving_streak = _safe_streak(changes, positive=True, epsilon=epsilon)
    worsening_streak = _safe_streak(changes, positive=False, epsilon=epsilon)

    if momentum > threshold + red_margin and worsening_streak >= 1:
        status = "red"
        detail = "20日动量显著高于阈值且仍在恶化"
    elif momentum > threshold:
        status = "yellow"
        detail = "20日动量高于阈值"
    elif momentum > threshold - 0.03 and worsening_streak >= 2:
        status = "yellow"
        detail = "尚未越线，但连续恶化并接近阈值"
    else:
        status = "green"
        detail = "20日动量未超过阈值"

    direction = "改善" if change < -epsilon else "恶化" if change > epsilon else "持平"
    return {
        "status": status,
        "detail": detail,
        "momentum": momentum,
        "previous": momentum - change if math.isfinite(change) else float("nan"),
        "change": change,
        "threshold": threshold,
        "direction": direction,
        "improving_streak": improving_streak,
        "worsening_streak": worsening_streak,
    }


def _credit_vote(df: pd.DataFrame, pos: int, p: dict) -> Dict[str, Any]:
    row = df.iloc[pos]
    close = _finite(row["HYG_close"])
    ma60 = _finite(row["HYG_MA60"])
    deviation = _finite(row["HYG_deviation_MA60"])
    change = _finite(row["HYG_deviation_change_1d"])
    slope = _finite(row["HYG_slope_short"])
    light = float(p.get("four_dim_hyg_light_pct", -0.01))
    red_line = float(p.get("four_dim_hyg_red_pct", -0.02))

    if deviation >= 0:
        status = "green"
        detail = "HYG 位于 MA60 上方"
        severity = "正常"
    elif deviation <= red_line and slope < 0:
        status = "red"
        detail = "HYG 低于 MA60 超过 2% 且短期继续下行"
        severity = "重度"
    elif deviation > light:
        status = "yellow"
        detail = "HYG 低于 MA60 不足 1%，仅轻度信用预警"
        severity = "轻度"
    else:
        status = "yellow"
        detail = "HYG 低于 MA60 约 1%—2%，中度信用预警"
        severity = "中度"

    return {
        "status": status,
        "detail": detail,
        "severity": severity,
        "close": close,
        "ma60": ma60,
        "deviation": deviation,
        "previous_deviation": deviation - change if math.isfinite(change) else float("nan"),
        "deviation_change": change,
        "short_slope": slope,
    }


def _volatility_breadth_vote(df: pd.DataFrame, pos: int, p: dict) -> Dict[str, Any]:
    row = df.iloc[pos]
    vix = _finite(row["VIX_close"])
    vix_change = _finite(row["VIX_change_1d"])
    breadth = _finite(row.get("four_dim_breadth_mom", np.nan))
    breadth_change = _finite(row.get("four_dim_breadth_change_1d", np.nan))
    d_raw = bool(row.get("aux_d_dual_weak_raw", False)) if pd.notna(row.get("aux_d_dual_weak_raw", False)) else False
    d_confirmed = bool(row.get("aux_d_dual_weak_confirmed", False)) if pd.notna(row.get("aux_d_dual_weak_confirmed", False)) else False
    breadth_weak = math.isfinite(breadth) and breadth < 0
    breadth_worsening = breadth_weak and math.isfinite(breadth_change) and breadth_change < 0
    breadth_materially_weak = math.isfinite(breadth) and breadth < -0.0002

    if vix > float(p.get("four_dim_vix_hard", 30.0)):
        status = "red"
        detail = "VIX > 30，波动进入硬风险区"
    elif vix > float(p.get("four_dim_vix_red", 25.0)) or (d_confirmed and breadth_worsening):
        status = "red"
        detail = "VIX > 25 或 D 双弱确认且广度继续恶化"
    elif vix >= float(p.get("four_dim_vix_confirm", 22.0)) or breadth_materially_weak or d_raw:
        status = "yellow"
        detail = "VIX > 22、相对广度明显转弱或 D 模型预警"
    elif vix >= float(p.get("four_dim_vix_green_max", 20.0)) or breadth_weak:
        status = "yellow"
        detail = "VIX 20—22 或相对广度轻微转弱，尚未构成确认"
    else:
        status = "green"
        detail = "VIX < 20 且相对广度未转弱"

    return {
        "status": status,
        "detail": detail,
        "vix": vix,
        "previous_vix": vix - vix_change if math.isfinite(vix_change) else float("nan"),
        "vix_change": vix_change,
        "breadth_momentum": breadth,
        "previous_breadth_momentum": breadth - breadth_change if math.isfinite(breadth_change) else float("nan"),
        "breadth_change": breadth_change,
        "breadth_weak": breadth_weak,
        "breadth_worsening": breadth_worsening,
        "breadth_materially_weak": breadth_materially_weak,
        "d_raw": d_raw,
        "d_confirmed": d_confirmed,
        "confirmation_active": (
            status == "red"
            or vix >= float(p.get("four_dim_vix_confirm", 22.0))
            or breadth_materially_weak
            or d_raw
        ),
    }


def _candidate_level(votes: Dict[str, Dict[str, Any]], p: dict) -> Dict[str, Any]:
    statuses = [vote["status"] for vote in votes.values()]
    green_count = statuses.count("green")
    yellow_count = statuses.count("yellow")
    red_count = statuses.count("red")
    non_green_count = yellow_count + red_count

    source_risk = votes["macro"]["status"] != "green" or votes["credit"]["status"] != "green"
    cross_confirmation = (
        votes["price"]["status"] != "green"
        or votes["volatility_breadth"]["confirmation_active"]
    )
    hard = (
        red_count >= 3
        or (
            votes["price"]["status"] == "red"
            and votes["volatility_breadth"]["vix"] > float(p.get("four_dim_vix_hard", 30.0))
            and (votes["credit"]["status"] != "green" or votes["volatility_breadth"]["breadth_weak"])
        )
    )

    if hard:
        level = 3
        reason = "长期价格结构与至少两项压力同步恶化"
    elif (
        (red_count >= 2 and cross_confirmation)
        or (green_count <= 1 and non_green_count >= 3)
        or (source_risk and cross_confirmation)
    ):
        level = 2
        reason = "宏观/信用风险已获得价格或波动/广度的跨类别确认"
    elif non_green_count > 0:
        level = 1
        reason = "仅有单一类别或未获确认的预警"
    else:
        level = 0
        reason = "四个维度均未触发风险"

    # 边际改善只削弱执行强度，不抹去原始风险雷达；恶化也不能绕过跨类别确认。
    adjustment = 0
    adjustment_reason = "无"
    macro = votes["macro"]
    if level >= 2 and macro["improving_streak"] >= 2 and macro["momentum"] > macro["threshold"]:
        level -= 1
        adjustment = -1
        adjustment_reason = f"US10Y 20日动量连续改善 {macro['improving_streak']} 日，执行强度下调一级"
    elif level == 2 and macro["worsening_streak"] >= 2:
        adjustment = 1
        adjustment_reason = (
            f"US10Y 20日动量连续恶化 {macro['worsening_streak']} 日，Level 2 确认期缩短一级；"
            "未出现长期价格结构破坏，不越级进入 Hard Risk-Off"
        )

    return {
        "candidate_level": level,
        "hard": hard,
        "reason": reason,
        "marginal_adjustment": adjustment,
        "marginal_reason": adjustment_reason,
        "green_count": green_count,
        "yellow_count": yellow_count,
        "red_count": red_count,
        "source_risk": source_risk,
        "cross_confirmation": cross_confirmation,
    }


def add_four_dim_execution_layer(df: pd.DataFrame, asset: str, p: dict) -> pd.DataFrame:
    """逐日计算四维候选等级，并施加 2 日进入/2 日恢复防抖状态机。"""
    df = df.copy()
    assessments: List[Dict[str, Any]] = []
    execution_levels: List[int] = []
    current_level = 0
    risk_streak = 0
    recovery_streak = 0

    for pos in range(len(df)):
        votes = {
            "price": _price_vote(df, pos, asset, p),
            "macro": _macro_vote(df, pos, p),
            "credit": _credit_vote(df, pos, p),
            "volatility_breadth": _volatility_breadth_vote(df, pos, p),
        }
        decision = _candidate_level(votes, p)
        candidate = int(decision["candidate_level"])

        if candidate >= 2:
            risk_streak += 1
            required_confirm_days = max(
                1,
                int(p["four_dim_confirm_days"]) - max(0, int(decision["marginal_adjustment"])),
            )
            provisional = candidate if risk_streak >= required_confirm_days else 1
            if decision["hard"]:
                provisional = 3
        else:
            risk_streak = 0
            provisional = candidate

        if provisional >= current_level:
            current_level = provisional
            recovery_streak = 0
        else:
            recovery_streak += 1
            if recovery_streak >= int(p["four_dim_recovery_days"]):
                current_level = provisional
                recovery_streak = 0

        assessment = {
            "votes": votes,
            **decision,
            "risk_streak": risk_streak,
            "recovery_streak": recovery_streak,
            "execution_level": current_level,
        }
        assessments.append(assessment)
        execution_levels.append(current_level)

    df["four_dim_assessment"] = assessments
    df["four_dim_candidate_level"] = [item["candidate_level"] for item in assessments]
    df["four_dim_execution_level"] = execution_levels
    for dimension in ["price", "macro", "credit", "volatility_breadth"]:
        df[f"four_dim_{dimension}_status"] = [item["votes"][dimension]["status"] for item in assessments]
    return df


def add_historical_position(df: pd.DataFrame, asset: str, p: dict) -> pd.DataFrame:
    """保留旧回测序列，同时增加四维执行层的历史目标仓位代理。"""
    df = legacy.add_historical_position(df, asset, p)
    level = df["four_dim_execution_level"]
    desired = pd.Series(np.nan, index=df.index, dtype=float)
    desired[level == 0] = p["risk_on_pos"]
    desired[level == 2] = p["risk_warning_pos"]
    desired[level == 3] = p["risk_pos"]
    desired = desired.ffill().fillna(p["risk_on_pos"])
    df["legacy_position"] = df["position"]
    df["position_raw"] = desired
    df["position"] = desired.shift(1).fillna(p["risk_on_pos"])
    return df


def _current_position(asset: str, p: dict, current_price: float) -> float:
    portfolio = p.get("portfolio_value")
    shares = p.get("current_shares")
    if not portfolio or shares is None:
        return float(p["risk_on_pos"])
    return max(0.0, min(1.0, float(shares) * current_price / float(portfolio)))


def build_execution_decision(
    asset: str,
    p: dict,
    current_price: float,
    raw_signal: dict,
    assessment: Dict[str, Any],
) -> Dict[str, Any]:
    level = int(assessment["execution_level"])
    current_position = _current_position(asset, p, current_price)
    current_shares = p.get("current_shares")
    trim_shares = int(p.get("four_dim_first_trim_shares", 2))

    if level == 0:
        target_position = float(p["risk_on_pos"])
        action = "正常交易；按原策略管理，但不追涨"
    elif level == 1:
        target_position = current_position
        action = "持有不动；暂停新增仓位，不因单一宏观/信用信号减仓"
    elif level == 2:
        if current_shares is not None:
            defensive_floor_shares = math.ceil(float(p["risk_pos"]) * float(p["portfolio_value"]) / current_price)
            target_shares = max(defensive_floor_shares, int(current_shares) - trim_shares)
            actual_trim = max(0, int(current_shares) - target_shares)
            target_position = target_shares * current_price / float(p["portfolio_value"])
            trim_shares = actual_trim
            action = (
                f"第一档减仓：最多卖出 {actual_trim} 股；不直接降至 30%"
                if actual_trim
                else "现有仓位已不高于防守下限，持有不动"
            )
        else:
            target_position = float(p["risk_warning_pos"])
            action = f"第一档减仓至约 {target_position * 100:.0f}%"
    else:
        target_position = float(p["crash_pos"] if raw_signal.get("crash") else p["risk_pos"])
        action = f"快速降低至 {target_position * 100:.0f}% 防守仓；只减不加"

    return {
        "level": level,
        "state": LEVEL_NAMES[level],
        "target_position": target_position,
        "current_position": current_position,
        "action": action,
        "pause_new_buys": level >= 1,
        "first_trim_shares": trim_shares if level == 2 else 0,
        "raw_model_state": raw_signal["state"],
        "raw_model_target": float(raw_signal["target_position"]),
    }


def build_amount_plan(asset: str, p: dict, current_price: float, execution: dict) -> str:
    portfolio = p.get("portfolio_value")
    shares = p.get("current_shares")
    if not portfolio or shares is None:
        return "\n💰【实盘金额估算】未配置资金池或持仓股数，无法计算具体股数。\n"

    current_value = shares * current_price
    level = execution["level"]
    if level == 1:
        action = "0 操作（停止加仓，保留现有持仓）"
        target_value = current_value
    elif level == 2:
        sell_shares = min(int(shares), int(execution["first_trim_shares"]))
        action = f"卖出 {sell_shares} 股（第一档确认减仓）" if sell_shares else "0 操作"
        target_value = (int(shares) - sell_shares) * current_price
    else:
        target_value = float(portfolio) * float(execution["target_position"])
        diff = target_value - current_value
        trade_shares = int(abs(diff) / current_price + 1e-9)
        if abs(diff) < current_price:
            action = "0 操作（差额不足 1 股）"
        elif diff > 0 and level >= 1:
            action = "0 操作（风险状态禁止主动补仓）"
        elif diff > 0:
            action = f"买入 {trade_shares} 股"
        else:
            action = f"卖出 {trade_shares} 股"

    return (
        "\n💰【四维实盘金额估算】\n"
        f"   • 资金池规模     : ${portfolio:,.2f}\n"
        f"   • 当前持仓       : {shares} 股，市值约 ${current_value:,.2f}，占资金池 {current_value / portfolio:.1%}\n"
        f"   • 四维执行后市值 : 约 ${target_value:,.2f}，占资金池 {target_value / portfolio:.1%}\n"
        f"   • 明日最终动作   : {action}\n"
    )


def build_four_dim_report(asset: str, p: dict, assessment: dict, execution: dict) -> str:
    votes = assessment["votes"]
    macro = votes["macro"]
    credit = votes["credit"]
    vol = votes["volatility_breadth"]
    price = votes["price"]

    vote_lines = []
    for key, label in [
        ("price", "价格趋势"),
        ("macro", "宏观利率"),
        ("credit", "信用 HYG"),
        ("volatility_breadth", "波动/广度"),
    ]:
        vote = votes[key]
        vote_lines.append(f"   • {label:<10}: {LIGHTS[vote['status']]} {LIGHT_CN[vote['status']]} — {vote['detail']}")

    return (
        "🧩【四维风险确认与执行层】\n"
        + "\n".join(vote_lines)
        + "\n"
        f"   • 票数          : {assessment['green_count']}绿 + {assessment['yellow_count']}黄 + {assessment['red_count']}红\n"
        f"   • 候选等级      : {LEVEL_NAMES[int(assessment['candidate_level'])]}\n"
        f"   • 防抖后等级    : {execution['state']}（风险连续 {assessment['risk_streak']} 日；恢复连续 {assessment['recovery_streak']} 日）\n"
        f"   • 跨类别确认    : {'是' if assessment['cross_confirmation'] else '否'}\n"
        f"   • 边际调整      : {assessment['marginal_reason']}\n"
        f"   • 最终动作      : {execution['action']}\n\n"
        "📐【关键指标：前值 → 现值（变化）】\n"
        f"   • US10Y 20日动量: {macro['previous']:+.2f} → {macro['momentum']:+.2f} ({macro['change']:+.2f})；"
        f"阈值 {macro['threshold']:+.2f}，方向={macro['direction']}\n"
        f"   • HYG/MA60偏离  : {credit['previous_deviation']:+.2%} → {credit['deviation']:+.2%} "
        f"({credit['deviation_change']:+.2%})；{credit['severity']}，5日斜率 {credit['short_slope']:+.2%}\n"
        f"   • VIX           : {vol['previous_vix']:.2f} → {vol['vix']:.2f} ({vol['vix_change']:+.2f})\n"
        f"   • RSP/VOO动量   : {vol['previous_breadth_momentum'] * 100:+.4f}%/日 → "
        f"{vol['breadth_momentum'] * 100:+.4f}%/日 ({vol['breadth_change'] * 100:+.4f}%/日)\n"
        f"   • {asset}/MA100  : {price['ma100_distance']:+.2%}；/MA150 {price['ma150_distance']:+.2%}；"
        f"/MA200 {price['ma200_distance']:+.2%}\n\n"
        "🛡️【执行纪律】Risk-Off ≠ Sell；先检查四维确认。Level 1 只停加仓，"
        "Level 2 才执行首档减仓，Level 3 才快速防守。\n\n"
    )


def _snapshot_payload(
    asset: str,
    trade_date: str,
    p: dict,
    assessment: dict,
    execution: dict,
) -> Dict[str, Any]:
    votes = assessment["votes"]
    return {
        "schema_version": VERSION_TAG,
        "trade_date": trade_date,
        "asset": asset,
        "execution": {key: _json_value(value) for key, value in execution.items()},
        "assessment": {
            key: _json_value(value)
            for key, value in assessment.items()
            if key != "votes"
        },
        "votes": {
            dimension: {key: _json_value(value) for key, value in vote.items()}
            for dimension, vote in votes.items()
        },
        "parameters": {
            "us10y_threshold": p["us10y_th"],
            "confirm_days": p["four_dim_confirm_days"],
            "recovery_days": p["four_dim_recovery_days"],
            "hyg_light_threshold": p["four_dim_hyg_light_pct"],
            "hyg_red_threshold": p["four_dim_hyg_red_pct"],
            "vix_confirmation": p["four_dim_vix_confirm"],
            "vix_hard": p["four_dim_vix_hard"],
        },
    }


def _write_snapshot(snapshot: dict, file_date_str: str) -> Path:
    dated = OUTPUT_PATH / f"four_dim_risk_snapshot_{VERSION_TAG}_{file_date_str}.json"
    latest = OUTPUT_PATH / f"four_dim_risk_snapshot_{VERSION_TAG}_latest.json"
    content = json.dumps(snapshot, ensure_ascii=False, indent=2, allow_nan=False)
    dated.write_text(content, encoding="utf-8")
    latest.write_text(content, encoding="utf-8")
    return dated


def plot_four_dim_summary_pdf(
    asset: str,
    trade_date: str,
    file_date_str: str,
    assessment: dict,
    execution: dict,
) -> Path:
    """生成一页四维执行摘要，供指数、个股流水线和投研报告共同引用。"""
    votes = assessment["votes"]
    path = OUTPUT_PATH / f"four_dim_risk_summary_{VERSION_TAG}_{file_date_str}_{asset.lower()}.pdf"
    colors = {"green": "#27ae60", "yellow": "#f39c12", "red": "#c0392b"}

    with PdfPages(path) as pdf:
        fig = plt.figure(figsize=(11.69, 8.27))
        fig.patch.set_facecolor("#f7f9fb")
        fig.text(0.06, 0.92, f"{asset} 四维风险确认执行摘要", fontsize=24, fontweight="bold", color="#233044")
        fig.text(0.06, 0.875, f"{trade_date}  |  {VERSION_TAG}", fontsize=12, color="#667085")
        fig.text(0.06, 0.81, execution["state"], fontsize=20, fontweight="bold", color="#233044")
        fig.text(0.06, 0.765, execution["action"], fontsize=13, color="#344054")

        labels = ["价格趋势", "宏观利率", "信用 HYG", "波动/广度"]
        keys = ["price", "macro", "credit", "volatility_breadth"]
        for idx, (label, key) in enumerate(zip(labels, keys)):
            y = 0.66 - idx * 0.105
            status = votes[key]["status"]
            fig.text(0.07, y, "●", fontsize=25, color=colors[status], va="center")
            fig.text(0.115, y, label, fontsize=14, fontweight="bold", va="center")
            fig.text(0.27, y, votes[key]["detail"], fontsize=12, color="#475467", va="center")

        macro = votes["macro"]
        credit = votes["credit"]
        vol = votes["volatility_breadth"]
        metrics = (
            f"US10Y 20日动量  {macro['previous']:+.2f} → {macro['momentum']:+.2f}  ({macro['change']:+.2f})\n"
            f"HYG/MA60偏离    {credit['previous_deviation']:+.2%} → {credit['deviation']:+.2%}  "
            f"({credit['deviation_change']:+.2%})\n"
            f"VIX             {vol['previous_vix']:.2f} → {vol['vix']:.2f}  ({vol['vix_change']:+.2f})"
        )
        fig.text(0.06, 0.23, metrics, fontsize=13, linespacing=1.55, color="#233044")
        fig.text(
            0.06,
            0.075,
            "规则：单一宏观/信用风险不能触发大规模减仓；Level 2 连续两日确认；恢复同样连续两日。",
            fontsize=11,
            color="#667085",
        )
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)
    return path


def generate_daily_report() -> Dict[str, Any]:
    print("=" * 80)
    print("🚀 VOO / QQQ 四维风险确认执行器 260904 v5 启动")
    print("=" * 80)
    master_df = legacy.load_master_data()
    snapshots: Dict[str, Any] = {}
    last_file_date = ""

    for asset, p in ASSET_CONFIG.items():
        print(f"\n⏳ 正在处理 {asset} 四维执行层...")
        asset_df = legacy.filter_real_trading_days(master_df, asset)
        df = process_asset_indicators(asset_df, asset, p)
        df = legacy.add_aux_d_indicators(df, asset, p)
        df = add_four_dim_execution_layer(df, asset, p)
        df = add_historical_position(df, asset, p)

        today = df.iloc[-1]
        trade_date = today.name.strftime("%Y-%m-%d")
        file_date_str = today.name.strftime("%Y_%m_%d")
        last_file_date = file_date_str
        raw_signal = legacy.classify_market_state(df, asset, p)
        assessment = today["four_dim_assessment"]
        execution = build_execution_decision(
            asset,
            p,
            float(today[f"{asset}_close"]),
            raw_signal,
            assessment,
        )

        amount_plan = build_amount_plan(asset, p, float(today[f"{asset}_close"]), execution)
        four_dim_text = build_four_dim_report(asset, p, assessment, execution)
        raw_reason = raw_signal["action_reason"]
        report_content = (
            f"{'=' * 68}\n"
            f"📊 {asset} 量化日报 · 四维执行版 {VERSION_TAG} | 结算日: {trade_date}\n"
            f"{'=' * 68}\n\n"
            "📡【原模型风险雷达（保留，不直接等同交易）】\n"
            f"   • 原模型状态     : {raw_signal['state']}\n"
            f"   • 原模型目标仓位 : {raw_signal['target_position'] * 100:.0f}%\n"
            f"   • 原模型说明     : {raw_reason}\n\n"
            f"{four_dim_text}"
            f"{amount_plan}\n"
            f"{'=' * 68}\n"
        )
        print(report_content)

        txt_path = OUTPUT_PATH / f"index_signal_{VERSION_TAG}_{file_date_str}_{asset.lower()}.txt"
        txt_path.write_text(report_content, encoding="utf-8")
        summary_pdf = plot_four_dim_summary_pdf(asset, trade_date, file_date_str, assessment, execution)

        # 旧版绘图函数在独立的新目录中运行，因此不会覆盖旧产物。
        chart_path = legacy.plot_snapshot_with_levels(
            df=df,
            asset=asset,
            p=p,
            target_position=execution["target_position"],
            market_state=execution["state"],
            trade_date=trade_date,
            file_date_str=file_date_str,
        )
        aux_pdf: Optional[Path] = None
        try:
            aux_pdf = legacy.plot_aux_d_risk_pdf_v2(df, asset, p, trade_date, file_date_str)
        except Exception as exc:
            print(f"⚠️ D辅助风险PDF生成失败，不影响四维主报告：{exc}")

        payload = _snapshot_payload(asset, trade_date, p, assessment, execution)
        payload["files"] = {
            "text_report": str(txt_path),
            "summary_pdf": str(summary_pdf),
            "chart": str(chart_path),
            "aux_d_pdf": str(aux_pdf) if aux_pdf else None,
        }
        snapshots[asset] = payload
        print(f"💾 四维文本报告: {txt_path}")
        print(f"📄 四维摘要PDF: {summary_pdf}")
        print(f"🖼️ 四维雷达图: {chart_path}")

    snapshot = {
        "schema_version": VERSION_TAG,
        "trade_date": max(item["trade_date"] for item in snapshots.values()),
        "assets": snapshots,
        "portfolio_rule": {
            "level_0": "正常交易",
            "level_1": "停止加仓，不减仓",
            "level_2": "跨类别且连续两日确认；VOO 首档减3股，QQQ 首档减2股",
            "level_3": "长期价格结构与多维风险同步恶化，快速防守",
            "core_principle": "Risk-Off != Sell; Risk-Off -> 四维确认 -> 执行动作",
        },
    }
    snapshot_path = _write_snapshot(snapshot, last_file_date)
    print(f"\n✅ 四维风险快照已保存: {snapshot_path}")
    print(f"📂 新版产物目录: {OUTPUT_PATH}")
    return snapshot


if __name__ == "__main__":
    generate_daily_report()
