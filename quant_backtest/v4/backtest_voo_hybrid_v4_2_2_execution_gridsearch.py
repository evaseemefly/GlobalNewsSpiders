#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VOO Hybrid V4.2.2 彻底解耦版 (Signal & Execution Layer Separation)

架构重构说明：
1. 彻底解耦：V3 作为“信号层”负责定调（目标满仓 100% 还是防守 30%），V4.2.2 作为“执行层”只负责微操（分几步买回）。
2. 参数固化：废弃无效中间态（如 risk_warning_pos），固化 panic_drop_pct=0.020。
3. 测速仪机制：VIX 15-20 不再强制降仓，而是作为买回速度的“限速器”。VIX < 15 一步满仓，VIX 15-20 碎步买入。
4. 不对称过滤：直接剔除 CAGR 低于 V3 收益 0.5% 的所有组合，防止网格搜索为了追求低回撤而过度牺牲复利。
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
RESULT_CSV_PATH = OUTPUT_DIR / "voo_v4_2_2_gridsearch_results.csv"
BEST_JSON_PATH = OUTPUT_DIR / "voo_v4_2_2_best_params.json"
TOP10_PIC_PATH = OUTPUT_DIR / "voo_v4_2_2_top10_nav_compare.png"
BEST_PIC_PATH = OUTPUT_DIR / "voo_v4_2_2_best_vs_v3_baseline.png"

TRANSACTION_COST_RATE = 0.0002

# ============================================================
# 2. V3 固化核心信号参数 & V4.2.2 固化状态参数
# ============================================================

V3_FIXED_PARAMS = {
    "asset": "VOO",
    "ma_len": 100,
    "us10y_th": 0.15,
    "vix_th": 45,
    "rsi_th": 30,
    "risk_pos": 0.30,
    "risk_on_pos": 1.00,
}

V4_FIXED_STATE_PARAMS = {
    "panic_drop_pct": 0.020,  # 固化高胜率狙击阈值
    "vix_warning_low": 15,  # 测速仪下限
    "vix_warning_high": 20,  # 测速仪上限
    "panic_rsi_low": 35,
    "panic_rsi_high": 45,
}

# ============================================================
# 3. V4.2.2 执行层网格搜索空间 (极简纯粹)
# ============================================================

V4_EXECUTION_GRID = {
    # 恢复满仓的速度：一步到位(1.00)，两步走(0.50)，三步走(0.33)
    "risk_on_gap_buy_pct": [0.33, 0.50, 1.00],

    # 恐慌暴跌日(-2.0%)的狙击目标仓位
    "panic_reversal_pos": [0.50, 0.60],

    # 配合 HYG 修复时，VIX 的安全确认线
    "risk_repair_vix_th": [18, 20],
}


# ============================================================
# 4. 工具函数
# ============================================================

def calculate_rsi(series, period=14):
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
    return df.set_index("trade_date_utc").sort_index()


def filter_real_trading_days(df, asset):
    df = df.copy()
    close_col = f"{asset}_close"
    volume_col = f"{asset}_volume"
    df = df[df[close_col].notna()]
    df = df[df.index.dayofweek < 5]
    if volume_col in df.columns:
        df = df[df[volume_col] > 0]
    return df.ffill()


def prepare_indicators(df, p):
    asset = p["asset"]
    close_col = f"{asset}_close"
    open_col = f"{asset}_open"
    df = df.copy()
    if open_col not in df.columns:
        df[open_col] = df[close_col]

    df[f"{asset}_MA"] = df[close_col].rolling(p["ma_len"], min_periods=1).mean()
    df["US10Y_diff_20"] = df["US10Y_close"].diff(20)
    df["HYG_MA60"] = df["HYG_close"].rolling(60, min_periods=1).mean()
    df["VIX_MA60"] = df["VIX_close"].rolling(60, min_periods=1).mean()
    df[f"RSI_14_{asset}"] = calculate_rsi(df[close_col])
    df[f"{asset}_ret"] = df[close_col].pct_change().fillna(0.0)
    df[f"{asset}_daily_return"] = df[close_col].pct_change()
    return df


def compute_nav_with_cost(position, asset_ret, cost_rate=TRANSACTION_COST_RATE):
    position = position.fillna(0.0)
    turnover_daily = position.diff().abs().fillna(0.0)
    daily_net_ret = position * asset_ret.fillna(0.0) - turnover_daily * cost_rate
    nav = (1 + daily_net_ret).cumprod()
    return nav, daily_net_ret, turnover_daily


def move_towards(current_pos, target_pos, gap_pct):
    if target_pos > current_pos:
        return current_pos + (target_pos - current_pos) * gap_pct
    return target_pos  # 降仓直接到位


# ============================================================
# 5. V3 纯净信号生成器
# ============================================================

def build_v3_signal_table(df, p):
    """只生成 V3 的纯净目标信号，没有任何微操。"""
    asset = p["asset"]
    close = df[f"{asset}_close"]
    open_price = df[f"{asset}_open"]
    ma = df[f"{asset}_MA"]
    rsi = df[f"RSI_14_{asset}"]

    base_trend = close > ma
    us10y_rising = df["US10Y_diff_20"] > p["us10y_th"]
    hyg_divergence = ((df["HYG_close"] < df["HYG_MA60"]).rolling(3).sum() == 3) & base_trend
    vix_risk = (df["VIX_close"] > p["vix_th"]) | (df["VIX_close"] > df["VIX_MA60"] * 1.8)

    risk_off = us10y_rising | hyg_divergence | vix_risk
    dip_buy = (rsi < p["rsi_th"]) & (close > open_price) & (df["VIX_close"] < df["VIX_close"].shift(1)) & (~risk_off)

    # 理论目标仓位（0 或 1 维度的定调）
    v3_target_pos = np.select(
        [dip_buy, risk_off, base_trend],
        [p["risk_on_pos"], p["risk_pos"], p["risk_on_pos"]],
        default=p["risk_pos"]
    )

    state_df = pd.DataFrame({
        "v3_target_pos": v3_target_pos,
        "risk_off_signal": risk_off,
        "daily_return": df[f"{asset}_daily_return"],
        "rsi": rsi,
        "vix": df["VIX_close"],
        "hyg": df["HYG_close"],
        "hyg_ma60": df["HYG_MA60"],
    }, index=df.index)

    return state_df


# ============================================================
# 6. 回测逻辑执行
# ============================================================

def run_v3_fixed_baseline(df, p):
    state_df = build_v3_signal_table(df, p)
    position = state_df["v3_target_pos"].shift(1).fillna(p["risk_pos"])
    nav, daily_net_ret, turnover_daily = compute_nav_with_cost(position, df[f"{p['asset']}_ret"])
    return {"nav": nav, "position": position, "daily_net_ret": daily_net_ret, "turnover_daily": turnover_daily}


def run_v4_2_2_execution_strategy(df, base_params, execution_params):
    p = {**base_params, **V4_FIXED_STATE_PARAMS, **execution_params}
    asset = p["asset"]
    state_df = build_v3_signal_table(df, p)

    current_pos = p["risk_pos"]
    position_for_return = []
    execution_notes = []

    for _, row in state_df.iterrows():
        position_for_return.append(current_pos)

        v3_target = row["v3_target_pos"]
        vix = row["vix"]
        is_panic_sniper = (row["risk_off_signal"]) and (row["daily_return"] <= -p["panic_drop_pct"]) and (
                    p["panic_rsi_low"] <= row["rsi"] <= p["panic_rsi_high"])
        hyg_repair = (row["hyg"] > row["hyg_ma60"]) and (vix < p["risk_repair_vix_th"])

        if is_panic_sniper:
            # 独立狙击事件：推翻 V3 的防守，执行一次高胜率抄底
            v3_target = p["panic_reversal_pos"]

        if current_pos > v3_target:
            # 降仓防守：一步到位，绝不拖延
            new_pos = v3_target
            note = "Reduce target -> fast exit"
        elif current_pos < v3_target:
            # 加仓进攻：绝对服从 V3 的方向，只用 VIX 调节速度
            if is_panic_sniper:
                new_pos = v3_target  # 狙击仓位直接打满
                note = "Panic sniper -> executed"
            elif vix < p["vix_warning_low"]:
                new_pos = move_towards(current_pos, v3_target, 1.00)  # VIX < 15，极度安全，全速满仓
                note = "VIX < 15 -> full speed recovery"
            elif (p["vix_warning_low"] <= vix <= p["vix_warning_high"]) or hyg_repair:
                new_pos = move_towards(current_pos, v3_target, p["risk_on_gap_buy_pct"])  # VIX 15-20 或已修复，按设定步长恢复
                note = "VIX 15-20 / Repair -> phased recovery"
            else:
                # 核心修复点：VIX > 20 且未修复。依然要加仓！但强制减速（例如只用设定步长的一半，或者最慢的 0.33）
                slow_speed = min(p["risk_on_gap_buy_pct"], 0.33)
                new_pos = move_towards(current_pos, v3_target, slow_speed)
                note = "VIX > 20 -> slow recovery"
        else:
            new_pos = current_pos
            note = "Maintain"

        new_pos = float(np.clip(new_pos, 0.0, 1.0))
        execution_notes.append(note)
        current_pos = new_pos

    position = pd.Series(position_for_return, index=df.index).fillna(p["risk_pos"])
    nav, daily_net_ret, turnover_daily = compute_nav_with_cost(position, df[f"{asset}_ret"])

    state_df = state_df.copy()
    state_df["position"] = position
    state_df["execution_note"] = execution_notes

    return {
        "nav": nav, "position": position, "daily_net_ret": daily_net_ret,
        "turnover_daily": turnover_daily, "state_df": state_df, "params": p
    }


# ============================================================
# 7. 绩效指标
# ============================================================

def calc_metrics(nav, position, daily_net_ret, turnover_daily):
    total_ret = nav.iloc[-1] - 1
    days = (nav.index[-1] - nav.index[0]).days
    years = days / 365.25
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0.0

    roll_max = nav.cummax()
    max_dd = ((nav - roll_max) / roll_max).min()
    calmar = cagr / abs(max_dd) if max_dd != 0 else np.nan

    total_turnover = turnover_daily.sum()
    annual_turnover = total_turnover / years if years > 0 else np.nan

    return {
        "cagr": cagr, "max_dd": max_dd, "calmar": calmar,
        "annual_turnover": annual_turnover,
        "trade_count": int((turnover_daily > 1e-8).sum()),
        "time_in_market": position.mean() * 100
    }


# ============================================================
# 8. 绘图与主程序
# ============================================================

def plot_best_vs_baseline(df, baseline, best_result):
    asset = V3_FIXED_PARAMS["asset"]
    buy_hold_nav, _, _ = compute_nav_with_cost(pd.Series(1.0, index=df.index), df[f"{asset}_ret"])

    fig, axes = plt.subplots(3, 1, figsize=(18, 12), gridspec_kw={"height_ratios": [3, 1, 1]}, sharex=True)

    axes[0].plot(df.index, buy_hold_nav, label="Buy & Hold VOO", color="gray", alpha=0.55, linewidth=2)
    axes[0].plot(df.index, baseline["nav"], label="V3 Fixed Baseline", color="black", linewidth=2.5)
    axes[0].plot(df.index, best_result["nav"], label="Best V4.2.2 Execution", color="#e74c3c", linewidth=2.5)
    axes[0].set_yscale("log")
    axes[0].set_title("Best V4.2.2 Execution vs V3 Baseline", fontsize=16, fontweight="bold")
    axes[0].legend(loc="upper left")
    axes[0].grid(True, alpha=0.4)

    axes[1].plot(df.index, baseline["position"], label="V3 Position", color="black", linewidth=1.5)
    axes[1].plot(df.index, best_result["position"], label="V4.2.2 Position", color="#e74c3c", linewidth=1.8)
    axes[1].set_ylim(0, 1.05)
    axes[1].set_ylabel("Position")
    axes[1].legend(loc="upper left")
    axes[1].grid(True, alpha=0.4)

    axes[2].plot(df.index, df["VIX_close"], label="VIX", color="#c0392b")
    axes[2].axhline(15, color='gray', linestyle='--', alpha=0.5, label='VIX=15 (Speed Limit)')
    axes[2].axhline(20, color='black', linestyle='--', alpha=0.5, label='VIX=20 (Hold Fire)')
    axes[2].legend(loc="upper left")
    axes[2].grid(True, alpha=0.4)

    fig.savefig(BEST_PIC_PATH, dpi=200, bbox_inches="tight")
    plt.close(fig)


def main():
    print("=== 🚀 VOO Hybrid V4.2.2 执行层参数搜索 (解耦净化版) ===")
    df = filter_real_trading_days(load_master_data(), V3_FIXED_PARAMS["asset"])
    df = prepare_indicators(df, V3_FIXED_PARAMS).iloc[220:].copy()

    baseline = run_v3_fixed_baseline(df, V3_FIXED_PARAMS)
    base_m = calc_metrics(baseline["nav"], baseline["position"], baseline["daily_net_ret"], baseline["turnover_daily"])

    print(
        f"\n📌 V3 基础模型 (信号源): CAGR {base_m['cagr'] * 100:.2f}% | MaxDD {base_m['max_dd'] * 100:.2f}% | Calmar {base_m['calmar']:.2f}")

    # 动态收益及格线：剔除所有年化收益低于 V3 - 0.8% 的废弃组合 (放宽容忍度)
    CAGR_THRESHOLD = base_m["cagr"] - 0.008
    print(f"🔒 启动不对称收益护城河：自动剔除 CAGR < {CAGR_THRESHOLD * 100:.2f}% 的组合")

    keys = list(V4_EXECUTION_GRID.keys())
    values = [V4_EXECUTION_GRID[k] for k in keys]
    results = []

    for combo in product(*values):
        exec_params = dict(zip(keys, combo))
        res = run_v4_2_2_execution_strategy(df, V3_FIXED_PARAMS, exec_params)
        m = calc_metrics(res["nav"], res["position"], res["daily_net_ret"], res["turnover_daily"])

        # 强制护城河过滤
        if m["cagr"] < CAGR_THRESHOLD:
            continue

        row = {**exec_params, **m, "nav": res["nav"], "position": res["position"], "state_df": res["state_df"]}
        results.append(row)

    # 排序：Calmar 优先，换手率作为第二惩罚项
    results = sorted(results, key=lambda x: (x["calmar"], -x["annual_turnover"], x["cagr"]), reverse=True)

    if not results:
        print("⚠️ 警告：没有任何组合通过 CAGR 护城河测试！")
        return

    print("\n🏆 Top 10 V4.2.2 参数组合 (过滤低收益后，按 Calmar 排序):")
    print("-" * 130)
    print(
        f"{'排名':<4} {'RiskOnGap':<10} {'PanicPos':<9} {'RepairVIX':<10} | {'年化':<8} {'回撤':<8} {'Calmar':<8} {'年换手':<8} {'交易次数':<8}")
    print("-" * 130)

    for i, r in enumerate(results[:10], 1):
        print(
            f"{i:<4} {r['risk_on_gap_buy_pct']:<10.2f} {r['panic_reversal_pos']:<9.2f} {r['risk_repair_vix_th']:<10} | "
            f"{r['cagr'] * 100:>6.2f}% {r['max_dd'] * 100:>7.2f}% {r['calmar']:>7.2f} {r['annual_turnover']:>7.2f} {r['trade_count']:>6}")

    best = results[0]
    best_json = {
        "v3_base_metrics": {k: float(v) for k, v in base_m.items()},
        "v4_2_2_best_params": {k: best[k] for k in keys},
        "v4_2_2_best_metrics": {k: float(best[k]) for k in
                                ["cagr", "max_dd", "calmar", "annual_turnover", "trade_count"]}
    }
    BEST_JSON_PATH.write_text(json.dumps(best_json, indent=2, ensure_ascii=False), encoding="utf-8")

    plot_best_vs_baseline(df, baseline, {"nav": best["nav"], "position": best["position"]})
    print(f"\n✅ 执行完成。最优对比图已保存至 {BEST_PIC_PATH.name}")


if __name__ == "__main__":
    main()