#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VOO Hybrid 稳健性审计 (Robustness Audit)
用途：
1. 切片回测：验证 V3 与 V4.4 在不同宏观周期下的表现。
2. 事件归因：透视 Panic-Reversal 插件的历史触发点与真实胜率。
"""

import pandas as pd
import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

# ============================================================
# 1. 基础配置
# ============================================================
plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False

CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv")
OUTPUT_DIR = Path(__file__).parent
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TRANSACTION_COST_RATE = 0.0002

# 定义 V3 核心基准
V3_PARAMS = {
    "asset": "VOO", "ma_len": 100, "us10y_th": 0.15,
    "vix_th": 45, "rsi_th": 30, "risk_pos": 0.30,
}

# 定义 V4.4 最小插件 (采用 Score Top 1 的参数)
V4_4_PARAMS = {
    **V3_PARAMS,
    "panic_drop_pct": 0.020,
    "panic_reversal_pos": 0.50,
    "panic_rsi_low": 35,
    "panic_rsi_high": 45,
}

# 宏观周期切片
MACRO_REGIMES = {
    "1_加息与贸易战 (2015-2018)": ("2015-01-01", "2018-12-31"),
    "2_疫情与大放水 (2019-2021)": ("2019-01-01", "2021-12-31"),
    "3_通胀与大熊市 (2022-2023)": ("2022-01-01", "2023-12-31"),
    "4_AI狂飙与软着陆 (2024-至今)": ("2024-01-01", "2026-12-31"),
}


# ============================================================
# 2. 核心逻辑
# ============================================================

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def prep_data():
    df = pd.read_csv(CSV_FILE_PATH)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()

    asset = V3_PARAMS["asset"]
    df = df[df[f"{asset}_close"].notna()]
    df = df[df.index.dayofweek < 5].ffill()

    df[f"{asset}_MA"] = df[f"{asset}_close"].rolling(V3_PARAMS["ma_len"], min_periods=1).mean()
    df["US10Y_diff_20"] = df["US10Y_close"].diff(20)
    df["HYG_MA60"] = df["HYG_close"].rolling(60, min_periods=1).mean()
    df["VIX_MA60"] = df["VIX_close"].rolling(60, min_periods=1).mean()
    df["RSI_14"] = calculate_rsi(df[f"{asset}_close"])
    df[f"{asset}_ret"] = df[f"{asset}_close"].pct_change().fillna(0.0)

    # 向前未来收益 (用于事件归因)
    df["fwd_5d_ret"] = df[f"{asset}_close"].shift(-5) / df[f"{asset}_close"] - 1
    df["fwd_20d_ret"] = df[f"{asset}_close"].shift(-20) / df[f"{asset}_close"] - 1

    return df.iloc[220:].copy()


def get_signals(df, p):
    asset = p["asset"]
    base_trend = df[f"{asset}_close"] > df[f"{asset}_MA"]
    us10y_rising = df["US10Y_diff_20"] > p["us10y_th"]
    hyg_divergence = ((df["HYG_close"] < df["HYG_MA60"]).rolling(3).sum() == 3) & base_trend
    vix_risk = (df["VIX_close"] > p["vix_th"]) | (df["VIX_close"] > df["VIX_MA60"] * 1.8)

    risk_off = us10y_rising | hyg_divergence | vix_risk
    dip_buy = (df["RSI_14"] < p["rsi_th"]) & (df["VIX_close"] < df["VIX_close"].shift(1)) & (~risk_off)

    daily_ret = df[f"{asset}_ret"]
    panic_triggered = (
            risk_off &
            (daily_ret <= -p.get("panic_drop_pct", 999)) &
            (df["RSI_14"] >= p.get("panic_rsi_low", 0)) &
            (df["RSI_14"] <= p.get("panic_rsi_high", 0))
    )

    return base_trend, risk_off, dip_buy, panic_triggered


def run_models(df):
    asset = "VOO"
    base_trend, risk_off, dip_buy, panic_triggered = get_signals(df, V4_4_PARAMS)

    # V3 Position
    v3_pos = np.select([dip_buy, risk_off, base_trend], [1.0, V3_PARAMS["risk_pos"], 1.0],
                       default=V3_PARAMS["risk_pos"])
    v3_series = pd.Series(v3_pos, index=df.index).shift(1).fillna(V3_PARAMS["risk_pos"])

    # V4.4 Position
    v4_pos = np.select(
        [dip_buy, panic_triggered, risk_off, base_trend],
        [1.0, V4_4_PARAMS["panic_reversal_pos"], V3_PARAMS["risk_pos"], 1.0],
        default=V3_PARAMS["risk_pos"]
    )
    v4_series = pd.Series(v4_pos, index=df.index).shift(1).fillna(V3_PARAMS["risk_pos"])

    # Calc NAVs
    def calc_nav(pos):
        turnover = pos.diff().abs().fillna(0)
        net_ret = pos * df[f"{asset}_ret"] - turnover * TRANSACTION_COST_RATE
        return (1 + net_ret).cumprod()

    df["BuyHold_NAV"] = (1 + df[f"{asset}_ret"]).cumprod()
    df["V3_NAV"] = calc_nav(v3_series)
    df["V4_NAV"] = calc_nav(v4_series)
    df["Panic_Event"] = panic_triggered

    return df


def calc_period_metrics(df_slice, nav_col):
    if len(df_slice) < 2: return 0.0, 0.0
    nav = df_slice[nav_col] / df_slice[nav_col].iloc[0]  # Rebase to 1.0

    total_ret = nav.iloc[-1] - 1
    years = len(df_slice) / 252.0
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0

    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cagr, max_dd

# ============================================================
# 3. 绘图逻辑：分阶段净值 + 最大回撤标记
# ============================================================

def safe_filename(name: str) -> str:
    """
    将阶段名称转换为适合保存文件的文件名。
    """
    replace_chars = [" ", "(", ")", "（", "）", "/", "\\", ":", "：", "|"]
    safe = name
    for ch in replace_chars:
        safe = safe.replace(ch, "_")
    return safe


def get_max_drawdown_info(nav: pd.Series):
    """
    获取最大回撤信息：
    - 最大回撤幅度
    - 回撤起点 peak_date
    - 回撤低点 trough_date
    - peak_nav
    - trough_nav
    """
    nav = nav.dropna()

    if len(nav) < 2:
        return {
            "max_dd": 0.0,
            "peak_date": None,
            "trough_date": None,
            "peak_nav": np.nan,
            "trough_nav": np.nan,
        }

    roll_max = nav.cummax()
    drawdown = nav / roll_max - 1

    trough_date = drawdown.idxmin()
    max_dd = drawdown.loc[trough_date]

    # 最大回撤低点之前的最高点
    peak_date = nav.loc[:trough_date].idxmax()

    return {
        "max_dd": max_dd,
        "peak_date": peak_date,
        "trough_date": trough_date,
        "peak_nav": nav.loc[peak_date],
        "trough_nav": nav.loc[trough_date],
    }


def plot_single_regime_nav_with_drawdown(df_slice: pd.DataFrame, regime_name: str):
    """
    绘制单个宏观阶段内三种模型的净值曲线，并标记最大回撤。

    三种模型：
    - Buy & Hold
    - V3
    - V4.4
    """
    if df_slice.empty or len(df_slice) < 2:
        print(f"⚠️ 阶段 {regime_name} 数据不足，跳过绘图。")
        return

    model_config = {
        "Buy & Hold": {
            "nav_col": "BuyHold_NAV",
            "color": "gray",
            "linestyle": "-",
        },
        "V3 基准": {
            "nav_col": "V3_NAV",
            "color": "black",
            "linestyle": "-",
        },
        "V4.4 插件": {
            "nav_col": "V4_NAV",
            "color": "#e74c3c",
            "linestyle": "-",
        },
    }

    fig, ax = plt.subplots(figsize=(18, 9))

    dd_infos = {}

    for model_name, cfg in model_config.items():
        nav_col = cfg["nav_col"]

        if nav_col not in df_slice.columns:
            print(f"⚠️ 缺少字段 {nav_col}，跳过 {model_name}")
            continue

        # 每个阶段重新归一化到 1.0
        nav = df_slice[nav_col].dropna()
        nav = nav / nav.iloc[0]

        dd_info = get_max_drawdown_info(nav)
        dd_infos[model_name] = dd_info

        label = (
            f"{model_name} | "
            f"MaxDD {dd_info['max_dd'] * 100:.1f}%"
        )

        ax.plot(
            nav.index,
            nav,
            label=label,
            linewidth=2.4,
            color=cfg["color"],
            linestyle=cfg["linestyle"],
            alpha=0.9,
        )

        peak_date = dd_info["peak_date"]
        trough_date = dd_info["trough_date"]

        if peak_date is not None and trough_date is not None:
            peak_nav = dd_info["peak_nav"]
            trough_nav = dd_info["trough_nav"]
            max_dd = dd_info["max_dd"]

            # 标记 Peak 与 Trough
            ax.scatter(
                [peak_date],
                [peak_nav],
                color=cfg["color"],
                marker="^",
                s=80,
                zorder=5,
            )

            ax.scatter(
                [trough_date],
                [trough_nav],
                color=cfg["color"],
                marker="v",
                s=80,
                zorder=5,
            )

            # 画最大回撤线段
            ax.plot(
                [peak_date, trough_date],
                [peak_nav, trough_nav],
                color=cfg["color"],
                linestyle="--",
                linewidth=1.6,
                alpha=0.75,
            )

            # 标注最大回撤
            ax.annotate(
                f"{model_name}\nDD {max_dd * 100:.1f}%",
                xy=(trough_date, trough_nav),
                xytext=(10, -25),
                textcoords="offset points",
                fontsize=9,
                color=cfg["color"],
                arrowprops=dict(
                    arrowstyle="->",
                    color=cfg["color"],
                    lw=1.0,
                    alpha=0.8,
                ),
            )

    ax.set_title(
        f"{regime_name}：三种模型阶段净值对比与最大回撤标记",
        fontsize=16,
        fontweight="bold",
    )
    ax.set_ylabel("阶段归一化净值")
    ax.set_xlabel("日期")
    ax.grid(True, alpha=0.35)
    ax.legend(loc="upper left", fontsize=10)

    output_path = OUTPUT_DIR / f"regime_nav_dd_{safe_filename(regime_name)}.png"
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)

    print(f"🖼️ 阶段净值与最大回撤图已保存: {output_path.name}")


def plot_all_regimes_nav_with_drawdown(df: pd.DataFrame):
    """
    对 MACRO_REGIMES 中的每个阶段分别绘图。
    """
    print("\n🖼️ 开始绘制各阶段三模型净值曲线与最大回撤标记...")

    for regime_name, (start, end) in MACRO_REGIMES.items():
        mask = (df.index >= start) & (df.index <= end)
        df_slice = df[mask].copy()

        if df_slice.empty:
            print(f"⚠️ 阶段 {regime_name} 无数据，跳过。")
            continue

        plot_single_regime_nav_with_drawdown(df_slice, regime_name)

    print("✅ 各阶段净值与最大回撤绘图完成。")

def print_audit_report(df):
    print("=" * 80)
    print("🛡️ VOO 混合量化模型 —— 稳健性审计报告 (Robustness Audit)")
    print("=" * 80)

    # 1. 宏观切片分析
    print("\n📊 宏观周期切片表现 (Regime Walk-Forward):")
    print(f"{'阶段':<28} | {'Buy&Hold (CAGR/DD)':<20} | {'V3 基准 (CAGR/DD)':<20} | {'V4.4 插件 (CAGR/DD)':<20}")
    print("-" * 100)

    for name, (start, end) in MACRO_REGIMES.items():
        mask = (df.index >= start) & (df.index <= end)
        slice_df = df[mask]
        if slice_df.empty: continue

        bh_cagr, bh_dd = calc_period_metrics(slice_df, "BuyHold_NAV")
        v3_cagr, v3_dd = calc_period_metrics(slice_df, "V3_NAV")
        v4_cagr, v4_dd = calc_period_metrics(slice_df, "V4_NAV")

        print(
            f"{name:<25} | {bh_cagr * 100:>6.1f}% / {bh_dd * 100:>5.1f}% | {v3_cagr * 100:>6.1f}% / {v3_dd * 100:>5.1f}% | {v4_cagr * 100:>6.1f}% / {v4_dd * 100:>5.1f}%")

    # 2. Panic 事件归因
    print("\n🎯 Panic 插件独立事件归因 (Event Attribution):")
    panic_days = df[df["Panic_Event"]]

    print(f"   • 总触发次数: {len(panic_days)} 次")
    if not panic_days.empty:
        win_5d = (panic_days["fwd_5d_ret"] > 0).mean()
        win_20d = (panic_days["fwd_20d_ret"] > 0).mean()
        print(f"   • 触发后 5 日胜率:  {win_5d * 100:.1f}% (平均收益 {panic_days['fwd_5d_ret'].mean() * 100:.2f}%)")
        print(f"   • 触发后 20 日胜率: {win_20d * 100:.1f}% (平均收益 {panic_days['fwd_20d_ret'].mean() * 100:.2f}%)")

        print("\n   [历史触发日明细]")
        for idx, row in panic_days.iterrows():
            print(
                f"     - {idx.strftime('%Y-%m-%d')} | 当日跌幅: {row['VOO_ret'] * 100:.1f}% | RSI: {row['RSI_14']:.1f} | 未来5日: {row['fwd_5d_ret'] * 100:+.1f}% | 未来20日: {row['fwd_20d_ret'] * 100:+.1f}%")

    # 3. 最终全局对比
    bh_cagr, bh_dd = calc_period_metrics(df, "BuyHold_NAV")
    v3_cagr, v3_dd = calc_period_metrics(df, "V3_NAV")
    v4_cagr, v4_dd = calc_period_metrics(df, "V4_NAV")

    print("\n" + "=" * 80)
    print("🏆 全局结论 (Total History):")
    print(f"   Buy & Hold: CAGR {bh_cagr * 100:.2f}% | MaxDD {bh_dd * 100:.2f}%")
    print(f"   V3 基准   : CAGR {v3_cagr * 100:.2f}% | MaxDD {v3_dd * 100:.2f}%")
    print(f"   V4.4 插件 : CAGR {v4_cagr * 100:.2f}% | MaxDD {v4_dd * 100:.2f}%")
    print("=" * 80)


def main():
    df = prep_data()
    df = run_models(df)
    print_audit_report(df)


if __name__ == "__main__":
    main()