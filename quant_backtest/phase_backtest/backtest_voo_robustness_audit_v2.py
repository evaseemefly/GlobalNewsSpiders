#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VOO Hybrid 稳健性审计 v2 (Robustness Audit v2)

用途：
1. 切片回测：验证 Buy & Hold、V3、V4.4 在不同宏观周期下的表现。
2. 指标增强：阶段结果输出 CAGR / Total Return / MaxDD，并补充 Calmar、Sharpe、Sortino、换手、交易次数。
3. 信号修正：V3 Dip-Buy 严格复刻原始 V3 逻辑，恢复 VOO_close > VOO_open 条件。
4. 事件归因增强：
   - 同时输出“信号日收盘价作为入场基准”的未来收益；
   - 以及“下一交易日开盘入场”的未来收益，更贴近实盘执行。
5. Panic 插件区分：
   - Panic_Event_Raw：满足 panic 条件；
   - Panic_Event_Effective：在 V4.4 执行优先级下真正生效的 panic 事件。

# todo:26-06-08:
# - 修正 Dip-Buy 与 V3 原始代码保持一致：RSI < rsi_th + close > open + VIX 下降。
# - 增加 TotalRet 输出，避免把 CAGR 误解为阶段总收益。
# - 增加 next_open_fwd_5d_ret / next_open_fwd_20d_ret，用于更真实的 Panic 归因。
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
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv"
)

OUTPUT_DIR = Path(__file__).parent
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TRANSACTION_COST_RATE = 0.0002

# V3 核心基准
V3_PARAMS = {
    "asset": "VOO",
    "ma_len": 100,
    "us10y_th": 0.15,
    "vix_th": 45,
    "rsi_th": 30,
    "risk_pos": 0.30,
}

# V4.4 最小插件：采用 Score Top 1 参数
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
# 2. 工具函数
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
    close_col = f"{asset}_close"
    open_col = f"{asset}_open"
    volume_col = f"{asset}_volume"

    if close_col not in df.columns:
        raise KeyError(f"缺少字段: {close_col}")

    if open_col not in df.columns:
        df[open_col] = df[close_col]

    df = df[df[close_col].notna()]
    df = df[df.index.dayofweek < 5]

    if volume_col in df.columns:
        df = df[df[volume_col] > 0]

    df = df.ffill()

    df[f"{asset}_MA"] = df[close_col].rolling(V3_PARAMS["ma_len"], min_periods=1).mean()
    df["US10Y_diff_20"] = df["US10Y_close"].diff(20)
    df["HYG_MA60"] = df["HYG_close"].rolling(60, min_periods=1).mean()
    df["VIX_MA60"] = df["VIX_close"].rolling(60, min_periods=1).mean()
    df["RSI_14"] = calculate_rsi(df[close_col])

    df[f"{asset}_ret"] = df[close_col].pct_change().fillna(0.0)

    # ========================================================
    # 事件归因收益
    # ========================================================
    # 1) 信号日收盘价作为基准：与 close-to-close 回测更接近。
    df["signal_close_fwd_1d_ret"] = df[close_col].shift(-1) / df[close_col] - 1
    df["signal_close_fwd_5d_ret"] = df[close_col].shift(-5) / df[close_col] - 1
    df["signal_close_fwd_20d_ret"] = df[close_col].shift(-20) / df[close_col] - 1

    # 2) 下一交易日开盘入场：更贴近真实执行。
    #    信号在 t 日收盘后产生，假设 t+1 日开盘买入，
    #    持有到 t+1 / t+5 / t+20 的收盘。
    next_open = df[open_col].shift(-1)
    df["next_open_fwd_1d_ret"] = df[close_col].shift(-1) / next_open - 1
    df["next_open_fwd_5d_ret"] = df[close_col].shift(-5) / next_open - 1
    df["next_open_fwd_20d_ret"] = df[close_col].shift(-20) / next_open - 1

    # 跳过早期指标不稳定阶段
    return df.iloc[220:].copy()


def get_signals(df, p):
    """
    严格复刻 V3 信号层。

    注意：
    原始 V3 的 dip_buy 是：
        RSI < rsi_th
        close > open
        VIX < VIX.shift(1)

    原始 V3 中 np.select 的顺序是：
        [dip_buy, risk_off, base_trend]
    因此 dip_buy 在逻辑上优先于 risk_off。
    这里不额外添加 ~risk_off，以保持和 V3 原始代码一致。
    """
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

    # todo:26-06-08:
    # 修正点：恢复 close > open，且不额外添加 ~risk_off。
    dip_buy = (
        (df["RSI_14"] < p["rsi_th"])
        & (df[f"{asset}_close"] > df[f"{asset}_open"])
        & (df["VIX_close"] < df["VIX_close"].shift(1))
    )

    daily_ret = df[f"{asset}_ret"]

    panic_triggered_raw = (
        risk_off
        & (daily_ret <= -p.get("panic_drop_pct", 999))
        & (df["RSI_14"] >= p.get("panic_rsi_low", 0))
        & (df["RSI_14"] <= p.get("panic_rsi_high", 0))
    )

    # V4.4 执行顺序中 dip_buy 优先于 panic。
    # 因此真正由 panic 插件接管的事件需要排除 dip_buy。
    panic_triggered_effective = panic_triggered_raw & (~dip_buy)

    return base_trend, risk_off, dip_buy, panic_triggered_raw, panic_triggered_effective


def calc_nav_and_turnover(pos, asset_ret):
    turnover = pos.diff().abs().fillna(0.0)
    net_ret = pos * asset_ret - turnover * TRANSACTION_COST_RATE
    nav = (1 + net_ret).cumprod()
    return nav, net_ret, turnover


def run_models(df):
    asset = "VOO"
    base_trend, risk_off, dip_buy, panic_raw, panic_effective = get_signals(df, V4_4_PARAMS)

    # ========================================================
    # Buy & Hold
    # ========================================================
    buyhold_pos = pd.Series(1.0, index=df.index)
    buyhold_nav = (1 + df[f"{asset}_ret"]).cumprod()
    buyhold_net_ret = df[f"{asset}_ret"].copy()
    buyhold_turnover = pd.Series(0.0, index=df.index)

    # ========================================================
    # V3 Position
    # ========================================================
    v3_pos_raw = np.select(
        [dip_buy, risk_off, base_trend],
        [1.0, V3_PARAMS["risk_pos"], 1.0],
        default=V3_PARAMS["risk_pos"],
    )
    v3_pos = pd.Series(v3_pos_raw, index=df.index).shift(1).fillna(V3_PARAMS["risk_pos"])
    v3_nav, v3_net_ret, v3_turnover = calc_nav_and_turnover(v3_pos, df[f"{asset}_ret"])

    # ========================================================
    # V4.4 Position
    # 逻辑顺序和 V4.4 保持一致：
    # dip_buy > panic_effective > risk_off > base_trend > default
    # ========================================================
    v4_pos_raw = np.select(
        [dip_buy, panic_effective, risk_off, base_trend],
        [1.0, V4_4_PARAMS["panic_reversal_pos"], V3_PARAMS["risk_pos"], 1.0],
        default=V3_PARAMS["risk_pos"],
    )
    v4_pos = pd.Series(v4_pos_raw, index=df.index).shift(1).fillna(V3_PARAMS["risk_pos"])
    v4_nav, v4_net_ret, v4_turnover = calc_nav_and_turnover(v4_pos, df[f"{asset}_ret"])

    df = df.copy()

    df["BuyHold_Position"] = buyhold_pos
    df["BuyHold_NAV"] = buyhold_nav
    df["BuyHold_NetRet"] = buyhold_net_ret
    df["BuyHold_Turnover"] = buyhold_turnover

    df["V3_Position"] = v3_pos
    df["V3_NAV"] = v3_nav
    df["V3_NetRet"] = v3_net_ret
    df["V3_Turnover"] = v3_turnover

    df["V4_Position"] = v4_pos
    df["V4_NAV"] = v4_nav
    df["V4_NetRet"] = v4_net_ret
    df["V4_Turnover"] = v4_turnover

    df["Base_Trend"] = base_trend
    df["Risk_Off"] = risk_off
    df["Dip_Buy"] = dip_buy
    df["Panic_Event_Raw"] = panic_raw
    df["Panic_Event_Effective"] = panic_effective

    return df


def max_recovery_days(nav):
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


def calc_period_metrics(df_slice, nav_col, netret_col=None, turnover_col=None, position_col=None):
    """
    返回某个阶段的完整指标。
    nav_col 会先 rebase 到 1.0。
    """
    if len(df_slice) < 2:
        return {
            "total_ret": 0.0,
            "cagr": 0.0,
            "max_dd": 0.0,
            "calmar": np.nan,
            "sharpe": np.nan,
            "sortino": np.nan,
            "annual_turnover": 0.0,
            "trade_count": 0,
            "avg_position": np.nan,
            "recovery_days": 0,
        }

    nav = df_slice[nav_col] / df_slice[nav_col].iloc[0]
    total_ret = nav.iloc[-1] - 1

    years = len(df_slice) / 252.0
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0.0

    drawdown = (nav - nav.cummax()) / nav.cummax()
    max_dd = drawdown.min()
    calmar = cagr / abs(max_dd) if max_dd != 0 else np.nan

    if netret_col and netret_col in df_slice.columns:
        daily_ret = df_slice[netret_col].fillna(0.0)
    else:
        daily_ret = nav.pct_change().fillna(0.0)

    ann_vol = daily_ret.std() * np.sqrt(252)
    sharpe = cagr / ann_vol if ann_vol and ann_vol > 0 else np.nan

    downside = daily_ret[daily_ret < 0]
    downside_vol = downside.std() * np.sqrt(252) if len(downside) > 1 else np.nan
    sortino = cagr / downside_vol if downside_vol and downside_vol > 0 else np.nan

    if turnover_col and turnover_col in df_slice.columns:
        turnover_daily = df_slice[turnover_col].fillna(0.0)
        total_turnover = turnover_daily.sum()
        annual_turnover = total_turnover / years if years > 0 else 0.0
        trade_count = int((turnover_daily > 1e-8).sum())
    else:
        annual_turnover = 0.0
        trade_count = 0

    if position_col and position_col in df_slice.columns:
        avg_position = df_slice[position_col].mean()
    else:
        avg_position = np.nan

    recovery_days = max_recovery_days(nav)

    return {
        "total_ret": total_ret,
        "cagr": cagr,
        "max_dd": max_dd,
        "calmar": calmar,
        "sharpe": sharpe,
        "sortino": sortino,
        "annual_turnover": annual_turnover,
        "trade_count": trade_count,
        "avg_position": avg_position,
        "recovery_days": recovery_days,
    }


def event_stats(events, ret_col):
    clean = events[ret_col].dropna()
    if clean.empty:
        return np.nan, np.nan
    return (clean > 0).mean(), clean.mean()


def print_event_report(df, event_col, title):
    print(f"\n🎯 {title}")
    panic_days = df[df[event_col]].copy()

    print(f"   • 总触发次数: {len(panic_days)} 次")
    if panic_days.empty:
        return

    # 信号日收盘价基准
    win_5_close, mean_5_close = event_stats(panic_days, "signal_close_fwd_5d_ret")
    win_20_close, mean_20_close = event_stats(panic_days, "signal_close_fwd_20d_ret")

    # 下一交易日开盘入场
    win_5_open, mean_5_open = event_stats(panic_days, "next_open_fwd_5d_ret")
    win_20_open, mean_20_open = event_stats(panic_days, "next_open_fwd_20d_ret")

    print("   • 信号日收盘价基准：")
    print(f"     - 未来 5 日胜率:  {win_5_close * 100:5.1f}% | 平均收益 {mean_5_close * 100:6.2f}%")
    print(f"     - 未来20 日胜率:  {win_20_close * 100:5.1f}% | 平均收益 {mean_20_close * 100:6.2f}%")

    print("   • 下一交易日开盘入场：")
    print(f"     - 未来 5 日胜率:  {win_5_open * 100:5.1f}% | 平均收益 {mean_5_open * 100:6.2f}%")
    print(f"     - 未来20 日胜率:  {win_20_open * 100:5.1f}% | 平均收益 {mean_20_open * 100:6.2f}%")

    print("\n   [历史触发日明细：下一交易日开盘入场口径]")
    for idx, row in panic_days.iterrows():
        print(
            f"     - {idx.strftime('%Y-%m-%d')} | "
            f"当日跌幅: {row['VOO_ret'] * 100:>5.1f}% | "
            f"RSI: {row['RSI_14']:>4.1f} | "
            f"信号收盘后20日: {row['signal_close_fwd_20d_ret'] * 100:+6.1f}% | "
            f"次日开盘后20日: {row['next_open_fwd_20d_ret'] * 100:+6.1f}%"
        )


def print_audit_report(df):
    print("=" * 100)
    print("🛡️ VOO 混合量化模型 —— 稳健性审计报告 v2 (Robustness Audit)")
    print("=" * 100)

    # ========================================================
    # 1. 宏观切片分析
    # ========================================================
    print("\n📊 宏观周期切片表现 (Regime Walk-Forward):")
    print("说明：每格为 CAGR / TotalRet / MaxDD")
    print(
        f"{'阶段':<28} | "
        f"{'Buy&Hold':<28} | "
        f"{'V3 基准':<28} | "
        f"{'V4.4 插件':<28}"
    )
    print("-" * 120)

    for name, (start, end) in MACRO_REGIMES.items():
        mask = (df.index >= start) & (df.index <= end)
        slice_df = df[mask]
        if slice_df.empty:
            continue

        bh = calc_period_metrics(
            slice_df,
            "BuyHold_NAV",
            "BuyHold_NetRet",
            "BuyHold_Turnover",
            "BuyHold_Position",
        )
        v3 = calc_period_metrics(
            slice_df,
            "V3_NAV",
            "V3_NetRet",
            "V3_Turnover",
            "V3_Position",
        )
        v4 = calc_period_metrics(
            slice_df,
            "V4_NAV",
            "V4_NetRet",
            "V4_Turnover",
            "V4_Position",
        )

        bh_txt = f"{bh['cagr']*100:>5.1f}% / {bh['total_ret']*100:>6.1f}% / {bh['max_dd']*100:>6.1f}%"
        v3_txt = f"{v3['cagr']*100:>5.1f}% / {v3['total_ret']*100:>6.1f}% / {v3['max_dd']*100:>6.1f}%"
        v4_txt = f"{v4['cagr']*100:>5.1f}% / {v4['total_ret']*100:>6.1f}% / {v4['max_dd']*100:>6.1f}%"

        print(f"{name:<28} | {bh_txt:<28} | {v3_txt:<28} | {v4_txt:<28}")

    # ========================================================
    # 2. 更完整的阶段指标
    # ========================================================
    print("\n📌 阶段详细指标：V3 vs V4.4")
    print(
        f"{'阶段':<28} | "
        f"{'模型':<8} | "
        f"{'CAGR':>8} {'Total':>8} {'MaxDD':>8} {'Calmar':>8} "
        f"{'Sharpe':>8} {'Sortino':>8} {'换手/年':>8} {'交易':>6} {'均仓':>8}"
    )
    print("-" * 130)

    for name, (start, end) in MACRO_REGIMES.items():
        mask = (df.index >= start) & (df.index <= end)
        slice_df = df[mask]
        if slice_df.empty:
            continue

        for label, nav_col, net_col, turn_col, pos_col in [
            ("V3", "V3_NAV", "V3_NetRet", "V3_Turnover", "V3_Position"),
            ("V4.4", "V4_NAV", "V4_NetRet", "V4_Turnover", "V4_Position"),
        ]:
            m = calc_period_metrics(slice_df, nav_col, net_col, turn_col, pos_col)
            print(
                f"{name:<28} | "
                f"{label:<8} | "
                f"{m['cagr']*100:>7.2f}% "
                f"{m['total_ret']*100:>7.2f}% "
                f"{m['max_dd']*100:>7.2f}% "
                f"{m['calmar']:>8.2f} "
                f"{m['sharpe']:>8.2f} "
                f"{m['sortino']:>8.2f} "
                f"{m['annual_turnover']:>8.2f} "
                f"{m['trade_count']:>6} "
                f"{m['avg_position']*100:>7.1f}%"
            )

    # ========================================================
    # 3. Panic 事件归因
    # ========================================================
    print_event_report(
        df,
        event_col="Panic_Event_Raw",
        title="Panic 插件原始触发事件归因 (Raw Trigger)",
    )

    print_event_report(
        df,
        event_col="Panic_Event_Effective",
        title="Panic 插件实际生效事件归因 (Effective Trigger)",
    )

    # ========================================================
    # 4. 最终全局对比
    # ========================================================
    bh = calc_period_metrics(
        df,
        "BuyHold_NAV",
        "BuyHold_NetRet",
        "BuyHold_Turnover",
        "BuyHold_Position",
    )
    v3 = calc_period_metrics(
        df,
        "V3_NAV",
        "V3_NetRet",
        "V3_Turnover",
        "V3_Position",
    )
    v4 = calc_period_metrics(
        df,
        "V4_NAV",
        "V4_NetRet",
        "V4_Turnover",
        "V4_Position",
    )

    print("\n" + "=" * 100)
    print("🏆 全局结论 (Total History):")
    print(
        f"   Buy & Hold: CAGR {bh['cagr'] * 100:.2f}% | "
        f"TotalRet {bh['total_ret'] * 100:.2f}% | "
        f"MaxDD {bh['max_dd'] * 100:.2f}% | "
        f"Calmar {bh['calmar']:.2f}"
    )
    print(
        f"   V3 基准   : CAGR {v3['cagr'] * 100:.2f}% | "
        f"TotalRet {v3['total_ret'] * 100:.2f}% | "
        f"MaxDD {v3['max_dd'] * 100:.2f}% | "
        f"Calmar {v3['calmar']:.2f} | "
        f"Sharpe {v3['sharpe']:.2f} | "
        f"Sortino {v3['sortino']:.2f} | "
        f"Trades {v3['trade_count']}"
    )
    print(
        f"   V4.4 插件 : CAGR {v4['cagr'] * 100:.2f}% | "
        f"TotalRet {v4['total_ret'] * 100:.2f}% | "
        f"MaxDD {v4['max_dd'] * 100:.2f}% | "
        f"Calmar {v4['calmar']:.2f} | "
        f"Sharpe {v4['sharpe']:.2f} | "
        f"Sortino {v4['sortino']:.2f} | "
        f"Trades {v4['trade_count']}"
    )
    print("=" * 100)

    # ========================================================
    # 5. 保存审计明细
    # ========================================================
    out_csv = OUTPUT_DIR / "voo_robustness_audit_v2_timeseries.csv"
    df.to_csv(out_csv, encoding="utf-8-sig")
    print(f"\n💾 审计明细已保存: {out_csv.name}")

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

def main():
    df = prep_data()
    df = run_models(df)
    print_audit_report(df)
    # todo:26-06-08:
    # 新增绘图：按宏观阶段绘制 Buy & Hold / V3 / V4.4 净值曲线，
    # 并自动标记各模型在该阶段内的最大回撤。
    plot_all_regimes_nav_with_drawdown(df)


if __name__ == "__main__":
    main()
