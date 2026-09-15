"""
主要持仓个股下载、四维风险确认与多周期 PDF 报告——完整可读自包含版。

本文件直接包含其调用的指数四维引擎、个股下载器和 PDF 绘图器，
不读取或导入其他项目 Python 文件。原文件保持不变。
"""

"""
VOO / QQQ 量化交易与四维风险确认执行层——完整可读自包含版。

本文件直接包含旧版风险雷达及 260904 v5 四维执行层，不读取或导入任何
其他项目 Python 文件。原文件保持不变。
"""

import pandas as pd
import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# todo 26-07-06
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors import LinearSegmentedColormap, Normalize
from matplotlib.patches import Rectangle, FancyBboxPatch

from pathlib import Path
from enum import Enum, auto

# todo 26-07-06
from typing import Optional


# ============================================================
# 1. 环境配置
# ============================================================

class EnvType(Enum):
    HOME = auto()
    WORK = auto()


def get_env_config(env: EnvType) -> dict:
    """根据运行环境返回路径配置。"""
    if env == EnvType.HOME:
        base_path = Path("/Users/evaseemefly/03data/05-spiders")
    elif env == EnvType.WORK:
        base_path = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
    else:
        raise ValueError(f"未知环境类型: {env}")

    config = {
        "csv_file": base_path / "broad_market_history/historical_broad_market_master.csv",
        "output_dir": base_path / "output/trade_msg",
        "figures_dir": base_path / "output/trade_msg/figures",
    }

    config["output_dir"].mkdir(parents=True, exist_ok=True)
    config["figures_dir"].mkdir(parents=True, exist_ok=True)

    return config


CURRENT_ENV = EnvType.WORK
CONFIG = get_env_config(CURRENT_ENV)

CSV_FILE_PATH = CONFIG["csv_file"]
OUTPUT_PATH = CONFIG["output_dir"]
FIGURES_PATH = CONFIG["figures_dir"]

print(f"⚙️ 运行环境: [{CURRENT_ENV.name}]")
print(f"📂 数据路径: {CSV_FILE_PATH}")

# ============================================================
# 2. 资产参数配置
# ============================================================

ASSET_CONFIG = {
    "QQQ": {
        # QQQ Three-Layer v3 Final:
        # MA200 | US10Y>0.15 | VIX>45 或 1.7×VIX_MA60 | RSI<30 | risk_pos=0.3。
        "strategy_name": "QQQ Three-Layer v3 Final",
        "strategy_profile": "qqq_three_layer_v3",

        # 趋势均线
        "ma_len": 200,
        "crash_ma_len": 200,

        # 利率阈值
        "us10y_th": 0.15,

        # VIX 风险阈值
        "vix_warning_low": 15,
        "vix_warning_high": 20,
        "vix_risk_th": 45,
        "vix_ma_multiplier": 1.7,
        "vix_crash_th": 30,
        "vix_extreme_th": 45,

        # RSI / 恐慌反转
        "rsi_th": 30,
        "panic_rsi_low": 35,
        "panic_rsi_high": 45,
        "panic_drop_pct": 0.045,

        # 三层仓位
        "risk_on_pos": 1.00,
        "risk_warning_pos": 0.60,
        "risk_pos": 0.30,
        "panic_reversal_pos": 0.40,
        "crash_pos": 0.25,

        # 实盘资金池与持仓
        "portfolio_value": 35730,
        "current_shares": 23,
    },

    "VOO": {
        # VOO v3_cagr Final:
        # 先用 MaxDD 不超过 -18%、Calmar >= 0.60 作为筛选约束，
        # 再人工选择样本外 2022-至今表现更优的第 7 名参数。
        # 最终执行参数：MA150 | US10Y>0.08 | VIX>38 | RSI<32 | risk_pos=0.3。
        "strategy_name": "VOO v3_cagr Final",
        "strategy_profile": "v3_cagr",
        "selection_objective": "maximize_cagr",
        "selection_max_dd_limit": -0.18,
        "selection_calmar_floor": 0.60,
        "aux_d_enabled": True,
        "aux_d_slope_lookback": 10,
        "aux_d_slope_epsilon": 0.0,
        "aux_d_breadth_lookback": 20,
        "aux_d_breadth_epsilon": 0.0,
        "aux_d_confirm_days": 3,
        "aux_d_recovery_days": 3,
        "aux_d_percentile_window": 252,

        # todo 26-07-06
        # D辅助风险PDF配置
        "aux_d_chart_lookback": 20,
        "aux_d_kline_lookback": 20,
        "aux_d_pdf_enabled": True,

        # 趋势均线
        "ma_len": 150,
        "crash_ma_len": 200,

        # 利率阈值
        "us10y_th": 0.08,

        # VIX 风险阈值
        "vix_warning_low": 15,
        "vix_warning_high": 20,
        "vix_risk_th": 38,
        "vix_ma_multiplier": 1.8,
        "vix_crash_th": 30,
        "vix_extreme_th": 38,

        # RSI / 恐慌反转
        "rsi_th": 32,
        "panic_rsi_low": 35,
        "panic_rsi_high": 45,
        "panic_drop_pct": 0.025,

        # 仓位参数
        "risk_on_pos": 1.00,
        "risk_warning_pos": 0.70,
        "risk_pos": 0.30,
        "panic_reversal_pos": 0.50,
        "crash_pos": 0.30,

        # 实盘资金池与持仓
        "portfolio_value": 53600,
        "current_shares": 33,
    },
}

# todo 26-07-06
# 扩充中文字体候选，适配PDF中的中文标题和注释
plt.rcParams["font.sans-serif"] = [
    "PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti",
    "Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans",
]
plt.rcParams["axes.unicode_minus"] = False


# ============================================================
# 3. 基础指标函数
# ============================================================

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """计算 RSI。"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def is_v3_cagr_profile(p: dict) -> bool:
    """判断是否使用 VOO v3_cagr Final 执行逻辑。"""
    return p.get("strategy_profile") == "v3_cagr"


def is_qqq_three_layer_profile(p: dict) -> bool:
    """判断是否使用 QQQ Three-Layer v3 Final 执行逻辑。"""
    return p.get("strategy_profile") == "qqq_three_layer_v3"


def is_three_layer_profile(p: dict) -> bool:
    """判断是否使用三层状态机，而非五层状态机。"""
    return is_v3_cagr_profile(p) or is_qqq_three_layer_profile(p)


def current_streak(mask: pd.Series) -> int:
    """计算当前连续 True 天数。"""
    if mask.empty:
        return 0

    count = 0
    for value in mask.fillna(False).iloc[::-1]:
        if bool(value):
            count += 1
        else:
            break
    return count


def last_percentile_rank(series: pd.Series, window: int) -> float:
    """
    计算最新值在最近 window 个交易日中的百分位。
    注意：这是 RSP/VOO 相对强弱百分位，不是真实成份股广度比例。
    """
    sample = series.dropna().tail(window)
    if sample.empty:
        return np.nan
    return float((sample <= sample.iloc[-1]).mean())


# todo 26-07-06
def rolling_percentile_rank(series: pd.Series, window: int) -> pd.Series:
    """
    计算每个交易日的滚动百分位排名。

    返回值范围为 0~1。当前值越接近 1，说明 RSP/VOO 比值
    在最近 window 个交易日中越高。该指标是相对强弱代理，
    不是标普500成份股真实广度比例。
    """
    min_periods = window
    return series.rolling(window=window, min_periods=min_periods).apply(
        lambda values: float(np.mean(values <= values[-1])),
        raw=True,
    )


# todo 26-07-06
def aux_d_risk_level_from_row(row: pd.Series) -> str:
    """根据最新一行 D 指标生成风险等级。"""
    if bool(row.get("aux_d_dual_weak_confirmed", False)):
        return "黄色预警（双弱已连续确认）"
    if bool(row.get("aux_d_dual_weak_raw", False)):
        return "黄色预警（双弱初现/未确认）"
    if int(row.get("aux_d_dual_weak_score", 0)) == 1:
        return "观察（单项转弱）"
    return "正常"


# todo 26-07-06
def percentile_zone(percentile_value: float) -> str:
    """将 0~1 的百分位转成便于阅读的区间描述。"""
    if pd.isna(percentile_value):
        return "数据不足"
    if percentile_value < 0.20:
        return "低位区"
    if percentile_value < 0.50:
        return "中低位区"
    if percentile_value < 0.80:
        return "中高位区"
    return "高位区"


def build_confirmed_dual_weak_state(
        ma_slope_weak: pd.Series,
        breadth_weak: pd.Series,
        confirm_days: int,
        recovery_days: int,
) -> pd.Series:
    """
    D 辅助风险状态：
    - B 和 C 连续 confirm_days 同时弱，进入 confirmed dual weak；
    - 任一指标连续 recovery_days 恢复，退出 confirmed 状态。
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

    return pd.Series(active_values, index=ma_slope_weak.index, name="aux_d_dual_weak_confirmed")


def load_master_data() -> pd.DataFrame:
    """读取主数据。"""
    if not CSV_FILE_PATH.exists():
        raise FileNotFoundError(f"找不到主数据文件: {CSV_FILE_PATH}")

    df = pd.read_csv(CSV_FILE_PATH)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()

    return df


def filter_real_trading_days(df: pd.DataFrame, asset: str) -> pd.DataFrame:
    """
    过滤真实交易日，避免周末 ffill 导致日涨跌幅失真。

    核心修正：
    - 去掉周六、周日。
    - 若存在 volume 字段，优先用 volume > 0 过滤。
    - 不在全局 master_df 上提前 ffill 周末价格。
    """
    df = df.copy()

    close_col = f"{asset}_close"
    volume_col = f"{asset}_volume"

    if close_col not in df.columns:
        raise KeyError(f"缺少字段: {close_col}")

    # 去掉没有资产价格的行
    df = df[df[close_col].notna()]

    # 去掉周末
    df = df[df.index.dayofweek < 5]

    # 若有成交量字段，过滤无成交日
    if volume_col in df.columns:
        df = df[df[volume_col] > 0]

    # 过滤后再 ffill 宏观字段，避免周末复制行参与日涨跌计算
    df = df.ffill()

    return df


def process_asset_indicators(df: pd.DataFrame, asset: str, p: dict) -> pd.DataFrame:
    """
    计算单资产指标。
    注意：df 必须已经过真实交易日过滤。
    """
    df = df.copy()

    close_col = f"{asset}_close"
    open_col = f"{asset}_open"

    df[f"{asset}_MA"] = df[close_col].rolling(p["ma_len"], min_periods=1).mean()
    df[f"{asset}_MA100"] = df[close_col].rolling(100, min_periods=1).mean()
    df[f"{asset}_MA200"] = df[close_col].rolling(200, min_periods=1).mean()

    # 关键：此处 daily_return 基于真实交易日序列计算
    df[f"{asset}_daily_return"] = df[close_col].pct_change()

    df["US10Y_diff_20"] = df["US10Y_close"].diff(20)
    df["HYG_MA60"] = df["HYG_close"].rolling(60, min_periods=1).mean()
    df["VIX_MA60"] = df["VIX_close"].rolling(60, min_periods=1).mean()

    df[f"RSI_14_{asset}"] = calculate_rsi(df[close_col])

    # 若没有 open 字段，用 close 代替，避免报错
    if open_col not in df.columns:
        df[open_col] = df[close_col]

    return df


def add_aux_d_indicators(df: pd.DataFrame, asset: str, p: dict) -> pd.DataFrame:
    """
    添加 D 辅助风险指标。
    该模块只用于 VOO v3_cagr 日报提示，不参与仓位计算。
    """
    if asset != "VOO" or not is_v3_cagr_profile(p) or not p.get("aux_d_enabled", False):
        return df

    required_cols = ["RSP_close", "VOO_close", "VOO_MA"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        df["aux_d_available"] = False
        return df

    df = df.copy()
    slope_lookback = p.get("aux_d_slope_lookback", 10)
    slope_epsilon = p.get("aux_d_slope_epsilon", 0.0)
    breadth_lookback = p.get("aux_d_breadth_lookback", 20)
    breadth_epsilon = p.get("aux_d_breadth_epsilon", 0.0)
    confirm_days = p.get("aux_d_confirm_days", 3)
    recovery_days = p.get("aux_d_recovery_days", 3)
    # todo 26-07-06
    percentile_window = p.get("aux_d_percentile_window", 252)

    log_ma = np.log(df["VOO_MA"])
    df["aux_d_ma_log_daily_slope"] = (log_ma - log_ma.shift(slope_lookback)) / slope_lookback
    df["aux_d_ma_slope_weak"] = df["aux_d_ma_log_daily_slope"] <= slope_epsilon
    df["aux_d_ma_slope_period_change"] = np.exp(df["aux_d_ma_log_daily_slope"] * slope_lookback) - 1

    df["aux_d_rsp_voo_ratio"] = df["RSP_close"] / df["VOO_close"]
    log_ratio = np.log(df["aux_d_rsp_voo_ratio"])
    df["aux_d_rsp_voo_rel_daily_mom"] = (log_ratio - log_ratio.shift(breadth_lookback)) / breadth_lookback
    df["aux_d_breadth_weak"] = df["aux_d_rsp_voo_rel_daily_mom"] <= breadth_epsilon
    df["aux_d_rsp_lookback_return"] = df["RSP_close"].pct_change(breadth_lookback)
    df["aux_d_voo_lookback_return"] = df["VOO_close"].pct_change(breadth_lookback)
    df["aux_d_rsp_voo_return_spread"] = df["aux_d_rsp_lookback_return"] - df["aux_d_voo_lookback_return"]
    # todo 26-07-06
    # 生成每日滚动百分位序列，供近20日曲线使用
    df["aux_d_rsp_voo_percentile"] = rolling_percentile_rank(
        df["aux_d_rsp_voo_ratio"],
        percentile_window,
    )

    df["aux_d_dual_weak_score"] = (
            df["aux_d_ma_slope_weak"].astype(int)
            + df["aux_d_breadth_weak"].astype(int)
    )
    df["aux_d_dual_weak_raw"] = df["aux_d_dual_weak_score"] == 2
    df["aux_d_dual_weak_confirmed"] = build_confirmed_dual_weak_state(
        df["aux_d_ma_slope_weak"],
        df["aux_d_breadth_weak"],
        confirm_days,
        recovery_days,
    )
    df["aux_d_available"] = True

    return df


# ============================================================
# 4. 市场状态分类
# ============================================================

def classify_market_state(df: pd.DataFrame, asset: str, p: dict) -> dict:
    """
    五层风险状态分类：

    1. Risk-On
    2. Risk-Warning
    3. Risk-Off
    4. Panic-Reversal
    5. Crash

    优先级：
    Crash > Panic-Reversal > Risk-Off > Risk-Warning > Dip-Buy > Risk-On > Trend-Weak
    """
    today = df.iloc[-1]
    yesterday = df.iloc[-2]

    close = today[f"{asset}_close"]
    open_price = today[f"{asset}_open"]
    ma = today[f"{asset}_MA"]
    ma100 = today[f"{asset}_MA100"]
    ma200 = today[f"{asset}_MA200"]

    rsi = today[f"RSI_14_{asset}"]
    daily_return = today[f"{asset}_daily_return"]

    base_trend = close > ma

    us10y_rising = today["US10Y_diff_20"] > p["us10y_th"]
    credit_weak = today["HYG_close"] < today["HYG_MA60"]

    if len(df) >= 3:
        hyg_last_3 = df["HYG_close"].iloc[-3:] < df["HYG_MA60"].iloc[-3:]
        hyg_divergence = (hyg_last_3.sum() == 3) and base_trend
    else:
        hyg_divergence = False

    vix = today["VIX_close"]

    # 三层状态机：
    # - QQQ Three-Layer v3: dip_buy -> risk_off -> base_trend -> default。
    # - VOO v3_cagr: risk_off -> dip_buy -> base_trend -> default。
    if is_three_layer_profile(p):
        strategy_name = p.get("strategy_name", "Three-Layer v3")
        vix_dynamic_risk = vix > today["VIX_MA60"] * p.get("vix_ma_multiplier", 1.8)
        vix_risk = (vix > p["vix_risk_th"]) or vix_dynamic_risk

        risk_off = us10y_rising or hyg_divergence or vix_risk
        raw_dip_buy = (
                rsi < p["rsi_th"]
                and close > open_price
                and today["VIX_close"] < yesterday["VIX_close"]
        )
        dip_buy = raw_dip_buy if is_qqq_three_layer_profile(p) else raw_dip_buy and not risk_off

        if is_qqq_three_layer_profile(p) and dip_buy:
            state = "Dip-Buy"
            target_position = p["risk_on_pos"]
            action_reason = (
                f"🚨【{strategy_name} Dip-Buy】RSI 跌破 {p['rsi_th']}，"
                f"收盘强于开盘且 VIX 回落，允许恢复至 {int(target_position * 100)}%。"
            )
        elif risk_off:
            state = "Risk-Off"
            target_position = p["risk_pos"]
            trigger_parts = []
            if us10y_rising:
                trigger_parts.append(f"US10Y 20日变化>{p['us10y_th']}")
            if hyg_divergence:
                trigger_parts.append("HYG 连续3日弱于 MA60 且趋势仍在")
            if vix > p["vix_risk_th"]:
                trigger_parts.append(f"VIX>{p['vix_risk_th']}")
            if vix_dynamic_risk:
                trigger_parts.append(f"VIX>{p.get('vix_ma_multiplier', 1.8)}×VIX_MA60")
            trigger_text = "；".join(trigger_parts) if trigger_parts else "风险条件触发"
            action_reason = (
                f"🛡️【{strategy_name} Risk-Off】{trigger_text}，"
                f"降低至 {int(target_position * 100)}% 防守仓。"
            )
        elif dip_buy:
            state = "Dip-Buy"
            target_position = p["risk_on_pos"]
            action_reason = (
                f"🚨【{strategy_name} 非 Risk-Off 抄底】RSI 跌破 {p['rsi_th']}，"
                f"收盘强于开盘且 VIX 回落，允许恢复至 {int(target_position * 100)}%。"
            )
        elif base_trend:
            state = "Risk-On"
            target_position = p["risk_on_pos"]
            action_reason = (
                f"📈【{strategy_name} 顺势做多】价格站上 MA{p['ma_len']}，"
                f"维持 {int(target_position * 100)}% 仓位。"
            )
        else:
            state = "Trend-Weak"
            target_position = p["risk_pos"]
            action_reason = (
                f"📉【{strategy_name} 趋势走弱】跌破 MA{p['ma_len']}，"
                f"保持 {int(target_position * 100)}% 防守仓。"
            )

        return {
            "state": state,
            "target_position": target_position,
            "action_reason": action_reason,

            "base_trend": base_trend,
            "us10y_rising": us10y_rising,
            "credit_weak": credit_weak,
            "hyg_divergence": hyg_divergence,
            "vix_warning": False,
            "vix_risk": vix_risk,
            "vix_dynamic_risk": vix_dynamic_risk,
            "risk_warning": False,
            "risk_off": risk_off,
            "panic_reversal": False,
            "crash": False,
            "raw_dip_buy": raw_dip_buy,
            "dip_buy": dip_buy,
            "daily_return": daily_return,
        }

    vix_warning = p["vix_warning_low"] <= vix <= p["vix_warning_high"]
    vix_risk = vix > p["vix_risk_th"]
    vix_crash = vix > p["vix_crash_th"]
    vix_extreme = vix > p["vix_extreme_th"]

    risk_warning = (credit_weak or vix_warning) and not vix_risk

    risk_off = (
            us10y_rising
            or hyg_divergence
            or (credit_weak and vix_risk)
            or vix_extreme
    )

    panic_reversal = (
            risk_off
            and daily_return <= -p["panic_drop_pct"]
            and p["panic_rsi_low"] <= rsi <= p["panic_rsi_high"]
    )

    crash = (
            (close < ma200 and vix_crash)
            or (close < ma100 and vix > p["vix_crash_th"])
    )

    dip_buy = (
            rsi < p["rsi_th"]
            and close > open_price
            and today["VIX_close"] < yesterday["VIX_close"]
            and not risk_off
    )

    if crash:
        state = "Crash"
        target_position = p["crash_pos"]
        action_reason = (
            f"🧊【Crash 防守】价格跌破关键均线且 VIX>{p['vix_crash_th']}，"
            f"降至 {int(target_position * 100)}% 防守仓。"
        )

    elif panic_reversal:
        state = "Panic-Reversal"
        target_position = p["panic_reversal_pos"]
        action_reason = (
            f"🟡【Risk-Off 恐慌反弹试探】单日跌幅 {daily_return * 100:.2f}%，"
            f"RSI={rsi:.2f}，允许小比例逆向试探至 {int(target_position * 100)}%。"
        )

    elif risk_off:
        state = "Risk-Off"
        target_position = p["risk_pos"]
        action_reason = (
            f"🛡️【宏观避险】信用/波动/利率触发风险，"
            f"降低至 {int(target_position * 100)}% 底仓防守。"
        )

    elif risk_warning:
        state = "Risk-Warning"
        target_position = p["risk_warning_pos"]
        action_reason = (
            f"⚠️【风险预警】HYG 或 VIX 转弱但未进入 Risk-Off，"
            f"降至 {int(target_position * 100)}% 观察仓。"
        )

    elif dip_buy:
        state = "Dip-Buy"
        target_position = p["risk_on_pos"]
        action_reason = (
            f"🚨【非 Risk-Off 抄底】RSI 跌破 {p['rsi_th']} 且 VIX 回落，"
            f"允许恢复至 {int(target_position * 100)}%。"
        )

    elif base_trend:
        state = "Risk-On"
        target_position = p["risk_on_pos"]
        action_reason = (
            f"📈【顺势做多】稳站 MA{p['ma_len']} 之上，"
            f"维持 {int(target_position * 100)}% 仓位。"
        )

    else:
        state = "Trend-Weak"
        target_position = p["risk_pos"]
        action_reason = (
            f"📉【趋势走弱】跌破 MA{p['ma_len']}，"
            f"保持 {int(target_position * 100)}% 防守仓。"
        )

    return {
        "state": state,
        "target_position": target_position,
        "action_reason": action_reason,

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
        "daily_return": daily_return,
    }


# ============================================================
# 5. 历史仓位序列，用于净值曲线
# ============================================================

def add_historical_position(df: pd.DataFrame, asset: str, p: dict) -> pd.DataFrame:
    """
    给历史数据增加 position，用于回测净值曲线。
    注意：
    - 当日信号用于下一交易日仓位，因此最后 shift(1)。
    - Panic-Reversal 只是 Risk-Off 下的小幅逆向提高仓位，不恢复满仓。
    """
    df = df.copy()

    close = df[f"{asset}_close"]
    ma = df[f"{asset}_MA"]
    ma100 = df[f"{asset}_MA100"]
    ma200 = df[f"{asset}_MA200"]
    rsi = df[f"RSI_14_{asset}"]

    base_trend = close > ma
    us10y_rising = df["US10Y_diff_20"] > p["us10y_th"]
    credit_weak = df["HYG_close"] < df["HYG_MA60"]

    hyg_divergence = (
                             (df["HYG_close"] < df["HYG_MA60"]).rolling(3).sum() == 3
                     ) & base_trend

    vix = df["VIX_close"]

    # 三层模型的历史仓位序列严格对齐各自回测规则。
    if is_three_layer_profile(p):
        vix_dynamic_risk = vix > df["VIX_MA60"] * p.get("vix_ma_multiplier", 1.8)
        vix_risk = (vix > p["vix_risk_th"]) | vix_dynamic_risk
        risk_off = us10y_rising | hyg_divergence | vix_risk

        open_col = f"{asset}_open"
        raw_dip_buy = (
                (rsi < p["rsi_th"])
                & (df[f"{asset}_close"] > df[open_col])
                & (df["VIX_close"] < df["VIX_close"].shift(1))
        )
        dip_buy = raw_dip_buy if is_qqq_three_layer_profile(p) else raw_dip_buy & (~risk_off)

        df["raw_dip_buy"] = raw_dip_buy
        df["dip_buy"] = dip_buy
        df["risk_off"] = risk_off
        df["vix_dynamic_risk"] = vix_dynamic_risk

        if is_qqq_three_layer_profile(p):
            conditions = [dip_buy, risk_off, base_trend]
            choices = [p["risk_on_pos"], p["risk_pos"], p["risk_on_pos"]]
        else:
            conditions = [risk_off, dip_buy, base_trend]
            choices = [p["risk_pos"], p["risk_on_pos"], p["risk_on_pos"]]

        df["position_raw"] = np.select(
            conditions,
            choices,
            default=p["risk_pos"],
        )

        df["position"] = df["position_raw"].shift(1).fillna(p["risk_pos"])

        return df

    vix_warning = (vix >= p["vix_warning_low"]) & (vix <= p["vix_warning_high"])
    vix_risk = vix > p["vix_risk_th"]
    vix_crash = vix > p["vix_crash_th"]
    vix_extreme = vix > p["vix_extreme_th"]

    risk_warning = (credit_weak | vix_warning) & (~vix_risk)

    risk_off = (
            us10y_rising
            | hyg_divergence
            | (credit_weak & vix_risk)
            | vix_extreme
    )

    panic_reversal = (
            risk_off
            & (df[f"{asset}_daily_return"] <= -p["panic_drop_pct"])
            & rsi.between(p["panic_rsi_low"], p["panic_rsi_high"])
    )

    crash = (
            ((close < ma200) & vix_crash)
            | ((close < ma100) & (vix > p["vix_crash_th"]))
    )

    open_col = f"{asset}_open"
    dip_buy = (
            (rsi < p["rsi_th"])
            & (df[f"{asset}_close"] > df[open_col])
            & (df["VIX_close"] < df["VIX_close"].shift(1))
            & (~risk_off)
    )

    df["position_raw"] = np.select(
        [
            crash,
            panic_reversal,
            risk_off,
            risk_warning,
            dip_buy,
            base_trend,
        ],
        [
            p["crash_pos"],
            p["panic_reversal_pos"],
            p["risk_pos"],
            p["risk_warning_pos"],
            p["risk_on_pos"],
            p["risk_on_pos"],
        ],
        default=p["risk_pos"],
    )

    df["position"] = df["position_raw"].shift(1).fillna(p["risk_pos"])

    return df


# ============================================================
# 6. 实盘金额与股数计算
# ============================================================

def build_execution_amount_plan(
        asset: str,
        p: dict,
        current_price: float,
        target_position: float,
        market_state: str,
) -> str:
    """
    根据目标仓位计算明日买卖股数。

    关键修正：
    - Risk-Off / Crash 状态下，如果当前仓位低于目标仓位，不主动买入。
    - Panic-Reversal 才允许 Risk-Off 下逆向补仓。
    """
    portfolio_value = p.get("portfolio_value")
    current_shares = p.get("current_shares")

    if portfolio_value is None or current_shares is None:
        return (
            "\n💰【实盘金额估算】\n"
            "   • 尚未配置 portfolio_value / current_shares，因此只输出目标仓位，不计算具体买卖股数。\n"
            "   • 可在 ASSET_CONFIG 中加入：'portfolio_value': 19890, 'current_shares': 13。\n"
        )

    current_value = current_shares * current_price
    target_value = portfolio_value * target_position
    diff_value = target_value - current_value
    trade_shares = int(abs(diff_value) / current_price)

    # 差额不足一股，不交易
    if abs(diff_value) < current_price:
        action = "持有不动"
        detail = "差额不足 1 股，暂不需要交易。"

    # Risk-Off / Crash 下禁止主动补仓
    elif market_state in ["Risk-Off", "Crash"] and diff_value > 0:
        action = "持有不动"
        detail = (
            f"当前市值 ${current_value:,.2f} 低于目标市值 ${target_value:,.2f}，"
            f"但市场状态为 {market_state}，禁止主动补仓；仅允许持有或减仓。"
        )

    elif diff_value > 0:
        action = f"买入 {trade_shares} 股"
        detail = f"预计增加约 ${trade_shares * current_price:,.2f}。"

    else:
        action = f"卖出 {trade_shares} 股"
        detail = f"预计回收约 ${trade_shares * current_price:,.2f}。"

    return (
        "\n💰【实盘金额估算】\n"
        f"   • 账户/资金池规模 : ${portfolio_value:,.2f}\n"
        f"   • 当前持仓       : {current_shares} 股，市值约 ${current_value:,.2f}\n"
        f"   • 目标仓位       : {target_position * 100:.0f}%，目标市值约 ${target_value:,.2f}\n"
        f"   • 明日动作       : {action}，{detail}\n"
    )


# ============================================================
# 7. 执行建议文本
# ============================================================

def build_execution_suggestion(
        df: pd.DataFrame,
        asset: str,
        p: dict,
        market_state: str,
        target_position: float,
        amount_plan: str,
) -> str:
    today = df.iloc[-1]

    recent = df.tail(60)
    support = recent[f"{asset}_close"].min()
    resistance = recent[f"{asset}_close"].max()
    mid_price = (support + resistance) / 2

    if market_state in ["Risk-On", "Dip-Buy"]:
        return f"""
📍 当前箱体区间: {support:.2f} — {resistance:.2f}（中轴 {mid_price:.2f}）
💡 执行建议:
   • 初始建仓: 当前价或 {mid_price:.2f} 附近（建议占总仓位 40%）
   • 第1次加仓: 回落至 MA{p['ma_len']} 附近 或 -6%（+20%）
   • 第2次加仓: 回落至箱体支撑 {support:.2f} 附近 或 -11%（+20%）
   • 第3次加仓: 极端恐慌（RSI<{p['rsi_th']}）（+20%）
{amount_plan}
"""

    if market_state == "Panic-Reversal":
        return f"""
📍 当前箱体区间: {support:.2f} — {resistance:.2f}（中轴 {mid_price:.2f}）
💡 Panic-Reversal 执行建议:
   • 当前仍是 Risk-Off，禁止恢复满仓。
   • 允许从防守仓小幅提高至 {target_position * 100:.0f}% 试探仓。
   • 若连续 2 日不创新低，或盘中跌破后收回，可考虑下一笔 10%。
   • 若 HYG 重新站上 MA60 且 VIX 回落至 18 以下，再恢复到 60%—70%。
   • 若跌破 MA100/MA200 或 VIX > {p['vix_crash_th']}，停止加仓并切入 Crash 防守。
{amount_plan}
"""

    if market_state == "Risk-Warning":
        return f"""
📍 当前为 Risk-Warning 状态：降低到 {target_position * 100:.0f}% 观察仓。
💡 执行建议:
   • 不追涨，不满仓。
   • 若 HYG 修复且 VIX 回落，可恢复趋势仓。
   • 若 HYG 跌破 MA60 且 VIX > {p['vix_risk_th']}，切换至 Risk-Off。
{amount_plan}
"""

    if market_state == "Risk-Off":
        # 三层模型没有 Panic-Reversal 插件，Risk-Off 期间只保留防守仓。
        if is_three_layer_profile(p):
            strategy_name = p.get("strategy_name", "Three-Layer v3")
            return f"""
📍 当前为 {strategy_name} Risk-Off 状态：保持 {target_position * 100:.0f}% 防守仓。
💡 执行建议:
   • 暂不新增趋势仓，也不做 Risk-Off 期间满仓抄底。
   • 等待 Risk-Off 条件解除后，再按 MA{p['ma_len']} 趋势状态恢复仓位。
   • 核心观察项：US10Y 20日变化、HYG/MA60、VIX 是否低于 {p['vix_risk_th']}。
{amount_plan}
"""

        return f"""
📍 当前为 Risk-Off 状态：保持 {target_position * 100:.0f}% 防守仓。
💡 执行建议:
   • 暂不新增趋势仓。
   • 即使当前仓位低于目标仓，普通 Risk-Off 下也不主动补仓。
   • 只有出现单日恐慌跌幅且 RSI 落入 {p['panic_rsi_low']}—{p['panic_rsi_high']}，才允许小仓试探。
{amount_plan}
"""

    if market_state == "Crash":
        return f"""
📍 当前为 Crash 状态：目标仓位 {target_position * 100:.0f}%。
💡 执行建议:
   • 只降仓，不补仓。
   • 等待 VIX 回落、价格重新站回关键均线后再恢复。
{amount_plan}
"""

    return f"""
📍 当前为 {market_state} 状态：目标仓位 {target_position * 100:.0f}%。
💡 执行建议:
    • 优先控制回撤，不做主动加仓。
{amount_plan}
"""


def build_aux_d_report(df: pd.DataFrame, asset: str, p: dict) -> str:
    """生成 VOO v3_cagr 的 D 辅助风险仪表盘文本。"""
    if asset != "VOO" or not is_v3_cagr_profile(p) or not p.get("aux_d_enabled", False):
        return ""

    if "aux_d_available" not in df.columns or not bool(df["aux_d_available"].iloc[-1]):
        return (
            "🧭【D辅助风险仪表盘】\n"
            "   • 状态          : 不可用，缺少 RSP/VOO 相关字段。\n"
            "   • 说明          : D仅作辅助提示，不参与A模型仓位计算。\n\n"
        )

    today = df.iloc[-1]
    slope_lookback = p.get("aux_d_slope_lookback", 10)
    breadth_lookback = p.get("aux_d_breadth_lookback", 20)
    percentile_window = p.get("aux_d_percentile_window", 252)

    ma_slope_weak = bool(today["aux_d_ma_slope_weak"])
    breadth_weak = bool(today["aux_d_breadth_weak"])
    dual_weak_raw = bool(today["aux_d_dual_weak_raw"])
    dual_weak_confirmed = bool(today["aux_d_dual_weak_confirmed"])
    weak_score = int(today["aux_d_dual_weak_score"])

    if dual_weak_confirmed:
        risk_level = "黄色预警（双弱已连续确认）"
    elif dual_weak_raw:
        risk_level = "黄色预警（双弱初现/未确认）"
    elif weak_score == 1:
        risk_level = "观察（单项转弱）"
    else:
        risk_level = "正常"

    trend_quality = "弱" if ma_slope_weak else "正常/修复"
    breadth_quality = "弱" if breadth_weak else "正常/扩散"
    raw_streak = current_streak(df["aux_d_dual_weak_raw"])
    confirmed_streak = current_streak(df["aux_d_dual_weak_confirmed"])
    slope_streak = current_streak(df["aux_d_ma_slope_weak"])
    breadth_streak = current_streak(df["aux_d_breadth_weak"])
    ratio_percentile = last_percentile_rank(df["aux_d_rsp_voo_ratio"], percentile_window)

    slope_daily = today["aux_d_ma_log_daily_slope"] * 100
    slope_period = today["aux_d_ma_slope_period_change"] * 100
    rel_daily_mom = today["aux_d_rsp_voo_rel_daily_mom"] * 100
    rsp_ret = today["aux_d_rsp_lookback_return"] * 100
    voo_ret = today["aux_d_voo_lookback_return"] * 100
    spread = today["aux_d_rsp_voo_return_spread"] * 100
    percentile_text = f"{ratio_percentile * 100:.1f}%" if pd.notna(ratio_percentile) else "N/A"

    return (
        "🧭【D辅助风险仪表盘】\n"
        "   • 定位          : 仅作风险质量提示，不改变A模型目标仓位。\n"
        f"   • 趋势质量      : {trend_quality} "
        f"(MA{p['ma_len']} {slope_lookback}日对数日斜率 {slope_daily:+.4f}%/日，"
        f"{slope_lookback}日累计 {slope_period:+.2f}%，连续弱 {slope_streak} 日)\n"
        f"   • 相对广度      : {breadth_quality} "
        f"(RSP {breadth_lookback}日 {rsp_ret:+.2f}% vs VOO {voo_ret:+.2f}%，"
        f"差值 {spread:+.2f}%，相对动量 {rel_daily_mom:+.4f}%/日)\n"
        f"   • 广度代理百分位: RSP/VOO 最近{percentile_window}日百分位 {percentile_text} "
        "(非成份股真实广度比例)\n"
        f"   • 双弱共振状态  : {'是' if dual_weak_raw else '否'} "
        f"(连续 {raw_streak} 日；3日确认状态 {'是' if dual_weak_confirmed else '否'}，"
        f"连续 {confirmed_streak} 日)\n"
        f"   • 风险等级      : {risk_level}\n"
        "   • 行动约束      : 不自动减仓；若为黄色预警，暂停额外主观加仓/杠杆/闲置现金追加入VOO。\n\n"
    )


def draw_candles(ax, recent: pd.DataFrame, asset: str) -> None:
    """绘制简洁 OHLC K 线，使用A股习惯：红涨绿跌。"""
    open_col = f"{asset}_open"
    high_col = f"{asset}_high"
    low_col = f"{asset}_low"
    close_col = f"{asset}_close"

    y_range = max(recent[high_col].max() - recent[low_col].min(), 1e-6)
    min_body = y_range * 0.006
    width = 0.58

    for x, (_, row) in enumerate(recent.iterrows()):
        open_price = row[open_col]
        high_price = row[high_col]
        low_price = row[low_col]
        close_price = row[close_col]
        up = close_price >= open_price
        color = "#c0392b" if up else "#16a085"

        ax.vlines(x, low_price, high_price, color=color, linewidth=1.2, alpha=0.95)
        body_low = min(open_price, close_price)
        body_height = max(abs(close_price - open_price), min_body)
        if abs(close_price - open_price) < min_body:
            body_low = (open_price + close_price) / 2 - body_height / 2
        rect = Rectangle(
            (x - width / 2, body_low),
            width,
            body_height,
            facecolor=color,
            edgecolor=color,
            linewidth=1.0,
            alpha=0.82,
        )
        ax.add_patch(rect)


def format_recent_xaxis(ax, recent: pd.DataFrame) -> None:
    """统一设置近20日图表的日期横轴。"""
    x_values = np.arange(len(recent))
    labels = [dt.strftime("%m-%d") for dt in recent.index]
    ax.set_xlim(-0.8, len(recent) - 0.2)
    ax.set_xticks(x_values)
    ax.set_xticklabels(labels, rotation=35, ha="right")


SLOPE_COLOR_STOPS = [
    "#00429d",
    "#4771b2",
    "#73a2c6",
    "#a5d5d8",
    "#ffffe0",
    "#ffbcaf",
    "#f4777f",
    "#cf3759",
    "#93003a",
]
SLOPE_CMAP = LinearSegmentedColormap.from_list("aux_d_slope_window_scale", SLOPE_COLOR_STOPS)


def slope_arrow(value: float, neutral_epsilon: float = 1e-10) -> str:
    """把MA斜率转换成方向箭头。"""
    if pd.isna(value) or abs(value) <= neutral_epsilon:
        return "→"
    return "↑" if value > 0 else "↓"


def slope_arrow_color(value: float, neutral_epsilon: float = 1e-10) -> str:
    """箭头颜色遵循A股习惯：红色向上，绿色向下。"""
    if pd.isna(value) or abs(value) <= neutral_epsilon:
        return "#7f8c8d"
    return "#c0392b" if value > 0 else "#16a085"


def slope_window_colors(values: pd.Series) -> list:
    """按当前展示窗口斜率上下限，用指定色标线性映射柱状图颜色。"""
    values = pd.Series(values).astype(float)
    valid = values.dropna()
    if valid.empty:
        return ["#bdc3c7" for _ in values]

    vmin = float(valid.min())
    vmax = float(valid.max())
    if np.isclose(vmin, vmax):
        return [SLOPE_CMAP(0.5) if pd.notna(v) else "#bdc3c7" for v in values]

    norm = Normalize(vmin=vmin, vmax=vmax)
    return [SLOPE_CMAP(norm(v)) if pd.notna(v) else "#bdc3c7" for v in values]


def add_aux_d_summary_table(
        ax,
        latest: pd.Series,
        asset: str,
        p: dict,
        risk_level: str,
) -> None:
    """在标题下方绘制当前D辅助风险摘要表，避免遮挡K线。"""
    latest_close = latest[f"{asset}_close"]
    latest_ma = latest[f"{asset}_MA"]
    latest_ma100 = latest[f"{asset}_MA100"]
    latest_ma200 = latest[f"{asset}_MA200"]
    latest_slope = latest["aux_d_ma_log_daily_slope"] * 100
    latest_breadth = latest["aux_d_rsp_voo_rel_daily_mom"] * 100

    columns = [
        "收盘",
        f"距MA{p['ma_len']}",
        "距MA100",
        "距MA200",
        "MA斜率",
        "RSP/VOO动量",
        "风险等级",
    ]
    values = [
        f"{latest_close:.2f}",
        f"{latest_close / latest_ma - 1:+.2%}",
        f"{latest_close / latest_ma100 - 1:+.2%}",
        f"{latest_close / latest_ma200 - 1:+.2%}",
        f"{slope_arrow(latest_slope)} {latest_slope:+.4f}%/日",
        f"{slope_arrow(latest_breadth)} {latest_breadth:+.4f}%/日",
        risk_level,
    ]

    ax.axis("off")
    table = ax.table(
        cellText=[values],
        colLabels=columns,
        cellLoc="center",
        loc="center",
        colWidths=[0.10, 0.12, 0.12, 0.12, 0.15, 0.17, 0.22],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8.4)
    table.scale(1.0, 1.38)
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#d0d7de")
        cell.set_linewidth(0.65)
        if row == 0:
            cell.set_facecolor("#eef5fb")
            cell.set_text_props(weight="bold")
        else:
            cell.set_facecolor("#fbfdff")


# ============================================================
# 8. 图表生成
# ============================================================

# todo 26-07-06
def plot_aux_d_risk_pdf(
        df: pd.DataFrame,
        asset: str,
        p: dict,
        trade_date: str,
        file_date_str: str,
) -> Optional[Path]:
    """
    生成 VOO D 辅助风险 PDF 报告。

    PDF 共三页：
    1. MA150 对数日斜率 + RSP/VOO 相对广度动量；
    2. RSP/VOO 近252日滚动百分位。
    3. 最近20个交易日K线 + MA均线 + RSI/VIX 风险参考。

    该报告仅展示辅助风险状态，不参与 A 模型仓位计算。
    """
    if (
            asset != "VOO"
            or not is_v3_cagr_profile(p)
            or not p.get("aux_d_enabled", False)
            or not p.get("aux_d_pdf_enabled", True)
    ):
        return None

    if "aux_d_available" not in df.columns or not bool(df["aux_d_available"].iloc[-1]):
        return None

    required_cols = [
        "aux_d_ma_log_daily_slope",
        "aux_d_rsp_voo_rel_daily_mom",
        "aux_d_rsp_voo_percentile",
        "aux_d_dual_weak_confirmed",
        f"{asset}_open",
        f"{asset}_high",
        f"{asset}_low",
        f"{asset}_close",
        f"{asset}_MA",
        f"{asset}_MA100",
        f"{asset}_MA200",
        f"RSI_14_{asset}",
        "VIX_close",
        "VIX_MA60",
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"⚠️ D辅助风险PDF未生成，缺少字段: {missing}")
        return None

    chart_lookback = int(p.get("aux_d_chart_lookback", 20))
    kline_lookback = int(p.get("aux_d_kline_lookback", chart_lookback))
    recent = df.tail(chart_lookback).copy()
    kline_recent = df.tail(kline_lookback).copy()
    if recent.empty:
        return None

    today = recent.iloc[-1]
    risk_level = aux_d_risk_level_from_row(today)
    slope_lookback = int(p.get("aux_d_slope_lookback", 10))
    breadth_lookback = int(p.get("aux_d_breadth_lookback", 20))
    percentile_window = int(p.get("aux_d_percentile_window", 252))

    slope_pct = recent["aux_d_ma_log_daily_slope"] * 100
    breadth_mom_pct = recent["aux_d_rsp_voo_rel_daily_mom"] * 100
    percentile_pct = recent["aux_d_rsp_voo_percentile"] * 100

    pdf_name = f"aux_d_risk_report_kline_{file_date_str}_{asset.lower()}.pdf"
    pdf_path = OUTPUT_PATH / pdf_name

    metadata = {
        "Title": f"{asset} D Auxiliary Risk Report - {trade_date}",
        "Author": "VOO v3_cagr Quant System",
        "Subject": "MA slope, RSP/VOO relative breadth, percentile and candlestick",
        "Keywords": "VOO, MA150, RSP, breadth, candlestick, quantitative risk",
    }

    with PdfPages(pdf_path, metadata=metadata) as pdf:
        # -------------------- 第1页：趋势质量 + 相对广度 --------------------
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        ax.plot(
            recent.index,
            slope_pct,
            marker="o",
            linewidth=2.0,
            label=f"MA{p['ma_len']} {slope_lookback}日对数日斜率",
        )
        ax.plot(
            recent.index,
            breadth_mom_pct,
            marker="s",
            linewidth=2.0,
            label=f"RSP/VOO {breadth_lookback}日相对动量",
        )
        ax.axhline(0.0, linestyle="--", linewidth=1.2, label="0轴（强弱分界）")

        confirmed_dates = recent.index[recent["aux_d_dual_weak_confirmed"].fillna(False)]
        if len(confirmed_dates) > 0:
            y_min, y_max = ax.get_ylim()
            marker_y = y_min + (y_max - y_min) * 0.04
            ax.scatter(
                confirmed_dates,
                np.full(len(confirmed_dates), marker_y),
                marker="v",
                s=55,
                label="双弱已确认",
                zorder=5,
            )

        latest_slope = slope_pct.iloc[-1]
        latest_breadth = breadth_mom_pct.iloc[-1]
        ax.scatter(recent.index[-1], latest_slope, s=90, zorder=6)
        ax.scatter(recent.index[-1], latest_breadth, s=90, zorder=6)
        status_text = (
            f"当前值\n"
            f"MA斜率：{latest_slope:+.4f}%/日\n"
            f"相对动量：{latest_breadth:+.4f}%/日\n"
            f"风险等级：{risk_level}"
        )
        ax.text(
            0.985,
            0.965,
            status_text,
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=10,
            bbox={"boxstyle": "round,pad=0.45", "alpha": 0.88},
        )

        ax.set_title(
            f"{asset} D辅助风险趋势图 1/3：趋势质量与相对广度\n"
            f"最近{chart_lookback}个交易日 | 截至 {trade_date} | 风险等级：{risk_level}",
            fontsize=15,
            fontweight="bold",
        )
        ax.set_xlabel("交易日期")
        ax.set_ylabel("变化速度（%/交易日）")
        ax.grid(True, linestyle="--", alpha=0.35)
        ax.legend(loc="best", fontsize=9)
        fig.autofmt_xdate(rotation=35)
        fig.text(
            0.02,
            0.025,
            "解读：两条曲线同时低于0时表示趋势动力与相对广度共同转弱；"
            "D模块仅提示风险，不改变A模型目标仓位。",
            fontsize=9,
        )
        fig.text(0.97, 0.025, "第 1 / 3 页", ha="right", fontsize=9)
        plt.tight_layout(rect=(0.02, 0.06, 0.98, 0.96))
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # -------------------- 第2页：近252日百分位 --------------------
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        valid_percentile = percentile_pct.dropna()
        if valid_percentile.empty:
            ax.text(
                0.5,
                0.5,
                f"历史数据不足，暂无法计算{percentile_window}日滚动百分位。",
                ha="center",
                va="center",
                transform=ax.transAxes,
                fontsize=14,
            )
        else:
            ax.plot(
                valid_percentile.index,
                valid_percentile,
                marker="o",
                linewidth=2.2,
                label=f"RSP/VOO {percentile_window}日百分位",
            )
            ax.axhline(20, linestyle="--", linewidth=1.0, label="20%：低位参考")
            ax.axhline(50, linestyle="--", linewidth=1.0, label="50%：中位参考")
            ax.axhline(80, linestyle="--", linewidth=1.0, label="80%：高位参考")
            ax.set_ylim(0, 100)

            latest_percentile = valid_percentile.iloc[-1]
            latest_rank_raw = latest_percentile / 100
            ax.scatter(valid_percentile.index[-1], latest_percentile, s=100, zorder=6)
            percentile_text = (
                f"当前值：{latest_percentile:.1f}%\n"
                f"区间：{percentile_zone(latest_rank_raw)}\n"
                f"窗口：近{percentile_window}个交易日"
            )
            ax.text(
                0.02,
                0.965,
                percentile_text,
                transform=ax.transAxes,
                ha="left",
                va="top",
                fontsize=10,
                bbox={"boxstyle": "round,pad=0.45", "alpha": 0.88},
            )

        ax.set_title(
            f"{asset} D辅助风险趋势图 2/3：RSP/VOO近{percentile_window}日百分位\n"
            f"展示最近{chart_lookback}个交易日 | 截至 {trade_date}",
            fontsize=15,
            fontweight="bold",
        )
        ax.set_xlabel("交易日期")
        ax.set_ylabel(f"近{percentile_window}日百分位（%）")
        ax.grid(True, linestyle="--", alpha=0.35)
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            ax.legend(loc="best", fontsize=9)
        fig.autofmt_xdate(rotation=35)
        fig.text(
            0.02,
            0.025,
            "说明：该百分位衡量RSP/VOO相对强弱在近一年中的位置，"
            "不是标普500成份股上涨比例。",
            fontsize=9,
        )
        fig.text(0.97, 0.025, "第 2 / 3 页", ha="right", fontsize=9)
        plt.tight_layout(rect=(0.02, 0.06, 0.98, 0.96))
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # -------------------- 第3页：20日K线 + MA均线 + RSI/VIX --------------------
        fig, (ax_price, ax_rsi, ax_vix) = plt.subplots(
            3,
            1,
            figsize=(11.69, 8.27),
            gridspec_kw={"height_ratios": [3.2, 1.1, 1.1]},
            sharex=True,
        )

        x_values = np.arange(len(kline_recent))
        draw_candles(ax_price, kline_recent, asset)

        ax_price.plot(
            x_values,
            kline_recent[f"{asset}_MA"],
            color="#f39c12",
            linewidth=1.8,
            label=f"MA{p['ma_len']}",
        )
        ax_price.plot(
            x_values,
            kline_recent[f"{asset}_MA100"],
            color="#16a085",
            linewidth=1.4,
            alpha=0.85,
            label="MA100",
        )
        ax_price.plot(
            x_values,
            kline_recent[f"{asset}_MA200"],
            color="#8e44ad",
            linewidth=1.4,
            alpha=0.85,
            label="MA200",
        )

        y_min, y_max = ax_price.get_ylim()
        d_raw_mask = kline_recent["aux_d_dual_weak_raw"].fillna(False)
        d_confirmed_mask = kline_recent["aux_d_dual_weak_confirmed"].fillna(False)
        for i, is_dual_weak in enumerate(d_raw_mask):
            if is_dual_weak:
                ax_price.axvspan(i - 0.5, i + 0.5, color="#f39c12", alpha=0.12, linewidth=0)
        confirmed_x = x_values[d_confirmed_mask.to_numpy()]
        if len(confirmed_x) > 0:
            ax_price.scatter(
                confirmed_x,
                np.full(len(confirmed_x), y_min + (y_max - y_min) * 0.05),
                marker="v",
                s=55,
                color="#c0392b",
                label="D双弱确认",
                zorder=6,
            )

        slope_sign = np.sign(kline_recent["aux_d_ma_log_daily_slope"].fillna(0))
        sign_change = slope_sign.ne(slope_sign.shift(1)).fillna(False)
        sign_change.iloc[0] = False
        for i in np.where(sign_change.to_numpy())[0]:
            slope_value = kline_recent["aux_d_ma_log_daily_slope"].iloc[i]
            label = "斜率转强" if slope_value > 0 else "斜率转弱"
            color = "#27ae60" if slope_value > 0 else "#c0392b"
            ax_price.axvline(i, color=color, linestyle="--", linewidth=1.0, alpha=0.75)
            ax_price.text(
                i,
                y_max,
                label,
                rotation=90,
                ha="right",
                va="top",
                fontsize=8,
                color=color,
            )

        latest = kline_recent.iloc[-1]
        latest_close = latest[f"{asset}_close"]
        latest_ma = latest[f"{asset}_MA"]
        latest_ma100 = latest[f"{asset}_MA100"]
        latest_ma200 = latest[f"{asset}_MA200"]
        ma_dist = latest_close / latest_ma - 1
        ma100_dist = latest_close / latest_ma100 - 1
        ma200_dist = latest_close / latest_ma200 - 1
        latest_slope_pct = latest["aux_d_ma_log_daily_slope"] * 100
        latest_breadth_pct = latest["aux_d_rsp_voo_rel_daily_mom"] * 100
        price_note = (
            f"收盘 {latest_close:.2f}\n"
            f"距MA{p['ma_len']} {ma_dist:+.2%}\n"
            f"距MA100 {ma100_dist:+.2%} | 距MA200 {ma200_dist:+.2%}\n"
            f"MA斜率 {latest_slope_pct:+.4f}%/日\n"
            f"RSP/VOO动量 {latest_breadth_pct:+.4f}%/日"
        )
        ax_price.text(
            0.985,
            0.965,
            price_note,
            transform=ax_price.transAxes,
            ha="right",
            va="top",
            fontsize=9,
            bbox={"boxstyle": "round,pad=0.45", "alpha": 0.88},
        )

        ax_price.set_title(
            f"{asset} D辅助风险趋势图 3/3：最近{kline_lookback}个交易日K线与关键均线\n"
            f"截至 {trade_date} | 底色=双弱共振日 | 虚线=MA斜率符号变化",
            fontsize=14,
            fontweight="bold",
        )
        ax_price.set_ylabel("价格")
        ax_price.grid(True, linestyle="--", alpha=0.28)
        ax_price.legend(loc="upper left", fontsize=8, ncol=4)

        ax_rsi.plot(
            x_values,
            kline_recent[f"RSI_14_{asset}"],
            color="#34495e",
            linewidth=1.8,
            marker="o",
            markersize=3.5,
            label="RSI(14)",
        )
        ax_rsi.axhline(p["rsi_th"], color="#c0392b", linestyle="--", linewidth=1.1, label=f"RSI阈值 {p['rsi_th']}")
        ax_rsi.set_ylabel("RSI")
        ax_rsi.set_ylim(0, 100)
        ax_rsi.grid(True, linestyle="--", alpha=0.28)
        ax_rsi.legend(loc="upper left", fontsize=8)

        ax_vix.plot(
            x_values,
            kline_recent["VIX_close"],
            color="#7f8c8d",
            linewidth=1.8,
            marker="o",
            markersize=3.5,
            label="VIX",
        )
        ax_vix.plot(
            x_values,
            kline_recent["VIX_MA60"],
            color="#2980b9",
            linewidth=1.4,
            linestyle="-.",
            label="VIX_MA60",
        )
        ax_vix.axhline(p["vix_risk_th"], color="#c0392b", linestyle="--", linewidth=1.1,
                       label=f"VIX风险阈值 {p['vix_risk_th']}")
        ax_vix.set_ylabel("VIX")
        ax_vix.grid(True, linestyle="--", alpha=0.28)
        ax_vix.legend(loc="upper left", fontsize=8, ncol=3)

        format_recent_xaxis(ax_vix, kline_recent)
        fig.text(
            0.02,
            0.025,
            "解读：本页用于观察D信号与实际价格行为的同步关系；D信号仍只作风险提示，不改变A模型仓位。",
            fontsize=9,
        )
        fig.text(0.97, 0.025, "第 3 / 3 页", ha="right", fontsize=9)
        plt.tight_layout(rect=(0.02, 0.06, 0.98, 0.95))
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

    return pdf_path


def plot_aux_d_risk_pdf_v2(
        df: pd.DataFrame,
        asset: str,
        p: dict,
        trade_date: str,
        file_date_str: str,
) -> Optional[Path]:
    """
    生成 VOO D 辅助风险 PDF 报告 v2。

    PDF 共三页：
    1. 综合风险图：摘要表 + K线/均线 + MA斜率箭头行 + MA斜率柱 + RSP/VOO动量 + RSI + VIX；
    2. RSP/VOO 近252日滚动百分位；
    3. 保留版趋势图：MA斜率 + RSP/VOO相对广度动量。

    该报告仅展示辅助风险状态，不参与 A 模型仓位计算。
    """
    if (
            asset != "VOO"
            or not is_v3_cagr_profile(p)
            or not p.get("aux_d_enabled", False)
            or not p.get("aux_d_pdf_enabled", True)
    ):
        return None

    if "aux_d_available" not in df.columns or not bool(df["aux_d_available"].iloc[-1]):
        return None

    required_cols = [
        "aux_d_ma_log_daily_slope",
        "aux_d_rsp_voo_rel_daily_mom",
        "aux_d_rsp_voo_percentile",
        "aux_d_dual_weak_raw",
        "aux_d_dual_weak_confirmed",
        f"{asset}_open",
        f"{asset}_high",
        f"{asset}_low",
        f"{asset}_close",
        f"{asset}_MA",
        f"{asset}_MA100",
        f"{asset}_MA200",
        f"RSI_14_{asset}",
        "VIX_close",
        "VIX_MA60",
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"⚠️ D辅助风险PDF未生成，缺少字段: {missing}")
        return None

    chart_lookback = int(p.get("aux_d_chart_lookback", 20))
    recent = df.tail(chart_lookback).copy()
    if recent.empty:
        return None

    today = recent.iloc[-1]
    risk_level = aux_d_risk_level_from_row(today)
    slope_lookback = int(p.get("aux_d_slope_lookback", 10))
    breadth_lookback = int(p.get("aux_d_breadth_lookback", 20))
    percentile_window = int(p.get("aux_d_percentile_window", 252))

    x_values = np.arange(len(recent))
    slope_pct = recent["aux_d_ma_log_daily_slope"] * 100
    breadth_mom_pct = recent["aux_d_rsp_voo_rel_daily_mom"] * 100
    percentile_pct = recent["aux_d_rsp_voo_percentile"] * 100

    pdf_name = f"aux_d_risk_report_integrated_v2_{file_date_str}_{asset.lower()}.pdf"
    pdf_path = OUTPUT_PATH / pdf_name

    metadata = {
        "Title": f"{asset} D Integrated Auxiliary Risk Report v2 - {trade_date}",
        "Author": "VOO v3_cagr Quant System",
        "Subject": "Integrated candlestick, MA slope arrows, RSP/VOO breadth and percentile",
        "Keywords": "VOO, MA150, RSP, breadth, candlestick, quantitative risk",
    }

    with PdfPages(pdf_path, metadata=metadata) as pdf:
        # -------------------- 第1页：综合风险图 --------------------
        fig = plt.figure(figsize=(11.69, 8.27))
        gs = fig.add_gridspec(
            7,
            1,
            height_ratios=[0.52, 2.85, 0.30, 0.54, 0.90, 0.78, 0.78],
            hspace=0.25,
        )
        ax_table = fig.add_subplot(gs[0])
        ax_price = fig.add_subplot(gs[1])
        ax_arrow = fig.add_subplot(gs[2], sharex=ax_price)
        ax_slope = fig.add_subplot(gs[3], sharex=ax_price)
        ax_breadth = fig.add_subplot(gs[4], sharex=ax_price)
        ax_rsi = fig.add_subplot(gs[5], sharex=ax_price)
        ax_vix = fig.add_subplot(gs[6], sharex=ax_price)

        fig.suptitle(
            f"{asset} D辅助风险综合图 1/3：价格行为、MA斜率与市场广度\n"
            f"最近{chart_lookback}个交易日 | 截至 {trade_date} | D信号仅作风险提示，不改变A模型仓位",
            fontsize=13.6,
            fontweight="bold",
            y=0.985,
        )
        add_aux_d_summary_table(ax_table, today, asset, p, risk_level)

        draw_candles(ax_price, recent, asset)
        ax_price.plot(
            x_values,
            recent[f"{asset}_MA"],
            color="#f39c12",
            linewidth=1.9,
            label=f"MA{p['ma_len']}",
        )
        ax_price.plot(
            x_values,
            recent[f"{asset}_MA100"],
            color="#16a085",
            linewidth=1.45,
            alpha=0.9,
            label="MA100",
        )
        ax_price.plot(
            x_values,
            recent[f"{asset}_MA200"],
            color="#8e44ad",
            linewidth=1.45,
            alpha=0.9,
            label="MA200",
        )

        d_raw_mask = recent["aux_d_dual_weak_raw"].fillna(False)
        d_confirmed_mask = recent["aux_d_dual_weak_confirmed"].fillna(False)
        for i, is_dual_weak in enumerate(d_raw_mask):
            if is_dual_weak:
                ax_price.axvspan(i - 0.5, i + 0.5, color="#f39c12", alpha=0.11, linewidth=0)
        for i, is_confirmed in enumerate(d_confirmed_mask):
            if is_confirmed:
                ax_price.axvspan(i - 0.5, i + 0.5, color="#c0392b", alpha=0.10, linewidth=0)

        ax_price.set_ylabel("价格")
        ax_price.grid(True, linestyle="--", alpha=0.25)
        ax_price.legend(loc="upper left", fontsize=8.4, ncol=3)

        # MA斜率方向单独成行，避免在K线主图上叠加符号。
        ax_arrow.set_ylim(0, 1)
        ax_arrow.set_yticks([])
        ax_arrow.set_ylabel("斜率\n方向", fontsize=8)
        ax_arrow.axhline(0.5, color="#ecf0f1", linewidth=0.8)
        for i, value in enumerate(slope_pct):
            ax_arrow.text(
                i,
                0.5,
                slope_arrow(value),
                color=slope_arrow_color(value),
                ha="center",
                va="center",
                fontsize=11,
                fontweight="bold",
            )
        for spine in ["top", "right", "left"]:
            ax_arrow.spines[spine].set_visible(False)

        slope_colors = slope_window_colors(slope_pct)
        ax_slope.bar(x_values, slope_pct, color=slope_colors, width=0.68, edgecolor="none", alpha=0.95)
        ax_slope.axhline(0, color="#34495e", linestyle="--", linewidth=0.8)
        slope_abs_max = max(abs(float(slope_pct.min())), abs(float(slope_pct.max())), 0.01)
        ax_slope.set_ylim(-slope_abs_max * 1.18, slope_abs_max * 1.18)
        ax_slope.set_ylabel("MA斜率\n%/日", fontsize=8)
        ax_slope.grid(True, axis="y", linestyle="--", alpha=0.22)
        ax_slope.text(
            0.995,
            0.88,
            "柱色按本窗口斜率上下限线性映射",
            transform=ax_slope.transAxes,
            ha="right",
            va="top",
            fontsize=7.4,
            color="#555555",
        )

        ax_breadth.plot(
            x_values,
            breadth_mom_pct,
            color="#d35400",
            linewidth=1.9,
            marker="o",
            markersize=3.7,
            label=f"RSP/VOO {breadth_lookback}日相对动量",
        )
        ax_breadth.axhline(0, color="#34495e", linestyle="--", linewidth=0.9, label="0轴")
        ax_breadth.fill_between(
            x_values,
            breadth_mom_pct,
            0,
            where=breadth_mom_pct >= 0,
            color="#27ae60",
            alpha=0.10,
        )
        ax_breadth.fill_between(
            x_values,
            breadth_mom_pct,
            0,
            where=breadth_mom_pct < 0,
            color="#c0392b",
            alpha=0.12,
        )
        ax_breadth.set_ylabel("RSP/VOO\n动量")
        ax_breadth.grid(True, linestyle="--", alpha=0.25)
        ax_breadth.legend(loc="upper left", fontsize=8)

        ax_rsi.plot(
            x_values,
            recent[f"RSI_14_{asset}"],
            color="#34495e",
            linewidth=1.7,
            marker="o",
            markersize=3.2,
            label="RSI(14)",
        )
        ax_rsi.axhline(
            p["rsi_th"],
            color="#c0392b",
            linestyle="--",
            linewidth=1.0,
            label=f"RSI阈值 {p['rsi_th']}",
        )
        ax_rsi.set_ylabel("RSI")
        ax_rsi.set_ylim(0, 100)
        ax_rsi.grid(True, linestyle="--", alpha=0.25)
        ax_rsi.legend(loc="upper left", fontsize=8)

        ax_vix.plot(
            x_values,
            recent["VIX_close"],
            color="#7f8c8d",
            linewidth=1.7,
            marker="o",
            markersize=3.2,
            label="VIX",
        )
        ax_vix.plot(
            x_values,
            recent["VIX_MA60"],
            color="#2980b9",
            linewidth=1.35,
            linestyle="-.",
            label="VIX_MA60",
        )
        ax_vix.axhline(
            p["vix_risk_th"],
            color="#c0392b",
            linestyle="--",
            linewidth=1.0,
            label=f"VIX风险阈值 {p['vix_risk_th']}",
        )
        ax_vix.set_ylabel("VIX")
        ax_vix.grid(True, linestyle="--", alpha=0.25)
        ax_vix.legend(loc="upper left", fontsize=8, ncol=3)

        for ax in [ax_price, ax_arrow, ax_slope, ax_breadth, ax_rsi]:
            ax.tick_params(labelbottom=False)
        format_recent_xaxis(ax_vix, recent)

        fig.text(
            0.02,
            0.025,
            "解读：表格看当前状态；主图看价格与均线；箭头行和柱状图看MA斜率方向与强弱；"
            "RSP/VOO动量看市场参与面扩散。",
            fontsize=8.6,
        )
        fig.text(0.97, 0.025, "第 1 / 3 页", ha="right", fontsize=9)
        fig.subplots_adjust(left=0.10, right=0.96, top=0.88, bottom=0.09, hspace=0.25)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # -------------------- 第2页：近252日百分位 --------------------
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        valid_percentile = percentile_pct.dropna()
        if valid_percentile.empty:
            ax.text(
                0.5,
                0.5,
                f"历史数据不足，暂无法计算{percentile_window}日滚动百分位。",
                ha="center",
                va="center",
                transform=ax.transAxes,
                fontsize=14,
            )
        else:
            ax.plot(
                valid_percentile.index,
                valid_percentile,
                marker="o",
                linewidth=2.2,
                label=f"RSP/VOO {percentile_window}日百分位",
            )
            ax.axhline(20, linestyle="--", linewidth=1.0, label="20%：低位参考")
            ax.axhline(50, linestyle="--", linewidth=1.0, label="50%：中位参考")
            ax.axhline(80, linestyle="--", linewidth=1.0, label="80%：高位参考")
            ax.set_ylim(0, 100)

            latest_percentile = valid_percentile.iloc[-1]
            latest_rank_raw = latest_percentile / 100
            ax.scatter(valid_percentile.index[-1], latest_percentile, s=100, zorder=6)
            percentile_text = (
                f"当前值：{latest_percentile:.1f}%\n"
                f"区间：{percentile_zone(latest_rank_raw)}\n"
                f"窗口：近{percentile_window}个交易日"
            )
            ax.text(
                0.02,
                0.965,
                percentile_text,
                transform=ax.transAxes,
                ha="left",
                va="top",
                fontsize=10,
                bbox={"boxstyle": "round,pad=0.45", "alpha": 0.88},
            )

        ax.set_title(
            f"{asset} D辅助风险综合图 2/3：RSP/VOO近{percentile_window}日百分位\n"
            f"展示最近{chart_lookback}个交易日 | 截至 {trade_date}",
            fontsize=15,
            fontweight="bold",
        )
        ax.set_xlabel("交易日期")
        ax.set_ylabel(f"近{percentile_window}日百分位（%）")
        ax.grid(True, linestyle="--", alpha=0.35)
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            ax.legend(loc="best", fontsize=9)
        fig.autofmt_xdate(rotation=35)
        fig.text(
            0.02,
            0.025,
            "说明：该百分位衡量RSP/VOO相对强弱在近一年中的位置，不是标普500成份股上涨比例。",
            fontsize=9,
        )
        fig.text(0.97, 0.025, "第 2 / 3 页", ha="right", fontsize=9)
        plt.tight_layout(rect=(0.02, 0.06, 0.98, 0.96))
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # -------------------- 第3页：保留版趋势质量 + 相对广度 --------------------
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        ax.plot(
            recent.index,
            slope_pct,
            marker="o",
            linewidth=2.0,
            label=f"MA{p['ma_len']} {slope_lookback}日对数日斜率",
        )
        ax.plot(
            recent.index,
            breadth_mom_pct,
            marker="s",
            linewidth=2.0,
            label=f"RSP/VOO {breadth_lookback}日相对动量",
        )
        ax.axhline(0.0, linestyle="--", linewidth=1.2, label="0轴（强弱分界）")

        confirmed_dates = recent.index[recent["aux_d_dual_weak_confirmed"].fillna(False)]
        if len(confirmed_dates) > 0:
            y_min, y_max = ax.get_ylim()
            marker_y = y_min + (y_max - y_min) * 0.04
            ax.scatter(
                confirmed_dates,
                np.full(len(confirmed_dates), marker_y),
                marker="v",
                s=55,
                label="双弱已确认",
                zorder=5,
            )

        latest_slope = slope_pct.iloc[-1]
        latest_breadth = breadth_mom_pct.iloc[-1]
        ax.scatter(recent.index[-1], latest_slope, s=90, zorder=6)
        ax.scatter(recent.index[-1], latest_breadth, s=90, zorder=6)

        ax.set_title(
            f"{asset} D辅助风险综合图 3/3：趋势质量与相对广度\n"
            f"最近{chart_lookback}个交易日 | 截至 {trade_date} | 风险等级：{risk_level}",
            fontsize=15,
            fontweight="bold",
        )
        ax.set_xlabel("交易日期")
        ax.set_ylabel("变化速度（%/交易日）")
        ax.grid(True, linestyle="--", alpha=0.35)
        ax.legend(loc="best", fontsize=9)
        fig.autofmt_xdate(rotation=35)
        fig.text(
            0.02,
            0.025,
            "解读：本页保留原始双线视角；两条曲线同时低于0时表示趋势动力与相对广度共同转弱。",
            fontsize=9,
        )
        fig.text(0.97, 0.025, "第 3 / 3 页", ha="right", fontsize=9)
        plt.tight_layout(rect=(0.02, 0.06, 0.98, 0.96))
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

    return pdf_path


def plot_snapshot_with_levels(
        df: pd.DataFrame,
        asset: str,
        p: dict,
        target_position: float,
        market_state: str,
        trade_date: str,
        file_date_str: str,
) -> Path:
    """生成雷达图表。"""
    recent = df.tail(60)
    support = recent[f"{asset}_close"].min()
    resistance = recent[f"{asset}_close"].max()
    mid_price = (support + resistance) / 2

    fig, (ax1, ax2, ax3, ax4) = plt.subplots(
        4,
        1,
        figsize=(14, 18),
        gridspec_kw={"height_ratios": [2.3, 2.0, 1.0, 1.0]},
        sharex=False,
    )

    today = df.iloc[-1]

    # 主价格图
    ax1.plot(df.index, df[f"{asset}_close"], label=f"{asset} Price", color="#2c3e50", linewidth=1.5)
    ax1.plot(df.index, df[f"{asset}_MA"], label=f"MA{p['ma_len']}", color="#e67e22", linewidth=2)
    ax1.plot(df.index, df[f"{asset}_MA100"], label="MA100", color="#16a085", alpha=0.8)
    ax1.plot(df.index, df[f"{asset}_MA200"], label="MA200", color="#c0392b", alpha=0.8)

    dot_color = "#27ae60" if target_position >= 0.7 else "#c0392b"
    ax1.scatter(
        today.name,
        today[f"{asset}_close"],
        color=dot_color,
        s=160,
        zorder=5,
        edgecolors="white",
        linewidth=2,
    )

    ax1.set_title(
        f"{asset} Daily Radar | {trade_date} | State: {market_state} | Target: {target_position * 100:.0f}%",
        fontsize=16,
        fontweight="bold",
    )
    ax1.legend(loc="upper left")
    ax1.grid(True, linestyle="--", alpha=0.4)

    # 箱体图
    recent_df = df.tail(120)
    ax2.plot(recent_df.index, recent_df[f"{asset}_close"], label=f"{asset} Close", color="#2980b9", linewidth=2)
    ax2.plot(recent_df.index, recent_df[f"{asset}_MA"], label=f"MA{p['ma_len']}", color="#f39c12", linewidth=1.5)

    box_start = recent.index[0]
    box_end = recent.index[-1]
    ax2.axvspan(box_start, box_end, color="lightgray", alpha=0.2, label="Box Horizon 60d")

    ax2.axhline(resistance, color="red", linestyle="--", alpha=0.6, label=f"Resistance {resistance:.2f}")
    ax2.axhline(support, color="green", linestyle="--", alpha=0.6, label=f"Support {support:.2f}")
    ax2.axhline(mid_price, color="blue", linestyle="-.", alpha=0.8, label=f"Mid {mid_price:.2f}")

    buy1_price = resistance * 0.94
    buy2_price = resistance * 0.89
    ax2.axhline(buy1_price, color="purple", linestyle=":", linewidth=2, label=f"Buy1 -6% {buy1_price:.2f}")
    ax2.axhline(buy2_price, color="brown", linestyle=":", linewidth=2, label=f"Buy2 -11% {buy2_price:.2f}")

    # 三层模型图表提示改为回测一致的优先级口径。
    risk_note = (
        "QQQ v3: Dip-Buy 优先；Risk-Off 次之"
        if is_qqq_three_layer_profile(p)
        else "v3_cagr: Risk-Off 优先；Dip-Buy 仅在非 Risk-Off 下生效"
        if is_v3_cagr_profile(p)
        else "Risk-Off 下禁止恢复满仓；Panic-Reversal 仅允许小仓试探"
    )

    ax2.text(
        0.02,
        0.05,
        risk_note,
        transform=ax2.transAxes,
        color="red",
        fontsize=11,
        fontweight="bold",
        bbox=dict(facecolor="white", alpha=0.85, edgecolor="lightgray"),
    )

    ax2.set_title(f"{asset} 近期箱体与潜在买点", fontsize=14)
    ax2.legend(loc="lower left", fontsize=9, ncol=2)
    ax2.grid(True, alpha=0.3)

    # RSI
    ax3.plot(df.index, df[f"RSI_14_{asset}"], label="RSI(14)", color="#8e44ad", linewidth=1.5)
    ax3.axhline(p["rsi_th"], color="red", linestyle="--", alpha=0.6, label=f"RSI {p['rsi_th']}")
    # 三层模型不展示 Panic RSI 区间，避免和最终策略逻辑混淆。
    if not is_three_layer_profile(p):
        ax3.axhline(p["panic_rsi_low"], color="orange", linestyle="--", alpha=0.6, label="Panic RSI Low")
        ax3.axhline(p["panic_rsi_high"], color="orange", linestyle="--", alpha=0.6, label="Panic RSI High")
    ax3.legend(loc="upper left")
    ax3.grid(True, alpha=0.3)

    # 仓位序列
    ax4.plot(df.index, df["position"], label="Target Position", color="#34495e", linewidth=1.8)
    ax4.set_ylim(0, 1.05)
    ax4.set_title("Historical Target Position")
    ax4.legend(loc="upper left")
    ax4.grid(True, alpha=0.3)

    # 三层模型输出文件名单独标记，避免和旧五层 V3 图混淆。
    file_strategy_tag = (
        "v3_cagr"
        if is_v3_cagr_profile(p)
        else "qqq_three_layer_v3"
        if is_qqq_three_layer_profile(p)
        else "v3"
    )
    pic_name = f"index_signal_{file_strategy_tag}_{file_date_str}_{asset.lower()}.png"
    save_path = FIGURES_PATH / pic_name

    plt.tight_layout()
    fig.savefig(save_path, dpi=160, bbox_inches="tight")
    plt.close(fig)

    return save_path


# ============================================================
# 9. 报告生成
# ============================================================

def generate_daily_report():
    print("=" * 80)
    print("🚀 VOO / QQQ 量化交易信号生成器 V3 启动")
    print("=" * 80)

    master_df = load_master_data()

    for asset, p in ASSET_CONFIG.items():
        print(f"\n⏳ 正在处理 {asset}...")

        # 关键修正：先过滤真实交易日，再计算指标
        asset_df = filter_real_trading_days(master_df, asset)
        df = process_asset_indicators(asset_df, asset, p)
        df = add_aux_d_indicators(df, asset, p)

        # 加入历史仓位，用于净值/仓位图
        df = add_historical_position(df, asset, p)

        today = df.iloc[-1]

        trade_date = today.name.strftime("%Y-%m-%d")
        file_date_str = today.name.strftime("%Y_%m_%d")

        signal = classify_market_state(df, asset, p)

        market_state = signal["state"]
        target_position = signal["target_position"]
        action_reason = signal["action_reason"]

        current_price = today[f"{asset}_close"]

        amount_plan = build_execution_amount_plan(
            asset=asset,
            p=p,
            current_price=current_price,
            target_position=target_position,
            market_state=market_state,
        )

        exec_suggestion = build_execution_suggestion(
            df=df,
            asset=asset,
            p=p,
            market_state=market_state,
            target_position=target_position,
            amount_plan=amount_plan,
        )
        aux_d_report = build_aux_d_report(df, asset, p)
        # todo 26-07-06
        # 生成VOO的D辅助风险两页PDF，不影响A模型交易逻辑
        try:
            aux_d_pdf_path = plot_aux_d_risk_pdf_v2(
                df=df,
                asset=asset,
                p=p,
                trade_date=trade_date,
                file_date_str=file_date_str,
            )
        except Exception as exc:
            aux_d_pdf_path = None
            print(f"⚠️ D辅助风险PDF生成失败，不影响主日报：{exc}")
        # todo 26-07-06
        aux_d_pdf_line = (
            f"📎【D辅助风险PDF】{aux_d_pdf_path.name}\n\n"
            if aux_d_pdf_path is not None
            else ""
        )

        trend_dist = (today[f"{asset}_close"] / today[f"{asset}_MA"] - 1) * 100

        # 日报标题使用策略名，三层模型明确标注最终参数方案。
        strategy_name = p.get("strategy_name", f"{asset} V3")
        strategy_param_line = ""
        if is_three_layer_profile(p):
            param_label = "QQQ v3 参数" if is_qqq_three_layer_profile(p) else "v3_cagr 参数"
            strategy_param_line = (
                f"🎛️【{param_label}】MA{p['ma_len']} | US10Y>{p['us10y_th']} | "
                f"VIX>{p['vix_risk_th']} 或 VIX>{p.get('vix_ma_multiplier', 1.8)}×MA60 | "
                f"RSI<{p['rsi_th']} | Risk仓位 {p['risk_pos'] * 100:.0f}%\n"
            )
            if is_v3_cagr_profile(p):
                strategy_param_line += (
                    f"🎚️【参数筛选约束】MaxDD≥{p['selection_max_dd_limit'] * 100:.0f}% | "
                    f"Calmar≥{p['selection_calmar_floor']:.2f} | 样本外优先\n"
                )
            strategy_param_line += "\n"

        report_content = (
            f"{'=' * 60}\n"
            f"📊 {asset} 极客量化交易日报 {strategy_name} | 结算日: {trade_date}\n"
            f"{'=' * 60}\n\n"
            f"{strategy_param_line}"
            f"🎯【明日实盘交易指令】\n"
            f"   市场状态     : {market_state}\n"
            f"   执行目标仓位 : {target_position * 100:.0f}%\n"
            f"   逻辑触发说明 : {action_reason}\n"
            f"{exec_suggestion}\n"
            f"{aux_d_report}"
            f"{aux_d_pdf_line}"
            f"🔍【关键指标快照】\n"
            f"   • {asset} 价格  : {today[f'{asset}_close']:.2f} "
            f"(MA{p['ma_len']}: {today[f'{asset}_MA']:.2f})\n"
            f"   • MA100         : {today[f'{asset}_MA100']:.2f}\n"
            f"   • MA200         : {today[f'{asset}_MA200']:.2f}\n"
            f"   • 均线偏离度    : {trend_dist:+.2f}%\n"
            f"   • RSI (14)      : {today[f'RSI_14_{asset}']:.2f}\n"
            f"   • 单日涨跌幅    : {signal['daily_return'] * 100:+.2f}%\n"
            f"   • 美债20日动量  : {today['US10Y_diff_20']:+.2f}\n"
            f"   • HYG 信用      : {today['HYG_close']:.2f} "
            f"(MA60: {today['HYG_MA60']:.2f})\n"
            f"   • VIX 恐慌      : {today['VIX_close']:.2f}\n"
            f"{'=' * 60}\n"
        )

        print(report_content)

        # 三层模型文本报告文件名单独标记，避免覆盖旧五层 V3 结果。
        file_strategy_tag = (
            "v3_cagr"
            if is_v3_cagr_profile(p)
            else "qqq_three_layer_v3"
            if is_qqq_three_layer_profile(p)
            else "v3"
        )
        txt_name = f"index_signal_{file_strategy_tag}_{file_date_str}_{asset.lower()}.txt"
        txt_path = OUTPUT_PATH / txt_name
        txt_path.write_text(report_content, encoding="utf-8")
        print(f"💾 文本报告已保存: {txt_path.name}")

        chart_path = plot_snapshot_with_levels(
            df=df,
            asset=asset,
            p=p,
            target_position=target_position,
            market_state=market_state,
            trade_date=trade_date,
            file_date_str=file_date_str,
        )
        print(f"🖼️ 图表已保存: {chart_path.name}")
        # todo 26-07-06
        if aux_d_pdf_path is not None:
            print(f"📄 D辅助风险PDF已保存: {aux_d_pdf_path.name}")

    print("\n" + "=" * 80)
    print("🎉 VOO / QQQ 全部信号生成完成")
    print(f"📂 输出目录: {OUTPUT_PATH}")
    print("=" * 80)


# ============================================================
# 10. 四维风险确认执行层（260904 v5，自包含整合）
# ============================================================
# 保存上方旧版函数/参数的静态引用。后续同名函数可以覆盖主模块名称，
# 但四维层仍能明确调用原始风险雷达实现，不产生递归，也不读取其他源码文件。
import json
import math
from types import SimpleNamespace as _SimpleNamespace
from typing import Any, Dict, List, Optional

VERSION_TAG = "4dim_260909_v6"

legacy = _SimpleNamespace(
    OUTPUT_PATH=OUTPUT_PATH,
    FIGURES_PATH=FIGURES_PATH,
    ASSET_CONFIG=ASSET_CONFIG,
    process_asset_indicators=process_asset_indicators,
    add_historical_position=add_historical_position,
    load_master_data=load_master_data,
    filter_real_trading_days=filter_real_trading_days,
    add_aux_d_indicators=add_aux_d_indicators,
    classify_market_state=classify_market_state,
    plot_snapshot_with_levels=plot_snapshot_with_levels,
    plot_aux_d_risk_pdf_v2=plot_aux_d_risk_pdf_v2,
)

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
    d_confirmed = bool(row.get("aux_d_dual_weak_confirmed", False)) if pd.notna(
        row.get("aux_d_dual_weak_confirmed", False)) else False
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


# ============================================================
# 个股历史数据下载器（由 indivalual_stocks_download_v2.py 整合）
# ============================================================
import os
import time
import random
import pandas as pd
import yfinance as yf
import numpy as np
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path
import arrow
from enum import Enum, auto
from curl_cffi import requests as cffi_requests


# ==================== 1. 定义环境与代理枚举 ====================
class DownloaderEnvType(Enum):
    HOME = auto()
    WORK = auto()


# ==================== 2. 配置获取函数 ====================
def get_downloader_env_config(env: DownloaderEnvType) -> dict:
    if env == DownloaderEnvType.HOME:
        base_path = Path("/Users/evaseemefly/03data/05-spiders")
        proxy_url = 'http://127.0.0.1:1087'
    elif env == DownloaderEnvType.WORK:
        base_path = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
        proxy_url = 'http://127.0.0.1:1087'  # Mac V2Ray 端口
    else:
        raise ValueError(f"未知的环境类型: {env}")

    config = {
        'csv_file': base_path / "broad_market_history/historical_broad_market_master.csv",
        'output_dir': base_path / "output/trade_msg",
        'figures_dir': base_path / "output/trade_msg/figures",
        'ind_stock_dir': base_path / "individual_stocks",  # 个股专用目录
        'proxy_url': proxy_url
    }

    # 创建目录
    config['output_dir'].mkdir(parents=True, exist_ok=True)
    config['figures_dir'].mkdir(parents=True, exist_ok=True)
    config['ind_stock_dir'].mkdir(parents=True, exist_ok=True)

    return config


# ==================== 3. 顶层配置加载 ====================
DOWNLOADER_CURRENT_ENV = DownloaderEnvType.WORK
DOWNLOADER_CONFIG = get_downloader_env_config(DOWNLOADER_CURRENT_ENV)

DOWNLOADER_PROXY_URL = DOWNLOADER_CONFIG['proxy_url']

os.environ['HTTP_PROXY'] = DOWNLOADER_PROXY_URL
os.environ['HTTPS_PROXY'] = DOWNLOADER_PROXY_URL
if 'NO_PROXY' in os.environ:
    del os.environ['NO_PROXY']

print(f"⚙️ 运行环境: [{DOWNLOADER_CURRENT_ENV.name}]")
print(f"🌐 系统代理已设置为: {DOWNLOADER_PROXY_URL}")
print(f"📂 个股数据将分别保存至: {DOWNLOADER_CONFIG['ind_stock_dir']}\n")

# ==================== 4. 防封禁 Session + 数据质量防线 ====================
# todo 26-09-09
import json
from datetime import datetime as dt_datetime, time as dt_time, timedelta as dt_timedelta
from zoneinfo import ZoneInfo
from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    Holiday,
    nearest_workday,
    USMartinLutherKingJr,
    USPresidentsDay,
    GoodFriday,
    USMemorialDay,
    USLaborDay,
    USThanksgivingDay,
)

try:
    # todo 26-09-09
    # 若环境已安装 exchange_calendars，优先使用 XNYS 交易日历；未安装则回退到内置 NYSE 常规假日日历。
    import exchange_calendars as xcals
except Exception:
    xcals = None


# todo 26-09-09
class USStockMarketHolidayCalendar(AbstractHolidayCalendar):
    """
    todo 26-09-09
    NYSE/Nasdaq 常规整日休市日历的本地回退实现。

    说明：优先使用 exchange_calendars 的 XNYS；只有当该库不可用时才使用本类。
    """

    rules = [
        Holiday("New Year's Day", month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday(
            "Juneteenth National Independence Day",
            month=6,
            day=19,
            start_date=pd.Timestamp("2022-06-19"),
            observance=nearest_workday,
        ),
        Holiday("Independence Day", month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday("Christmas Day", month=12, day=25, observance=nearest_workday),
    ]


# todo 26-09-09
def get_downloader_custom_session():
    """
    todo 26-09-09
    创建 curl_cffi 防封禁 Session。异常重拉时会重新创建 Session，尽量绕开会话级缓存。
    """
    proxies_dict = {"http": DOWNLOADER_PROXY_URL, "https": DOWNLOADER_PROXY_URL}
    return cffi_requests.Session(proxies=proxies_dict, impersonate="chrome110")


# todo 26-09-09
def is_us_market_session_include(day) -> bool:
    """
    todo 26-09-09
    判断给定日期是否为美国股票市场常规交易日。

    优先使用 exchange_calendars/XNYS；若环境未安装，则使用本地 NYSE 常规假日日历回退。
    """
    ts = pd.Timestamp(day).normalize()

    if xcals is not None:
        try:
            cal = xcals.get_calendar("XNYS")
            return bool(cal.is_session(ts))
        except Exception:
            pass

    if ts.dayofweek >= 5:
        return False

    calendar = USStockMarketHolidayCalendar()
    holidays = calendar.holidays(
        start=ts - pd.Timedelta(days=10),
        end=ts + pd.Timedelta(days=10),
    )
    return ts not in holidays


# todo 26-09-09
def get_expected_latest_us_trade_date_include(now_et=None) -> pd.Timestamp:
    """
    todo 26-09-09
    计算当前时点之前最近一个“应当已经完成日线结算”的美股交易日。

    - 使用 America/New_York 时区；
    - 正常交易日 17:00 ET 之后才把当天视为已完成，给 Yahoo 日线留出约 1 小时缓冲；
    - 周末和常规美股假日自动回退到上一交易日。
    """
    ny_tz = ZoneInfo("America/New_York")

    if now_et is None:
        now_et = dt_datetime.now(ny_tz)
    elif now_et.tzinfo is None:
        now_et = now_et.replace(tzinfo=ny_tz)
    else:
        now_et = now_et.astimezone(ny_tz)

    candidate = now_et.date()

    # 16:00 ET 收盘后再留 1 小时缓冲，避免拿到尚未定稿的最后一根日 K。
    if now_et.time() < dt_time(17, 0):
        candidate = candidate - dt_timedelta(days=1)

    while not is_us_market_session_include(candidate):
        candidate = candidate - dt_timedelta(days=1)

    return pd.Timestamp(candidate)


# todo 26-09-15 修正下载bug
def download_yf_history_include(
        symbol: str,
        start_date: str,
        end_date: str,
        fresh_session: bool = True,
        auto_adjust: bool = True,
        actions: bool = False,
) -> pd.DataFrame:
    """
    todo 26-09-15 修正下载bug
    从 Yahoo/yfinance 下载日线候选数据，并允许显式选择 adjusted/raw 价格口径。

    - 主历史仍使用 auto_adjust=True，保持既有 MA/RSI/MACD 的历史口径；
    - 只有最新交易日 adjusted OHLC 异常时，才使用 auto_adjust=False 做单日恢复；
    - Yahoo 返回值始终只视为候选数据，后续仍必须通过 OHLCV/最新性/克隆 bar 校验。
    """
    session = get_downloader_custom_session() if fresh_session else None
    ticker_obj = yf.Ticker(symbol, session=session)

    kwargs = dict(
        start=start_date,
        end=end_date,
        interval="1d",
        auto_adjust=auto_adjust,
        actions=actions,
        prepost=False,
        keepna=True,
    )

    try:
        hist = ticker_obj.history(**kwargs)
    except TypeError as exc:
        # todo 26-09-15 修正下载bug
        # 仅在确属“旧版 yfinance 不支持某个关键字参数”时去掉 keepna 重试；
        # 诸如 data['chart'] 为 None 引起的 TypeError 应交给外层重试，而不是误判为参数兼容问题。
        msg = str(exc).lower()
        unsupported_kw = (
            "unexpected keyword" in msg
            or "got an unexpected keyword" in msg
            or "keepna" in msg
        )
        if not unsupported_kw:
            raise
        hist = ticker_obj.history(
            start=start_date,
            end=end_date,
            interval="1d",
            auto_adjust=auto_adjust,
            actions=actions,
            prepost=False,
        )

    if hist is None or hist.empty:
        return pd.DataFrame()

    hist = hist.copy()
    hist.index = pd.to_datetime(hist.index)
    if getattr(hist.index, "tz", None) is not None:
        hist.index = hist.index.tz_localize(None)
    hist.index = hist.index.normalize()
    hist = hist.sort_index()
    return hist



# todo 26-09-15 修正下载bug
def get_us_session_bounds_et_include(day: pd.Timestamp):
    """
    todo 26-09-15 修正下载bug
    返回指定美股交易日在 America/New_York 时区下的常规交易时段开/收盘时间。

    优先使用 exchange_calendars/XNYS，从而兼容提前收盘日；若库不可用或查询失败，
    回退为常规 09:30-16:00 ET。该函数仅服务于 intraday 日K恢复完整性检查。
    """
    ny_tz = ZoneInfo("America/New_York")
    session_day = pd.Timestamp(day).normalize()

    if xcals is not None:
        try:
            cal = xcals.get_calendar("XNYS")
            session = session_day
            open_ts = cal.session_open(session)
            close_ts = cal.session_close(session)
            open_dt = pd.Timestamp(open_ts).tz_convert(ny_tz).to_pydatetime()
            close_dt = pd.Timestamp(close_ts).tz_convert(ny_tz).to_pydatetime()
            return open_dt, close_dt
        except Exception:
            pass

    open_dt = dt_datetime.combine(session_day.date(), dt_time(9, 30)).replace(tzinfo=ny_tz)
    close_dt = dt_datetime.combine(session_day.date(), dt_time(16, 0)).replace(tzinfo=ny_tz)
    return open_dt, close_dt


# todo 26-09-15 修正下载bug
def download_yf_intraday_include(
        symbol: str,
        trade_date: pd.Timestamp,
        interval: str,
        fresh_session: bool = True,
) -> pd.DataFrame:
    """
    todo 26-09-15 修正下载bug
    下载指定交易日的 Yahoo 盘中数据，仅用于 daily 最新 bar 缺 Close 时的灾备恢复。

    - interval 当前仅允许 1m / 5m；
    - auto_adjust=False，避免再次进入 adjusted daily 的异常链路；
    - prepost=False，只取常规交易时段；
    - 保留时区，供完整性检查和收盘 bar 判断使用。
    """
    if interval not in {"1m", "5m"}:
        raise ValueError(f"不支持的 intraday interval: {interval}")

    trade_date = pd.Timestamp(trade_date).normalize()
    start_date = trade_date.strftime("%Y-%m-%d")
    end_date = (trade_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    session = get_downloader_custom_session() if fresh_session else None
    ticker_obj = yf.Ticker(symbol, session=session)
    kwargs = dict(
        start=start_date,
        end=end_date,
        interval=interval,
        auto_adjust=False,
        actions=False,
        prepost=False,
        keepna=True,
    )

    try:
        hist = ticker_obj.history(**kwargs)
    except TypeError as exc:
        msg = str(exc).lower()
        unsupported_kw = (
            "unexpected keyword" in msg
            or "got an unexpected keyword" in msg
            or "keepna" in msg
        )
        if not unsupported_kw:
            raise
        hist = ticker_obj.history(
            start=start_date,
            end=end_date,
            interval=interval,
            auto_adjust=False,
            actions=False,
            prepost=False,
        )

    if hist is None or hist.empty:
        return pd.DataFrame()

    hist = hist.copy().sort_index()
    hist.index = pd.to_datetime(hist.index)
    return hist


# todo 26-09-15 修正下载bug
def rebuild_expected_bar_from_intraday_include(
        symbol: str,
        expected_trade_date: pd.Timestamp,
        raw_daily_hist: pd.DataFrame,
        interval: str,
        current_date_str: str,
        debug_tag: str,
) -> dict:
    """
    todo 26-09-15 修正下载bug
    用 5m/1m 盘中数据恢复 expected_trade_date 的 Close，并与 raw daily 的 O/H/L/V 交叉验证。

    最终 Hybrid Daily Bar 的来源：
    - Open/High/Low/Volume：Yahoo raw daily；
    - Close：Yahoo intraday 最后一根完整常规时段 bar；
    - intraday O/H/L 必须与 daily O/H/L 在 0.1% + 0.03 美元容差内吻合；
    - 盘中数据必须覆盖到该交易日理论最后一个 bar，并至少达到理论 bar 数的 90%。
    """
    expected = pd.Timestamp(expected_trade_date).normalize()
    minutes = 1 if interval == "1m" else 5

    if raw_daily_hist is None or raw_daily_hist.empty or expected not in raw_daily_hist.index:
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": "raw daily 缺少预期交易日，无法构建 Hybrid Daily Bar",
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }

    daily_row = raw_daily_hist.loc[expected]
    if isinstance(daily_row, pd.DataFrame):
        daily_row = daily_row.iloc[-1]

    def _num(name):
        return pd.to_numeric(pd.Series([daily_row.get(name)]), errors="coerce").iloc[0]

    daily_open = _num("Open")
    daily_high = _num("High")
    daily_low = _num("Low")
    daily_volume = _num("Volume")

    daily_ohlv_valid = (
        pd.notna(daily_open)
        and pd.notna(daily_high)
        and pd.notna(daily_low)
        and pd.notna(daily_volume)
        and float(daily_open) > 0
        and float(daily_high) > 0
        and float(daily_low) > 0
        and float(daily_volume) > 0
        and float(daily_high) >= float(daily_open)
        and float(daily_low) <= float(daily_open)
        and float(daily_high) >= float(daily_low)
    )
    if not daily_ohlv_valid:
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": "raw daily 的 O/H/L/Volume 本身不完整，禁止仅靠 intraday Close 修复",
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }

    try:
        intraday = download_yf_intraday_include(
            symbol=symbol,
            trade_date=expected,
            interval=interval,
            fresh_session=True,
        )
    except Exception as exc:
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": f"{interval} 请求失败: {exc}",
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }

    if intraday.empty:
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": f"{interval} 返回空数据",
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }

    save_data_quality_debug_include(
        symbol, intraday, f"{debug_tag}_{interval}_raw", current_date_str
    )

    local = intraday.copy()
    idx = pd.to_datetime(local.index)
    ny_tz = ZoneInfo("America/New_York")
    if getattr(idx, "tz", None) is None:
        idx = idx.tz_localize(ny_tz)
    else:
        idx = idx.tz_convert(ny_tz)
    local.index = idx

    open_dt, close_dt = get_us_session_bounds_et_include(expected)
    session_open = pd.Timestamp(open_dt)
    session_close = pd.Timestamp(close_dt)

    day_mask = pd.Series(local.index.date == expected.date(), index=local.index)
    session_mask = (
        day_mask
        & pd.Series(local.index >= session_open, index=local.index)
        & pd.Series(local.index < session_close, index=local.index)
    )
    local = local.loc[session_mask.values].copy()

    required_cols = ["Open", "High", "Low", "Close"]
    missing_cols = [col for col in required_cols if col not in local.columns]
    if missing_cols:
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": f"{interval} 缺少字段 {missing_cols}",
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }

    for col in required_cols:
        local[col] = pd.to_numeric(local[col], errors="coerce")
    local = local.dropna(subset=required_cols)
    if local.empty:
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": f"{interval} 常规时段无有效 OHLC",
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }

    expected_last_start = session_close - pd.Timedelta(minutes=minutes)
    expected_bar_count = max(1, int(round((session_close - session_open).total_seconds() / 60.0 / minutes)))
    min_bar_count = max(1, int(expected_bar_count * 0.90))
    first_ts = local.index[0]
    last_ts = local.index[-1]

    if first_ts > session_open + pd.Timedelta(minutes=minutes):
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": f"{interval} 开盘数据不完整，首根 {first_ts:%H:%M} 晚于预期 {session_open:%H:%M}",
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }
    if last_ts < expected_last_start:
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": f"{interval} 收盘数据不完整，末根 {last_ts:%H:%M} 早于要求 {expected_last_start:%H:%M}",
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }
    if len(local) < min_bar_count:
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": f"{interval} bar 数不足：{len(local)}/{expected_bar_count}，低于90%完整性要求",
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }

    intra_open = float(local["Open"].iloc[0])
    intra_high = float(local["High"].max())
    intra_low = float(local["Low"].min())
    intra_close = float(local["Close"].iloc[-1])

    comparisons = {
        "Open": (float(daily_open), intra_open),
        "High": (float(daily_high), intra_high),
        "Low": (float(daily_low), intra_low),
    }
    mismatches = []
    for name, (daily_value, intra_value) in comparisons.items():
        if not np.isclose(daily_value, intra_value, rtol=0.001, atol=0.03):
            mismatches.append(
                f"{name}: daily={daily_value:.4f}, {interval}={intra_value:.4f}"
            )
    if mismatches:
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": f"{interval} 与 raw daily O/H/L 交叉校验不一致：" + "; ".join(mismatches),
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }

    hybrid = pd.DataFrame(
        {
            f"{symbol}_open": [float(daily_open)],
            f"{symbol}_high": [float(daily_high)],
            f"{symbol}_low": [float(daily_low)],
            f"{symbol}_close": [intra_close],
            f"{symbol}_volume": [float(daily_volume)],
        },
        index=[expected],
    )
    hybrid.index.name = "trade_date_utc"
    clean_bar, issues = clean_stock_ohlcv_include(hybrid, symbol)
    if clean_bar.empty or expected not in clean_bar.index:
        return {
            "success": False,
            "bar": pd.DataFrame(),
            "reason": "；".join(issues) if issues else f"{interval} Hybrid Daily Bar 未通过最终 OHLCV 校验",
            "recovery_mode": f"INTRADAY_{interval.upper()}",
            "data_source": interval,
            "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        }

    return {
        "success": True,
        "bar": clean_bar.loc[[expected]].copy(),
        "reason": (
            f"{interval} intraday 恢复成功：O/H/L/V 使用 raw daily，Close 使用 {last_ts:%H:%M} ET 最后一根有效 bar"
        ),
        "recovery_mode": f"INTRADAY_{interval.upper()}",
        "data_source": interval,
        "data_source_detail": f"INTRADAY_{interval.upper()}_HYBRID",
        "intraday_rows": int(len(local)),
        "intraday_first": first_ts.isoformat(),
        "intraday_last": last_ts.isoformat(),
        "intraday_close": intra_close,
    }


# todo 26-09-09
def extract_stock_ohlcv_include(hist: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    todo 26-09-09
    从 yfinance 原始返回中仅提取 OHLCV。此处绝不对个股 OHLC 做 ffill。
    """
    required = ["Open", "High", "Low", "Close", "Volume"]
    missing = [col for col in required if col not in hist.columns]
    if missing:
        raise KeyError(f"{symbol} Yahoo 返回缺少字段: {missing}")

    df_single = pd.DataFrame({
        f"{symbol}_open": pd.to_numeric(hist["Open"], errors="coerce").round(4),
        f"{symbol}_high": pd.to_numeric(hist["High"], errors="coerce").round(4),
        f"{symbol}_low": pd.to_numeric(hist["Low"], errors="coerce").round(4),
        f"{symbol}_close": pd.to_numeric(hist["Close"], errors="coerce").round(4),
        f"{symbol}_volume": pd.to_numeric(hist["Volume"], errors="coerce"),
    })
    df_single.index = pd.to_datetime(df_single.index).normalize()
    df_single.index.name = "trade_date_utc"
    return df_single.sort_index()


# todo 26-09-10
def clean_stock_ohlcv_include(df_single: pd.DataFrame, symbol: str):
    """
    todo 26-09-10
    清洗候选 OHLCV，返回 (clean_df, issues)。

    校验包括：重复日期、周末、NaN、非正价格、零/负成交量、High/Low 与 Open/Close 的逻辑关系。
    对无效 bar 输出具体原因和 OHLCV 快照，便于区分 Yahoo 占位 bar、零成交量与价格逻辑错误。
    """
    df = df_single.copy().sort_index()
    issues = []

    if df.index.duplicated().any():
        duplicate_dates = df.index[df.index.duplicated(keep=False)].strftime("%Y-%m-%d").tolist()
        issues.append(f"重复日期已去重: {duplicate_dates}")
        df = df[~df.index.duplicated(keep="last")]

    weekend_mask = df.index.dayofweek >= 5
    if weekend_mask.any():
        weekend_dates = df.index[weekend_mask].strftime("%Y-%m-%d").tolist()
        issues.append(f"删除周末 bar: {weekend_dates}")
        df = df.loc[~weekend_mask]

    o = f"{symbol}_open"
    h = f"{symbol}_high"
    l = f"{symbol}_low"
    c = f"{symbol}_close"
    v = f"{symbol}_volume"
    price_cols = [o, h, l, c]

    invalid_missing = df[price_cols + [v]].isna().any(axis=1)
    invalid_nonpositive_price = (~invalid_missing) & (df[price_cols] <= 0).any(axis=1)
    invalid_volume = (~invalid_missing) & (df[v] <= 0)
    invalid_ohlc_logic = (
            (~invalid_missing)
            & (~invalid_nonpositive_price)
            & (
                    (df[h] < df[o])
                    | (df[h] < df[c])
                    | (df[l] > df[o])
                    | (df[l] > df[c])
                    | (df[h] < df[l])
            )
    )

    def _fmt_value(value):
        if pd.isna(value):
            return "NaN"
        try:
            return f"{float(value):.4f}"
        except Exception:
            return str(value)

    def _append_issue(mask: pd.Series, reason: str) -> None:
        bad_idx = df.index[mask]
        if len(bad_idx) == 0:
            return
        # 最多展开 8 根，避免极端异常时日志失控；其余只给数量。
        for dt in bad_idx[-8:]:
            row = df.loc[dt]
            issues.append(
                f"{dt:%Y-%m-%d} {reason} "
                f"(O={_fmt_value(row[o])}, H={_fmt_value(row[h])}, "
                f"L={_fmt_value(row[l])}, C={_fmt_value(row[c])}, V={_fmt_value(row[v])})"
            )
        if len(bad_idx) > 8:
            issues.append(f"另有 {len(bad_idx) - 8} 根 {reason} bar 未展开显示")

    _append_issue(invalid_missing, "OHLCV存在缺失")
    _append_issue(invalid_nonpositive_price, "OHLC存在非正价格")
    _append_issue(invalid_volume, "成交量<=0")
    _append_issue(invalid_ohlc_logic, "OHLC高低价逻辑异常")

    invalid_mask = (
            invalid_missing
            | invalid_nonpositive_price
            | invalid_volume
            | invalid_ohlc_logic
    )

    if invalid_mask.any():
        df = df.loc[~invalid_mask]

    return df.sort_index(), issues


# todo 26-09-09
def latest_clone_bar_reason_include(df_single: pd.DataFrame, symbol: str) -> Optional[str]:
    """
    todo 26-09-09
    检测最新日 K 是否呈现“上一交易日 OHLC 四价完全克隆，但成交量不同”的高可疑形态。

    该规则只检查最后一根 bar，命中后先触发短窗口重拉；重拉仍异常才隔离最新 bar。
    """
    if len(df_single) < 2:
        return None

    prev = df_single.iloc[-2]
    last = df_single.iloc[-1]
    price_cols = [
        f"{symbol}_open",
        f"{symbol}_high",
        f"{symbol}_low",
        f"{symbol}_close",
    ]
    volume_col = f"{symbol}_volume"

    same_ohlc = all(
        np.isclose(float(last[col]), float(prev[col]), rtol=0.0, atol=1e-8)
        for col in price_cols
    )
    volume_changed = not np.isclose(
        float(last[volume_col]),
        float(prev[volume_col]),
        rtol=0.0,
        atol=1e-8,
    )

    if same_ohlc and volume_changed:
        return (
            f"最新 bar {df_single.index[-1]:%Y-%m-%d} 的 OHLC 与上一交易日完全一致，"
            "但成交量不同，疑似 Yahoo 最后一根日 K 未定稿/缓存异常"
        )
    return None


# todo 26-09-15 修正下载bug
def corporate_action_reason_for_expected_day_include(
        hist: pd.DataFrame,
        expected_trade_date: pd.Timestamp,
) -> Optional[str]:
    """
    todo 26-09-15 修正下载bug
    检查 raw daily 恢复日是否发生分红/拆股/资本利得分配。

    若 expected_trade_date 当天存在 corporate action，则不把 raw OHLC 直接拼接到 adjusted 历史，
    避免在口径切换日制造假的日收益率、RSI、ATR 或 MACD 信号。
    """
    if hist is None or hist.empty:
        return "raw daily 返回空数据"

    expected = pd.Timestamp(expected_trade_date).normalize()
    if expected not in hist.index:
        return f"raw daily 未返回预期交易日 {expected:%Y-%m-%d}"

    row = hist.loc[expected]
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]

    nonzero_actions = []
    for col in ("Dividends", "Stock Splits", "Capital Gains"):
        if col not in hist.columns:
            continue
        value = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]
        if pd.notna(value) and not np.isclose(float(value), 0.0, rtol=0.0, atol=1e-12):
            nonzero_actions.append(f"{col}={float(value):g}")

    if nonzero_actions:
        return (
            f"{expected:%Y-%m-%d} 存在 corporate action（"
            + ", ".join(nonzero_actions)
            + "），禁止 raw bar 直接拼接 adjusted 历史"
        )
    return None


# todo 26-09-15 修正下载bug
def recover_expected_bar_from_raw_include(
        symbol: str,
        expected_trade_date: pd.Timestamp,
        request_end_date: str,
        current_date_str: str,
        debug_tag: str = "raw_daily_recovery",
) -> dict:
    """
    todo 26-09-15 修正下载bug
    latest bar 恢复总入口：raw daily -> 5m Hybrid -> 1m Hybrid。

    恢复顺序：
    1) auto_adjust=False 的 raw daily 若完整，直接使用 DAILY_RAW；
    2) raw daily 若仅 Close 缺失而 O/H/L/V 正常，优先用 5m intraday 恢复 Close；
    3) 5m 不完整/交叉校验失败，再用 1m intraday；
    4) 全部失败后返回失败，交由上层决定 SOURCE_PENDING/STALE。

    corporate action 日保持保守：禁止把 raw/hybrid bar 直接拼接到 adjusted 历史。
    """
    expected = pd.Timestamp(expected_trade_date).normalize()
    raw_start = (expected - pd.Timedelta(days=7)).strftime("%Y-%m-%d")

    base_result = {
        "success": False,
        "bar": pd.DataFrame(),
        "reason": "",
        "raw_hist": pd.DataFrame(),
        "recovery_mode": "NONE",
        "data_source": "daily",
        "data_source_detail": "DAILY_RAW",
        "intraday_interval": None,
    }

    try:
        raw_hist = download_yf_history_include(
            symbol=symbol,
            start_date=raw_start,
            end_date=request_end_date,
            fresh_session=True,
            auto_adjust=False,
            actions=True,
        )
    except Exception as exc:
        base_result["reason"] = f"raw daily 请求失败: {exc}"
        return base_result

    base_result["raw_hist"] = raw_hist
    if raw_hist.empty:
        base_result["reason"] = "raw daily 返回空数据"
        return base_result

    if expected not in raw_hist.index:
        save_data_quality_debug_include(
            symbol, raw_hist, f"{debug_tag}_missing_expected", current_date_str
        )
        base_result["reason"] = f"raw daily 未返回预期交易日 {expected:%Y-%m-%d}"
        return base_result

    action_reason = corporate_action_reason_for_expected_day_include(raw_hist, expected)
    if action_reason:
        save_data_quality_debug_include(
            symbol, raw_hist, f"{debug_tag}_corporate_action", current_date_str
        )
        base_result["reason"] = action_reason
        return base_result

    expected_hist = raw_hist.loc[[expected]].copy()
    try:
        raw_bar = extract_stock_ohlcv_include(expected_hist, symbol)
        clean_bar, issues = clean_stock_ohlcv_include(raw_bar, symbol)
    except Exception as exc:
        save_data_quality_debug_include(
            symbol, raw_hist, f"{debug_tag}_extract_error", current_date_str
        )
        base_result["reason"] = f"raw daily 提取/清洗失败: {exc}"
        return base_result

    if not clean_bar.empty and expected in clean_bar.index:
        return {
            **base_result,
            "success": True,
            "bar": clean_bar.loc[[expected]].copy(),
            "reason": "raw daily 最新交易日完整，恢复成功",
            "recovery_mode": "RAW_DAILY",
            "data_source": "daily",
            "data_source_detail": "DAILY_RAW",
        }

    save_data_quality_debug_include(
        symbol, raw_hist, f"{debug_tag}_raw_daily_incomplete", current_date_str
    )
    raw_reason = "；".join(issues) if issues else "raw daily 预期日 bar 未通过 OHLCV 校验"

    # todo 26-09-15 修正下载bug
    # 已实测 yfinance 1.2.0 可能出现 raw daily O/H/L/V 正常但 Close/Adj Close=NaN。
    # 此时优先 5m，失败后 1m；Volume 始终保留 daily 官方值。
    intraday_failures = []
    for interval in ("5m", "1m"):
        # todo 26-09-15 修正下载bug
        print(f"      🔄 {symbol} raw daily 不完整，尝试 {interval} intraday Hybrid 恢复 Close...")
        result = rebuild_expected_bar_from_intraday_include(
            symbol=symbol,
            expected_trade_date=expected,
            raw_daily_hist=raw_hist,
            interval=interval,
            current_date_str=current_date_str,
            debug_tag=f"{debug_tag}_intraday",
        )
        if result.get("success"):
            print(
                f"      ✅ {symbol} {interval} intraday 恢复成功 | "
                f"Close={result.get('intraday_close', 'N/A')} | "
                f"rows={result.get('intraday_rows', 'N/A')}"
            )
            return {
                **base_result,
                **result,
                "raw_hist": raw_hist,
                "intraday_interval": interval,
                "reason": f"raw daily 不完整（{raw_reason}）；{result.get('reason', '')}",
            }
        print(f"      ⚠️ {symbol} {interval} intraday 恢复失败: {result.get('reason', '未知失败')}")
        intraday_failures.append(f"{interval}: {result.get('reason', '未知失败')}")

    base_result["reason"] = (
        f"raw daily 不完整（{raw_reason}）；intraday fallback 全部失败："
        + " | ".join(intraday_failures)
    )
    return base_result


# todo 26-09-15 修正下载bug
SOURCE_PROBE_SYMBOLS_INCLUDE = ("SPY", "QQQ", "MSFT")


# todo 26-09-15 修正下载bug
def probe_yahoo_daily_source_include(
        expected_trade_date: pd.Timestamp,
        request_end_date: str,
        current_date_str: str,
        probe_symbols=SOURCE_PROBE_SYMBOLS_INCLUDE,
) -> dict:
    """
    todo 26-09-15 修正下载bug
    在正式下载全部股票前做源级预检：adjusted daily -> raw daily -> 5m -> 1m。

    只有多数探针在全部恢复链后仍无法达到 expected_trade_date，才判定 SOURCE_PENDING。
    """
    expected_trade_date = pd.Timestamp(expected_trade_date).normalize()
    probe_start = (expected_trade_date - pd.Timedelta(days=15)).strftime("%Y-%m-%d")
    probe_results = {}
    valid_latest_dates = []

    print("🔎 Yahoo 日线源级预检...")

    for symbol in probe_symbols:
        result = {
            "status": "ERROR",
            "latest_trusted_date": None,
            "expected_bar_present": False,
            "issues": [],
            "recovery_mode": "NONE",
            "data_source": "daily",
            "data_source_detail": "DAILY_ADJUSTED",
            "raw_daily_recovered": False,
            "intraday_interval": None,
        }
        try:
            hist = download_yf_history_include(
                symbol=symbol,
                start_date=probe_start,
                end_date=request_end_date,
                fresh_session=True,
                auto_adjust=True,
                actions=False,
            )
            if hist.empty:
                raise RuntimeError("Yahoo adjusted daily 返回空数据")

            raw = extract_stock_ohlcv_include(hist, symbol)
            raw = raw.loc[raw.index <= expected_trade_date]
            result["expected_bar_present"] = bool(expected_trade_date in raw.index)
            clean, issues = clean_stock_ohlcv_include(raw, symbol)
            result["issues"] = list(issues)

            clone_reason = latest_clone_bar_reason_include(clean, symbol) if not clean.empty else None
            needs_recovery = (
                clean.empty
                or clean.index[-1].normalize() < expected_trade_date
                or clone_reason is not None
            )

            if needs_recovery:
                if clone_reason:
                    result["issues"].append(clone_reason)
                recovery = recover_expected_bar_from_raw_include(
                    symbol=symbol,
                    expected_trade_date=expected_trade_date,
                    request_end_date=request_end_date,
                    current_date_str=current_date_str,
                    debug_tag="source_probe_recovery",
                )
                if recovery.get("success"):
                    clean = merge_retry_window_include(clean, recovery["bar"])
                    clean, merged_issues = clean_stock_ohlcv_include(clean, symbol)
                    result["issues"].extend(merged_issues)
                    final_clone = latest_clone_bar_reason_include(clean, symbol) if not clean.empty else None
                    if final_clone:
                        result["issues"].append(final_clone)
                        if not clean.empty and clean.index[-1].normalize() == expected_trade_date:
                            clean = clean.iloc[:-1].copy()
                    elif (not clean.empty) and clean.index[-1].normalize() == expected_trade_date:
                        result["recovery_mode"] = recovery.get("recovery_mode", "NONE")
                        result["data_source"] = recovery.get("data_source", "daily")
                        result["data_source_detail"] = recovery.get("data_source_detail", "UNKNOWN")
                        result["raw_daily_recovered"] = recovery.get("recovery_mode") == "RAW_DAILY"
                        result["intraday_interval"] = recovery.get("intraday_interval")
                else:
                    result["issues"].append(
                        f"fallback失败: {recovery.get('reason', '未知原因')}"
                    )

            if clean.empty:
                raise RuntimeError("adjusted/fallback 清洗后均无可信 bar")

            final_clone = latest_clone_bar_reason_include(clean, symbol)
            if final_clone:
                result["issues"].append(final_clone)
                clean = clean.iloc[:-1].copy()
            if clean.empty:
                raise RuntimeError("隔离克隆 bar 后无可信数据")

            latest_date = clean.index[-1].normalize()
            result["latest_trusted_date"] = latest_date.strftime("%Y-%m-%d")
            if latest_date == expected_trade_date:
                mode = result.get("recovery_mode", "NONE")
                if mode == "RAW_DAILY":
                    result["status"] = "READY_RAW"
                elif mode == "INTRADAY_5M":
                    result["status"] = "READY_5M"
                elif mode == "INTRADAY_1M":
                    result["status"] = "READY_1M"
                else:
                    result["status"] = "READY"
                    result["recovery_mode"] = "ADJUSTED"
                    result["data_source"] = "daily"
                    result["data_source_detail"] = "DAILY_ADJUSTED"
            else:
                result["status"] = "BEHIND"
            valid_latest_dates.append(result["latest_trusted_date"])

            if result["issues"]:
                save_data_quality_debug_include(
                    symbol, hist, "source_probe_adjusted_fallback", current_date_str
                )

            print(
                f"   • {symbol}: {result['status']} | 可信截止 {result['latest_trusted_date']} | "
                f"数据源 {result['data_source']} ({result['data_source_detail']})"
            )
        except Exception as exc:
            result["issues"] = list(result.get("issues", [])) + [str(exc)]
            print(f"   • {symbol}: ERROR | {exc}")

        probe_results[symbol] = result

    quorum = len(tuple(probe_symbols)) // 2 + 1
    source_status = "SOURCE_ERROR"
    source_available_date = None
    source_mode = "UNKNOWN"

    if len(valid_latest_dates) >= quorum:
        counts = {d: valid_latest_dates.count(d) for d in set(valid_latest_dates)}
        common_date, common_count = max(counts.items(), key=lambda kv: (kv[1], kv[0]))
        if common_count >= quorum:
            source_available_date = common_date
            source_ts = pd.Timestamp(common_date)
            source_status = "SOURCE_READY" if source_ts >= expected_trade_date else "SOURCE_PENDING"

            matching_modes = [
                item.get("recovery_mode", "NONE")
                for item in probe_results.values()
                if item.get("latest_trusted_date") == common_date
            ]
            if matching_modes:
                unique_modes = set(matching_modes)
                if unique_modes == {"ADJUSTED"}:
                    source_mode = "ADJUSTED"
                elif unique_modes == {"RAW_DAILY"}:
                    source_mode = "RAW_FALLBACK"
                elif unique_modes == {"INTRADAY_5M"}:
                    source_mode = "INTRADAY_5M_FALLBACK"
                elif unique_modes == {"INTRADAY_1M"}:
                    source_mode = "INTRADAY_1M_FALLBACK"
                else:
                    source_mode = "MIXED"

    print(
        f"📡 Yahoo 源级状态: {source_status} | "
        f"市场预期 {expected_trade_date:%Y-%m-%d} | "
        f"Yahoo可信截止 {source_available_date or '无法统一判定'} | 模式 {source_mode}"
    )

    return {
        "source_status": source_status,
        "source_mode": source_mode,
        "market_expected_date": expected_trade_date.strftime("%Y-%m-%d"),
        "source_available_date": source_available_date,
        "probe_symbols": list(probe_symbols),
        "probe_results": probe_results,
    }


# todo 26-09-09
def save_data_quality_debug_include(symbol: str, df_or_hist: pd.DataFrame, tag: str, current_date_str: str) -> Optional[
    Path]:
    """
    todo 26-09-09
    数据质量异常时保存原始/候选数据最后 10 行，便于追溯 Yahoo 当时到底返回了什么。
    """
    if df_or_hist is None or df_or_hist.empty:
        return None

    debug_dir = DOWNLOADER_CONFIG["ind_stock_dir"] / "debug_data_quality"
    debug_dir.mkdir(parents=True, exist_ok=True)
    path = debug_dir / f"{symbol}_{current_date_str}_{tag}.csv"
    df_or_hist.tail(10).to_csv(path)
    return path


# todo 26-09-09
def merge_retry_window_include(base_df: pd.DataFrame, retry_df: pd.DataFrame) -> pd.DataFrame:
    """
    todo 26-09-09
    用短窗口重拉结果覆盖同日期的基础候选数据，并保留其余历史记录。
    """
    if base_df.empty:
        return retry_df.copy().sort_index()
    if retry_df.empty:
        return base_df.copy().sort_index()

    merged = base_df.copy()
    for idx, row in retry_df.iterrows():
        merged.loc[idx, row.index] = row.values
    return merged.sort_index()


# todo 26-09-09
def add_stock_indicators_include(df_single: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    todo 26-09-09
    仅在 OHLCV 通过数据质量校验后计算 MA / RSI / Bollinger / ATR / Volume MA / MACD。
    """
    df_single = df_single.copy().sort_index()
    close_col = f"{symbol}_close"
    high_col = f"{symbol}_high"
    low_col = f"{symbol}_low"
    vol_col = f"{symbol}_volume"

    # 1. 均线系统 (MAs)
    df_single["MA5"] = df_single[close_col].rolling(window=5).mean().round(4)
    df_single["MA20"] = df_single[close_col].rolling(window=20).mean().round(4)
    df_single["MA50"] = df_single[close_col].rolling(window=50).mean().round(4)
    df_single["MA100"] = df_single[close_col].rolling(window=100).mean().round(4)
    df_single["MA200"] = df_single[close_col].rolling(window=200).mean().round(4)

    # 2. RSI (14)
    delta = df_single[close_col].diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = -1 * delta.clip(upper=0).ewm(alpha=1 / 14, adjust=False).mean()
    rs = gain / loss
    df_single["RSI_14"] = (100 - (100 / (1 + rs))).round(2)

    # 3. Bollinger Bands (20, 2σ)
    std20 = df_single[close_col].rolling(window=20).std()
    df_single["BB_Upper"] = (df_single["MA20"] + (std20 * 2)).round(4)
    df_single["BB_Lower"] = (df_single["MA20"] - (std20 * 2)).round(4)

    # 4. ATR (14)
    high_low = df_single[high_col] - df_single[low_col]
    high_close = np.abs(df_single[high_col] - df_single[close_col].shift())
    low_close = np.abs(df_single[low_col] - df_single[close_col].shift())
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df_single["ATR_14"] = true_range.rolling(window=14).mean().round(4)

    # 5. Volume MA20
    df_single["Volume_MA20"] = df_single[vol_col].rolling(window=20).mean().round(0)

    # 6. MACD (12, 26, 9)
    ema12 = df_single[close_col].ewm(span=12, adjust=False).mean()
    ema26 = df_single[close_col].ewm(span=26, adjust=False).mean()
    df_single["MACD_DIF"] = (ema12 - ema26).round(4)
    df_single["MACD_DEA"] = df_single["MACD_DIF"].ewm(span=9, adjust=False).mean().round(4)
    df_single["MACD_Hist"] = (2 * (df_single["MACD_DIF"] - df_single["MACD_DEA"])).round(4)

    return df_single


# todo 26-09-09
def atomic_to_csv_include(df: pd.DataFrame, final_path: Path) -> None:
    """
    todo 26-09-09
    先写同目录临时文件，再用 os.replace 原子替换正式文件，避免写入中断损坏 last-known-good master。
    """
    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = final_path.with_name(final_path.name + ".tmp")
    df.to_csv(tmp_path)
    os.replace(tmp_path, final_path)


# todo 26-09-09
def read_existing_master_latest_date_include(symbol: str) -> Optional[str]:
    """
    todo 26-09-09
    读取现有 last-known-good master 的最后可信交易日；读取失败时返回 None。
    """
    path = DOWNLOADER_CONFIG["ind_stock_dir"] / f"individual_stocks_master_{symbol}.csv"
    if not path.exists():
        return None
    try:
        old = pd.read_csv(path, usecols=["trade_date_utc"])
        if old.empty:
            return None
        dates = pd.to_datetime(old["trade_date_utc"], errors="coerce").dropna()
        if dates.empty:
            return None
        return dates.max().strftime("%Y-%m-%d")
    except Exception:
        return None


# todo 26-09-15 修正下载bug
def save_data_quality_status_include(status_map: dict, current_date_str: str) -> None:
    """
    todo 26-09-15 修正下载bug
    保存本轮个股数据质量状态，并记录每个标的最终 data_source（daily / 5m / 1m）及 source_mode。
    """
    first_status = next(iter(status_map.values()), {}) if status_map else {}
    payload = {
        "generated_at": dt_datetime.now().isoformat(timespec="seconds"),
        "source": {
            "source_status": first_status.get("source_status", "UNKNOWN"),
            "source_mode": first_status.get("source_mode", "UNKNOWN"),
            "market_expected_date": first_status.get("market_expected_date", first_status.get("expected_trade_date")),
            "source_available_date": first_status.get("source_available_date"),
        },
        "symbols": status_map,
    }
    dated_path = DOWNLOADER_CONFIG["ind_stock_dir"] / f"individual_stocks_data_quality_{current_date_str}.json"
    latest_path = DOWNLOADER_CONFIG["ind_stock_dir"] / "individual_stocks_data_quality_latest.json"

    dated_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# todo 26-09-09
def load_latest_data_quality_status_include() -> dict:
    """
    todo 26-09-09
    读取最近一次个股数据质量状态；不存在或损坏时返回空字典。
    """
    latest_path = DOWNLOADER_CONFIG["ind_stock_dir"] / "individual_stocks_data_quality_latest.json"
    if not latest_path.exists():
        return {}
    try:
        payload = json.loads(latest_path.read_text(encoding="utf-8"))
        return payload.get("symbols", {})
    except Exception:
        return {}


# ==================== 5. 个股每日数据下载（按个股单独保存） ====================
# todo 26-09-15 修正下载bug
def fetch_daily_stock_data_include(tickers: list, start_date: str = "2020-01-01"):
    """
    todo 26-09-15 修正下载bug
    下载并更新个股日线。主历史保持 adjusted 口径；最新 daily bar 异常时依次尝试 raw daily、5m、1m，
    并在处理完成后明确输出每个标的实际使用的数据源。

    数据质量链：
    1) 个股 OHLC 禁止 ffill；
    2) adjusted 全历史 OHLCV 完整性/逻辑校验；
    3) 最新 bar 克隆异常检测；
    4) SPY/QQQ/MSFT 源级预检执行 adjusted daily -> raw daily -> 5m -> 1m fallback；
    5) raw daily 完整则只恢复 expected 日；若仅 Close 缺失，则用 5m/1m 恢复 Close 并与 daily O/H/L 交叉校验；
    6) daily/intraday 恢复失败后，才进入新 Session + adjusted 短窗口重拉；
    7) 区分 market_expected_date 与 source_available_date；
    8) 只有可信数据才原子覆盖 master，真正 stale 时保留 last-known-good。
    """
    print(f"📥 正在通过防封禁通道下载 {tickers} 的每日交易数据...")

    current_date_str = arrow.now().format("YYYY_MM_DD")
    expected_trade_date = get_expected_latest_us_trade_date_include()
    expected_trade_date_str = expected_trade_date.strftime("%Y-%m-%d")
    request_end_date = (expected_trade_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"🗓️ 市场预期最新完整美股交易日: {expected_trade_date_str}")
    print(f"📡 Yahoo 请求 end（exclusive）: {request_end_date}")

    # todo 26-09-15 修正下载bug
    source_info = probe_yahoo_daily_source_include(
        expected_trade_date=expected_trade_date,
        request_end_date=request_end_date,
        current_date_str=current_date_str,
    )
    source_status = source_info.get("source_status", "SOURCE_ERROR")
    source_mode = source_info.get("source_mode", "UNKNOWN")
    source_available_date_str = source_info.get("source_available_date")
    source_available_date = (
        pd.Timestamp(source_available_date_str).normalize()
        if source_available_date_str
        else None
    )

    if source_status == "SOURCE_PENDING" and source_available_date is not None:
        required_trade_date = source_available_date
    else:
        required_trade_date = expected_trade_date

    print(
        f"🎯 本轮个股最低可信截止要求: {required_trade_date:%Y-%m-%d} "
        f"({source_status}, {source_mode})"
    )

    status_map = {}

    for symbol in tickers:
        max_retries = 4
        success = False
        retry_used_any = False
        raw_daily_recovered_any = False
        intraday_recovered_any = False
        recovery_mode_any = "NONE"
        data_source_used = "daily"
        data_source_detail = "DAILY_ADJUSTED"
        intraday_interval_used = None
        latest_bar_suspect_any = False
        last_reason = ""
        final_candidate = pd.DataFrame()

        master_save_path = DOWNLOADER_CONFIG["ind_stock_dir"] / f"individual_stocks_master_{symbol}.csv"
        existing_master_date = read_existing_master_latest_date_include(symbol)

        status = {
            "expected_trade_date": expected_trade_date_str,
            "market_expected_date": expected_trade_date_str,
            "source_available_date": source_available_date_str,
            "source_status": source_status,
            "source_mode": source_mode,
            "required_trade_date": required_trade_date.strftime("%Y-%m-%d"),
            "actual_trade_date": existing_master_date,
            "data_status": "PENDING",
            "retry_used": False,
            "raw_daily_recovered": False,
            "intraday_recovered": False,
            "recovery_mode": "NONE",
            "data_source": "daily",
            "data_source_detail": "DAILY_ADJUSTED",
            "intraday_interval": None,
            "latest_bar_suspect": False,
            "master_updated": False,
            "message": "",
        }

        for attempt in range(max_retries):
            try:
                print(f"   📥 {symbol} 第 {attempt + 1}/{max_retries} 次尝试...")

                hist = download_yf_history_include(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=request_end_date,
                    fresh_session=True,
                    auto_adjust=True,
                    actions=False,
                )
                if hist.empty:
                    raise RuntimeError("Yahoo adjusted daily 返回空历史数据")

                adjusted_df = extract_stock_ohlcv_include(hist, symbol)
                adjusted_df = adjusted_df.loc[adjusted_df.index <= expected_trade_date]
                clean_df, issues = clean_stock_ohlcv_include(adjusted_df, symbol)

                for issue in issues:
                    print(f"      ⚠️ {symbol}: {issue}")

                if clean_df.empty:
                    adjusted_clone_reason = None
                    adjusted_actual_date = None
                else:
                    adjusted_clone_reason = latest_clone_bar_reason_include(clean_df, symbol)
                    adjusted_actual_date = clean_df.index[-1].normalize()

                # todo 26-09-15 修正下载bug
                # 只要 adjusted 最新日未达到 expected，或最新 bar 命中克隆异常，依次尝试 raw daily -> 5m -> 1m 恢复 expected 一根。
                needs_raw_recovery = (
                    clean_df.empty
                    or adjusted_actual_date is None
                    or adjusted_actual_date < expected_trade_date
                    or adjusted_clone_reason is not None
                )

                if needs_raw_recovery:
                    reason_parts = []
                    if clean_df.empty:
                        reason_parts.append("adjusted 清洗后无有效数据")
                    elif adjusted_actual_date < expected_trade_date:
                        reason_parts.append(
                            f"adjusted 最新可信 {adjusted_actual_date:%Y-%m-%d} 早于市场预期 {expected_trade_date_str}"
                        )
                    if adjusted_clone_reason:
                        reason_parts.append(adjusted_clone_reason)
                    print(
                        f"      🔄 {symbol} adjusted 最新日异常，启动 daily/5m/1m 恢复链: "
                        + "；".join(reason_parts)
                    )

                    save_data_quality_debug_include(
                        symbol, hist, f"attempt{attempt + 1}_adjusted_before_raw", current_date_str
                    )

                    recovery = recover_expected_bar_from_raw_include(
                        symbol=symbol,
                        expected_trade_date=expected_trade_date,
                        request_end_date=request_end_date,
                        current_date_str=current_date_str,
                        debug_tag=f"attempt{attempt + 1}_raw_daily_recovery",
                    )

                    if recovery.get("success"):
                        recovery_mode_any = recovery.get("recovery_mode", "NONE")
                        data_source_used = recovery.get("data_source", "daily")
                        data_source_detail = recovery.get("data_source_detail", "UNKNOWN")
                        intraday_interval_used = recovery.get("intraday_interval")
                        raw_daily_recovered_any = recovery_mode_any == "RAW_DAILY"
                        intraday_recovered_any = recovery_mode_any in {"INTRADAY_5M", "INTRADAY_1M"}
                        recovered_candidate = merge_retry_window_include(clean_df, recovery["bar"])
                        recovered_candidate, recovery_issues = clean_stock_ohlcv_include(
                            recovered_candidate, symbol
                        )
                        for issue in recovery_issues:
                            print(f"      ⚠️ {symbol} raw恢复后: {issue}")

                        recovery_clone_reason = (
                            latest_clone_bar_reason_include(recovered_candidate, symbol)
                            if not recovered_candidate.empty
                            else None
                        )
                        recovery_latest = (
                            recovered_candidate.index[-1].normalize()
                            if not recovered_candidate.empty
                            else None
                        )

                        if recovery_clone_reason:
                            print(f"      ❌ {symbol} fallback恢复后仍命中克隆异常: {recovery_clone_reason}")
                            latest_bar_suspect_any = True
                            last_reason = recovery_clone_reason
                            # todo 26-09-15 修正下载bug
                            # 恢复bar未被采用，撤销其数据源标记，避免最终状态误报为5m/1m。
                            recovery_mode_any = "NONE"
                            data_source_used = "daily"
                            data_source_detail = "DAILY_ADJUSTED"
                            intraday_interval_used = None
                            raw_daily_recovered_any = False
                            intraday_recovered_any = False
                        elif recovery_latest == expected_trade_date:
                            clean_df = recovered_candidate
                            adjusted_clone_reason = None
                            print(
                                f"      ✅ {symbol} 最新bar恢复成功: {expected_trade_date_str} | "
                                f"数据源 {data_source_used} ({data_source_detail}) | 历史仍保持 adjusted 口径"
                            )
                        else:
                            last_reason = (
                                f"fallback恢复后仍未达到市场预期 {expected_trade_date_str}"
                            )
                            print(f"      ⚠️ {symbol} {last_reason}")
                            # todo 26-09-15 修正下载bug
                            recovery_mode_any = "NONE"
                            data_source_used = "daily"
                            data_source_detail = "DAILY_ADJUSTED"
                            intraday_interval_used = None
                            raw_daily_recovered_any = False
                            intraday_recovered_any = False
                    else:
                        last_reason = f"daily/intraday 恢复失败: {recovery.get('reason', '未知原因')}"
                        print(f"      ⚠️ {symbol} {last_reason}")

                if clean_df.empty:
                    actual_date = None
                    clone_reason = None
                    stale_vs_source = True
                else:
                    clone_reason = latest_clone_bar_reason_include(clean_df, symbol)
                    actual_date = clean_df.index[-1].normalize()
                    stale_vs_source = actual_date < required_trade_date

                # todo 26-09-15 修正下载bug
                # daily/intraday fallback 失败后，才进入原有 adjusted 短窗口重拉；SOURCE_PENDING 下若已达到 source_available，
                # 则不进行无意义的 4 次完整重试。
                if clone_reason or stale_vs_source:
                    retry_used_any = True
                    latest_bar_suspect_any = latest_bar_suspect_any or bool(clone_reason)

                    reasons = []
                    if clone_reason:
                        reasons.append(clone_reason)
                    if stale_vs_source:
                        actual_text = actual_date.strftime("%Y-%m-%d") if actual_date is not None else "N/A"
                        reasons.append(
                            f"最新可信日期 {actual_text} 早于本轮最低可信截止 "
                            f"{required_trade_date:%Y-%m-%d}"
                        )
                    last_reason = "；".join(reasons)
                    print(f"      ⚠️ {symbol} 候选数据仍异常: {last_reason}")

                    retry_start = (expected_trade_date - pd.Timedelta(days=15)).strftime("%Y-%m-%d")
                    retry_hist = download_yf_history_include(
                        symbol=symbol,
                        start_date=retry_start,
                        end_date=request_end_date,
                        fresh_session=True,
                        auto_adjust=True,
                        actions=False,
                    )
                    save_data_quality_debug_include(
                        symbol, retry_hist, f"attempt{attempt + 1}_adjusted_short_retry", current_date_str
                    )

                    if not retry_hist.empty:
                        retry_adjusted = extract_stock_ohlcv_include(retry_hist, symbol)
                        retry_adjusted = retry_adjusted.loc[retry_adjusted.index <= expected_trade_date]
                        retry_clean, retry_issues = clean_stock_ohlcv_include(retry_adjusted, symbol)
                        for issue in retry_issues:
                            print(f"      ⚠️ {symbol} adjusted重拉: {issue}")

                        clean_df = merge_retry_window_include(clean_df, retry_clean)
                        clean_df, merged_issues = clean_stock_ohlcv_include(clean_df, symbol)
                        for issue in merged_issues:
                            print(f"      ⚠️ {symbol} 合并后: {issue}")

                if clean_df.empty:
                    last_reason = "最终清洗后无有效数据"
                    raise RuntimeError(last_reason)

                final_clone_reason = latest_clone_bar_reason_include(clean_df, symbol)
                if final_clone_reason:
                    latest_bar_suspect_any = True
                    last_reason = final_clone_reason
                    print(f"      ❌ {symbol}: {final_clone_reason}")
                    clean_df = clean_df.iloc[:-1].copy()

                if clean_df.empty:
                    last_reason = "隔离异常最新 bar 后无有效数据"
                    raise RuntimeError(last_reason)

                actual_date = clean_df.index[-1].normalize()
                # todo 26-09-15 修正下载bug
                # 若最终依赖的是 adjusted 路径（未成功使用 raw/intraday 恢复），明确记录为 daily。
                if recovery_mode_any == "NONE":
                    data_source_used = "daily"
                    data_source_detail = "DAILY_ADJUSTED"
                final_candidate = clean_df.copy()

                if actual_date < required_trade_date:
                    last_reason = (
                        f"数据新鲜度不通过：市场预期 {expected_trade_date_str}，"
                        f"Yahoo可信要求 {required_trade_date:%Y-%m-%d}，"
                        f"实际可信 {actual_date:%Y-%m-%d}"
                    )
                    print(f"      ⚠️ {symbol} {last_reason}")

                    if attempt < max_retries - 1:
                        sleep_time = random.uniform(3.0, 8.0)
                        print(f"      🔄 {symbol} 将再次完整重试，等待 {sleep_time:.1f} 秒...")
                        time.sleep(sleep_time)
                        continue
                    break

                df_single = add_stock_indicators_include(clean_df, symbol)
                indicator_save_path = (
                    DOWNLOADER_CONFIG["ind_stock_dir"]
                    / f"individual_stocks_indicators_{symbol}_{current_date_str}.csv"
                )
                atomic_to_csv_include(df_single, master_save_path)
                atomic_to_csv_include(df_single, indicator_save_path)

                # todo 26-09-15 修正下载bug
                if actual_date == expected_trade_date:
                    if recovery_mode_any == "INTRADAY_5M":
                        final_status = "INTRADAY_5M_REBUILT"
                        final_message = (
                            "Yahoo daily Close 缺失；使用完整 5m 常规时段恢复 Close，"
                            "O/H/L/Volume 使用 raw daily 并完成交叉校验"
                        )
                    elif recovery_mode_any == "INTRADAY_1M":
                        final_status = "INTRADAY_1M_REBUILT"
                        final_message = (
                            "Yahoo daily Close 缺失；5m fallback 未通过后使用完整 1m 常规时段恢复 Close，"
                            "O/H/L/Volume 使用 raw daily 并完成交叉校验"
                        )
                    elif recovery_mode_any == "RAW_DAILY":
                        final_status = "RAW_DAILY_RECOVERED"
                        final_message = (
                            "adjusted 最新日异常，已使用完整 raw daily 最新bar恢复；"
                            "历史数据仍保持 adjusted 口径"
                        )
                    elif retry_used_any:
                        final_status = "RETRY_RECOVERED"
                        final_message = "adjusted 异常后短窗口重拉恢复，最终达到市场预期交易日"
                    else:
                        final_status = "OK"
                        final_message = "数据质量校验通过"
                elif (
                    source_status == "SOURCE_PENDING"
                    and source_available_date is not None
                    and actual_date >= source_available_date
                ):
                    final_status = "SOURCE_PENDING"
                    final_message = (
                        f"所有 daily/intraday fallback 均未恢复到市场预期 {expected_trade_date_str}；"
                        f"本标的已达到 Yahoo 当前可信截止 {source_available_date:%Y-%m-%d}"
                    )
                else:
                    final_status = "RETRY_RECOVERED" if retry_used_any else "OK"
                    final_message = "数据通过当前可信截止校验"


                status.update({
                    "actual_trade_date": actual_date.strftime("%Y-%m-%d"),
                    "data_status": final_status,
                    "retry_used": retry_used_any,
                    "raw_daily_recovered": raw_daily_recovered_any,
                    "intraday_recovered": intraday_recovered_any,
                    "recovery_mode": recovery_mode_any,
                    "data_source": data_source_used,
                    "data_source_detail": data_source_detail,
                    "intraday_interval": intraday_interval_used,
                    "latest_bar_suspect": latest_bar_suspect_any,
                    "master_updated": True,
                    "message": final_message,
                })

                print(
                    f"   ✅ {symbol} 下载及特征计算成功: {len(df_single)} 条记录 | "
                    f"最新可信 {actual_date:%Y-%m-%d} | 状态 {final_status}"
                )
                print(
                    f"      🧾 {symbol} 最终数据源: {data_source_used} ({data_source_detail})"
                )
                print(f"      📁 指标文件已保存 -> {indicator_save_path.name}")
                success = True
                break

            except Exception as exc:
                last_reason = str(exc)
                if attempt < max_retries - 1:
                    sleep_time = random.uniform(4.0, 12.0)
                    print(f"   ⚠️ {symbol} 本轮失败，等待 {sleep_time:.1f} 秒后重试... ({exc})")
                    time.sleep(sleep_time)
                else:
                    print(f"   ❌ {symbol} 最终下载/校验失败: {exc}")

        if not success:
            # todo 26-09-15 修正下载bug
            # 真正 stale/失败时绝不覆盖已有 last-known-good master。
            actual_from_candidate = (
                final_candidate.index[-1].strftime("%Y-%m-%d")
                if not final_candidate.empty
                else None
            )

            if master_save_path.exists():
                actual_trusted_date = existing_master_date
                status_code = "STALE" if actual_from_candidate else "DOWNLOAD_FAILED"
                message = (
                    f"保留既有 last-known-good master；市场预期 {expected_trade_date_str}，"
                    f"Yahoo可信截止 {source_available_date_str or '无法统一判定'}，"
                    f"现有可信 {actual_trusted_date or 'N/A'}；"
                    f"本轮原因: {last_reason or '未知'}"
                )
            elif not final_candidate.empty:
                df_stale = add_stock_indicators_include(final_candidate, symbol)
                atomic_to_csv_include(df_stale, master_save_path)
                actual_trusted_date = actual_from_candidate
                status_code = "STALE_INITIALIZED"
                message = (
                    f"首次运行无旧 master，保存已清洗但未达到当前可信要求的候选数据；"
                    f"市场预期 {expected_trade_date_str}，实际 {actual_trusted_date or 'N/A'}"
                )
            else:
                actual_trusted_date = None
                status_code = "DOWNLOAD_FAILED"
                message = f"无可用候选数据且无旧 master；原因: {last_reason or '未知'}"

            status.update({
                "actual_trade_date": actual_trusted_date,
                "data_status": status_code,
                "retry_used": retry_used_any,
                "raw_daily_recovered": raw_daily_recovered_any,
                "intraday_recovered": intraday_recovered_any,
                "recovery_mode": recovery_mode_any,
                "data_source": data_source_used,
                "data_source_detail": data_source_detail,
                "intraday_interval": intraday_interval_used,
                "latest_bar_suspect": latest_bar_suspect_any,
                "master_updated": status_code == "STALE_INITIALIZED",
                "message": message,
            })

            print("   " + "=" * 60)
            print(f"   ⚠️ {symbol} 数据完整性警告")
            print(f"      市场预期交易日 : {expected_trade_date_str}")
            print(f"      Yahoo可信截止  : {source_available_date_str or '无法统一判定'}")
            print(f"      实际可信交易日 : {actual_trusted_date or 'N/A'}")
            print(f"      数据状态       : {status_code}")
            print(f"      本轮数据源     : {data_source_used} ({data_source_detail})")
            print(f"      说明           : {message}")
            print("      本次技术指标/PDF不得视为市场预期交易日的最新信号。")
            print("   " + "=" * 60)

        status_map[symbol] = status
        time.sleep(random.uniform(1.5, 3.5))

    save_data_quality_status_include(status_map, current_date_str)

    # todo 26-09-15 修正下载bug
    market_latest_statuses = {
        "OK", "RETRY_RECOVERED", "RAW_DAILY_RECOVERED",
        "INTRADAY_5M_REBUILT", "INTRADAY_1M_REBUILT",
    }
    market_latest_count = sum(
        1 for item in status_map.values()
        if item.get("data_status") in market_latest_statuses
    )
    raw_recovered_count = sum(
        1 for item in status_map.values()
        if item.get("data_status") == "RAW_DAILY_RECOVERED"
    )
    intraday_5m_count = sum(
        1 for item in status_map.values()
        if item.get("data_status") == "INTRADAY_5M_REBUILT"
    )
    intraday_1m_count = sum(
        1 for item in status_map.values()
        if item.get("data_status") == "INTRADAY_1M_REBUILT"
    )
    source_pending_count = sum(
        1 for item in status_map.values()
        if item.get("data_status") == "SOURCE_PENDING"
    )
    bad_count = len(status_map) - market_latest_count - source_pending_count

    print(
        f"\n✅ 个股下载阶段完成：市场最新 {market_latest_count}/{len(status_map)} | "
        f"daily raw恢复 {raw_recovered_count} | 5m恢复 {intraday_5m_count} | 1m恢复 {intraday_1m_count} | "
        f"Yahoo当前可信但待源更新 {source_pending_count}/{len(status_map)} | "
        f"过期/失败 {bad_count}/{len(status_map)}"
        f"\n📂 数据目录: {DOWNLOADER_CONFIG['ind_stock_dir']}"
    )

    # todo 26-09-15 修正下载bug
    # 数据处理完成后逐标的输出最终实际使用的数据源，便于人工核验 daily / 5m / 1m。
    print("\n📊 本轮最终数据源明细:")
    for symbol in tickers:
        item = status_map.get(symbol, {})
        print(
            f"   • {symbol}: {item.get('data_source', 'unknown')} "
            f"({item.get('data_source_detail', 'UNKNOWN')}) | "
            f"可信日期 {item.get('actual_trade_date') or 'N/A'} | "
            f"状态 {item.get('data_status', 'UNKNOWN')}"
        )

    return status_map


# ==================== 6. 主程序入口 ====================
# todo 26-09-15 修正下载bug
def download_stocks_main(target_stocks=None, start_date="2023-01-01"):
    """
    todo 26-09-15 修正下载bug
    个股下载器入口。返回每个标的数据质量状态、market/source 两层日期及最终数据源（daily / 5m / 1m）。
    """
    if target_stocks is None:
        target_stocks = ["META", "MSFT", "NVDA", "TSLA", "MU", "ASML", "AMZN", "AVGO"]

    print("=== 🚀 启动个股历史数据防封禁下载引擎 ===")
    return fetch_daily_stock_data_include(target_stocks, start_date=start_date)


# ============================================================
# 多周期 PDF 报告器（由 generate_stock_report_pdf_v2.py 整合）
# ============================================================
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from pathlib import Path
import arrow
from enum import Enum, auto

# ==================== 0. 修复中文字体显示问题 ====================
# 优先使用 Mac 自带的黑体/苹方，如果未来放到 Windows 上，会自动回退寻找 SimHei
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC', 'Heiti TC', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False  # 确保坐标轴上的负号正常显示


# ===============================================================

# ==================== 1. 环境配置 ====================
class StockReporterEnvType(Enum):
    HOME = auto()
    WORK = auto()


def get_stock_reporter_env_config(env: StockReporterEnvType) -> dict:
    if env == StockReporterEnvType.HOME:
        base_path = Path("/Users/evaseemefly/03data/05-spiders")
    elif env == StockReporterEnvType.WORK:
        base_path = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
    else:
        raise ValueError(f"未知的环境类型: {env}")

    config = {
        'ind_stock_dir': base_path / "individual_stocks",
        'report_dir': base_path / "output/reports",
    }
    config['report_dir'].mkdir(parents=True, exist_ok=True)
    return config


STOCK_REPORTER_CURRENT_ENV = StockReporterEnvType.WORK
STOCK_REPORTER_CONFIG = get_stock_reporter_env_config(STOCK_REPORTER_CURRENT_ENV)


# ==================== 2. 核心绘图引擎 ====================
def create_stock_chart_figure_include(ticker, df, days):
    """为个股生成指定周期的图表，四轴联动 (主图 + MACD + RSI + ATR)"""
    if 'trade_date_utc' in df.columns:
        df = df.set_index('trade_date_utc')
    df.index = pd.to_datetime(df.index)

    plot_df = df.tail(days).copy()
    if len(plot_df) < 20: return None  # 数据太少画图没有意义

    # 创建 4 个子图，调整高度比例 (4:1.5:1:1)，稍微拉高一点画布高度到 10 以防太挤
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(11.69, 10),
                                             gridspec_kw={'height_ratios': [4, 1.5, 1, 1]},
                                             sharex=True)
    # 缩小各个子图之间的纵向间距
    fig.subplots_adjust(hspace=0.05)

    close_col = f'{ticker}_close'

    # ---------------- ax1: 主图 (价格 + 均线 + 布林带) ----------------
    if close_col in plot_df.columns:
        ax1.plot(plot_df.index, plot_df[close_col], label='Price', color='#1f77b4', linewidth=2)

    for ma in ['MA20', 'MA50', 'MA100', 'MA200']:
        if ma in plot_df.columns:
            lw = 2 if ma == 'MA200' else 1
            ls = '-' if ma == 'MA200' else '--'
            ax1.plot(plot_df.index, plot_df[ma], label=ma, linestyle=ls, linewidth=lw, alpha=0.8)

    if 'BB_Lower' in plot_df.columns and 'BB_Upper' in plot_df.columns:
        ax1.fill_between(plot_df.index, plot_df['BB_Lower'], plot_df['BB_Upper'],
                         color='gray', alpha=0.15, label='Bollinger Bands')

    ax1.set_title(f"{ticker} Technical Analysis - Last {days} Days", fontsize=16, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=9, ncol=3)
    ax1.grid(True, linestyle=':', alpha=0.6)

    # ---------------- ax2: MACD ----------------
    if 'MACD_DIF' in plot_df.columns:
        ax2.plot(plot_df.index, plot_df['MACD_DIF'], color='#1f77b4', label='DIF (Fast)', linewidth=1.2)
        ax2.plot(plot_df.index, plot_df['MACD_DEA'], color='#ff7f0e', label='DEA (Slow)', linewidth=1.2)

        # 绘制红绿柱子 (大于0为红色，小于0为绿色)
        colors = ['#d62728' if val > 0 else '#2ca02c' for val in plot_df['MACD_Hist']]
        ax2.bar(plot_df.index, plot_df['MACD_Hist'], color=colors, alpha=0.6, width=0.8, label='MACD Hist')

        ax2.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        ax2.set_ylabel('MACD', fontsize=10)
        ax2.legend(loc='upper left', fontsize=8, ncol=3)
        ax2.grid(True, linestyle=':', alpha=0.6)

    # ---------------- ax3: RSI ----------------
    if 'RSI_14' in plot_df.columns:
        ax3.plot(plot_df.index, plot_df['RSI_14'], color='#9467bd', label='RSI(14)')
        ax3.axhline(y=70, color='#d62728', linestyle='--', alpha=0.5)
        ax3.axhline(y=30, color='#2ca02c', linestyle='--', alpha=0.5)
        ax3.fill_between(plot_df.index, 30, 70, color='#9467bd', alpha=0.05)
        ax3.set_ylim(0, 100)
        ax3.set_ylabel('RSI', fontsize=10)
        ax3.legend(loc='upper left', fontsize=8)
        ax3.grid(True, linestyle=':', alpha=0.6)

    # ---------------- ax4: ATR (真实波动幅度) ----------------
    if 'ATR_14' in plot_df.columns:
        ax4.plot(plot_df.index, plot_df['ATR_14'], color='#8c564b', label='ATR(14)')
        ax4.set_ylabel('ATR', fontsize=10)
        ax4.legend(loc='upper left', fontsize=8)
        ax4.grid(True, linestyle=':', alpha=0.6)

    plt.xticks(rotation=0)
    # plt.tight_layout() # 因为使用了 subplots_adjust，这里关掉 tight_layout 以防布局冲突
    return fig


# ==================== 3. 报告合成逻辑 ====================
def generate_stock_pdf_main(target_stocks=None):
    # 增加参数判断：如果外部没传，则使用默认股票池
    if target_stocks is None:
        target_stocks = ['META', 'MSFT', 'NVDA', 'TSLA', 'MU', 'ASML', 'AMZN']
    current_date = arrow.now().format('YYYY_MM_DD')

    report_name = f"Stock_MultiPeriod_Report_{current_date}.pdf"
    output_path = STOCK_REPORTER_CONFIG['report_dir'] / report_name

    print(f"🚀 开始生成 {current_date} 深度量价分析报告...")

    # 使用 PdfPages 直接将图表写入 PDF
    with PdfPages(output_path) as pdf:

        # 1. 绘制一个精美的 PDF 封面页
        fig_cover = plt.figure(figsize=(11.69, 8.27))  # A4 横向尺寸
        fig_cover.text(0.5, 0.6, "个股多周期深度分析报告", ha='center', va='center', fontsize=32, color='#1f77b4',
                       fontweight='bold')
        fig_cover.text(0.5, 0.5, f"生成日期: {arrow.now().format('YYYY-MM-DD')}", ha='center', va='center', fontsize=16)
        fig_cover.text(0.5, 0.45, "数据来源: Local individual_stocks_master", ha='center', va='center', fontsize=12,
                       color='gray')
        fig_cover.text(0.5, 0.35, "drcc Quant Engine", ha='center', va='center', fontsize=14, fontstyle='italic',
                       color='#777777')
        pdf.savefig(fig_cover)
        plt.close(fig_cover)

        # 2. 遍历个股和周期生成图表页
        for ticker in target_stocks:
            file_path = STOCK_REPORTER_CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
            if not file_path.exists():
                print(f"⚠️ 跳过 {ticker}: 找不到历史数据文件")
                continue

            print(f"📊 正在处理 {ticker}...")
            df = pd.read_csv(file_path)

            for days in [200]:
                # for days in [20, 50, 100, 200]:
                fig = create_stock_chart_figure_include(ticker, df, days)
                if fig:
                    pdf.savefig(fig)  # 直接将图表保存为 PDF 的一页
                    plt.close(fig)  # 及时释放内存

    print(f"\n✅ 报告生成成功！\n📂 保存路径: {output_path}")


# ============================================================
# 四维个股流水线（run_quant_pipeline_4dim_260904_v5.py）
# ============================================================
"""
个股行情下载 + 四维风险确认 + 多周期 PDF 流水线（260904 v5）。

旧版 downloader / reporter 保持不变。本入口先刷新统一的 VOO/QQQ 四维风险快照，
再把风险摘要作为新版 PDF 的首页和每个标的页面的执行提示。
"""

import json
import sys
import time
import types
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# todo 26-09-15 修正下载bug
# 风险引擎版本沿用上方四维执行层已经定义的 VERSION_TAG（当前为 4dim_260909_v6）。
# 流水线版本独立维护，避免把“模型版本日期”误解成“市场数据日期”。
RISK_ENGINE_VERSION = VERSION_TAG
PIPELINE_VERSION = "pipeline_260915_v3"


def refresh_four_dim_snapshot() -> Dict[str, Any]:
    """运行统一风险引擎；失败时读取最近一次快照，避免整条个股流水线中断。"""
    engine = _load_risk_engine()
    try:
        return engine.generate_daily_report()
    except Exception as exc:
        latest = engine.OUTPUT_PATH / f"four_dim_risk_snapshot_{RISK_ENGINE_VERSION}_latest.json"
        if latest.exists():
            print(f"⚠️ 四维风险刷新失败，改用最近快照: {exc}")
            return json.loads(latest.read_text(encoding="utf-8"))
        raise RuntimeError(f"四维风险刷新失败且没有可用快照: {exc}") from exc


def _asset_risk_for_ticker(snapshot: Dict[str, Any], ticker: str) -> Dict[str, Any]:
    """科技/成长标的参考 QQQ，其余标的参考 VOO。"""
    qqq_proxy = {
        "QQQ", "MSFT", "NVDA", "GOOGL", "DRAM", "AVGO", "AAPL", "APH", "ANET",
        "CSCO", "WDC", "SOXX", "TSLA", "MU", "ASML", "AMZN", "MAGS", "SMH", "META",
    }
    proxy = "QQQ" if ticker.upper() in qqq_proxy else "VOO"
    return snapshot.get("assets", {}).get(proxy, {})


# todo 26-09-04
FOUR_DIM_BUTTON_SPECS = [
    ("price", "P", "价格"),
    ("macro", "M", "宏观"),
    ("credit", "C", "信用"),
    ("volatility_breadth", "VB", "波动/广度"),
]

# todo 26-09-04
FOUR_DIM_STATUS_STYLES = {
    "green": {
        "face": "#2e7d32",
        "edge": "#1b5e20",
        "text": "white",
    },
    "yellow": {
        "face": "#f2c94c",
        "edge": "#b7791f",
        "text": "#233044",
    },
    "red": {
        "face": "#d92d20",
        "edge": "#912018",
        "text": "white",
    },
    "unknown": {
        "face": "#98a2b3",
        "edge": "#667085",
        "text": "white",
    },
}


# todo 26-09-04
def _normalize_four_dim_status(status: Any) -> str:
    """将 green/yellow/red、G/Y/R、中文颜色等统一为标准状态。"""
    if status is None:
        return "unknown"

    value = str(status).strip().lower()

    mapping = {
        "g": "green",
        "green": "green",
        "绿": "green",
        "绿色": "green",
        "normal": "green",
        "ok": "green",
        "y": "yellow",
        "yellow": "yellow",
        "黄": "yellow",
        "黄色": "yellow",
        "warning": "yellow",
        "warn": "yellow",
        "r": "red",
        "red": "red",
        "红": "red",
        "红色": "red",
        "risk": "red",
        "danger": "red",
    }

    return mapping.get(value, "unknown")


# todo 26-09-04
def _four_dim_status_map(votes: Dict[str, Any]) -> Dict[str, str]:
    """从四维 votes 中提取每个维度的风险灯状态。"""
    result = {}

    for key, _, _ in FOUR_DIM_BUTTON_SPECS:
        raw_status = votes.get(key, {}).get("status", None)
        result[key] = _normalize_four_dim_status(raw_status)

    return result


# todo 26-09-04
def _draw_four_dim_buttons_in_cell(
        ax,
        cell,
        statuses: Dict[str, str],
) -> None:
    """
    在表格单元格内绘制 P/M/C/VB 四个彩色长方形按钮。

    说明：
    - P  = Price，价格结构；
    - M  = Macro，宏观/利率；
    - C  = Credit，信用；
    - VB = Volatility & Breadth，波动/广度。
    """
    x0 = cell.get_x()
    y0 = cell.get_y()
    width = cell.get_width()
    height = cell.get_height()

    pad_x = width * 0.055
    gap = width * 0.025
    button_width = (width - 2 * pad_x - 3 * gap) / 4
    button_height = height * 0.48
    button_y = y0 + (height - button_height) / 2

    for idx, (key, label, _) in enumerate(FOUR_DIM_BUTTON_SPECS):
        status = statuses.get(key, "unknown")
        style = FOUR_DIM_STATUS_STYLES.get(
            status,
            FOUR_DIM_STATUS_STYLES["unknown"],
        )

        button_x = x0 + pad_x + idx * (button_width + gap)

        patch = FancyBboxPatch(
            (button_x, button_y),
            button_width,
            button_height,
            boxstyle="round,pad=0.004,rounding_size=0.01",
            transform=ax.transAxes,
            facecolor=style["face"],
            edgecolor=style["edge"],
            linewidth=0.8,
            alpha=0.96,
            clip_on=False,
            zorder=20,
        )
        ax.add_patch(patch)

        ax.text(
            button_x + button_width / 2,
            button_y + button_height / 2,
            label,
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=8.2,
            fontweight="bold",
            color=style["text"],
            zorder=21,
            clip_on=False,
        )


# todo 26-09-04
def _draw_four_dim_buttons_for_table(
        fig,
        ax,
        table,
        row_statuses: list,
        status_col: int = 2,
) -> None:
    """将表格中四维状态列由 G/Y/R 文本替换为彩色按钮。"""
    # 先让 matplotlib 完成表格布局计算，否则 cell 坐标可能还未刷新。
    fig.canvas.draw()

    for row_index, statuses in enumerate(row_statuses, start=1):
        cell = table[(row_index, status_col)]
        cell.get_text().set_text("")
        cell.set_facecolor("#ffffff")
        _draw_four_dim_buttons_in_cell(
            ax=ax,
            cell=cell,
            statuses=statuses,
        )


# todo 26-09-04
def _four_dim_button_legend_text() -> str:
    return (
        "P=价格结构  M=宏观/利率  C=信用  VB=波动/广度    "
        "绿色=正常  黄色=预警  红色=风险"
    )


# todo 26-09-15 修正下载bug
def _draw_four_dim_cover(snapshot: Dict[str, Any], data_quality: Optional[Dict[str, Any]] = None):
    """
    todo 26-09-15 修正下载bug
    绘制四维风险执行首页，并显示 Yahoo adjusted/raw 恢复模式、市场预期日及个股数据状态。
    """
    legacy_reporter = _load_sibling_module("generate_stock_report_pdf_v2.py", "legacy_stock_reporter_v2_cover")
    plt = legacy_reporter.plt
    fig = plt.figure(figsize=(11.69, 8.27))
    fig.patch.set_facecolor("#f7f9fb")
    fig.text(0.06, 0.90, "个股多周期报告 · 四维风险执行首页", fontsize=26, fontweight="bold", color="#233044")
    fig.text(
        0.06,
        0.855,
        f"市场风险数据日: {snapshot.get('trade_date', 'N/A')}  |  报告生成: {datetime.now():%Y-%m-%d}  |  "
        f"风险引擎: {RISK_ENGINE_VERSION}  |  流水线: {PIPELINE_VERSION}",
        fontsize=10.5,
        color="#667085",
    )

    quality_map = data_quality or {}
    quality_total = len(quality_map)
    first_dq = next(iter(quality_map.values()), {}) if quality_map else {}
    market_expected = first_dq.get("market_expected_date", first_dq.get("expected_trade_date", "N/A"))
    source_available = first_dq.get("source_available_date", "N/A")
    source_status = first_dq.get("source_status", "UNKNOWN")
    # todo 26-09-15 修正下载bug
    source_mode = first_dq.get("source_mode", "UNKNOWN")

    if quality_total > 0:
        source_color = {
            "SOURCE_READY": "#067647",
            "SOURCE_PENDING": "#b54708",
            "SOURCE_ERROR": "#b42318",
        }.get(source_status, "#667085")
        fig.text(
            0.06,
            0.817,
            f"数据源状态: {source_status}  |  市场预期交易日: {market_expected}  |  "
            f"Yahoo可信截止: {source_available or 'N/A'}  |  源模式: {source_mode}",
            fontsize=10.5,
            color=source_color,
            fontweight="bold",
        )

        market_latest = sum(
            1 for item in quality_map.values()
            if item.get("data_status") in {"OK", "RETRY_RECOVERED", "RAW_DAILY_RECOVERED", "INTRADAY_5M_REBUILT", "INTRADAY_1M_REBUILT"}
        )
        # todo 26-09-15 修正下载bug
        raw_recovered = sum(
            1 for item in quality_map.values()
            if item.get("data_status") == "RAW_DAILY_RECOVERED"
        )
        intraday_5m = sum(
            1 for item in quality_map.values()
            if item.get("data_status") == "INTRADAY_5M_REBUILT"
        )
        intraday_1m = sum(
            1 for item in quality_map.values()
            if item.get("data_status") == "INTRADAY_1M_REBUILT"
        )
        pending = sum(
            1 for item in quality_map.values()
            if item.get("data_status") == "SOURCE_PENDING"
        )
        bad = quality_total - market_latest - pending
        quality_text = (
            f"个股数据: 市场最新 {market_latest}/{quality_total}（raw {raw_recovered} / 5m {intraday_5m} / 1m {intraday_1m}）  |  "
            f"Yahoo当前可信/待源更新 {pending}/{quality_total}  |  "
            f"过期或异常 {bad}/{quality_total}"
        )
        fig.text(
            0.06,
            0.787,
            quality_text,
            fontsize=10.2,
            color="#b42318" if bad > 0 else ("#b54708" if pending > 0 else "#067647"),
            fontweight="bold",
        )

    # todo 26-09-04
    rows = []
    row_statuses = []
    for asset in ["VOO", "QQQ"]:
        item = snapshot.get("assets", {}).get(asset, {})
        execution = item.get("execution", {})
        votes = item.get("votes", {})
        macro = votes.get("macro", {})
        credit = votes.get("credit", {})
        row_statuses.append(_four_dim_status_map(votes))
        rows.append([
            asset,
            execution.get("state", "N/A"),
            "",
            f"{macro.get('momentum', 0):+.2f} ({macro.get('change', 0):+.2f})",
            f"{credit.get('deviation', 0):+.2%}",
            execution.get("action", "N/A"),
        ])

    ax = fig.add_axes([0.055, 0.43, 0.89, 0.32])
    ax.axis("off")
    table = ax.table(
        cellText=rows,
        colLabels=["代理", "防抖后等级", "四维风险灯", "US10Y动量(环比)", "HYG偏离MA60", "最终动作"],
        cellLoc="center",
        colWidths=[0.07, 0.20, 0.18, 0.16, 0.13, 0.26],
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.0, 2.0)
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#d0d5dd")
        if row == 0:
            cell.set_facecolor("#344054")
            cell.set_text_props(color="white", weight="bold")
        else:
            cell.set_facecolor("#ffffff")

    _draw_four_dim_buttons_for_table(
        fig=fig, ax=ax, table=table, row_statuses=row_statuses, status_col=2
    )

    fig.text(0.06, 0.385, _four_dim_button_legend_text(), fontsize=9.8, color="#667085")
    fig.text(0.06, 0.34, "执行规则", fontsize=15, fontweight="bold", color="#233044")
    fig.text(
        0.06, 0.27,
        "Level 0：正常交易\nLevel 1：停止新增仓位，不减仓\n"
        "Level 2：跨类别且连续两日确认，先减核心 Beta 第一档\n"
        "Level 3：价格结构与多维风险同步恶化，快速防守",
        fontsize=12, linespacing=1.5, color="#475467",
    )
    fig.text(
        0.06, 0.08,
        "注意：个股技术图仍是风险雷达；四维层只覆盖新增仓位动作，不取消个股既有止损/止盈纪律。",
        fontsize=10.5, color="#667085",
    )
    return fig


# todo 26-09-15 修正下载bug
def generate_stock_report_with_four_dim(
        target_stocks,
        snapshot: Dict[str, Any],
        data_quality: Optional[Dict[str, Any]] = None,
) -> Path:
    """
    todo 26-09-15 修正下载bug
    复用旧绘图引擎生成四维整合版 PDF；个股页显示 adjusted/raw 恢复状态和最后可信交易日。
    """
    legacy_reporter = _load_sibling_module("generate_stock_report_pdf_v2.py", "legacy_stock_reporter_v2")
    report_dir = legacy_reporter.CONFIG["report_dir"] / RISK_ENGINE_VERSION
    report_dir.mkdir(parents=True, exist_ok=True)
    current_date = datetime.now().strftime("%Y_%m_%d")
    output_path = report_dir / f"Stock_MultiPeriod_Report_{current_date}_{PIPELINE_VERSION}.pdf"

    print(f"🚀 开始生成四维整合个股报告: {output_path.name}")
    with legacy_reporter.PdfPages(output_path) as pdf:
        cover = _draw_four_dim_cover(snapshot, data_quality=data_quality)
        pdf.savefig(cover)
        legacy_reporter.plt.close(cover)

        for ticker in target_stocks:
            file_path = legacy_reporter.CONFIG["ind_stock_dir"] / f"individual_stocks_master_{ticker}.csv"
            if not file_path.exists():
                print(f"⚠️ 跳过 {ticker}: 找不到历史数据文件")
                continue
            print(f"📊 正在处理 {ticker}...")
            df = legacy_reporter.pd.read_csv(file_path)
            fig = legacy_reporter.create_chart_figure(ticker, df, 200)
            if fig is None:
                continue

            risk = _asset_risk_for_ticker(snapshot, ticker)
            execution = risk.get("execution", {})

            # todo 26-09-15 修正下载bug
            dq = (data_quality or {}).get(ticker, {})
            dq_status = dq.get("data_status", "UNKNOWN")
            dq_expected = dq.get("market_expected_date", dq.get("expected_trade_date", "N/A"))
            dq_source = dq.get("source_available_date", "N/A")
            # todo 26-09-15 修正下载bug
            dq_data_source = dq.get("data_source", "unknown")
            dq_data_source_detail = dq.get("data_source_detail", "UNKNOWN")

            # 即使质量 JSON 缺失，也从实际 master 文件读取最后日期，保证每页都能看到数据截止日。
            file_actual = "N/A"
            if "trade_date_utc" in df.columns:
                parsed_dates = legacy_reporter.pd.to_datetime(df["trade_date_utc"], errors="coerce").dropna()
                if not parsed_dates.empty:
                    file_actual = parsed_dates.max().strftime("%Y-%m-%d")
            dq_actual = dq.get("actual_trade_date") or file_actual

            if dq_status == "OK":
                data_line = (
                    f"最新可信交易日 {dq_actual} | 数据源 {dq_data_source} ({dq_data_source_detail}) | "
                    f"数据状态：OK（已达到市场预期 {dq_expected}）"
                )
                data_color = "#067647"
                data_weight = "normal"
            elif dq_status == "RETRY_RECOVERED":
                data_line = (
                    f"最新可信交易日 {dq_actual} | 数据源 {dq_data_source} ({dq_data_source_detail}) | "
                    f"数据状态：adjusted重拉后恢复 | 市场预期 {dq_expected}"
                )
                data_color = "#b54708"
                data_weight = "normal"
            # todo 26-09-15 修正下载bug
            elif dq_status == "RAW_DAILY_RECOVERED":
                data_line = (
                    f"最新可信交易日 {dq_actual} | 数据源 {dq_data_source} ({dq_data_source_detail}) | 数据状态：Raw日线恢复 | "
                    f"历史保持adjusted口径 | 市场预期 {dq_expected}"
                )
                data_color = "#b54708"
                data_weight = "bold"
            # todo 26-09-15 修正下载bug
            elif dq_status == "INTRADAY_5M_REBUILT":
                data_line = (
                    f"最新可信交易日 {dq_actual} | 数据源 5m ({dq_data_source_detail}) | "
                    f"Yahoo daily Close缺失，5m恢复 | 市场预期 {dq_expected}"
                )
                data_color = "#b54708"
                data_weight = "bold"
            elif dq_status == "INTRADAY_1M_REBUILT":
                data_line = (
                    f"最新可信交易日 {dq_actual} | 数据源 1m ({dq_data_source_detail}) | "
                    f"Yahoo daily Close缺失，1m恢复 | 市场预期 {dq_expected}"
                )
                data_color = "#b54708"
                data_weight = "bold"
            elif dq_status == "SOURCE_PENDING":
                data_line = (
                    f"最新可信交易日 {dq_actual} | 数据源 {dq_data_source} ({dq_data_source_detail}) | Yahoo日线待更新 | "
                    f"市场预期 {dq_expected} | Yahoo可信截止 {dq_source}"
                )
                data_color = "#b54708"
                data_weight = "bold"
            elif dq_status == "UNKNOWN":
                data_line = f"最新可信交易日 {dq_actual} | 数据源 {dq_data_source} | 数据状态：UNKNOWN（未加载本轮质量状态）"
                data_color = "#667085"
                data_weight = "normal"
            else:
                data_line = (
                    f"⚠ 最新可信交易日 {dq_actual} | 数据源 {dq_data_source} ({dq_data_source_detail}) | 数据状态 {dq_status} | "
                    f"市场预期 {dq_expected} | Yahoo可信截止 {dq_source}；不得视为最新信号"
                )
                data_color = "#b42318"
                data_weight = "bold"

            fig.text(
                0.5, 0.032, data_line, ha="center", fontsize=9.2,
                color=data_color, fontweight=data_weight,
            )

            fig.text(
                0.5,
                0.012,
                f"四维执行覆盖（{risk.get('asset', 'N/A')}代理）：{execution.get('state', 'N/A')} | "
                f"{execution.get('action', 'N/A')}",
                ha="center",
                fontsize=10,
                color="#b42318" if execution.get("level", 0) >= 2 else "#475467",
                fontweight="bold",
            )
            pdf.savefig(fig)
            legacy_reporter.plt.close(fig)

    print(f"✅ 四维整合个股报告已生成: {output_path}")
    return output_path


# todo 26-09-15 修正下载bug
def run_pipeline(is_need_download: bool = True, refresh_risk: bool = True) -> Optional[Path]:
    """
    todo 26-09-15 修正下载bug
    四维整合主流水线：加入 adjusted daily -> raw daily -> 5m -> 1m 恢复链、Yahoo 源级预检及逐页数据源显示。
    """
    print("=" * 68)
    print(f"🌟 DRCC Quant Engine · {PIPELINE_VERSION} | 风险引擎 {RISK_ENGINE_VERSION}")
    print("=" * 68)

    # target_stocks = [
    #     "MSFT", "NVDA", "GOOGL", "DRAM", "AVGO", "AAPL", "APH", "ANET", "CSCO", "WDC", "SOXX",
    #     "QQQ", "VOO", "SPY", "TSLA", "MU", "ASML", "AMZN", "MAGS", "SMH", "META",
    # ]
    target_stocks = [
        "MSFT", "NVDA", "GOOGL", "DRAM", "AVGO", "AAPL", "APH", "ANET", "SOXX",
        "TSLA", "MU", "ASML", "AMZN", "MAGS", "SMH", "META", "QQQ", "VOO", "SPY", "XLP", "XLV", "COWZ", 'SCHD'
    ]
    start_date = "2023-01-01"
    print(f"📌 当前全局监控股票池: {target_stocks}\n")

    try:
        # todo 26-09-15 修正下载bug
        data_quality = {}
        if is_need_download:
            print(">> [阶段 1/3] 下载个股行情并更新技术指标...")
            downloader = _load_sibling_module("indivalual_stocks_download_v2.py", "legacy_stock_downloader_v2")
            data_quality = downloader.main(target_stocks=target_stocks, start_date=start_date) or {}
            time.sleep(2)
        else:
            print(">> [阶段 1/3] 跳过个股行情下载。")
            data_quality = load_latest_data_quality_status_include()

        print(">> [阶段 2/3] 刷新四维风险、边际变化与防抖状态...")
        if refresh_risk:
            snapshot = refresh_four_dim_snapshot()
        else:
            engine = _load_risk_engine()
            latest = engine.OUTPUT_PATH / f"four_dim_risk_snapshot_{RISK_ENGINE_VERSION}_latest.json"
            if not latest.exists():
                raise FileNotFoundError(f"找不到四维风险快照: {latest}")
            snapshot = json.loads(latest.read_text(encoding="utf-8"))

        print(">> [阶段 3/3] 生成带四维执行首页的个股 PDF...")
        result = generate_stock_report_with_four_dim(target_stocks, snapshot, data_quality=data_quality)
        print("\n🎉 四维整合流水线执行完毕！")
        return result
    except Exception as exc:
        print(f"\n❌ 流水线执行过程中遇到严重错误: {exc}")
        return None


# ============================================================
# 自包含依赖桥：以下对象只引用本文件上方已经定义的代码
# ============================================================
_INCLUDE_RISK_ENGINE = _SimpleNamespace(
    OUTPUT_PATH=OUTPUT_PATH,
    VERSION_TAG=RISK_ENGINE_VERSION,
    generate_daily_report=generate_daily_report,
)
_INCLUDE_DOWNLOADER = _SimpleNamespace(main=download_stocks_main)
_INCLUDE_STOCK_REPORTER = _SimpleNamespace(
    plt=plt,
    pd=pd,
    PdfPages=PdfPages,
    CONFIG=STOCK_REPORTER_CONFIG,
    create_chart_figure=create_stock_chart_figure_include,
)


def _load_risk_engine():
    return _INCLUDE_RISK_ENGINE


def _load_sibling_module(filename: str, module_name: str):
    modules = {
        "indivalual_stocks_download_v2.py": _INCLUDE_DOWNLOADER,
        "generate_stock_report_pdf_v2.py": _INCLUDE_STOCK_REPORTER,
    }
    if filename not in modules:
        raise ImportError(f"自包含版未注册模块: {filename}")
    return modules[filename]


if __name__ == "__main__":
    run_pipeline(True)
