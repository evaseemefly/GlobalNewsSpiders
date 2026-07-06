import pandas as pd
import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# todo 26-07-06
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors import LinearSegmentedColormap, Normalize
from matplotlib.patches import Rectangle

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
        "current_shares": 20,
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
        ax_vix.axhline(p["vix_risk_th"], color="#c0392b", linestyle="--", linewidth=1.1, label=f"VIX风险阈值 {p['vix_risk_th']}")
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


if __name__ == "__main__":
    generate_daily_report()
