import pandas as pd
import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pathlib import Path
from enum import Enum, auto


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
        # todo 26-06-17: QQQ 保持原五层 V3，只增加 profile 以便和 VOO Final #7 分流。
        "strategy_name": "QQQ Five-Layer V3",
        "strategy_profile": "five_layer_v3",

        # 趋势均线
        "ma_len": 200,
        "crash_ma_len": 200,

        # 利率阈值
        "us10y_th": 0.15,

        # VIX 风险阈值
        "vix_warning_low": 15,
        "vix_warning_high": 20,
        "vix_risk_th": 20,
        "vix_crash_th": 30,
        "vix_extreme_th": 45,

        # RSI / 恐慌反转
        "rsi_th": 30,
        "panic_rsi_low": 35,
        "panic_rsi_high": 45,
        "panic_drop_pct": 0.045,

        # 五层仓位
        "risk_on_pos": 1.00,
        "risk_warning_pos": 0.60,
        "risk_pos": 0.30,
        "panic_reversal_pos": 0.40,
        "crash_pos": 0.25,

        # 实盘资金池与持仓
        "portfolio_value": 35730,
        "current_shares": 13,
    },

    "VOO": {
        # todo 26-06-17: VOO 切换为最终确认的 #7 参数化方案。
        "strategy_name": "VOO Final #7",
        "strategy_profile": "final7_v3",

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

        # 五层仓位
        "risk_on_pos": 1.00,
        "risk_warning_pos": 0.70,
        "risk_pos": 0.30,
        "panic_reversal_pos": 0.50,
        "crash_pos": 0.30,

        # 实盘资金池与持仓
        "portfolio_value": 53600,
        "current_shares": 25,
    },
}

plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
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


def is_final7_profile(p: dict) -> bool:
    # todo 26-06-17: 用 profile 区分 VOO Final #7 与 QQQ 原五层模型，避免误改 QQQ。
    """判断是否使用 VOO Final #7 执行逻辑。"""
    return p.get("strategy_profile") == "final7_v3"


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

    # todo 26-06-17: VOO Final #7 使用回测一致的三层执行逻辑：
    # risk_off -> dip_buy -> base_trend -> default，不启用 QQQ 的五层状态。
    if is_final7_profile(p):
        vix_dynamic_risk = vix > today["VIX_MA60"] * p.get("vix_ma_multiplier", 1.8)
        vix_risk = (vix > p["vix_risk_th"]) or vix_dynamic_risk

        risk_off = us10y_rising or hyg_divergence or vix_risk
        raw_dip_buy = (
                rsi < p["rsi_th"]
                and close > open_price
                and today["VIX_close"] < yesterday["VIX_close"]
        )
        dip_buy = raw_dip_buy and not risk_off

        if risk_off:
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
                f"🛡️【Final #7 Risk-Off】{trigger_text}，"
                f"降低至 {int(target_position * 100)}% 防守仓。"
            )
        elif dip_buy:
            state = "Dip-Buy"
            target_position = p["risk_on_pos"]
            action_reason = (
                f"🚨【Final #7 非 Risk-Off 抄底】RSI 跌破 {p['rsi_th']}，"
                f"收盘强于开盘且 VIX 回落，允许恢复至 {int(target_position * 100)}%。"
            )
        elif base_trend:
            state = "Risk-On"
            target_position = p["risk_on_pos"]
            action_reason = (
                f"📈【Final #7 顺势做多】价格站上 MA{p['ma_len']}，"
                f"维持 {int(target_position * 100)}% 仓位。"
            )
        else:
            state = "Trend-Weak"
            target_position = p["risk_pos"]
            action_reason = (
                f"📉【Final #7 趋势走弱】跌破 MA{p['ma_len']}，"
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

    # todo 26-06-17: VOO Final #7 的历史仓位序列严格对齐回测规则。
    if is_final7_profile(p):
        vix_dynamic_risk = vix > df["VIX_MA60"] * p.get("vix_ma_multiplier", 1.8)
        vix_risk = (vix > p["vix_risk_th"]) | vix_dynamic_risk
        risk_off = us10y_rising | hyg_divergence | vix_risk

        open_col = f"{asset}_open"
        raw_dip_buy = (
                (rsi < p["rsi_th"])
                & (df[f"{asset}_close"] > df[open_col])
                & (df["VIX_close"] < df["VIX_close"].shift(1))
        )
        dip_buy = raw_dip_buy & (~risk_off)

        df["raw_dip_buy"] = raw_dip_buy
        df["dip_buy"] = dip_buy
        df["risk_off"] = risk_off
        df["vix_dynamic_risk"] = vix_dynamic_risk

        df["position_raw"] = np.select(
            [
                risk_off,
                dip_buy,
                base_trend,
            ],
            [
                p["risk_pos"],
                p["risk_on_pos"],
                p["risk_on_pos"],
            ],
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
        # todo 26-06-17: VOO Final #7 没有 Panic-Reversal 插件，Risk-Off 期间只保留防守仓。
        if is_final7_profile(p):
            return f"""
📍 当前为 Final #7 Risk-Off 状态：保持 {target_position * 100:.0f}% 防守仓。
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


# ============================================================
# 8. 图表生成
# ============================================================

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

    # todo 26-06-17: VOO Final #7 图表提示改为回测一致的 risk_off 优先口径。
    risk_note = (
        "Final #7: Risk-Off 优先；Dip-Buy 仅在非 Risk-Off 下生效"
        if is_final7_profile(p)
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
    # todo 26-06-17: Final #7 不展示 Panic RSI 区间，避免和最终策略逻辑混淆。
    if not is_final7_profile(p):
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

    # todo 26-06-17: VOO Final #7 输出文件名单独标记，避免和旧 V3 图混淆。
    file_strategy_tag = "final7" if is_final7_profile(p) else "v3"
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
    print("🚀 VOO / QQQ 五层量化交易信号生成器 V3 启动")
    print("=" * 80)

    master_df = load_master_data()

    for asset, p in ASSET_CONFIG.items():
        print(f"\n⏳ 正在处理 {asset}...")

        # 关键修正：先过滤真实交易日，再计算指标
        asset_df = filter_real_trading_days(master_df, asset)
        df = process_asset_indicators(asset_df, asset, p)

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

        trend_dist = (today[f"{asset}_close"] / today[f"{asset}_MA"] - 1) * 100

        # todo 26-06-17: 日报标题使用策略名，VOO 明确标注 Final #7 参数方案。
        strategy_name = p.get("strategy_name", f"{asset} V3")
        strategy_param_line = ""
        if is_final7_profile(p):
            strategy_param_line = (
                f"🎛️【Final #7 参数】MA{p['ma_len']} | US10Y>{p['us10y_th']} | "
                f"VIX>{p['vix_risk_th']} 或 VIX>{p.get('vix_ma_multiplier', 1.8)}×MA60 | "
                f"RSI<{p['rsi_th']} | Risk仓位 {p['risk_pos'] * 100:.0f}%\n\n"
            )

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

        # todo 26-06-17: VOO Final #7 文本报告文件名单独标记，避免覆盖旧 V3 结果。
        file_strategy_tag = "final7" if is_final7_profile(p) else "v3"
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

    print("\n" + "=" * 80)
    print("🎉 VOO / QQQ 全部信号生成完成")
    print(f"📂 输出目录: {OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    generate_daily_report()
