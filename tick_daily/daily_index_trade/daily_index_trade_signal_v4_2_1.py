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
# 2. 资产参数配置 (V4.2.1 核心参数集)
# ============================================================

ASSET_CONFIG = {
    "VOO": {
        # --- V3 保留的核心参数 ---
        "ma_len": 100,
        "us10y_th": 0.15,
        "vix_th": 45,
        "rsi_th": 30,

        # --- 1. 状态识别边界 (风控雷达) ---
        "vix_warning_low": 15,  # 进入预警观察区的下限
        "vix_warning_high": 20,  # 预警转实质风险的上限边界
        "vix_risk_low": 20,  # 尾部风险激增区下限
        "vix_risk_high": 30,  # 恐慌极值区上限
        "vix_crash_th": 30,  # 配合均线破位判定崩盘的 VIX 阈值
        "panic_drop_pct": 0.020,  # 单日跌幅 2.0% 触发独立狙击
        "panic_rsi_low": 35,
        "panic_rsi_high": 45,

        # --- 2. 目标仓位设定 (理论底线) ---
        "risk_warning_pos": 0.60,  # VIX 15-20 时的防御缓冲仓
        "panic_reversal_pos": 0.40,  # 暴跌狙击初期的试探仓
        "risk_pos": 0.30,  # Risk-Off/Crash 时的绝对底仓
        "crash_pos": 0.30,  # (同 Risk-Off，统一为 0.30)
        "risk_on_pos": 1.00,  # 满仓进攻

        # --- 3. 执行节奏控制 (调仓微操) ---
        "risk_warning_confirm_days": 3,  # 连续预警 3 天允许操作
        "risk_warning_gap_buy_pct": 0.25,  # 预警区补仓单次买回 1/4
        "risk_repair_vix_th": 20,  # 风险初步解除阈值
        "risk_repair_gap_buy_pct": 0.67,  # 风险修复单次买回 2/3
        "risk_on_gap_buy_pct": 1.00,  # 重回 Risk-On 一步满仓
        "panic_gap_buy_pct": 0.50,  # 暴跌狙击单次推入 1/2

        # --- 实盘资金池与持仓 ---
        "portfolio_value": 53600,
        "current_shares": 25,
    },

    "QQQ": {
        # QQQ 沿用 V3 均线与波动逻辑，执行层参数同步 VOO (可自行针对 QQQ 微调)
        "ma_len": 200,
        "us10y_th": 0.15,
        "vix_th": 45,
        "rsi_th": 30,

        "vix_warning_low": 15,
        "vix_warning_high": 20,
        "vix_risk_low": 20,
        "vix_risk_high": 30,
        "vix_crash_th": 30,
        "panic_drop_pct": 0.045,  # QQQ 波动率大，保留 4.5%
        "panic_rsi_low": 35,
        "panic_rsi_high": 45,

        "risk_warning_pos": 0.60,
        "panic_reversal_pos": 0.40,
        "risk_pos": 0.30,
        "crash_pos": 0.25,
        "risk_on_pos": 1.00,

        "risk_warning_confirm_days": 3,
        "risk_warning_gap_buy_pct": 0.25,
        "risk_repair_vix_th": 20,
        "risk_repair_gap_buy_pct": 0.67,
        "risk_on_gap_buy_pct": 1.00,
        "panic_gap_buy_pct": 0.50,

        "portfolio_value": 35730,
        "current_shares": 13,
    },
}

plt.rcParams["font.sans-serif"] = ["PingFang SC", "Arial Unicode MS", "Heiti TC", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False


# ============================================================
# 3. 基础指标函数
# ============================================================

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def load_master_data() -> pd.DataFrame:
    if not CSV_FILE_PATH.exists():
        raise FileNotFoundError(f"找不到主数据文件: {CSV_FILE_PATH}")
    df = pd.read_csv(CSV_FILE_PATH)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    return df.set_index("trade_date_utc").sort_index()


def filter_real_trading_days(df: pd.DataFrame, asset: str) -> pd.DataFrame:
    df = df.copy()
    close_col = f"{asset}_close"
    volume_col = f"{asset}_volume"
    if close_col not in df.columns:
        raise KeyError(f"缺少字段: {close_col}")
    df = df[df[close_col].notna()]
    df = df[df.index.dayofweek < 5]
    if volume_col in df.columns:
        df = df[df[volume_col] > 0]
    return df.ffill()


def process_asset_indicators(df: pd.DataFrame, asset: str, p: dict) -> pd.DataFrame:
    df = df.copy()
    close_col = f"{asset}_close"
    open_col = f"{asset}_open"

    df[f"{asset}_MA"] = df[close_col].rolling(p["ma_len"], min_periods=1).mean()
    df[f"{asset}_MA100"] = df[close_col].rolling(100, min_periods=1).mean()
    df[f"{asset}_MA200"] = df[close_col].rolling(200, min_periods=1).mean()
    df[f"{asset}_daily_return"] = df[close_col].pct_change()

    df["US10Y_diff_20"] = df["US10Y_close"].diff(20)
    df["HYG_MA60"] = df["HYG_close"].rolling(60, min_periods=1).mean()
    df["VIX_MA60"] = df["VIX_close"].rolling(60, min_periods=1).mean()
    df[f"RSI_14_{asset}"] = calculate_rsi(df[close_col])

    if open_col not in df.columns:
        df[open_col] = df[close_col]

    return df


def move_towards(current_pos: float, target_pos: float, gap_pct: float) -> float:
    """按百分比填补缺口；降仓直接一步到位"""
    if target_pos > current_pos:
        return current_pos + (target_pos - current_pos) * gap_pct
    if target_pos < current_pos:
        return target_pos
    return current_pos


# ============================================================
# 4. V4.2.1 状态识别与执行层路径模拟 (核心重构)
# ============================================================

def calculate_states_and_positions(df: pd.DataFrame, asset: str, p: dict) -> pd.DataFrame:
    """
    V4.2.1 完整解耦逻辑：
    1. 向量化计算所有历史状态 (State Definition)
    2. 逐行计算路径依赖的实际执行仓位 (Execution Engine)
    """
    df = df.copy()
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

    vix_warning_zone = (vix >= p["vix_warning_low"]) & (vix < p["vix_warning_high"])
    vix_warning = vix_warning_zone & vix_above_ma60

    vix_tail_risk_zone = (vix >= p["vix_risk_low"]) & (vix < p["vix_risk_high"])
    vix_panic_zone = (vix >= p["vix_risk_high"]) & (vix < p.get("vix_extreme_th", 45))
    vix_extreme = vix >= p.get("vix_extreme_th", 45)
    vix_legacy_risk = (vix > p["vix_th"]) | (vix > df["VIX_MA60"] * 1.8)

    risk_off = (
            us10y_rising
            | hyg_divergence
            | (vix_tail_risk_zone & vix_above_ma60)
            | (credit_weak & (vix >= p["vix_risk_low"]))
            | vix_legacy_risk
    )

    risk_warning_signal = (credit_weak | vix_warning) & (~risk_off)

    panic_reversal = (
            (risk_off | vix_panic_zone | vix_extreme)
            & (daily_return <= -p["panic_drop_pct"])
            & rsi.between(p["panic_rsi_low"], p["panic_rsi_high"])
    )

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
            & (df["VIX_close"] < p["risk_repair_vix_th"])
    )

    states = np.select(
        [crash, panic_reversal, risk_off, risk_warning_signal, dip_buy, base_trend],
        ["Crash", "Panic-Reversal", "Risk-Off", "Risk-Warning", "Dip-Buy", "Risk-On"],
        default="Trend-Weak",
    )
    df["state"] = states
    df["risk_repair"] = risk_repair

    # 统计 Risk-Warning 连续天数
    streak = []
    count = 0
    for state in states:
        if state == "Risk-Warning":
            count += 1
        elif state in ["Risk-Off", "Panic-Reversal", "Crash"]:
            count = count  # 挂起
        else:
            count = 0
        streak.append(count)
    df["risk_warning_streak"] = streak

    # --- 执行层路径推演 ---
    current_pos = p["risk_pos"]
    execution_target_signal = []
    model_target_signal = []
    execution_notes = []

    for idx, row in df.iterrows():
        state = row["state"]

        # 1. 获取理论目标仓位
        if state == "Crash":
            model_target = p["crash_pos"]
        elif state == "Panic-Reversal":
            model_target = p["panic_reversal_pos"]
        elif state == "Risk-Off":
            model_target = p["risk_pos"]
        elif state == "Risk-Warning":
            model_target = p["risk_warning_pos"]
        elif state == "Dip-Buy":
            model_target = p["risk_on_pos"]
        elif state == "Risk-On":
            model_target = p["risk_on_pos"]
        else:
            model_target = p["risk_pos"]

        new_pos = current_pos
        note = ""

        # 2. V4.2.1 差距买回微操逻辑
        if state in ["Crash", "Risk-Off", "Trend-Weak"]:
            new_pos = min(current_pos, model_target)
            note = f"{state}: 仅允许防守降仓，禁止主动补仓"

        elif state == "Panic-Reversal":
            if model_target > current_pos:
                new_pos = move_towards(current_pos, model_target, p["panic_gap_buy_pct"])
                note = f"暴跌狙击：单次推入目标缺口 {p['panic_gap_buy_pct'] * 100}%"
            else:
                new_pos = model_target
                note = "暴跌狙击：受控试探"

        elif state == "Risk-Warning":
            if current_pos > model_target:
                new_pos = model_target
                note = "Risk-Warning: 降至预警缓冲仓"
            else:
                if bool(row["risk_repair"]):
                    new_pos = move_towards(current_pos, model_target, p["risk_repair_gap_buy_pct"])
                    note = f"风险初步解除 (VIX<{p['risk_repair_vix_th']}+HYG修复)：买回缺口 {p['risk_repair_gap_buy_pct'] * 100:.0f}%"
                elif row["risk_warning_streak"] >= p["risk_warning_confirm_days"]:
                    new_pos = move_towards(current_pos, model_target, p["risk_warning_gap_buy_pct"])
                    note = f"连续预警 {p['risk_warning_confirm_days']} 天确认：买回缺口 {p['risk_warning_gap_buy_pct'] * 100:.0f}%"
                else:
                    new_pos = current_pos
                    note = "Risk-Warning 观察期：不盲目补仓"

        elif state in ["Dip-Buy", "Risk-On"]:
            new_pos = move_towards(current_pos, model_target, p["risk_on_gap_buy_pct"])
            note = f"{state}: 警报解除，恢复趋势目标"

        new_pos = float(np.clip(new_pos, 0.0, 1.0))

        model_target_signal.append(model_target)
        execution_target_signal.append(new_pos)
        execution_notes.append(note)

        # 当日判定结果，作为下一日的起手仓位
        current_pos = new_pos

    df["model_target_position"] = model_target_signal
    df["execution_target_position"] = execution_target_signal
    df["execution_note"] = execution_notes

    # 画图用的历史实际仓位 (平移1天)
    df["position"] = pd.Series(execution_target_signal, index=df.index).shift(1).fillna(p["risk_pos"])

    return df


# ============================================================
# 5. 实盘执行建议生成
# ============================================================

def build_execution_amount_plan(asset: str, p: dict, current_price: float, target_position: float,
                                market_state: str) -> str:
    portfolio_value = p.get("portfolio_value")
    current_shares = p.get("current_shares")

    if portfolio_value is None or current_shares is None:
        return "💰【实盘金额估算】缺少配置，不计算股数。"

    current_value = current_shares * current_price
    target_value = portfolio_value * target_position
    diff_value = target_value - current_value
    trade_shares = int(abs(diff_value) / current_price)

    if abs(diff_value) < current_price:
        action, detail = "持有不动", "差额不足 1 股，暂不需要交易。"
    elif market_state in ["Risk-Off", "Crash", "Trend-Weak"] and diff_value > 0:
        action, detail = "持有不动", f"市场状态为 {market_state}，禁止主动补仓。"
    elif diff_value > 0:
        action, detail = f"买入 {trade_shares} 股", f"预计增加约 ${trade_shares * current_price:,.2f}。"
    else:
        action, detail = f"卖出 {trade_shares} 股", f"预计回收约 ${trade_shares * current_price:,.2f}。"

    return (
        "\n💰【实盘金额估算】\n"
        f"   • 账户池规模   : ${portfolio_value:,.2f}\n"
        f"   • 当前实际持仓 : {current_shares} 股，市值约 ${current_value:,.2f}\n"
        f"   • 最终执行仓位 : {target_position * 100:.0f}%，目标市值约 ${target_value:,.2f}\n"
        f"   • 🤖 明日动作  : {action}，{detail}\n"
    )


def build_execution_suggestion(df: pd.DataFrame, asset: str, p: dict, target_position: float, amount_plan: str) -> str:
    today = df.iloc[-1]
    market_state = today["state"]
    exec_note = today["execution_note"]

    recent = df.tail(60)
    support, resistance = recent[f"{asset}_close"].min(), recent[f"{asset}_close"].max()
    mid_price = (support + resistance) / 2

    return f"""
📍 当前箱体区间: {support:.2f} — {resistance:.2f}（中轴 {mid_price:.2f}）
💡 V4.2.1 调仓微操解读:
   • 状态判定: {market_state}
   • 动作引擎: {exec_note}
{amount_plan}
"""


# ============================================================
# 6. 图表与报告生成
# ============================================================

def plot_snapshot_with_levels(df: pd.DataFrame, asset: str, p: dict, target_position: float, market_state: str,
                              trade_date: str, file_date_str: str) -> Path:
    recent = df.tail(60)
    support, resistance = recent[f"{asset}_close"].min(), recent[f"{asset}_close"].max()
    mid_price = (support + resistance) / 2

    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(14, 18),
                                             gridspec_kw={"height_ratios": [2.3, 2.0, 1.0, 1.0]}, sharex=False)
    today = df.iloc[-1]

    # 1. 价格与均线
    ax1.plot(df.index, df[f"{asset}_close"], label=f"{asset} Price", color="#2c3e50", linewidth=1.5)
    ax1.plot(df.index, df[f"{asset}_MA"], label=f"MA{p['ma_len']}", color="#e67e22", linewidth=2)
    ax1.plot(df.index, df[f"{asset}_MA100"], label="MA100", color="#16a085", alpha=0.8)
    ax1.plot(df.index, df[f"{asset}_MA200"], label="MA200", color="#c0392b", alpha=0.8)

    dot_color = "#27ae60" if target_position >= 0.7 else "#c0392b"
    ax1.scatter(today.name, today[f"{asset}_close"], color=dot_color, s=160, zorder=5, edgecolors="white", linewidth=2)
    ax1.set_title(
        f"{asset} Daily Radar | {trade_date} | State: {market_state} | Exec Target: {target_position * 100:.0f}%",
        fontsize=16, fontweight="bold")
    ax1.legend(loc="upper left");
    ax1.grid(True, linestyle="--", alpha=0.4)

    # 2. 箱体
    recent_df = df.tail(120)
    ax2.plot(recent_df.index, recent_df[f"{asset}_close"], label=f"{asset} Close", color="#2980b9", linewidth=2)
    ax2.axvspan(recent.index[0], recent.index[-1], color="lightgray", alpha=0.2, label="Box Horizon 60d")
    ax2.axhline(resistance, color="red", linestyle="--", alpha=0.6, label=f"Resistance {resistance:.2f}")
    ax2.axhline(support, color="green", linestyle="--", alpha=0.6, label=f"Support {support:.2f}")

    ax2.text(0.02, 0.05, "V4.2.1: 严格根据 VIX 修复与跌幅限制碎步买回", transform=ax2.transAxes, color="red",
             fontsize=11, fontweight="bold", bbox=dict(facecolor="white", alpha=0.85, edgecolor="lightgray"))
    ax2.set_title(f"{asset} 近期箱体与潜在买点", fontsize=14);
    ax2.legend(loc="lower left", ncol=2);
    ax2.grid(True, alpha=0.3)

    # 3. RSI
    ax3.plot(df.index, df[f"RSI_14_{asset}"], label="RSI(14)", color="#8e44ad", linewidth=1.5)
    ax3.axhline(p["rsi_th"], color="red", linestyle="--", alpha=0.6)
    ax3.legend(loc="upper left");
    ax3.grid(True, alpha=0.3)

    # 4. 执行仓位序列
    ax4.plot(df.index, df["position"], label="Executed Target Position", color="#34495e", linewidth=1.8)
    ax4.plot(df.index, df["model_target_position"].shift(1), label="Model Theoretical Target", color="gray",
             linestyle=":", alpha=0.6)
    ax4.set_ylim(0, 1.05);
    ax4.set_title("V4.2.1 Path-Dependent Position")
    ax4.legend(loc="upper left");
    ax4.grid(True, alpha=0.3)

    pic_name = f"index_signal_v421_{file_date_str}_{asset.lower()}.png"
    save_path = FIGURES_PATH / pic_name
    plt.tight_layout();
    fig.savefig(save_path, dpi=160, bbox_inches="tight");
    plt.close(fig)
    return save_path


def generate_daily_report():
    print("=" * 80)
    print("🚀 VOO / QQQ 量化交易执行层 V4.2.1 启动 (Path-Dependent Model)")
    print("=" * 80)

    master_df = load_master_data()

    for asset, p in ASSET_CONFIG.items():
        print(f"\n⏳ 正在推演 {asset} 路径依赖模型...")

        # 1. 计算指标
        asset_df = filter_real_trading_days(master_df, asset)
        df = process_asset_indicators(asset_df, asset, p)

        # 2. 状态判定与路径执行推演
        df = calculate_states_and_positions(df, asset, p)

        today = df.iloc[-1]
        trade_date = today.name.strftime("%Y-%m-%d")
        file_date_str = today.name.strftime("%Y_%m_%d")

        market_state = today["state"]
        model_target = today["model_target_position"]
        exec_target = today["execution_target_position"]
        current_price = today[f"{asset}_close"]

        amount_plan = build_execution_amount_plan(asset, p, current_price, exec_target, market_state)
        exec_suggestion = build_execution_suggestion(df, asset, p, exec_target, amount_plan)

        trend_dist = (current_price / today[f"{asset}_MA"] - 1) * 100

        report_content = (
            f"{'=' * 60}\n"
            f"🛡️ {asset} 极客量化防守执行层 V4.2.1 | 结算日: {trade_date}\n"
            f"{'=' * 60}\n\n"
            f"🎯【明日实盘交易指令】\n"
            f"   理论模型状态 : {market_state} (目标 {model_target * 100:.0f}%)\n"
            f"   实际执行仓位 : {exec_target * 100:.0f}%\n"
            f"{exec_suggestion}\n"
            f"🔍【关键指标快照】\n"
            f"   • {asset} 价格  : {current_price:.2f} (MA{p['ma_len']}: {today[f'{asset}_MA']:.2f})\n"
            f"   • MA100         : {today[f'{asset}_MA100']:.2f}\n"
            f"   • 均线偏离度    : {trend_dist:+.2f}%\n"
            f"   • RSI (14)      : {today[f'RSI_14_{asset}']:.2f}\n"
            f"   • 单日涨跌幅    : {today[f'{asset}_daily_return'] * 100:+.2f}%\n"
            f"   • VIX 恐慌指数  : {today['VIX_close']:.2f} (MA60: {today['VIX_MA60']:.2f})\n"
            f"   • 预警连续天数  : {today['risk_warning_streak']} 天\n"
            f"   • 修复条件满足  : {'是' if today['risk_repair'] else '否'}\n"
            f"{'=' * 60}\n"
        )
        print(report_content)

        txt_path = OUTPUT_PATH / f"index_signal_v421_{file_date_str}_{asset.lower()}.txt"
        txt_path.write_text(report_content, encoding="utf-8")

        chart_path = plot_snapshot_with_levels(df, asset, p, exec_target, market_state, trade_date, file_date_str)
        print(f"🖼️ 图表已保存: {chart_path.name}")

    print("\n" + "=" * 80)
    print("🎉 VOO / QQQ V4.2.1 每日指令生成完成")
    print(f"📂 输出目录: {OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    generate_daily_report()