import pandas as pd
import backtrader as bt
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from datetime import datetime
from enum import Enum, auto
from typing import Optional
import warnings

warnings.filterwarnings("ignore")


# ==================== 1. 环境配置 ====================
class EnvType(Enum):
    HOME = auto()
    WORK = auto()


def get_env_config(env: EnvType) -> dict:
    if env == EnvType.HOME:
        base_path = Path("/Users/evaseemefly/03data/05-spiders")
    elif env == EnvType.WORK:
        base_path = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
    else:
        raise ValueError(f"未知的环境类型: {env}")
    return {"ind_stock_dir": base_path / "individual_stocks"}


CURRENT_ENV = EnvType.WORK
CONFIG = get_env_config(CURRENT_ENV)


# ==================== 2. 通用唐奇安趋势策略 ====================
class DonchianTrendStrategy(bt.Strategy):
    params = (
        ("entry_period", 30),
        ("exit_period", 20),
        ("alloc_pct", 0.95),
        ("verbose", True),
    )

    def __init__(self):
        self.highest_high = bt.indicators.Highest(self.data.high(-1), period=self.params.entry_period)
        self.lowest_low = bt.indicators.Lowest(self.data.low(-1), period=self.params.exit_period)
        self.order = None
        self.entry_price = 0.0
        self.stage = 0
        self.trades_history = []
        self.account_stats = {}
        self.closed_pnl = []

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                if self.params.verbose:
                    print(
                        f"   ↳ ⚡ [历史回测成交] 🟢 买入突破 | 均价: ${order.executed.price:.2f} | 数量: {order.executed.size} 股")
                self.stage = 1
                self.entry_price = order.executed.price
            elif order.issell():
                if self.params.verbose:
                    print(
                        f"   ↳ ⚡ [历史回测成交] 🔴 破位卖出 | 均价: ${order.executed.price:.2f} | 数量: {abs(order.executed.size)} 股")
                self.stage = 0
                self.entry_price = 0.0
            self.order = None
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def notify_trade(self, trade):
        if trade.isclosed:
            dt = self.data.datetime.date(0)
            self.closed_pnl.append((dt, trade.pnlcomm))

    def next(self):
        dt = self.data.datetime.date(0)
        self.account_stats[dt] = (self.broker.get_value(), self.broker.get_cash(), self.position.size)

        if len(self) < max(self.params.entry_period, self.params.exit_period) or self.order:
            return

        price = self.data.close[0]
        if self.stage > 0:
            if price < self.lowest_low[0]:
                size = self.position.size
                self.trades_history.append(("SELL", dt, price, 0, size))
                self.order = self.close()
                return
        else:
            if price > self.highest_high[0]:
                size = int((self.broker.get_value() * self.params.alloc_pct) / price)
                if size > 0:
                    self.order = self.buy(size=size)
                    self.trades_history.append(("BUY", dt, price, 1, size))


# ==================== 3. 辅助函数 ====================
def load_price_data(ticker: str) -> pd.DataFrame:
    file_path = CONFIG["ind_stock_dir"] / f"individual_stocks_master_{ticker}.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"找不到文件: {file_path}")
    df = pd.read_csv(file_path)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()
    df = df.rename(columns={f"{ticker}_open": "open", f"{ticker}_high": "high", f"{ticker}_low": "low",
                            f"{ticker}_close": "close"})
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for n in [10, 20, 50, 100, 200, 250]:
        df[f"MA{n}"] = df["close"].rolling(n).mean()
    delta = df["close"].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=13, adjust=False).mean()
    ema_down = down.ewm(com=13, adjust=False).mean()
    df["RSI"] = 100 - (100 / (1 + (ema_up / ema_down)))
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["MACD_DIF"] = ema12 - ema26
    df["MACD_DEA"] = df["MACD_DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = (df["MACD_DIF"] - df["MACD_DEA"]) * 2
    std20 = df["close"].rolling(20).std()
    df["BB_Upper"] = df["MA20"] + 2 * std20
    df["BB_Lower"] = df["MA20"] - 2 * std20
    return df


def normalize_live_state(live_state: Optional[dict]) -> dict:
    if live_state is None:
        return {"stage": None, "cost_price": None, "shares": None, "cash": None}
    return {"stage": live_state.get("stage", 0), "cost_price": live_state.get("cost_price", 0.0),
            "shares": live_state.get("shares", 0), "cash": live_state.get("cash", None)}


def estimate_first_tranche_shares(current_price: float, live_cash: Optional[float], first_tranche_pct: float) -> \
Optional[int]:
    if live_cash is None or live_cash <= 0:
        return None
    return int((live_cash * first_tranche_pct) / current_price)


# ==================== 4. 报表与指令生成核心 ====================
def generate_strategy_report(ticker: str, config: dict):
    print(f"🚀 开始生成 {ticker} 动量趋势追踪策略专业报告图 (通用模板)...")
    df = load_price_data(ticker)
    df = add_indicators(df)

    data = bt.feeds.PandasData(dataname=df)
    cerebro = bt.Cerebro(optreturn=False)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)

    cerebro.addstrategy(DonchianTrendStrategy,
                        entry_period=config.get("entry_period", 30),
                        exit_period=config.get("exit_period", 20),
                        alloc_pct=config.get("alloc_pct", 0.95),
                        verbose=config.get("verbose", True))

    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")

    results = cerebro.run()
    strat = results[0][0] if isinstance(results[0], list) else results[0]

    total_return = (strat.broker.get_value() - 100000.0) / 100000.0 * 100
    max_dd = strat.analyzers.drawdown.get_analysis().get("max", {}).get("drawdown", 0.0)

    # ==================== 彻底回滚的 8 轴绘图逻辑 ====================
    plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "PingFang SC", "SimHei", "Microsoft YaHei"]
    plt.rcParams["axes.unicode_minus"] = False
    fig, axs = plt.subplots(8, 1, figsize=(16, 24), sharex=True,
                            gridspec_kw={"height_ratios": [1.1, 1.5, 1.2, 1, 1, 4.5, 1.2, 1.2]})
    fig.subplots_adjust(hspace=0.12)
    fig.suptitle(
        f"{ticker} Trend Strategy Backtest Report V2\nTotal Return: {total_return:.2f}%   Max Drawdown: {max_dd:.2f}%\n注意：图中 Broker / Cash / Position 均为历史回测账户，不代表真实账户",
        fontsize=18, fontweight="bold", y=0.94)

    plot_df = df[-200:].copy()
    dates = plot_df.index

    vals = [strat.account_stats.get(d.date(), (np.nan, np.nan, np.nan))[0] for d in dates]
    cash = [strat.account_stats.get(d.date(), (np.nan, np.nan, np.nan))[1] for d in dates]
    s_vals, s_cash = pd.Series(vals, index=dates).ffill().bfill(), pd.Series(cash, index=dates).ffill().bfill()

    # 面板 0: 参数表
    axs[0].axis("off")
    p = strat.params
    col_labels = ["模块", "回测参数 / 状态", "实盘解释", "V2 风控修正"]
    table_data = [
        ["入场动量引擎", f"突破过去 {p.entry_period} 日最高价", "仅当实盘空仓时才考虑买入",
         "首笔不再默认 95%，改为分层建仓"],
        ["退出风控矩阵", f"跌破过去 {p.exit_period} 日最低价", "实盘持仓时作为移动止损线", "触发则执行清仓纪律"],
        ["回测账户", f"回测目标暴露 {p.alloc_pct * 100:.0f}%", "图中 Cash/Position 是回测结果", "不可直接等同真实账户"],
        ["实盘账户", f"由 config 自动注入", "以 live_state 为准", "明日指令只基于实盘状态生成"]
    ]
    table = axs[0].table(cellText=table_data, colLabels=col_labels, loc="center", cellLoc="center")
    table.set_fontsize(10)
    table.scale(1, 1.7)
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#bdc3c7")
        if row == 0:
            cell.set_text_props(weight="bold", color="white")
            cell.set_facecolor("#2c3e50")
        elif col == 0:
            cell.set_text_props(weight="bold", color="#2c3e50")
            cell.set_facecolor("#ecf0f1")

    # 面板 1: 净值对比
    axs[1].plot(dates, plot_df["close"] / plot_df["close"].iloc[0], label=f"Baseline {ticker}", color="gray", alpha=0.6,
                linewidth=1.5)
    axs[1].plot(dates, s_vals / s_vals.iloc[0], label=f"Quant Strategy", color="#e74c3c", linewidth=2.5)
    axs[1].axhline(1.0, color="black", linestyle="--", linewidth=0.8, alpha=0.5)
    axs[1].set_ylabel("Net Value", fontsize=10)
    axs[1].legend(loc="upper left")
    axs[1].grid(True, linestyle=":", alpha=0.6)

    # 面板 2: 资金
    axs[2].plot(dates, s_vals, label="Total Value", color="blue", linewidth=1.5)
    axs[2].plot(dates, s_cash, label="Cash", color="red", linewidth=1.5)
    axs[2].set_ylabel("Broker ($)", fontsize=10)
    axs[2].legend(loc="upper left")
    axs[2].grid(True, linestyle=":", alpha=0.6)

    # 面板 3: PnL
    axs[3].axhline(0, color="black", linewidth=0.8)
    has_pnl = False
    for dt, pnl in strat.closed_pnl:
        if pd.Timestamp(dt) in dates:
            has_pnl = True
            color = "red" if pnl > 0 else "green"
            axs[3].scatter(dt, pnl, color=color, s=120, edgecolors="black", linewidth=0.5, zorder=5)
    if not has_pnl: axs[3].set_ylim(-1000, 1000)
    axs[3].set_ylabel("Net PnL ($)", fontsize=10)
    axs[3].grid(True, linestyle=":", alpha=0.6)

    # 面板 4: 仓位比例
    s_pos_ratio = (s_vals - s_cash) / s_vals * 100
    axs[4].bar(dates, s_pos_ratio, color="#3498db", alpha=0.7, width=1.0)
    axs[4].set_ylabel("Position (%)", fontsize=10)
    axs[4].set_ylim(0, 105)
    axs[4].set_yticks([0, 25, 50, 75, 100])
    axs[4].grid(True, linestyle=":", alpha=0.6)

    # 面板 5: 主图 + 交易记录 + 数量
    axs[5].plot(dates, plot_df["close"], label="Price", color="#1f77b4", linewidth=2.5, zorder=4)
    axs[5].plot(dates, plot_df['MA10'], label='MA10', color='#95a5a6', linewidth=1, alpha=0.7)
    axs[5].plot(dates, plot_df['MA20'], label='MA20', color='#8e44ad', linewidth=1.2, linestyle='--')
    axs[5].plot(dates, plot_df['MA50'], label='MA50', color='#f39c12', linewidth=1.5, alpha=0.8)
    axs[5].plot(dates, plot_df['MA200'], label='MA200', color='#c0392b', linewidth=2)
    axs[5].fill_between(dates, plot_df['BB_Lower'], plot_df['BB_Upper'], color='gray', alpha=0.15,
                        label='Bollinger Bands')

    plot_df["Donchian_Upper"] = df["high"].shift(1).rolling(p.entry_period).max().reindex(plot_df.index)
    plot_df["Donchian_Lower"] = df["low"].shift(1).rolling(p.exit_period).min().reindex(plot_df.index)
    axs[5].plot(dates, plot_df["Donchian_Upper"], label=f"Donchian Upper {p.entry_period}D", color="#d35400",
                linewidth=1.4, linestyle=":")
    axs[5].plot(dates, plot_df["Donchian_Lower"], label=f"Donchian Lower {p.exit_period}D", color="#16a085",
                linewidth=1.4, linestyle=":")

    for action, date, p_trade, stage, size in strat.trades_history:
        if pd.Timestamp(date) in dates:
            color = "red" if action == "BUY" else "green"
            offset = 12 if action == "BUY" else -22
            axs[5].scatter(date, p_trade, color=color, marker="o", s=120, zorder=8, edgecolors="white", linewidth=1.5)
            # 补回了带数量的买卖标记！
            axs[5].annotate(f"BT {'+' if action == 'BUY' else '-'}{size}", xy=(date, p_trade), xytext=(0, offset),
                            textcoords="offset points", ha="center", color=color, fontsize=9, fontweight="bold",
                            zorder=10, bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7))
    axs[5].set_ylabel("Price", fontsize=10)
    axs[5].legend(loc="upper left", ncol=3, fontsize=8)
    axs[5].grid(True, linestyle=":", alpha=0.6)

    # 面板 6: MACD
    axs[6].plot(dates, plot_df["MACD_DIF"], color="#1f77b4", label="DIF", linewidth=1)
    axs[6].plot(dates, plot_df["MACD_DEA"], color="#ff7f0e", label="DEA", linewidth=1)
    colors = ["#d62728" if m > 0 else "#2ca02c" for m in plot_df["MACD_Hist"]]
    axs[6].bar(dates, plot_df["MACD_Hist"], color=colors, alpha=0.6, width=0.8, label="MACD Hist")
    axs[6].axhline(0, color="gray", linestyle="--", linewidth=0.8)
    axs[6].set_ylabel("MACD", fontsize=10)
    axs[6].legend(loc="upper left", ncol=3, fontsize=9)
    axs[6].grid(True, linestyle=":", alpha=0.6)

    # 面板 7: RSI
    axs[7].plot(dates, plot_df["RSI"], label="RSI(14)", color="#8e44ad", linewidth=1.5)
    axs[7].axhline(70, color="red", linestyle="--", alpha=0.5)
    axs[7].axhline(30, color="green", linestyle="--", alpha=0.5)
    axs[7].fill_between(dates, 30, 70, color="#8e44ad", alpha=0.05)
    axs[7].set_ylim(0, 100)
    axs[7].set_ylabel("RSI(14)", fontsize=10)
    axs[7].legend(loc="upper left", fontsize=9)
    axs[7].grid(True, linestyle=":", alpha=0.6)

    pdf_path = CONFIG["ind_stock_dir"] / f"{ticker}_trend_strategy_report.pdf"
    fig.savefig(pdf_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"✅ {ticker} 专属 8轴趋势研报图 V2 成功生成！")

    # -------------- 打印实盘指令 --------------
    print_next_day_signals_v2(strat, df, config, ticker)


def print_next_day_signals_v2(strat, df, config, ticker):
    current_date = df.index[-1].strftime("%Y-%m-%d")
    current_price = float(df["close"].iloc[-1])
    entry_p = config.get("entry_period", 30)
    exit_p = config.get("exit_period", 20)
    first_tranche_pct = config.get("live_first_tranche_pct", 0.35)
    live = normalize_live_state(config.get("live_state"))
    donchian_upper = float(df["high"].shift(1).rolling(entry_p).max().iloc[-1])
    donchian_lower = float(df["low"].shift(1).rolling(exit_p).min().iloc[-1])

    print("\n" + "🔮" * 30 + f"\n🎯 【明日实盘交易指令】 {ticker} (趋势模块) | 基准日: {current_date}\n" + "🔮" * 30)
    print(f"📊 收盘价: ${current_price:.2f} | 📈 突破线: ${donchian_upper:.2f} | 📉 止损线: ${donchian_lower:.2f}")

    if live["stage"] is not None and live["stage"] > 0:
        print("   🔴 【趋势持仓防守监控】")
        print(f"      - 纪律：价格若跌破 < ${donchian_lower:.2f}，执行 100% 清仓。让利润奔跑！")
    elif live["stage"] is not None and live["stage"] == 0:
        print("   🟢 【右侧动量突破监控】")
        if current_price >= donchian_upper:
            print(f"      - ✅ 买入信号触发：已站上 {entry_p} 日突破线。")
            est_shares = estimate_first_tranche_shares(current_price, live["cash"], first_tranche_pct)
            print(f"      - 建议动作: 动用首笔资金 {first_tranche_pct * 100:.0f}%" + (
                f" (预估买入 {est_shares} 股)" if est_shares else ""))
        else:
            print(f"      - ⚪ 空仓观望。需有效上破 > ${donchian_upper:.2f} 方可入场。")
    print("🔮" * 30 + "\n")