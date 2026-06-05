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
    df = df.rename(
        columns={f"{ticker}_open": "open", f"{ticker}_high": "high", f"{ticker}_low": "low", f"{ticker}_close": "close",
                 f"{ticker}_volume": "volume"})
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

    cerebro.addstrategy(DonchianTrendStrategy, entry_period=config.get("entry_period", 30),
                        exit_period=config.get("exit_period", 20), alloc_pct=config.get("alloc_pct", 0.95),
                        verbose=config.get("verbose", True))
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")

    results = cerebro.run()
    strat = results[0][0] if isinstance(results[0], list) else results[0]

    total_return = (strat.broker.get_value() - 100000.0) / 100000.0 * 100
    max_dd = strat.analyzers.drawdown.get_analysis().get("max", {}).get("drawdown", 0.0)

    # -------------- 绘图逻辑 (缩略，与原版完全一致) --------------
    plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "PingFang SC", "SimHei", "Microsoft YaHei"]
    plt.rcParams["axes.unicode_minus"] = False
    fig, axs = plt.subplots(8, 1, figsize=(16, 24), sharex=True,
                            gridspec_kw={"height_ratios": [1.1, 1.5, 1.2, 1, 1, 4.5, 1.2, 1.2]})
    fig.subplots_adjust(hspace=0.12)
    fig.suptitle(
        f"{ticker} Trend Strategy Backtest Report\nTotal Return: {total_return:.2f}%   Max Drawdown: {max_dd:.2f}%\n注意：图中 Broker / Cash / Position 均为历史回测账户，不代表真实账户",
        fontsize=18, fontweight="bold", y=0.94)

    plot_df = df[-200:].copy()
    dates = plot_df.index

    vals = [strat.account_stats.get(d.date(), (np.nan, np.nan, np.nan))[0] for d in dates]
    cash = [strat.account_stats.get(d.date(), (np.nan, np.nan, np.nan))[1] for d in dates]
    s_vals, s_cash = pd.Series(vals, index=dates).ffill().bfill(), pd.Series(cash, index=dates).ffill().bfill()

    axs[0].axis("off")
    table = axs[0].table(cellText=[
        ["配置", f"突破 {strat.params.entry_period} 日最高价", f"跌破 {strat.params.exit_period} 日最低价",
         "分层建仓"]], colLabels=["模块", "入场参数", "退场参数", "说明"], loc="center", cellLoc="center")
    table.scale(1, 1.7)

    axs[1].plot(dates, plot_df["close"] / plot_df["close"].iloc[0], label=f"Baseline {ticker}", color="gray")
    axs[1].plot(dates, s_vals / s_vals.iloc[0], label=f"Quant Strategy", color="#e74c3c")
    axs[1].legend(loc="upper left")

    axs[2].plot(dates, s_vals, label="Total Value", color="blue")
    axs[2].plot(dates, s_cash, label="Cash", color="red")
    axs[2].legend(loc="upper left")

    axs[5].plot(dates, plot_df["close"], label="Price", color="#1f77b4", linewidth=2.5)
    plot_df["Donchian_Upper"] = df["high"].shift(1).rolling(strat.params.entry_period).max().reindex(plot_df.index)
    plot_df["Donchian_Lower"] = df["low"].shift(1).rolling(strat.params.exit_period).min().reindex(plot_df.index)
    axs[5].plot(dates, plot_df["Donchian_Upper"], label="Donchian Upper", color="#d35400", linestyle=":")
    axs[5].plot(dates, plot_df["Donchian_Lower"], label="Donchian Lower", color="#16a085", linestyle=":")
    axs[5].legend(loc="upper left")

    pdf_path = CONFIG["ind_stock_dir"] / f"{ticker}_trend_strategy_report.pdf"
    fig.savefig(pdf_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"✅ {ticker} 趋势图表已生成: {pdf_path}")

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