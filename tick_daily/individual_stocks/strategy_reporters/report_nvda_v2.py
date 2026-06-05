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

# ============================================================
# report_nvda_v2.py
# NVDA 唐奇安通道趋势策略报告 V2
#
# V2 核心修正：
# 1) 明确区分 Backtest Account 与 Live Account。
# 2) 图表中的 Cash / Position / Trades 全部标注为“回测账户状态”。
# 3) 明日实盘指令只基于 live_state 生成，不再让回测仓位误导实盘判断。
# 4) 将“无脑 95% 追入”改为“分层建仓建议”。
# 5) 当回测状态与实盘状态不一致时，输出显式提醒。
# ============================================================


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


# ==================== 2. 唐奇安通道趋势策略 ====================
class DonchianTrendStrategy(bt.Strategy):
    params = (
        ("entry_period", 30),
        ("exit_period", 20),
        ("alloc_pct", 0.95),
        ("verbose", True),
    )

    def __init__(self):
        # 使用昨日之前的数据计算通道，避免未来函数
        self.highest_high = bt.indicators.Highest(
            self.data.high(-1), period=self.params.entry_period
        )
        self.lowest_low = bt.indicators.Lowest(
            self.data.low(-1), period=self.params.exit_period
        )

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
                        f"   ↳ ⚡ [历史回测成交] 🟢 买入突破 | "
                        f"均价: ${order.executed.price:.2f} | 数量: {order.executed.size} 股"
                    )
                self.stage = 1
                self.entry_price = order.executed.price

            elif order.issell():
                if self.params.verbose:
                    print(
                        f"   ↳ ⚡ [历史回测成交] 🔴 破位卖出 | "
                        f"均价: ${order.executed.price:.2f} | 数量: {abs(order.executed.size)} 股"
                    )
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

        # 记录回测账户状态：注意，这不是实盘账户
        self.account_stats[dt] = (
            self.broker.get_value(),
            self.broker.get_cash(),
            self.position.size,
        )

        if len(self) < max(self.params.entry_period, self.params.exit_period) or self.order:
            return

        price = self.data.close[0]

        if self.stage > 0:
            # 持仓时：跌破过去 exit_period 日低点，清仓
            if price < self.lowest_low[0]:
                size = self.position.size
                self.trades_history.append(("SELL", dt, price, 0, size))
                self.order = self.close()
                return
        else:
            # 空仓时：突破过去 entry_period 日高点，买入
            if price > self.highest_high[0]:
                size = int((self.broker.get_value() * self.params.alloc_pct) / price)
                if size > 0:
                    self.order = self.buy(size=size)
                    self.trades_history.append(("BUY", dt, price, 1, size))


# ==================== 3. 数据准备与技术指标 ====================
def load_price_data(ticker: str) -> pd.DataFrame:
    file_path = CONFIG["ind_stock_dir"] / f"individual_stocks_master_{ticker}.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"找不到文件: {file_path}")

    df = pd.read_csv(file_path)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()

    df = df.rename(
        columns={
            f"{ticker}_open": "open",
            f"{ticker}_high": "high",
            f"{ticker}_low": "low",
            f"{ticker}_close": "close",
            f"{ticker}_volume": "volume",
        }
    )

    required_cols = ["open", "high", "low", "close", "volume"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{ticker} 数据缺少必要字段: {missing}")

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
    rs = ema_up / ema_down
    df["RSI"] = 100 - (100 / (1 + rs))

    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["MACD_DIF"] = ema12 - ema26
    df["MACD_DEA"] = df["MACD_DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = (df["MACD_DIF"] - df["MACD_DEA"]) * 2

    std20 = df["close"].rolling(20).std()
    df["BB_Upper"] = df["MA20"] + 2 * std20
    df["BB_Lower"] = df["MA20"] - 2 * std20

    return df


# ==================== 4. 实盘状态辅助函数 ====================
def normalize_live_state(live_state: Optional[dict]) -> dict:
    if live_state is None:
        return {
            "stage": None,
            "cost_price": None,
            "shares": None,
            "cash": None,
            "target_position_pct": None,
        }

    return {
        "stage": live_state.get("stage", 0),
        "cost_price": live_state.get("cost_price", 0.0),
        "shares": live_state.get("shares", 0),
        "cash": live_state.get("cash", None),
        "target_position_pct": live_state.get("target_position_pct", None),
    }

def get_live_stage_text(live: dict) -> str:
    if live["stage"] is None:
        return "未注入实盘状态"
    if live["stage"] > 0 or (live["shares"] is not None and live["shares"] > 0):
        return f"实盘持仓中 | 成本 ${live['cost_price']:.2f} | 股数 {live['shares']}"
    return "实盘空仓观望"

def estimate_first_tranche_shares(
    current_price: float,
    live_cash: Optional[float],
    first_tranche_pct: float,
) -> Optional[int]:
    if live_cash is None or live_cash <= 0:
        return None
    return int((live_cash * first_tranche_pct) / current_price)


# ==================== 5. 回测执行 ====================
def run_backtest(df: pd.DataFrame, config: dict):
    data = bt.feeds.PandasData(dataname=df)

    cerebro = bt.Cerebro(optreturn=False)
    cerebro.adddata(data)
    cerebro.broker.setcash(config.get("initial_cash", 100000.0))
    cerebro.broker.setcommission(commission=config.get("commission", 0.001))

    cerebro.addstrategy(
        DonchianTrendStrategy,
        entry_period=config.get("entry_period", 30),
        exit_period=config.get("exit_period", 20),
        alloc_pct=config.get("alloc_pct", 0.95),
        verbose=config.get("verbose", True),
    )

    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")

    results = cerebro.run()
    strat = results[0][0] if isinstance(results[0], list) else results[0]

    final_value = strat.broker.get_value()
    initial_cash = config.get("initial_cash", 100000.0)
    total_return = (final_value - initial_cash) / initial_cash * 100
    max_dd = strat.analyzers.drawdown.get_analysis().get("max", {}).get("drawdown", 0.0)

    return strat, final_value, total_return, max_dd


# ==================== 6. 生成报告图 ====================
def generate_report_figure(
    ticker: str,
    df: pd.DataFrame,
    strat: DonchianTrendStrategy,
    config: dict,
    total_return: float,
    max_dd: float,
):
    plt.rcParams["font.sans-serif"] = [
        "Arial Unicode MS",
        "PingFang SC",
        "SimHei",
        "Microsoft YaHei",
    ]
    plt.rcParams["axes.unicode_minus"] = False

    live = normalize_live_state(config.get("live_state"))

    fig, axs = plt.subplots(
        8,
        1,
        figsize=(16, 24),
        sharex=True,
        gridspec_kw={
            "height_ratios": [1.1, 1.5, 1.2, 1, 1, 4.5, 1.2, 1.2]
        },
    )
    fig.subplots_adjust(hspace=0.12)

    fig.suptitle(
        f"{ticker} Quant Strategy Backtest Report V2\n"
        f"Backtest Total Return: {total_return:.2f}%   Backtest Max Drawdown: {max_dd:.2f}%\n"
        f"注意：图中 Broker / Cash / Position 均为历史回测账户，不代表真实账户",
        fontsize=18,
        fontweight="bold",
        y=0.94,
    )

    plot_df = df[-200:].copy()
    dates = plot_df.index

    vals = [strat.account_stats.get(d.date(), (np.nan, np.nan, np.nan))[0] for d in dates]
    cash = [strat.account_stats.get(d.date(), (np.nan, np.nan, np.nan))[1] for d in dates]
    pos_size = [strat.account_stats.get(d.date(), (np.nan, np.nan, np.nan))[2] for d in dates]

    s_vals = pd.Series(vals, index=dates).ffill().bfill()
    s_cash = pd.Series(cash, index=dates).ffill().bfill()
    s_pos_size = pd.Series(pos_size, index=dates).ffill().bfill()

    # 面板 0：参数 + 实盘状态表
    ax_table = axs[0]
    ax_table.axis("off")

    p = strat.params
    live_text = get_live_stage_text(live)

    col_labels = ["模块", "回测参数 / 状态", "实盘解释", "V2 风控修正"]
    table_data = [
        [
            "入场动量引擎",
            f"突破过去 {p.entry_period} 日最高价",
            "仅当实盘空仓时才考虑买入",
            "首笔不再默认 95%，改为分层建仓",
        ],
        [
            "退出风控矩阵",
            f"跌破过去 {p.exit_period} 日最低价",
            "实盘持仓时作为移动止损线",
            "触发则执行清仓纪律",
        ],
        [
            "回测账户",
            f"回测目标暴露 {p.alloc_pct * 100:.0f}%",
            "图中 Cash/Position 是回测结果",
            "不可直接等同真实账户",
        ],
        [
            "实盘账户",
            live_text,
            "由 config['live_state'] 注入",
            "明日指令只基于实盘状态生成",
        ],
    ]

    table = ax_table.table(
        cellText=table_data,
        colLabels=col_labels,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.7)

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#bdc3c7")
        if row == 0:
            cell.set_text_props(weight="bold", color="white", fontsize=11)
            cell.set_facecolor("#2c3e50")
        elif col == 0:
            cell.set_text_props(weight="bold", color="#2c3e50")
            cell.set_facecolor("#ecf0f1")

    # 面板 1：回测净值 vs 买入持有
    ax_netval = axs[1]
    strat_net_value = s_vals / s_vals.iloc[0]
    baseline_net_value = plot_df["close"] / plot_df["close"].iloc[0]
    ax_netval.plot(
        dates,
        baseline_net_value,
        label=f"Baseline / Buy & Hold {ticker}",
        color="gray",
        alpha=0.6,
        linewidth=1.5,
    )
    ax_netval.plot(
        dates,
        strat_net_value,
        label=f"Backtest Quant Strategy {ticker}",
        color="#e74c3c",
        linewidth=2.5,
    )
    ax_netval.axhline(1.0, color="black", linestyle="--", linewidth=0.8, alpha=0.5)
    ax_netval.set_ylabel("Backtest Net Value", fontsize=10)
    ax_netval.set_title("近 200 日回测策略净值 vs 股价基准，起始 = 1.0", loc="left", fontsize=10)
    ax_netval.legend(loc="upper left", fontsize=9)
    ax_netval.grid(True, linestyle=":", alpha=0.6)

    # 面板 2：回测账户资金
    ax_broker = axs[2]
    ax_broker.plot(dates, s_vals, label="Backtest Total Value", color="blue", linewidth=1.5)
    ax_broker.plot(dates, s_cash, label="Backtest Cash", color="red", linewidth=1.5)
    ax_broker.set_ylabel("Backtest Broker ($)", fontsize=10)
    ax_broker.set_title("Backtest Broker：仅代表回测账户，不代表实盘现金", loc="left", fontsize=10)
    ax_broker.legend(loc="upper left", fontsize=9)
    ax_broker.grid(True, linestyle=":", alpha=0.6)

    # 面板 3：已平仓收益
    ax_pnl = axs[3]
    ax_pnl.axhline(0, color="black", linewidth=0.8)
    has_pnl = False
    for dt, pnl in strat.closed_pnl:
        if pd.Timestamp(dt) in dates:
            has_pnl = True
            color = "red" if pnl > 0 else "green"
            ax_pnl.scatter(
                dt,
                pnl,
                color=color,
                s=120,
                edgecolors="black",
                linewidth=0.5,
                zorder=5,
            )
    if not has_pnl:
        ax_pnl.set_ylim(-1000, 1000)
    ax_pnl.set_ylabel("Backtest PnL ($)", fontsize=10)
    ax_pnl.set_title("Backtest Closed Trades PnL", loc="left", fontsize=10)
    ax_pnl.grid(True, linestyle=":", alpha=0.6)

    # 面板 4：回测仓位比例
    ax_pos = axs[4]
    s_pos_ratio = (s_vals - s_cash) / s_vals * 100
    ax_pos.bar(dates, s_pos_ratio, color="#3498db", alpha=0.7, width=1.0)
    ax_pos.set_ylabel("Backtest Position (%)", fontsize=10)
    ax_pos.set_ylim(0, 105)
    ax_pos.set_yticks([0, 25, 50, 75, 100])
    ax_pos.grid(True, linestyle=":", alpha=0.6)
    ax_pos.set_title("Backtest Position Ratio：历史回测仓位，不是当前实盘仓位", loc="left", fontsize=10)

    # 面板 5：价格主图
    ax_price = axs[5]
    price = plot_df["close"]
    ax_price.plot(dates, price, label="Price", color="#1f77b4", linewidth=2.5, zorder=4)

    ma_styles = {
        "MA10": ("#95a5a6", 1, "-"),
        "MA20": ("#8e44ad", 1.2, "--"),
        "MA50": ("#f39c12", 1.5, "-"),
        "MA100": ("#27ae60", 1.5, "-"),
        "MA200": ("#c0392b", 2, "-"),
        "MA250": ("#2c3e50", 2, "-."),
    }

    for ma, (color, lw, ls) in ma_styles.items():
        ax_price.plot(dates, plot_df[ma], label=ma, color=color, linewidth=lw, linestyle=ls, alpha=0.9)

    ax_price.fill_between(
        dates,
        plot_df["BB_Lower"],
        plot_df["BB_Upper"],
        color="gray",
        alpha=0.15,
        label="Bollinger Bands",
    )

    # 画出当前唐奇安入场/退出线
    entry_p = config.get("entry_period", 30)
    exit_p = config.get("exit_period", 20)
    plot_df["Donchian_Upper"] = df["high"].shift(1).rolling(entry_p).max().reindex(plot_df.index)
    plot_df["Donchian_Lower"] = df["low"].shift(1).rolling(exit_p).min().reindex(plot_df.index)

    ax_price.plot(
        dates,
        plot_df["Donchian_Upper"],
        label=f"Donchian Upper {entry_p}D",
        color="#d35400",
        linewidth=1.4,
        linestyle=":",
    )
    ax_price.plot(
        dates,
        plot_df["Donchian_Lower"],
        label=f"Donchian Lower {exit_p}D",
        color="#16a085",
        linewidth=1.4,
        linestyle=":",
    )

    ax_price.set_ylabel("Price", fontsize=10)
    ax_price.legend(loc="upper left", ncol=3, fontsize=8)
    ax_price.grid(True, linestyle=":", alpha=0.6)

    for action, date, p_trade, stage, size in strat.trades_history:
        if pd.Timestamp(date) in dates:
            if action == "BUY":
                ax_price.scatter(
                    date,
                    p_trade,
                    color="red",
                    marker="o",
                    s=120,
                    zorder=8,
                    edgecolors="white",
                    linewidth=1.5,
                )
                ax_price.annotate(
                    f"Backtest +{size}",
                    xy=(date, p_trade),
                    xytext=(0, 12),
                    textcoords="offset points",
                    ha="center",
                    color="red",
                    fontsize=9,
                    fontweight="bold",
                    zorder=10,
                    bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7),
                )
            else:
                ax_price.scatter(
                    date,
                    p_trade,
                    color="green",
                    marker="o",
                    s=120,
                    zorder=8,
                    edgecolors="white",
                    linewidth=1.5,
                )
                ax_price.annotate(
                    f"Backtest -{size}",
                    xy=(date, p_trade),
                    xytext=(0, -22),
                    textcoords="offset points",
                    ha="center",
                    color="green",
                    fontsize=9,
                    fontweight="bold",
                    zorder=10,
                    bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7),
                )

    # 面板 6：MACD
    ax_macd = axs[6]
    ax_macd.plot(dates, plot_df["MACD_DIF"], color="#1f77b4", label="DIF", linewidth=1)
    ax_macd.plot(dates, plot_df["MACD_DEA"], color="#ff7f0e", label="DEA", linewidth=1)
    colors = ["#d62728" if m > 0 else "#2ca02c" for m in plot_df["MACD_Hist"]]
    ax_macd.bar(dates, plot_df["MACD_Hist"], color=colors, alpha=0.6, width=0.8, label="MACD Hist")
    ax_macd.axhline(0, color="gray", linestyle="--", linewidth=0.8)
    ax_macd.set_ylabel("MACD", fontsize=10)
    ax_macd.legend(loc="upper left", ncol=3, fontsize=9)
    ax_macd.grid(True, linestyle=":", alpha=0.6)

    # 面板 7：RSI
    ax_rsi = axs[7]
    ax_rsi.plot(dates, plot_df["RSI"], label="RSI(14)", color="#8e44ad", linewidth=1.5)
    ax_rsi.axhline(70, color="red", linestyle="--", alpha=0.5)
    ax_rsi.axhline(30, color="green", linestyle="--", alpha=0.5)
    ax_rsi.fill_between(dates, 30, 70, color="#8e44ad", alpha=0.05)
    ax_rsi.set_ylim(0, 100)
    ax_rsi.set_ylabel("RSI(14)", fontsize=10)
    ax_rsi.legend(loc="upper left", fontsize=9)
    ax_rsi.grid(True, linestyle=":", alpha=0.6)

    for label in ax_rsi.get_xticklabels():
        label.set_rotation(45)
        label.set_ha("right")

    output_dir = CONFIG["ind_stock_dir"]
    pdf_path = output_dir / f"{ticker}_best_strategy_report_v2.pdf"
    png_path = output_dir / f"{ticker}_best_strategy_report_v2.png"

    fig.savefig(pdf_path, dpi=300, bbox_inches="tight")
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    return pdf_path, png_path


# ==================== 7. 明日实盘信号 V2 ====================
def print_next_day_signals_v2(
    strat: DonchianTrendStrategy,
    df: pd.DataFrame,
    config: dict,
    ticker: str,
):
    current_date = df.index[-1].strftime("%Y-%m-%d")
    current_price = float(df["close"].iloc[-1])

    entry_p = config.get("entry_period", 30)
    exit_p = config.get("exit_period", 20)
    backtest_alloc_pct = config.get("alloc_pct", 0.95)

    first_tranche_pct = config.get("live_first_tranche_pct", 0.35)
    second_tranche_pct = config.get("live_second_tranche_pct", 0.25)
    final_tranche_pct = config.get("live_final_tranche_pct", 0.35)

    live = normalize_live_state(config.get("live_state"))

    donchian_upper = float(df["high"].shift(1).rolling(entry_p).max().iloc[-1])
    donchian_lower = float(df["low"].shift(1).rolling(exit_p).min().iloc[-1])

    latest_bt_stats = strat.account_stats.get(df.index[-1].date(), None)
    if latest_bt_stats:
        bt_value, bt_cash, bt_pos_size = latest_bt_stats
        bt_pos_ratio = (bt_value - bt_cash) / bt_value * 100 if bt_value else 0
    else:
        bt_value, bt_cash, bt_pos_size, bt_pos_ratio = np.nan, np.nan, np.nan, np.nan

    live_stage = live["stage"]
    live_shares = live["shares"] or 0

    print("\n" + "🔮" * 30)
    print(f"🎯 【明日实盘交易指令预测 V2】 {ticker} | 基准日: {current_date}")
    print("🔮" * 30)

    print("\n📌 V2 解释原则:")
    print("   1) 图中的 Cash / Position / Trades 是【历史回测账户】，不是你的真实账户。")
    print("   2) 下方 Action Plan 只基于 config['live_state'] 的真实持仓状态生成。")
    print("   3) 回测参数 alloc_pct=95% 仅代表策略资金池的理论暴露，不再直接等于实盘一笔买入 95%。")

    print("\n📊 当前盘面趋势状态:")
    print(f"   最新收盘价: ${current_price:.2f}")
    print(f"   📈 动量突破买入关注线 / Donchian Upper ({entry_p}日高点): ${donchian_upper:.2f}")
    print(f"   📉 移动趋势止损清仓线 / Donchian Lower ({exit_p}日低点): ${donchian_lower:.2f}")

    print("\n🧪 回测账户状态 / Backtest Account:")
    print(f"   回测总资产: ${bt_value:,.2f}")
    print(f"   回测现金: ${bt_cash:,.2f}")
    print(f"   回测持仓股数: {bt_pos_size}")
    print(f"   回测仓位比例: {bt_pos_ratio:.2f}%")
    print("   ⚠️ 以上只是历史模拟状态，不代表你现在的真实 NVDA 仓位。")

    print("\n💼 实盘账户状态 / Live Account:")
    if live_stage is None:
        print("   ⚠️ 未检测到 live_state，请在 config 中注入真实持仓状态。")
        print("   示例: {'stage': 0, 'cost_price': 0.0, 'shares': 0, 'cash': 15000}")
    elif live_stage > 0 or live_shares > 0:
        print(f"   当前实盘状态: 【🟢 持仓中】")
        print(f"   实盘成本: ${live['cost_price']:.2f}")
        print(f"   实盘股数: {live_shares}")
    else:
        print("   当前实盘状态: 【⚪ 空仓观望】")

    # 显式冲突提示
    print("\n🧭 状态一致性检查:")
    if live_stage == 0 and bt_pos_ratio > 10:
        print("   ⚠️ 回测账户显示当前阶段大概率持仓，但实盘配置显示空仓。")
        print("   这不是交易信号矛盾，而是【回测状态】与【真实账户状态】不同。")
        print("   若你真实账户已经买入 NVDA，请把 live_state 改为 stage=1 并填入成本和股数。")
    elif live_stage and bt_pos_ratio < 10:
        print("   ⚠️ 回测账户当前接近空仓，但实盘配置显示持仓。")
        print("   请确认你是否是在回测信号之外手动买入，止损线仍可参考 Donchian Lower。")
    else:
        print("   ✅ 回测状态与实盘状态没有明显冲突，或冲突不影响下一步判断。")

    print("\n📋 明日操作建议 / Action Plan V2:")

    # ========== 实盘持仓：只看退出线 ==========
    if live_stage is not None and (live_stage > 0 or live_shares > 0):
        print("   🔴 【趋势持仓防守监控】")
        print("      - 当前策略状态：实盘已有 NVDA，不能再生成空仓买入指令。")
        print(f"      - 核心纪律：只要价格不跌破 ${donchian_lower:.2f}，继续让利润奔跑。")
        print(f"      - 盘中/收盘风险线：若价格有效跌破 < ${donchian_lower:.2f}，执行 100% 清仓。")
        print("      - 不建议因为 RSI 高或短期涨幅大而主观止盈，除非你主动降低组合风险。")

        if current_price < donchian_lower:
            print("      - ⚠️ 当前收盘已跌破移动止损线：明日应优先执行清仓纪律。")
        else:
            distance_to_stop = (current_price - donchian_lower) / current_price * 100
            print(f"      - 当前距离移动止损线约 {distance_to_stop:.2f}%，趋势仍未破坏。")

    # ========== 实盘空仓：判断是否入场 ==========
    elif live_stage is not None and live_stage == 0:
        print("   🟢 【右侧动量突破监控】")

        if current_price >= donchian_upper:
            print(f"      - ✅ 买入信号触发：当前价格已经站上 {entry_p} 日突破线。")
            print("      - V2 修正：不再建议一笔 95% 追入，改为分层建仓。")
            print(f"      - 第一笔建议：动用 NVDA 计划资金池的 {first_tranche_pct * 100:.0f}% 作为试探仓。")
            print(f"      - 第二笔条件：突破后 3–5 个交易日仍站稳突破线或 MA10/MA20，再加 {second_tranche_pct * 100:.0f}%。")
            print(f"      - 第三笔条件：回踩不破突破线/MA20 后再次上行，再加 {final_tranche_pct * 100:.0f}%。")
            print(f"      - 最终策略资金池暴露上限仍可参考回测参数 {backtest_alloc_pct * 100:.0f}%，但需服从你的组合总仓位上限。")

            est_shares = estimate_first_tranche_shares(
                current_price=current_price,
                live_cash=live["cash"],
                first_tranche_pct=first_tranche_pct,
            )
            if est_shares is not None:
                print(f"      - 按 live_state.cash=${live['cash']:,.2f} 估算，第一笔约买入 {est_shares} 股。")
            else:
                print("      - 若希望自动估算股数，请在 live_state 中加入 cash 字段，例如 cash: 15000。")

            print(f"      - 风险线：买入后统一使用 ${donchian_lower:.2f} 作为趋势失效清仓线。")
        else:
            gap = (donchian_upper - current_price) / current_price * 100
            print("      - ⚪ 当前尚未触发突破买入。")
            print(f"      - 距离突破线还差约 {gap:.2f}%。")
            print(f"      - 激活条件：价格有效上破 > ${donchian_upper:.2f} 后，再考虑第一笔试探仓。")

    # ========== 没有 live_state ==========
    else:
        print("   ⚠️ 无法生成可靠实盘指令，因为未配置 live_state。")
        print("   请先在调用 config 中加入真实账户状态。")

    print("🔮" * 30 + "\n")


# ==================== 8. 主函数 ====================
def generate_strategy_report(ticker: str = "NVDA", config: Optional[dict] = None):
    print(f"🚀 开始生成 {ticker} 动量趋势追踪策略专业报告图 V2...")

    if config is None:
        config = {}

    df = load_price_data(ticker)
    df = add_indicators(df)

    strat, final_value, total_return, max_dd = run_backtest(df, config)

    print(f"回测完成！收益率: {total_return:.2f}%  最大回撤: {max_dd:.2f}%")

    pdf_path, png_path = generate_report_figure(
        ticker=ticker,
        df=df,
        strat=strat,
        config=config,
        total_return=total_return,
        max_dd=max_dd,
    )

    print(f"\n✅ {ticker} 专属 8轴动量趋势研报图 V2 成功生成！")
    print(f"   PDF: {pdf_path}")
    print(f"   PNG: {png_path}")

    print_next_day_signals_v2(strat=strat, df=df, config=config, ticker=ticker)


# ==================== 9. 单独运行入口 ====================
if __name__ == "__main__":
    nvda_v2_config = {
        "entry_period": 30,
        "exit_period": 20,       # NVDA 的退出周期是 20
        "alloc_pct": 0.95,
        "initial_cash": 100000.0,
        "commission": 0.001,
        "verbose": True,

        # V2 实盘分层建仓参数
        "live_first_tranche_pct": 0.35,
        "live_second_tranche_pct": 0.25,
        "live_final_tranche_pct": 0.35,

        # V2 实盘状态注入
        "live_state": {
            "stage": 0,
            "cost_price": 0.0,
            "shares": 0,
            "cash": 15000,
        },
    }

    generate_strategy_report("NVDA", config=nvda_v2_config)