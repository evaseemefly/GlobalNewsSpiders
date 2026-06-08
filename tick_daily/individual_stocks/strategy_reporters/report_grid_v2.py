import pandas as pd
import backtrader as bt
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from datetime import datetime
from enum import Enum, auto
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
    return {'ind_stock_dir': base_path / "individual_stocks"}


CURRENT_ENV = EnvType.WORK
CONFIG = get_env_config(CURRENT_ENV)


def estimate_grid_shares(current_price, live_cash, alloc_pct):
    if live_cash is None or live_cash <= 0:
        return None
    return int((live_cash * alloc_pct) / current_price)


# ==================== 2. 通用多阶段网格策略 ====================
class UniversalGridStrategy(bt.Strategy):
    params = (
        ('rsi_entry_th', 50),
        ('rsi_exit_th', 80),
        ('drop1_pct', 0.05),
        ('drop2_pct', 0.05),
        ('ma_period', 150),
        ('initial_alloc', 0.30),
        ('add1_alloc', 0.30),
        ('add2_alloc', 0.40),
        ('update_ref_on_add', False),
        ('profit_target_pct', 0.15),
        ('trailing_drop_pct', 0.0),
        ('verbose', True),
    )

    def __init__(self):
        self.ma_long = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
        self.order = None
        self.initial_buy_price = 0.0
        self.last_buy_price = 0.0
        self.stage = 0
        self.highest_price_since_buy = 0.0
        self.trades_history = []
        self.account_stats = {}
        self.closed_pnl = []

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted, order.Canceled, order.Margin, order.Rejected]:
            if order.status not in [order.Submitted, order.Accepted]:
                self.order = None
            return
        if order.status == order.Completed:
            if order.isbuy():
                if self.params.verbose:
                    print(
                        f"   ↳ ⚡ [历史回测成交] 🟢 买入 | 均价: ${order.executed.price:.2f} | 数量: {order.executed.size} 股")
            elif order.issell():
                if self.params.verbose:
                    print(
                        f"   ↳ ⚡ [历史回测成交] 🔴 卖出 | 均价: ${order.executed.price:.2f} | 数量: {abs(order.executed.size)} 股")
            self.order = None

    def notify_trade(self, trade):
        if trade.isclosed:
            dt = self.data.datetime.date(0)
            self.closed_pnl.append((dt, trade.pnlcomm))

    def next(self):
        dt = self.data.datetime.date(0)
        self.account_stats[dt] = (self.broker.get_value(), self.broker.get_cash())

        if len(self) < self.params.ma_period or self.order:
            return

        price = self.data.close[0]
        if self.stage > 0:
            if price > self.highest_price_since_buy:
                self.highest_price_since_buy = price

        if self.stage > 0:
            if self.rsi[0] >= self.params.rsi_exit_th or price < self.ma_long[0] * 0.85:
                size = self.position.size
                self.trades_history.append(('SELL', dt, price, self.stage, size))
                self.order = self.close()
                self.stage, self.highest_price_since_buy = 0, 0.0
                return

            if self.params.profit_target_pct > 0 and self.position:
                avg_price = self.position.price
                if self.params.trailing_drop_pct > 0:
                    if self.highest_price_since_buy >= avg_price * (1 + self.params.profit_target_pct):
                        if price <= self.highest_price_since_buy * (1 - self.params.trailing_drop_pct):
                            size = self.position.size
                            self.trades_history.append(('SELL', dt, price, self.stage, size))
                            self.order = self.close()
                            self.stage, self.highest_price_since_buy = 0, 0.0
                            return
                else:
                    if price >= avg_price * (1 + self.params.profit_target_pct):
                        size = self.position.size
                        self.trades_history.append(('SELL', dt, price, self.stage, size))
                        self.order = self.close()
                        self.stage, self.highest_price_since_buy = 0, 0.0
                        return

        if self.stage == 0:
            if self.rsi[0] <= self.params.rsi_entry_th and price > self.ma_long[0]:
                size = int(self.broker.get_value() * self.params.initial_alloc / price)
                self.order = self.buy(size=size)
                self.initial_buy_price = self.last_buy_price = self.highest_price_since_buy = price
                self.stage = 1
                self.trades_history.append(('BUY', dt, price, self.stage, size))
        elif self.stage in [1, 2]:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            drop_pct = self.params.drop1_pct if self.stage == 1 else self.params.drop2_pct
            alloc = self.params.add1_alloc if self.stage == 1 else self.params.add2_alloc
            if price <= ref_price * (1 - drop_pct):
                size = int(self.broker.get_value() * alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add: self.last_buy_price = price
                self.stage += 1
                self.trades_history.append(('BUY', dt, price, self.stage, size))


# ==================== 3. 报表与指令生成核心 ====================
def generate_strategy_report(ticker: str, config: dict):
    print(f"🚀 开始生成 {ticker} 网格策略专业报告图 (通用模板)...")
    file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    df = pd.read_csv(file_path)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()
    df = df.rename(columns={f'{ticker}_open': 'open', f'{ticker}_high': 'high', f'{ticker}_low': 'low',
                            f'{ticker}_close': 'close'})

    # 为画图准备技术指标
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA50'] = df['close'].rolling(50).mean()
    ma_period = config.get('ma_period', 150)
    df[f'MA{ma_period}'] = df['close'].rolling(ma_period).mean()

    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    df['RSI'] = 100 - (100 / (1 + (up.ewm(com=13, adjust=False).mean() / down.ewm(com=13, adjust=False).mean())))

    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD_DIF'] = ema12 - ema26
    df['MACD_DEA'] = df['MACD_DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (df['MACD_DIF'] - df['MACD_DEA']) * 2

    std20 = df['close'].rolling(20).std()
    df['BB_Upper'] = df['MA20'] + 2 * std20
    df['BB_Lower'] = df['MA20'] - 2 * std20

    data = bt.feeds.PandasData(dataname=df)
    cerebro = bt.Cerebro(optreturn=False)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)

    cerebro.addstrategy(
        UniversalGridStrategy,
        rsi_entry_th=config.get('rsi_entry_th', 50),
        rsi_exit_th=config.get('rsi_exit_th', 80),
        drop1_pct=config.get('drop1_pct', 0.05),
        drop2_pct=config.get('drop2_pct', 0.05),
        ma_period=ma_period,
        initial_alloc=config.get('initial_alloc', 0.30),
        add1_alloc=config.get('add1_alloc', 0.30),
        add2_alloc=config.get('add2_alloc', 0.40),
        update_ref_on_add=config.get('update_ref_on_add', False),
        profit_target_pct=config.get('profit_target_pct', 0.15),
        trailing_drop_pct=config.get('trailing_drop_pct', 0.0),
        verbose=config.get('verbose', True)
    )

    results = cerebro.run()
    strat = results[0]

    # ==================== 彻底回滚的 8 轴绘图逻辑 ====================
    total_return = (strat.broker.get_value() - 100000.0) / 100000.0 * 100

    plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "PingFang SC", "SimHei", "Microsoft YaHei"]
    plt.rcParams["axes.unicode_minus"] = False
    fig, axs = plt.subplots(8, 1, figsize=(16, 24), sharex=True,
                            gridspec_kw={"height_ratios": [1.1, 1.5, 1.2, 1, 1, 4.5, 1.2, 1.2]})
    fig.subplots_adjust(hspace=0.12)
    fig.suptitle(
        f"{ticker} Grid Strategy Backtest Report V2\nTotal Return: {total_return:.2f}%\n注意：图中 Broker / Cash / Position 均为历史回测虚拟账户",
        fontsize=18, fontweight="bold", y=0.94)

    plot_df = df[-200:].copy()
    dates = plot_df.index

    vals = [strat.account_stats.get(d.date(), (np.nan, np.nan))[0] for d in dates]
    cash = [strat.account_stats.get(d.date(), (np.nan, np.nan))[1] for d in dates]
    s_vals, s_cash = pd.Series(vals, index=dates).ffill().bfill(), pd.Series(cash, index=dates).ffill().bfill()

    # 面板 0: 动态参数表格
    axs[0].axis("off")
    trail_pct = config.get('trailing_drop_pct', 0.0)
    mode_text = "移动追踪止盈" if trail_pct > 0 else "固定目标硬止盈"
    exit_logic = f"激活利润 >= {config.get('profit_target_pct') * 100:.0f}% 后，最高点回落 {trail_pct * 100:.0f}% 清仓" if trail_pct > 0 else f"利润 >= {config.get('profit_target_pct') * 100:.0f}% 直接100%落袋"

    table_data = [
        ["入场与风控", f"RSI 抄底阈值: < {config.get('rsi_entry_th')}", f"长线均线过滤: 站上 MA{ma_period}",
         f"破位止损线: MA均线 -15%"],
        ["网格与资金", f"初始底仓: {config.get('initial_alloc') * 100:.0f}%",
         f"一档加仓 (跌{config.get('drop1_pct') * 100:.0f}%): {config.get('add1_alloc') * 100:.0f}%",
         f"极限满仓 (跌{config.get('drop2_pct') * 100:.0f}%): {config.get('add2_alloc') * 100:.0f}%"],
        [mode_text, exit_logic, f"RSI超买强平: >= {config.get('rsi_exit_th')}", "实盘指令完全基于真实持仓阶段"]
    ]
    table = axs[0].table(cellText=table_data, colLabels=["模块 (Module)", "参数设定 1", "参数设定 2", "参数设定 3"],
                         loc="center", cellLoc="center")
    table.set_fontsize(10)
    table.scale(1, 1.7)
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor('#bdc3c7')
        if row == 0:
            cell.set_text_props(weight='bold', color='white')
            cell.set_facecolor('#2c3e50')
        elif col == 0:
            cell.set_text_props(weight='bold', color='#2c3e50')
            cell.set_facecolor('#ecf0f1')

    # 面板 1: 净值曲线
    axs[1].plot(dates, plot_df["close"] / plot_df["close"].iloc[0], label=f"Baseline {ticker}", color="gray", alpha=0.6,
                linewidth=1.5)
    axs[1].plot(dates, s_vals / s_vals.iloc[0], label="Quant Strategy", color="#e74c3c", linewidth=2.5)
    axs[1].axhline(1.0, color='black', linestyle='--', linewidth=0.8, alpha=0.5)
    axs[1].set_ylabel('Net Value', fontsize=10)
    axs[1].legend(loc="upper left")
    axs[1].grid(True, linestyle=':', alpha=0.6)

    # 面板 2: 资金曲线
    axs[2].plot(dates, s_vals, label="Total Value", color="blue", linewidth=1.5)
    axs[2].plot(dates, s_cash, label="Cash", color="red", linewidth=1.5)
    axs[2].set_ylabel('Broker ($)', fontsize=10)
    axs[2].legend(loc="upper left")
    axs[2].grid(True, linestyle=':', alpha=0.6)

    # 面板 3: Trades PnL
    axs[3].axhline(0, color='black', linewidth=0.8)
    has_pnl = False
    for dt, pnl in strat.closed_pnl:
        if pd.Timestamp(dt) in dates:
            has_pnl = True
            color = 'red' if pnl > 0 else 'green'
            axs[3].scatter(dt, pnl, color=color, s=120, edgecolors='black', linewidth=0.5, zorder=5)
    if not has_pnl: axs[3].set_ylim(-1000, 1000)
    axs[3].set_ylabel('Net PnL ($)', fontsize=10)
    axs[3].grid(True, linestyle=':', alpha=0.6)

    # 面板 4: 仓位比例
    s_pos_ratio = (s_vals - s_cash) / s_vals * 100
    axs[4].bar(dates, s_pos_ratio, color='#3498db', alpha=0.7, width=1.0)
    axs[4].set_ylabel('Position (%)', fontsize=10)
    axs[4].set_ylim(0, 105)
    axs[4].set_yticks([0, 25, 50, 75, 100])
    axs[4].grid(True, linestyle=':', alpha=0.6)

    # 面板 5: 主图 + MA + BB + 带股数的买卖点
    axs[5].plot(dates, plot_df["close"], label="Price", color="#1f77b4", linewidth=2.5, zorder=4)
    axs[5].plot(dates, plot_df['MA10'], label='MA10', color='#95a5a6', linewidth=1, alpha=0.7)
    axs[5].plot(dates, plot_df['MA20'], label='MA20', color='#8e44ad', linewidth=1.2, linestyle='--')
    axs[5].plot(dates, plot_df['MA50'], label='MA50', color='#f39c12', linewidth=1.5, alpha=0.8)
    axs[5].plot(dates, plot_df[f'MA{ma_period}'], label=f'MA{ma_period}', color='#c0392b', linewidth=2)
    axs[5].fill_between(dates, plot_df['BB_Lower'], plot_df['BB_Upper'], color='gray', alpha=0.15,
                        label='Bollinger Bands')

    for action, date, p_trade, stage, size in strat.trades_history:
        if pd.Timestamp(date) in dates:
            color = 'red' if action == 'BUY' else 'green'
            offset = 12 if action == 'BUY' else -22
            axs[5].scatter(date, p_trade, color=color, marker='o', s=120, zorder=8, edgecolors='white', linewidth=1.5)
            # 补回带股数的标签！
            axs[5].annotate(f"BT {'+' if action == 'BUY' else '-'}{size}", xy=(date, p_trade), xytext=(0, offset),
                            textcoords='offset points', ha='center', color=color, fontsize=9, fontweight='bold',
                            zorder=10, bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7))
    axs[5].set_ylabel('Price', fontsize=10)
    axs[5].legend(loc="upper left", ncol=3, fontsize=9)
    axs[5].grid(True, linestyle=':', alpha=0.6)

    # 面板 6: MACD
    axs[6].plot(dates, plot_df['MACD_DIF'], color='#1f77b4', label='DIF', linewidth=1)
    axs[6].plot(dates, plot_df['MACD_DEA'], color='#ff7f0e', label='DEA', linewidth=1)
    colors = ['#d62728' if m > 0 else '#2ca02c' for m in plot_df['MACD_Hist']]
    axs[6].bar(dates, plot_df['MACD_Hist'], color=colors, alpha=0.6, width=0.8, label='MACD Hist')
    axs[6].axhline(0, color='gray', linestyle='--', linewidth=0.8)
    axs[6].set_ylabel('MACD', fontsize=10)
    axs[6].legend(loc='upper left', ncol=3, fontsize=9)
    axs[6].grid(True, linestyle=':', alpha=0.6)

    # 面板 7: RSI
    axs[7].plot(dates, plot_df['RSI'], color='#8e44ad', linewidth=1.5)
    axs[7].axhline(config.get('rsi_exit_th', 80), color='red', linestyle='--', alpha=0.5)
    axs[7].axhline(config.get('rsi_entry_th', 50), color='green', linestyle='--', alpha=0.5)
    axs[7].fill_between(dates, 30, 70, color='#8e44ad', alpha=0.05)
    axs[7].set_ylim(0, 100)
    axs[7].set_ylabel('RSI(14)', fontsize=10)
    axs[7].grid(True, linestyle=':', alpha=0.6)
    for label in axs[7].get_xticklabels():
        label.set_rotation(45)
        label.set_ha('right')

    pdf_path = CONFIG["ind_stock_dir"] / f"{ticker}_grid_strategy_report.pdf"
    fig.savefig(pdf_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"✅ {ticker} 专属 8轴网格研报图 V2 成功生成！")

    # -------------- 实盘指令打印 --------------
    last_date = df.index[-1].strftime("%Y-%m-%d")
    last_price = df['close'].iloc[-1]
    print_next_day_signals_v2(strat, ticker, last_date, last_price, config.get('live_state'))


def print_next_day_signals_v2(strat, ticker, current_date, current_price, live_state=None):
    live_stage = live_state.get('stage', 0) if live_state else 0
    live_cost = live_state.get('cost_price', 0.0) if live_state else 0.0
    live_cash = live_state.get('cash', None) if live_state else None
    params = strat.params
    ma_line_val = strat.ma_long[0] if len(strat) >= params.ma_period else current_price

    print("\n" + "🔮" * 30 + f"\n🎯 【明日实盘交易指令】 {ticker} (网格模块) | 基准日: {current_date}\n" + "🔮" * 30)
    print(f"📊 收盘价: ${current_price:.2f} | MA{params.ma_period}: ${ma_line_val:.2f} | RSI: {strat.rsi[0]:.2f}")

    if live_stage > 0:
        exit_price_sl = ma_line_val * 0.85
        print(f"   🔴 【减仓/离场监控】")
        if params.trailing_drop_pct > 0:
            activation_price = live_cost * (1 + params.profit_target_pct)
            print(
                f"      - [模式: 追踪止盈] 激活线: >= ${activation_price:.2f} | 触发后最高点回撤 {params.trailing_drop_pct * 100:.0f}% 自动清仓")
        else:
            exit_price_tp = live_cost * (1 + params.profit_target_pct)
            print(f"      - [模式: 硬止盈] 目标利润价: >= ${exit_price_tp:.2f} (触发 100% 清仓落袋)")
        print(f"      - 破位硬止损: < ${exit_price_sl:.2f} (触发割肉离场)")

        if live_stage == 1:
            target_drop = live_cost * (1 - params.drop1_pct)
            shares_to_buy = estimate_grid_shares(target_drop, live_cash, params.add1_alloc)
            print(f"\n   🟢 【网格加仓(一档)】 击穿 <= ${target_drop:.2f} 买入" + (
                f" (约 {shares_to_buy} 股)" if shares_to_buy else ""))
        elif live_stage == 2:
            target_drop = live_cost * (1 - params.drop2_pct)
            shares_to_buy = estimate_grid_shares(target_drop, live_cash, params.add2_alloc)
            print(f"\n   🟢 【网格满仓(二档)】 击穿 <= ${target_drop:.2f} 买入" + (
                f" (约 {shares_to_buy} 股)" if shares_to_buy else ""))
    else:
        if strat.rsi[0] <= params.rsi_entry_th and current_price > ma_line_val:
            shares_to_buy = estimate_grid_shares(current_price, live_cash, params.initial_alloc)
            print(f"   🟢 【底仓买入】 已满足抄底条件，建议明日开盘买入" + (
                f" (约 {shares_to_buy} 股)" if shares_to_buy else ""))
        else:
            print(f"   ⚪ 【空仓观望】 未达底仓条件 (需 RSI <= {params.rsi_entry_th} 且价格 > MA{params.ma_period})")
    print("🔮" * 30 + "\n")