import pandas as pd
import backtrader as bt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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
    return {'ind_stock_dir': base_path / "individual_stocks"}


CURRENT_ENV = EnvType.WORK
CONFIG = get_env_config(CURRENT_ENV)


# ==================== 2. 最优策略 (TSLA: 普通多阶段网格策略) ====================
class MultiStageStrategy(bt.Strategy):
    params = (
        ('rsi_entry_th', 50),
        ('rsi_exit_th', 70),
        ('drop1_pct', 0.05),
        ('drop2_pct', 0.08),
        ('ma_period', 200),
        ('initial_alloc', 0.30),
        ('add1_alloc', 0.30),
        ('add2_alloc', 0.40),
        ('update_ref_on_add', False),
        ('profit_target_pct', 0.20),  # TSLA 使用 20% 的硬性止盈
        ('verbose', True),
    )

    def __init__(self):
        self.ma200 = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
        self.order = None
        self.initial_buy_price = 0.0
        self.last_buy_price = 0.0
        self.stage = 0

        self.trades_history = []
        self.account_stats = {}
        self.closed_pnl = []

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        dt = self.data.datetime.date(0)
        if order.status in [order.Completed]:
            if order.isbuy():
                if self.params.verbose:
                    print(
                        f"   ↳ ⚡ [历史回测成交] 🟢 买入 | 均价: ${order.executed.price:.2f} | 数量: {order.executed.size} 股")
            elif order.issell():
                if self.params.verbose:
                    print(
                        f"   ↳ ⚡ [历史回测成交] 🔴 卖出 | 均价: ${order.executed.price:.2f} | 数量: {abs(order.executed.size)} 股")
            self.order = None
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
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

        # ==================== 1. 离场逻辑 (硬止盈/止损) ====================
        if self.stage > 0:
            if (self.rsi[0] >= self.params.rsi_exit_th or
                    (self.params.profit_target_pct > 0 and self.position and self.position.size != 0 and
                     price >= self.position.price * (1 + self.params.profit_target_pct)) or
                    price < self.ma200[0] * 0.85):
                size = self.position.size
                self.trades_history.append(('SELL', dt, price, self.stage, size))
                self.order = self.close()
                self.stage = 0
                return

        # ==================== 2. 建仓 / 加仓逻辑 ====================
        if self.stage == 0:
            if self.rsi[0] <= self.params.rsi_entry_th and price > self.ma200[0]:
                size = int(self.broker.get_value() * self.params.initial_alloc / price)
                self.order = self.buy(size=size)
                self.initial_buy_price = self.last_buy_price = price
                self.stage = 1
                self.trades_history.append(('BUY', dt, price, self.stage, size))

        elif self.stage == 1:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop1_pct):
                size = int(self.broker.get_value() * self.params.add1_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add:
                    self.last_buy_price = price
                self.stage = 2
                self.trades_history.append(('BUY', dt, price, self.stage, size))

        elif self.stage == 2:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop2_pct):
                size = int(self.broker.get_value() * self.params.add2_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add:
                    self.last_buy_price = price
                self.stage = 3
                self.trades_history.append(('BUY', dt, price, self.stage, size))


# ==================== 3. 实盘辅助函数 ====================
def estimate_grid_shares(current_price: float, live_cash: Optional[float], alloc_pct: float) -> Optional[int]:
    if live_cash is None or live_cash <= 0:
        return None
    return int((live_cash * alloc_pct) / current_price)


# ==================== 4. 完美复刻专业报告图 (V2) ====================
def generate_strategy_report(ticker: str = "TSLA", config: dict = None):
    print(f"🚀 开始生成 {ticker} 最优策略专业报告图 (V2实盘隔离版)...")

    file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    if not file_path.exists():
        print(f"❌ 找不到文件: {file_path}")
        return

    df = pd.read_csv(file_path)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()
    df = df.rename(columns={f'{ticker}_open': 'open', f'{ticker}_high': 'high',
                            f'{ticker}_low': 'low', f'{ticker}_close': 'close',
                            f'{ticker}_volume': 'volume'})

    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA50'] = df['close'].rolling(50).mean()
    df['MA100'] = df['close'].rolling(100).mean()
    df['MA200'] = df['close'].rolling(200).mean()
    df['MA250'] = df['close'].rolling(250).mean()

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
    cerebro.broker.setcommission(commission=0.001)

    if config:
        cerebro.addstrategy(
            MultiStageStrategy,
            rsi_entry_th=config.get('rsi_entry_th', 50),
            rsi_exit_th=config.get('rsi_exit_th', 70),
            drop1_pct=config.get('drop1_pct', 0.05),
            drop2_pct=config.get('drop2_pct', 0.08),
            ma_period=config.get('ma_period', 200),
            initial_alloc=config.get('initial_alloc', 0.30),
            add1_alloc=config.get('add1_alloc', 0.30),
            add2_alloc=config.get('add2_alloc', 0.40),
            update_ref_on_add=config.get('update_ref_on_add', False),
            profit_target_pct=config.get('profit_target_pct', 0.20),
            verbose=config.get('verbose', True)
        )
    else:
        cerebro.addstrategy(MultiStageStrategy)

    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    results = cerebro.run()
    strat = results[0][0] if isinstance(results[0], list) else results[0]

    final_value = strat.broker.get_value()
    total_return = (final_value - 100000) / 100000 * 100
    max_dd = strat.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 0.0)

    print(f"回测完成！收益率: {total_return:.2f}%  最大回撤: {max_dd:.2f}%")

    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC', 'SimHei', 'Microsoft YaHei']
    plt.rcParams['axes.unicode_minus'] = False

    fig, axs = plt.subplots(8, 1, figsize=(16, 24), sharex=True,
                            gridspec_kw={'height_ratios': [1.1, 1.5, 1.2, 1, 1, 4.5, 1.2, 1.2]})
    fig.subplots_adjust(hspace=0.12)

    # V2版标题更新
    fig.suptitle(f'{ticker} Quant Strategy Backtest Report V2\n'
                 f'Total Return: {total_return:.2f}%   Max Drawdown: {max_dd:.2f}%\n'
                 f'注意：图中 Broker / Cash / Position 均为历史回测账户，不代表真实账户',
                 fontsize=18, fontweight='bold', y=0.94)

    plot_df = df[-200:].copy()
    dates = plot_df.index

    vals = [strat.account_stats.get(d.date(), (np.nan, np.nan))[0] for d in dates]
    cash = [strat.account_stats.get(d.date(), (np.nan, np.nan))[1] for d in dates]
    s_vals = pd.Series(vals).ffill().bfill()
    s_cash = pd.Series(cash).ffill().bfill()

    # 面板 0: 参数表 (网格版)
    ax_table = axs[0]
    ax_table.axis('off')
    params = strat.params
    col_labels = ["模块 (Module)", "参数设定 1", "参数设定 2", "V2 风控修正"]
    table_data = [
        ["入场与风控", f"RSI 抄底阈值: < {params.rsi_entry_th}", f"长线均线过滤: 站上 MA{params.ma_period}",
         f"破位止损线: MA200 -15%"],
        ["网格与资金", f"一档加仓(跌{params.drop1_pct * 100:.0f}%): {params.add1_alloc * 100:.0f}%",
         f"二档满仓(跌{params.drop2_pct * 100:.0f}%): {params.add2_alloc * 100:.0f}%",
         "实盘将根据配置的 Cash 计算股数"],
        ["离场与平仓", f"RSI 超买止盈: >= {params.rsi_exit_th}",
         f"目标利润硬止盈: {params.profit_target_pct * 100:.0f}%", "实盘指令完全基于真实持仓阶段"]
    ]
    table = ax_table.table(cellText=table_data, colLabels=col_labels, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.7)
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor('#bdc3c7')
        if row == 0:
            cell.set_text_props(weight='bold', color='white', fontsize=11)
            cell.set_facecolor('#2c3e50')
        elif col == 0:
            cell.set_text_props(weight='bold', color='#2c3e50')
            cell.set_facecolor('#ecf0f1')

    # 面板 1: 净值曲线对比
    ax_netval = axs[1]
    strat_net_value = s_vals / s_vals.iloc[0]
    baseline_net_value = plot_df['close'] / plot_df['close'].iloc[0]
    ax_netval.plot(dates, baseline_net_value, label=f'Baseline (死拿 {ticker})', color='gray', alpha=0.6, linewidth=1.5)
    ax_netval.plot(dates, strat_net_value, label=f'Quant Strategy ({ticker})', color='#e74c3c', linewidth=2.5)
    ax_netval.axhline(1.0, color='black', linestyle='--', linewidth=0.8, alpha=0.5)
    ax_netval.set_ylabel('净值 (Net Value)', fontsize=10)
    ax_netval.set_title(f'近 200 日回测策略净值 vs 股价基准 (起始 = 1.0)', loc='left', fontsize=10)
    ax_netval.legend(loc='upper left', fontsize=9)
    ax_netval.grid(True, linestyle=':', alpha=0.6)

    # 面板 2: Broker
    ax_broker = axs[2]
    ax_broker.plot(dates, s_vals, label='Backtest Total Value', color='blue', linewidth=1.5)
    ax_broker.plot(dates, s_cash, label='Backtest Cash', color='red', linewidth=1.5)
    ax_broker.set_ylabel('Broker ($)', fontsize=10)
    ax_broker.set_title("Backtest Broker: 不代表实盘真实资金", loc="left", fontsize=10)
    ax_broker.legend(loc='upper left', fontsize=9)
    ax_broker.grid(True, linestyle=':', alpha=0.6)

    # 面板 3: Trades PnL
    ax_pnl = axs[3]
    ax_pnl.axhline(0, color='black', linewidth=0.8)
    has_pnl = False
    for dt, pnl in strat.closed_pnl:
        if pd.Timestamp(dt) in dates:
            has_pnl = True
            color = 'red' if pnl > 0 else 'green'
            ax_pnl.scatter(dt, pnl, color=color, s=120, edgecolors='black', linewidth=0.5, zorder=5)
    if not has_pnl: ax_pnl.set_ylim(-1000, 1000)
    ax_pnl.set_ylabel('Net PnL ($)', fontsize=10)
    ax_pnl.set_title('Backtest Trades PnL', loc='left', fontsize=10)
    ax_pnl.grid(True, linestyle=':', alpha=0.6)

    # 面板 4: Position Ratio
    ax_pos = axs[4]
    s_pos_ratio = (s_vals - s_cash) / s_vals * 100
    ax_pos.bar(dates, s_pos_ratio, color='#3498db', alpha=0.7, width=1.0)
    ax_pos.set_ylabel('Position (%)', fontsize=10)
    ax_pos.set_ylim(0, 105)
    ax_pos.set_yticks([0, 25, 50, 75, 100])
    ax_pos.grid(True, linestyle=':', alpha=0.6)
    ax_pos.set_title('Backtest Position Ratio', loc='left', fontsize=10)

    # 面板 5: Price Main Chart
    ax_price = axs[5]
    price = plot_df['close']
    ax_price.plot(dates, price, label='Price', color='#1f77b4', linewidth=2.5, zorder=4)
    ax_price.plot(dates, plot_df['MA10'], label='MA10', color='#95a5a6', linewidth=1, alpha=0.7)
    ax_price.plot(dates, plot_df['MA20'], label='MA20', color='#8e44ad', linewidth=1.2, linestyle='--')
    ax_price.plot(dates, plot_df['MA50'], label='MA50', color='#f39c12', linewidth=1.5, alpha=0.8)
    ax_price.plot(dates, plot_df['MA100'], label='MA100', color='#27ae60', linewidth=1.5, alpha=0.8)
    ax_price.plot(dates, plot_df['MA200'], label='MA200', color='#c0392b', linewidth=2)
    ax_price.plot(dates, plot_df['MA250'], label='MA250', color='#2c3e50', linewidth=2, linestyle='-.')
    ax_price.fill_between(dates, plot_df['BB_Lower'], plot_df['BB_Upper'], color='gray', alpha=0.15,
                          label='Bollinger Bands')
    ax_price.set_ylabel('Price', fontsize=10)
    ax_price.legend(loc='upper left', ncol=3, fontsize=9)
    ax_price.grid(True, linestyle=':', alpha=0.6)

    for action, date, p_trade, stage, size in strat.trades_history:
        if pd.Timestamp(date) in dates:
            if action == 'BUY':
                ax_price.scatter(date, p_trade, color='red', marker='o', s=120, zorder=8, edgecolors='white',
                                 linewidth=1.5)
                ax_price.annotate(f'BT +{size}', xy=(date, p_trade), xytext=(0, 12), textcoords='offset points',
                                  ha='center', color='red', fontsize=9, fontweight='bold', zorder=10,
                                  bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7))
            else:
                ax_price.scatter(date, p_trade, color='green', marker='o', s=120, zorder=8, edgecolors='white',
                                 linewidth=1.5)
                ax_price.annotate(f'BT -{size}', xy=(date, p_trade), xytext=(0, -22), textcoords='offset points',
                                  ha='center', color='green', fontsize=9, fontweight='bold', zorder=10,
                                  bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7))

    # 面板 6: MACD
    ax_macd = axs[6]
    ax_macd.plot(dates, plot_df['MACD_DIF'], color='#1f77b4', label='DIF', linewidth=1)
    ax_macd.plot(dates, plot_df['MACD_DEA'], color='#ff7f0e', label='DEA', linewidth=1)
    colors = ['#d62728' if m > 0 else '#2ca02c' for m in plot_df['MACD_Hist']]
    ax_macd.bar(dates, plot_df['MACD_Hist'], color=colors, alpha=0.6, width=0.8, label='MACD Hist')
    ax_macd.axhline(0, color='gray', linestyle='--', linewidth=0.8)
    ax_macd.set_ylabel('MACD', fontsize=10)
    ax_macd.legend(loc='upper left', ncol=3, fontsize=9)
    ax_macd.grid(True, linestyle=':', alpha=0.6)

    # 面板 7: RSI
    ax_rsi = axs[7]
    ax_rsi.plot(dates, strat.rsi.get(size=len(dates))[-200:], label='RSI(14)', color='#8e44ad', linewidth=1.5)
    ax_rsi.axhline(70, color='red', linestyle='--', alpha=0.5)
    ax_rsi.axhline(30, color='green', linestyle='--', alpha=0.5)
    ax_rsi.fill_between(dates, 30, 70, color='#8e44ad', alpha=0.05)
    ax_rsi.set_ylim(0, 100)
    ax_rsi.set_ylabel('RSI(14)', fontsize=10)
    ax_rsi.grid(True, linestyle=':', alpha=0.6)
    for label in ax_rsi.get_xticklabels():
        label.set_rotation(45)
        label.set_ha('right')

    output_dir = CONFIG["ind_stock_dir"]
    pdf_path = output_dir / f"{ticker}_best_strategy_report_v2.pdf"
    png_path = output_dir / f"{ticker}_best_strategy_report_v2.png"

    fig.savefig(pdf_path, dpi=300, bbox_inches="tight")
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"\n✅ {ticker} 专属 8轴网格研报图 V2 成功生成！")

    last_date = df.index[-1].strftime("%Y-%m-%d")
    last_price = df['close'].iloc[-1]
    live_state = config.get('live_state') if config else None

    print_next_day_signals_v2(strat, ticker, last_date, last_price, live_state=live_state)


# ==================== 5. 实盘指令打印 (V2 版) ====================
def print_next_day_signals_v2(strat, ticker, current_date, current_price, live_state=None):
    print("\n" + "🔮" * 30)
    print(f"🎯 【明日实盘交易指令预测 V2】 {ticker} | 基准日: {current_date}")
    print("🔮" * 30)

    # 1. 提取实盘状态
    live_stage = live_state.get('stage', 0) if live_state else None
    live_cost = live_state.get('cost_price', 0.0) if live_state else 0.0
    live_shares = live_state.get('shares', 0) if live_state else 0
    live_cash = live_state.get('cash', None) if live_state else None

    # 2. 提取盘面指标
    ma200 = strat.ma200[0]
    rsi = strat.rsi[0]
    params = strat.params

    # 3. 提取回测状态
    bt_stage = strat.stage

    print("\n📌 V2 解释原则:")
    print("   1) 报告图表中的 Cash/Position 均为【历史回测账户】，不代表你真实的可用资金。")
    print("   2) 下方的 Action Plan 仅基于你在 config 中配置的 live_state 真实状态生成。")

    print(f"\n📊 当前盘面状态 (收盘):")
    print(f"   收盘价: ${current_price:.2f} | MA200: ${ma200:.2f} | RSI: {rsi:.2f}")

    print("\n💼 账户状态对比:")
    print(f"   [回测宇宙] 持仓阶段: Stage {bt_stage} | 成本: ${strat.last_buy_price:.2f}")

    if live_stage is None:
        print(f"   [实盘宇宙] ⚠️ 未注入 live_state 配置！")
    else:
        print(
            f"   [实盘宇宙] 持仓阶段: Stage {live_stage} | 成本: ${live_cost:.2f} | 可用现金: ${live_cash if live_cash else 0:.2f}")

    # 显式冲突提示
    if live_stage is not None and live_stage != bt_stage:
        print("   ⚠️ 提示: 回测进度与实盘阶段不一致！指令已强制按实盘 Stage 执行。")

    print("\n📋 明日操作建议 (Action Plan V2):")

    if live_stage is None:
        print("   ⚠️ 请在配置中加入 live_state 后查看实盘指令。")
        return

    # =============== 实盘操作逻辑 ===============
    if live_stage > 0:
        exit_price_sl = ma200 * 0.85
        exit_price_tp = live_cost * (1 + params.profit_target_pct)

        print(f"   🔴 【减仓/离场监控】")
        print(
            f"      - RSI 超买止盈线: 盘中监控 RSI 是否冲破 {params.rsi_exit_th} (触发无条件全仓落袋 {live_shares} 股)")
        print(f"      - 目标利润硬止盈价: >= ${exit_price_tp:.2f} (触发则 100% 清仓落袋)")
        print(f"      - 破位止损底线: < ${exit_price_sl:.2f} (触发则 100% 割肉离场)")

        # 还要判断是否触发下一档网格加仓
        if live_stage == 1:
            target_drop_price = live_cost * (1 - params.drop1_pct)
            print(f"\n   🟢 【左侧网格加仓 (一档)】")
            print(f"      - 触发条件: 向下击穿 <= ${target_drop_price:.2f}")
            shares_to_buy = estimate_grid_shares(target_drop_price, live_cash, params.add1_alloc)
            print(f"      - 建议动作: 动用资金池 {params.add1_alloc * 100:.0f}%" + (
                f" (预估买入 {shares_to_buy} 股)" if shares_to_buy else ""))

        elif live_stage == 2:
            target_drop_price = live_cost * (1 - params.drop2_pct)
            print(f"\n   🟢 【左侧极限加仓 (满仓)】")
            print(f"      - 触发条件: 向下击穿 <= ${target_drop_price:.2f}")
            shares_to_buy = estimate_grid_shares(target_drop_price, live_cash, params.add2_alloc)
            print(f"      - 建议动作: 动用资金池 {params.add2_alloc * 100:.0f}%" + (
                f" (预估买入 {shares_to_buy} 股)" if shares_to_buy else ""))

        elif live_stage >= 3:
            print(f"\n   🛡️ 【仓位已满】: 子弹已打光，严格盯紧上方的【🔴 减仓/离场监控】纪律。")

    elif live_stage == 0:
        if rsi <= params.rsi_entry_th and current_price > ma200:
            print(f"   🟢 【强烈底仓买入信号】(已满足底仓条件)")
            shares_to_buy = estimate_grid_shares(current_price, live_cash, params.initial_alloc)
            print(f"      - 动作: 建议明日开盘/盘中直接买入！")
            print(f"      - 建议动作: 动用资金池的 {params.initial_alloc * 100:.0f}%" + (
                f" (预估买入 {shares_to_buy} 股)" if shares_to_buy else ""))
        else:
            print(f"   ⚪ 【空仓观望等待】")
            print(f"      - 尚未触发底仓买点。")
            print(f"      - 底仓条件: RSI回落至 <= {params.rsi_entry_th}，且价格在 MA200 (${ma200:.2f}) 之上。")

    print("🔮" * 30 + "\n")


if __name__ == '__main__':
    tsla_config = {
        'rsi_entry_th': 50,
        'rsi_exit_th': 70,
        'drop1_pct': 0.05,
        'drop2_pct': 0.08,
        'ma_period': 200,
        'initial_alloc': 0.30,
        'add1_alloc': 0.30,
        'add2_alloc': 0.40,
        'update_ref_on_add': False,
        'profit_target_pct': 0.20,
        'verbose': True,
        'live_state': {
            'stage': 2,
            'cost_price': 404.0,
            'shares': 12,
            'cash': 10000
        }
    }
    generate_strategy_report("TSLA", config=tsla_config)
