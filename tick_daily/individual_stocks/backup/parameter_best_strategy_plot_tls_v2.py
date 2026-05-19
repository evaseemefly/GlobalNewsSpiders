import pandas as pd
import backtrader as bt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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


# ==================== 2. 最优策略 (含资金与盈亏追踪) ====================
# ==================== 2. 最优策略 (含资金、盈亏追踪与精细日志) ====================
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
        ('profit_target_pct', 0.20),
        ('verbose', True),
    )

    def __init__(self):
        self.ma200 = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)

        self.order = None
        self.initial_buy_price = 0.0
        self.last_buy_price = 0.0
        self.stage = 0

        # --- 绘图数据追踪容器 ---
        self.trades_history = []  # 记录: (action, date, price, stage, size)
        self.account_stats = {}  # 记录每日: date -> (value, cash)
        self.closed_pnl = []  # 记录每笔平仓: (date, pnl)

    # ==================== 订单执行日志 (实盘成交) ====================
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        dt = self.data.datetime.date(0)

        if order.status in [order.Completed]:
            # 订单成功撮合成交，打印真实动用的资金和精准的成交价
            if order.isbuy():
                print(
                    f"   ↳ ⚡ [实盘成交] 🟢 买入 | 均价: ${order.executed.price:.2f} | 数量: {order.executed.size} 股 | 动用资金: ${order.executed.value:,.2f} | 剩余现金: ${self.broker.get_cash():,.2f}")
            elif order.issell():
                print(
                    f"   ↳ ⚡ [实盘成交] 🔴 卖出 | 均价: ${order.executed.price:.2f} | 数量: {abs(order.executed.size)} 股 | 回笼资金: ${order.executed.value:,.2f} | 剩余现金: ${self.broker.get_cash():,.2f}")
            self.order = None

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print(f"   ↳ ⚠️ [订单异常] 状态: {order.getstatusname()} | 资金可能不足或触发风控")
            self.order = None

    # ==================== 结单盈亏日志 ====================
    def notify_trade(self, trade):
        # 当一笔交易（买+卖的闭环）完成时，打印净盈亏
        if trade.isclosed:
            dt = self.data.datetime.date(0)
            self.closed_pnl.append((dt, trade.pnlcomm))

            pnl_color = "🟩 净盈利" if trade.pnlcomm > 0 else "🟥 净亏损"
            print(
                f"[{dt}] 🏁 波段结单 | {pnl_color}: ${trade.pnlcomm:,.2f} | 账户总资产: ${self.broker.get_value():,.2f}")
            print("-" * 75)

    # ==================== 每日决策逻辑 ====================
    def next(self):
        dt = self.data.datetime.date(0)
        self.account_stats[dt] = (self.broker.get_value(), self.broker.get_cash())

        if len(self) < self.params.ma_period or self.order:
            return

        price = self.data.close[0]

        # 离场
        if self.stage > 0:
            if (self.rsi[0] >= self.params.rsi_exit_th or
                    (self.params.profit_target_pct > 0 and self.position and self.position.size != 0 and
                     price >= self.position.price * (1 + self.params.profit_target_pct)) or
                    price < self.ma200[0] * 0.85):

                size = self.position.size
                self.trades_history.append(('SELL', dt, price, self.stage, size))

                if self.params.verbose:
                    reason = "RSI超买止盈" if self.rsi[0] >= self.params.rsi_exit_th else (
                        "目标利润止盈" if price >= self.position.price * (
                                1 + self.params.profit_target_pct) else "跌破年线止损")
                    print(f"[{dt}] 📡 触发信号 | Stage {self.stage} → 0: 清仓 ({reason}) @ 当日收盘价 ${price:.2f}")

                self.order = self.close()
                self.stage = 0
                return

        # 建仓 / 加仓
        if self.stage == 0:
            if self.rsi[0] <= self.params.rsi_entry_th and price > self.ma200[0]:
                size = int(self.broker.get_value() * self.params.initial_alloc / price)
                self.order = self.buy(size=size)
                self.initial_buy_price = self.last_buy_price = price
                self.stage = 1
                self.trades_history.append(('BUY', dt, price, self.stage, size))
                if self.params.verbose:
                    print(
                        f"[{dt}] 📡 触发信号 | Stage 0 → 1: 初始建仓 (RSI底背离) @ 当日收盘价 ${price:.2f} (计划买入 {size} 股)")

        elif self.stage == 1:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop1_pct):
                size = int(self.broker.get_value() * self.params.add1_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add:
                    self.last_buy_price = price
                self.stage = 2
                self.trades_history.append(('BUY', dt, price, self.stage, size))
                if self.params.verbose:
                    print(
                        f"[{dt}] 📡 触发信号 | Stage 1 → 2: 第一次加仓 (跌破网格1) @ 当日收盘价 ${price:.2f} (计划买入 {size} 股)")

        elif self.stage == 2:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop2_pct):
                size = int(self.broker.get_value() * self.params.add2_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add:
                    self.last_buy_price = price
                self.stage = 3
                self.trades_history.append(('BUY', dt, price, self.stage, size))
                if self.params.verbose:
                    print(
                        f"[{dt}] 📡 触发信号 | Stage 2 → 3: 极限加仓 (跌破网格2) @ 当日收盘价 ${price:.2f} (计划买入 {size} 股)")


# ==================== 3. 完美复刻专业报告图 ====================
# ==================== 3. 完美复刻专业报告图 (7轴联动带参数表) ====================
def generate_strategy_report(ticker: str = "TSLA"):
    print(f"🚀 开始生成 {ticker} 最优策略专业报告图 (含参数表)...")

    file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    df = pd.read_csv(file_path)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()
    df = df.rename(columns={f'{ticker}_open': 'open', f'{ticker}_high': 'high',
                            f'{ticker}_low': 'low', f'{ticker}_close': 'close',
                            f'{ticker}_volume': 'volume'})

    # ---------------- 预计算全局指标 ----------------
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
    # ------------------------------------------------

    data = bt.feeds.PandasData(dataname=df)
    cerebro = bt.Cerebro(optreturn=False)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)

    cerebro.addstrategy(MultiStageStrategy)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

    results = cerebro.run()
    strat = results[0][0] if isinstance(results[0], list) else results[0]

    final_value = strat.broker.get_value()
    total_return = (final_value - 100000) / 100000 * 100
    max_dd = strat.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 0.0)

    print(f"回测完成！收益率: {total_return:.2f}%  最大回撤: {max_dd:.2f}%")

    # ==================== Matplotlib 绘图配置 ====================
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC', 'SimHei', 'Microsoft YaHei']
    plt.rcParams['axes.unicode_minus'] = False

    # 布局升级为 7 行面板 (参数表、资产、盈亏点、持仓比例、主图、MACD、RSI)
    fig, axs = plt.subplots(7, 1, figsize=(16, 21), sharex=True,
                            gridspec_kw={'height_ratios': [0.8, 1.2, 1, 1, 4.5, 1.2, 1.2]})

    fig.subplots_adjust(hspace=0.1)
    fig.suptitle(f'{ticker} Quant Strategy Backtest Report\n'
                 f'Total Return: {total_return:.2f}%   Max Drawdown: {max_dd:.2f}%',
                 fontsize=20, fontweight='bold', y=0.93)

    plot_df = df[-200:].copy()
    dates = plot_df.index

    # ==================== 面板 0: Strategy Parameters Table ====================
    ax_table = axs[0]
    ax_table.axis('off')  # 隐藏坐标轴

    params = strat.params
    # 动态提取策略参数构建表格数据
    col_labels = ["模块 (Module)", "参数设定 1", "参数设定 2", "参数设定 3"]
    table_data = [
        ["入场与风控", f"RSI 抄底阈值: < {params.rsi_entry_th}", f"长线均线过滤: 站上 MA{params.ma_period}",
         f"破位止损线: MA200 -15%"],
        ["网格与资金", f"初始底仓: {params.initial_alloc * 100:.0f}%",
         f"一档加仓 (跌{params.drop1_pct * 100:.0f}%): {params.add1_alloc * 100:.0f}%",
         f"极限满仓 (跌{params.drop2_pct * 100:.0f}%): {params.add2_alloc * 100:.0f}%"],
        ["离场与平仓", f"RSI 超买止盈: >= {params.rsi_exit_th}", f"目标利润止盈: {params.profit_target_pct * 100:.0f}%",
         f"基准价格更新: {params.update_ref_on_add}"]
    ]

    table = ax_table.table(cellText=table_data, colLabels=col_labels, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 1.8)  # 拉伸表格高度让其更美观

    # 为表格注入机构级 UI 质感
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor('#bdc3c7')
        if row == 0:
            cell.set_text_props(weight='bold', color='white', fontsize=12)
            cell.set_facecolor('#2c3e50')  # 深蓝色表头
        elif col == 0:
            cell.set_text_props(weight='bold', color='#2c3e50')
            cell.set_facecolor('#ecf0f1')  # 浅灰色列头

    # ==================== 面板 1: Broker (Value & Cash) ====================
    ax_broker = axs[1]
    vals = [strat.account_stats.get(d.date(), (np.nan, np.nan))[0] for d in dates]
    cash = [strat.account_stats.get(d.date(), (np.nan, np.nan))[1] for d in dates]
    s_vals = pd.Series(vals).ffill()
    s_cash = pd.Series(cash).ffill()

    ax_broker.plot(dates, s_vals, label='Total Value', color='blue', linewidth=1.5)
    ax_broker.plot(dates, s_cash, label='Cash', color='red', linewidth=1.5)
    ax_broker.set_ylabel('Broker ($)', fontsize=10)
    ax_broker.legend(loc='upper left', fontsize=9)
    ax_broker.grid(True, linestyle=':', alpha=0.6)

    # ==================== 面板 2: Trades Net Profit/Loss ====================
    ax_pnl = axs[2]
    ax_pnl.axhline(0, color='black', linewidth=0.8)

    has_pnl = False
    for dt, pnl in strat.closed_pnl:
        if pd.Timestamp(dt) in dates:
            has_pnl = True
            color = 'red' if pnl > 0 else 'green'
            ax_pnl.scatter(dt, pnl, color=color, s=120, edgecolors='black', linewidth=0.5, zorder=5)

    if not has_pnl: ax_pnl.set_ylim(-1000, 1000)
    ax_pnl.set_ylabel('Net PnL ($)', fontsize=10)
    ax_pnl.set_title('Trades PnL (Red=Win, Green=Loss)', loc='left', fontsize=10)
    ax_pnl.grid(True, linestyle=':', alpha=0.6)

    # ==================== 面板 3: 持仓比例 (Position Ratio) ====================
    ax_pos = axs[3]
    s_pos_ratio = (s_vals - s_cash) / s_vals * 100
    ax_pos.bar(dates, s_pos_ratio, color='#3498db', alpha=0.7, width=1.0)
    ax_pos.set_ylabel('Position (%)', fontsize=10)
    ax_pos.set_ylim(0, 105)
    ax_pos.set_yticks([0, 25, 50, 75, 100])
    ax_pos.grid(True, linestyle=':', alpha=0.6)
    ax_pos.set_title('Position Ratio (%)', loc='left', fontsize=10)

    # ==================== 面板 4: Price Main Chart ====================
    ax_price = axs[4]
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

    for action, date, p, stage, size in strat.trades_history:
        if pd.Timestamp(date) in dates:
            if action == 'BUY':
                ax_price.scatter(date, p, color='red', marker='o', s=120, zorder=8, edgecolors='white', linewidth=1.5)
                ax_price.annotate(f'+ {size}', xy=(date, p), xytext=(0, 12),
                                  textcoords='offset points', ha='center', color='red', fontsize=10, fontweight='bold',
                                  zorder=10, bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7))
            else:
                ax_price.scatter(date, p, color='green', marker='o', s=120, zorder=8, edgecolors='white', linewidth=1.5)
                ax_price.annotate(f'- {size}', xy=(date, p), xytext=(0, -22),
                                  textcoords='offset points', ha='center', color='green', fontsize=10,
                                  fontweight='bold',
                                  zorder=10, bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7))

    # ==================== 面板 5: MACD ====================
    ax_macd = axs[5]
    ax_macd.plot(dates, plot_df['MACD_DIF'], color='#1f77b4', label='DIF', linewidth=1)
    ax_macd.plot(dates, plot_df['MACD_DEA'], color='#ff7f0e', label='DEA', linewidth=1)

    colors = ['#d62728' if m > 0 else '#2ca02c' for m in plot_df['MACD_Hist']]
    ax_macd.bar(dates, plot_df['MACD_Hist'], color=colors, alpha=0.6, width=0.8, label='MACD Hist')
    ax_macd.axhline(0, color='gray', linestyle='--', linewidth=0.8)

    ax_macd.set_ylabel('MACD', fontsize=10)
    ax_macd.legend(loc='upper left', ncol=3, fontsize=9)
    ax_macd.grid(True, linestyle=':', alpha=0.6)

    # ==================== 面板 6: RSI ====================
    ax_rsi = axs[6]
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

    # ==================== 输出保存 ====================
    pdf_path = CONFIG['ind_stock_dir'] / f"{ticker}_best_strategy_report.pdf"
    png_path = CONFIG['ind_stock_dir'] / f"{ticker}_best_strategy_report.png"

    fig.savefig(pdf_path, dpi=300, bbox_inches='tight')
    fig.savefig(png_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

    print(f"\n✅ 终极研报图 (7轴带参数配置表) 已生成！")

    # ==================== 👇 新增：提取明日交易指令 ====================
    last_date = df.index[-1].strftime("%Y-%m-%d")
    last_price = df['close'].iloc[-1]
    print_next_day_signals(strat, ticker, last_date, last_price)


def print_next_day_signals(strat, ticker, current_date, current_price):
    """
    提取策略最后一日的状态，推算下一交易日的交易计划
    """
    print("\n" + "🔮" * 30)
    print(f"🎯 【明日实盘交易指令预测】 {ticker} | 基准日: {current_date}")
    print("🔮" * 30)

    # 提取当前状态
    stage = strat.stage
    ma200 = strat.ma200[0]
    rsi = strat.rsi[0]
    params = strat.params

    print(f"📊 当前盘面状态 (收盘):")
    print(f"   收盘价: ${current_price:.2f} | MA200: ${ma200:.2f} | RSI: {rsi:.2f}")
    print(f"   当前持仓阶段: Stage {stage}")

    if stage > 0:
        print(f"   最新建仓均价: ${strat.last_buy_price:.2f}")
        # 如果有持仓，尝试获取持仓成本价
        cost_price = strat.position.price if strat.position else strat.last_buy_price
        print(f"   当前持仓成本: ${cost_price:.2f}")

    print("\n📋 明日操作建议 (Action Plan):")

    # ================= 1. 离场/防守条件预测 =================
    if stage > 0:
        exit_price_sl = ma200 * 0.85
        print(f"   🔴 【减仓/离场监控】")
        print(f"      - RSI 超买止盈线: 盘中监控 RSI 是否冲破 {params.rsi_exit_th} (触发则 100% 清仓)")

        if params.profit_target_pct > 0 and strat.position:
            exit_price_tp = strat.position.price * (1 + params.profit_target_pct)
            print(f"      - 目标利润止盈价: >= ${exit_price_tp:.2f} (触发则 100% 清仓落袋)")

        print(f"      - 破位止损底线: < ${exit_price_sl:.2f} (触发则 100% 清仓离场)")
        print(f"      *(注: 本网格策略采用单波段清仓制，一旦触发上述条件，建议清空当前波段所有筹码)*")
        print()

    # ================= 2. 加仓/建仓条件预测 =================
    if stage == 0:
        if rsi <= params.rsi_entry_th and current_price > ma200:
            print(f"   🟢 【强烈买入信号】(已满足底仓条件)")
            print(f"      - 动作: 建议明日开盘/盘中直接买入！")
            print(f"      - 建议仓位: 动用当前可用总资金的 {params.initial_alloc * 100:.0f}%")
        else:
            print(f"   ⚪ 【观望等待】")
            print(f"      - 尚未触发底仓买点。")
            print(f"      - 触发条件: RSI 需回落至 <= {params.rsi_entry_th}，且价格维持在 MA200 (${ma200:.2f}) 之上。")

    elif stage == 1:
        ref_price = strat.last_buy_price if params.update_ref_on_add else strat.initial_buy_price
        target_drop_price = ref_price * (1 - params.drop1_pct)
        print(f"   🟢 【左侧网格加仓监控 (第一次加仓)】")
        print(f"      - 触发条件: 价格向下击穿 <= ${target_drop_price:.2f}")
        print(f"      - 建议仓位: 打入总资金的 {params.add1_alloc * 100:.0f}%")

    elif stage == 2:
        ref_price = strat.last_buy_price if params.update_ref_on_add else strat.initial_buy_price
        target_drop_price = ref_price * (1 - params.drop2_pct)
        print(f"   🟢 【左侧极限加仓监控 (第二次满仓)】")
        print(f"      - 触发条件: 价格向下击穿 <= ${target_drop_price:.2f}")
        print(f"      - 建议仓位: 打入总资金的 {params.add2_alloc * 100:.0f}%")

    elif stage == 3:
        print(f"   🛡️ 【仓位已满】")
        print(f"      - 动作: 子弹已打光，严格执行上方的【🔴 减仓/离场监控】纪律，等待反弹止盈或认错止损。")

    print("🔮" * 30 + "\n")


if __name__ == '__main__':
    generate_strategy_report("TSLA")
