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


# ==================== 2. 最优策略 (唐奇安通道全仓动量追踪) ====================
class MultiStageStrategy(bt.Strategy):
    params = (
        ('entry_period', 30),  # 突破多少日高点全仓追入
        ('exit_period', 20),   # 跌破多少日低点清仓
        ('alloc_pct', 0.95),   # 满仓比例
        ('verbose', True),
    )

    def __init__(self):
        # 1. 唐奇安通道核心指标 (仅保留必须的交易指标，防止拉长预热期)
        self.highest_high = bt.indicators.Highest(self.data.high(-1), period=self.params.entry_period)
        self.lowest_low = bt.indicators.Lowest(self.data.low(-1), period=self.params.exit_period)

        # ❌ 已删除 self.ma200 和 self.rsi，避免 Backtrader 强制等待 200 天！

        self.order = None
        self.entry_price = 0.0
        self.stage = 0  # 0=空仓, 1=全仓在场

        # --- 绘图数据追踪容器 ---
        self.trades_history = []
        self.account_stats = {}
        self.closed_pnl = []

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        dt = self.data.datetime.date(0)
        if order.status in [order.Completed]:
            if order.isbuy():
                print(
                    f"   ↳ ⚡ [历史回测成交] 🟢 买入突破 | 均价: ${order.executed.price:.2f} | 数量: {order.executed.size} 股")
                self.stage = 1
                self.entry_price = order.executed.price
            elif order.issell():
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
        self.account_stats[dt] = (self.broker.get_value(), self.broker.get_cash())

        # 确保数据预热完全
        if len(self) < max(self.params.entry_period, self.params.exit_period) or self.order:
            return

        price = self.data.close[0]

        # ==================== 1. 离场逻辑 ====================
        if self.stage > 0:
            if price < self.lowest_low[0]:
                size = self.position.size
                self.trades_history.append(('SELL', dt, price, 0, size))
                self.order = self.close()
                return

        # ==================== 2. 入场逻辑 ====================
        else:
            if price > self.highest_high[0]:
                size = int((self.broker.get_value() * self.params.alloc_pct) / price)
                if size > 0:
                    self.order = self.buy(size=size)
                    self.trades_history.append(('BUY', dt, price, 1, size))


# ==================== 3. 完美复刻专业报告图 (接受配置注入) ====================
def generate_strategy_report(ticker: str = "NVDA", config: dict = None):
    print(f"🚀 开始生成 {ticker} 动量趋势追踪最优策略专业报告图...")

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

    # 预计算技术指标 (在外部 Pandas 中计算，不影响 Backtrader 预热)
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA50'] = df['close'].rolling(50).mean()
    df['MA100'] = df['close'].rolling(100).mean()
    df['MA200'] = df['close'].rolling(200).mean()
    df['MA250'] = df['close'].rolling(250).mean()

    # 💡 新增：用 Pandas 计算 RSI(14) 供画图使用
    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=13, adjust=False).mean()
    ema_down = down.ewm(com=13, adjust=False).mean()
    rs = ema_up / ema_down
    df['RSI'] = 100 - (100 / (1 + rs))

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

    # =============== 核心逻辑：动态参数注入 ===============
    if config:
        cerebro.addstrategy(
            MultiStageStrategy,
            entry_period=config.get('entry_period', 30),
            exit_period=config.get('exit_period', 20),
            alloc_pct=config.get('alloc_pct', 0.95),
            verbose=config.get('verbose', True)
        )
    else:
        cerebro.addstrategy(MultiStageStrategy)
    # ======================================================

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
    ax_table.axis('off')

    p = strat.params
    col_labels = ["动态核心模块", "精细化配置参数 1", "精细化配置参数 2", "精细化配置参数 3"]
    table_data = [
        ["入场动量引擎", f"唐奇安上轨周期: {p.entry_period} 天", f"开仓触发机制: 突破上轨右侧追涨", "长线滤网: 暂关闭"],
        ["资金头寸风控", f"最大全仓暴露比例: {p.alloc_pct * 100:.0f}%", "开仓资金分配: 一把推(无备用弹药)",
         "ATR波动率调配: 暂关闭"],
        ["移动退出矩阵", f"唐奇安下轨周期: {p.exit_period} 天", "平仓触发机制: 跌破下轨无条件清仓",
         "固定/硬目标止盈: 未启用"]
    ]

    table = ax_table.table(cellText=table_data, colLabels=col_labels, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 1.8)

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor('#bdc3c7')
        if row == 0:
            cell.set_text_props(weight='bold', color='white', fontsize=12)
            cell.set_facecolor('#2c3e50')
        elif col == 0:
            cell.set_text_props(weight='bold', color='#2c3e50')
            cell.set_facecolor('#ecf0f1')

    # ==================== 面板 1: Broker ====================
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

    # ==================== 面板 2: Trades PnL ====================
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

    # ==================== 面板 3: Position Ratio ====================
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

    for action, date, p_trade, stage, size in strat.trades_history:
        if pd.Timestamp(date) in dates:
            if action == 'BUY':
                ax_price.scatter(date, p_trade, color='red', marker='o', s=120, zorder=8, edgecolors='white', linewidth=1.5)
                ax_price.annotate(f'+ {size}', xy=(date, p_trade), xytext=(0, 12),
                                  textcoords='offset points', ha='center', color='red', fontsize=10, fontweight='bold',
                                  zorder=10, bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7))
            else:
                ax_price.scatter(date, p_trade, color='green', marker='o', s=120, zorder=8, edgecolors='white', linewidth=1.5)
                ax_price.annotate(f'- {size}', xy=(date, p_trade), xytext=(0, -22),
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
    # 💡 替换为读取外部 pandas 计算好的 RSI
    ax_rsi.plot(dates, plot_df['RSI'], label='RSI(14)', color='#8e44ad', linewidth=1.5)
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

    print(f"\n✅ NVDA专属 7轴动量趋势研报图 (已回滚经典TSLA绘图风格) 成功生成！")

    # ==================== 👇 提取明日交易指令 ====================
    last_date = df.index[-1].strftime("%Y-%m-%d")
    last_price = df['close'].iloc[-1]

    live_state = config.get('live_state') if config else None

    # 💡 修复：将 strat 对象也传进去，直接读取最真实的持仓状态
    print_next_day_signals(strat, df, config, ticker, last_date, last_price, live_state=live_state)


def print_next_day_signals(strat, df, config, ticker, current_date, current_price, live_state=None):
    """
    自适应唐奇安突破系统的实盘交易信号生成器 (已修复滚动窗口Bug与状态同步)
    """
    print("\n" + "🔮" * 30)
    print(f"🎯 【明日实盘交易指令预测】 {ticker} | 基准日: {current_date}")
    print("🔮" * 30)

    # 参数获取
    entry_p = config.get('entry_period', 30)
    exit_p = config.get('exit_period', 20)
    alloc_pct = config.get('alloc_pct', 0.95)

    # 💡 修复：加上 .rolling()，精准获取过去 30 天/20 天的极值 (排除今天)
    donchian_upper = df['high'].shift(1).rolling(entry_p).max().iloc[-1]
    donchian_lower = df['low'].shift(1).rolling(exit_p).min().iloc[-1]

    # 状态劫持与真实状态读取
    if live_state is not None:
        print("⚠️ [系统提示] 检测到实盘配置，已劫持回测状态，按真实账户生成指令！\n")
        stage = live_state['stage']  # 0=空仓, 1=持有
        cost_price = live_state['cost_price']
    else:
        # 💡 修复：直接读取 Backtrader 策略对象里的真实状态！绝不盲猜！
        stage = strat.stage
        cost_price = strat.entry_price

    print(f"📊 当前盘面趋势状态 (收盘):")
    print(f"   最新收盘价: ${current_price:.2f}")
    print(f"   📈 动量突破买入关注线 ({entry_p}日高点): ${donchian_upper:.2f}")
    print(f"   📉 移动趋势止损清仓线 ({exit_p}日低点): ${donchian_lower:.2f}")

    if stage == 1:
        print(f"   当前持仓状态: 【🟢 满仓在场】 | 持仓成本: ${cost_price:.2f}")
    else:
        print(f"   当前持仓状态: 【⚪ 空仓观望】")

    print("\n📋 明日操作建议 (Action Plan):")

    if stage == 1:
        print(f"   🔴 【趋势多头防守监控】")
        print(f"      - 动作说明: 坚决让利润奔跑，不预设任何盲目止盈位。")
        print(f"      - 盘中死守防线: 价格若跌破 < ${donchian_lower:.2f}")
        print(f"      - 执行纪律: 触发则立即一次性【🔴 100% 满仓清仓】锁定大波段利润。")
    else:
        print(f"   🟢 【右侧动量追涨监控】")
        if current_price >= donchian_upper:
            print(f"      - 💥 触发紧急买入指令！当前价格已站上 {entry_p} 日高位突破口。")
            print(f"      - 动作: 建议明日开盘直接无脑追入。")
            print(f"      - 建议仓位: 全力调动可用资金的 {alloc_pct * 100:.0f}% 满仓突击。")
        else:
            print(f"   ⚪ 【静待临界点破位】")
            print(f"      - 动作: 当前价格未形成方向选择，保持空仓装死。")
            print(f"      - 激活条件: 明日盘中价格需向上顶穿 > ${donchian_upper:.2f}，方能全仓杀入。")

    print("🔮" * 30 + "\n")


if __name__ == '__main__':
    nvda_best_config = {
        'entry_period': 30,
        'exit_period': 20,
        'alloc_pct': 0.95,
        'verbose': True
    }
    generate_strategy_report("NVDA", config=nvda_best_config)