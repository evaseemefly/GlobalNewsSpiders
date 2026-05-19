from enum import Enum, auto

import pandas as pd
import backtrader as bt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from datetime import datetime
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


# ==================== 2. 最优策略 ====================
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
        self.trades = []  # (action, date, price, stage)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def next(self):
        if len(self) < self.params.ma_period or self.order:
            return

        price = self.data.close[0]
        date = self.data.datetime.date(0)

        # 离场
        if self.stage > 0:
            if (self.rsi[0] >= self.params.rsi_exit_th or
                    (self.params.profit_target_pct > 0 and self.position and self.position.size != 0 and
                     price >= self.position.price * (1 + self.params.profit_target_pct)) or
                    price < self.ma200[0] * 0.85):
                self.trades.append(('SELL', date, price, self.stage))
                if self.params.verbose:
                    print(f"[{date}] Stage {self.stage} → 清仓 @ {price:.2f}")
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
                self.trades.append(('BUY', date, price, self.stage))
                if self.params.verbose:
                    print(f"[{date}] Stage 0 → 1: 初始建仓 @ {price:.2f}")

        elif self.stage == 1:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop1_pct):
                size = int(self.broker.get_value() * self.params.add1_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add:
                    self.last_buy_price = price
                self.stage = 2
                self.trades.append(('BUY', date, price, self.stage))
                if self.params.verbose:
                    print(f"[{date}] Stage 1 → 2: 第一次加仓 @ {price:.2f}")

        elif self.stage == 2:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop2_pct):
                size = int(self.broker.get_value() * self.params.add2_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add:
                    self.last_buy_price = price
                self.stage = 3
                self.trades.append(('BUY', date, price, self.stage))
                if self.params.verbose:
                    print(f"[{date}] Stage 2 → 3: 第二次加仓 @ {price:.2f}")


# ==================== 3. 专业报告图生成（已完美修复） ====================
def generate_strategy_report(ticker: str = "TSLA"):
    print(f"🚀 开始生成 {ticker} 最优策略专业报告图...")

    file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    df = pd.read_csv(file_path)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()
    df = df.rename(columns={f'{ticker}_open': 'open', f'{ticker}_high': 'high',
                            f'{ticker}_low': 'low', f'{ticker}_close': 'close',
                            f'{ticker}_volume': 'volume'})

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

    # ==================== Matplotlib 配置（解决中文乱码） ====================
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC', 'SimHei', 'Microsoft YaHei']
    plt.rcParams['axes.unicode_minus'] = False

    # ==================== 绘图 ====================
    fig, axs = plt.subplots(4, 1, figsize=(15, 11), sharex=True,
                            gridspec_kw={'height_ratios': [3.5, 1.3, 1.3, 1.0]})
    fig.suptitle(f'{ticker} 最优策略回测报告 - Last 200 Days\n'
                 f'收益率: {total_return:.2f}%   最大回撤: {max_dd:.2f}%   '
                 f'生成日期: {datetime.now().strftime("%Y-%m-%d")}', fontsize=16, y=0.98)

    plot_df = df[-200:].copy()
    dates = plot_df.index
    price = plot_df['close']

    # Price Panel
    ax = axs[0]
    ax.plot(dates, price, label='Price', color='#1f77b4', linewidth=2)
    ax.plot(dates, price.rolling(50).mean(), label='MA50', color='orange', alpha=0.85)
    ax.plot(dates, price.rolling(100).mean(), label='MA100', color='green', alpha=0.85)
    ax.plot(dates, price.rolling(200).mean(), label='MA200', color='red', linewidth=2)

    bb_mid = price.rolling(20).mean()
    bb_upper = bb_mid + 2 * price.rolling(20).std()
    bb_lower = bb_mid - 2 * price.rolling(20).std()
    ax.fill_between(dates, bb_lower, bb_upper, color='gray', alpha=0.15, label='Bollinger Bands')

    ax.set_ylabel('Price')
    ax.legend(loc='upper left')
    ax.grid(True, alpha=0.3)

    # ==================== 买卖点标记（清晰标注仓位） ====================
    for action, date, p, stage in strat.trades:
        if pd.Timestamp(date) in dates:
            if action == 'BUY':
                ax.scatter(date, p, color='green', marker='^', s=180, zorder=5, edgecolor='darkgreen')
                ax.annotate(f'买{stage}', xy=(date, p), xytext=(0, 25),
                            textcoords='offset points', ha='center', color='green',
                            fontsize=12, fontweight='bold',
                            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="green", alpha=0.9))
            else:
                ax.scatter(date, p, color='red', marker='v', s=180, zorder=5, edgecolor='darkred')
                ax.annotate('卖', xy=(date, p), xytext=(0, -30),
                            textcoords='offset points', ha='center', color='red',
                            fontsize=12, fontweight='bold',
                            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="red", alpha=0.9))

    # MACD Panel
    axs[1].set_ylabel('MACD')
    axs[1].grid(True, alpha=0.3)

    # RSI Panel
    axs[2].plot(dates, strat.rsi.get(size=len(dates))[-200:], label='RSI(14)', color='purple', linewidth=2)
    axs[2].axhline(70, color='red', linestyle='--', alpha=0.7)
    axs[2].axhline(30, color='green', linestyle='--', alpha=0.7)
    axs[2].set_ylabel('RSI(14)')
    axs[2].grid(True, alpha=0.3)
    axs[2].legend(loc='upper left')

    # ATR Panel
    high = plot_df['high']
    low = plot_df['low']
    close = plot_df['close']
    tr = pd.concat([high - low, abs(high - close.shift()), abs(low - close.shift())], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    axs[3].plot(dates, atr[-200:], label='ATR(14)', color='brown', linewidth=2)
    axs[3].set_ylabel('ATR(14)')
    axs[3].grid(True, alpha=0.3)
    axs[3].legend(loc='upper left')

    plt.tight_layout()

    # 保存
    pdf_path = CONFIG['ind_stock_dir'] / f"{ticker}_best_strategy_report.pdf"
    png_path = CONFIG['ind_stock_dir'] / f"{ticker}_best_strategy_report.png"

    fig.savefig(pdf_path, dpi=300, bbox_inches='tight')
    fig.savefig(png_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

    print(f"✅ 报告图生成完成！（已修复所有问题）")
    print(f"   📄 PDF: {pdf_path}")
    print(f"   🖼️ PNG: {png_path}")


if __name__ == '__main__':
    generate_strategy_report("TSLA")
