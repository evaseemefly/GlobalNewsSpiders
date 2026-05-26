 # + 26-05-26
# meta 采用了：
# 闪击追踪网格
# 移动止盈策略
import pandas as pd
import backtrader as bt
from pathlib import Path
import time
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


# ==================== 2. 闪击网格 + 移动追踪止盈策略 ====================
class MultiStageStrategy(bt.Strategy):
    params = (
        ('rsi_entry_th', 40),
        ('rsi_exit_th', 70),
        ('drop1_pct', 0.05),
        ('drop2_pct', 0.10),
        ('ma_period', 200),

        ('initial_alloc', 0.30),
        ('add1_alloc', 0.30),
        ('add2_alloc', 0.40),

        ('update_ref_on_add', False),  # ⚡️ 闪击网格核心：固定基准，快速满仓

        # 🌟 移动止盈核心参数
        ('profit_target_pct', 0.15),
        ('trailing_drop_pct', 0.05),

        ('verbose', False),
    )

    def __init__(self):
        self.ma200 = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
        self.order = None
        self.initial_buy_price = 0.0
        self.last_buy_price = 0.0
        self.stage = 0
        self.entry_date = None
        self.highest_price_since_buy = 0.0

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

        # 动态刷新最高价记录
        if self.stage > 0:
            if price > self.highest_price_since_buy:
                self.highest_price_since_buy = price

        # ==================== 1. 离场逻辑 ====================
        if self.stage > 0:
            # 条件 A：RSI 极度超买 (全仓离场)
            if self.rsi[0] >= self.params.rsi_exit_th:
                self.order = self.close()
                self.stage = 0
                self.highest_price_since_buy = 0.0
                return

            # 条件 B：硬止损跌破长线均线 (全仓割肉)
            if price < self.ma200[0] * 0.85:
                self.order = self.close()
                self.stage = 0
                self.highest_price_since_buy = 0.0
                return

            # ⚡️ 条件 C：移动追踪止盈 (Trailing Stop)
            if self.params.profit_target_pct > 0 and self.position:
                avg_price = self.position.price
                # 激活警报
                if self.highest_price_since_buy >= avg_price * (1 + self.params.profit_target_pct):
                    # 跌破追踪线
                    trigger_price = self.highest_price_since_buy * (1 - self.params.trailing_drop_pct)
                    if price <= trigger_price:
                        self.order = self.close()
                        self.stage = 0
                        self.highest_price_since_buy = 0.0
                        return

        # ==================== 2. 建仓 / 加仓逻辑 ====================
        if self.stage == 0:
            if self.rsi[0] <= self.params.rsi_entry_th and price > self.ma200[0]:
                size = int(self.broker.get_value() * self.params.initial_alloc / price)
                self.order = self.buy(size=size)
                self.initial_buy_price = price
                self.last_buy_price = price
                self.stage = 1
                self.entry_date = date
                self.highest_price_since_buy = price

        elif self.stage == 1:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop1_pct):
                size = int(self.broker.get_value() * self.params.add1_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add:
                    self.last_buy_price = price
                self.stage = 2

        elif self.stage == 2:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop2_pct):
                size = int(self.broker.get_value() * self.params.add2_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add:
                    self.last_buy_price = price
                self.stage = 3


# ==================== 3. 主回测函数 ====================
def run_optimization(ticker: str, verbose_strategy: bool = False):
    print(f"🚀 开始对 {ticker} 进行【闪击网格】参数寻优...")

    file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    if not file_path.exists():
        print(f"❌ 找不到文件: {file_path}")
        return

    df = pd.read_csv(file_path)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()

    df = df.rename(columns={
        f'{ticker}_open': 'open', f'{ticker}_high': 'high',
        f'{ticker}_low': 'low', f'{ticker}_close': 'close',
        f'{ticker}_volume': 'volume'
    })

    data = bt.feeds.PandasData(dataname=df)
    cerebro = bt.Cerebro(optreturn=False)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

    # Buy & Hold 基准计算 (新增最大回撤统计)
    bh_df = df.iloc[200:].copy()
    start_price = bh_df['close'].iloc[0]
    end_price = bh_df['close'].iloc[-1]
    bh_return_pct = (end_price - start_price) / start_price * 100

    bh_df['rolling_max'] = bh_df['close'].cummax()
    bh_df['drawdown'] = (bh_df['close'] - bh_df['rolling_max']) / bh_df['rolling_max']
    bh_max_dd = abs(bh_df['drawdown'].min() * 100)

    print("\n" + "⚖️" * 30)
    print(f"📈 【Buy & Hold 基准】{ticker}（从第200天起）")
    print(f"建仓价: ${start_price:.2f} → 最终价: ${end_price:.2f}")
    print(f"总收益率: {bh_return_pct:.2f}%")
    print(f"最大回撤: {bh_max_dd:.2f}%")
    print("⚖️" * 30 + "\n")

    # ⚡️ 闪击战专属寻优网格
    cerebro.optstrategy(
        MultiStageStrategy,
        rsi_entry_th=[40, 45, 50],
        rsi_exit_th=[70, 75, 80],
        drop1_pct=[0.03, 0.05],  # 极速一档
        drop2_pct=[0.05, 0.08, 0.10],  # 极速二档满仓
        ma_period=[150, 200],
        update_ref_on_add=[False],  # 强制固定基准
        profit_target_pct=[0.10, 0.15],  # 追踪激活线
        trailing_drop_pct=[0.05, 0.08, 0.10],  # META 波动大，追踪回撤可适当放宽
        verbose=[verbose_strategy]
    )

    print("⏳ 正在进行参数回测...")
    start_time = time.time()
    opt_runs = cerebro.run(maxcpus=None)

    results = []
    for run in opt_runs:
        for strategy in run:
            p = strategy.params
            final_value = strategy.broker.get_value()
            total_return = (final_value - 100000) / 100000 * 100

            drawdown = strategy.analyzers.drawdown.get_analysis()
            max_dd = drawdown.get('max', {}).get('drawdown', 0.0)

            results.append({
                'rsi_entry': p.rsi_entry_th,
                'rsi_exit': p.rsi_exit_th,
                'drop1_pct': f"{p.drop1_pct * 100:.0f}%",
                'drop2_pct': f"{p.drop2_pct * 100:.0f}%",
                'ma_period': p.ma_period,
                'update_ref': '固定',
                'profit_target': f"{p.profit_target_pct * 100:.0f}%",
                'trailing_drop': f"{p.trailing_drop_pct * 100:.0f}%",
                'final_value': round(final_value, 2),
                'return_%': round(total_return, 2),
                'max_drawdown_%': round(max_dd, 2),
            })

    res_df = pd.DataFrame(results).sort_values(by='final_value', ascending=False)

    print("\n" + "=" * 100)
    print(f"🎯 {ticker} 闪击网格寻优 Top 20 结果")
    print("=" * 100)
    print(res_df.head(20).to_string(index=False))
    print("=" * 100)
    print(f"⏱️ 总耗时: {time.time() - start_time:.2f} 秒")


if __name__ == '__main__':
    run_optimization("META")