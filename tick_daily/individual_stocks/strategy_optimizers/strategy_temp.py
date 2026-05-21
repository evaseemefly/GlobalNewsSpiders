# strategy_temp.py
import pandas as pd
import backtrader as bt
from pathlib import Path
import time
from enum import Enum, auto

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

# ==================== 2. 海龟/唐奇安趋势跟踪策略 ====================
class TrendFollowingStrategy(bt.Strategy):
    params = (
        ('entry_period', 50),
        ('exit_period', 20),
        ('alloc_pct', 0.95),  # 95%仓位防滑点
        ('verbose', False),
    )

    def __init__(self):
        self.highest_high = bt.indicators.Highest(self.data.high(-1), period=self.params.entry_period)
        self.lowest_low = bt.indicators.Lowest(self.data.low(-1), period=self.params.exit_period)
        self.entry_price = 0.0
        self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def next(self):
        if len(self) < max(self.params.entry_period, self.params.exit_period) or self.order:
            return

        price = self.data.close[0]
        date = self.data.datetime.date(0)

        if not self.position:
            if price > self.highest_high[0]:
                size = int((self.broker.get_value() * self.params.alloc_pct) / price)
                self.order = self.buy(size=size)
                self.entry_price = price
        else:
            if price < self.lowest_low[0]:
                self.order = self.close()

# ==================== 3. 针对 MU 的精细化寻优 ====================
def run_optimization(ticker: str, verbose_strategy: bool = False):
    print(f"🚀 开始对 {ticker} 进行【趋势跟踪】精细化参数寻优...")

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

    start_price = df['close'].iloc[200]
    end_price = df['close'].iloc[-1]
    bh_return_pct = (end_price - start_price) / start_price * 100
    print("\n" + "⚖️" * 30)
    print(f"📈 【Buy & Hold 基准】{ticker}（从第200天起）")
    print(f"建仓价: ${start_price:.2f} → 最终价: ${end_price:.2f}")
    print(f"总收益率: {bh_return_pct:.2f}%")
    print("⚖️" * 30 + "\n")

    # ================= 核心修改：拓展参数边界 =================
    print("🤖 正在突破边界，探测更短的进场周期与更长的离场周期...")
    cerebro.optstrategy(
        TrendFollowingStrategy,
        entry_period=[15, 20, 25, 30, 35],  # 突破天数：向更小周期探索
        exit_period=[20, 25, 30, 35, 40],   # 跌破天数：向更大周期探索
        verbose=[verbose_strategy]
    )
    # ==========================================================

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
                'entry_period (突破天数)': p.entry_period,
                'exit_period (跌破天数)': p.exit_period,
                'alloc_pct': f"{p.alloc_pct * 100:.0f}%",
                'final_value': round(final_value, 2),
                'return_%': round(total_return, 2),
                'max_drawdown_%': round(max_dd, 2),
            })

    res_df = pd.DataFrame(results).sort_values(by='final_value', ascending=False)

    print("\n" + "=" * 90)
    print(f"🎯 {ticker} 精细化参数寻优 Top 20 结果")
    print("=" * 90)
    print(res_df.head(20).to_string(index=False))
    print("=" * 90)
    print(f"⏱️ 总耗时: {time.time() - start_time:.2f} 秒")

if __name__ == '__main__':
    run_optimization("MU")