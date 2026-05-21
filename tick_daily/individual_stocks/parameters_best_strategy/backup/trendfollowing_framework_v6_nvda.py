import math

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


# ==================== 2. 最终优化版三阶段建仓策略 ====================
class NvdaSuperTrendStrategy(bt.Strategy):
    params = (
        ('entry_period', 30),  # 突破多少日高点全仓追入
        ('exit_period', 20),  # 跌破多少日低点清仓
        ('ma_period', 200),  # 牛熊分界线
        ('alloc_pct', 0.95),  # 满仓比例
        ('verbose', False),
    )

    def __init__(self):
        # 1. 唐奇安通道
        self.highest_high = bt.indicators.Highest(self.data.high(-1), period=self.params.entry_period)
        self.lowest_low = bt.indicators.Lowest(self.data.low(-1), period=self.params.exit_period)

        # 2. 长期趋势均线（过滤熊市假突破）
        self.ma = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
        self.order = None

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

        # ==================== 1. 离场逻辑 ====================
        if self.position:
            # 满足以下任意条件，全仓离场锁定利润：
            # A. 跌破长期均线（大趋势走坏）
            # B. 跌破唐奇安下轨
            if price < self.ma[0] or price < self.lowest_low[0]:
                if self.params.verbose:
                    print(f"[{date}] 🛑 趋势终结平仓 @ {price:.2f}")
                self.order = self.close()
                return

        # ==================== 2. 入场逻辑 ====================
        else:
            # 只有在200日均线之上（牛市），且突破前高的那一刻，才一注独赢全仓杀入
            if price > self.ma[0] and price > self.highest_high[0]:
                size = int((self.broker.get_value() * self.params.alloc_pct) / price)
                if size > 0:
                    if self.params.verbose:
                        print(f"[{date}] 🚀 顺势多头突破，95%全仓突击 @ {price:.2f}")
                    self.order = self.buy(size=size)


# ==================== 3. 主回测函数 ====================
def run_optimization(ticker: str, verbose_strategy: bool = False):
    print(f"🚀 开始对 {ticker} 进行完整参数寻优...")

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

    # Buy & Hold 基准
    start_price = df['close'].iloc[200]
    end_price = df['close'].iloc[-1]
    bh_return_pct = (end_price - start_price) / start_price * 100
    print("\n" + "⚖️" * 30)
    print(f"📈 【Buy & Hold 基准】{ticker}（从第200天起）")
    print(f"建仓价: ${start_price:.2f} → 最终价: ${end_price:.2f}")
    print(f"总收益率: {bh_return_pct:.2f}%")
    print("⚖️" * 30 + "\n")

    print("🤖 启动 NVDA 专属【全仓动量趋势】策略寻优...")
    cerebro.optstrategy(
        NvdaSuperTrendStrategy,
        entry_period=[20, 30, 40],
        exit_period=[15, 20, 25, 30],  # 适当放宽离场周期，给牛头留出呼吸的空间
        ma_period=[150, 200],
        verbose=[verbose_strategy]
    )

    print("⏳ 正在进行参数回测...")
    start_time = time.time()
    opt_runs = cerebro.run(maxcpus=None)  # 可改成 None 加速

    results = []
    for run in opt_runs:
        for strategy in run:
            p = strategy.params
            final_value = strategy.broker.get_value()
            total_return = (final_value - 100000) / 100000 * 100

            drawdown = strategy.analyzers.drawdown.get_analysis()
            max_dd = drawdown.get('max', {}).get('drawdown', 0.0)

            #  修复后：严格对应 NvdaSuperTrendStrategy 的参数
            results.append({
                'entry_period (突破天数)': p.entry_period,
                'exit_period (离场天数)': p.exit_period,
                'ma_period (趋势均线)': p.ma_period,
                'alloc_pct (全仓比例)': f"{p.alloc_pct * 100:.0f}%",
                'final_value': round(final_value, 2),
                'return_%': round(total_return, 2),
                'max_drawdown_%': round(max_dd, 2),
            })

    res_df = pd.DataFrame(results).sort_values(by='final_value', ascending=False)

    print("\n" + "=" * 90)
    print(f"🎯 {ticker} 参数寻优 Top 20 结果")
    print("=" * 90)
    print(res_df.head(20).to_string(index=False))
    print("=" * 90)
    print(f"⏱️ 总耗时: {time.time() - start_time:.2f} 秒")

    res_df.to_csv(CONFIG['ind_stock_dir'] / f"{ticker}_parameter_optimization_final_{ticker}.csv", index=False)
    print(f"💾 完整结果已保存为 {ticker}_parameter_optimization_final.csv")


if __name__ == '__main__':
    # run_optimization("TSLA", verbose_strategy=False)  # 改为 True 可看到详细交易日志
    run_optimization("NVDA")
