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


# ==================== 2. 最优参数策略 ====================
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
        ('update_ref_on_add', False),      # 固定初始价（最优）
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

        # 离场逻辑
        if self.stage > 0:
            if self.rsi[0] >= self.params.rsi_exit_th:
                if self.params.verbose:
                    print(f"[{date}] Stage {self.stage} → 清仓 (RSI止盈 @ {price:.2f})")
                self.order = self.close()
                self.stage = 0
                return

            if self.params.profit_target_pct > 0 and self.position and self.position.size != 0:
                avg_price = self.position.price
                if price >= avg_price * (1 + self.params.profit_target_pct):
                    if self.params.verbose:
                        print(f"[{date}] Stage {self.stage} → 清仓 (成本止盈 {self.params.profit_target_pct*100:.0f}%)")
                    self.order = self.close()
                    self.stage = 0
                    return

            if price < self.ma200[0] * 0.85:
                if self.params.verbose:
                    print(f"[{date}] Stage {self.stage} → 清仓 (止损 MA-15%)")
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
                if self.params.verbose:
                    print(f"[{date}] Stage 1 → 2: 第一次加仓 (跌幅 {self.params.drop1_pct*100:.0f}%) @ {price:.2f}")

        elif self.stage == 2:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop2_pct):
                size = int(self.broker.get_value() * self.params.add2_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add:
                    self.last_buy_price = price
                self.stage = 3
                if self.params.verbose:
                    print(f"[{date}] Stage 2 → 3: 第二次加仓 (跌幅 {self.params.drop2_pct*100:.0f}%) @ {price:.2f}")


# ==================== 3. 最优参数详细回测 ====================
def run_best_backtest(ticker: str = "TSLA"):
    print(f"🚀 开始 {ticker} 最优参数详细回测...")

    file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    if not file_path.exists():
        print(f"❌ 文件不存在: {file_path}")
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

    # 关键修复：明确设置 optreturn=False
    cerebro = bt.Cerebro(optreturn=False)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)

    cerebro.addstrategy(
        MultiStageStrategy,
        rsi_entry_th=50,
        rsi_exit_th=70,
        drop1_pct=0.05,
        drop2_pct=0.08,
        ma_period=200,
        update_ref_on_add=False,
        profit_target_pct=0.20,
        verbose=True
    )

    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

    print("⏳ 正在运行回测...")
    start_time = time.time()
    results = cerebro.run()

    # 健壮获取策略对象
    strat = results[0][0] if isinstance(results[0], list) else results[0]

    final_value = strat.broker.get_value()
    total_return = (final_value - 100000) / 100000 * 100
    max_dd = strat.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 0.0)

    print("\n" + "=" * 80)
    print("🎯 最优参数回测完成！")
    print("=" * 80)
    print(f"初始资金     : $100,000.00")
    print(f"最终资金     : ${final_value:,.2f}")
    print(f"总收益率     : {total_return:.2f}%")
    print(f"最大回撤     : {max_dd:.2f}%")
    print(f"回测耗时     : {time.time() - start_time:.2f} 秒")
    print("=" * 80)

    print("📊 正在生成回测曲线图（会弹出窗口）...")
    cerebro.plot(style='candlestick', volume=False)


if __name__ == '__main__':
    run_best_backtest("TSLA")