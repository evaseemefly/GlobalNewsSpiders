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
    )

    def __init__(self):
        self.ma200 = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
        self.order = None
        self.last_buy_price = 0.0          # 关键：记录上一次实际买入价
        self.stage = 0

    def notify_order(self, order):
        """订单生命周期管理（Gemini 关键修复）"""
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def next(self):
        if len(self) < self.params.ma_period or self.order:
            return

        price = self.data.close[0]
        date = self.data.datetime.date(0)

        # ==================== 止盈止损 ====================
        if self.stage > 0:
            if self.rsi[0] >= self.params.rsi_exit_th or price < self.ma200[0] * 0.85:
                self.order = self.close()
                self.stage = 0
                print(f"[{date}] Stage {self.stage} → 清仓 (止盈/止损)")
                return

        # ==================== 建仓 / 加仓逻辑 ====================
        if self.stage == 0:                                   # 初始建仓
            if self.rsi[0] <= self.params.rsi_entry_th and price > self.ma200[0]:
                size = int(self.broker.get_value() * self.params.initial_alloc / price)
                self.order = self.buy(size=size)
                self.last_buy_price = price
                self.stage = 1
                print(f"[{date}] Stage 0 → 1: 初始建仓 @ {price:.2f}")

        elif self.stage == 1:                                 # 第一次加仓
            if price <= self.last_buy_price * (1 - self.params.drop1_pct):
                size = int(self.broker.get_value() * self.params.add1_alloc / price)
                self.order = self.buy(size=size)
                self.last_buy_price = price
                self.stage = 2
                print(f"[{date}] Stage 1 → 2: 第一次加仓 (跌幅 {self.params.drop1_pct*100:.0f}%) @ {price:.2f}")

        elif self.stage == 2:                                 # 第二次加仓
            if price <= self.last_buy_price * (1 - self.params.drop2_pct):
                size = int(self.broker.get_value() * self.params.add2_alloc / price)
                self.order = self.buy(size=size)
                self.last_buy_price = price
                self.stage = 3
                print(f"[{date}] Stage 2 → 3: 第二次加仓 (跌幅 {self.params.drop2_pct*100:.0f}%) @ {price:.2f}")


# ==================== 3. 主回测函数 ====================
def run_optimization(ticker):
    print(f"🚀 开始对 {ticker} 进行完整参数寻优...")

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

    data = bt.feeds.PandasData(dataname=df)

    cerebro = bt.Cerebro(optreturn=False)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)

    # 更全面的参数网格
    cerebro.optstrategy(
        MultiStageStrategy,
        rsi_entry_th=[35, 40, 45, 50],
        drop1_pct=[0.03, 0.05, 0.08, 0.10],
        drop2_pct=[0.08, 0.10, 0.12, 0.15, 0.20],
        ma_period=[150, 200, 250]
    )

    print("⏳ 正在进行参数回测...")
    start_time = time.time()
    opt_runs = cerebro.run()

    results = []
    for run in opt_runs:
        for strategy in run:
            p = strategy.params
            final_value = strategy.broker.get_value()
            total_return = (final_value - 100000) / 100000 * 100

            results.append({
                'rsi_entry': p.rsi_entry_th,
                'drop1_pct': f"{p.drop1_pct*100:.0f}%",
                'drop2_pct': f"{p.drop2_pct*100:.0f}%",
                'ma_period': p.ma_period,
                'final_value': round(final_value, 2),
                'return_%': round(total_return, 2),
            })

    res_df = pd.DataFrame(results).sort_values(by='final_value', ascending=False)

    print("\n" + "=" * 70)
    print(f"🎯 {ticker} 参数寻优 Top 15 结果")
    print("=" * 70)
    print(res_df.head(15).to_string(index=False))
    print("=" * 70)
    print(f"⏱️ 总耗时: {time.time() - start_time:.2f} 秒")

    res_df.to_csv(CONFIG['ind_stock_dir'] / f"{ticker}_parameter_optimization_final.csv", index=False)
    print(f"💾 完整结果已保存为 {ticker}_parameter_optimization_final.csv")


if __name__ == '__main__':
    # run_optimization("NVDA")   # 先测试 NVDA
    run_optimization("TSLA")
    # run_optimization("MU")