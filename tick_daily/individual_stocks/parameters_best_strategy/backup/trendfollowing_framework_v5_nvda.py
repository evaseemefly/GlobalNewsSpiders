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
class TrueTurtleStrategy(bt.Strategy):
    params = (
        ('entry_period', 20),  # 唐奇安上轨（买入突破周期，海龟系统一是20天）
        ('exit_period', 10),  # 唐奇安下轨（卖出离场周期，海龟系统一是10天）
        ('atr_period', 20),  # 计算N值(ATR)的周期
        ('risk_pct', 0.01),  # 每一个仓位单位(Unit)承担账户总资产1%的风险
        ('max_units', 4),  # 最大允许加仓到4个单位
        ('verbose', False),
    )

    def __init__(self):
        # 1. 唐奇安通道指标
        self.highest_high = bt.indicators.Highest(self.data.high(-1), period=self.params.entry_period)
        self.lowest_low = bt.indicators.Lowest(self.data.low(-1), period=self.params.exit_period)

        # 2. 海龟的 N 值 (ATR)
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)

        # 3. 核心状态变量
        self.order = None
        self.stage = 0  # 持有仓位单位数 (0到4)
        self.last_buy_price = 0.0  # 最后一次买入（加仓）的价格
        self.stop_loss = 0.0  # 动态追踪止损价

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        date = self.data.datetime.date(0)
        if order.status == order.Completed:
            if order.isbuy():
                self.stage += 1
                self.last_buy_price = order.executed.price
                # 每一次买入/加仓成功，立即统一更新动态止损线：最后一次执行价 - 2 * N
                self.stop_loss = self.last_buy_price - 2 * self.atr[0]
                if self.params.verbose:
                    print(
                        f"[{date}] 已成交 Buy Unit {self.stage} @ {order.executed.price:.2f} | 统一新止损线: {self.stop_loss:.2f}")
            else:
                if self.params.verbose:
                    print(f"[{date}] 已成交 全仓清仓卖出 @ {order.executed.price:.2f}")
                self.stage = 0
                self.last_buy_price = 0.0
                self.stop_loss = 0.0

        self.order = None

    def next(self):
        # 等待指标预热，且当前不能有未完结的订单
        if len(self) < max(self.params.entry_period, self.params.atr_period) or self.order:
            return

        price = self.data.close[0]
        date = self.data.datetime.date(0)
        n_value = self.atr[0]

        # 如果 ATR 异常为 0（极端情况），不进行交易
        if n_value <= 0:
            return

        # ==================== 1. 持仓离场与硬止损逻辑 ====================
        if self.stage > 0:
            # 离场点 A：触及动态追踪止损线（最后一次买入价 - 2N）
            # 离场点 B：跌破唐奇安通道下轨（系统一离场点）
            if price < self.stop_loss or price < self.lowest_low[0]:
                reason = "触及2N止损" if price < self.stop_loss else f"跌破{self.params.exit_period}日下轨"
                if self.params.verbose:
                    print(
                        f"[{date}] 🛑 触发离场 ({reason}): 当前价 {price:.2f} | 止损线 {self.stop_loss:.2f} | 下轨 {self.lowest_low[0]:.2f}")
                self.order = self.close()
                return

        # ==================== 2. 开仓与顺势加仓逻辑 ====================
        # 情况A：完全空仓，寻找突破开第一仓 (Unit 1 -> 直接占总资金 40%)
        if self.stage == 0:
            if price > self.highest_high[0]:
                total_value = self.broker.get_value()
                # 魔改：第一仓直接干 40% 的仓位
                unit_size = math.floor((total_value * 0.40) / price)

                if unit_size > 0:
                    if self.params.verbose:
                        print(f"[{date}] 🚀 突破开仓 Unit 1. 投入40%资金买入: {unit_size} 股")
                    self.order = self.buy(size=unit_size)

        # 情况B：已有持仓，顺势加仓 (Unit 2, 3, 4 -> 每次占总资金 20%)
        elif self.stage < self.params.max_units:
            if price >= self.last_buy_price + 0.5 * n_value:
                total_value = self.broker.get_value()
                # 魔改：后续每次加仓使用 20% 资金
                unit_size = math.floor((total_value * 0.20) / price)

                if unit_size > 0:
                    if self.params.verbose:
                        print(f"[{date}] 📈 顺势加仓 Unit {self.stage + 1}. 投入20%资金买入: {unit_size} 股")
                    self.order = self.buy(size=unit_size)

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

    # print("🤖 针对 NVDA：应用【海龟/唐奇安趋势跟踪】策略寻优...")
    print("🤖 针对 NVDA：应用【正宗海龟 ATR 动态加仓】策略寻优...")
    cerebro.optstrategy(
        TrueTurtleStrategy,
        entry_period=[20, 30, 40],  # 入场突破天数（海龟经典是20）
        exit_period=[10, 15, 20],  # 离场下轨天数（海龟经典是10）
        atr_period=[20],  # ATR窗口，通常保持20固定即可
        risk_pct=[0.01, 0.015],  # 每一个仓位单位控制在总资产 1% 到 1.5% 的风险
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

            # 👇 替换为趋势跟踪策略专属的参数
            results.append({
                'entry_period (突破天数)': p.entry_period,
                'exit_period (离场天数)': p.exit_period,
                'risk_pct (单Unit风险)': f"{p.risk_pct * 100:.1f}%",
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
