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
        # 核心参数（全部可优化）
        ('rsi_entry_th', 40),
        ('rsi_exit_th', 70),
        ('drop1_pct', 0.05),
        ('drop2_pct', 0.10),
        ('ma_period', 200),

        # 资金分配比例
        ('initial_alloc', 0.30),
        ('add1_alloc', 0.30),
        ('add2_alloc', 0.40),

        # 【关键改进】是否在每次加仓后更新参考价
        ('update_ref_on_add', True),   # False = Gemini风格（固定初始价）

        # 额外止盈：基于平均成本的止盈阈值（0=关闭）
        ('profit_target_pct', 0.0),

        # 打印控制（优化时建议设为False）
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
        if self.stage > 0:
            # RSI 止盈
            if self.rsi[0] >= self.params.rsi_exit_th:
                if self.params.verbose:
                    print(f"[{date}] Stage {self.stage} → 清仓 (RSI止盈 @ {price:.2f})")
                self.order = self.close()
                self.stage = 0
                return

            # 【修复】基于平均成本的止盈
            if self.params.profit_target_pct > 0 and self.position and self.position.size != 0:
                avg_price = self.position.price          # ← 这里是正确的属性！
                if price >= avg_price * (1 + self.params.profit_target_pct):
                    if self.params.verbose:
                        print(f"[{date}] Stage {self.stage} → 清仓 (成本止盈 {self.params.profit_target_pct*100:.0f}% @ {price:.2f})")
                    self.order = self.close()
                    self.stage = 0
                    return

            # 硬止损（跌破MA200 15%）
            if price < self.ma200[0] * 0.85:
                if self.params.verbose:
                    print(f"[{date}] Stage {self.stage} → 清仓 (止损 MA-15% @ {price:.2f})")
                self.order = self.close()
                self.stage = 0
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

    cerebro.optstrategy(
        MultiStageStrategy,
        rsi_entry_th=[35, 40, 45, 50],
        rsi_exit_th=[65, 70, 75, 80],
        drop1_pct=[0.03, 0.05, 0.08, 0.10],
        drop2_pct=[0.08, 0.10, 0.12, 0.15, 0.20],
        ma_period=[150, 200, 250],
        update_ref_on_add=[True, False],
        profit_target_pct=[0.0, 0.15, 0.20],
        verbose=[verbose_strategy]
    )

    print("⏳ 正在进行参数回测...")
    start_time = time.time()
    opt_runs = cerebro.run(maxcpus=None)   # 可改成 None 加速

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
                'drop1_pct': f"{p.drop1_pct*100:.0f}%",
                'drop2_pct': f"{p.drop2_pct*100:.0f}%",
                'ma_period': p.ma_period,
                'update_ref': '固定初始价' if not p.update_ref_on_add else '链式更新',
                'profit_target': f"{p.profit_target_pct*100:.0f}%" if p.profit_target_pct > 0 else '关闭',
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

    res_df.to_csv(CONFIG['ind_stock_dir'] / f"{ticker}_parameter_optimization_final.csv", index=False)
    print(f"💾 完整结果已保存为 {ticker}_parameter_optimization_final.csv")


if __name__ == '__main__':
    run_optimization("TSLA", verbose_strategy=False)   # 改为 True 可看到详细交易日志
    # run_optimization("NVDA")