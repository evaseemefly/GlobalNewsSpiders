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


# ==================== 2. 最终优化版三阶段建仓策略 (含50%分批止盈机制) ====================
# class MultiStageStrategy(bt.Strategy):
#     params = (
#         # 核心参数
#         ('rsi_entry_th', 40),
#         ('rsi_exit_th', 70),
#         ('drop1_pct', 0.05),
#         ('drop2_pct', 0.10),
#         ('ma_period', 200),
#
#         # 资金分配比例
#         ('initial_alloc', 0.30),
#         ('add1_alloc', 0.30),
#         ('add2_alloc', 0.40),
#
#         ('update_ref_on_add', True),
#         ('profit_target_pct', 0.15),  # 分批止盈触发线
#
#         ('verbose', False),
#     )
#
#     def __init__(self):
#         self.ma200 = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
#         self.rsi = bt.indicators.RSI(self.data.close, period=14)
#         self.order = None
#
#         self.initial_buy_price = 0.0
#         self.last_buy_price = 0.0
#         self.stage = 0
#         self.entry_date = None
#
#         # 🌟 新增：记录当前波段是否已经触发过 50% 分批止盈
#         self.partial_profit_taken = False
#
#     def notify_order(self, order):
#         if order.status in [order.Submitted, order.Accepted]:
#             return
#         if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
#             self.order = None
#
#     def next(self):
#         if len(self) < self.params.ma_period or self.order:
#             return
#
#         price = self.data.close[0]
#         date = self.data.datetime.date(0)
#
#         # ==================== 1. 离场逻辑 ====================
#         if self.stage > 0:
#
#             # 🔴 条件 A：RSI 极度超买 (全仓离场)
#             if self.rsi[0] >= self.params.rsi_exit_th:
#                 if self.params.verbose:
#                     print(f"[{date}] Stage {self.stage} → 全仓离场 (RSI极度超买 @ {price:.2f})")
#                 self.order = self.close()
#                 self.stage = 0
#                 self.partial_profit_taken = False  # 🌟 重置止盈锁
#                 return
#
#             # 🔴 条件 B：硬止损跌破长线均线 (全仓割肉)
#             if price < self.ma200[0] * 0.85:
#                 if self.params.verbose:
#                     print(f"[{date}] Stage {self.stage} → 割肉清仓 (跌破趋势底线 @ {price:.2f})")
#                 self.order = self.close()
#                 self.stage = 0
#                 self.partial_profit_taken = False  # 🌟 重置止盈锁
#                 return
#
#             # 🟡 条件 C：分批止盈 (核心改造区：达标后仅抛售 50%)
#             # 必须满足：有持仓 + 设定了止盈目标 + 之前没触发过半仓止盈
#             if self.params.profit_target_pct > 0 and self.position and not self.partial_profit_taken:
#                 avg_price = self.position.price
#                 if price >= avg_price * (1 + self.params.profit_target_pct):
#                     sell_size = self.position.size // 2  # 计算当前持股数的一半
#
#                     if sell_size > 0:
#                         if self.params.verbose:
#                             print(
#                                 f"[{date}] Stage {self.stage} → 减仓 50% 落袋为安 (利润达标 {self.params.profit_target_pct * 100:.0f}% @ {price:.2f})")
#                         self.order = self.sell(size=sell_size)
#                         self.partial_profit_taken = True  # 🌟 上锁：本轮不再触发基础止盈
#                         # 注意：这里我们【不】重置 self.stage = 0，让底仓继续待在网格里享受趋势！
#                     else:
#                         # 如果股数少于2股（比如只剩1股了），直接全仓卖出
#                         self.order = self.close()
#                         self.stage = 0
#                         self.partial_profit_taken = False
#                     return
#
#         # ==================== 2. 建仓 / 加仓逻辑 ====================
#         if self.stage == 0:
#             if self.rsi[0] <= self.params.rsi_entry_th and price > self.ma200[0]:
#                 size = int(self.broker.get_value() * self.params.initial_alloc / price)
#                 self.order = self.buy(size=size)
#                 self.initial_buy_price = price
#                 self.last_buy_price = price
#                 self.stage = 1
#                 self.entry_date = date
#                 self.partial_profit_taken = False  # 🌟 开启新一轮网格时，确保锁是打开的
#                 if self.params.verbose:
#                     print(f"[{date}] Stage 0 → 1: 初始建仓 @ {price:.2f}")
#
#         elif self.stage == 1:
#             ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
#             if price <= ref_price * (1 - self.params.drop1_pct):
#                 size = int(self.broker.get_value() * self.params.add1_alloc / price)
#                 self.order = self.buy(size=size)
#                 if self.params.update_ref_on_add:
#                     self.last_buy_price = price
#                 self.stage = 2
#                 if self.params.verbose:
#                     print(f"[{date}] Stage 1 → 2: 第一次加仓 (跌幅 {self.params.drop1_pct * 100:.0f}%) @ {price:.2f}")
#
#         elif self.stage == 2:
#             ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
#             if price <= ref_price * (1 - self.params.drop2_pct):
#                 size = int(self.broker.get_value() * self.params.add2_alloc / price)
#                 self.order = self.buy(size=size)
#                 if self.params.update_ref_on_add:
#                     self.last_buy_price = price
#                 self.stage = 3
#                 if self.params.verbose:
#                     print(f"[{date}] Stage 2 → 3: 第二次加仓 (跌幅 {self.params.drop2_pct * 100:.0f}%) @ {price:.2f}")


# ==================== 2. 最终优化版三阶段建仓策略 (含移动追踪止盈机制) ====================
class MultiStageStrategy(bt.Strategy):
    params = (
        # 核心参数
        ('rsi_entry_th', 40),
        ('rsi_exit_th', 70),
        ('drop1_pct', 0.05),
        ('drop2_pct', 0.10),
        ('ma_period', 200),

        # 资金分配比例
        ('initial_alloc', 0.30),
        ('add1_alloc', 0.30),
        ('add2_alloc', 0.40),

        ('update_ref_on_add', True),

        # 🌟 移动止盈核心参数
        ('profit_target_pct', 0.15),  # 激活线：利润达到 15% 时“激活”追踪警报
        ('trailing_drop_pct', 0.05),  # 回撤线：激活后，从最高点回撤 5% 触发全仓止盈

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

        # 🌟 新增：追踪买入后的最高价
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

        # 🌟 新增：只要有持仓，每天动态刷新最高价记录
        if self.stage > 0:
            if price > self.highest_price_since_buy:
                self.highest_price_since_buy = price

        # ==================== 1. 离场逻辑 ====================
        if self.stage > 0:

            # 🔴 条件 A：RSI 极度超买 (全仓离场)
            if self.rsi[0] >= self.params.rsi_exit_th:
                if self.params.verbose:
                    print(f"[{date}] Stage {self.stage} → 全仓离场 (RSI极度超买 @ {price:.2f})")
                self.order = self.close()
                self.stage = 0
                self.highest_price_since_buy = 0.0  # 🌟 离场后重置最高价
                return

            # 🔴 条件 B：硬止损跌破长线均线 (全仓割肉)
            if price < self.ma200[0] * 0.85:
                if self.params.verbose:
                    print(f"[{date}] Stage {self.stage} → 割肉清仓 (跌破趋势底线 @ {price:.2f})")
                self.order = self.close()
                self.stage = 0
                self.highest_price_since_buy = 0.0  # 🌟 离场后重置最高价
                return

            # 🟡 条件 C：移动追踪止盈 (Trailing Stop)
            if self.params.profit_target_pct > 0 and self.position:
                avg_price = self.position.price

                # 步骤 1：判断买入后的历史最高价，是否曾触及过“激活线”（如利润 15%）
                if self.highest_price_since_buy >= avg_price * (1 + self.params.profit_target_pct):

                    # 步骤 2：计算触发止盈的动态防守底线（最高价向下回撤 5%）
                    trigger_price = self.highest_price_since_buy * (1 - self.params.trailing_drop_pct)

                    # 步骤 3：跌破防守线，全仓斩获利润！
                    if price <= trigger_price:
                        if self.params.verbose:
                            print(
                                f"[{date}] Stage {self.stage} → 追踪止盈触发！(最高点 ${self.highest_price_since_buy:.2f} 回撤 {self.params.trailing_drop_pct * 100:.0f}% @ {price:.2f})")
                        self.order = self.close()
                        self.stage = 0
                        self.highest_price_since_buy = 0.0  # 🌟 离场后重置最高价
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
                self.highest_price_since_buy = price  # 🌟 建仓时，初始化最高价为买入价
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
                    print(f"[{date}] Stage 1 → 2: 第一次加仓 (跌幅 {self.params.drop1_pct * 100:.0f}%) @ {price:.2f}")

        elif self.stage == 2:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop2_pct):
                size = int(self.broker.get_value() * self.params.add2_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add:
                    self.last_buy_price = price
                self.stage = 3
                if self.params.verbose:
                    print(f"[{date}] Stage 2 → 3: 第二次加仓 (跌幅 {self.params.drop2_pct * 100:.0f}%) @ {price:.2f}")


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
        # 微软极少出现极度超卖，把 RSI 进场门槛稍微调高一点点，防止踏空
        rsi_entry_th=[40, 45, 50, 55],
        rsi_exit_th=[65, 70, 75, 80],

        # 网格间距收紧！探测更浅的黄金坑
        drop1_pct=[0.03, 0.05, 0.07],  # 一档加仓：跌3%、5%、7%
        drop2_pct=[0.05, 0.08, 0.10, 0.12],  # 二档满仓：跌5%、8%、10%、12%

        ma_period=[150, 200],  # 减少一些不必要的均线测试，提升回测速度
        update_ref_on_add=[True, False],

        # 微软由于波动小，单次波段可能赚不到 20%，加入 10% 和 15% 的止盈测试
        profit_target_pct=[0.0, 0.10, 0.15, 0.20],
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

            results.append({
                'rsi_entry': p.rsi_entry_th,
                'rsi_exit': p.rsi_exit_th,
                'drop1_pct': f"{p.drop1_pct * 100:.0f}%",
                'drop2_pct': f"{p.drop2_pct * 100:.0f}%",
                'ma_period': p.ma_period,
                'update_ref': '固定' if not p.update_ref_on_add else '链式',
                'profit_target': f"{p.profit_target_pct * 100:.0f}%" if p.profit_target_pct > 0 else '关闭',
                # 👇 补上这行！让表格打印出追踪止损的数值
                'trailing_drop': f"{p.trailing_drop_pct * 100:.0f}%",
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
    # run_optimization("TSLA", verbose_strategy=False)   # 改为 True 可看到详细交易日志
    run_optimization("MSFT")
