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


# ==================== 2. 定义右侧趋势追踪策略 ====================
class TrendFollowingStrategy(bt.Strategy):
    """
    右侧动量趋势追踪策略 (适用于 NVDA 等极强趋势股):
    1. 初始建仓 (Stage 1): 价格站上 MA20 (短期强势) 且在 MA200 之上 (长期牛市)。买入 50%。
    2. 顺势加仓 (Stage 2): 价格较买入价上涨 add1_pct (如 8%)，确认趋势爆发。追涨 30%。
    3. 满仓逼空 (Stage 3): 价格较买入价上涨 add2_pct (如 15%)，主升浪确立。再追涨 20%。
    4. 移动止损 (Trailing Stop): 记录持有期间的“最高价”，一旦价格从最高点回撤 trail_pct (如 15%)，立刻全部获利了结/止损。
    """
    params = (
        ('ma_short', 20),  # 短期突破均线
        ('ma_long', 200),  # 长期牛市判定线
        ('add1_pct', 0.08),  # [待优化] 第一次加仓涨幅阈值
        ('add2_pct', 0.15),  # 第二次加仓涨幅阈值 (相对初始买价)
        ('trail_pct', 0.15),  # [待优化] 移动止损比例 (如 0.15 = 允许最高点回撤15%)
    )

    def __init__(self):
        self.ma_s = bt.indicators.SMA(self.data.close, period=self.params.ma_short)
        self.ma_l = bt.indicators.SMA(self.data.close, period=self.params.ma_long)

        self.order = None
        self.buy_price = 0.0
        self.highest_price = 0.0  # 用于计算移动止损
        self.stage = 0

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 订单执行完毕或取消后，释放锁
        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def next(self):
        if len(self) < self.params.ma_long:
            return

        if self.order:
            return

        # 更新持有期间的历史最高价 (移动止损基准)
        if self.stage > 0:
            self.highest_price = max(self.highest_price, self.data.close[0])

            # ================= 离场逻辑：动态移动止损 =================
            # 一旦价格跌破持有期最高价的 (1 - trail_pct)，不管赚亏，全部清仓
            stop_price = self.highest_price * (1 - self.params.trail_pct)
            if self.data.close[0] < stop_price:
                self.order = self.close()
                self.stage = 0
                self.highest_price = 0.0  # 重置最高价
                return

        # ================= 建仓/顺势加仓逻辑 =================
        if self.stage == 0:
            # 突破入场：站上 20 日均线，且长线处于牛市 (MA200之上)
            if self.data.close[0] > self.ma_s[0] and self.data.close[0] > self.ma_l[0]:
                target_value = self.broker.get_value() * 0.50  # 开局直接动用 50% 兵力
                size = int(target_value / self.data.close[0])
                self.order = self.buy(size=size)
                self.buy_price = self.data.close[0]
                self.highest_price = self.data.close[0]
                self.stage = 1

        elif self.stage == 1:
            # 金字塔加仓 1：确认涨势 (+8%)
            if self.data.close[0] >= self.buy_price * (1 + self.params.add1_pct):
                target_value = self.broker.get_value() * 0.30  # 再追加 30% 兵力
                size = int(target_value / self.data.close[0])
                self.order = self.buy(size=size)
                self.stage = 2

        elif self.stage == 2:
            # 金字塔加仓 2：主升浪逼空 (+15%)
            if self.data.close[0] >= self.buy_price * (1 + self.params.add2_pct):
                # 动用剩余的绝大部分资金（留一点点避免保证金爆仓）
                target_value = self.broker.get_value() * 0.18
                size = int(target_value / self.data.close[0])
                self.order = self.buy(size=size)
                self.stage = 3


# ==================== 3. 主引擎与优化 ====================
def run_optimization(ticker):
    print(f"🚀 开始对 {ticker} 进行 [右侧趋势追踪] 参数寻优...")

    file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    if not file_path.exists():
        print(f"❌ 找不到文件: {file_path}")
        return

    df = pd.read_csv(file_path)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()

    df = df.rename(columns={
        f'{ticker}_open': 'open',
        f'{ticker}_high': 'high',
        f'{ticker}_low': 'low',
        f'{ticker}_close': 'close',
        f'{ticker}_volume': 'volume'
    })

    data = bt.feeds.PandasData(dataname=df)

    cerebro = bt.Cerebro(optreturn=False)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)

    # ==== 优化右侧策略的核心参数 ====
    # 1. 加仓敏感度 (涨 5% 就加仓，还是涨 10% 才加仓？)
    # 2. 移动止损容忍度 (回撤 10% 就跑，还是容忍 20% 的洗盘？)
    cerebro.optstrategy(
        TrendFollowingStrategy,
        add1_pct=[0.05, 0.08, 0.12],
        trail_pct=[0.10, 0.15, 0.20]
    )

    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

    # ==== 计算 Buy & Hold 基准收益 ====
    start_price = df['close'].iloc[200]
    end_price = df['close'].iloc[-1]
    bh_return_pct = (end_price - start_price) / start_price * 100

    print("\n" + "⚖️" * 25)
    print(f"📈 【基准比对】{ticker} 满仓死拿不动 (Buy & Hold)")
    print(f"建仓价格: ${start_price:.2f} | 最终价格: ${end_price:.2f}")
    print(f"死拿总收益率: {bh_return_pct:.2f}%")
    print("⚖️" * 25 + "\n")

    start_time = time.time()
    print("⏳ 正在进行右侧网格参数计算，请稍候...")
    opt_runs = cerebro.run()

    results_list = []
    for run in opt_runs:
        for strategy in run:
            params = strategy.params
            final_value = strategy.broker.get_value()
            max_drawdown = strategy.analyzers.drawdown.get_analysis()['max']['drawdown']

            results_list.append({
                '加仓阈值(涨幅)': f"{params.add1_pct * 100:.0f}%",
                '移动止损(回撤)': f"{params.trail_pct * 100:.0f}%",
                'final_value': round(final_value, 2),
                'return_pct': round((final_value - 100000) / 100000 * 100, 2),
                'Max_Drawdown': f"{max_drawdown:.2f}%"
            })

    res_df = pd.DataFrame(results_list).sort_values(by='final_value', ascending=False)

    print("\n" + "=" * 60)
    print(f"🎯 {ticker} 右侧趋势追踪 最优参数报告")
    print("=" * 60)
    print(res_df.to_string(index=False))
    print("=" * 60)
    print(f"⏱️ 寻优耗时: {time.time() - start_time:.2f} 秒")


if __name__ == '__main__':
    run_optimization("NVDA")