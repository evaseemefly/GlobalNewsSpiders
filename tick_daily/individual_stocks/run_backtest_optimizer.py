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


# ==================== 2. 定义三阶段建仓策略 ====================
class MultiStageStrategy(bt.Strategy):
    """
    三阶段防守反击策略：
    1. 初始建仓 (Stage 1): 价格在 MA200 之上，且 RSI < rsi_entry_th (例如 40，轻度回调)。买入 30% 仓位。
    2. 第一次加仓 (Stage 2): 价格比第一次买入价下跌了 drop1_pct (例如 5%)。加仓 30%。
    3. 第二次加仓 (Stage 3): 价格比第一次买入价下跌了 drop2_pct (例如 10%)。加仓 40%。
    4. 离场 (Exit): RSI > rsi_exit_th (例如 70，超买止盈)，或价格跌破 MA200 一定幅度 (止损)。
    """
    params = (
        ('rsi_entry_th', 40),  # 初始入场 RSI 阈值
        ('rsi_exit_th', 70),  # 止盈 RSI 阈值
        ('drop1_pct', 0.05),  # [待优化参数] 第一次加仓跌幅 (如 0.05 = 5%)
        ('drop2_pct', 0.10),  # [待优化参数] 第二次加仓跌幅 (如 0.10 = 10%)
        ('ma_period', 200),  # 长期均线
    )

    def __init__(self):
        # 初始化指标
        self.ma200 = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)

        # 记录交易状态
        self.order = None
        self.buy_price = 0.0
        self.stage = 0  # 0: 空仓, 1: 一阶段, 2: 二阶段, 3: 满仓

    def next(self):
        # 还没有足够的数据计算 MA200 时，不操作
        if len(self) < self.params.ma_period:
            return

        if self.order:
            return  # 有未完成的订单，等待执行完毕

        # ================= 离场逻辑 (止盈或破位止损) =================
        if self.stage > 0:
            # 条件A: 出现超买，获利了结
            if self.rsi[0] >= self.params.rsi_exit_th:
                self.order = self.close()  # 全部平仓
                self.stage = 0
                return
            # 条件B: 价格彻底跌破 MA200 达到 3% 以上，认错止损
            if self.data.close[0] < self.ma200[0] * 0.97:
                self.order = self.close()
                self.stage = 0
                return

        # ================= 建仓/加仓逻辑 =================
        # 空仓状态，寻找初始入场点 (RSI回调 + 趋势健康)
        if self.stage == 0:
            if self.rsi[0] <= self.params.rsi_entry_th and self.data.close[0] > self.ma200[0]:
                target_value = self.broker.get_cash() * 0.30  # 动用 30% 资金
                size = int(target_value / self.data.close[0])
                self.order = self.buy(size=size)
                self.buy_price = self.data.close[0]
                self.stage = 1

        # 已有底仓，寻找第一次加仓点
        elif self.stage == 1:
            if self.data.close[0] <= self.buy_price * (1 - self.params.drop1_pct):
                target_value = self.broker.get_value() * 0.30  # 动用总资产的 30%
                size = int(target_value / self.data.close[0])
                self.order = self.buy(size=size)
                self.stage = 2

        # 已有一/二次仓位，寻找最后一次极限加仓点
        elif self.stage == 2:
            if self.data.close[0] <= self.buy_price * (1 - self.params.drop2_pct):
                target_value = self.broker.get_value() * 0.40  # 动用剩余的 40%
                size = int(target_value / self.data.close[0])
                self.order = self.buy(size=size)
                self.stage = 3


# ==================== 3. 主引擎与数据加载 ====================
def run_optimization(ticker):
    print(f"🚀 开始对 {ticker} 进行多阶段加仓参数寻优...")

    # 1. 加载并清洗本地数据
    file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    if not file_path.exists():
        print(f"❌ 找不到文件: {file_path}")
        return

    df = pd.read_csv(file_path)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()

    # 规范化列名以适配 Backtrader
    df = df.rename(columns={
        f'{ticker}_open': 'open',
        f'{ticker}_high': 'high',
        f'{ticker}_low': 'low',
        f'{ticker}_close': 'close',
        f'{ticker}_volume': 'volume'
    })

    data = bt.feeds.PandasData(dataname=df)

    # 2. 初始化回测大脑 (Cerebro)
    cerebro = bt.Cerebro(optreturn=False)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)  # 初始资金 10万美金
    cerebro.broker.setcommission(commission=0.001)  # 设置千分之一手续费

    # 3. 设定网格搜索参数 (OptStrategy)
    # 我们让系统自己去尝试：
    # 第一档加仓跌幅：跌 3%, 5%, 8%？
    # 第二档加仓跌幅：跌 10%, 15%, 20%？
    cerebro.optstrategy(
        MultiStageStrategy,
        drop1_pct=[0.03, 0.05, 0.08],
        drop2_pct=[0.10, 0.15, 0.20]
    )

    # 4. 运行回测
    start_time = time.time()
    print("⏳ 正在进行网格参数回测计算，请稍候...")
    opt_runs = cerebro.run()

    # 5. 解析最优结果
    results_list = []
    for run in opt_runs:
        for strategy in run:
            params = strategy.params
            final_value = strategy.broker.get_value()
            results_list.append({
                'drop1': f"{params.drop1_pct * 100:.0f}%",
                'drop2': f"{params.drop2_pct * 100:.0f}%",
                'final_value': round(final_value, 2),
                'return_pct': round((final_value - 100000) / 100000 * 100, 2)
            })

    # 将结果转换为 DataFrame 并按收益率排序
    res_df = pd.DataFrame(results_list).sort_values(by='final_value', ascending=False)

    print("\n" + "=" * 50)
    print(f"🎯 {ticker} 最优参数寻优报告 (初始资金: $100,000)")
    print("=" * 50)
    print(res_df.head(10).to_string(index=False))
    print("=" * 50)
    print(f"⏱️ 寻优耗时: {time.time() - start_time:.2f} 秒")


if __name__ == '__main__':
    # 你可以把这里的 NVDA 换成 TSLA 或 META 来测试不同股性的最优加仓间距
    run_optimization("NVDA")