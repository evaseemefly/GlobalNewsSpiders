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
# ==================== 2. 定义三阶段建仓策略 (修复版) ====================
class MultiStageStrategy(bt.Strategy):
    params = (
        ('rsi_entry_th', 50),
        ('rsi_exit_th', 75),
        ('drop1_pct', 0.05),
        ('drop2_pct', 0.10),
        ('ma_period', 200),
    )

    def __init__(self):
        self.ma200 = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
        self.order = None
        self.buy_price = 0.0
        self.stage = 0

        # ================= 关键修复：订单生命周期管理 =================

    def notify_order(self, order):
        # 订单处于提交或接受状态，还在处理中，不执行任何操作
        if order.status in [order.Submitted, order.Accepted]:
            return

        # 订单已完成、被取消或被拒绝，重置 self.order 变量！(解锁植物人状态)
        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    # ==============================================================

    def next(self):
        if len(self) < self.params.ma_period:
            return

        # 必须等待上一个订单处理完
        if self.order:
            return

            # ================= 离场逻辑 =================
        if self.stage > 0:
            if self.rsi[0] >= self.params.rsi_exit_th:
                self.order = self.close()
                self.stage = 0
                return

            if self.data.close[0] < self.ma200[0] * 0.85:
                self.order = self.close()
                self.stage = 0
                return

        # ================= 建仓/加仓逻辑 =================
        if self.stage == 0:
            if self.rsi[0] <= self.params.rsi_entry_th and self.data.close[0] > self.ma200[0]:
                target_value = self.broker.get_value() * 0.30  # 改用 get_value 保证复利滚动
                size = int(target_value / self.data.close[0])
                self.order = self.buy(size=size)
                self.buy_price = self.data.close[0]
                self.stage = 1

        elif self.stage == 1:
            if self.data.close[0] <= self.buy_price * (1 - self.params.drop1_pct):
                target_value = self.broker.get_value() * 0.30
                size = int(target_value / self.data.close[0])
                self.order = self.buy(size=size)
                self.stage = 2

        elif self.stage == 2:
            if self.data.close[0] <= self.buy_price * (1 - self.params.drop2_pct):
                target_value = self.broker.get_value() * 0.40
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

    # ==== 新增：添加分析器 ====
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    # ==========================

    # ==================== 👇 新增：计算 Buy & Hold 基准收益 ====================
    # 为了公平起见，我们从 MA200 能够计算出来的那一天（第200天）开始算起
    start_price = df['close'].iloc[200]
    end_price = df['close'].iloc[-1]
    bh_return_pct = (end_price - start_price) / start_price * 100

    print("\n" + "⚖️" * 25)
    print(f"📈 【基准比对】{ticker} 满仓死拿不动 (Buy & Hold)")
    print(f"建仓价格: ${start_price:.2f} | 最终价格: ${end_price:.2f}")
    print(f"死拿总收益率: {bh_return_pct:.2f}%")
    print("⚖️" * 25 + "\n")
    # =========================================================================

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

            # ==== 新增：提取最大回撤 ====
            max_drawdown = strategy.analyzers.drawdown.get_analysis()['max']['drawdown']
            # ==============================

            results_list.append({
                'drop1': f"{params.drop1_pct * 100:.0f}%",
                'drop2': f"{params.drop2_pct * 100:.0f}%",
                'final_value': round(final_value, 2),
                'return_pct': round((final_value - 100000) / 100000 * 100, 2),
                'Max_Drawdown': f"{max_drawdown:.2f}%"  # 显示最大回撤
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
    # run_optimization("TSLA")
