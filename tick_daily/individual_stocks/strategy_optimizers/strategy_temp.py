import pandas as pd
import backtrader as bt
import time
from pathlib import Path
from enum import Enum, auto
import warnings

warnings.filterwarnings("ignore")


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


# ==================== 2. 你的策略类 (原封不动) ====================
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
        ('update_ref_on_add', False),  # ⚡️ 闪击网格核心：固定基准
        ('profit_target_pct', 0.15),
        ('trailing_drop_pct', 0.05),
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

        if self.stage > 0:
            if price > self.highest_price_since_buy:
                self.highest_price_since_buy = price

        # 1. 离场逻辑
        if self.stage > 0:
            if self.rsi[0] >= self.params.rsi_exit_th:
                self.order = self.close()
                self.stage, self.highest_price_since_buy = 0, 0.0
                return
            if price < self.ma200[0] * 0.85:
                self.order = self.close()
                self.stage, self.highest_price_since_buy = 0, 0.0
                return
            if self.params.profit_target_pct > 0 and self.position:
                avg_price = self.position.price
                if self.highest_price_since_buy >= avg_price * (1 + self.params.profit_target_pct):
                    trigger_price = self.highest_price_since_buy * (1 - self.params.trailing_drop_pct)
                    if price <= trigger_price:
                        self.order = self.close()
                        self.stage, self.highest_price_since_buy = 0, 0.0
                        return

        # 2. 建仓 / 加仓逻辑
        if self.stage == 0:
            if self.rsi[0] <= self.params.rsi_entry_th and price > self.ma200[0]:
                size = int(self.broker.get_value() * self.params.initial_alloc / price)
                self.order = self.buy(size=size)
                self.initial_buy_price = self.last_buy_price = price
                self.stage = 1
                self.entry_date = date
                self.highest_price_since_buy = price
        elif self.stage == 1:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop1_pct):
                size = int(self.broker.get_value() * self.params.add1_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add: self.last_buy_price = price
                self.stage = 2
        elif self.stage == 2:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            if price <= ref_price * (1 - self.params.drop2_pct):
                size = int(self.broker.get_value() * self.params.add2_alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add: self.last_buy_price = price
                self.stage = 3


# ==================== 3. 滚动窗盲测 (WFO) 引擎 ====================
def run_wfo_for_avgo(ticker="AVGO"):
    print(f"🚀 开始对 {ticker} 执行 Walk-Forward Optimization (滚动窗盲测)...")

    # --- A. 数据加载 ---
    file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    if not file_path.exists():
        print(f"❌ 找不到文件: {file_path}")
        return

    df = pd.read_csv(file_path)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()
    df = df.rename(columns={f'{ticker}_open': 'open', f'{ticker}_high': 'high',
                            f'{ticker}_low': 'low', f'{ticker}_close': 'close', f'{ticker}_volume': 'volume'})

    # --- B. WFO 参数配置 ---
    train_months = 24  # 用过去2年数据寻找参数
    test_months = 6  # 盲测紧接着的未来半年
    ma_period = 200  # 你的长线均线预热需求

    if len(df) <= ma_period:
        print("❌ 数据总长度不足以支撑 200 天预热。")
        return

    # 第一个有效的训练起点：跳过最初的 200 天预热期
    current_train_start = df.index[ma_period]
    end_of_data = df.index[-1]

    oos_results = []
    step = 1
    start_time = time.time()

    # --- C. 核心滚动循环 ---
    while True:
        train_end = current_train_start + pd.DateOffset(months=train_months)
        test_end = train_end + pd.DateOffset(months=test_months)

        # 截断处理：如果测试期超出了我们的最新数据，就结束滚动
        if test_end > end_of_data:
            if train_end >= end_of_data: break
            test_end = end_of_data

        print("\n" + "=" * 70)
        print(f"🔄 [Step {step}] 训练窗(已知历史): {current_train_start.date()} 至 {train_end.date()}")
        print(f"👁️‍🗨️ [Step {step}] 盲测窗(未知未来): {train_end.date()} 至 {test_end.date()}")

        # ---------------------------------------------------------
        # 阶段 1：训练寻找当前周期最优参数 (In-Sample)
        # ---------------------------------------------------------
        # 💡 神级操作：往回多取 200 个交易日垫底，防止均线断层！
        train_start_idx = df.index.searchsorted(current_train_start)
        train_warmup_idx = max(0, train_start_idx - ma_period)
        train_end_idx = df.index.searchsorted(train_end)
        train_df = df.iloc[train_warmup_idx: train_end_idx]

        cerebro_train = bt.Cerebro(optreturn=False)
        cerebro_train.adddata(bt.feeds.PandasData(dataname=train_df))
        cerebro_train.broker.setcash(100000.0)

        # 组合网格 (2x2x2 = 8种组合，快速寻优)
        cerebro_train.optstrategy(
            MultiStageStrategy,
            rsi_entry_th=[45, 50],
            drop1_pct=[0.03, 0.05],
            drop2_pct=[0.05, 0.08],
            profit_target_pct=[0.10, 0.15],
            trailing_drop_pct=[0.05, 0.08],
            verbose=[False]
        )

        opt_runs = cerebro_train.run(maxcpus=None)

        # 选出这 24 个月里赚钱最多的参数组合
        best_params = None
        best_return = -999.0

        for run in opt_runs:
            for strategy in run:
                ret = (strategy.broker.get_value() - 100000) / 100000
                if ret > best_return:
                    best_return = ret
                    best_params = strategy.params

        print(f"   🏆 训练期第一名参数: RSI入场={best_params.rsi_entry_th}, "
              f"一档跌幅={best_params.drop1_pct * 100:.0f}%, 激活线={best_params.profit_target_pct * 100:.0f}%, "
              f"回撤清仓={best_params.trailing_drop_pct * 100:.0f}%")

        # ---------------------------------------------------------
        # 阶段 2：盲测未来 6 个月的真实表现 (Out-of-Sample)
        # ---------------------------------------------------------
        # 同样往回取 200 个交易日，确保盲测第一天就能算出均线并交易
        test_start_idx = df.index.searchsorted(train_end)
        test_warmup_idx = max(0, test_start_idx - ma_period)
        test_end_idx = df.index.searchsorted(test_end)
        test_df = df.iloc[test_warmup_idx: test_end_idx]

        cerebro_test = bt.Cerebro()
        cerebro_test.adddata(bt.feeds.PandasData(dataname=test_df))
        cerebro_test.broker.setcash(100000.0)

        # ⚠️ 锁定训练得到的参数，绝对不许修改，直接放入盲测
        cerebro_test.addstrategy(
            MultiStageStrategy,
            rsi_entry_th=best_params.rsi_entry_th,
            drop1_pct=best_params.drop1_pct,
            drop2_pct=best_params.drop2_pct,
            profit_target_pct=best_params.profit_target_pct,
            trailing_drop_pct=best_params.trailing_drop_pct,
            verbose=False
        )

        cerebro_test.run()
        oos_return = (cerebro_test.broker.get_value() - 100000) / 100000 * 100
        print(f"   📊 盲测期真实收益率: {oos_return:.2f}%")

        oos_results.append({
            'Step': step,
            'Blind_Test_Period': f"{train_end.date()} to {test_end.date()}",
            'Real_Return_%': round(oos_return, 2),
            'Params (RSI/Drop1/TP/Trail)': f"{best_params.rsi_entry_th} / {best_params.drop1_pct} / {best_params.profit_target_pct} / {best_params.trailing_drop_pct}"
        })

        # 窗口整体往前滚动 6 个月，进入下一轮时间切片
        current_train_start += pd.DateOffset(months=test_months)
        step += 1

    # --- D. 打印终极盲测战报 ---
    print("\n" + "🌟" * 35)
    print(f"【{ticker}】 Walk-Forward Optimization 盲测历史总战报")
    print("🌟" * 35)

    results_df = pd.DataFrame(oos_results)
    print(results_df.to_string(index=False))

    total_oos_return = results_df['Real_Return_%'].sum()
    print(f"\n💰 盲测净利润累加 (剔除所有参数作弊): {total_oos_return:.2f}%")
    print(f"⏱️ 引擎运行耗时: {time.time() - start_time:.1f} 秒")
    print("🌟" * 35 + "\n")


if __name__ == '__main__':
    # 你可以把 AVGO 的 CSV 数据准备好后运行
    run_wfo_for_avgo("AVGO")
