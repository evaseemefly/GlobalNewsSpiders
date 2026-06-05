import pandas as pd
import backtrader as bt
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from datetime import datetime
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

def estimate_grid_shares(current_price, live_cash, alloc_pct):
    if live_cash is None or live_cash <= 0:
        return None
    return int((live_cash * alloc_pct) / current_price)

# ==================== 2. 通用多阶段网格策略 (自适应追踪止盈) ====================
class UniversalGridStrategy(bt.Strategy):
    params = (
        ('rsi_entry_th', 50),
        ('rsi_exit_th', 80),
        ('drop1_pct', 0.05),
        ('drop2_pct', 0.05),
        ('ma_period', 150),
        ('initial_alloc', 0.30),
        ('add1_alloc', 0.30),
        ('add2_alloc', 0.40),
        ('update_ref_on_add', False),
        ('profit_target_pct', 0.15),
        ('trailing_drop_pct', 0.0),  # 🌟 通用化核心：默认为0(硬止盈)，大于0则开启追踪止盈
        ('verbose', True),
    )

    def __init__(self):
        self.ma_long = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
        self.order = None
        self.initial_buy_price = 0.0
        self.last_buy_price = 0.0
        self.stage = 0
        self.highest_price_since_buy = 0.0
        self.trades_history = []
        self.account_stats = {}
        self.closed_pnl = []

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted, order.Canceled, order.Margin, order.Rejected]:
            if order.status not in [order.Submitted, order.Accepted]:
                self.order = None
            return
        if order.status == order.Completed:
            if order.isbuy():
                if self.params.verbose:
                    print(f"   ↳ ⚡ [历史回测成交] 🟢 买入 | 均价: ${order.executed.price:.2f} | 数量: {order.executed.size} 股")
            elif order.issell():
                if self.params.verbose:
                    print(f"   ↳ ⚡ [历史回测成交] 🔴 卖出 | 均价: ${order.executed.price:.2f} | 数量: {abs(order.executed.size)} 股")
            self.order = None

    def notify_trade(self, trade):
        if trade.isclosed:
            dt = self.data.datetime.date(0)
            self.closed_pnl.append((dt, trade.pnlcomm))

    def next(self):
        dt = self.data.datetime.date(0)
        self.account_stats[dt] = (self.broker.get_value(), self.broker.get_cash())

        if len(self) < self.params.ma_period or self.order:
            return

        price = self.data.close[0]
        if self.stage > 0:
            if price > self.highest_price_since_buy:
                self.highest_price_since_buy = price

        # ==================== 1. 离场逻辑 ====================
        if self.stage > 0:
            # RSI 超买强平 / 均线破位硬止损
            if self.rsi[0] >= self.params.rsi_exit_th or price < self.ma_long[0] * 0.85:
                size = self.position.size
                self.trades_history.append(('SELL', dt, price, self.stage, size))
                self.order = self.close()
                self.stage, self.highest_price_since_buy = 0, 0.0
                return

            # 动态止盈模块 (根据参数自适应)
            if self.params.profit_target_pct > 0 and self.position:
                avg_price = self.position.price
                if self.params.trailing_drop_pct > 0:
                    # 启用追踪止盈 (MSFT, META)
                    if self.highest_price_since_buy >= avg_price * (1 + self.params.profit_target_pct):
                        if price <= self.highest_price_since_buy * (1 - self.params.trailing_drop_pct):
                            size = self.position.size
                            self.trades_history.append(('SELL', dt, price, self.stage, size))
                            self.order = self.close()
                            self.stage, self.highest_price_since_buy = 0, 0.0
                            return
                else:
                    # 启用硬目标止盈 (TSLA)
                    if price >= avg_price * (1 + self.params.profit_target_pct):
                        size = self.position.size
                        self.trades_history.append(('SELL', dt, price, self.stage, size))
                        self.order = self.close()
                        self.stage, self.highest_price_since_buy = 0, 0.0
                        return

        # ==================== 2. 网格建仓逻辑 ====================
        if self.stage == 0:
            if self.rsi[0] <= self.params.rsi_entry_th and price > self.ma_long[0]:
                size = int(self.broker.get_value() * self.params.initial_alloc / price)
                self.order = self.buy(size=size)
                self.initial_buy_price = self.last_buy_price = self.highest_price_since_buy = price
                self.stage = 1
                self.trades_history.append(('BUY', dt, price, self.stage, size))
        elif self.stage in [1, 2]:
            ref_price = self.last_buy_price if self.params.update_ref_on_add else self.initial_buy_price
            drop_pct = self.params.drop1_pct if self.stage == 1 else self.params.drop2_pct
            alloc = self.params.add1_alloc if self.stage == 1 else self.params.add2_alloc
            if price <= ref_price * (1 - drop_pct):
                size = int(self.broker.get_value() * alloc / price)
                self.order = self.buy(size=size)
                if self.params.update_ref_on_add: self.last_buy_price = price
                self.stage += 1
                self.trades_history.append(('BUY', dt, price, self.stage, size))

# ==================== 3. 报表与指令生成核心 ====================
def generate_strategy_report(ticker: str, config: dict):
    print(f"🚀 开始生成 {ticker} 网格策略专业报告图 (通用模板)...")
    file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    df = pd.read_csv(file_path)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()
    df = df.rename(columns={f'{ticker}_open': 'open', f'{ticker}_high': 'high', f'{ticker}_low': 'low', f'{ticker}_close': 'close'})

    df['MA10'] = df['close'].rolling(10).mean()
    df['MA200'] = df['close'].rolling(200).mean()
    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    df['RSI'] = 100 - (100 / (1 + (up.ewm(com=13, adjust=False).mean() / down.ewm(com=13, adjust=False).mean())))

    data = bt.feeds.PandasData(dataname=df)
    cerebro = bt.Cerebro(optreturn=False)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)

    # 动态注入配置，兜底 trailing_drop 为 0
    cerebro.addstrategy(
        UniversalGridStrategy,
        rsi_entry_th=config.get('rsi_entry_th', 50),
        rsi_exit_th=config.get('rsi_exit_th', 80),
        drop1_pct=config.get('drop1_pct', 0.05),
        drop2_pct=config.get('drop2_pct', 0.05),
        ma_period=config.get('ma_period', 150),
        initial_alloc=config.get('initial_alloc', 0.30),
        add1_alloc=config.get('add1_alloc', 0.30),
        add2_alloc=config.get('add2_alloc', 0.40),
        update_ref_on_add=config.get('update_ref_on_add', False),
        profit_target_pct=config.get('profit_target_pct', 0.15),
        trailing_drop_pct=config.get('trailing_drop_pct', 0.0),
        verbose=config.get('verbose', True)
    )

    results = cerebro.run()
    strat = results[0]

    # -------------- 实盘指令打印 --------------
    last_date = df.index[-1].strftime("%Y-%m-%d")
    last_price = df['close'].iloc[-1]
    print_next_day_signals_v2(strat, ticker, last_date, last_price, config.get('live_state'))

def print_next_day_signals_v2(strat, ticker, current_date, current_price, live_state=None):
    live_stage = live_state.get('stage', 0) if live_state else 0
    live_cost = live_state.get('cost_price', 0.0) if live_state else 0.0
    live_cash = live_state.get('cash', None) if live_state else None
    params = strat.params
    ma_line_val = strat.ma_long[0] if len(strat) >= params.ma_period else current_price

    print("\n" + "🔮" * 30 + f"\n🎯 【明日实盘交易指令】 {ticker} (网格模块) | 基准日: {current_date}\n" + "🔮" * 30)
    print(f"📊 收盘价: ${current_price:.2f} | MA{params.ma_period}: ${ma_line_val:.2f} | RSI: {strat.rsi[0]:.2f}")

    if live_stage > 0:
        exit_price_sl = ma_line_val * 0.85
        print(f"   🔴 【减仓/离场监控】")
        if params.trailing_drop_pct > 0:
            activation_price = live_cost * (1 + params.profit_target_pct)
            print(f"      - [模式: 追踪止盈] 激活线: >= ${activation_price:.2f} | 触发后最高点回撤 {params.trailing_drop_pct*100:.0f}% 自动清仓")
        else:
            exit_price_tp = live_cost * (1 + params.profit_target_pct)
            print(f"      - [模式: 硬止盈] 目标利润价: >= ${exit_price_tp:.2f} (触发 100% 清仓落袋)")
        print(f"      - 破位硬止损: < ${exit_price_sl:.2f} (触发割肉离场)")

        if live_stage == 1:
            target_drop = live_cost * (1 - params.drop1_pct)
            shares_to_buy = estimate_grid_shares(target_drop, live_cash, params.add1_alloc)
            print(f"\n   🟢 【网格加仓(一档)】 击穿 <= ${target_drop:.2f} 买入" + (f" (约 {shares_to_buy} 股)" if shares_to_buy else ""))
        elif live_stage == 2:
            target_drop = live_cost * (1 - params.drop2_pct)
            shares_to_buy = estimate_grid_shares(target_drop, live_cash, params.add2_alloc)
            print(f"\n   🟢 【网格满仓(二档)】 击穿 <= ${target_drop:.2f} 买入" + (f" (约 {shares_to_buy} 股)" if shares_to_buy else ""))
    else:
        if strat.rsi[0] <= params.rsi_entry_th and current_price > ma_line_val:
            shares_to_buy = estimate_grid_shares(current_price, live_cash, params.initial_alloc)
            print(f"   🟢 【底仓买入】 已满足抄底条件，建议明日开盘买入" + (f" (约 {shares_to_buy} 股)" if shares_to_buy else ""))
        else:
            print(f"   ⚪ 【空仓观望】 未达底仓条件 (需 RSI <= {params.rsi_entry_th} 且价格 > MA{params.ma_period})")
    print("🔮" * 30 + "\n")