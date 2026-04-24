import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# ==================== 配置 ====================
data_path = '/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_max_20140917_to_20260424.csv'
figures = Path('figures')
figures.mkdir(exist_ok=True)

# ==================== 加载数据 ====================
df = pd.read_csv(data_path)
df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
df = df.set_index('trade_date_utc').sort_index()

qqq = df['QQQ_close'].dropna()
returns = qqq.pct_change().dropna()

# ==================== Baseline：全仓持有 ====================
position = pd.Series(1.0, index=returns.index)
strategy_returns = returns * position
cum_returns = (1 + strategy_returns).cumprod()


# ==================== 绩效指标 ====================
def calc_performance(cum_ret, name="Strategy"):
    total_ret = cum_ret.iloc[-1] - 1
    days = (cum_ret.index[-1] - cum_ret.index[0]).days
    years = days / 365.25
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0

    peak = cum_ret.cummax()
    drawdown = (cum_ret - peak) / peak
    max_dd = drawdown.min()

    print(f"\n=== {name} ===")
    print(f"起始日期: {cum_ret.index[0].date()}")
    print(f"结束日期: {cum_ret.index[-1].date()}")
    print(f"总收益率: {total_ret:.2%}")
    print(f"年化收益率 (CAGR): {cagr:.2%}")
    print(f"最大回撤: {max_dd:.2%}")
    return max_dd


baseline_dd = calc_performance(cum_returns, "Baseline - 全仓持有 QQQ")

# ==================== 绘图 ====================
plt.figure(figsize=(14, 8))
plt.plot(cum_returns, label='Baseline (QQQ Buy & Hold)', linewidth=2)
plt.plot(qqq / qqq.iloc[0], label='QQQ 价格 (归一化)', linestyle='--', alpha=0.7)
plt.title('Step 1: Baseline - 全仓持有 QQQ vs 价格')
plt.ylabel('累计净值 / 归一化价格')
plt.xlabel('日期')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(figures / '01_baseline_equity.png', dpi=200, bbox_inches='tight')
plt.close()
print(f"\n✅ 图表已保存: figures/01_baseline_equity.png")