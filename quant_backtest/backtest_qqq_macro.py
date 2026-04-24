import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import platform
from pathlib import Path


# ==========================================
# 1. 路径配置与数据加载
# ==========================================
def get_data_path() -> Path:
    sys_name = platform.system()
    if sys_name == "Darwin":
        # Mac 环境使用你提供的路径 (请确保文件名与你本地真实文件名一致)
        return Path(
            "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_1mo_20260324_to_20260423.csv")  # ← 请替换为你真实的 CSV 文件名
    else:
        return Path("/home/evaseemefly/01data/05-spiders/broad_market_history/your_file_name.csv")


# ⚠️ 请在这里填入你刚刚抓取下来的 30年(12年) 数据的真实文件名
CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_max_20140917_to_20260424.csv")


def calculate_metrics(nav_series: pd.Series, name: str):
    """计算年化收益率和最大回撤"""
    # 年化收益率 (CAGR)
    total_days = len(nav_series)
    cagr = (nav_series.iloc[-1] ** (252 / total_days)) - 1

    # 最大回撤 (Max Drawdown)
    roll_max = nav_series.cummax()
    drawdown = nav_series / roll_max - 1.0
    max_dd = drawdown.min()

    print(f"[{name}] \t 年化收益率: {cagr * 100:.2f}% \t 最大回撤: {max_dd * 100:.2f}%")
    return cagr, max_dd


def main():
    print("=== 🚀 QQQ 中长线量化择时回测引擎启动 ===")

    if not CSV_FILE_PATH.exists():
        print(f"❌ 找不到数据文件: {CSV_FILE_PATH}")
        return

    # 读取数据，设置日期为索引
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc')
    df = df.sort_index()

    # 提取需要的收盘价数据
    price_qqq = df['QQQ_close']
    price_hyg = df['HYG_close']

    # 计算每日底层收益率
    # pct_change() 计算 (今天/昨天 - 1)
    df['QQQ_ret'] = price_qqq.pct_change().fillna(0)

    # ==========================================
    # 第一步：Baseline (一直死拿 QQQ)
    # ==========================================
    df['NAV_Baseline'] = (1 + df['QQQ_ret']).cumprod()

    # ==========================================
    # 第二步：单因子择时 (MA200 趋势跟踪)
    # 规则：QQQ > MA200 时满仓，否则空仓(现金)
    # ==========================================
    df['QQQ_MA200'] = price_qqq.rolling(window=200, min_periods=1).mean()

    # 产生信号：1表示做多，0表示空仓。
    # ⚠️ 必须用 shift(1)，代表"昨天收盘"产生的信号，指导"今天"的交易盈亏！
    df['Signal_MA200'] = (price_qqq > df['QQQ_MA200']).astype(int).shift(1).fillna(0)

    # 计算策略收益率与净值
    df['Ret_MA200'] = df['Signal_MA200'] * df['QQQ_ret']
    df['NAV_MA200'] = (1 + df['Ret_MA200']).cumprod()

    # ==========================================
    # 第三步：宏观多因子 (MA200 + HYG 信用过滤)
    # 规则：QQQ > MA200 且 垃圾债(HYG) > 60日均线 时，才允许做多。否则立刻逃顶。
    # ==========================================
    df['HYG_MA60'] = price_hyg.rolling(window=60, min_periods=1).mean()

    # 双重过滤信号
    cond_trend = price_qqq > df['QQQ_MA200']
    cond_macro_safe = price_hyg > df['HYG_MA60']

    df['Signal_Macro'] = (cond_trend & cond_macro_safe).astype(int).shift(1).fillna(0)

    # 计算宏观策略收益率与净值
    df['Ret_Macro'] = df['Signal_Macro'] * df['QQQ_ret']
    df['NAV_Macro'] = (1 + df['Ret_Macro']).cumprod()

    # ==========================================
    # 打印核心绩效指标
    # ==========================================
    print("\n📊 回测结果 (2014-2026):")
    print("-" * 60)
    calculate_metrics(df['NAV_Baseline'], "大盘基准 (死拿QQQ)")
    calculate_metrics(df['NAV_MA200'], "单因子 (QQQ均线择时)")
    calculate_metrics(df['NAV_Macro'], "多因子 (均线+HYG风控)")
    print("-" * 60)

    # ==========================================
    # 绘制完美的可视化图表
    # ==========================================
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.05)

    # 上图：净值曲线对比
    ax1.plot(df.index, df['NAV_Baseline'], label='Baseline (Buy & Hold QQQ)', color='gray', alpha=0.7, linewidth=1.5)
    ax1.plot(df.index, df['NAV_MA200'], label='Trend Strategy (MA200)', color='#3498db', linewidth=1.5)
    ax1.plot(df.index, df['NAV_Macro'], label='Macro Overlay (MA200 + HYG Filter)', color='#e74c3c', linewidth=2)

    ax1.set_title('Quantitative Backtest: QQQ vs Trend vs Macro Overlay (2014-2026)', fontsize=16, fontweight='bold')
    ax1.set_ylabel('Net Asset Value (Initial = 1)', fontsize=12, fontweight='bold')
    ax1.set_yscale('log')  # 使用对数坐标轴更能看清长期的复利增长
    ax1.legend(loc='upper left', fontsize=11)
    ax1.grid(True, linestyle='--', alpha=0.5)

    # 下图：QQQ 价格走势与 MA200，展示牛熊周期
    ax2.plot(df.index, df['QQQ_close'], label='QQQ Price', color='black', linewidth=1)
    ax2.plot(df.index, df['QQQ_MA200'], label='QQQ MA200', color='orange', linewidth=1.5, linestyle='--')

    # 用红色背景标出宏观策略空仓（避险）的区间
    ax2.fill_between(df.index, ax2.get_ylim()[0], ax2.get_ylim()[1],
                     where=(df['Signal_Macro'] == 0), color='red', alpha=0.1, label='Cash Position (Risk Off)')

    ax2.set_ylabel('QQQ Price', fontsize=12, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=10)
    ax2.grid(True, linestyle='--', alpha=0.5)

    # 保存图表
    output_pic_path = Path(__file__).parent / "backtest_result.png"
    plt.savefig(output_pic_path, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"\n🖼️ 回测图表已保存至: {output_pic_path.name}")


if __name__ == "__main__":
    main()