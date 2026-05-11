import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from pathlib import Path
import arrow
from enum import Enum, auto

# ==================== 0. 修复中文字体显示问题 ====================
# 优先使用 Mac 自带的黑体/苹方，如果未来放到 Windows 上，会自动回退寻找 SimHei
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC', 'Heiti TC', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False  # 确保坐标轴上的负号正常显示


# ===============================================================

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

    config = {
        'ind_stock_dir': base_path / "individual_stocks",
        'report_dir': base_path / "output/reports",
    }
    config['report_dir'].mkdir(parents=True, exist_ok=True)
    return config


CURRENT_ENV = EnvType.WORK
CONFIG = get_env_config(CURRENT_ENV)


# ==================== 2. 核心绘图引擎 ====================
def create_chart_figure(ticker, df, days):
    """为个股生成指定周期的图表，四轴联动 (主图 + MACD + RSI + ATR)"""
    if 'trade_date_utc' in df.columns:
        df = df.set_index('trade_date_utc')
    df.index = pd.to_datetime(df.index)

    plot_df = df.tail(days).copy()
    if len(plot_df) < 20: return None  # 数据太少画图没有意义

    # 创建 4 个子图，调整高度比例 (4:1.5:1:1)，稍微拉高一点画布高度到 10 以防太挤
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(11.69, 10),
                                             gridspec_kw={'height_ratios': [4, 1.5, 1, 1]},
                                             sharex=True)
    # 缩小各个子图之间的纵向间距
    fig.subplots_adjust(hspace=0.05)

    close_col = f'{ticker}_close'

    # ---------------- ax1: 主图 (价格 + 均线 + 布林带) ----------------
    if close_col in plot_df.columns:
        ax1.plot(plot_df.index, plot_df[close_col], label='Price', color='#1f77b4', linewidth=2)

    for ma in ['MA20', 'MA50', 'MA100', 'MA200']:
        if ma in plot_df.columns:
            lw = 2 if ma == 'MA200' else 1
            ls = '-' if ma == 'MA200' else '--'
            ax1.plot(plot_df.index, plot_df[ma], label=ma, linestyle=ls, linewidth=lw, alpha=0.8)

    if 'BB_Lower' in plot_df.columns and 'BB_Upper' in plot_df.columns:
        ax1.fill_between(plot_df.index, plot_df['BB_Lower'], plot_df['BB_Upper'],
                         color='gray', alpha=0.15, label='Bollinger Bands')

    ax1.set_title(f"{ticker} Technical Analysis - Last {days} Days", fontsize=16, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=9, ncol=3)
    ax1.grid(True, linestyle=':', alpha=0.6)

    # ---------------- ax2: MACD ----------------
    if 'MACD_DIF' in plot_df.columns:
        ax2.plot(plot_df.index, plot_df['MACD_DIF'], color='#1f77b4', label='DIF (Fast)', linewidth=1.2)
        ax2.plot(plot_df.index, plot_df['MACD_DEA'], color='#ff7f0e', label='DEA (Slow)', linewidth=1.2)

        # 绘制红绿柱子 (大于0为红色，小于0为绿色)
        colors = ['#d62728' if val > 0 else '#2ca02c' for val in plot_df['MACD_Hist']]
        ax2.bar(plot_df.index, plot_df['MACD_Hist'], color=colors, alpha=0.6, width=0.8, label='MACD Hist')

        ax2.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        ax2.set_ylabel('MACD', fontsize=10)
        ax2.legend(loc='upper left', fontsize=8, ncol=3)
        ax2.grid(True, linestyle=':', alpha=0.6)

    # ---------------- ax3: RSI ----------------
    if 'RSI_14' in plot_df.columns:
        ax3.plot(plot_df.index, plot_df['RSI_14'], color='#9467bd', label='RSI(14)')
        ax3.axhline(y=70, color='#d62728', linestyle='--', alpha=0.5)
        ax3.axhline(y=30, color='#2ca02c', linestyle='--', alpha=0.5)
        ax3.fill_between(plot_df.index, 30, 70, color='#9467bd', alpha=0.05)
        ax3.set_ylim(0, 100)
        ax3.set_ylabel('RSI', fontsize=10)
        ax3.legend(loc='upper left', fontsize=8)
        ax3.grid(True, linestyle=':', alpha=0.6)

    # ---------------- ax4: ATR (真实波动幅度) ----------------
    if 'ATR_14' in plot_df.columns:
        ax4.plot(plot_df.index, plot_df['ATR_14'], color='#8c564b', label='ATR(14)')
        ax4.set_ylabel('ATR', fontsize=10)
        ax4.legend(loc='upper left', fontsize=8)
        ax4.grid(True, linestyle=':', alpha=0.6)

    plt.xticks(rotation=0)
    # plt.tight_layout() # 因为使用了 subplots_adjust，这里关掉 tight_layout 以防布局冲突
    return fig

# ==================== 3. 报告合成逻辑 ====================
def main(target_stocks=None):
    # 增加参数判断：如果外部没传，则使用默认股票池
    if target_stocks is None:
        target_stocks = ['META', 'MSFT', 'NVDA', 'TSLA', 'MU', 'ASML', 'AMZN']
    current_date = arrow.now().format('YYYY_MM_DD')

    report_name = f"Stock_MultiPeriod_Report_{current_date}.pdf"
    output_path = CONFIG['report_dir'] / report_name

    print(f"🚀 开始生成 {current_date} 深度量价分析报告...")

    # 使用 PdfPages 直接将图表写入 PDF
    with PdfPages(output_path) as pdf:

        # 1. 绘制一个精美的 PDF 封面页
        fig_cover = plt.figure(figsize=(11.69, 8.27))  # A4 横向尺寸
        fig_cover.text(0.5, 0.6, "个股多周期深度分析报告", ha='center', va='center', fontsize=32, color='#1f77b4',
                       fontweight='bold')
        fig_cover.text(0.5, 0.5, f"生成日期: {arrow.now().format('YYYY-MM-DD')}", ha='center', va='center', fontsize=16)
        fig_cover.text(0.5, 0.45, "数据来源: Local individual_stocks_master", ha='center', va='center', fontsize=12,
                       color='gray')
        fig_cover.text(0.5, 0.35, "drcc Quant Engine", ha='center', va='center', fontsize=14, fontstyle='italic',
                       color='#777777')
        pdf.savefig(fig_cover)
        plt.close(fig_cover)

        # 2. 遍历个股和周期生成图表页
        for ticker in target_stocks:
            file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
            if not file_path.exists():
                print(f"⚠️ 跳过 {ticker}: 找不到历史数据文件")
                continue

            print(f"📊 正在处理 {ticker}...")
            df = pd.read_csv(file_path)

            for days in [200]:
                # for days in [20, 50, 100, 200]:
                fig = create_chart_figure(ticker, df, days)
                if fig:
                    pdf.savefig(fig)  # 直接将图表保存为 PDF 的一页
                    plt.close(fig)  # 及时释放内存

    print(f"\n✅ 报告生成成功！\n📂 保存路径: {output_path}")


if __name__ == "__main__":
    main()
