import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import arrow
import base64
from io import BytesIO
from enum import Enum, auto
from weasyprint import HTML


# ==================== 1. 环境配置 ====================
# #todo: 26-05-09 独立报告生成脚本配置
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
# #todo: 26-05-09 定义 10/20/50/100/200 多周期绘图逻辑
def plot_to_base64(ticker, df, days):
    """为个股生成指定周期的图表并返回 Base64 字符串用于 PDF 嵌入"""
    plot_df = df.tail(days).copy()
    if len(plot_df) < 5: return None

    plt.figure(figsize=(10, 6))
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 7), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)

    close_col = f'{ticker}_close'
    # 主图：价格 + 均线 + 布林带
    ax1.plot(plot_df.index, plot_df[close_col], label='Price', color='#1f77b4', linewidth=2)
    for ma in ['MA20', 'MA50', 'MA100', 'MA200']:
        if ma in plot_df.columns:
            lw = 2 if ma == 'MA200' else 1
            ls = '-' if ma == 'MA200' else '--'
            ax1.plot(plot_df.index, plot_df[ma], label=ma, linestyle=ls, linewidth=lw, alpha=0.8)

    if 'BB_Lower' in plot_df.columns and 'BB_Upper' in plot_df.columns:
        ax1.fill_between(plot_df.index, plot_df['BB_Lower'], plot_df['BB_Upper'], color='gray', alpha=0.1)

    ax1.set_title(f"{ticker} - Last {days} Days Analysis", fontsize=14)
    ax1.legend(loc='upper left', fontsize=8, ncol=2)
    ax1.grid(True, alpha=0.2)

    # 副图：RSI
    if 'RSI_14' in plot_df.columns:
        ax2.plot(plot_df.index, plot_df['RSI_14'], color='#9467bd', label='RSI(14)')
        ax2.axhline(y=70, color='red', linestyle='--', alpha=0.4)
        ax2.axhline(y=30, color='green', linestyle='--', alpha=0.4)
        ax2.set_ylim(0, 100)
        ax2.legend(loc='upper left', fontsize=8)
        ax2.grid(True, alpha=0.2)

    plt.tight_layout()

    # 转换为 Base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=120)
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode('utf-8')


# ==================== 3. 报告合成逻辑 ====================
def main():
    target_stocks = ['META', 'MSFT', 'NVDA', 'TSLA', 'MU', 'ASML', 'AMZN']
    current_date = arrow.now().format('YYYY-MM-DD')

    print(f"🚀 开始生成 {current_date} 深度量价分析报告...")

    html_content = f"""
    <html>
    <head>
        <style>
            @page {{ size: A4; margin: 15mm; background-color: #ffffff; }}
            body {{ font-family: 'Helvetica', sans-serif; color: #333; margin: 0; padding: 0; }}
            .header {{ text-align: center; border-bottom: 2px solid #1f77b4; padding-bottom: 10px; margin-bottom: 20px; }}
            .stock-section {{ page-break-after: always; }}
            .ticker-title {{ color: #1f77b4; font-size: 24pt; margin-top: 0; }}
            .chart-grid {{ display: block; }}
            .chart-box {{ margin-bottom: 15px; text-align: center; }}
            img {{ max-width: 100%; height: auto; border: 1px solid #eee; }}
            .footer {{ font-size: 9pt; color: #777; text-align: right; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>个股多周期深度分析报告</h1>
            <p>生成日期: {current_date} | 数据来源: Local individual_stocks_master</p>
        </div>
    """

    for ticker in target_stocks:
        file_path = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
        if not file_path.exists():
            print(f"⚠️ 跳过 {ticker}: 文件不存在")
            continue

        print(f"📊 正在处理 {ticker}...")
        df = pd.read_csv(file_path, index_name='trade_date_utc')

        html_content += f'<div class="stock-section"><h2 class="ticker-title">{ticker} 趋势矩阵</h2>'

        # todo: 26-05-09 遍历 5 个周期生成图表并嵌入 HTML
        for days in [10, 20, 50, 100, 200]:
            img_b64 = plot_to_base64(ticker, df, days)
            if img_b64:
                html_content += f"""
                <div class="chart-box">
                    <img src="data:image/png;base64,{img_b64}">
                </div>"""

        html_content += '<div class="footer">Gemini Quant Engine - 仅供内部参考</div></div>'

    html_content += "</body></html>"

    # 导出 PDF
    report_name = f"Stock_MultiPeriod_Report_{arrow.now().format('YYYY_MM_DD')}.pdf"
    output_path = CONFIG['report_dir'] / report_name
    HTML(string=html_content).write_pdf(output_path)

    print(f"\n✅ 报告生成成功！\n📂 保存路径: {output_path}")


if __name__ == "__main__":
    main()