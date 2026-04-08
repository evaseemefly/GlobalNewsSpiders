import pathlib

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler

import matplotlib

matplotlib.use('Agg')


def calculate_gk_volatility(df: pd.DataFrame, window_size: int = 60) -> pd.DataFrame:
    df_calc = df.copy()
    constant = 2 * np.log(2) - 1

    # Pandas 的运算会自动忽略 NaN，所以含有缺省值的行算出的波动率也是 NaN，完全符合要求
    log_hl = np.log(df_calc['high'] / (df_calc['low'] + np.finfo(float).eps))
    log_co = np.log(df_calc['close'] / (df_calc['open'] + np.finfo(float).eps))

    df_calc['GK_variance'] = 0.5 * (log_hl ** 2) - constant * (log_co ** 2)

    # min_periods=1 确保只要窗口内有有效数据就能计算，遇到大段缺失会自动断开
    vol_col_name = f'GK_volatility_{window_size}m'
    df_calc[vol_col_name] = np.sqrt(df_calc['GK_variance'].rolling(window=window_size, min_periods=1).mean())

    return df_calc


def plot_candlestick_and_volatility(df: pd.DataFrame, window_size: int, output_dir: Path):
    """绘制最近 7 天的 K 线图和波动率，并标记极值"""
    vol_col = f'GK_volatility_{window_size}m'
    plot_df = df.copy()

    if plot_df.empty:
        return

    # todo 26-04-03: 限制只绘制最近 7 天的数据
    latest_time = plot_df['timestamp_utc'].max()
    start_time = latest_time - pd.Timedelta(days=7)
    plot_df = plot_df[plot_df['timestamp_utc'] >= start_time].copy()

    start_str = plot_df['timestamp_utc'].min().strftime('%Y-%m-%d')
    end_str = plot_df['timestamp_utc'].max().strftime('%Y-%m-%d')

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
    plt.subplots_adjust(hspace=0.05)

    # ====================
    # 上图：绘制 K 线图
    # ====================
    # 按照中国股市习惯：阳线(收>=开)为红色，阴线(收<开)为绿色
    up = plot_df[plot_df['close'] >= plot_df['open']]
    down = plot_df[plot_df['close'] < plot_df['open']]

    # K 线宽度设定 (5分钟在时间轴上非常窄，0.0025 约等于图上的像素宽度)
    width = 0.0025

    # 画上下影线
    ax1.vlines(up['timestamp_utc'], up['low'], up['high'], color='red', linewidth=1)
    ax1.vlines(down['timestamp_utc'], down['low'], down['high'], color='green', linewidth=1)
    # 画实体
    ax1.bar(up['timestamp_utc'], up['close'] - up['open'], bottom=up['open'], color='red', width=width)
    ax1.bar(down['timestamp_utc'], down['open'] - down['close'], bottom=down['close'], color='green', width=width)

    ax1.set_ylabel('Price (USD)', fontsize=12, fontweight='bold')
    ax1.set_title(f'Gold 5m Candlestick & GK Volatility ({start_str} to {end_str} / Last 7 Days)', fontsize=16,
                  fontweight='bold', pad=20)
    ax1.grid(True, linestyle='--', alpha=0.5)

    # todo 26-04-03: 标记价格极大值和极小值
    valid_price = plot_df.dropna(subset=['high', 'low'])
    if not valid_price.empty:
        max_p = valid_price['high'].max()
        max_t = valid_price.loc[valid_price['high'].idxmax(), 'timestamp_utc']
        min_p = valid_price['low'].min()
        min_t = valid_price.loc[valid_price['low'].idxmin(), 'timestamp_utc']

        ax1.annotate(f'Max: {max_p:.2f}', xy=(max_t, max_p), xytext=(0, 15), textcoords='offset points',
                     arrowprops=dict(arrowstyle="->", color='darkred'), ha='center', color='darkred', fontweight='bold')
        ax1.annotate(f'Min: {min_p:.2f}', xy=(min_t, min_p), xytext=(0, -15), textcoords='offset points',
                     arrowprops=dict(arrowstyle="->", color='darkgreen'), ha='center', color='darkgreen',
                     fontweight='bold', va='top')

    # ====================
    # 下图：绘制波动率
    # ====================
    ax2.fill_between(plot_df['timestamp_utc'], plot_df[vol_col], color='#ff7f0e', alpha=0.3)
    ax2.plot(plot_df['timestamp_utc'], plot_df[vol_col], color='#d62728', linewidth=1)
    ax2.set_ylabel('Volatility', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Time (UTC)', fontsize=12)
    ax2.grid(True, linestyle='--', alpha=0.5)

    # todo 26-04-03: 标记波动率极大值和极小值
    valid_vol = plot_df.dropna(subset=[vol_col])
    if not valid_vol.empty:
        max_v = valid_vol[vol_col].max()
        max_vt = valid_vol.loc[valid_vol[vol_col].idxmax(), 'timestamp_utc']
        min_v = valid_vol[vol_col].min()
        min_vt = valid_vol.loc[valid_vol[vol_col].idxmin(), 'timestamp_utc']

        ax2.annotate(f'Max: {max_v:.4f}', xy=(max_vt, max_v), xytext=(0, 15), textcoords='offset points',
                     arrowprops=dict(arrowstyle="->", color='purple'), ha='center', color='purple', fontweight='bold')

    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d\n%H:%M'))
    plt.xticks(rotation=0, ha='center')

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = output_dir / f"gk_vol_{window_size}m_latest_7days.png"
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close(fig)


def process_gold_directory(input_dir: str, feature_dir: str, pic_dir: str, window_size: int = 60):
    input_path = Path(input_dir)
    feature_path = Path(feature_dir)
    pic_path = Path(pic_dir)

    if not input_path.exists():
        return

    csv_files = sorted(list(input_path.glob("precious_metals_*.csv")))
    if not csv_files:
        return

    print(f"📥 正在合并数据进行时间序列对齐...")
    df_list = [pd.read_csv(file) for file in csv_files]
    df_all = pd.concat(df_list, ignore_index=True)
    df_all['timestamp_utc'] = pd.to_datetime(df_all['timestamp_utc'])

    # 去重
    df_all = df_all.sort_values('timestamp_utc').drop_duplicates(subset=['timestamp_utc'])

    # todo 26-04-03: 强制重采样为严丝合缝的 5 分钟网格
    # 这一步极其关键：它会自动发现并填充那些因为网络问题断掉的 5 分钟时刻，并产生 NaN，从而在画图时形成完美的空白断层
    df_all = df_all.set_index('timestamp_utc')
    df_all = df_all.resample('5min').asfreq().reset_index()

    # 计算 GK 特征 (基于带有 NaN 的数据算，保证数学正确性)
    print(f"⚙️ 正在计算 Garman-Klass 波动率...")
    df_processed = calculate_gk_volatility(df_all, window_size=window_size)

    # 绘图 (传给绘图函数的数据必须保留 NaN，以便画出断层)
    print(f"🎨 正在生成最近 7 天 K 线图与极值标记...")
    plot_candlestick_and_volatility(df_processed, window_size, pic_path)

    # todo 26-04-03: 满足存储为缺省值 -999 的要求
    csv_df = df_processed.copy()
    csv_df['symbol'] = csv_df['symbol'].fillna("XAUUSD")
    csv_df['fetch_time_utc'] = csv_df['fetch_time_utc'].fillna("Missing")

    # 填补 OHLC 和特征空值为 -999
    cols_to_fill = ['open', 'high', 'low', 'close', 'GK_variance', f'GK_volatility_{window_size}m']
    csv_df[cols_to_fill] = csv_df[cols_to_fill].fillna(-999)

    feature_path.mkdir(parents=True, exist_ok=True)
    start_date = csv_df['timestamp_utc'].min().strftime('%Y%m%d')
    end_date = csv_df['timestamp_utc'].max().strftime('%Y%m%d')
    final_output_file = feature_path / f"gold_features_{start_date}_to_{end_date}.csv"

    csv_df.to_csv(final_output_file, index=False, encoding='utf-8-sig')
    print(f"💾 聚合特征 CSV 已保存，缺失值已填充为 -999: {final_output_file}")
    print("-" * 60)


def main():
    print("=== 量化特征工程：K线重构与极值标记启动 ===")
    ROOT_PATH: pathlib.Path = pathlib.Path('/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders')

    INPUT_DATA_DIR = ROOT_PATH / "market_prices"
    OUTPUT_FEATURE_DIR = ROOT_PATH / 'output' / "gold_features"
    OUTPUT_PIC_DIR = ROOT_PATH / 'output' / "gold_gk_pics"
    WINDOW = 60

    # def job_task():
    #     from datetime import datetime
    #     print(f"\n🕒 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 触发定时更新任务...")
    #     process_gold_directory(
    #         input_dir=str(INPUT_DATA_DIR),
    #         feature_dir=str(OUTPUT_FEATURE_DIR),
    #         pic_dir=str(OUTPUT_PIC_DIR),
    #         window_size=WINDOW
    #     )
    #
    # job_task()

    process_gold_directory(
        input_dir=str(INPUT_DATA_DIR),
        feature_dir=str(OUTPUT_FEATURE_DIR),
        pic_dir=str(OUTPUT_PIC_DIR),
        window_size=WINDOW
    )

    # scheduler = BlockingScheduler(timezone="UTC")
    # scheduler.add_job(job_task, 'interval', minutes=10, id='gk_calc_job')
    #
    # try:
    #     scheduler.start()
    # except (KeyboardInterrupt, SystemExit):
    #     print("\n🛑 模块已安全停止。")


if __name__ == "__main__":
    main()
