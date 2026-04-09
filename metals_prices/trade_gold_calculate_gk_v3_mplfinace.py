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

    log_hl = np.log(df_calc['high'] / (df_calc['low'] + np.finfo(float).eps))
    log_co = np.log(df_calc['close'] / (df_calc['open'] + np.finfo(float).eps))

    df_calc['GK_variance'] = 0.5 * (log_hl ** 2) - constant * (log_co ** 2)

    vol_col_name = f'GK_volatility_{window_size}m'
    df_calc[vol_col_name] = np.sqrt(df_calc['GK_variance'].rolling(window=window_size, min_periods=1).mean())

    # 幽灵波动率熔断机制
    df_calc.loc[df_calc['close'].isna(), vol_col_name] = np.nan

    return df_calc


def plot_candlestick_and_volatility(df: pd.DataFrame, window_size: int, output_dir: Path):
    """绘制最近 7 天的 K 线图和波动率，包含去断层、均线、警戒线及周末剔除"""
    vol_col = f'GK_volatility_{window_size}m'
    plot_df = df.copy()

    if plot_df.empty:
        return

    latest_time = plot_df['timestamp_utc'].max()
    start_time = latest_time - pd.Timedelta(days=7)
    plot_df = plot_df[plot_df['timestamp_utc'] >= start_time].copy()

    # todo 26-04-09: 关键修改 - 彻底剔除周末假死数据，把周五和周一无缝连接
    plot_df = plot_df.dropna(subset=['close']).reset_index(drop=True)

    if plot_df.empty:
        return

    # todo 26-04-09: 视觉优化 2 - 在剔除了周末的“干净连续数组”上计算均线
    # 这样 MA20 就不会被周末的假死数据拉成一条直线了
    plot_df['MA20'] = plot_df['close'].rolling(window=20, min_periods=1).mean()
    plot_df['MA60'] = plot_df['close'].rolling(window=60, min_periods=1).mean()

    start_str = plot_df['timestamp_utc'].min().strftime('%Y-%m-%d')
    end_str = plot_df['timestamp_utc'].max().strftime('%Y-%m-%d')

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
    plt.subplots_adjust(hspace=0.05)

    # todo 26-04-09: 视觉优化 1 - 核心技巧：使用连续的整数序列作为 X 轴避开物理断层
    x_indices = np.arange(len(plot_df))

    # ====================
    # 上图：绘制 K 线图与均线
    # ====================
    up_mask = plot_df['close'] >= plot_df['open']
    down_mask = plot_df['close'] < plot_df['open']

    width = 0.6

    ax1.vlines(x_indices[up_mask], plot_df.loc[up_mask, 'low'], plot_df.loc[up_mask, 'high'], color='red', linewidth=1)
    ax1.vlines(x_indices[down_mask], plot_df.loc[down_mask, 'low'], plot_df.loc[down_mask, 'high'], color='green',
               linewidth=1)

    ax1.bar(x_indices[up_mask], plot_df.loc[up_mask, 'close'] - plot_df.loc[up_mask, 'open'],
            bottom=plot_df.loc[up_mask, 'open'], color='red', width=width)
    ax1.bar(x_indices[down_mask], plot_df.loc[down_mask, 'open'] - plot_df.loc[down_mask, 'close'],
            bottom=plot_df.loc[down_mask, 'close'], color='green', width=width)

    ax1.plot(x_indices, plot_df['MA20'], color='#f39c12', linewidth=1.5, label='MA20', alpha=0.85)
    ax1.plot(x_indices, plot_df['MA60'], color='#3498db', linewidth=1.5, label='MA60', alpha=0.85)

    ax1.set_ylabel('Price (USD)', fontsize=12, fontweight='bold')
    ax1.set_title(f'Gold 5m Candlestick & GK Volatility ({start_str} to {end_str} / Last 7 Days)', fontsize=16,
                  fontweight='bold', pad=20)
    ax1.grid(True, linestyle='--', alpha=0.5)
    ax1.legend(loc='upper left')

    valid_price = plot_df.dropna(subset=['high', 'low'])
    if not valid_price.empty:
        max_idx = valid_price['high'].idxmax()
        max_p = valid_price.loc[max_idx, 'high']
        min_idx = valid_price['low'].idxmin()
        min_p = valid_price.loc[min_idx, 'low']

        ax1.annotate(f'Max: {max_p:.2f}', xy=(max_idx, max_p), xytext=(0, 15), textcoords='offset points',
                     arrowprops=dict(arrowstyle="->", color='darkred'), ha='center', color='darkred', fontweight='bold')
        ax1.annotate(f'Min: {min_p:.2f}', xy=(min_idx, min_p), xytext=(0, -15), textcoords='offset points',
                     arrowprops=dict(arrowstyle="->", color='darkgreen'), ha='center', color='darkgreen',
                     fontweight='bold', va='top')

    # ====================
    # 下图：绘制波动率与警戒线
    # ====================
    ax2.fill_between(x_indices, plot_df[vol_col], color='#ff7f0e', alpha=0.3)
    ax2.plot(x_indices, plot_df[vol_col], color='#d62728', linewidth=1, label='GK Volatility')

    # todo 26-04-09: 视觉优化 3 - 绘制动态警戒线
    vol_mean = plot_df[vol_col].mean()
    vol_std = plot_df[vol_col].std()
    warning_line = vol_mean + 2 * vol_std

    ax2.axhline(warning_line, color='purple', linestyle='-.', linewidth=1.2, label=f'Alert Line (+2 Std)')

    ax2.set_ylabel('Volatility', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Time (UTC)', fontsize=12)
    ax2.grid(True, linestyle='--', alpha=0.5)
    ax2.legend(loc='upper left')

    valid_vol = plot_df.dropna(subset=[vol_col])
    if not valid_vol.empty:
        max_v_idx = valid_vol[vol_col].idxmax()
        max_v = valid_vol.loc[max_v_idx, vol_col]

        ax2.annotate(f'Max: {max_v:.4f}', xy=(max_v_idx, max_v), xytext=(0, 15), textcoords='offset points',
                     arrowprops=dict(arrowstyle="->", color='purple'), ha='center', color='purple', fontweight='bold')

    # ====================
    # 重新贴上日期标签
    # ====================
    step = max(1, len(plot_df) // 10)
    tick_indices = x_indices[::step]
    tick_labels = plot_df['timestamp_utc'].dt.strftime('%m-%d\n%H:%M').iloc[::step]

    ax2.set_xticks(tick_indices)
    ax2.set_xticklabels(tick_labels, rotation=0, ha='center')

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

    # todo 26-04-09: 底层防线 1 - 洗掉历史遗留的 -999.0
    df_all = df_all.replace(-999.0, np.nan)

    # 强制对齐到绝对的 5 分钟网格
    df_all['timestamp_utc'] = df_all['timestamp_utc'].dt.floor('5min')

    # todo 26-04-09: 底层防线 2 - 物理周末熔断 (精准屏蔽周五 21:00 至 周日 22:00 UTC)
    is_weekend = (df_all['timestamp_utc'].dt.dayofweek == 5) | \
                 ((df_all['timestamp_utc'].dt.dayofweek == 4) & (df_all['timestamp_utc'].dt.hour >= 21)) | \
                 ((df_all['timestamp_utc'].dt.dayofweek == 6) & (df_all['timestamp_utc'].dt.hour < 22))

    # todo 26-04-09: 底层防线 3 - 流动性假期熔断 (专杀 Good Friday 和僵尸 API)
    # 黄金在正常交易时段 5 分钟振幅通常大于 1.0 美元。
    # 如果连续 3 根 K 线（15分钟）的最高低点振幅都不超过 0.3 美元，绝对是休市噪音。
    amplitude = df_all['high'] - df_all['low']
    is_dead_feed = amplitude.rolling(window=3, min_periods=1).max() < 0.3

    # 强制将这些假死时段的行情设为 NaN，彻底杀死僵尸数据
    df_all.loc[is_weekend | is_dead_feed, ['open', 'high', 'low', 'close']] = np.nan

    # 去重
    df_all = df_all.sort_values(['timestamp_utc', 'fetch_time_utc']).drop_duplicates(subset=['timestamp_utc'],
                                                                                     keep='last')

    df_all = df_all.set_index('timestamp_utc')
    df_all = df_all.resample('5min').asfreq().reset_index()

    print(f"⚙️ 正在计算 Garman-Klass 波动率...")
    df_processed = calculate_gk_volatility(df_all, window_size=window_size)

    print(f"🎨 正在生成最近 7 天 K 线图与极值标记...")
    # 这里传进去的 df_processed 已经被挖空了假期数据，画图函数中的 dropna 会完美衔接它们
    plot_candlestick_and_volatility(df_processed, window_size, pic_path)

    csv_df = df_processed.copy()
    csv_df['symbol'] = csv_df['symbol'].fillna("XAUUSD")
    csv_df['fetch_time_utc'] = csv_df['fetch_time_utc'].fillna("Missing")

    # 填补空值为 -999，供下一次读取或模型使用
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
    print("=== 量化特征工程：K线重构与视觉增强启动 ===")
    ROOT_PATH: pathlib.Path = pathlib.Path('/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders')

    INPUT_DATA_DIR = ROOT_PATH / "market_prices"
    OUTPUT_FEATURE_DIR = ROOT_PATH / 'output' / "gold_features"
    OUTPUT_PIC_DIR = ROOT_PATH / 'output' / "gold_gk_pics"
    WINDOW = 60

    process_gold_directory(
        input_dir=str(INPUT_DATA_DIR),
        feature_dir=str(OUTPUT_FEATURE_DIR),
        pic_dir=str(OUTPUT_PIC_DIR),
        window_size=WINDOW
    )


if __name__ == "__main__":
    main()
