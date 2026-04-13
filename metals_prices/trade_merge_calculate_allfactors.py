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
    df_calc.loc[df_calc['close'].isna(), vol_col_name] = np.nan
    return df_calc


def calculate_yz_volatility(df: pd.DataFrame, window_size: int = 60) -> pd.DataFrame:
    df_calc = df.copy()
    eps = np.finfo(float).eps
    prev_close = df_calc['close'].ffill().shift(1)

    log_o_c1 = np.log(df_calc['open'] / (prev_close + eps))
    log_c_o = np.log(df_calc['close'] / (df_calc['open'] + eps))
    log_h_o = np.log(df_calc['high'] / (df_calc['open'] + eps))
    log_l_o = np.log(df_calc['low'] / (df_calc['open'] + eps))

    V_o = log_o_c1.rolling(window=window_size, min_periods=2).var().fillna(0)
    V_c = log_c_o.rolling(window=window_size, min_periods=2).var().fillna(0)

    rs_term = log_h_o * (log_h_o - log_c_o) + log_l_o * (log_l_o - log_c_o)
    V_rs = rs_term.rolling(window=window_size, min_periods=1).mean().fillna(0)

    n = window_size
    k = 0.34 / (1.34 + (n + 1) / max(1, n - 1))

    df_calc['YZ_variance'] = V_o + k * V_c + (1 - k) * V_rs
    vol_col_name = f'YZ_volatility_{window_size}m'
    df_calc[vol_col_name] = np.sqrt(df_calc['YZ_variance'])
    df_calc.loc[df_calc['close'].isna(), vol_col_name] = np.nan
    return df_calc


# todo 26-04-15: 新增真实宏观数据加载器
def load_real_macro_data(macro_dir: str) -> pd.DataFrame:
    """从真实目录加载并清洗所有宏观数据 CSV"""
    macro_path = Path(macro_dir)
    if not macro_path.exists():
        print(f"⚠️ 找不到宏观数据目录: {macro_dir}")
        return pd.DataFrame()

    csv_files = sorted(list(macro_path.glob("macro_daily_*.csv")))
    if not csv_files:
        print(f"⚠️ {macro_dir} 目录下没有找到宏观 CSV 文件")
        return pd.DataFrame()

    print(f"📡 正在加载 {len(csv_files)} 个真实宏观数据文件...")
    df_list = [pd.read_csv(file) for file in csv_files]
    df_macro = pd.concat(df_list, ignore_index=True)

    # 规范化列名和时间格式
    if 'datetime_utc' in df_macro.columns:
        df_macro = df_macro.rename(columns={'datetime_utc': 'timestamp_utc'})

    df_macro['timestamp_utc'] = pd.to_datetime(df_macro['timestamp_utc'])
    df_macro = df_macro.sort_values('timestamp_utc').reset_index(drop=True)

    return df_macro


def plot_candlestick_and_volatility(df: pd.DataFrame, window_size: int, output_dir: Path, vol_type: str = 'GK'):
    vol_col = f'{vol_type}_volatility_{window_size}m'
    plot_df = df.copy()

    if plot_df.empty: return

    latest_time = plot_df['timestamp_utc'].max()
    start_time = latest_time - pd.Timedelta(days=7)
    plot_df = plot_df[plot_df['timestamp_utc'] >= start_time].copy()
    plot_df = plot_df.dropna(subset=['close']).reset_index(drop=True)

    if plot_df.empty: return

    plot_df['MA20'] = plot_df['close'].rolling(window=20, min_periods=1).mean()
    plot_df['MA60'] = plot_df['close'].rolling(window=60, min_periods=1).mean()

    start_str = plot_df['timestamp_utc'].min().strftime('%Y-%m-%d')
    end_str = plot_df['timestamp_utc'].max().strftime('%Y-%m-%d')

    fig, axes = plt.subplots(4, 1, figsize=(16, 16), sharex=True, gridspec_kw={'height_ratios': [3, 1, 1, 1]})
    plt.subplots_adjust(hspace=0.08)
    ax1, ax2, ax3, ax4 = axes

    x_indices = np.arange(len(plot_df))

    # --- 1. 黄金主图 ---
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

    ax1.set_ylabel('Gold Price', fontsize=12, fontweight='bold')
    ax1.set_title(f'Gold 5m Quant Dashboard with Macro Anchors & {vol_type} Volatility ({start_str} to {end_str})',
                  fontsize=16, fontweight='bold', pad=20)
    ax1.grid(True, linestyle='--', alpha=0.5)
    ax1.legend(loc='upper left')

    # --- 2. 波动率图 ---
    ax2.fill_between(x_indices, plot_df[vol_col], color='#ff7f0e', alpha=0.3)
    ax2.plot(x_indices, plot_df[vol_col], color='#d62728', linewidth=1, label=f'{vol_type} Volatility')

    vol_mean = plot_df[vol_col].mean()
    vol_std = plot_df[vol_col].std()
    warning_line = vol_mean + 2 * vol_std

    ax2.axhline(warning_line, color='purple', linestyle='-.', linewidth=1.2, label=f'Alert Line (+2 Std)')
    ax2.set_ylabel('Volatility', fontsize=12, fontweight='bold')
    ax2.grid(True, linestyle='--', alpha=0.5)
    ax2.legend(loc='upper left')

    # --- 3. 美元指数 (DXY) ---
    if 'DXY' in plot_df.columns and not plot_df['DXY'].isna().all():
        ax3.plot(x_indices, plot_df['DXY'], color='#2ca02c', linewidth=1.5, label='US Dollar Index (DXY)')
    ax3.set_ylabel('DXY', fontsize=12, fontweight='bold')
    ax3.grid(True, linestyle='--', alpha=0.5)
    ax3.legend(loc='upper left')

    # --- 4. 10年期美债 (US10Y) ---
    if 'US10Y' in plot_df.columns and not plot_df['US10Y'].isna().all():
        ax4.plot(x_indices, plot_df['US10Y'], color='#9467bd', linewidth=1.5, label='US 10-Year Treasury Yield (%)')
    ax4.set_ylabel('US10Y (%)', fontsize=12, fontweight='bold')
    ax4.set_xlabel('Time (UTC)', fontsize=12)
    ax4.grid(True, linestyle='--', alpha=0.5)
    ax4.legend(loc='upper left')

    step = max(1, len(plot_df) // 10)
    tick_indices = x_indices[::step]
    tick_labels = plot_df['timestamp_utc'].dt.strftime('%m-%d\n%H:%M').iloc[::step]
    ax4.set_xticks(tick_indices)
    ax4.set_xticklabels(tick_labels, rotation=0, ha='center')

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = output_dir / f"{vol_type.lower()}_macro_dashboard_{window_size}m_{start_str}_to_{end_str}.png"
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close(fig)


def save_feature_csv(df_processed: pd.DataFrame, window_size: int, feature_path: Path, vol_type: str):
    csv_df = df_processed.copy()
    csv_df['symbol'] = csv_df['symbol'].fillna("XAUUSD")
    csv_df['fetch_time_utc'] = csv_df['fetch_time_utc'].fillna("Missing")

    # 动态检查是否存在宏观列，如果存在则将其也填补为 -999，否则不管它
    cols_to_fill = ['open', 'high', 'low', 'close', f'{vol_type}_variance', f'{vol_type}_volatility_{window_size}m']
    if 'DXY' in csv_df.columns: cols_to_fill.append('DXY')
    if 'US10Y' in csv_df.columns: cols_to_fill.append('US10Y')

    csv_df[cols_to_fill] = csv_df[cols_to_fill].fillna(-999)

    feature_path.mkdir(parents=True, exist_ok=True)
    start_date = csv_df['timestamp_utc'].min().strftime('%Y%m%d')
    end_date = csv_df['timestamp_utc'].max().strftime('%Y%m%d')
    final_output_file = feature_path / f"gold_features_{vol_type.lower()}_{start_date}_to_{end_date}.csv"

    csv_df.to_csv(final_output_file, index=False, encoding='utf-8-sig')
    print(f"💾 {vol_type} 特征 CSV 已独立保存: {final_output_file}")


def process_gold_directory(input_dir: str, feature_dir: str, pic_dir: str, macro_dir: str, window_size: int = 60):
    input_path = Path(input_dir)
    feature_path = Path(feature_dir)
    pic_path = Path(pic_dir)
    # /Users/evaseemefly/03data/05-spiders/market_prices
    if not input_path.exists(): return
    csv_files = sorted(list(input_path.glob("precious_metals_*.csv")))
    if not csv_files: return

    print(f"📥 正在合并黄金数据进行时间序列对齐...")
    df_list = [pd.read_csv(file) for file in csv_files]
    df_all = pd.concat(df_list, ignore_index=True)
    df_all['timestamp_utc'] = pd.to_datetime(df_all['timestamp_utc'])

    df_all = df_all.replace(-999.0, np.nan)
    df_all['timestamp_utc'] = df_all['timestamp_utc'].dt.floor('5min')

    df_all = df_all.sort_values(['timestamp_utc', 'fetch_time_utc']).drop_duplicates(subset=['timestamp_utc'],
                                                                                     keep='last')

    # todo 26-04-15: 引入真实宏观数据，执行分离纯净合并
    df_macro = load_real_macro_data(macro_dir)

    if not df_macro.empty:
        print("🔗 正在将真实 DXY 和 US10Y 宏观数据对齐到黄金 5 分钟网格...")
        # 1. 拆分并剔除各自的 NaN，确保 merge_asof 能找到最后一个“真实有效”的数值
        df_dxy = df_macro[['timestamp_utc', 'DXY']].dropna().sort_values('timestamp_utc')
        df_us10y = df_macro[['timestamp_utc', 'US10Y']].dropna().sort_values('timestamp_utc')

        # 2. 依次向后查找最近的有效值并拼接，绝对杜绝未来函数
        df_all = pd.merge_asof(df_all, df_dxy, on='timestamp_utc', direction='backward')
        df_all = pd.merge_asof(df_all, df_us10y, on='timestamp_utc', direction='backward')
    else:
        # 容错：如果没有宏观数据，生成空列保证画图不出错
        df_all['DXY'] = np.nan
        df_all['US10Y'] = np.nan

    is_weekend = (df_all['timestamp_utc'].dt.dayofweek == 5) | \
                 ((df_all['timestamp_utc'].dt.dayofweek == 4) & (df_all['timestamp_utc'].dt.hour >= 21)) | \
                 ((df_all['timestamp_utc'].dt.dayofweek == 6) & (df_all['timestamp_utc'].dt.hour < 22))

    amplitude = df_all['high'] - df_all['low']
    is_dead_feed = amplitude.rolling(window=3, min_periods=1).max() < 0.3

    df_all.loc[is_weekend | is_dead_feed, ['open', 'high', 'low', 'close']] = np.nan

    df_all = df_all.set_index('timestamp_utc')
    df_all = df_all.resample('5min').asfreq().reset_index()

    print(f"⚙️ [1/2] 正在生成 GK 看板...")
    df_gk = calculate_gk_volatility(df_all, window_size=window_size)
    plot_candlestick_and_volatility(df_gk, window_size, pic_path, vol_type='GK')
    save_feature_csv(df_gk, window_size, feature_path, vol_type='GK')

    print(f"⚙️ [2/2] 正在生成 YZ 看板...")
    df_yz = calculate_yz_volatility(df_all, window_size=window_size)
    plot_candlestick_and_volatility(df_yz, window_size, pic_path, vol_type='YZ')
    save_feature_csv(df_yz, window_size, feature_path, vol_type='YZ')

    print("-" * 60)


def main():
    print("=== 量化特征工程：多重波动率 + 真实宏观锚点双引擎启动 ===")
    # workplace
    # ROOT_PATH: pathlib.Path = pathlib.Path('/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders')
    # home-mac
    ROOT_PATH: pathlib.Path = pathlib.Path('/Users/evaseemefly/03data/05-spiders/')

    # home-workplace
    # ROOT_PATH: pathlib.Path = pathlib.Path('/home/evaseemefly/01data/05-spiders')

    INPUT_DATA_DIR = ROOT_PATH / "market_prices"
    OUTPUT_FEATURE_DIR = ROOT_PATH / 'output' / "gold_features"
    OUTPUT_PIC_DIR = ROOT_PATH / 'output' / "gold_gk_pics"
    # todo 26-04-15: 新增真实宏观数据路径配置
    MACRO_DATA_DIR = ROOT_PATH / "macro_data"

    WINDOW = 60

    def job_task():
        from datetime import datetime
        print(f"\n🕒 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 触发定时特征重构任务...")
        process_gold_directory(
            input_dir=str(INPUT_DATA_DIR),
            feature_dir=str(OUTPUT_FEATURE_DIR),
            pic_dir=str(OUTPUT_PIC_DIR),
            macro_dir=str(MACRO_DATA_DIR),
            window_size=WINDOW
        )

    job_task()

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(job_task, 'interval', minutes=10, id='gk_calc_job')

    print("\n🚀 宏观双引擎定时任务已注册，系统自动运行中...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n🛑 波动率定时处理模块已安全停止。")


if __name__ == "__main__":
    main()
