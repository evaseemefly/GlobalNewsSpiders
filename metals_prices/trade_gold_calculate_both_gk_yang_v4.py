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
    """计算 Garman-Klass (GK) 波动率 - 对日内连续波动极度敏锐"""
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


# todo 26-04-13: 新增 Yang-Zhang (YZ) 波动率计算引擎
def calculate_yz_volatility(df: pd.DataFrame, window_size: int = 60) -> pd.DataFrame:
    """计算 Yang-Zhang (YZ) 波动率 - 完美捕捉隔夜/周末跳空缺口"""
    df_calc = df.copy()
    eps = np.finfo(float).eps

    # 核心：获取真实的“上一个有效收盘价”，完美跨越周末 48 小时的 NaN 假死数据
    prev_close = df_calc['close'].ffill().shift(1)

    # 1. 计算三组对数收益率
    # 隔夜跳空幅度 (今日开盘 / 昨日收盘)
    log_o_c1 = np.log(df_calc['open'] / (prev_close + eps))
    # 日内波动幅度 (今日收盘 / 今日开盘)
    log_c_o = np.log(df_calc['close'] / (df_calc['open'] + eps))
    # 高低振幅
    log_h_o = np.log(df_calc['high'] / (df_calc['open'] + eps))
    log_l_o = np.log(df_calc['low'] / (df_calc['open'] + eps))

    # 2. 计算 YZ 模型的三个方差分量
    # V_o: 隔夜跳空方差 (rolling var)
    V_o = log_o_c1.rolling(window=window_size, min_periods=2).var()
    # V_c: 日内开收盘方差 (rolling var)
    V_c = log_c_o.rolling(window=window_size, min_periods=2).var()
    # V_rs: 盘中震荡的 Rogers-Satchell 均值
    rs_term = log_h_o * (log_h_o - log_c_o) + log_l_o * (log_l_o - log_c_o)
    V_rs = rs_term.rolling(window=window_size, min_periods=1).mean()

    # 因为 var 在样本数为 1 时返回 NaN，安全起见填补 0
    V_o = V_o.fillna(0)
    V_c = V_c.fillna(0)
    V_rs = V_rs.fillna(0)

    # 3. 计算组合权重系数 k
    n = window_size
    k = 0.34 / (1.34 + (n + 1) / max(1, n - 1))

    # 4. 合成终极 YZ 波动率
    df_calc['YZ_variance'] = V_o + k * V_c + (1 - k) * V_rs

    vol_col_name = f'YZ_volatility_{window_size}m'
    df_calc[vol_col_name] = np.sqrt(df_calc['YZ_variance'])

    # 幽灵波动率熔断机制
    df_calc.loc[df_calc['close'].isna(), vol_col_name] = np.nan

    return df_calc


# todo 26-04-13: 将绘图函数参数化，支持动态接收不同的模型名称（vol_type）
def plot_candlestick_and_volatility(df: pd.DataFrame, window_size: int, output_dir: Path, vol_type: str = 'GK'):
    """绘制动态可视化看板，支持 GK 或 YZ 类型"""
    vol_col = f'{vol_type}_volatility_{window_size}m'
    plot_df = df.copy()

    if plot_df.empty:
        return

    latest_time = plot_df['timestamp_utc'].max()
    start_time = latest_time - pd.Timedelta(days=7)
    plot_df = plot_df[plot_df['timestamp_utc'] >= start_time].copy()

    plot_df = plot_df.dropna(subset=['close']).reset_index(drop=True)

    if plot_df.empty:
        return

    plot_df['MA20'] = plot_df['close'].rolling(window=20, min_periods=1).mean()
    plot_df['MA60'] = plot_df['close'].rolling(window=60, min_periods=1).mean()

    start_str = plot_df['timestamp_utc'].min().strftime('%Y-%m-%d')
    end_str = plot_df['timestamp_utc'].max().strftime('%Y-%m-%d')

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
    plt.subplots_adjust(hspace=0.05)

    x_indices = np.arange(len(plot_df))

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

    # 标题动态显示当前使用的是哪种模型
    ax1.set_title(f'Gold 5m Candlestick & {vol_type} Volatility ({start_str} to {end_str} / Last 7 Days)', fontsize=16,
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

    ax2.fill_between(x_indices, plot_df[vol_col], color='#ff7f0e', alpha=0.3)
    # 图例动态显示
    ax2.plot(x_indices, plot_df[vol_col], color='#d62728', linewidth=1, label=f'{vol_type} Volatility')

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

    step = max(1, len(plot_df) // 10)
    tick_indices = x_indices[::step]
    tick_labels = plot_df['timestamp_utc'].dt.strftime('%m-%d\n%H:%M').iloc[::step]
    ax2.set_xticks(tick_indices)
    ax2.set_xticklabels(tick_labels, rotation=0, ha='center')

    output_dir.mkdir(parents=True, exist_ok=True)

    # 动态命名图片文件：区分 gk 或 yz 前缀
    filename = output_dir / f"{vol_type.lower()}_vol_{window_size}m_{start_str}_to_{end_str}.png"
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close(fig)


# todo 26-04-13: 抽离通用的 CSV 保存逻辑，支持两套体系分别存储
def save_feature_csv(df_processed: pd.DataFrame, window_size: int, feature_path: Path, vol_type: str):
    """通用特征保存器，动态填补对应列的 NaN 并生成对应前缀的 CSV"""
    csv_df = df_processed.copy()
    csv_df['symbol'] = csv_df['symbol'].fillna("XAUUSD")
    csv_df['fetch_time_utc'] = csv_df['fetch_time_utc'].fillna("Missing")

    # 动态填补目标模型的特征列
    cols_to_fill = ['open', 'high', 'low', 'close', f'{vol_type}_variance', f'{vol_type}_volatility_{window_size}m']
    csv_df[cols_to_fill] = csv_df[cols_to_fill].fillna(-999)

    feature_path.mkdir(parents=True, exist_ok=True)
    start_date = csv_df['timestamp_utc'].min().strftime('%Y%m%d')
    end_date = csv_df['timestamp_utc'].max().strftime('%Y%m%d')

    # 文件命名带上模型后缀，例如: gold_features_gk_20260406_to_20260413.csv
    final_output_file = feature_path / f"gold_features_{vol_type.lower()}_{start_date}_to_{end_date}.csv"

    csv_df.to_csv(final_output_file, index=False, encoding='utf-8-sig')
    print(f"💾 {vol_type} 特征 CSV 已独立保存: {final_output_file}")


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

    df_all = df_all.replace(-999.0, np.nan)
    df_all['timestamp_utc'] = df_all['timestamp_utc'].dt.floor('5min')

    is_weekend = (df_all['timestamp_utc'].dt.dayofweek == 5) | \
                 ((df_all['timestamp_utc'].dt.dayofweek == 4) & (df_all['timestamp_utc'].dt.hour >= 21)) | \
                 ((df_all['timestamp_utc'].dt.dayofweek == 6) & (df_all['timestamp_utc'].dt.hour < 22))

    amplitude = df_all['high'] - df_all['low']
    is_dead_feed = amplitude.rolling(window=3, min_periods=1).max() < 0.3

    df_all.loc[is_weekend | is_dead_feed, ['open', 'high', 'low', 'close']] = np.nan

    df_all = df_all.sort_values(['timestamp_utc', 'fetch_time_utc']).drop_duplicates(subset=['timestamp_utc'],
                                                                                     keep='last')
    df_all = df_all.set_index('timestamp_utc')
    df_all = df_all.resample('5min').asfreq().reset_index()

    # ==========================
    # todo 26-04-13: 双分支特征工厂
    # ==========================

    # --- 分支一：执行 GK 处理流程 ---
    print(f"⚙️ [1/2] 正在计算并生成 GK (日内敏感型) 波动率体系...")
    df_gk = calculate_gk_volatility(df_all, window_size=window_size)
    plot_candlestick_and_volatility(df_gk, window_size, pic_path, vol_type='GK')
    save_feature_csv(df_gk, window_size, feature_path, vol_type='GK')

    # --- 分支二：执行 YZ 处理流程 ---
    print(f"⚙️ [2/2] 正在计算并生成 YZ (跳空免疫型) 波动率体系...")
    df_yz = calculate_yz_volatility(df_all, window_size=window_size)
    plot_candlestick_and_volatility(df_yz, window_size, pic_path, vol_type='YZ')
    save_feature_csv(df_yz, window_size, feature_path, vol_type='YZ')

    print("-" * 60)


def main():
    print("=== 量化特征工程：多重波动率双引擎启动 ===")
    # workplace
    # ROOT_PATH: pathlib.Path = pathlib.Path('/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders')
    # home
    ROOT_PATH: pathlib.Path = pathlib.Path('/home/evaseemefly/01data/05-spiders')

    # home-workplace
    # ROOT_PATH: pathlib.Path = pathlib.Path('/home/evaseemefly/01data/05-spiders')

    INPUT_DATA_DIR = ROOT_PATH / "market_prices"
    OUTPUT_FEATURE_DIR = ROOT_PATH / 'output' / "gold_features"
    OUTPUT_PIC_DIR = ROOT_PATH / 'output' / "gold_gk_pics"
    WINDOW = 60

    def job_task():
        from datetime import datetime
        print(f"\n🕒 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 触发定时特征重构任务...")
        process_gold_directory(
            input_dir=str(INPUT_DATA_DIR),
            feature_dir=str(OUTPUT_FEATURE_DIR),
            pic_dir=str(OUTPUT_PIC_DIR),
            window_size=WINDOW
        )

    job_task()

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(job_task, 'interval', minutes=10, id='gk_calc_job')

    print("\n🚀 双引擎定时任务已注册，系统自动运行中 (每 10 分钟生成独立的两套指标)...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n🛑 波动率定时处理模块已安全停止。")


if __name__ == "__main__":
    main()
