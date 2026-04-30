"""
【金价量化特征工程主脚本 v7 (防极端异常值版)】
功能：
  - 读取 market_prices 目录下所有 precious_metals_*.csv 文件
  - 进行时间对齐、缺失值处理、周末停盘平滑延续
  - 执行强力数据清洗，剔除 <=0, -999 及极其不合理的极小值（防止波动率异常飙升）
  - 计算两种高级波动率：Garman-Klass (GK) 和 Yang-Zhang (YZ)
  - 生成过去7天的 K线 + 波动率看板图 (彻底修复向下扎针与波动率异常跃迁 bug)
  - 保存两套独立的特征 CSV（GK 和 YZ）
"""

import pathlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler

import matplotlib
# 强制使用 Agg 后端，确保在无桌面的 Linux 服务器上也能正常画图
matplotlib.use('Agg')

# ⚠️ 既然图表内的文字(Title/Label)全是英文，直接注释掉这部分，避免 Linux 服务器疯狂报找不到字体的警告
# plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Heiti TC', 'STHeiti']
# plt.rcParams['axes.unicode_minus'] = False


# ===================================================================
# 1. 坏数据清洗函数 (v7 核心升级)
# ===================================================================
def clean_bad_price_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    升级版清洗引擎：无情抹除一切 <=0, -999.0 以及不合理的极小值。
    新增：针对黄金(XAUUSD)，设定硬性下限 $1000，过滤引发波动率异常飙升的极小值。
    """
    df = df.copy()
    price_cols = ['open', 'high', 'low', 'close']

    print("🧹 执行高级坏数据清洗（强制剔除 -999, 0 及不合理极小值）...")

    # 1. 强行将恶劣数据转为 NaN
    for col in price_cols:
        # 判定条件：<= 0 或 == -999.0 或 > 20000(过高) 或 < 1000(对于黄金来说过低的离谱值)
        # 注意：如果你处理的资产价格较低（如白银），请务必调整这里的下限 1000！
        bad_mask = (df[col] <= 0) | (df[col] == -999.0) | (df[col] > 20000) | (df[col] < 1000)

        bad_count = bad_mask.sum()
        if bad_count > 0:
             print(f"   发现 {bad_count} 个异常离谱值将被剔除 → 列: {col}")

        df.loc[bad_mask, col] = np.nan

    # 2. 如果某一行4个价格全变成 NaN 了，直接抛弃
    df = df.dropna(subset=price_cols, how='all')

    # 3. 局部断裂修复：使用 ffill (前向填充) 把前一分钟的价格拉平过来
    if 'timestamp_utc' in df.columns:
        df = df.sort_values('timestamp_utc')
    elif df.index.name == 'timestamp_utc':
        df = df.sort_index()

    df[price_cols] = df[price_cols].ffill().bfill()

    print(f"✅ 清洗完成，已过滤异常跳空数据，剩余记录数: {len(df)}")
    return df


# ===================================================================
# 2. 波动率计算引擎
# ===================================================================
def calculate_gk_volatility(df: pd.DataFrame, window_size: int = 60) -> pd.DataFrame:
    """计算 Garman-Klass (GK) 波动率"""
    df_calc = df.copy()
    constant = 2 * np.log(2) - 1

    log_hl = np.log(df_calc['high'] / (df_calc['low'] + np.finfo(float).eps))
    log_co = np.log(df_calc['close'] / (df_calc['open'] + np.finfo(float).eps))

    df_calc['GK_variance'] = 0.5 * (log_hl ** 2) - constant * (log_co ** 2)

    vol_col_name = f'GK_volatility_{window_size}m'
    # 确保平方根内不出现负数
    df_calc[vol_col_name] = np.sqrt(np.maximum(df_calc['GK_variance'].rolling(window=window_size, min_periods=1).mean(), 0))

    return df_calc

def calculate_yz_volatility(df: pd.DataFrame, window_size: int = 60) -> pd.DataFrame:
    """计算 Yang-Zhang (YZ) 波动率"""
    df_calc = df.copy()
    eps = np.finfo(float).eps

    prev_close = df_calc['close'].ffill().shift(1)

    log_o_c1 = np.log(df_calc['open'] / (prev_close + eps))
    log_c_o = np.log(df_calc['close'] / (df_calc['open'] + eps))
    log_h_o = np.log(df_calc['high'] / (df_calc['open'] + eps))
    log_l_o = np.log(df_calc['low'] / (df_calc['open'] + eps))

    V_o = log_o_c1.rolling(window=window_size, min_periods=2).var()
    V_c = log_c_o.rolling(window=window_size, min_periods=2).var()
    rs_term = log_h_o * (log_h_o - log_c_o) + log_l_o * (log_l_o - log_c_o)
    V_rs = rs_term.rolling(window=window_size, min_periods=1).mean()

    V_o = V_o.fillna(0)
    V_c = V_c.fillna(0)
    V_rs = V_rs.fillna(0)

    n = window_size
    k = 0.34 / (1.34 + (n + 1) / max(1, n - 1))

    df_calc['YZ_variance'] = V_o + k * V_c + (1 - k) * V_rs

    vol_col_name = f'YZ_volatility_{window_size}m'
    df_calc['YZ_variance'] = np.maximum(df_calc['YZ_variance'], 0)
    df_calc[vol_col_name] = np.sqrt(df_calc['YZ_variance'])

    return df_calc


# ===================================================================
# 3. 动态可视化与特征落盘
# ===================================================================
def plot_candlestick_and_volatility(df: pd.DataFrame, window_size: int, output_dir: Path, vol_type: str = 'GK'):
    """绘制动态可视化看板"""
    vol_col = f'{vol_type}_volatility_{window_size}m'
    plot_df = df.copy()

    if plot_df.empty:
        return

    latest_time = plot_df['timestamp_utc'].max()
    start_time = latest_time - pd.Timedelta(days=7)
    plot_df = plot_df[plot_df['timestamp_utc'] >= start_time].copy()

    # 二次保险：绘图前再次剔除任何可能导致画图崩溃的异常价格
    plot_df = plot_df[(plot_df['close'] > 1000) & (plot_df['close'] != -999.0)]
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

    # --- 上半部分：K线与均线 ---
    up_mask = plot_df['close'] >= plot_df['open']
    down_mask = plot_df['close'] < plot_df['open']
    width = 0.6

    ax1.vlines(x_indices[up_mask], plot_df.loc[up_mask, 'low'], plot_df.loc[up_mask, 'high'], color='red', linewidth=1)
    ax1.vlines(x_indices[down_mask], plot_df.loc[down_mask, 'low'], plot_df.loc[down_mask, 'high'], color='green', linewidth=1)
    ax1.bar(x_indices[up_mask], plot_df.loc[up_mask, 'close'] - plot_df.loc[up_mask, 'open'], bottom=plot_df.loc[up_mask, 'open'], color='red', width=width)
    ax1.bar(x_indices[down_mask], plot_df.loc[down_mask, 'open'] - plot_df.loc[down_mask, 'close'], bottom=plot_df.loc[down_mask, 'close'], color='green', width=width)

    ax1.plot(x_indices, plot_df['MA20'], color='#f39c12', linewidth=1.5, label='MA20', alpha=0.85)
    ax1.plot(x_indices, plot_df['MA60'], color='#3498db', linewidth=1.5, label='MA60', alpha=0.85)

    ax1.set_ylabel('Price (USD)', fontsize=12, fontweight='bold')
    ax1.set_title(f'Gold 5m Candlestick & {vol_type} Volatility ({start_str} to {end_str} / Last 7 Days)', fontsize=16, fontweight='bold', pad=20)
    ax1.grid(True, linestyle='--', alpha=0.5)
    ax1.legend(loc='upper left')

    # --- 下半部分：波动率追踪 ---
    ax2.fill_between(x_indices, plot_df[vol_col], color='#ff7f0e', alpha=0.3)
    ax2.plot(x_indices, plot_df[vol_col], color='#d62728', linewidth=1, label=f'{vol_type} Volatility')

    vol_mean = plot_df[vol_col].mean()
    vol_std = plot_df[vol_col].std()
    warning_line = vol_mean + 2 * vol_std

    ax2.axhline(warning_line, color='purple', linestyle='-.', linewidth=1.2, label=f'Alert Line (+2 Std)')
    ax2.set_ylabel('Volatility', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Time (UTC)', fontsize=12)
    ax2.grid(True, linestyle='--', alpha=0.5)
    ax2.legend(loc='upper left')

    step = max(1, len(plot_df) // 10)
    tick_indices = x_indices[::step]
    tick_labels = plot_df['timestamp_utc'].dt.strftime('%m-%d\n%H:%M').iloc[::step]
    ax2.set_xticks(tick_indices)
    ax2.set_xticklabels(tick_labels, rotation=0, ha='center')

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = output_dir / f"{vol_type.lower()}_vol_{window_size}m_{start_str}_to_{end_str}.png"
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close(fig)

def save_feature_csv(df_processed: pd.DataFrame, feature_path: Path, vol_type: str):
    """保存最终 CSV"""
    csv_df = df_processed.copy()

    feature_path.mkdir(parents=True, exist_ok=True)
    start_date = csv_df['timestamp_utc'].min().strftime('%Y%m%d')
    end_date = csv_df['timestamp_utc'].max().strftime('%Y%m%d')
    final_output_file = feature_path / f"gold_features_{vol_type.lower()}_{start_date}_to_{end_date}.csv"

    csv_df.to_csv(final_output_file, index=False, encoding='utf-8-sig')
    print(f"💾 {vol_type} 特征 CSV 已安全落盘: {final_output_file.name}")


# ===================================================================
# 4. 主控引擎：合并与连续性缝合
# ===================================================================
def process_gold_directory(input_dir: str, feature_dir: str, pic_dir: str, window_size: int = 60):
    input_path = Path(input_dir)
    feature_path = Path(feature_dir)
    pic_path = Path(pic_dir)

    if not input_path.exists():
        print(f"❌ 输入目录不存在: {input_path}")
        return

    csv_files = sorted(list(input_path.glob("precious_metals_*.csv")))
    if not csv_files:
        print("⚠️ 未找到任何 precious_metals CSV 文件")
        return

    print(f"📥 正在合并 {len(csv_files)} 个 CSV 文件...")
    df_list = [pd.read_csv(file) for file in csv_files]
    df_all = pd.concat(df_list, ignore_index=True)
    df_all['timestamp_utc'] = pd.to_datetime(df_all['timestamp_utc'])

    # 1. 坏数据彻底清洗（引入硬性下限）
    df_all = clean_bad_price_data(df_all)
    df_all['timestamp_utc'] = df_all['timestamp_utc'].dt.floor('5min')

    # 2. 排序与去重
    df_all = df_all.sort_values(['timestamp_utc', 'fetch_time_utc']).drop_duplicates(subset=['timestamp_utc'], keep='last')
    df_all = df_all.set_index('timestamp_utc')

    # 3. 强制时间轴连续
    print("🧵 正在进行时间轴连续性缝合 (Resample)...")
    df_all = df_all.resample('5min').asfreq().reset_index()

    # 4. 智能前向填充补齐空洞
    df_all['symbol'] = df_all['symbol'].fillna("XAUUSD")
    df_all['fetch_time_utc'] = df_all['fetch_time_utc'].ffill()

    df_all['close'] = df_all['close'].ffill()
    df_all['open'] = df_all['open'].fillna(df_all['close'])
    df_all['high'] = df_all['high'].fillna(df_all['close'])
    df_all['low'] = df_all['low'].fillna(df_all['close'])

    # 5. GK 引擎处理
    print(f"⚙️ [1/2] 计算并生成 GK 波动率特征...")
    df_gk = calculate_gk_volatility(df_all, window_size=window_size)
    plot_candlestick_and_volatility(df_gk, window_size, pic_path, vol_type='GK')
    save_feature_csv(df_gk, feature_path, vol_type='GK')

    # 6. YZ 引擎处理
    print(f"⚙️ [2/2] 计算并生成 YZ 波动率特征...")
    df_yz = calculate_yz_volatility(df_all, window_size=window_size)
    plot_candlestick_and_volatility(df_yz, window_size, pic_path, vol_type='YZ')
    save_feature_csv(df_yz, feature_path, vol_type='YZ')

    print("-" * 60)


def main():
    print("=== 🚀 量化特征工程：GK + YZ 双波动率平滑引擎启动 ===")

    # 🚨 注意：请根据你的实际环境修改这个 ROOT_PATH
    ROOT_PATH: pathlib.Path = pathlib.Path('/home/evaseemefly/01data/05-spiders')

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

    # 启动时先跑一次
    job_task()

    # 注册定时任务
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(job_task, 'interval', minutes=10, id='gk_calc_job')

    print("\n⏳ 双引擎定时任务已注册，每10分钟自动生成 GK 和 YZ 两套严密指标，挂机中...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n🛑 波动率定时处理模块已安全停止。")


if __name__ == "__main__":
    main()