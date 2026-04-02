import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# 设置 Matplotlib 使用 'Agg' 后端，确保在 Linux 服务器（无 GUI）上也能画图
import matplotlib

matplotlib.use('Agg')


def calculate_gk_volatility(df: pd.DataFrame, window_size: int = 60) -> pd.DataFrame:
    """核心计算：Garman-Klass 波动率"""
    df_calc = df.copy()
    constant = 2 * np.log(2) - 1

    # 向量化计算 H/L 和 C/O 的对数收益率
    # 使用 np.finfo(float).eps 避免除以 0 或 ln(1) 导致的 NaN，增加鲁棒性
    log_hl = np.log(df_calc['high'] / (df_calc['low'] + np.finfo(float).eps))
    log_co = np.log(df_calc['close'] / (df_calc['open'] + np.finfo(float).eps))

    df_calc['GK_variance'] = 0.5 * (log_hl ** 2) - constant * (log_co ** 2)

    vol_col_name = f'GK_volatility_{window_size}m'
    df_calc[vol_col_name] = np.sqrt(df_calc['GK_variance'].rolling(window=window_size).mean())

    return df_calc


def plot_volatility_chart(df: pd.DataFrame, window_size: int, output_dir: Path):
    """可视化：绘制价格与滚动 GK 波动率的双子图"""
    vol_col = f'GK_volatility_{window_size}m'

    # 剔除绘图早期因滚动计算产生的 NaN，确保图表对齐
    plot_df = df.dropna(subset=[vol_col]).copy()

    if plot_df.empty:
        print("⚠️ 数据太少，无法绘制波动率图表。")
        return

    # 获取起止日期用于文件命名和标题
    start_str = plot_df['timestamp_utc'].iloc[0].strftime('%Y-%m-%d')
    end_str = plot_df['timestamp_utc'].iloc[-1].strftime('%Y-%m-%d')

    # 创建双子图 (2行1列)，共享 X 轴（时间轴）
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
    plt.subplots_adjust(hspace=0.05)  # 减小两个子图之间的间距

    # --- 上图：黄金收盘价 ---
    ax1.plot(plot_df['timestamp_utc'], plot_df['close'], color='#1f77b4', label='Gold Close Price', linewidth=1.5)
    ax1.set_ylabel('Price (USD)', fontsize=12, fontweight='bold')
    ax1.set_title(f'Gold Price vs. {window_size}-min Garman-Klass Volatility ({start_str} to {end_str}) by @drcc', fontsize=16,
                  fontweight='bold', pad=20)
    ax1.grid(True, linestyle='--', alpha=0.5)
    ax1.legend(loc='upper left')

    # --- 下图：GK 波动率 ---
    # 使用填充色，让波动率的暴涨更加醒目
    ax2.fill_between(plot_df['timestamp_utc'], plot_df[vol_col], color='#ff7f0e', alpha=0.3)
    ax2.plot(plot_df['timestamp_utc'], plot_df[vol_col], color='#d62728', label=f'GK Volatility ({window_size}m)',
             linewidth=1)
    ax2.set_ylabel('Volatility', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Time (UTC)', fontsize=12)
    ax2.grid(True, linestyle='--', alpha=0.5)
    ax2.legend(loc='upper left')

    # 优化时间轴显示 (横跨多天时，自动调整刻度)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d\n%H:%M'))
    plt.xticks(rotation=0, ha='center')

    # 保存图片
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = output_dir / f"gk_vol_{window_size}m_{start_str}_to_{end_str}.png"

    # bbox_inches='tight' 确保保存时标签不被裁切
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close(fig)  # 释放内存

    print(f"🖼️ 特征可视化图表已保存至: {filename}")


def process_gold_directory(input_dir: str, feature_dir: str, pic_dir: str, window_size: int = 60):
    """全局处理：合并、计算、存储 CSV、画图输出"""
    input_path = Path(input_dir)
    feature_path = Path(feature_dir)
    pic_path = Path(pic_dir)

    if not input_path.exists():
        print(f"❌ 找不到数据目录: {input_path.resolve()}")
        return

    # 1. 获取并排序 CSV 文件
    csv_files = sorted(list(input_path.glob("precious_metals_*.csv")))

    if not csv_files:
        print(f"❌ 在 {input_path} 下没有找到匹配的 CSV 文件")
        return

    print(f"📥 找到 {len(csv_files)} 个数据文件，正在合并并进行时间轴排序...")
    df_list = [pd.read_csv(file) for file in csv_files]
    df_all = pd.concat(df_list, ignore_index=True)

    # 2. 数据标准化清理
    df_all['timestamp_utc'] = pd.to_datetime(df_all['timestamp_utc'])
    df_all = df_all.sort_values('timestamp_utc').reset_index(drop=True)

    # 3. 计算 GK 特征
    print(f"⚙️ 正在全局计算 {window_size} 周期 Garman-Klass 波动率特征...")
    df_processed = calculate_gk_volatility(df_all, window_size=window_size)

    # 4. 存储特征 CSV
    feature_path.mkdir(parents=True, exist_ok=True)
    start_date = df_processed['timestamp_utc'].iloc[0].strftime('%Y%m%d')
    end_date = df_processed['timestamp_utc'].iloc[-1].strftime('%Y%m%d')
    final_output_file = feature_path / f"gold_features_{start_date}_to_{end_date}.csv"

    df_processed.to_csv(final_output_file, index=False, encoding='utf-8-sig')
    print(f"💾 聚合特征 CSV 已保存至: {final_output_file}")

    # 5. 核心变动：生成可视化图表
    print(f"🎨 正在生成特征可视化图表...")
    plot_volatility_chart(df_processed, window_size, pic_path)

    print("-" * 60)
    print(f"✅ 处理完成！跨日连续数据总行数: {len(df_processed)}")


def main():
    print("=== 量化特征工程：全局波动率计算与可视化模块启动 ===")

    # 路径配置
    INPUT_DATA_DIR = "/home/evaseemefly/01data/05-spiders/market_prices"
    OUTPUT_FEATURE_DIR = "/home/evaseemefly/01data/05-spiders/output/gold_features"
    OUTPUT_PIC_DIR = "/home/evaseemefly/01data/05-spiders/output/gold_gk_pics"  # 按要求新增图片存储目录

    # 窗口大小 (分钟)
    WINDOW = 60

    process_gold_directory(
        input_dir=INPUT_DATA_DIR,
        feature_dir=OUTPUT_FEATURE_DIR,
        pic_dir=OUTPUT_PIC_DIR,
        window_size=WINDOW
    )


if __name__ == "__main__":
    main()