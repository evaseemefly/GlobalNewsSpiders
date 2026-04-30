import os
import pathlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler
import platform

import matplotlib

matplotlib.use('Agg')


# ==========================================
# 1. 波动率计算引擎 (保持逻辑不变)
# ==========================================
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


# ==========================================
# 2. 增强型绘图引擎 (五子图看板)
# ==========================================
def plot_comprehensive_dashboard(df: pd.DataFrame, window_size: int, output_dir: Path, vol_type: str = 'YZ'):
    """生成包含黄金、波动率及三大宏观指标的终极看板"""
    vol_col = f'{vol_type}_volatility_{window_size}m'
    plot_df = df.copy()

    if plot_df.empty: return

    # 截取最近 7 天数据
    latest_time = plot_df['timestamp_utc'].max()
    start_time = latest_time - pd.Timedelta(days=7)
    plot_df = plot_df[plot_df['timestamp_utc'] >= start_time].copy()
    plot_df = plot_df.dropna(subset=['close']).reset_index(drop=True)

    if plot_df.empty: return

    # 预计算均线
    plot_df['MA20'] = plot_df['close'].rolling(window=20, min_periods=1).mean()
    plot_df['MA60'] = plot_df['close'].rolling(window=60, min_periods=1).mean()

    start_str = plot_df['timestamp_utc'].min().strftime('%Y-%m-%d')
    end_str = plot_df['timestamp_utc'].max().strftime('%Y-%m-%d')

    # 初始化 5 行画布
    fig, axes = plt.subplots(5, 1, figsize=(16, 20), sharex=True, gridspec_kw={'height_ratios': [3, 1, 1, 1, 1]})
    plt.subplots_adjust(hspace=0.1)
    ax_gold, ax_vol, ax_dxy, ax_us10y, ax_vix = axes

    x_indices = np.arange(len(plot_df))

    # --- Plot 1: Gold Candlestick ---
    up = plot_df['close'] >= plot_df['open']
    down = ~up
    ax_gold.vlines(x_indices[up], plot_df.loc[up, 'low'], plot_df.loc[up, 'high'], color='red', linewidth=1)
    ax_gold.vlines(x_indices[down], plot_df.loc[down, 'low'], plot_df.loc[down, 'high'], color='green', linewidth=1)
    ax_gold.bar(x_indices[up], plot_df.loc[up, 'close'] - plot_df.loc[up, 'open'], bottom=plot_df.loc[up, 'open'],
                color='red', width=0.6)
    ax_gold.bar(x_indices[down], plot_df.loc[down, 'open'] - plot_df.loc[down, 'close'],
                bottom=plot_df.loc[down, 'close'], color='green', width=0.6)
    ax_gold.plot(x_indices, plot_df['MA20'], color='#f39c12', label='MA20', alpha=0.8)
    ax_gold.plot(x_indices, plot_df['MA60'], color='#3498db', label='MA60', alpha=0.8)
    ax_gold.set_ylabel('Gold Price', fontweight='bold')
    ax_gold.set_title(f'Comprehensive Quant Dashboard ({vol_type}) | {start_str} to {end_str}', fontsize=16,
                      fontweight='bold')
    ax_gold.legend(loc='upper left')

    # --- Plot 2: Volatility ---
    ax_vol.fill_between(x_indices, plot_df[vol_col], color='#ff7f0e', alpha=0.2)
    ax_vol.plot(x_indices, plot_df[vol_col], color='#d62728', label=f'{vol_type} Vol')
    ax_vol.axhline(plot_df[vol_col].mean() + 2 * plot_df[vol_col].std(), color='purple', linestyle='--',
                   label='Alert (+2std)')
    ax_vol.set_ylabel('Volatility', fontweight='bold')
    ax_vol.legend(loc='upper left')

    # --- Plot 3: DXY ---
    ax_dxy.plot(x_indices, plot_df['DXY'], color='#2ca02c', label='DXY (US Dollar Index)')
    ax_dxy.set_ylabel('DXY', fontweight='bold')
    ax_dxy.legend(loc='upper left')

    # --- Plot 4: US10Y ---
    ax_us10y.plot(x_indices, plot_df['US10Y'], color='#9467bd', label='US10Y (Yield %)')
    ax_us10y.set_ylabel('US10Y', fontweight='bold')
    ax_us10y.legend(loc='upper left')

    # --- Plot 5: VIX ---
    ax_vix.plot(x_indices, plot_df['VIX'], color='#8c564b', label='VIX (Fear Index)')
    ax_vix.set_ylabel('VIX', fontweight='bold')
    ax_vix.legend(loc='upper left')

    # Formatting X-axis
    step = max(1, len(plot_df) // 10)
    ax_vix.set_xticks(x_indices[::step])
    ax_vix.set_xticklabels(plot_df['timestamp_utc'].dt.strftime('%m-%d\n%H:%M').iloc[::step])
    for ax in axes: ax.grid(True, alpha=0.3)

    # Save
    filename = output_dir / f"{vol_type.lower()}_vol_merge_all_{start_str}_to_{end_str}.png"
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"🖼️ 看板图片已生成: {filename.name}")


# ==========================================
# 3. 数据融合与处理逻辑
# ==========================================
def process_comprehensive_data(root_path: Path, window_size: int = 60):
    input_dir = root_path / "market_prices"
    macro_dir = root_path / "macro_realtime"
    output_feature_dir = root_path / "output" / "gold_features"
    output_pic_dir = root_path / "output" / "gold_gk_pics"

    # A. 加载黄金价格
    gold_files = sorted(list(input_dir.glob("precious_metals_*.csv")))
    if not gold_files: return
    df_gold = pd.concat([pd.read_csv(f) for f in gold_files], ignore_index=True)
    df_gold['timestamp_utc'] = pd.to_datetime(df_gold['timestamp_utc'])
    df_gold = df_gold.replace(-999.0, np.nan).sort_values('timestamp_utc')
    df_gold = df_gold.drop_duplicates('timestamp_utc', keep='last')

    # B. 加载高频宏观数据
    macro_files = sorted(list(macro_dir.glob("macro_realtime_*.csv")))
    if not macro_files:
        df_macro = pd.DataFrame(columns=['timestamp_utc', 'DXY', 'US10Y', 'VIX'])
    else:
        df_macro = pd.concat([pd.read_csv(f) for f in macro_files], ignore_index=True)
        df_macro['timestamp_utc'] = pd.to_datetime(df_macro['timestamp_utc'])
        # 仅保留核心指标，过滤掉抓取时间戳，方便前向填充
        df_macro = df_macro[['timestamp_utc', 'DXY', 'US10Y', 'VIX']].dropna(subset=['DXY', 'US10Y', 'VIX'], how='all')
        df_macro = df_macro.sort_values('timestamp_utc').drop_duplicates('timestamp_utc', keep='last')

    # C. 合并与对齐 (以黄金时间轴为基准)
    print(f"🔗 正在合并黄金与高频宏观数据...")
    df_all = pd.merge(df_gold, df_macro, on='timestamp_utc', how='left')

    # 执行前向填充，处理宏观数据的短时缺失
    df_all[['DXY', 'US10Y', 'VIX']] = df_all[['DXY', 'US10Y', 'VIX']].ffill()

    # D. 剔除非交易时间 (周末/停盘)
    is_weekend = (df_all['timestamp_utc'].dt.dayofweek == 5) | \
                 ((df_all['timestamp_utc'].dt.dayofweek == 4) & (df_all['timestamp_utc'].dt.hour >= 21)) | \
                 ((df_all['timestamp_utc'].dt.dayofweek == 6) & (df_all['timestamp_utc'].dt.hour < 22))

    # 流动性熔断判定
    amplitude = df_all['high'] - df_all['low']
    is_dead = amplitude.rolling(window=3, min_periods=1).max() < 0.3
    df_all.loc[is_weekend | is_dead, ['open', 'high', 'low', 'close', 'DXY', 'US10Y', 'VIX']] = np.nan

    # E. 计算波动率分支
    df_gk = calculate_gk_volatility(df_all, window_size)
    df_yz = calculate_yz_volatility(df_all, window_size)

    # 合并 YZ 波动率到主表用于存储
    df_final = df_yz.copy()
    df_final['GK_volatility'] = df_gk[f'GK_volatility_{window_size}m']

    # F. 保存特征 CSV
    start_date = df_final['timestamp_utc'].min().strftime('%Y%m%d')
    end_date = df_final['timestamp_utc'].max().strftime('%Y%m%d')
    csv_name = output_feature_dir / f"gold_features_yfinance_{start_date}_to_{end_date}.csv"

    df_save = df_final.copy()
    fill_cols = ['open', 'high', 'low', 'close', 'DXY', 'US10Y', 'VIX', f'YZ_volatility_{window_size}m',
                 'GK_volatility']
    df_save[fill_cols] = df_save[fill_cols].fillna(-999)
    df_save.to_csv(csv_name, index=False, encoding='utf-8-sig')
    print(f"💾 特征文件已保存: {csv_name.name}")

    # G. 绘图
    plot_comprehensive_dashboard(df_final, window_size, output_pic_dir, vol_type='YZ')
    print("-" * 60)


# ==========================================
# 4. 主程序入口
# ==========================================
def main():
    # 路径自适应
    sys_name = platform.system()
    if sys_name == "Darwin":
        ROOT_PATH = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
    else:
        # Ubuntu 部署路径
        ROOT_PATH = Path("/home/evaseemefly/01data/05-spiders")

    print(f"=== 黄金+高频宏观 综合特征工厂启动 ===")
    print(f"📍 工作目录: {ROOT_PATH}")

    WINDOW = 60

    def job():
        from datetime import datetime
        print(f"\n🕒 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 触发定时融合任务...")
        process_comprehensive_data(ROOT_PATH, WINDOW)

    job()

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(job, 'interval', minutes=10, id='comprehensive_merge_job')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n🛑 融合任务已停止。")


if __name__ == "__main__":
    main()
