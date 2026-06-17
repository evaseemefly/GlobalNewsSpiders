import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# ==================== Mac 中文字体修复 ====================
plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Heiti TC', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv"
)


def load_and_prepare_data():
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index()

    # 过滤真实交易日
    df = df[df['VOO_close'].notna()]
    df = df[df.index.dayofweek < 5]
    df = df.ffill()

    # 计算指标
    df['VOO_ret'] = df['VOO_close'].pct_change()
    df['VIX_MA60'] = df['VIX_close'].rolling(60, min_periods=1).mean()

    # 计算未来 N 天的远期收益率 (Forward Returns)
    df['fwd_ret_5d'] = df['VOO_close'].shift(-5) / df['VOO_close'] - 1
    df['fwd_ret_20d'] = df['VOO_close'].shift(-20) / df['VOO_close'] - 1

    return df.dropna(subset=['fwd_ret_20d'])


def analyze_vix_regimes(df):
    """验证 VIX 阈值切分是否合理"""
    # 定义测试的 VIX 桶 (Bins)
    bins = [0, 15, 20, 30, 45, 100]
    labels = ['平静 (<15)', '预警 (15-20)', '风险 (20-30)', '恐慌 (30-45)', '极端 (>45)']

    df['VIX_Regime'] = pd.cut(df['VIX_close'], bins=bins, labels=labels)

    # 统计特征
    stats = df.groupby('VIX_Regime', observed=False).agg(
        样本天数=('VOO_close', 'count'),
        胜率_20天=('fwd_ret_20d', lambda x: (x > 0).mean()),
        均值_20天=('fwd_ret_20d', 'mean'),
        中位数_20天=('fwd_ret_20d', 'median'),
        极度恶化风险_20天=('fwd_ret_20d', lambda x: (x < -0.05).mean())  # 未来20天跌幅超5%的概率
    ).round(4)

    print("\n=== VIX 阈值状态远期收益统计 (未来 20 个交易日) ===")
    print(stats)

    # 可视化分布
    plt.figure(figsize=(12, 6))
    sns.violinplot(x='VIX_Regime', y='fwd_ret_20d', data=df, inner="quartile", palette="muted")
    plt.axhline(0, color='red', linestyle='--', alpha=0.5)
    plt.title('不同 VIX 状态下的 VOO 未来 20 天收益率分布', fontsize=14, fontweight='bold')
    plt.ylabel('未来 20 天收益率')
    plt.grid(True, alpha=0.3)
    plt.savefig('vix_regime_validation.png', dpi=200, bbox_inches='tight')
    plt.close()
    print("🖼️ VIX 状态分布图已保存: vix_regime_validation.png")


def analyze_panic_drops(df):
    """验证单日跌幅 (Panic Drop) 阈值是否合理"""
    # 测试不同的跌幅阈值
    drop_thresholds = [-0.015, -0.020, -0.025, -0.030]

    results = []
    for th in drop_thresholds:
        mask = df['VOO_ret'] <= th
        subset = df[mask]
        if len(subset) == 0:
            continue

        results.append({
            '单日跌幅阈值': f"<= {th * 100:.1f}%",
            '触发次数': len(subset),
            '胜率_5天': (subset['fwd_ret_5d'] > 0).mean(),
            '均值_5天': subset['fwd_ret_5d'].mean(),
            '胜率_20天': (subset['fwd_ret_20d'] > 0).mean(),
            '均值_20天': subset['fwd_ret_20d'].mean(),
        })

    stats_df = pd.DataFrame(results).round(4)
    print("\n=== 单日暴跌 (Panic Drop) 远期收益统计 ===")
    print(stats_df)


def main():
    print("⏳ 正在加载数据并计算远期收益...")
    df = load_and_prepare_data()
    analyze_vix_regimes(df)
    analyze_panic_drops(df)
    print("\n✅ 验证计算完成。")


if __name__ == "__main__":
    main()