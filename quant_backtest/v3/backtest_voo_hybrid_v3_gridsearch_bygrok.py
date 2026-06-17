import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from itertools import product

# ==================== Mac 中文字体修复 ====================
plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Heiti TC', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv")


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def run_strategy(df, ma_len, us10y_th, vix_th, rsi_th, risk_pos):
    df = df.copy()
    df['VOO_MA'] = df['VOO_close'].rolling(window=ma_len, min_periods=1).mean()
    df['US10Y_MA60'] = df['US10Y_close'].rolling(60, min_periods=1).mean()
    df['HYG_MA60'] = df['HYG_close'].rolling(60, min_periods=1).mean()
    df['VIX_MA60'] = df['VIX_close'].rolling(60, min_periods=1).mean()
    df['RSI_14'] = calculate_rsi(df['VOO_close'])
    df['VOO_ret'] = df['VOO_close'].pct_change().fillna(0)

    base_trend = df['VOO_close'] > df['VOO_MA']
    us10y_rising = df['US10Y_close'].diff(20) > us10y_th
    hyg_divergence = ((df['HYG_close'] < df['HYG_MA60']).rolling(3).sum() == 3) & base_trend
    vix_risk = (df['VIX_close'] > vix_th) | (df['VIX_close'] > df['VIX_MA60'] * 1.8)
    risk_off = us10y_rising | hyg_divergence | vix_risk

    dip_buy = (df['RSI_14'] < rsi_th) & (df['VOO_close'] > df['VOO_open']) & (
            df['VIX_close'] < df['VIX_close'].shift(1))

    conditions = [dip_buy, risk_off, base_trend]
    choices = [1.0, risk_pos, 1.0]
    position = np.select(conditions, choices, default=risk_pos)
    position = pd.Series(position, index=df.index).shift(1).fillna(risk_pos)

    nav = (1 + position * df['VOO_ret']).cumprod()

    total_ret = nav.iloc[-1] - 1
    days = (nav.index[-1] - nav.index[0]).days
    years = days / 365.25
    cagr = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0

    roll_max = nav.cummax()
    drawdown = (nav - roll_max) / roll_max
    max_dd = drawdown.min()

    calmar = cagr / abs(max_dd) if max_dd != 0 else 0
    time_in_market = (position.mean() * 100)

    return {
        'ma_len': ma_len, 'us10y_th': us10y_th, 'vix_th': vix_th, 'rsi_th': rsi_th,
        'risk_pos': risk_pos, 'cagr': cagr, 'max_dd': max_dd, 'calmar': calmar,
        'time_in_market': time_in_market, 'nav': nav, 'position': position
    }


def main():
    print("=== 🚀 VOO Hybrid 参数自动搜索（Grid Search）===")
    df = pd.read_csv(CSV_FILE_PATH)
    df['trade_date_utc'] = pd.to_datetime(df['trade_date_utc'])
    df = df.set_index('trade_date_utc').sort_index().ffill()

    ma_lens = [80, 100, 120, 150, 180, 200]
    us10y_ths = [0.08, 0.10, 0.12, 0.15]
    vix_ths = [35, 38, 40, 45]
    rsi_ths = [30, 32, 35]
    risk_poss = [0.3, 0.4, 0.5]

    results = []
    print(f"开始搜索... 共 {len(ma_lens)*len(us10y_ths)*len(vix_ths)*len(rsi_ths)*len(risk_poss)} 种组合\n")

    for ma, us_th, vix_th, rsi_th, risk_pos in product(ma_lens, us10y_ths, vix_ths, rsi_ths, risk_poss):
        res = run_strategy(df, ma, us_th, vix_th, rsi_th, risk_pos)
        results.append(res)

    results = sorted(results, key=lambda x: x['calmar'], reverse=True)

    print("🏆 Top 10 最优参数组合 (按 Calmar 排序):")
    print("-" * 120)
    print(f"{'排名':<4} {'MA':<4} {'US10Y↑':<7} {'VIX>':<5} {'RSI<':<5} {'风险仓位':<8} {'年化':<8} {'回撤':<8} {'Calmar':<8} {'持仓%':<6}")
    print("-" * 120)
    for i, r in enumerate(results[:10], 1):
        print(f"{i:<4} {r['ma_len']:<4} {r['us10y_th']:<7.2f} {r['vix_th']:<5} {r['rsi_th']:<5} "
              f"{r['risk_pos']:<8.1f} {r['cagr']*100:>6.2f}% {r['max_dd']*100:>7.2f}% "
              f"{r['calmar']:>7.2f} {r['time_in_market']:>6.1f}%")

    # ==================== 最优参数可视化（Top 10 曲线叠加） ====================
    best = results[0]
    print(f"\n🎯 最优参数（VOO）：MA{best['ma_len']} | US10Y>{best['us10y_th']} | VIX>{best['vix_th']} | RSI<{best['rsi_th']} | 风险仓位 {best['risk_pos']}")
    print(f"年化: {best['cagr']*100:.2f}% | 最大回撤: {best['max_dd']*100:.2f}% | Calmar: {best['calmar']:.2f}")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 12), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)
    plt.subplots_adjust(hspace=0.05)

    # 上图：Baseline + Top 10 曲线
    baseline_nav = (1 + df['VOO_close'].pct_change().fillna(0)).cumprod()
    ax1.plot(df.index, baseline_nav, label='Baseline (死拿 VOO)', color='gray', alpha=0.7, linewidth=2)

    colors = plt.cm.tab10(np.linspace(0, 1, 10))   # 10 种不同颜色
    for i, r in enumerate(results[:10]):
        label = f"Top{i+1} (MA{r['ma_len']} Calmar {r['calmar']:.2f})"
        linewidth = 3.0 if i == 0 else 1.5
        alpha = 1.0 if i == 0 else 0.85
        ax1.plot(df.index, r['nav'], label=label, color=colors[i], linewidth=linewidth, alpha=alpha)

    ax1.set_title('VOO 最优 Hybrid 参数回测结果（Top 10 曲线对比）', fontsize=16, fontweight='bold')
    ax1.set_ylabel('Net Asset Value (log scale)')
    ax1.set_yscale('log')
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.5)

    # 下图：价格 + MA + 避险区域（以 Top 1 为准）
    ax2.plot(df.index, df['VOO_close'], label='VOO Price', color='black')
    ax2.plot(df.index, df['VOO_close'].rolling(best['ma_len']).mean(),
             label=f'VOO MA{best["ma_len"]}', color='orange', linestyle='--')
    ax2.fill_between(df.index, ax2.get_ylim()[0], ax2.get_ylim()[1],
                     where=(best['position'] < 0.6), color='red', alpha=0.15,
                     label=f'Risk-Off (保留 {best["risk_pos"]} 仓位)')

    ax2.set_ylabel('VOO Price')
    ax2.legend()
    ax2.grid(True, alpha=0.5)

    output_pic_path = Path(__file__).parent / "09_best_hybrid_voo_top10.png"
    plt.savefig(output_pic_path, dpi=200, bbox_inches='tight')
    plt.close()
    print(f"\n🖼️ Top 10 策略对比图已保存: {output_pic_path.name}")


if __name__ == "__main__":
    main()