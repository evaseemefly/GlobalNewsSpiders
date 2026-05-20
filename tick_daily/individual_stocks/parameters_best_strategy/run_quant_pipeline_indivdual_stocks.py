import sys
import time
from pathlib import Path

# 导入你刚刚修改好的底层画图与回测脚本
# （注意：文件名请与你实际的 Python 文件名保持一致，去掉 .py）
import parameter_best_strategy_plot_tls_v3 as plotter

ASSET_CONFIG = {
    'TSLA': {
        'strategy': 'grid',
        # --- 策略参数 (这里填入你回测出来的最优解) ---
        'rsi_entry_th': 50,
        'rsi_exit_th': 70,
        'drop1_pct': 0.05,
        'drop2_pct': 0.08,
        'ma_period': 200,
        'initial_alloc': 0.30,
        'add1_alloc': 0.30,
        'add2_alloc': 0.40,
        'update_ref_on_add': False,
        'profit_target_pct': 0.20,
        'verbose': True,
        # --- 实盘账户状态注入 ---
        'live_state': {
            'stage': 2,  # 真实持仓阶段
            'cost_price': 404.0,  # 真实成本价
            'shares': 12  # 真实持股数
        }
    },
    # 'NVDA': { ... }
}


def main():
    print("=" * 60)
    print("🚀 [个人量化对冲基金] 全自动化投研流水线启动")
    print("=" * 60)

    # 遍历你的资产池配置
    for ticker, config in ASSET_CONFIG.items():
        print(f"\n▶️ [正在调度] 处理 {ticker}...")

        try:
            # 策略分流器
            if config['strategy'] == 'grid':
                # 核心：将 ticker 和对应的配置字典，传给底层画图脚本！
                plotter.generate_strategy_report(ticker=ticker, config=config)

            elif config['strategy'] == 'trend':
                print(f"⚠️ {ticker} 被配置为趋势策略，暂未接入右侧趋势画图脚本，已跳过。")
                # 未来你可以写一个 trend_plotter.py，在这里接入

            time.sleep(1)  # 缓冲

        except Exception as e:
            print(f"❌ 处理 {ticker} 时发生严重错误: {e}")
            continue

    print("\n" + "=" * 60)
    print("🎉 今日全部投研任务执行完毕！")
    print("=" * 60)


if __name__ == "__main__":
    main()
