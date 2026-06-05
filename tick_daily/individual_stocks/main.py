import sys
import time
from pathlib import Path

ASSET_CONFIG = {
    # ---------------- 网格策略组 ----------------
    'TSLA': {
        'strategy': 'grid',
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
        'trailing_drop_pct': 0.0,  # 🌟 关键：填 0 代表 TSLA 启用硬止盈
        'verbose': False,
        'live_state': {
            'stage': 2,
            'cost_price': 404.29,
            'shares': 12,
            'cash': 6000
        }
    },
    'MSFT': {
        'strategy': 'grid',
        'rsi_entry_th': 45,
        'rsi_exit_th': 80,
        'drop1_pct': 0.03,
        'drop2_pct': 0.05,
        'ma_period': 150,
        'initial_alloc': 0.30,
        'add1_alloc': 0.30,
        'add2_alloc': 0.40,
        'update_ref_on_add': False,
        'profit_target_pct': 0.10,
        'trailing_drop_pct': 0.05,  # 🌟 填 0.05 代表启用追踪雷达
        'verbose': False,
        'live_state': {
            'stage': 2,
            'cost_price': 400.268,
            'shares': 11,
            'cash': 7000
        }
    },
    'META': {
        'strategy': 'grid',
        'rsi_entry_th': 50,
        'rsi_exit_th': 80,
        'drop1_pct': 0.05,
        'drop2_pct': 0.05,
        'ma_period': 150,
        'initial_alloc': 0.30,
        'add1_alloc': 0.30,
        'add2_alloc': 0.40,
        'update_ref_on_add': False,
        'profit_target_pct': 0.15,
        'trailing_drop_pct': 0.08,  # 🌟 填 0.08 代表启用追踪雷达
        'verbose': False,
        'live_state': {
            'stage': 1,
            'cost_price': 647.5,
            'shares': 3,
            'cash': 5000
        }
    },

    # ---------------- 趋势策略组 ----------------
    'MU': {
        'strategy': 'trend',
        'entry_period': 30,
        'exit_period': 40,
        'alloc_pct': 0.95,
        'verbose': False,
        'live_first_tranche_pct': 0.35,
        'live_state': {
            'stage': 0,
            'cost_price': 0.0,
            'shares': 0,
            'cash': 6000,
        }
    },
    'NVDA': {
        'strategy': 'trend',
        'entry_period': 30,
        'exit_period': 20,
        'alloc_pct': 0.95,
        'verbose': False,
        'live_first_tranche_pct': 0.35,
        'live_state': {
            'stage': 1,
            'cost_price': 184.15,
            'shares': 32,
            'cash': 0
        }
    }
}


def main():
    print("=" * 60)
    print("🚀 [个人量化对冲基金] 全自动化投研流水线启动")
    print("=" * 60)

    for ticker, config in ASSET_CONFIG.items():
        print(f"\n▶️ [正在调度] 处理 {ticker}...")
        try:
            # ================= 极其干净的路由器 =================
            if config['strategy'] == 'grid':
                from strategy_reporters import report_grid_v2
                report_grid_v2.generate_strategy_report(ticker=ticker, config=config)

            elif config['strategy'] == 'trend':
                from strategy_reporters import report_trend_v2
                report_trend_v2.generate_strategy_report(ticker=ticker, config=config)

            else:
                print(f"⚠️ {ticker} 的策略类型 '{config['strategy']}' 未知！")
            # ====================================================
            time.sleep(1)

        except Exception as e:
            print(f"❌ 处理 {ticker} 时发生严重错误: {e}")
            continue

    print("\n" + "=" * 60 + "\n🎉 今日全部投研任务执行完毕！\n" + "=" * 60)


if __name__ == "__main__":
    main()
