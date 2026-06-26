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
            'cost_price': 408.21,
            'shares': 12,
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
            'stage': 0,
            'cost_price': 0.0,
            'shares': 0,
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
            'cost_price': 1093.73,
            'shares': 1,
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
            'cost_price': 182.583,
            'shares': 28,
            'cash': 8000
        }
    },
    # 🌟 新增：博通 (强动量白马 - 紧凑网格 + 宽容追踪止盈)
    'AVGO': {
        'strategy': 'grid',
        'rsi_entry_th': 50,  # WFO寻优结果：低于50才建仓
        'rsi_exit_th': 80,  # 极度超买强平线
        'drop1_pct': 0.03,  # WFO寻优结果：跌3%极速加仓 (强势股特性)
        'drop2_pct': 0.05,  # 紧凑防守：跌5%瞬间满仓
        'ma_period': 200,  # 使用 200 日线作为牛熊分界
        'initial_alloc': 0.30,
        'add1_alloc': 0.30,
        'add2_alloc': 0.40,
        'update_ref_on_add': False,  # 固定基准闪电战
        'profit_target_pct': 0.15,  # WFO寻优结果：15%激活追踪雷达
        'trailing_drop_pct': 0.08,  # WFO寻优结果：8%的宽容回撤清仓 (兼顾防守与吃大波段)
        'verbose': False,
        'live_state': {
            'stage': 1,  # 假设目前空仓观望
            'cost_price': 401.356,
            'shares': 5,
            'cash': 8000  # 你准备分配给博通的建仓总资金 (请根据实际情况修改)
        }
    },

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
