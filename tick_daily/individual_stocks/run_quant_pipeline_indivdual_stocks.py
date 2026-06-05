import sys
import time
from pathlib import Path

# ==================== 1. 全局资产配置字典 ====================
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
        'verbose': True,
        'live_state': {
            'stage': 2,  # 实盘处于第二档加仓状态
            'cost_price': 404.29,
            'shares': 12,
            'cash': 6000  # 填入你准备用来补仓的剩余现金
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
        'trailing_drop_pct': 0.05,
        'verbose': True,
        'live_state': {
            'stage': 2,
            'cost_price': 400.268,
            'shares': 11,
            'cash': 7000  # 准备补仓的资金
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
        'trailing_drop_pct': 0.08,
        'verbose': True,
        'live_state': {
            'stage': 1,  # 空仓等待中
            'cost_price': 647.5,
            'shares': 3,
            'cash': 5000  # 准备建仓的第一笔资金
        }
    },

    # ---------------- 趋势策略组 ----------------
    'MU': {
        'strategy': 'trend',
        'entry_period': 30,
        'exit_period': 40,
        'alloc_pct': 0.95,
        'verbose': True,

        # 趋势专属：实盘分层建仓参数
        'live_first_tranche_pct': 0.35,
        'live_second_tranche_pct': 0.25,
        'live_final_tranche_pct': 0.35,

        'live_state': {
            'stage': 0,  # 空仓等待突破
            'cost_price': 0.0,
            'shares': 0,
            'cash': 6000,  # 计划打入 MU 的总资金
        }
    },
    'NVDA': {
        'strategy': 'trend',
        'entry_period': 30,
        'exit_period': 20,
        'alloc_pct': 0.95,
        'verbose': True,

        # 趋势专属：实盘分层建仓参数
        'live_first_tranche_pct': 0.35,
        'live_second_tranche_pct': 0.25,
        'live_final_tranche_pct': 0.35,

        'live_state': {
            'stage': 1,  # 💡 修正：趋势策略持仓状态必须是 1
            'cost_price': 184.15,
            'shares': 32,
            'cash': 0  # 已满仓，留存现金为 0
        }
    }
}


# ==================== 2. 主流水线调度器 ====================
def main():
    print("=" * 60)
    print("🚀 [个人量化对冲基金] 全自动化投研流水线启动")
    print("=" * 60)

    # 遍历你的资产池配置
    for ticker, config in ASSET_CONFIG.items():
        print(f"\n▶️ [正在调度] 处理 {ticker}...")

        try:
            # ================= 核心：策略路由器 (Router) =================
            # 根据 ticker 和 strategy 动态调用对应的 report 脚本
            if config['strategy'] == 'grid' and ticker == 'TSLA':
                from strategy_reporters import report_tsla_v2
                report_tsla_v2.generate_strategy_report(ticker=ticker, config=config)

            elif config['strategy'] == 'grid' and ticker == 'MSFT':
                # 🌟 新增路由：呼叫 MSFT 专属画图脚本
                from strategy_reporters import report_msft_v2
                report_msft_v2.generate_strategy_report(ticker=ticker, config=config)
                # 🌟 新增：接入 META 的路由逻辑
            elif config['strategy'] == 'grid' and ticker == 'META':
                from strategy_reporters import report_meta_v2
                report_meta_v2.generate_strategy_report(ticker=ticker, config=config)

            elif config['strategy'] == 'trend' and ticker == 'MU':
                # 注意：请确保你在 strategy_reporters 目录下创建了 report_mu.py
                # from strategy_reporters import report_mu
                from strategy_reporters import report_mu_v2_fixed_py39
                report_mu_v2_fixed_py39.generate_strategy_report(ticker=ticker, config=config)

            elif config['strategy'] == 'trend' and ticker == 'NVDA':
                from strategy_reporters import report_nvda_v2
                report_nvda_v2.generate_strategy_report(ticker=ticker, config=config)

            else:
                print(f"⚠️ {ticker} 的报告脚本未接入路由，请检查 main() 中的分发逻辑！")
            # ==============================================================

            time.sleep(1)  # 缓冲

        except ImportError as ie:
            print(f"❌ 导入模块失败: 请检查 strategy_reporters 目录下是否有 {ticker} 的专属脚本！报错: {ie}")
        except Exception as e:
            print(f"❌ 处理 {ticker} 时发生严重错误: {e}")
            continue

    print("\n" + "=" * 60)
    print("🎉 今日全部投研任务执行完毕！")
    print("=" * 60)


if __name__ == "__main__":
    main()
