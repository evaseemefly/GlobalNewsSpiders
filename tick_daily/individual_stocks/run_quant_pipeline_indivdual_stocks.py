import sys
import time
from pathlib import Path

# ==================== 1. 全局资产配置字典 ====================
ASSET_CONFIG = {
    'TSLA': {
        'strategy': 'grid',
        # --- TSLA 网格最优参数 ---
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
    'MU': {
        'strategy': 'trend',
        'entry_period': 30,
        'exit_period': 40,
        'alloc_pct': 0.95,
        'verbose': True,

        'live_first_tranche_pct': 0.35,
        'live_second_tranche_pct': 0.25,
        'live_final_tranche_pct': 0.35,

        'live_state': {
            'stage': 0,
            'cost_price': 0.0,
            'shares': 0,
            'cash': 7900,
            'min_trade_shares': 1,
        }
    },
    # 💡 未来你跑完 NVDA 寻优后，可以直接取消注释并填入参数
    'NVDA': {
        'strategy': 'trend',
        'entry_period': 30,
        'exit_period': 20,
        'alloc_pct': 0.95,
        'verbose': True,
        'live_state': {
            'stage': 2,  # 0 代表当前实盘空仓，等待发车
            'cost_price': 404.29,  # 空仓状态下成本价设为 0
            'shares': 12  # 空仓状态下股数设为 0
        }
    },
    # 🌟 新增：微软 (稳健大白马 - 闪击网格+追踪止盈)
    'MSFT': {
        'strategy': 'grid',
        # --- MSFT 专属移动追踪网格最优参数 ---
        'rsi_entry_th': 45,  # 进场更严谨
        'rsi_exit_th': 80,
        'drop1_pct': 0.03,  # 浅坑极速加仓
        'drop2_pct': 0.05,  # 快速打满防守
        'ma_period': 150,
        'initial_alloc': 0.30,
        'add1_alloc': 0.30,
        'add2_alloc': 0.40,
        'update_ref_on_add': False,  # 固定建仓基准
        'profit_target_pct': 0.10,  # 追踪激活线：10%
        'trailing_drop_pct': 0.05,  # 回撤清仓线：最高点回落5%
        'verbose': True,
        # --- 实盘账户状态注入 ---
        'live_state': {
            'stage': 2,  # 假设目前空仓，等待入场
            'cost_price': 400.268,
            'shares': 11
        }
    },
    # 🌟 新增：META (高弹性妖股 - 闪击网格+宽容追踪止盈)
    'META': {
        'strategy': 'grid',
        'rsi_entry_th': 50,
        'rsi_exit_th': 80,
        'drop1_pct': 0.05,  # 极速一档
        'drop2_pct': 0.05,  # 极速二档 (瞬间打满)
        'ma_period': 150,
        'initial_alloc': 0.30,
        'add1_alloc': 0.30,
        'add2_alloc': 0.40,
        'update_ref_on_add': False,  # 固定基准，闪电战
        'profit_target_pct': 0.15,  # 追踪激活线：15%
        'trailing_drop_pct': 0.08,  # 宽容回撤清仓线：最高点回落8%
        'verbose': True,
        'live_state': {
            'stage': 0,
            'cost_price': 0.0,
            'shares': 0
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
                from strategy_reporters import report_tsla
                report_tsla.generate_strategy_report(ticker=ticker, config=config)

            elif config['strategy'] == 'grid' and ticker == 'MSFT':
                # 🌟 新增路由：呼叫 MSFT 专属画图脚本
                from strategy_reporters import report_msft
                report_msft.generate_strategy_report(ticker=ticker, config=config)
                # 🌟 新增：接入 META 的路由逻辑
            elif config['strategy'] == 'grid' and ticker == 'META':
                from strategy_reporters import report_meta
                report_meta.generate_strategy_report(ticker=ticker, config=config)

            elif config['strategy'] == 'trend' and ticker == 'MU':
                # 注意：请确保你在 strategy_reporters 目录下创建了 report_mu.py
                # from strategy_reporters import report_mu
                from strategy_reporters import report_mu_v2_fixed_py39
                report_mu_v2_fixed_py39.generate_strategy_report(ticker=ticker, config=config)

            elif config['strategy'] == 'trend' and ticker == 'NVDA':
                from strategy_reporters import report_nvda
                report_nvda.generate_strategy_report(ticker=ticker, config=config)

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
