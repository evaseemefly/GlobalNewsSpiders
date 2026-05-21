import sys
import time
from pathlib import Path

# ==================== 1. 全局资产配置字典 ====================
ASSET_CONFIG = {
    # 'TSLA': {
    #     'strategy': 'grid',
    #     # --- TSLA 网格最优参数 ---
    #     'rsi_entry_th': 50,
    #     'rsi_exit_th': 70,
    #     'drop1_pct': 0.05,
    #     'drop2_pct': 0.08,
    #     'ma_period': 200,
    #     'initial_alloc': 0.30,
    #     'add1_alloc': 0.30,
    #     'add2_alloc': 0.40,
    #     'update_ref_on_add': False,
    #     'profit_target_pct': 0.20,
    #     'verbose': True,
    #     # --- 实盘账户状态注入 ---
    #     'live_state': {
    #         'stage': 2,           # 真实持仓阶段
    #         'cost_price': 404.0,  # 真实成本价
    #         'shares': 12          # 真实持股数
    #     }
    # },
    'MU': {
        'strategy': 'trend',
        # --- MU 海龟非对称趋势最优参数 ---
        'entry_period': 30,       # 敏锐进场：突破过去30日最高点买入
        'exit_period': 40,        # 宽容防守：跌破过去40日最低点卖出
        'alloc_pct': 0.95,        # 动用95%仓位防滑点
        'verbose': True,
        # --- 实盘账户状态注入 ---
        'live_state': {
            'stage': 0,           # 0 代表当前实盘空仓，等待发车
            'cost_price': 0.0,    # 空仓状态下成本价设为 0
            'shares': 0           # 空仓状态下股数设为 0
        }
    },
    # 💡 未来你跑完 NVDA 寻优后，可以直接取消注释并填入参数
    # 'NVDA': {
    #     'strategy': 'trend',
    #     'entry_period': 50,
    #     'exit_period': 20,
    #     'alloc_pct': 0.95,
    #     'verbose': True,
    #     'live_state': None
    # }
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

            elif config['strategy'] == 'trend' and ticker == 'MU':
                # 注意：请确保你在 strategy_reporters 目录下创建了 report_mu.py
                from strategy_reporters import report_mu
                report_mu.generate_strategy_report(ticker=ticker, config=config)

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