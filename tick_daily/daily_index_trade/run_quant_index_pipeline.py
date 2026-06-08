#!/usr/bin/env python3
"""
统一运行主控脚本
一键依次执行 Grok 版 和 Gemini 版的每日交易信号报告
"""

import sys
import time
from pathlib import Path
from daily_singal_generator_all_grok_v2 import generate_daily_report as run_grok
from daily_trade_radar_all_gemini_v2 import generate_daily_report as run_gemini


def main():
    print("=" * 80)
    print("🚀 量化交易信号统一运行管道启动")
    print("=" * 80)
    print()

    # ==================== 1. 运行 Grok 版报告 ====================
    print("📌 正在执行 Grok 版报告生成器...")
    try:

        run_grok()
        print("✅ Grok 版报告生成完成\n")
    except Exception as e:
        print(f"❌ Grok 版运行失败: {e}\n")

    time.sleep(1)  # 短暂间隔，让日志更清晰

    # ==================== 2. 运行 Gemini 版报告 ====================
    print("📌 正在执行 Gemini 版报告生成器...")
    try:

        run_gemini()
        print("✅ Gemini 版报告生成完成\n")
    except Exception as e:
        print(f"❌ Gemini 版运行失败: {e}\n")

    # ==================== 3. 结束提示 ====================
    print("=" * 80)
    print("🎉 所有宏观大盘量化报告已全部生成完成！")
    print(f"📂 请前往输出目录查看图表及文本指令。")
    print("=" * 80)


if __name__ == "__main__":
    main()
