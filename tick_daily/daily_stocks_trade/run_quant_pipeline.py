"""
汇总流水线脚本 (Pipeline)
功能：统一管理股票池，按顺序执行数据下载和PDF报告生成
"""
import time

# 1. 通过 import 引用原有的两个独立脚本
# 注意：导入时，Python会自动执行被导入脚本中暴露在全局的代码（比如环境变量设置、CONFIG初始化等）
# 但不会执行 if __name__ == "__main__": 里的代码。
import indivalual_stocks_download_v2 as downloader
import generate_stock_report_pdf_v2 as reporter


def run_pipeline(is_need_download: bool = True):
    print("=" * 60)
    print("    🌟 DRCC Quant Engine - 自动化投研流水线启动 🌟")
    print("=" * 60)

    # ---------------------------------------------------------
    # 核心变量集中管理区：
    # 以后您只需要在这个主脚本里修改股票池，两个子脚本都会同步生效
    # 这里我们放入您之前提到的核心颠覆和高弹性标的
    # ---------------------------------------------------------
    GLOBAL_TARGET_STOCKS = ['META', 'MSFT', 'NVDA', 'TSLA', 'MU', 'ASML', 'AMZN', 'MAGS', 'SMH', 'DRAM', 'SOXX', 'QQQ',
                            'VOO', 'SPY']
    # GLOBAL_TARGET_STOCKS = ['MAGS', 'SMH', 'DRAM', 'SOXX', 'QQQ', 'VOO', 'SPY']
    GLOBAL_START_DATE = "2023-01-01"

    print(f"📌 当前设定的全局监控股票池: {GLOBAL_TARGET_STOCKS}\n")

    try:
        # ==================== 阶段 1：数据下载与指标计算 ====================
        print("-" * 60)
        print(">> [阶段 1/2] 正在调用下载模块获取最新行情并计算技术指标...")
        print("-" * 60)
        if is_need_download:
            # 调用 downloader 脚本中的 main 方法，并将统一的变量传给它
            downloader.main(target_stocks=GLOBAL_TARGET_STOCKS, start_date=GLOBAL_START_DATE)

            # 增加一点缓冲时间，确保文件系统 I/O 写入完成
            time.sleep(2)

            # ==================== 阶段 2：PDF 报告渲染与生成 ====================
            print("\n" + "-" * 60)
            print(">> [阶段 2/2] 正在调用报告模块，将最新数据渲染为可视化 PDF...")
            print("-" * 60)

        else:
            print(print(">> [阶段 1/2] 跳过重新下载个股数据技术指标..."))
        # 调用 reporter 脚本中的 main 方法，使用同样的股票池
        reporter.main(target_stocks=GLOBAL_TARGET_STOCKS)

        print("\n" + "=" * 60)
        print(" 🎉 自动化流水线执行完毕！")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 流水线执行过程中遇到严重错误: {e}")


if __name__ == "__main__":
    run_pipeline(True)
