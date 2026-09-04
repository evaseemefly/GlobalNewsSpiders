"""
个股行情下载 + 四维风险确认 + 多周期 PDF 流水线（260904 v5）。

旧版 downloader / reporter 保持不变。本入口先刷新统一的 VOO/QQQ 四维风险快照，
再把风险摘要作为新版 PDF 的首页和每个标的页面的执行提示。
"""

from __future__ import annotations

import importlib.util
import json
import sys
import time
import types
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

VERSION_TAG = "4dim_260904_v5"
RISK_ENGINE_PATH = Path(__file__).parents[1] / "daily_index_trade" / (
    "daily_index_trade_signal_v3_cagr_aux_d_pdf_integrated_v2_4dim_260904_v5.py"
)


def _load_risk_engine():
    spec = importlib.util.spec_from_file_location("four_dim_risk_engine_v5", RISK_ENGINE_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载四维风险引擎: {RISK_ENGINE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_sibling_module(filename: str, module_name: str):
    # 旧 reporter 只为日期格式依赖 arrow；没有安装时提供等价的最小兼容层。
    if filename == "generate_stock_report_pdf_v2.py" and "arrow" not in sys.modules:
        try:
            __import__("arrow")
        except ImportError:
            arrow_compat = types.ModuleType("arrow")

            class _ArrowNow:
                @staticmethod
                def format(pattern: str) -> str:
                    return datetime.now().strftime(pattern.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d"))

            arrow_compat.now = lambda: _ArrowNow()
            sys.modules["arrow"] = arrow_compat
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载模块: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def refresh_four_dim_snapshot() -> Dict[str, Any]:
    """运行统一风险引擎；失败时读取最近一次快照，避免整条个股流水线中断。"""
    engine = _load_risk_engine()
    try:
        return engine.generate_daily_report()
    except Exception as exc:
        latest = engine.OUTPUT_PATH / f"four_dim_risk_snapshot_{VERSION_TAG}_latest.json"
        if latest.exists():
            print(f"⚠️ 四维风险刷新失败，改用最近快照: {exc}")
            return json.loads(latest.read_text(encoding="utf-8"))
        raise RuntimeError(f"四维风险刷新失败且没有可用快照: {exc}") from exc


def _asset_risk_for_ticker(snapshot: Dict[str, Any], ticker: str) -> Dict[str, Any]:
    """科技/成长标的参考 QQQ，其余标的参考 VOO。"""
    qqq_proxy = {
        "QQQ", "MSFT", "NVDA", "GOOGL", "DRAM", "AVGO", "AAPL", "APH", "ANET",
        "CSCO", "WDC", "SOXX", "TSLA", "MU", "ASML", "AMZN", "MAGS", "SMH", "META",
    }
    proxy = "QQQ" if ticker.upper() in qqq_proxy else "VOO"
    return snapshot.get("assets", {}).get(proxy, {})


def _draw_four_dim_cover(snapshot: Dict[str, Any]):
    legacy_reporter = _load_sibling_module("generate_stock_report_pdf_v2.py", "legacy_stock_reporter_v2_cover")
    plt = legacy_reporter.plt
    fig = plt.figure(figsize=(11.69, 8.27))
    fig.patch.set_facecolor("#f7f9fb")
    fig.text(0.06, 0.90, "个股多周期报告 · 四维风险执行首页", fontsize=26, fontweight="bold", color="#233044")
    fig.text(
        0.06,
        0.855,
        f"市场数据日: {snapshot.get('trade_date', 'N/A')}  |  报告生成: {datetime.now():%Y-%m-%d}  |  {VERSION_TAG}",
        fontsize=11,
        color="#667085",
    )

    rows = []
    for asset in ["VOO", "QQQ"]:
        item = snapshot.get("assets", {}).get(asset, {})
        execution = item.get("execution", {})
        votes = item.get("votes", {})
        macro = votes.get("macro", {})
        credit = votes.get("credit", {})
        rows.append(
            [
                asset,
                execution.get("state", "N/A"),
                "/".join(votes.get(key, {}).get("status", "?")[0].upper() for key in ["price", "macro", "credit", "volatility_breadth"]),
                f"{macro.get('momentum', 0):+.2f} ({macro.get('change', 0):+.2f})",
                f"{credit.get('deviation', 0):+.2%}",
                execution.get("action", "N/A"),
            ]
        )

    ax = fig.add_axes([0.055, 0.43, 0.89, 0.34])
    ax.axis("off")
    table = ax.table(
        cellText=rows,
        colLabels=["代理", "防抖后等级", "四维 P/M/C/VB", "US10Y动量(环比)", "HYG偏离MA60", "最终动作"],
        cellLoc="center",
        colWidths=[0.07, 0.20, 0.13, 0.17, 0.14, 0.29],
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.0, 2.0)
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#d0d5dd")
        if row == 0:
            cell.set_facecolor("#344054")
            cell.set_text_props(color="white", weight="bold")
        else:
            cell.set_facecolor("#ffffff")

    fig.text(0.06, 0.34, "执行规则", fontsize=15, fontweight="bold", color="#233044")
    fig.text(
        0.06,
        0.27,
        "Level 0：正常交易\nLevel 1：停止新增仓位，不减仓\n"
        "Level 2：跨类别且连续两日确认，先减核心 Beta 第一档\n"
        "Level 3：价格结构与多维风险同步恶化，快速防守",
        fontsize=12,
        linespacing=1.5,
        color="#475467",
    )
    fig.text(
        0.06,
        0.08,
        "注意：个股技术图仍是风险雷达；四维层只覆盖新增仓位动作，不取消个股既有止损/止盈纪律。",
        fontsize=10.5,
        color="#667085",
    )
    return fig


def generate_stock_report_with_four_dim(
    target_stocks,
    snapshot: Dict[str, Any],
) -> Path:
    """复用旧绘图引擎，生成不会覆盖旧报告的四维整合版 PDF。"""
    legacy_reporter = _load_sibling_module("generate_stock_report_pdf_v2.py", "legacy_stock_reporter_v2")
    report_dir = legacy_reporter.CONFIG["report_dir"] / VERSION_TAG
    report_dir.mkdir(parents=True, exist_ok=True)
    current_date = datetime.now().strftime("%Y_%m_%d")
    output_path = report_dir / f"Stock_MultiPeriod_Report_{current_date}_{VERSION_TAG}.pdf"

    print(f"🚀 开始生成四维整合个股报告: {output_path.name}")
    with legacy_reporter.PdfPages(output_path) as pdf:
        cover = _draw_four_dim_cover(snapshot)
        pdf.savefig(cover)
        legacy_reporter.plt.close(cover)

        for ticker in target_stocks:
            file_path = legacy_reporter.CONFIG["ind_stock_dir"] / f"individual_stocks_master_{ticker}.csv"
            if not file_path.exists():
                print(f"⚠️ 跳过 {ticker}: 找不到历史数据文件")
                continue
            print(f"📊 正在处理 {ticker}...")
            df = legacy_reporter.pd.read_csv(file_path)
            fig = legacy_reporter.create_chart_figure(ticker, df, 200)
            if fig is None:
                continue

            risk = _asset_risk_for_ticker(snapshot, ticker)
            execution = risk.get("execution", {})
            fig.text(
                0.5,
                0.012,
                f"四维执行覆盖（{risk.get('asset', 'N/A')}代理）：{execution.get('state', 'N/A')} | "
                f"{execution.get('action', 'N/A')}",
                ha="center",
                fontsize=10,
                color="#b42318" if execution.get("level", 0) >= 2 else "#475467",
                fontweight="bold",
            )
            pdf.savefig(fig)
            legacy_reporter.plt.close(fig)

    print(f"✅ 四维整合个股报告已生成: {output_path}")
    return output_path


def run_pipeline(is_need_download: bool = True, refresh_risk: bool = True) -> Optional[Path]:
    print("=" * 68)
    print("🌟 DRCC Quant Engine · 四维风险整合流水线 260904 v5")
    print("=" * 68)

    target_stocks = [
        "MSFT", "NVDA", "GOOGL", "DRAM", "AVGO", "AAPL", "APH", "ANET", "CSCO", "WDC", "SOXX",
        "QQQ", "VOO", "SPY", "TSLA", "MU", "ASML", "AMZN", "MAGS", "SMH", "META",
    ]
    start_date = "2023-01-01"
    print(f"📌 当前全局监控股票池: {target_stocks}\n")

    try:
        if is_need_download:
            print(">> [阶段 1/3] 下载个股行情并更新技术指标...")
            downloader = _load_sibling_module("indivalual_stocks_download_v2.py", "legacy_stock_downloader_v2")
            downloader.main(target_stocks=target_stocks, start_date=start_date)
            time.sleep(2)
        else:
            print(">> [阶段 1/3] 跳过个股行情下载。")

        print(">> [阶段 2/3] 刷新四维风险、边际变化与防抖状态...")
        if refresh_risk:
            snapshot = refresh_four_dim_snapshot()
        else:
            engine = _load_risk_engine()
            latest = engine.OUTPUT_PATH / f"four_dim_risk_snapshot_{VERSION_TAG}_latest.json"
            if not latest.exists():
                raise FileNotFoundError(f"找不到四维风险快照: {latest}")
            snapshot = json.loads(latest.read_text(encoding="utf-8"))

        print(">> [阶段 3/3] 生成带四维执行首页的个股 PDF...")
        result = generate_stock_report_with_four_dim(target_stocks, snapshot)
        print("\n🎉 四维整合流水线执行完毕！")
        return result
    except Exception as exc:
        print(f"\n❌ 流水线执行过程中遇到严重错误: {exc}")
        return None


if __name__ == "__main__":
    run_pipeline(True)
