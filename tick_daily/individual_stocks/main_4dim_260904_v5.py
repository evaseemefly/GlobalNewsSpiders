"""
个人量化投研报告入口 · 四维风险确认版（260904 v5）。

保留原网格/趋势策略和原 reporter 文件；新版入口把 VOO/QQQ 四维市场风险作为
报告第一页与最终执行覆盖层。Level 1 以上暂停个股新增/加仓，但既有止损、止盈、
趋势退出纪律继续有效。最终 PDF 使用 _4dim_260904_v5 后缀，不覆盖旧版报告文件。
"""

from __future__ import annotations

import importlib.util
import json
import shutil
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


VERSION_TAG = "4dim_260904_v5"
RISK_ENGINE_PATH = Path(__file__).parents[1] / "daily_index_trade" / (
    "daily_index_trade_signal_v3_cagr_aux_d_pdf_integrated_v2_4dim_260904_v5.py"
)


ASSET_CONFIG = {
    "TSLA": {
        "strategy": "grid", "rsi_entry_th": 50, "rsi_exit_th": 70, "drop1_pct": 0.05,
        "drop2_pct": 0.08, "ma_period": 200, "initial_alloc": 0.30, "add1_alloc": 0.30,
        "add2_alloc": 0.40, "update_ref_on_add": False, "profit_target_pct": 0.20,
        "trailing_drop_pct": 0.0, "verbose": False,
        "live_state": {"stage": 2, "cost_price": 394.48, "shares": 14, "cash": 6000},
    },
    "MSFT": {
        "strategy": "grid", "rsi_entry_th": 45, "rsi_exit_th": 80, "drop1_pct": 0.03,
        "drop2_pct": 0.05, "ma_period": 150, "initial_alloc": 0.30, "add1_alloc": 0.30,
        "add2_alloc": 0.40, "update_ref_on_add": False, "profit_target_pct": 0.10,
        "trailing_drop_pct": 0.05, "verbose": False,
        "live_state": {"stage": 2, "cost_price": 408.21, "shares": 12, "cash": 7000},
    },
    "META": {
        "strategy": "grid", "rsi_entry_th": 50, "rsi_exit_th": 80, "drop1_pct": 0.05,
        "drop2_pct": 0.05, "ma_period": 150, "initial_alloc": 0.30, "add1_alloc": 0.30,
        "add2_alloc": 0.40, "update_ref_on_add": False, "profit_target_pct": 0.15,
        "trailing_drop_pct": 0.08, "verbose": False,
        "live_state": {"stage": 0, "cost_price": 0.0, "shares": 0, "cash": 5000},
    },
    "MU": {
        "strategy": "trend", "entry_period": 30, "exit_period": 40, "alloc_pct": 0.95,
        "verbose": False, "live_first_tranche_pct": 0.35,
        "live_state": {"stage": 1, "cost_price": 1076.26, "shares": 2, "cash": 6000},
    },
    "NVDA": {
        "strategy": "trend", "entry_period": 30, "exit_period": 20, "alloc_pct": 0.95,
        "verbose": False, "live_first_tranche_pct": 0.35,
        "live_state": {"stage": 1, "cost_price": 182.583, "shares": 28, "cash": 8000},
    },
    "AVGO": {
        "strategy": "grid", "rsi_entry_th": 50, "rsi_exit_th": 80, "drop1_pct": 0.03,
        "drop2_pct": 0.05, "ma_period": 200, "initial_alloc": 0.30, "add1_alloc": 0.30,
        "add2_alloc": 0.40, "update_ref_on_add": False, "profit_target_pct": 0.15,
        "trailing_drop_pct": 0.08, "verbose": False,
        "live_state": {"stage": 1, "cost_price": 401.356, "shares": 5, "cash": 8000},
    },
}


def _load_risk_engine():
    spec = importlib.util.spec_from_file_location("individual_four_dim_risk_engine_v5", RISK_ENGINE_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载四维风险引擎: {RISK_ENGINE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_four_dim_snapshot(refresh: bool = True) -> Dict[str, Any]:
    engine = _load_risk_engine()
    latest = engine.OUTPUT_PATH / f"four_dim_risk_snapshot_{VERSION_TAG}_latest.json"
    if refresh:
        try:
            return engine.generate_daily_report()
        except Exception as exc:
            if not latest.exists():
                raise
            print(f"⚠️ 四维风险刷新失败，使用最近快照: {exc}")
    if not latest.exists():
        raise FileNotFoundError(f"找不到四维风险快照: {latest}")
    return json.loads(latest.read_text(encoding="utf-8"))


def _proxy_risk(snapshot: Dict[str, Any], ticker: str) -> Dict[str, Any]:
    # 当前 ASSET_CONFIG 均属于高科技/成长 Beta，统一使用 QQQ 作为执行代理。
    proxy = "QQQ"
    item = snapshot.get("assets", {}).get(proxy)
    if item is None:
        raise KeyError(f"四维风险快照缺少 {proxy}")
    return item


def _execution_overlay_text(ticker: str, risk: Dict[str, Any]) -> Tuple[str, str]:
    execution = risk["execution"]
    level = int(execution["level"])
    if level == 0:
        conclusion = "允许按个股原策略执行；仍采用分批建仓，不追涨。"
    elif level == 1:
        conclusion = "覆盖新增/加仓指令：今日 0 操作；已有止损、止盈和趋势退出继续有效。"
    elif level == 2:
        conclusion = "先降低 VOO/QQQ 核心 Beta 第一档；个股不新开仓、不加仓，并复核组合科技暴露。"
    else:
        conclusion = "Hard Risk-Off：停止个股新增/加仓；按纪律处理破位仓位并快速降低组合 Beta。"
    return execution["state"], conclusion


def print_execution_overlay(ticker: str, risk: Dict[str, Any]) -> None:
    state, conclusion = _execution_overlay_text(ticker, risk)
    votes = risk.get("votes", {})
    macro = votes.get("macro", {})
    credit = votes.get("credit", {})
    print("\n" + "🧩" * 28)
    print(f"【四维最终执行覆盖】{ticker} | {risk.get('asset')} 代理 | {state}")
    print(f"   • US10Y 20日动量: {macro.get('previous', 0):+.2f} → {macro.get('momentum', 0):+.2f} "
          f"({macro.get('change', 0):+.2f})")
    print(f"   • HYG/MA60偏离  : {credit.get('previous_deviation', 0):+.2%} → "
          f"{credit.get('deviation', 0):+.2%} ({credit.get('deviation_change', 0):+.2%})")
    print(f"   • 最终结论      : {conclusion}")
    print("🧩" * 28 + "\n")


def _render_overlay_pdf(ticker: str, risk: Dict[str, Any], output_path: Path) -> None:
    execution = risk["execution"]
    assessment = risk["assessment"]
    votes = risk["votes"]
    state, conclusion = _execution_overlay_text(ticker, risk)
    colors = {"green": "#27ae60", "yellow": "#f39c12", "red": "#c0392b"}

    with PdfPages(output_path) as pdf:
        fig = plt.figure(figsize=(16, 24))
        fig.patch.set_facecolor("#f7f9fb")
        fig.text(0.07, 0.94, f"{ticker} 投研报告 · 四维最终执行页", fontsize=28, fontweight="bold", color="#233044")
        fig.text(0.07, 0.91, f"市场数据日: {risk.get('trade_date')}  |  QQQ 风险代理  |  {VERSION_TAG}", fontsize=13, color="#667085")
        fig.text(0.07, 0.855, state, fontsize=22, fontweight="bold", color="#233044")
        fig.text(0.07, 0.815, conclusion, fontsize=14, color="#344054")

        for idx, (key, label) in enumerate([
            ("price", "价格趋势"), ("macro", "宏观利率"),
            ("credit", "信用 HYG"), ("volatility_breadth", "波动/广度"),
        ]):
            vote = votes[key]
            y = 0.74 - idx * 0.075
            fig.text(0.08, y, "●", color=colors[vote["status"]], fontsize=27, va="center")
            fig.text(0.12, y, label, fontsize=15, fontweight="bold", va="center")
            fig.text(0.27, y, vote["detail"], fontsize=13, color="#475467", va="center")

        macro = votes["macro"]
        credit = votes["credit"]
        vol = votes["volatility_breadth"]
        fig.text(0.07, 0.39, "边际变化", fontsize=17, fontweight="bold", color="#233044")
        fig.text(
            0.07,
            0.29,
            f"US10Y 20日动量  {macro['previous']:+.2f} → {macro['momentum']:+.2f}  ({macro['change']:+.2f})\n"
            f"HYG/MA60偏离    {credit['previous_deviation']:+.2%} → {credit['deviation']:+.2%}  "
            f"({credit['deviation_change']:+.2%})\n"
            f"VIX             {vol['previous_vix']:.2f} → {vol['vix']:.2f}  ({vol['vix_change']:+.2f})",
            fontsize=15,
            linespacing=1.6,
            color="#233044",
        )
        fig.text(
            0.07,
            0.15,
            f"票数：{assessment['green_count']}绿 + {assessment['yellow_count']}黄 + {assessment['red_count']}红\n"
            f"防抖：风险连续 {assessment['risk_streak']} 日；恢复连续 {assessment['recovery_streak']} 日\n"
            "原则：Risk-Off ≠ Sell；单一类别风险不得触发大规模仓位变化。",
            fontsize=13,
            linespacing=1.6,
            color="#475467",
        )
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)


def _merge_overlay_with_report(overlay: Path, source_report: Path, output_report: Path) -> bool:
    try:
        try:
            from pypdf import PdfReader, PdfWriter
        except ImportError:
            from PyPDF2 import PdfReader, PdfWriter
        writer = PdfWriter()
        for page in PdfReader(str(overlay)).pages:
            writer.add_page(page)
        for page in PdfReader(str(source_report)).pages:
            writer.add_page(page)
        with output_report.open("wb") as stream:
            writer.write(stream)
        return True
    except ImportError:
        return False


@contextmanager
def _inject_four_dim_banner(risk: Dict[str, Any]):
    """给旧 reporter 生成的单页图加四维最终执行横幅，无需修改旧 reporter。"""
    from matplotlib.figure import Figure

    original_savefig = Figure.savefig
    execution = risk["execution"]
    macro = risk["votes"]["macro"]
    _, conclusion = _execution_overlay_text(risk.get("asset", "QQQ"), risk)

    def savefig_with_banner(fig, *args, **kwargs):
        fig.text(
            0.5,
            0.985,
            f"4D FINAL: {execution['state']} | US10Y环比 {macro['change']:+.2f} | {conclusion}",
            ha="center",
            va="top",
            fontsize=10.5,
            fontweight="bold",
            color="#b42318" if int(execution["level"]) >= 2 else "#475467",
            bbox={"boxstyle": "round,pad=0.28", "facecolor": "#fff7ed", "edgecolor": "#fed7aa", "alpha": 0.96},
        )
        return original_savefig(fig, *args, **kwargs)

    Figure.savefig = savefig_with_banner
    try:
        yield
    finally:
        Figure.savefig = original_savefig


def _report_paths(module, ticker: str, strategy: str) -> Tuple[Path, Path, Path]:
    data_dir = Path(module.CONFIG["ind_stock_dir"])
    legacy_name = f"{ticker}_{'grid' if strategy == 'grid' else 'trend'}_strategy_report.pdf"
    legacy_path = data_dir / legacy_name
    output_dir = data_dir / VERSION_TAG
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / f"{legacy_path.stem}_{VERSION_TAG}.pdf"
    overlay = output_dir / f"{ticker}_four_dim_execution_cover.pdf"
    return legacy_path, output, overlay


def main(refresh_risk: bool = True) -> None:
    print("=" * 68)
    print("🚀 [个人量化对冲基金] 四维风险整合投研流水线启动")
    print("=" * 68)
    snapshot = load_four_dim_snapshot(refresh=refresh_risk)

    for ticker, base_config in ASSET_CONFIG.items():
        config = dict(base_config)
        risk = _proxy_risk(snapshot, ticker)
        config["four_dim_risk"] = risk
        print(f"\n▶️ [正在调度] 处理 {ticker}...")
        print_execution_overlay(ticker, risk)

        try:
            if config["strategy"] == "grid":
                from strategy_reporters import report_grid_v2 as reporter
            elif config["strategy"] == "trend":
                from strategy_reporters import report_trend_v2 as reporter
            else:
                print(f"⚠️ {ticker} 的策略类型 '{config['strategy']}' 未知！")
                continue

            legacy_path, output_path, overlay_path = _report_paths(reporter, ticker, config["strategy"])
            with _inject_four_dim_banner(risk):
                reporter.generate_strategy_report(ticker=ticker, config=config)
            print_execution_overlay(ticker, risk)  # 原策略信号之后再次明确最终覆盖结论。

            if not legacy_path.exists():
                raise FileNotFoundError(f"原策略报告未生成: {legacy_path}")
            _render_overlay_pdf(ticker, risk, overlay_path)
            if _merge_overlay_with_report(overlay_path, legacy_path, output_path):
                overlay_path.unlink(missing_ok=True)
                print(f"✅ 四维整合投研报告: {output_path}")
            else:
                shutil.copy2(legacy_path, output_path)
                print(f"⚠️ 未安装 pypdf/PyPDF2；策略报告已复制到 {output_path}")
                print(f"📄 四维执行页单独保存: {overlay_path}")
            time.sleep(1)
        except Exception as exc:
            print(f"❌ 处理 {ticker} 时发生错误: {exc}")
            continue

    print("\n" + "=" * 68 + "\n🎉 今日四维整合投研任务执行完毕！\n" + "=" * 68)


if __name__ == "__main__":
    main()
