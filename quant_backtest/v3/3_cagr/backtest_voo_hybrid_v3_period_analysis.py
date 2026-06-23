import importlib.util
from itertools import product
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd


V3_CAGR_SCRIPT = Path(
    __file__
).with_name(
    "backtest_voo_hybrid_v3_cagr_constraints.py"
)
CSV_FILE_PATH = Path(
    "/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders/broad_market_history/historical_broad_market_master.csv"
)
OUTPUT_DIR = Path(__file__).parent
FINAL_MAX_DD_LIMIT = -0.18
CALMAR_FLOOR = 0.60
FINAL_SELECTED_PARAMS = {
    "ma_len": 150,
    "us10y_th": 0.08,
    "vix_th": 38,
    "rsi_th": 32,
    "risk_pos": 0.3,
}

PERIODS = [
    ("全样本", None, None),
    ("训练段 2014-2021", "2014-01-01", "2021-12-31"),
    ("样本外 2022-至今", "2022-01-01", None),
    ("COVID 2020", "2020-01-01", "2020-12-31"),
    ("加息熊市 2022", "2022-01-01", "2022-12-31"),
    ("近三年 2023-至今", "2023-01-01", None),
]


def load_module(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def calc_metrics_from_returns(returns: pd.Series, exposure: pd.Series) -> dict:
    returns = returns.dropna()
    if returns.empty:
        return {
            "total_return": 0.0,
            "cagr": 0.0,
            "max_dd": 0.0,
            "calmar": 0.0,
            "avg_exposure": 0.0,
            "dd_peak": pd.NaT,
            "dd_trough": pd.NaT,
        }

    returns = returns.copy()
    returns.iloc[0] = 0.0
    nav = (1 + returns).cumprod()
    total_return = nav.iloc[-1] - 1
    days = (nav.index[-1] - nav.index[0]).days
    years = days / 365.25
    cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 and total_return > -1 else 0.0

    roll_max = nav.cummax()
    drawdown = nav / roll_max - 1
    max_dd = drawdown.min()
    dd_trough = drawdown.idxmin()
    dd_peak = nav.loc[:dd_trough].idxmax()
    calmar = cagr / abs(max_dd) if max_dd < 0 else 0.0

    exposure = exposure.reindex(returns.index).dropna()
    avg_exposure = exposure.mean() * 100 if not exposure.empty else 0.0

    return {
        "total_return": total_return,
        "cagr": cagr,
        "max_dd": max_dd,
        "calmar": calmar,
        "avg_exposure": avg_exposure,
        "dd_peak": dd_peak,
        "dd_trough": dd_trough,
    }


def filter_real_trading_days(df: pd.DataFrame, asset: str = "VOO") -> pd.DataFrame:
    """
    过滤真实交易日，避免周末 ffill 复制价格导致日涨跌幅和信号滞后失真。
    """
    df = df.copy()
    close_col = f"{asset}_close"
    volume_col = f"{asset}_volume"

    if close_col not in df.columns:
        raise KeyError(f"缺少字段: {close_col}")

    df = df[df[close_col].notna()]
    df = df[df.index.dayofweek < 5]

    if volume_col in df.columns:
        df = df[df[volume_col] > 0]

    return df.ffill()


def calc_period_metrics(df: pd.DataFrame, position: pd.Series, start: Optional[str], end: Optional[str]) -> dict:
    returns = position * df["VOO_close"].pct_change().fillna(0)
    if start:
        returns = returns.loc[returns.index >= pd.Timestamp(start)]
    if end:
        returns = returns.loc[returns.index <= pd.Timestamp(end)]
    return calc_metrics_from_returns(returns, position)


def calc_period_nav(df: pd.DataFrame, position: pd.Series, start: Optional[str], end: Optional[str]) -> pd.Series:
    returns = position * df["VOO_close"].pct_change().fillna(0)
    if start:
        returns = returns.loc[returns.index >= pd.Timestamp(start)]
    if end:
        returns = returns.loc[returns.index <= pd.Timestamp(end)]
    if returns.empty:
        return pd.Series(dtype=float)

    returns = returns.copy()
    returns.iloc[0] = 0.0
    return (1 + returns).cumprod()


def sort_for_cagr_constraint(results: list) -> list:
    return sorted(
        results,
        key=lambda x: (x["cagr"], x["calmar"], x["max_dd"], -x["avg_exposure"]),
        reverse=True,
    )


def format_params(r: dict) -> str:
    return (
        f"MA{r['ma_len']} | US10Y>{r['us10y_th']} | "
        f"VIX>{r['vix_th']} | RSI<{r['rsi_th']} | risk_pos={r['risk_pos']}"
    )


def find_result_by_params(results: list, params: dict) -> dict:
    for r in results:
        if (
                r["ma_len"] == params["ma_len"]
                and r["us10y_th"] == params["us10y_th"]
                and r["vix_th"] == params["vix_th"]
                and r["rsi_th"] == params["rsi_th"]
                and r["risk_pos"] == params["risk_pos"]
        ):
            return r
    raise ValueError(f"未找到最终选定参数: {params}")


def format_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def format_date(value) -> str:
    return value.strftime("%Y-%m-%d") if pd.notna(value) else ""


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    headers = list(df.columns)
    rows = df.astype(str).values.tolist()
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def safe_filename(text: str) -> str:
    replacements = {
        " ": "_",
        "/": "_",
        "-": "_",
        "至": "to",
    }
    safe = text
    for old, new in replacements.items():
        safe = safe.replace(old, new)
    return safe


def plot_period_navs(df: pd.DataFrame, strategies: list, output_dir: Path) -> list:
    output_paths = []

    for period_name, start, end in PERIODS:
        fig, ax = plt.subplots(figsize=(14, 7))

        for strategy in strategies:
            nav = calc_period_nav(df, strategy["position"], start, end)
            if nav.empty:
                continue

            metrics = calc_period_metrics(df, strategy["position"], start, end)
            label = (
                f"{strategy['name']} | CAGR {metrics['cagr'] * 100:.2f}% | "
                f"DD {metrics['max_dd'] * 100:.2f}%"
            )
            ax.plot(nav.index, nav, label=label, linewidth=strategy.get("linewidth", 2.2), alpha=strategy.get("alpha", 1.0))

        ax.set_title(f"{period_name}：净值走势对比", fontsize=15, fontweight="bold")
        ax.set_ylabel("Normalized NAV")
        ax.grid(True, alpha=0.35)
        ax.legend(loc="upper left", fontsize=9)

        output_path = output_dir / f"v3_period_nav_{safe_filename(period_name)}.png"
        plt.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        output_paths.append(output_path)

    return output_paths


def main() -> None:
    mod = load_module(V3_CAGR_SCRIPT)

    df = pd.read_csv(CSV_FILE_PATH)
    df["trade_date_utc"] = pd.to_datetime(df["trade_date_utc"])
    df = df.set_index("trade_date_utc").sort_index()
    df = filter_real_trading_days(df, asset="VOO")

    ma_lens = [80, 100, 120, 150, 180, 200]
    us10y_ths = [0.08, 0.10, 0.12, 0.15]
    vix_ths = [35, 38, 40, 45]
    rsi_ths = [30, 32, 35]
    risk_poss = [0.3, 0.4, 0.5, 0.6, 0.7]

    results = []
    for ma, us_th, vix_th, rsi_th, risk_pos in product(ma_lens, us10y_ths, vix_ths, rsi_ths, risk_poss):
        results.append(mod.run_strategy(df, ma, us_th, vix_th, rsi_th, risk_pos))

    calmar_best = sorted(
        results,
        key=lambda x: (x["calmar"], x["cagr"], -x["avg_exposure"]),
        reverse=True,
    )[0]
    final_selected = find_result_by_params(results, FINAL_SELECTED_PARAMS)

    strategies = [
        {
            "name": "Buy&Hold VOO",
            "params": "100% VOO",
            "position": pd.Series(1.0, index=df.index),
        },
        {
            "name": "v3 Calmar最优",
            "params": format_params(calmar_best),
            "position": calmar_best["position"],
        },
        {
            "name": "v3_cagr最终选定",
            "params": (
                f"{format_params(final_selected)} | "
                f"selection: MaxDD>={FINAL_MAX_DD_LIMIT * 100:.0f}%, Calmar>={CALMAR_FLOOR:.2f}, OOS优先"
            ),
            "position": final_selected["position"],
        },
    ]

    plot_strategies = [
        {
            "name": "Buy&Hold VOO",
            "params": "100% VOO",
            "position": pd.Series(1.0, index=df.index),
            "linewidth": 2.0,
            "alpha": 0.8,
        },
        {
            "name": "原始v3 Calmar最优",
            "params": format_params(calmar_best),
            "position": calmar_best["position"],
            "linewidth": 2.4,
            "alpha": 0.95,
        },
        {
            "name": "v3_cagr最终选定",
            "params": format_params(final_selected),
            "position": final_selected["position"],
            "linewidth": 2.8,
            "alpha": 1.0,
        },
    ]

    rows = []
    for period_name, start, end in PERIODS:
        for strategy in strategies:
            metrics = calc_period_metrics(df, strategy["position"], start, end)
            rows.append(
                {
                    "period": period_name,
                    "strategy": strategy["name"],
                    "params": strategy["params"],
                    "total_return": metrics["total_return"],
                    "cagr": metrics["cagr"],
                    "max_dd": metrics["max_dd"],
                    "calmar": metrics["calmar"],
                    "avg_exposure": metrics["avg_exposure"],
                    "dd_peak": format_date(metrics["dd_peak"]),
                    "dd_trough": format_date(metrics["dd_trough"]),
                }
            )

    report = pd.DataFrame(rows)
    csv_path = OUTPUT_DIR / "v3_period_analysis_vs_buyhold.csv"
    md_path = OUTPUT_DIR / "v3_period_analysis_vs_buyhold.md"
    report.to_csv(csv_path, index=False)

    display = report.copy()
    for col in ["total_return", "cagr", "max_dd"]:
        display[col] = display[col].map(format_pct)
    display["calmar"] = display["calmar"].map(lambda x: f"{x:.2f}")
    display["avg_exposure"] = display["avg_exposure"].map(lambda x: f"{x:.1f}%")

    with md_path.open("w") as f:
        f.write("# V3 阶段回测对照\n\n")
        f.write(
            "对比对象：Buy&Hold VOO、v3 Calmar 最优、以及最终人工选定的 v3_cagr 方案。\n\n"
            f"v3_cagr 最终方案：{format_params(final_selected)}；"
            f"筛选约束 MaxDD>={FINAL_MAX_DD_LIMIT * 100:.0f}%，"
            f"Calmar>={CALMAR_FLOOR:.2f}，样本外优先。\n\n"
        )
        for period_name, _, _ in PERIODS:
            f.write(f"## {period_name}\n\n")
            part = display[display["period"] == period_name].drop(columns=["period", "params"])
            f.write(dataframe_to_markdown(part))
            f.write("\n\n")
        f.write("## 参数说明\n\n")
        params = display[["strategy", "params"]].drop_duplicates()
        f.write(dataframe_to_markdown(params))
        f.write("\n")

    print(f"阶段回测 CSV 已保存: {csv_path}")
    print(f"阶段回测 Markdown 已保存: {md_path}")

    plot_paths = plot_period_navs(df, plot_strategies, OUTPUT_DIR)
    print("阶段净值走势对比图已保存:")
    for path in plot_paths:
        print(f"  {path}")

    for period_name, _, _ in PERIODS:
        print(f"\n=== {period_name} ===")
        part = display[display["period"] == period_name].drop(columns=["period", "params"])
        print(part.to_string(index=False))


if __name__ == "__main__":
    main()
