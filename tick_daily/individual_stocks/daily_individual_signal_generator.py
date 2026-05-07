import pandas as pd
from pathlib import Path
from enum import Enum, auto
import random
import time
from datetime import datetime

# ==================== 1. 环境配置（与你现有代码完全一致） ====================
class EnvType(Enum):
    HOME = auto()
    WORK = auto()

def get_env_config(env: EnvType) -> dict:
    if env == EnvType.HOME:
        base_path = Path("/Users/evaseemefly/03data/05-spiders")
    elif env == EnvType.WORK:
        base_path = Path("/Volumes/DRCC_DATA/11SPIDER_DATA/05-spiders")
    else:
        raise ValueError(f"未知的环境类型: {env}")

    config = {
        'ind_stock_dir': base_path / "individual_stocks",
        'fundamental_dir': base_path / "output/trade_msg/fundamental",
        'master_macro': base_path / "broad_market_history/historical_broad_market_master.csv",
        'trade_msg_dir': base_path / "output/trade_msg",          # 信号文件统一保存在 trade_msg 目录
    }
    return config

CURRENT_ENV = EnvType.WORK
CONFIG = get_env_config(CURRENT_ENV)

print(f"⚙️ 运行环境: [{CURRENT_ENV.name}]")
print(f"📂 信号文件将保存至: {CONFIG['trade_msg_dir']}\n")

# ==================== 2. RSI 计算函数 ====================
def calculate_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]

# ==================== 3. 个股每日信号生成器（生成 TXT 文件） ====================
def generate_daily_signal(ticker: str):
    price_file = CONFIG['ind_stock_dir'] / f"individual_stocks_master_{ticker}.csv"
    fund_file = CONFIG['fundamental_dir'] / f"fundamental_score_{ticker}.csv"
    macro_file = CONFIG['master_macro']

    if not price_file.exists() or not fund_file.exists():
        print(f"❌ {ticker} 的价格或基本面文件不存在")
        return None

    # 1. 读取价格数据
    price_df = pd.read_csv(price_file)
    price_df['trade_date_utc'] = pd.to_datetime(price_df['trade_date_utc'])
    price_df = price_df.sort_values('trade_date_utc').set_index('trade_date_utc')

    # 2. 读取最新基本面得分
    fund_df = pd.read_csv(fund_file)
    latest_fund = fund_df.iloc[-1] if not fund_df.empty else {'fundamental_score': 0}

    # 3. 读取最新宏观数据
    macro_df = pd.read_csv(macro_file)
    macro_df['trade_date_utc'] = pd.to_datetime(macro_df['trade_date_utc'])
    latest_macro = macro_df.iloc[-1]

    # 4. 计算技术指标
    latest_price = price_df.iloc[-1]
    ma200 = price_df[f'{ticker}_close'].rolling(200).mean().iloc[-1]
    rsi14 = calculate_rsi(price_df[f'{ticker}_close'])

    # 5. 信号逻辑
    fund_score = latest_fund['fundamental_score']
    price_above_ma = latest_price[f'{ticker}_close'] > ma200
    is_dip_buy = (rsi14 < 32) and (latest_price[f'{ticker}_close'] > latest_price[f'{ticker}_open'])

    vix_high = latest_macro.get('VIX_close', 0) > 40
    risk_off = vix_high

    if fund_score < 50:
        suggestion = "⚠️ 基本面较差，建议回避或轻仓观察"
        position = "0% ~ 30%"
    elif risk_off:
        suggestion = "⚠️ 宏观风险较高，建议减仓"
        position = "30% ~ 50%"
    elif is_dip_buy and price_above_ma:
        suggestion = "🛒 极值抄底信号 + 趋势健康"
        position = "100%（加仓）"
    elif price_above_ma:
        suggestion = "✅ 趋势健康，可继续持有"
        position = "80% ~ 100%"
    else:
        suggestion = "⏳ 趋势不明，建议观望"
        position = "0% ~ 50%"

    today = datetime.now().strftime("%Y-%m-%d")

    # ==================== 生成信号卡片 ====================
    signal_text = f"""【{ticker}  {today} 每日信号】
============================================================
基本面得分: {fund_score} / 100
最新收盘价: {latest_price[f'{ticker}_close']:.2f} | MA200: {ma200:.2f}
RSI(14): {rsi14:.1f} | VIX: {latest_macro.get('VIX_close', 'N/A')}
趋势: {'🟢 多头' if price_above_ma else '🔴 空头/震荡'}
综合建议: {suggestion}
建议仓位: {position}
============================================================
"""

    # 保存为 TXT 文件
    txt_path = CONFIG['trade_msg_dir'] / f"{ticker}_signal_{today}.txt"
    txt_path.write_text(signal_text, encoding='utf-8')

    # 同时在控制台打印
    print(signal_text)

    return {
        'ticker': ticker,
        'date': today,
        'file': txt_path.name
    }

# ==================== 7. 主程序 ====================
def main():
    print("=== 🚀 个股每日信号生成器启动 ===")

    # ==================== 在这里手动添加你要监控的股票 ====================
    target_stocks = ['NVDA', 'TSLA', 'META', 'MSFT', 'AAPL']

    for ticker in target_stocks:
        generate_daily_signal(ticker)
        time.sleep(random.uniform(0.5, 1.5))

    print(f"\n✅ 所有个股信号生成完成！文件已保存在 {CONFIG['trade_msg_dir']} 目录下")


if __name__ == "__main__":
    main()