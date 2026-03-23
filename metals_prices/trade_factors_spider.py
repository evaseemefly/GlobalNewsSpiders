import os
import yfinance as yf
import pandas as pd
import arrow

# ==========================================
# 【核心修复】：在导入其他逻辑前，强制设置全局环境变量代理
# 请将 7890 替换为你实际的代理端口（如 Clash 是 7890，v2ray 是 10808）
# ==========================================
PROXY_URL = "http://127.0.0.1:10808"
os.environ["http_proxy"] = PROXY_URL
os.environ["https_proxy"] = PROXY_URL
# ==========================================

# 定义四大宏观指标的雅虎财经代码
TICKERS = {
    'WTI原油 (Crude Oil)': 'CL=F',
    '美元指数 (DXY)': 'DX-Y.NYB',
    '10年期美债 (US10Y)': '^TNX',
    '恐慌指数 (VIX)': '^VIX'
}


def fetch_macro_data():
    current_time = arrow.now().format('YYYY-MM-DD HH:mm:ss')
    print(f"[{current_time}] 正在获取全球宏观流动性指标...\n")
    print("-" * 50)

    results = {}
    for name, ticker in TICKERS.items():
        try:
            # 注意：这里不再传入 proxy 参数，它会自动读取上面设置的 os.environ
            data = yf.download(ticker, period="5d", progress=False)

            if data.empty:
                print(f"⚠️ {name} 获取的数据为空。")
                continue

            close_col = data['Close']
            latest_price = float(
                close_col.iloc[-1].iloc[0] if isinstance(close_col, pd.DataFrame) else close_col.iloc[-1])
            prev_price = float(
                close_col.iloc[-2].iloc[0] if isinstance(close_col, pd.DataFrame) else close_col.iloc[-2])

            change_pct = ((latest_price - prev_price) / prev_price) * 100

            results[name] = {
                'price': latest_price,
                'change_pct': change_pct,
                'is_down': latest_price < prev_price
            }

            trend_icon = "🔴 上涨 (警报)" if change_pct > 0 else "🟢 下跌 (缓解)"
            if name == '恐慌指数 (VIX)':
                trend_icon = "🔴 恐慌加剧" if change_pct > 0 else "🟢 情绪修复"

            print(f"{name}: 当前数值 {latest_price:.2f} | 单日变化: {change_pct:+.2f}% | 状态: {trend_icon}")

        except Exception as e:
            print(f"获取 {name} 数据失败: {e}")

    print("-" * 50)
    return results


def check_resonance_signal(data):
    if len(data) < 4:
        print("⚠️ 数据获取不全，无法进行共振判断。")
        return

    vix_val = data['恐慌指数 (VIX)']['price']
    oil_down = data['WTI原油 (Crude Oil)']['is_down']
    dxy_down = data['美元指数 (DXY)']['is_down']
    us10y_down = data['10年期美债 (US10Y)']['is_down']

    print("\n【宏观共振发令枪 研判报告】")

    vix_safe = vix_val < 25.0
    print(f"👉 条件1 (VIX低于25): {'✅ 满足' if vix_safe else f'❌ 未满足 (当前 {vix_val:.2f}，情绪依然恐慌)'}")
    print(f"👉 条件2 (原油回落): {'✅ 满足' if oil_down else '❌ 未满足 (油价仍在飙升，通胀警报未解除)'}")
    print(f"👉 条件3 (美元走弱): {'✅ 满足' if dxy_down else '❌ 未满足 (美元仍被抽水，外资流出压力大)'}")
    print(f"👉 条件4 (美债下行): {'✅ 满足' if us10y_down else '❌ 未满足 (无风险利率上升，压制风险资产)'}")

    print("-" * 50)

    if vix_safe and oil_down and dxy_down:
        print("🟢🟢🟢 结论：核心宏观指标出现向下共振！流动性危机大概率解除。")
        print("指令：可以解除账户锁定状态，择机启动超跌核心资产的右侧建仓/补仓计划。")
    else:
        print("🔴🔴🔴 结论：未形成缓和共振，宏观警报仍未解除！")
        print("指令：死死捂住现金，禁止任何左侧抄底！装死，关闭交易软件，去学英语。")


if __name__ == "__main__":
    macro_data = fetch_macro_data()
    if macro_data:
        check_resonance_signal(macro_data)