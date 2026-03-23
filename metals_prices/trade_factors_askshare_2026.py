import os
import csv
import arrow
import time
import akshare as ak
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler

# ==========================================
# 🛑 终极网络隔离：彻底屏蔽 macOS 所有的系统代理
# 强制让所有 requests 请求走直连，防止 SSL 握手卡死
# ==========================================
os.environ['NO_PROXY'] = '*'
for k in list(os.environ.keys()):
    if k.lower().endswith('proxy'):
        del os.environ[k]

CSV_FILE_PATH = Path("macro_data.csv")


def fetch_and_save_data():
    """核心数据拉取与存储函数"""
    now_utc = arrow.utcnow()
    datetime_str = now_utc.format('YYYY-MM-DD HH:mm:ss')
    timestamp_sec = now_utc.int_timestamp

    record = {
        'datetime_utc': datetime_str,
        'timestamp': timestamp_sec,
        'VIX': None,
        'US10Y': None,
        'DXY': None,
        'WTI': None
    }

    print(f"\n[{datetime_str} UTC] 🚀 开始执行定时采集任务...")

    # ==========================================
    # 1. 获取 WTI 原油 (CL连续合约) - 已验证成功
    # ==========================================
    try:
        df_wti = ak.futures_foreign_hist(symbol="CL")
        if not df_wti.empty:
            record['WTI'] = round(float(df_wti['close'].iloc[-1]), 4)
            print(f"✅ WTI 原油: {record['WTI']}")
    except Exception as e:
        print(f"❌ WTI 获取失败: {e}")

    # ==========================================
    # 2. 获取 美元指数 (DX) - 已验证成功
    # ==========================================
    try:
        df_dxy = ak.futures_foreign_hist(symbol="DX")
        if not df_dxy.empty:
            record['DXY'] = round(float(df_dxy['close'].iloc[-1]), 4)
            print(f"✅ 美元指数 (DXY): {record['DXY']}")
    except Exception as e:
        print(f"❌ DXY 获取失败: {e}")

    # ==========================================
    # 3. 获取 10年期美债 (US10Y)
    # ==========================================
    try:
        df_bond = ak.bond_zh_us_rate()
        if not df_bond.empty:
            # 找到含有 "10" 和 "美" 的列
            col = [c for c in df_bond.columns if '10' in c and '美' in c][0]
            valid_bonds = df_bond[col].dropna()
            if not valid_bonds.empty:
                record['US10Y'] = round(float(valid_bonds.iloc[-1]), 4)
                print(f"✅ 10Y美债: {record['US10Y']}")
    except Exception as e:
        print(f"❌ US10Y 获取失败 (可能非交易时段): {e}")

    # ==========================================
    # 4. 获取 恐慌指数 (VIX)
    # 修复：移除 adjust 参数，尝试直接获取，或使用新浪美股接口
    # ==========================================
    try:
        # 尝试 1: 不复权的美股日线
        df_vix = ak.stock_us_daily(symbol="VIX", adjust="")
        record['VIX'] = round(float(df_vix['close'].iloc[-1]), 4)
        print(f"✅ 恐慌指数 (VIX): {record['VIX']}")
    except Exception:
        try:
            # 尝试 2: 如果上面的方法被阻截，换用专门的美股接口
            if hasattr(ak, 'index_us_stock_sina'):
                df_vix = ak.index_us_stock_sina(symbol="VIX")
                record['VIX'] = round(float(df_vix['close'].iloc[-1]), 4)
                print(f"✅ 恐慌指数 (VIX): {record['VIX']}")
            else:
                print("❌ VIX 获取失败: 当前版本无可用接口")
        except Exception as e:
            print(f"❌ VIX 获取失败: {e}")

    # ==========================================
    # 5. 持久化追加写入 CSV
    # ==========================================
    file_exists = CSV_FILE_PATH.exists()
    fieldnames = ['datetime_utc', 'timestamp', 'VIX', 'US10Y', 'DXY', 'WTI']

    try:
        with open(CSV_FILE_PATH, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)

        print(f"💾 数据已成功追加至 {CSV_FILE_PATH.name}")
    except Exception as e:
        print(f"❌ 写入 CSV 失败: {e}")
    print("-" * 50)


def main():
    print("=== 全球宏观数据自动采集服务启动 ===")

    # 脚本启动时先执行一次，测试通道是否通畅
    fetch_and_save_data()

    # 初始化 APScheduler 调度器
    scheduler = BlockingScheduler(timezone="UTC")

    # 设定每小时的第 0 分钟执行一次 (例如: 13:00, 14:00, 15:00)
    # 你可以根据需求改成 minute='*/15' (每15分钟执行一次)
    scheduler.add_job(fetch_and_save_data, 'cron', minute='0', id='macro_job')

    print("⏳ 定时任务已注册！将一直在后台挂机运行...")
    print("   [提示] 按 Ctrl+C 可随时停止程序。")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n⏹️ 采集服务已手动停止。")


if __name__ == "__main__":
    main()