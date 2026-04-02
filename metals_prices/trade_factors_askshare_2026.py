import os
import csv
import arrow
import akshare as ak
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler

# ==========================================
# 🛑 终极网络隔离：屏蔽所有代理环境变量
# 强制 AkShare 的国内请求走直连，防止 Clash 代理导致 SSL 握手卡死
# ==========================================
os.environ['NO_PROXY'] = '*'
for k in list(os.environ.keys()):
    if k.lower().endswith('proxy'):
        del os.environ[k]

# ==========================================
# 目录配置：根目录 + 子目录
# ==========================================
# 在根目录下增加一个 macro_data 的子文件夹，让数据更整洁
SAVE_ROOT_PATH = Path("/home/evaseemefly/01data/05-spiders/macro_data")


def fetch_and_save_data():
    """核心数据拉取与存储函数"""
    now_utc = arrow.utcnow()
    # 提取完整时间用于日志，提取日期用于文件命名
    datetime_str = now_utc.format('YYYY-MM-DD HH:mm:ss')
    date_str = now_utc.format('YYYY-MM-DD')
    timestamp_sec = now_utc.int_timestamp

    # 动态生成今天的 CSV 文件路径
    # 确保子目录存在，如果不存在则自动创建
    SAVE_ROOT_PATH.mkdir(parents=True, exist_ok=True)
    csv_file_path = SAVE_ROOT_PATH / f"macro_daily_{date_str}.csv"

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
    # 1. 获取 WTI 原油 (CL连续合约)
    # ==========================================
    try:
        df_wti = ak.futures_foreign_hist(symbol="CL")
        if not df_wti.empty:
            record['WTI'] = round(float(df_wti['close'].iloc[-1]), 4)
            print(f"✅ WTI 原油: {record['WTI']}")
    except Exception as e:
        print(f"❌ WTI 获取失败: {e}")

    # ==========================================
    # 2. 获取 美元指数 (DX)
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
            col = [c for c in df_bond.columns if '10' in c and '美' in c][0]
            valid_bonds = df_bond[col].dropna()
            if not valid_bonds.empty:
                record['US10Y'] = round(float(valid_bonds.iloc[-1]), 4)
                print(f"✅ 10Y美债: {record['US10Y']}")
    except Exception as e:
        print(f"❌ US10Y 获取失败: {e}")

    # ==========================================
    # 4. 获取 恐慌指数 (VIX)
    # ==========================================
    try:
        df_vix = ak.stock_us_daily(symbol="VIX", adjust="")
        record['VIX'] = round(float(df_vix['close'].iloc[-1]), 4)
        print(f"✅ 恐慌指数 (VIX): {record['VIX']}")
    except Exception:
        try:
            if hasattr(ak, 'index_us_stock_sina'):
                df_vix = ak.index_us_stock_sina(symbol="VIX")
                record['VIX'] = round(float(df_vix['close'].iloc[-1]), 4)
                print(f"✅ 恐慌指数 (VIX): {record['VIX']}")
            else:
                print("❌ VIX 获取失败: 当前版本无可用接口")
        except Exception as e:
            print(f"❌ VIX 获取失败: {e}")

    # ==========================================
    # 5. 持久化追加写入当日 CSV
    # ==========================================
    file_exists = csv_file_path.exists()
    fieldnames = ['datetime_utc', 'timestamp', 'VIX', 'US10Y', 'DXY', 'WTI']

    try:
        with open(csv_file_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            # 如果是该日期的第一次运行（文件刚创建），先写入表头
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)

        print(f"💾 数据已成功追加至 {csv_file_path}")
    except Exception as e:
        print(f"❌ 写入 CSV 失败: {e}")
    print("-" * 50)


def main():
    print("=== 全球宏观数据自动采集服务启动 ===")

    # 启动时先测试执行一次
    fetch_and_save_data()

    # 初始化 APScheduler
    scheduler = BlockingScheduler(timezone="UTC")

    # 设定每小时的第 0 分钟执行一次
    scheduler.add_job(fetch_and_save_data, 'cron', minute='0', id='macro_job')

    print("⏳ 定时任务已注册！将一直在后台挂机运行...")
    print("   [提示] 按 Ctrl+C 可随时停止程序。")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n⏹️ 采集服务已手动停止。")


if __name__ == "__main__":
    main()