## 目录结构：

### 工程目录

```sh
(py39) evaseemefly@Mac-Studio GlobalNewsSpiders % tree -L 2
.
├── README.md
├── iran_news
│   └── iran_spiders.py
├── metals_prices
│   ├── gold_data
│   ├── gold_features
│   ├── gold_gk_pics
│   ├── gold_trade_spiders.py
│   ├── gold_trade_spiders_byothers.py
│   ├── news_data
│   ├── trade_factors_2026.py
│   ├── trade_factors_askshare_2026.py
│   ├── trade_factors_askshare_test.py
│   ├── trade_factors_spider.py
│   ├── trade_gold_calculate_gk.py
│   ├── trade_factors_yfinace_260415.py  									# 26-04-15 使用yfinace获取重要指标数据
│   └── trade_merge_calculate_all_yfinance_v1.py				  # 26-04-16 更新后的计算波动率以及绘制所有指标参数
└── news_data
    └── market_prices

9 directories, 9 files
```



### 数据存储目录

#### s1- 常用目录

| 存储内容                        | 目录                                                     | 备注                                 |
| ------------------------------- | -------------------------------------------------------- | ------------------------------------ |
| 基于实时金价提取后并添加波动率  | /home/evaseemefly/01data/05-spiders/output/gold_features |                                      |
| 存储波动率及金价 k 线图         | /home/evaseemefly/01data/05-spiders/output/gold_gk_pics  |                                      |
| 5 分钟实时金价原始数据          | /home/evaseemefly/01data/05-spiders/market_prices        |                                      |
| 10 年期美债以及美元指数         | /home/evaseemefly/01data/05-spiders/macro_data           |                                      |
| 获取的美债，美元指数以及vix指数 | /home/evaseemefly/01data/05-spiders/macro_realtime       | * 目前使用yfinance获取的各类宏观指数 |



#### 1- 数据存储根目录：

```
/home/evaseemefly/01data
```

#### 2- 当前存储样例及目录结构

```shell
(base) evaseemefly@evaseemefly-Precision-7530:~/01data$ tree -L 4
.
├── 01-openclaw
│   └── 测试连接qwen.shell
├── 05-spiders
│   ├── iran_news
│   │   ├── iran_conflict_news_2026-04-06.csv
│   │   └── iran_conflict_news_2026-04-07.csv
│   ├── macro_data
│   │   ├── macro_daily_2026-04-06.csv
│   │   └── macro_daily_2026-04-07.csv
│   ├── macro_realtime																	# 026-04-15 新增通过 yfinance 获取的宏观指标
│   │   ├── macro_realtime_2026-04-15.csv
│   │   └── macro_realtime_2026-04-16.csv
│   ├── market_prices																		# 金银实时价格
│   │   ├── precious_metals_2026-04-06.csv
│   │   └── precious_metals_2026-04-07.csv
│   └── output
│       ├── gold_features
│       │   ├── gold_features_20260320_to_20260406.csv
│       │   └── gold_features_20260320_to_20260407.csv
│       └── gold_gk_pics
│           ├── gk_vol_60m_2026-03-21_to_2026-04-06.png
│           └── gk_vol_60m_2026-03-21_to_2026-04-07.png
└── 05-spiders.zip
```

其中

```
│   └── output
│       ├── gold_features
```

这个目录

## 数据说明

### 1- 各类数据说明

* 各类宏观指数

  * 26-04-16

  ```json
  timestamp_utc,fetch_time_utc,DXY,DXY_time,US10Y,US10Y_time,VIX,VIX_time
  2026-04-16 00:00:00,2026-04-16 00:00:10,98.011,2026-04-15 23:50:00,4.282,2026-04-15 18:55:00,18.17,2026-04-15 20:10:00
  2026-04-16 00:05:00,2026-04-16 00:05:10,98.01,2026-04-15 23:55:00,4.282,2026-04-15 18:55:00,18.17,2026-04-15 20:10:00
  2026-04-16 00:10:00,2026-04-16 00:10:10,98.003,2026-04-16 00:00:00,4.282,2026-04-15 18:55:00,18.17,2026-04-15 20:10:00
  2026-04-16 00:15:00,2026-04-16 00:15:10,98.021,2026-04-16 00:05:00,4.282,2026-04-15 18:55:00,18.17,2026-04-15 20:10:00
  ```

  

## 运行脚本说明

### 1- 获取美伊新闻

`iran_spiders.py`

自动获取美伊的实时新闻，并存储至 csv 文件中。

### 2- 获取交易数据（十年期美债｜美元指数｜VIX）

* `trade_factors_yfinace_260415.py`  26-04-15 修改后的获取`DXY`  `US10Y` `VIX` 数据（使用 yfinance 接口）

  `DXY`  `VIX` 获取 5 分钟级别的数据（更新频率高）;

  `US10Y`  （更新频率低）

  * `US10Y` （10年期美债收益率）反映的是债券市场的交易结果。美债市场并不是像外汇（DXY）那样近乎 24 小时高频波动的，它有严格的交易时间和休市期（且流动性集中在纽约时段）。在你抓取数据时，亚洲早盘/欧洲时段的美债市场可能处于休市或极低流动性状态，所以雅虎财经返回的是上一个有效交易日的最终定价。这在宏观对冲中是完全正常的，不需要让它强制高频。
  * 对于 `DXY`  `VIX`  虽然我们无法消除 10 分钟的延迟，但我们可以**约束 API，让它不要返回零散的 `1m` 时间（比如 07:38），而是返回严格对齐到 `5m` 刻度的 K 线（比如 07:35）**。

### 3- 获取实时黄金金价

`trade_factors_spider.py`

### 4- 定时合并并计算黄金波动率

`trade_gold_calculate_gk.py`

自动处理近期的黄金实时金价以及计算波动率，并生成图片。

## TODO

26-04-02 实现定时合并黄金金价并计算黄金波动率