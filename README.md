## 目录结构：

### 工程目录

```
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
│   └── trade_gold_calculate_gk.py
└── news_data
    └── market_prices

9 directories, 9 files
```



### 数据存储目录

#### 1- 数据存储根目录：

```
/home/evaseemefly/01data
```

#### 2- 当前存储样例及目录结构

```
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
│   ├── market_prices
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

## 运行脚本说明

### 1- 获取美伊新闻

`iran_spiders.py`

自动获取美伊的实时新闻，并存储至 csv 文件中。

### 2- 获取交易数据

### 3- 获取实时黄金金价

`trade_factors_spider.py`

### 4- 定时合并并计算黄金波动率

`trade_gold_calculate_gk.py`

自动处理近期的黄金实时金价以及计算波动率，并生成图片。



## TODO

26-04-02 实现定时合并黄金金价并计算黄金波动率