## 目录结构：

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