# market_microstructure_lab

尝试构建一个基于 Python 的高频加密货币市场微观结构数据采集与分析项目，目前已完成数据收集部分。

该项目旨在提供一套**异步、高并发、低延迟**的解决方案，用于从多个交易所（Binance, Bitfinex, Kraken 等）同时采集**现货（Spot）**与**永续合约（Futures/Swap）**的逐笔成交（Trades）和全量订单簿（Orderbook）数据，并以 **Parquet** 作为存储格式。

此外，后续项目还将加入：原始数据清洗、聚合，关键的微观结构指标计算（如深度失衡、多档价差、VWAP 等），用于量化研究与策略建模。

---

## ✨ 主要介绍

* **多交易所支持**：通过 `ccxt.pro` 支持 Binance, OKX, Bitfinex, Coinbase, Kraken 等主流交易所。
* **多合约币种覆盖**：支持同时采集 **现货 (Spot)** 和 **U本位永续合约 (Swap)** 数据。
* **存储路径清晰**：原始数据按 `交易所/市场类型/币种/日期` 分层存储，结构清晰。
* **Parquet 存储**：更适合后续批量计算与列式读取。
* **实时 Dashboard**：内置 WebSocket + 图表展示实时成交与订单簿。
* **日志记录**：运行日志写入 `logs/`，便于异常排查。
* **统一时区**：所有数据统一使用 **UTC** 时间戳。
* **微观指标计算**：
    * **Trade**: OHLCV, VWAP, 成交笔数, 主动买入/卖出量。
    * **Orderbook**: 多档位（L1, L5, L10...）的 Spread, Depth, Imbalance, Micro-Price。
* **目前问题**：Bitfinex的orderbook数据获取存在问题，不确定是否能完全获取逐笔数据
* **P.S.**：初学项目，还在学习过程中，欢迎大佬指点
---

## 📂 目录结构

```text
Crypto Data Collection/
│
├── config/
│   ├── collector_config.yaml     # 采集任务配置 (设置交易所、币种)
│   └── analysis_config.yaml      # 分析任务配置 (时间窗口、指标算法)
│
├── data/
│   ├── raw_parquet/              # 原始数据 (Parquet)
│   │   ├── trades/
│   │   │   ├── spot/             # 现货成交
│   │   │   └── swap/             # 合约成交
│   │   └── orderbooks/           # 订单簿快照
│   └── processed/                # 清洗与特征工程后的汇总数据 (CSV)
│
├── scripts/
│   ├── run_collector.py          # 启动数据采集系统
│   └── run_dashboard.py          # 启动采集 + 前端展示
│   └── run_processor.py          # 启动数据处理与指标计算
│
├── src/                          # 核心源码
│   ├── collectors/               # 采集模块 (Websocket方式获取交易所实时数据)
│   ├── processors/               # 处理模块 (对量价特征或订单簿相关特征进行计算)
│   └── utils.py                  # 通用工具
│
└── requirements.txt              # 依赖库

---

## 🚀 快速运行 Dashboard

```bash
python scripts/run_collector.py
```

如需启用实时图表，可在 `config/collector_config.yaml` 中设置：

```yaml
system:
  dashboard_enabled: true
  dashboard_port: 8000
```

默认在 `http://localhost:8000` 提供实时图表。
