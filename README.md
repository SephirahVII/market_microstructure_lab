# Crypto Analysis (Microstructure Lab)

基于 Python 的量化投资系统基础设施。该项目提供了一套高度模块化、且支持**异步、高并发、低延迟**的解决方案。目前核心工程包含能够从多个交易所（Binance, Bitfinex, Kraken 等）同时采集**现货（Spot）**与**永续合约（Futures/Swap）**的逐笔成交（Trades）和全量订单簿（Orderbook）的大规模原始数据获取子系统。

---

## 🏗️ 核心架构与目录解耦说明

为了支持从单纯的“获取数据”向完整的“量化研究与执行系统”演进，本工程采用了模块化解耦设计，各子模块互不干扰：

```text
Crypto_Analysis/
│
├── config/                             # 统一的全局配置中心
│   ├── fetch_config.yaml               # 数据采集专用配置 (设置交易所、币种汇率对)
│
├── data/                               # 本地数据湖 (大型Parquet文件，由.gitignore忽略)
│   ├── raw/                            # 获取模块产出的原始数据 (按时间/交易所Hive分区)
│
├── src/                                # 核心微服务子系统
│   │
│   ├── data_fetch_ccxtpro/             # 📡 纯粹的数据摄取子系统 (Data Fetching Module)
│   │   ├── orderbook.py                # 自动维护OrderBook并定期Flush到硬盘
│   │   ├── trade.py                    # 逐笔Tick采集流处理
│   │   └── fetch_runner.py             # 核心爬虫主循环入口
│   │
│   ├── data_pipeline/                  # 🏷️ [建设中] 数据加工管道
│   │   └── .gitkeep                    # 用于存放清洗缺失值、时间重采样、微观指标生成的逻辑
│   │
│   └── models/                         # 🧠 [建设中] 策略与模型层
│       └── .gitkeep                    # 用于存放基于深度学习分析Orderbook失衡或高频因子的算法
│
├── research/                           # 🧪 [建设中] 实验与可视化工作区
│   └── .gitkeep                        # 用于存放Jupyter Notebooks进行探索性数据分析(EDA)
│
└── utils.py                            # 通用工具
```

---

### 各模块的作用机制详解

#### 1. `data_fetch_ccxtpro` (数据获取层)
这是系统中目前最核心的组件。它的**唯一职责**就是：与外部交易所 WebSocket 建立极速连接，将收到的一条条 JSON 消息序列化为内存表，并定期落盘压缩。该模块完全独立工作，无需知道数据之后会被用来画图还是做分析。

#### 2. `data_pipeline` (数据清洗与特征构建层)
原始的 Tick 数据是非常杂乱的（包含跳点、缺失、零星断连等）。在研究和建模前，数据必须流经 Pipeline 转化为结构化信号。例如：
- 把逐笔交易（Tick）聚合成 1s / 1min 的 OHLCV 与 VWAP。
- 从 Orderbook 表格中计算得出流动性价差（Spread）、多档失衡系数（OFI, Order Flow Imbalance）。
*注：目前该目录为空，由 `.gitkeep` 占位保留架构位置。*

#### 3. `models` (策略模型层)
有了 `pipeline` 清洗出来的干净特征库，这里用于编写真正驱动交易的算法大脑（例如 PyTorch 构建的 DeepLOB 神经网络结构、逻辑回归因子测试等）。
*注：目前该目录为空，由 `.gitkeep` 占位保留架构位置。*

#### 4. `research` (投研与可视化)
为量化研究员准备的沙盒环境。这里不存放生产级代码，只存放大量的 `Jupyter Notebook`。研究员可以在这里写代码进行数据探索（Exploratory Data Analysis）、把原始 OrderBook 画成热力图，做因子共线性分析等。

#### ❓ 为什么有 `.gitkeep` 文件？
在 Git 版本控制系统中，它是**不支持追踪“空文件夹”**的。如果创建了一个 `models/` 文件夹而不放任何文件，Git 会假装这个文件夹不存在。
为了确保团队中的每个人在克隆项目时，都能**原封不动地获得我们预设好的完美架构骨架**，我们在空文件夹里放一个没有实际含义的占位符文件（约定俗成命名为 `.gitkeep`）。这样就能强行把这些空壳架构目录留存在 GitHub 上，为未来的开发铺平道路。

---

## 🚀 如何启动数据采集

```bash
# 激活对应环境
conda activate quant312

# 运行专门的 Fetch 运行器
python src/data_fetch_ccxtpro/fetch_runner.py
```
所有采集到的庞大数据流将会遵循 `market_type/exchange/symbol/date/hour.parquet` 的极速加载切片结构自动沉淀到您的 `data/raw/` 湖中。
