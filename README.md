# Gribuki Trade

Gribuki Trade 是一个面向个人研究的 Python 交易工作台，当前以 **A 股全市场发现、证据化深度研究、PAPER 仿真和受控通知** 为主线，同时维护 Binance Spot Testnet 与 Charles Schwab API 的隔离适配层。

项目版本为 `0.1.0`，采用专有个人使用许可。它不是已经投入生产的自动交易机器人，也不提供投资收益承诺：研究结论、LLM 评分、人工复核和 PAPER 成交都不等于真实委托授权。

## 当前结论

截至 2026-08-14，项目已经形成以下闭环：

```text
全市场收盘筛选 / 盘中异常发现
              ↓
       append-only 候选事件库
              ↓
     有界跟踪 / 单标的或批量深研
              ↓
确定性技术分析 + 可追溯新闻/宏观证据 + LLM 解释
              ↓
     推荐门禁 → 报告 → QQ 通知 → 人工复核
              ↓（仅显式操作）
       A 股 PAPER 账本与保守撮合
              ↓
 walk-forward 策略实验与受控因子探索
```

最重要的边界是：**研究链不会直接调用券商，下游也没有面向用户的 LIVE 交易 CLI。**

| 能力 | 当前成熟度 | 准确边界 |
|---|---|---|
| A 股收盘全市场筛选 | 核心已实现 | 当前交易日盘后股票筛选；不是 ETF 全市场筛选，也不是买入清单 |
| A 股盘中异常发现 | 核心已实现 | 公开网页快照的线索发现；不是交易所 tick、L1/L2 或可执行报价 |
| 候选库、跟踪、深研、复核 | 核心已实现 / 编排部分实现 | 支持有限轮和有界批处理；尚无生产级常驻调度器 |
| 技术面 + 宏观/新闻融合 | 核心已实现 | 分数未经概率校准；LLM 不能单独把技术 `WATCH` 升级为入场 |
| A 股 PAPER | 账本 CLI 已实现；撮合/恢复为 Python API | 不模拟盘口队列、集合竞价、公司行动或真实冲击 |
| 策略实验与因子发现 | 安全研究骨架已实现 | 单证券评价器可用；尚不是全市场组合回测器，也不会自动发布权重 |
| PySide6 GUI | 演示原型 | 资金、K 线、订单和回测均为本地占位数据，未连接后台研究或券商 |
| Binance Spot Testnet | durable OMS、历史回放和 SHADOW 已实现 | 只有 Testnet/本地仿真入口；无生产 LIVE 编排 |
| Schwab | 适配代码与离线测试已实现 | 尚未用真实 Developer App 联调，无用户侧 CLI、streaming 和 durable 执行编排 |

“核心已实现”表示领域逻辑、持久化或 CLI 已存在并有测试，不表示已经完成连续运行、实盘或统计有效性验收。

## 六类研究功能是如何实现的

### 1. 收盘全市场三层筛选

入口是 `ashare-market-screen-once`，核心位于：

- [全市场数据适配器](src/gribuki_trade/adapters/ashare_screening.py)
- [硬过滤与横截面因子](src/gribuki_trade/features/ashare_screening.py)
- [三层编排服务](src/gribuki_trade/services/ashare_screening.py)

第一层在当前交易日 15:05（Asia/Shanghai）之后抓取全 A 股票快照：东方财富为主源，腾讯为独立回退，并补沪、深、北交所上市元数据。默认至少需要 4,500 条记录且三个交易所均有覆盖，否则整轮失败关闭。

L1 不做“缺失即中性”，而是用明确门槛过滤：

- 上市自然日不少于 250 日；
- 最新价不少于 1 元；
- 当日累计成交额不少于 2,000 万元；
- 总市值不少于 20 亿元；
- ST、停牌、交易状态未知或关键字段未知均排除；
- 研究范围允许沪深主板、创业板、科创板和北交所。研究覆盖不等于执行白名单。

第二层按当日成交额排序，只给最多 300 个幸存者补至少 201 个交易日的未复权历史，避免对全市场逐票打爆网页源。随后再要求 20 日平均成交额不少于 5,000 万元。

第三层对有效横截面做 2.5%/97.5% winsorize、tie-aware 百分位排名、方向归一化和加权。当前九个因子是：

| 因子 | 权重 | 方向 |
|---|---:|---|
| 20 日动量 | 10% | 高优 |
| 60 日动量 | 15% | 高优 |
| 120 日动量，跳过最近 5 日 | 20% | 高优 |
| MA20 / MA60 趋势 | 15% | 高优 |
| 20 日突破位置 | 10% | 高优 |
| 20 日量比 | 10% | 高优 |
| 60 日年化波动 | 8% | 低优 |
| 60 日最大回撤 | 7% | 低优 |
| 20 日 Amihud 非流动性 | 5% | 低优 |

截面至少需要 20 个有效观察，单票可用因子权重覆盖必须达到 80%。缺失因子不会被填成 0 或 50 分；每个候选都会保留原值、winsorized 值、百分位、方向分、贡献、缺失原因和来源 revision。默认输出 Top 30，同时将候选和规范化运行结果分别写入候选事件库与研究运行档案。

这条链只支持“当前日盘后抓取”。历史回放必须使用当时归档的股票池、快照和 source revision，不能用今天的网页结果回填过去。

### 2. 盘中全市场异常发现

盘中链由 [适配器](src/gribuki_trade/adapters/ashare_surveillance.py)、[纯评分函数](src/gribuki_trade/features/ashare_surveillance.py) 和 [服务](src/gribuki_trade/services/ashare_surveillance.py) 组成，仅在 09:30–11:30、13:00–15:00 工作。

它同样使用东方财富主源和腾讯回退，默认要求：

- 快照不超过 3 分钟；
- 至少 4,500 个标的且沪深京覆盖完整；
- 价格不少于 1 元、当日成交额不少于 200 万元；
- 横截面至少 100 个样本、可用因子权重覆盖不少于 55%。

横截面因子包括涨跌强度 25%、对数成交额 20%、日内区间位置 15%、开盘后延续 15%、量比 15% 和换手率 10%。默认候选阈值为 `0.35`，动量扩张阈值为 `0.55`，输出 `MOMENTUM_EXPANSION`、`ACTIVE_STRENGTH` 或 `OBSERVATION_ONLY`。

这些结果始终携带 `CURRENT_SESSION_SNAPSHOT_ONLY`，只用于发现线索。腾讯回退缺少 OHLC、量比等字段时会明确降级，而不会伪造完整评分。

### 3. 统一候选库与有界跟踪

[候选领域模型](src/gribuki_trade/domain/candidates.py)、[候选服务](src/gribuki_trade/services/candidate_universe.py) 和 [SQLite 存储](src/gribuki_trade/storage/candidate_store.py) 把手工关注、收盘筛选、盘中异常、策略和人工复核合并为一套事件语义。

候选具有：

- `LOW/NORMAL/HIGH/URGENT` 优先级；
- `ACTIVE/COOLING/EXPIRED/REMOVED` 状态；
- 发现时间、观察时间、TTL、原因、证据、来源 run 和 provenance；
- 稳定事件 ID、内容碰撞检测和按时点重放。

默认 TTL 为：收盘筛选 4 日、盘中异动 8 小时、策略 2 日、复核 7 日；手工候选默认无期限。只有 `ACTIVE` 会进入正常跟踪，`COOLING` 仅在显式诊断时可见。

`ashare-research-watch` 是顺序、有界、多标的分钟研究轮询，每个标的故障隔离；它不是无限 daemon，也不是收盘深研。长期运行必须交给 OS supervisor。候选库支持 `.BJ`，但现有分钟跟踪规范化仍只承诺 `.SH/.SZ`；北交所目前只承诺收盘日线深研，不承诺分钟链。

### 4. 特定标的收盘深研、新闻与宏观融合

入口是 `ashare-close-research-once` 或有上限的 `ashare-close-research-batch`。主要编排在 [收盘研究服务](src/gribuki_trade/services/ashare_close_analysis.py)。

#### 时间与行情

[交易日会话解析器](src/gribuki_trade/services/ashare_close_sessions.py) 使用 BaoStock 交易日历识别最近完成日和下一交易日；交易日 15:05 前不会偷看当天收盘线。

未复权日线使用分层路由：BaoStock 优先、AKShare 备用；ETF 另走合适的基金端点；两源只在最近重叠区间严格一致时允许受控尾部拼接；网络源失败后才使用本地不可变 market-evidence 归档。最近完成交易日缺失、历史不足、未来数据、乱序、复权模式错误或明显企业行动断点都会 `ABSTAIN`。

静态研究池中的标的使用 TOML 画像；动态候选可通过当前网页元数据补名称、行业、上市日和市值分层。ETF 动态画像只确认代码与名称，不猜跟踪指数、基金公司或费率。

#### 技术面

[收盘技术引擎](src/gribuki_trade/features/close_analysis.py) 至少使用 201 个交易日，计算：

- MA5/20/60/120/200 与 ATR 归一化斜率；
- 5/20/60/120-skip-5/200 日收益；
- 20/55 日 Donchian、Bollinger、MACD；
- RSI14、Wilder ADX/+DI/-DI、ATR14、Stochastic K；
- 量比、上涨/下跌成交额关系、换手覆盖和 Amihud；
- 20/60 日波动、下行波动、跳空波动和 60 日最大回撤。

五个方向家族的固定权重为趋势 30%、多周期动量 25%、突破/压缩 20%、顺势回撤 10%、量价流动性 15%。相对强弱/市场广度目前只展示，尚未经过样本外校准，因此不进入方向分。引擎还对相同日线家族给出 1–5 日和 2–8 周两种诊断重权视角，但不会据此制造第二套订单结论。

`ENTER_CANDIDATE` 需要趋势、突破、量能、RSI 和风险门同时满足，技术分至少 `0.55`；持仓情况下趋势破坏才可能 `REDUCE`，否则保持 `WATCH/ABSTAIN`。所有分数都标为 `UNCALIBRATED`，不是上涨概率。

#### 新闻、公告和结构化宏观数据

| 证据族 | 当前来源 | 用途与限制 |
|---|---|---|
| 媒体线索 | AKShare 包装的新浪、财联社、东方财富、同花顺全局快讯及东方财富个股新闻 | 线索层；保存来源、时间、URL、哈希和首次可见时间 |
| 公司公告 | AKShare 包装的 CNINFO 公告索引 | 股票官方披露层；保留原始响应和 revision |
| 官方宏观/政策 | NBS、PBOC、CSRC、Federal Reserve RSS | 事实主线；单源失败与其他源隔离 |
| 人民币与利率 | SAFE 中间价、官方 Shibor、ChinaMoney FR/FDR、中债收益率曲线 | 保留真实口径；FR/FDR 不冒充 R/DR |
| 全球风险与跨市场 | Cboe VIX EOD、A/港/美/亚太指数快照和历史关系 | 按各市场收盘可见性对齐；相关不写成因果 |
| A 股上下文 | 沪深京市场宽度、CFFEX IF、ETF 价/IOPV/份额、上交所期权风险参数 | 缺同日现货或不适用时不造基差、IV 或资金流 |
| 搜索发现 | 可选 Tavily、用户自管 SearXNG | 单一搜索供应商仅为 `HINT`，不能直接进入宏观评分 |

[原始文档与标准事件](src/gribuki_trade/domain/events.py) 分别记录 `published_at`、`first_seen_at`、`available_at`、内容哈希、解析版本和修订链。每个来源独立超时和降级；未来证据、过期证据、重复故事、低相关内容和提示注入不会进入模型。

[宏观研究服务](src/gribuki_trade/services/macro_research.py) 默认从最近 14 日选择最多 24 条、每源最多 6 条证据。默认 [DeepSeek 适配器](src/gribuki_trade/adapters/llm/deepseek_chat.py) 使用 `deepseek-v4-flash`，只接收结构化技术摘要与带 ID/URL/hash/时间的 EvidencePack，禁止工具调用，并要求严格 JSON 和逐结论证据引用。空响应、截断或本地 schema 失败最多做一次受控恢复；未知证据 ID、过滤或持续失败直接 `ABSTAIN`。OpenAI 适配器只作为显式备用。

#### 最终融合

[推荐门禁](src/gribuki_trade/policy/recommendation_gate.py) 默认技术 75%、宏观 25%，宏观权重上限为 40%。宏观结论为 `WATCH` 时其有效权重再减半；证据覆盖低于 10%、引用错配或模型弃权时不融合。强负面宏观分不高于 `-0.60` 时可以否决入场，但宏观永远不能把技术上未通过的 `WATCH` 升级为 `ENTER_CANDIDATE`。

报告将原始哈希转换为人类可读的证据编号，数值统一按可读精度展示；可原子导出 UTF-8 Markdown 和分页 PNG。长文本按换行拆成幂等 outbox 消息，再由 NapCat/OneBot 派发。

人工复核有 `PENDING_REVIEW/CONFIRMED/REJECTED/EXPIRED/CANCELLED` 五态及独立 append-only 审计。CLI 的 `confirm` 还必须显式输入 `RESEARCH_ONLY`；确认仅代表研究结论已复核，绝不产生委托。

### 5. A 股 PAPER、账本与保守撮合

A 股 PAPER 刻意拆成三层，避免把记账、撮合假设和崩溃恢复混为一谈：

1. [持久成交账本](src/gribuki_trade/services/ashare_paper.py) 有 CLI：保存现金、持仓、均价、已实现盈亏、`today_buy/available_to_sell` T+1、佣金/最低佣金/过户费/卖出印花税。人工与模拟 fill 共用契约；`fill_id` 幂等，事件带逐账户 SHA-256 链并由同一投影重放。
2. [保守日线撮合器](src/gribuki_trade/services/ashare_paper_matching.py) 是 Python API：支持 `PENDING/PARTIALLY_FILLED/FILLED/CANCELLED/REJECTED/EXPIRED`、NEXT_TRADING_BAR/GTD、限价触及、100 股买入整手、卖出尾仓、FIFO、默认 1% bar 成交量参与、跨 bar 部分成交和不穿限价的保守滑点。调用方必须提供完整未复权 bar 和明确价格区间；停牌、缺 OHLC、零量、缺价格带或越界全部不成交。
3. [durable 恢复层](src/gribuki_trade/services/ashare_paper_recovery.py) 与 [订单事件库](src/gribuki_trade/storage/paper_orders.py) 保存完整 `RUN_STARTED` 输入、状态、fill 和完成事件，并使用 writer lease。资金账本和订单库是两个 SQLite 数据库，因此采用确定性 `fill_id` + 恢复 saga，而不宣称跨库 ACID；调用方必须先 `recover()`。

当前撮合与恢复层没有 CLI/GUI，也不模拟盘口路径、排队、集合竞价、公司行动、自动历史涨跌停、融资、多币种或真实冲击。支持的股票与 ETF 暂按保守 T+1 管理。

### 6. 策略优化与技术面探索

[strategy_lab](src/gribuki_trade/strategy_lab) 是离线研究层，不会在线自动修改推荐权重：

- 冻结 Data/Strategy manifest、内容 SHA-256 和来源 revision；
- expanding walk-forward，训练/验证之间 purge，验证后 embargo；
- 最终 holdout 只在选择锁定后使用；
- simplex 权重、宏观权重不超过 40%、多成本情景取最差目标；
- 记录每折指标、基线、试验总数、贡献和多重假设警告；
- append-only 实验存储，同 ID 不同内容立即冲突。

因子 DSL 不使用 `eval`，只允许 OHLCVA 列、`lag/return/ma/vol/zscore` 和有限算术；属性访问、下标、导入、任意函数、动态窗口、除零和非有限值均失败关闭。候选生成由版本化模板 grammar 驱动，使用惰性笛卡尔积、显式 `max_trials` 和 100,000 次绝对上限；超预算整批失败，不返回截断的“幸运结果”。规范化重复、DSL 拒绝和开发集相关性筛除都有审计记录。

当前真实评价器是单证券 long/cash 的完整 PIT 日线模型，支持下一开盘/保守开盘限价、现金、T+1、整手、费用、滑点、价格带、停牌和成交量门禁。它还不是横截面组合回测器，不能处理历史全市场成分、退市股、公司行动和组合容量，因此实验结果不会自动发布到线上策略。

完整的工程判断与后续验收路线见 [后续开发计划实施评估](docs/DEVELOPMENT_PLAN_IMPLEMENTATION_260814.md)。

## 架构

```text
外部网页/API/文件
       │
       ▼
adapters + ingest ──────► immutable raw documents / normalized events
       │                              │
       ▼                              ▼
ports 契约                  evidence / revision / source health
       │                              │
       └────────► services 编排 ◄─────┘
                       │
          features / policy / strategy_lab
                       │
       candidate / recommendation / review / reports
                       │
              durable outbox / NapCat

研究输出 ──显式操作──► PAPER ledger / deterministic matcher

未来真实执行：signal → portfolio/plan → risk → OMS → guarded broker
```

核心依赖方向：

- `domain`：不可变值对象和状态语义；
- `features`、`strategy`、`backtest`、`strategy_lab`：尽量纯计算；
- `services`：用例编排、失败隔离和门禁；
- `ports`：行情、日历、LLM、通知、账本和 broker 契约；
- `adapters`、`ingest`：外部数据和协议边界；
- `storage`、`trading`：SQLite 审计、OMS 和执行状态；
- `runtime`、`security`：PAPER/SHADOW/LIVE 守卫与 OS 凭据；
- `reporting`、`gui`：报告产物和桌面展示。

策略、LLM、GUI 和复核服务都不能直接导入券商 SDK。`PAPER` 禁止访问真实 broker；`SHADOW` 只允许连接、查询和订阅；`LIVE` 守卫需要当前进程内精确确认短语、账户白名单和交易所白名单。该守卫只是安全原语，当前没有已经验收的用户侧 LIVE 工作流。

## 环境与安装

项目要求 Python `>=3.11,<3.13`，当前 CI 在 Windows 的 Python 3.11/3.12 上运行。Windows 是主要验收平台；Python 核心和 Qt 以跨平台为目标，但 macOS 尚未进入当前 CI。NapCat 启动脚本仅适用于 Windows。

```powershell
# 在仓库根目录创建独立环境
py -3.12 -m venv .venv

# 不必激活环境，直接用固定解释器安装
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"

# 查看完整命令面
.\.venv\Scripts\python.exe -m gribuki_trade --help

# 动态检查 SQLite shared-WAL 运行库安全性
.\.venv\Scripts\python.exe -m gribuki_trade sqlite-runtime-status

# 运行质量门槛
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m mypy src
.\.venv\Scripts\python.exe -m pytest -q

# 启动纯演示 GUI
.\.venv\Scripts\python.exe -m gribuki_trade gui
```

依赖和版本区间以 [pyproject.toml](pyproject.toml) 为准。

## 常用工作流

### A 股发现与研究

以下命令会访问外部市场/新闻源；盘中、盘后时间门槛由代码检查。

```powershell
# 静态研究覆盖池：45 个标的（36 股 + 9 ETF），不是持仓或买入名单
.\.venv\Scripts\python.exe -m gribuki_trade ashare-watchlist

# 15:05 后执行当前交易日股票三层筛选；默认同时写候选和运行档案
.\.venv\Scripts\python.exe -m gribuki_trade ashare-market-screen-once `
  --top-n 30 --factor-budget 300 `
  --candidate-db runtime/research/candidates.sqlite3 `
  --run-db runtime/research/runs.sqlite3 `
  --output runtime/screening/latest.json

# 开市时段运行一次异常发现
.\.venv\Scripts\python.exe -m gribuki_trade ashare-intraday-scan-once `
  --candidate-db runtime/research/candidates.sqlite3 `
  --run-db runtime/research/runs.sqlite3 `
  --output runtime/surveillance/latest.json

# 查看或维护候选
.\.venv\Scripts\python.exe -m gribuki_trade ashare-candidates list

# 只跟踪 ACTIVE 候选一个有限周期
.\.venv\Scripts\python.exe -m gribuki_trade ashare-research-watch `
  --candidate-db runtime/research/candidates.sqlite3 `
  --candidates-only --cycles 1

# 单标的收盘深研，并导出 Markdown + PNG
.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-once `
  --symbol 510300.SH --report-dir runtime/reports

# 有界批量深研 ACTIVE 候选
.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-batch `
  --candidate-db runtime/research/candidates.sqlite3 --limit 10

# 查看不可变筛选/扫描输出与 lineage
.\.venv\Scripts\python.exe -m gribuki_trade ashare-research-runs list
```

### DeepSeek

API key 通过无回显提示写入 Windows Credential Manager/macOS Keychain，不写入仓库或命令历史。

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade deepseek-configure
.\.venv\Scripts\python.exe -m gribuki_trade deepseek-status
```

默认模型是 `deepseek-v4-flash`。也可在收盘命令中显式指定 `--macro-provider openai`，但两种模型都受同一 EvidencePack 与推荐门禁约束。

### NapCatQQ

`vendor/` 被 Git 忽略，克隆项目不会自带 NapCat 源码、Shell 或 QQ 运行物。当前本机启动脚本依赖已安装/解压的特定 Windows 运行目录；安装、扫码登录和端口配置见 [本机研究配置](docs/LOCAL_RESEARCH_SETUP.md)。

```powershell
# 本机已有受支持的 vendor 运行物后启动
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start_napcat.ps1

# 只读状态检查
.\.venv\Scripts\python.exe -m gribuki_trade napcat-status

# 显式发送一条固定测试消息
.\.venv\Scripts\python.exe -m gribuki_trade napcat-send-test `
  --target-kind private --target-id <YOUR_QQ_ID> --confirm SEND_TEST

# 派发 durable outbox 中已入队的分段报告
.\.venv\Scripts\python.exe -m gribuki_trade napcat-dispatch `
  --target-kind private --target-id <YOUR_QQ_ID> --cycles 1

# 发送本地报告根内的一张 PNG；文件模式支持 GIF/JPEG/JPG/PNG/WEBP/CSV/MD/PDF/TXT
.\.venv\Scripts\python.exe -m gribuki_trade napcat-send-artifact `
  --target-kind private --target-id <YOUR_QQ_ID> `
  --artifact-kind image --artifact-root runtime/reports `
  --artifact 510300/page-01.png --confirm SEND_ARTIFACT
```

OneBot 只允许 loopback HTTP、Bearer token 和精确私聊/群白名单；系统不监听 QQ 命令。附件必须是报告根内的本地普通文件，拒绝 URL、UNC、路径穿越和符号链接。

### 推荐复核

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-review open `
  --recommendation-id <RECOMMENDATION_ID>
.\.venv\Scripts\python.exe -m gribuki_trade ashare-review list
.\.venv\Scripts\python.exe -m gribuki_trade ashare-review confirm `
  --case-id <CASE_ID> --reason MANUAL_EVIDENCE_REVIEWED `
  --confirm RESEARCH_ONLY
```

### A 股 PAPER 账本

这些 CLI 只记录显式动作，不会自动抓行情或撮合。

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper open `
  --account personal-paper --initial-cash 100000 `
  --session-date 2026-08-14

.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper fill `
  --account personal-paper --fill-id manual-20260814-001 `
  --symbol 600000.SH --side BUY --quantity 100 --price 10.00 `
  --instrument STOCK --source MANUAL --session-date 2026-08-14

.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper snapshot `
  --account personal-paper
```

durable 日线委托撮合仍只提供 Python API，详见 [A 股 PAPER 文档](docs/A_SHARE_PAPER_TRADING.md)。

### 策略实验

```powershell
# 只扩展版本化、安全、有限的因子 grammar；不访问行情或 holdout
.\.venv\Scripts\python.exe -m gribuki_trade strategy-factor-discover `
  --max-trials 100 --output runtime/strategy/factor-inventory.json
```

完整实验协议见 [策略实验室](docs/STRATEGY_LAB.md)。

### Binance 与 Schwab

Binance Spot Testnet 已有 status、order test、普通虚拟 cycle、durable OMS cycle/fill、公开历史归档、回测和本地 SHADOW。带 `cycle/fill` 的命令会改变远端 Testnet 状态，必须使用显式确认哨兵；它们不会使用 Live key。

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-status
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-order-test
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-oms-cycle --confirm TESTNET
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-oms-fill --confirm TESTNET_FILL
.\.venv\Scripts\python.exe -m gribuki_trade binance-history-sync --help
.\.venv\Scripts\python.exe -m gribuki_trade binance-backtest --help
.\.venv\Scripts\python.exe -m gribuki_trade binance-shadow-run --help
```

Schwab 当前没有 CLI 或真实生产联调。OAuth、Market Data、Trader REST 和简单限价接口只完成了代码与离线 transport 测试；获得 Developer App 后，应先做真实网络下的只读授权、entitlement、限频和账户对账。当前状态见 [Binance 模拟交易状态](docs/BINANCE_SIMULATION_STATUS.md) 和 [Binance/Schwab 接口边界](docs/BINANCE_SCHWAB_INTEGRATION.md)。

## 仓库结构

| 路径 | 职责 |
|---|---|
| `src/gribuki_trade/domain` | 订单、候选、推荐、复核、PAPER 等不可变领域模型 |
| `src/gribuki_trade/ports` | 行情、日历、LLM、通知、账本和 broker 协议 |
| `src/gribuki_trade/adapters` | AKShare/BaoStock、官方数据、DeepSeek/OpenAI、NapCat、Binance、Schwab 等外部边界 |
| `src/gribuki_trade/ingest` | 新闻、公告、官方宏观与搜索发现采集 |
| `src/gribuki_trade/features` | 盘中异常、收盘筛选和技术因子纯计算 |
| `src/gribuki_trade/services` | 筛选、研究、候选、复核、通知、PAPER 和恢复编排 |
| `src/gribuki_trade/policy` | 技术/宏观融合与推荐发布门禁 |
| `src/gribuki_trade/storage` | SQLite 事件库、候选、研究、outbox、PAPER、OMS 和运行档案 |
| `src/gribuki_trade/strategy` | 可复用的确定性策略基线 |
| `src/gribuki_trade/strategy_lab` | manifest、walk-forward、因子 DSL/发现与 A 股评价器 |
| `src/gribuki_trade/backtest` | 成本与推荐事后评价 |
| `src/gribuki_trade/trading` | broker-neutral OMS 与执行状态 |
| `src/gribuki_trade/reporting` | Markdown/PNG 研究产物 |
| `src/gribuki_trade/runtime` | PAPER/SHADOW/LIVE 模式和 broker 守卫 |
| `src/gribuki_trade/security` | OS keyring、token 和密钥边界 |
| `src/gribuki_trade/gui` | 使用占位数据的 PAPER 桌面演示壳 |
| `config` | 静态 A 股研究覆盖池 |
| `scripts` | Windows NapCat 本地启动辅助脚本 |
| `tests` | 离线契约、PIT、持久化、恢复、CLI 和 GUI smoke 测试 |
| `.github/workflows` | Windows Python 3.11/3.12 质量流水线 |

`runtime/`、`secrets/`、`vendor/`、`data/`、`logs/`、数据库、Parquet 和本地密钥文件均被 `.gitignore` 排除。

## 数据、时间与安全不变量

1. **Point-in-Time**：回放只能读取 `available_at <= decision_time` 的数据；发布时间早但首次抓取晚的内容不能穿越。
2. **原始值不覆盖**：原始响应、内容哈希和修订链追加保存；同自然 ID 的新内容是 revision，不覆写旧证据。
3. **未复权成交价与研究特征分离**：成交/账本用原始价格；当前缺完整 PIT 企业行动因子时宁可弃权，也不拿今天的前复权序列改写过去。
4. **缺失不是中性**：行情陈旧、覆盖不足、字段缺失、证据无效或模型错误均降级/弃权，不补成 0 分继续入场。
5. **LLM 只解释证据**：模型不抓网页、不调用工具、不接 broker，所有事实声明必须引用已保留证据。
6. **研究与执行隔离**：候选、推荐、通知和 `CONFIRMED` 复核都不是订单；策略不能绕过 portfolio/risk/OMS 边界。
7. **凭据不进仓库**：使用无回显输入和 OS keyring；日志、异常和 `repr` 不输出 token、账户或正文。
8. **公开网页源不是交易级行情**：AKShare/腾讯/东财用于研究和线索发现，不声称具备交易所序列、补包、低延迟或 SLA。

## SQLite shared-WAL 运行门槛

项目多个持久化组件使用 SQLite WAL。每次部署先运行：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade sqlite-runtime-status
```

当前安全策略只认可官方已修复线：`3.44.6`、`3.50.7` 和 `>=3.51.3`。若命令返回 `SQLITE_WAL_RESET_RUNTIME_UNSAFE`，单连接、本地、单 Store 流程仍可临时使用，但同一个数据库路径不得被两个 CLI、worker 或 Store 实例并发打开；常驻或多进程部署必须失败关闭。业务 writer lease 不能修复 SQLite checkpointer/writer 竞争。背景见 [SQLite WAL-reset 说明](https://www.sqlite.org/wal.html#walresetbug)。

## 质量门槛

[quality.yml](.github/workflows/quality.yml) 在 Windows、Python 3.11/3.12 上运行：

```powershell
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m mypy src
.\.venv\Scripts\python.exe -m pytest -q
```

测试覆盖 provider schema/超时/降级、PIT 与 revision、筛选/候选、技术/宏观门禁、LLM 证据引用、通知 outbox、PAPER/OMS 恢复、策略实验、CLI 和 GUI smoke。网络集成与长时间 soak 不由离线单元测试替代。

## 文档索引

### 当前状态与实现

- [后续开发计划：工程评估、落地状态与验收路线](docs/DEVELOPMENT_PLAN_IMPLEMENTATION_260814.md)：当前六类能力的权威实施状态。
- [A 股数据源矩阵](docs/A_SHARE_DATA_SOURCES.md)：来源口径、发布时间、PIT、降级与许可层级。
- [A 股 PAPER 账本与撮合](docs/A_SHARE_PAPER_TRADING.md)：账本、matcher 和崩溃恢复边界。
- [策略实验室](docs/STRATEGY_LAB.md)：walk-forward、DSL、候选发现与评价器。
- [本机 A 股研究配置](docs/LOCAL_RESEARCH_SETUP.md)：DeepSeek、NapCat、扫码和附件发送。
- [Binance 模拟交易状态](docs/BINANCE_SIMULATION_STATUS.md)：Testnet、历史回放和 SHADOW 的较新验收记录。

### 设计、调研与历史记录

- [原始后续开发计划](docs/后续开发计划260813.md)：用户提出的六类目标，不等同于完成清单。
- [A 股研究 MVP 与参考项目审阅](docs/A_SHARE_RESEARCH_MVP.md)：长篇设计和 `daily_stock_analysis` 取舍；部分状态是历史快照。
- [全市场筛选、搜索发现与宏观融合](docs/A_SHARE_SCREENING_AND_DISCOVERY.md)：算法和边界说明；顶部个别状态早于候选库/运行档案实现。
- [总体 ROADMAP](docs/ROADMAP.md)：目标态路线，不是当前功能清单。
- [零注册依赖策略基线](docs/ZERO_REGISTRATION_STRATEGIES.md)：早期策略与执行假设。
- [Binance Spot 与 Schwab 接入](docs/BINANCE_SCHWAB_INTEGRATION.md)：接口设计边界；Binance 部分“尚缺项”早于 durable OMS 实现。
- [广发证券接入清单](docs/GF_SECURITIES_ONBOARDING.md)：未来 QMT 询问与开户信息清单。

## 下一阶段

优先级不是继续增加更多自由 LLM Agent，而是补齐可否证的数据与运行闭环：

1. 每日归档完整全市场股票池、排除原因、因子输入、公司行动和历史证券主数据，形成真正的 PIT 横截面数据集。
2. 实现交易日历感知、互斥、背压、misfire 和休眠恢复的常驻协调器，并在安全 SQLite 运行库上做连续 soak。
3. 建立横截面组合级 A 股回测器，覆盖退市股、历史 ST/停牌/涨跌停、T+1、成本、容量、行业/规模暴露和基准。
4. 为 durable PAPER matcher 增加显式恢复 CLI、GUI 和完整运行观测，但仍不伪造盘口队列。
5. 建立策略注册、统计校正、人工审核和 shadow 发布流程；未通过独立 holdout 的新因子不得进入生产配置。
6. 把 GUI 从演示壳接入后台只读任务、候选矩阵、证据、PAPER 账本和 dead-letter 管理。
7. 真实券商只在只读联调、对账、风险、kill switch 和长时间 PAPER/SHADOW 验收完成后考虑开放。

