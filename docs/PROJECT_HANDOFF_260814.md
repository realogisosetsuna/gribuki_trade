# Gribuki Trade 项目完整交接文档（260814 计划，2026-08-15 冻结）

> 文档状态：依据 2026-08-15 工作树中的代码、CLI、测试与运行文档审计。
> 本文是该时点的维护快照，不是永久权威；当前行为始终以仓库代码、CLI parser、持久化 schema 和测试为准。

## 1. 交接结论

Gribuki Trade 当前是一套以 A 股研究、PAPER 仿真、已成交实盘事实观察和可审计报告为主线的
Python 交易工作台。它已经具备从全市场发现、候选管理、单标的研究、双轨 LLM 复核、PAPER
盘中运行、QUICK/DEEP 保护计划、盘后复盘、NapCat 通知，到离线策略评估的较完整业务组件。

当前最重要的三条边界如下：

1. **A 股链路没有真实券商下单权限。** `ashare-paper-day` 只在 PAPER 账本中撮合；
   `live-sync` 只接收用户已经在券商完成的成交事实，生成保护分析并提醒，不创建券商订单。
2. **仓库没有应用常驻运行框架。** 单次或有限轮 CLI 已实现，但交易日调度、长期进程监督、
   OneBot 入站监听和自动跨阶段衔接，等待未来统一应用 runtime；不使用 Windows Task Scheduler
   或独立 PowerShell watchdog。
3. **研究分数和退出参数尚未完成收益概率校准。** 技术分、LLM 分、R 倍目标和 ATR 参数均是
   可审计的策略输入或 barrier，不是上涨概率、预期收益承诺或已证明的最优参数。

当前可交付能力可以概括为：

```text
公开市场/新闻/官方数据
        │
        ├─► 收盘全市场筛选 / 盘中异常发现
        │            │
        │            └─► append-only 候选库与有限轮跟踪
        │
        ├─► 单标的技术研究 + PIT 新闻/宏观证据
        │            │
        │            └─► baseline + 结构化对抗双轨 LLM
        │                         │
        │                         └─► 推荐门禁 / 六类报告 / NapCat outbox
        │
        ├─► PAPER-day：买前 QUICK → 保守撮合 → 成交后 DEEP → barrier 观察
        │
        └─► live-sync：已成交事实双确认 → 独立实盘观察账本
                               → 成交后保护计划 → 单轮行情跟踪 → 卖出提醒

冻结历史样本 ─► strategy_lab walk-forward / holdout / trial registry
```

## 2. 产品目标与非目标

### 2.1 已实现目标

- 用公开数据完成 A 股股票全市场的收盘筛选和盘中异常发现；
- 把手工关注、筛选、异动和复核统一为可重放候选事件；
- 对单一股票或 ETF 生成带来源、时点、修订和反证条件的研究结论；
- 在所有生产语义分析路径中并行保留 baseline 与结构化对抗 LLM 结果；
- 用不可变 PAPER 账本和保守分钟 bar 规则模拟一个交易日；
- PAPER 在订单前建立 QUICK，成交后以多时间框架证据建立或降级到 DEEP；`live-sync` 则只能在券商 BUY 成交事实确认入账后有限尝试补建 QUICK；
- 接收并审计用户已经在真实券商完成的成交，但不获得下单权限；
- 用六类固定报告契约区分即时告警、成交回执、持仓复核、日报、标的深研和系统健康；
- 用 durable outbox 向 NapCat/OneBot 发送短消息或报告文件；
- 用冻结数据、purge/embargo walk-forward 和独立 holdout 评价权重、因子与退出策略。

### 2.2 明确非目标

- 不自动登录或操作 A 股券商；
- 不把 QQ 消息、LLM 结论、研究确认或 PAPER fill 解释为真实委托授权；
- 不提供交易所 tick、逐笔委托、L1/L2 队列或真实冲击成本仿真；
- 不在线学习，不根据当天盈亏自动改权重、参数或因子；
- 不在操作系统层安装计划任务、服务或 watchdog；
- 不自动处理、迁移或删除用户自行明文保存的 API 文件；
- 不宣称离线测试等于真实网络、真实交易日全天或真实 NapCat 交付验收。

## 3. 技术栈与运行环境

项目元数据见 [pyproject.toml](../pyproject.toml)。核心要求是 Python `>=3.11,<3.13`，主要依赖为：

- `akshare`：公开市场快照、新闻、公告及部分宏观接口；
- `baostock`：A 股未复权日线和交易日历；
- `httpx`：HTTP provider、OneBot 和 LLM 网络调用；
- `keyring`：API key 与 OneBot token 的 OS 凭据存储；
- `pandas`：数据转换；
- `PySide6`、`pyqtgraph`：桌面 GUI；
- `websockets`：已有市场适配器的流式连接；
- `pytest`、`ruff`、`mypy`：测试、静态检查和严格类型检查。

Windows 是当前主要验收平台，CI 覆盖 Python 3.11/3.12。Qt 与核心 Python 代码以跨平台为目标，
但不能把“可跨平台”理解成 macOS/Linux 已完成相同强度的运行验收。

推荐初始化：

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m gribuki_trade --help
```

## 4. 目录、职责与依赖方向

| 目录 | 当前职责 |
|---|---|
| [`domain`](../src/gribuki_trade/domain) | 候选、事件、推荐、订单、PAPER、退出计划、实盘观察等不可变领域语义 |
| [`features`](../src/gribuki_trade/features) | 收盘筛选、盘中异常、技术分析、QUICK/DEEP 退出计划等尽量纯计算逻辑 |
| [`strategy`](../src/gribuki_trade/strategy) | 可复用的确定性策略基线 |
| [`strategy_lab`](../src/gribuki_trade/strategy_lab) | 冻结 manifest、walk-forward、因子 DSL、候选发现、A 股与退出策略评价器 |
| [`backtest`](../src/gribuki_trade/backtest) | 成本模型、加密资产回测和推荐事后评价 |
| [`ports`](../src/gribuki_trade/ports) | 行情、新闻、LLM、通知、broker、账本等面向外部能力的协议 |
| [`adapters`](../src/gribuki_trade/adapters) | AKShare、BaoStock、DeepSeek、OpenAI、OneBot、Binance、Schwab 等具体适配器 |
| [`ingest`](../src/gribuki_trade/ingest) | 新闻、公告、RSS、官方网页和搜索发现采集 |
| [`pipeline`](../src/gribuki_trade/pipeline) | 标准化、去重和 revision 处理 |
| [`services`](../src/gribuki_trade/services) | 业务用例编排、门禁、失败隔离、恢复和通知接线 |
| [`policy`](../src/gribuki_trade/policy) | 技术/宏观融合及推荐发布门禁 |
| [`storage`](../src/gribuki_trade/storage) | SQLite 事件库、账本、哈希链、租约、outbox 和运行档案 |
| [`reporting`](../src/gribuki_trade/reporting) | 六类报告契约、Markdown/PNG 产物和 PAPER/盘后摘要 |
| [`runtime`](../src/gribuki_trade/runtime) | 模式守卫、PAPER 跨日账本、共享集成配置、系统唤醒和临时目录解析 |
| [`security`](../src/gribuki_trade/security) | keyring、秘密提供者和外部 URL 安全边界 |
| [`trading`](../src/gribuki_trade/trading) | broker-neutral OMS 与订单执行状态机 |
| [`gui`](../src/gribuki_trade/gui) | PySide6 桌面应用及 NapCat/LLM 集成管理 |
| [`config`](../config) | 静态 A 股研究覆盖池 |
| [`scripts`](../scripts) | 测试入口和受控临时目录盘点/归档；不含 OS 级交易编排 |
| [`tests`](../tests) | 领域、PIT、持久化、并发、恢复、CLI、报告和 GUI smoke 测试 |

依赖方向应保持为：领域和纯计算不依赖 provider；服务依赖端口；适配器实现端口；CLI、GUI 和
runtime 负责装配。研究、LLM、GUI 和候选服务不得直接导入券商 SDK。任何未来真实执行都必须
沿 `signal → portfolio/plan → risk → OMS → guarded broker` 进入，而不是从研究模块旁路下单。

## 5. 数据时点、修订与持久化不变量

### 5.1 Point-in-Time（PIT）

PIT 是全仓最重要的数据边界：

- 只有 `available_at <= decision_time` 且 `first_seen_at <= decision_time` 的证据可以进入决策；
- `published_at` 早，不代表本系统当时已经看到；回放不得把后来抓到的旧文章穿越回去；
- 已完成 bar 才能进入信号、QUICK、DEEP 或 barrier 观察；未完成分钟线不参与；
- 收盘研究在 15:05 前不使用当天收盘线；交易日和下一交易日由 BaoStock 日历验证；
- 未复权成交价与研究特征分离；企业行动信息不足时失败关闭，不用今天的前复权序列改写过去；
- 数据陈旧、覆盖不足、字段缺失或 revision 不一致时降级或弃权，不把缺失补成“中性 0 分”。

标准事件模型见 [events.py](../src/gribuki_trade/domain/events.py)，事件库存储见
[event_store.py](../src/gribuki_trade/storage/research/event_store.py)。同一自然事件出现新内容时追加 revision，
不覆盖旧版本；`latest_as_of()` 会返回决策时点真正可见的最新 revision，而不是今天的最终版本。

### 5.2 原始证据与来源血缘

原始响应按内容哈希落盘，规范化事件保留来源 ID、来源 revision、抓取/可见时间和内容摘要。
研究运行、市场证据和策略实验另行保存配置、代码/策略版本和输入哈希。由此可以回答：

- 当时系统看到了什么；
- 哪个来源、哪个 revision 形成了该结论；
- 相同输入和配置是否会得到同一运行 ID；
- 后来的修订有没有被错误用于旧决策。

### 5.3 append-only 与哈希链

并非每一张 SQLite 表都是哈希链，但关键不可变事实流使用逐事件 SHA-256 链，例如：

- PAPER 资金/成交账本；
- PAPER-day journal；
- PAPER durable order 事件；
- QUICK/DEEP 退出计划生命周期；
- 实盘观察成交账本；
- 双轨 LLM 角色与选择审计。

链中事件保存前序哈希、规范化 payload 哈希和当前事件哈希；读取或恢复时重新验证。候选、人工复核、
研究运行和策略实验也采用 append-only/idempotent 语义，但具体表契约不应被笼统称为同一种哈希链。

### 5.4 SQLite、事务与租约

多数存储使用 SQLite WAL、`BEGIN IMMEDIATE`、唯一幂等键和有期限 writer/worker lease。部署前必须运行：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade sqlite-runtime-status
```

当前只认可 SQLite 官方已修复 WAL-reset 问题的版本线：`3.44.6`、`3.50.7`、`>=3.51.3`。
若返回 `SQLITE_WAL_RESET_RUNTIME_UNSAFE`，同一路径不得由多个 CLI、进程或 Store 实例并发打开；
业务 lease 不能修复 SQLite runtime 自身的 checkpointer/writer 缺陷。

PAPER durable order store 与资金账本是两个数据库，使用确定性 fill ID 与恢复 saga，不宣称跨库 ACID。
实盘观察确认、批次投影和待处理保护工作位于自己的独立存储边界，不能与 PAPER 账户混用。

## 6. A 股发现、候选与研究链路

### 6.1 收盘全市场三层筛选

入口为 `ashare-market-screen-once`，实现见：

- [全市场数据适配器](../src/gribuki_trade/adapters/ashare/screening/screening.py)
- [筛选特征](../src/gribuki_trade/features/ashare_screening.py)
- [筛选服务](../src/gribuki_trade/services/ashare/research/ashare_screening.py)

当前流程只在收盘后运行股票全市场筛选：

1. 全市场网页快照使用东方财富主源、腾讯独立回退，并补沪、深、北交易所上市元数据；
2. L1 硬过滤上市时长、价格、当日成交额、市值、ST、停牌、交易状态和字段完整性；
3. 按当日成交额取最多 300 个幸存者补至少 201 个交易日未复权历史；
4. 要求 20 日平均成交额等流动性条件；
5. 对 20/60/120-skip-5 动量、MA 趋势、突破、量比、波动、回撤和 Amihud 等因子做
   winsorize、tie-aware 百分位、方向归一化和加权；
6. 默认输出 Top 30，同时写候选事件库和不可变研究运行档案。

候选只表示值得进一步研究，不是订单或买入清单。历史回放还必须有当时的股票池、快照和 source
revision；不能用今天的网页快照重建过去的全市场排名。

### 6.1.1 PAPER-day 盘前降级种子

`ashare-paper-day` 在上海当前自然日不晚于 09:25 时使用独立盘前适配器。它只在交易日历已经核验
上一交易日后，才把当前网页抓取结果保守绑定为上一交易日 research seed；行情与因子无条件标记
`DEGRADED`。这条路径只承诺沪深主板、创业板和科创板，排除北交所，也不等于集合竞价或历史收盘
归档。开盘后的当前 surveillance、技术、LLM、价格、数量、资金和 QUICK 门禁仍必须全部重新通过。

### 6.2 盘中异常发现与“有信号但没有订单”

入口为 `ashare-intraday-scan-once`，核心见
[ashare_surveillance.py](../src/gribuki_trade/services/ashare/research/ashare_surveillance.py)。当前交易时段为
09:30–11:30、13:00–15:00，使用当前全市场网页快照，检查快照新鲜度、沪深京覆盖、有效横截面
样本和可用因子权重。

异常分数包含涨跌强度、对数成交额、日内位置、开盘后延续、量比和换手率，输出类别包括
`MOMENTUM_EXPANSION`、`ACTIVE_STRENGTH`、`OBSERVATION_ONLY`。这些结果总带
`CURRENT_SESSION_SNAPSHOT_ONLY`，是线索，不是交易级报价。

PAPER-day 中“买入信号触发”与“创建订单”是两个阶段。出现以下稳定原因码时不下单属于正常门禁：

- `NO_CURRENT_SESSION_ANOMALY`：标的没有当日当前会话可用的异常证据；
- `ANOMALY_NOT_MOMENTUM_EXPANSION`：存在异常，但不属于允许进入买入复核的动量扩张类别；
- 盘中 LLM 尚未复核、失败或结果过期；
- 技术失效位、当日涨跌停价或可接受价格走廊无效；
- 资金储备、组合 gross、单标的敞口、单笔止损风险、数量规则或流动性不通过；
- 无法在订单前建立 QUICK，或第一根用于撮合的完整 bar 已触发 QUICK barrier；
- 下一完整分钟未触及限价、触及失效位、涨停封死或数据不足。

因此，通知中的“大写英文”是机器可重放的稳定审计码，不是 provider 的自然语言解释。用户报告层
通过 [报告契约](../src/gribuki_trade/reporting/contracts.py) 将已知码翻译为中文，同时保留原始码供定位。

### 6.3 统一候选库与有限轮跟踪

候选模型见 [candidates.py](../src/gribuki_trade/domain/candidates.py)，SQLite 存储见
[candidate_store.py](../src/gribuki_trade/storage/research/candidate_store.py)。候选支持：

- `LOW/NORMAL/HIGH/URGENT` 优先级；
- `ACTIVE/COOLING/EXPIRED/REMOVED` 状态；
- 来源、原因、证据、发现/观察时点和 TTL；
- 内容冲突检查、幂等事件和 PIT 重放。

默认 TTL 随来源不同，手工候选可长期保留。`ashare-research-watch` 是有上限的多标的轮询，支持
合并 ACTIVE 候选并隔离单标的失败；它不是 daemon。候选库支持 `.BJ`，但现有分钟研究链只承诺
`.SH/.SZ`；北交所当前主要覆盖收盘日线研究，不承诺盘中执行。

### 6.4 单标的和批量深研

主要入口为：

- `ashare-research-once`：盘中/分钟证据的一次研究；
- `ashare-close-research-once`：单标的收盘深研；
- `ashare-close-research-batch`：显式标的或 ACTIVE 候选的有界串行批处理；
- `ashare-post-close run`：对当天 PAPER 期末持仓生成盘后研究与日报。

收盘技术引擎见 [close_analysis.py](../src/gribuki_trade/features/close_analysis.py)，至少需要 201 个交易日，
覆盖 MA、ATR、Donchian、Bollinger、MACD、RSI、ADX、Stochastic、量价、Amihud、波动和回撤。
固定方向家族为趋势、多周期动量、突破/压缩、顺势回撤、量价流动性。相对强弱和市场宽度当前主要
展示，尚未完成样本外校准。

技术门先于 LLM：模型不能把技术 `WATCH` 升级成 `ENTER_CANDIDATE`。宏观/新闻可以降低置信度、
维持观察或否决入场；默认融合权重为技术 75%、宏观 25%，宏观上限 40%。强负面宏观可以否决，
但不能凭空创造买点。

## 7. 双轨 LLM 生产机制

生产装配见 [llm_production.py](../src/gribuki_trade/services/llm/llm_production.py)，对抗协议见
[adversarial_macro.py](../src/gribuki_trade/services/macro/adversarial_macro.py)，审计存储见
[adversarial_audit.py](../src/gribuki_trade/storage/execution/adversarial_audit.py)。

### 7.1 同一冻结证据上的两条轨道

每个生产语义分析 case 都并行运行：

- **baseline**：原分析器，对同一 EvidencePack 给出结构化结论；
- **adversarial**：固定角色、固定轮数的结构化对抗分析，要求引用证据 ID、提出反证与失效条件。

生产选择优先使用对抗结果，同时保留 baseline 供报告和审计。任一轨道不得自行抓网页、调用券商、
生成订单或改变确定性风险线。角色只能看到冻结 EvidencePack 和经过规范化的不可信同伴论点。

盘前上下文也遵循同一双轨边界：新生成的上下文必须同时携带 baseline、adversarial、生产采用轨和
已落盘的双轨审计 SHA，context ID 与候选计划清单会把这些字段一起纳入哈希。任一轨缺失、身份或
时点不一致、审计尚未落盘时，盘前语义上下文失败关闭。旧版 journal/manifest 仍可按原字段精确
恢复，但会明确标为历史记录未携带双轨信息，绝不根据当前模型反向补造过去结论。

当前档位为：

| 档位 | 对抗角色/轮次上限 | 单角色超时 | case 超时 | 使用场景 |
|---|---:|---:|---:|---|
| `FAST` / `INTRADAY` | 2 / 1 | 15 秒 | 24 秒 | PAPER 盘中候选及其成交后退出语义输入，外层协调上限 28 秒 |
| `STANDARD` | 3 / 2 | 60 秒 | 130 秒 | 普通研究、持仓复核 |
| `DEEP` | 5 / 3 | 180 秒 | 570 秒 | 收盘深研和 `live-sync` 成交后深度语义复核 |

“调用预算 unlimited”仅表示会话调用数上限为 `None`。角色数、轮数、单角色超时、case deadline、
证据引用校验和输出 schema 仍然有界，不能因为预算不限而无限循环。

这里的 LLM 档位名与退出计划的 `QUICK/DEEP` 生命周期不是同一个维度。PAPER 成交后的退出计划
虽然名为 `DEEP`，但仍复用当天 `INTRADAY/FAST` 双轨分析器，把单 case 限定在 24 秒 deadline
内；`live-sync` 在独立、可恢复的保护工作中使用 5 角色/3 轮的 `DEEP` 档位。

### 7.2 失败与冲突语义

- 对抗必需角色失败、超时、引用未知证据或聚合失败时，返回稳定失败码和 `ABSTAIN`/降级结果；
- baseline 与对抗出现实质方向冲突时，生产结果降低置信或失败关闭，不悄悄选择更乐观的一条；
- 盘中新买入 required 模式只读取已经落盘的复核缓存，不在临界路径等待 provider；
- `REDUCE` 不在分钟临界路径重新联网，而是读取成交后 DEEP 中持久保存的双轨评分；
- DEEP 或 barrier 通知要求两轨评分和生产采用轨彼此一致才标记“可用”；单轨成功时仍逐轨展示
  已取得的 baseline 或 adversarial 数值，并把另一轨标为不可用，但生产不会采用不完整双轨；
- 每个角色轮次、提示/证据哈希、模型身份、token 用量、终止原因、轨道结果和最终选择写入独立
  SQLite 哈希链；恢复后不会丢失“为什么选了这条轨道”。

### 7.3 Provider 与配置

当前支持 DeepSeek 与 OpenAI。默认模型配置来自
[integration_settings.py](../src/gribuki_trade/runtime/integration_settings.py)：DeepSeek 默认
`deepseek-v4-flash`，OpenAI 默认 `gpt-5.6`。生产 CLI 可显式覆盖 provider/model；未覆盖时读取
GUI 共享的非秘密配置。API key 由同一 OS 用户的 keyring 提供。

健康检查不属于语义交易分析：它只验证凭据/模型可用性，不写研究结论。OpenAI GUI 健康检查只读取
模型列表，不因此触发一次语义决策。

## 8. QUICK/DEEP 退出计划

退出计划领域模型见 [exit_plans.py](../src/gribuki_trade/domain/exit_plans.py)，生命周期服务见
[exit_plan_lifecycle.py](../src/gribuki_trade/services/exit/exit_plan_lifecycle.py)，持久化见
[exit_plans.py（存储）](../src/gribuki_trade/storage/execution/exit_plans.py)。

### 8.1 QUICK：成交前的临时保险

PAPER 新买单在提交前必须先建立 QUICK。算法见
[exit_planning.py](../src/gribuki_trade/features/exit_planning.py)：

- 只使用决策时已可见的完整分钟线；
- 计算 median/MAD 与 EWMA 组合的 robust ATR；
- 候选包含技术失效位、`2×robust ATR` 波动止损、突破支撑和已确认 swing low；
- 对多头取合法候选中更宽松、但仍严格低于最坏许可买价的保护位；
- 用“最坏许可买入限价 − QUICK 止损”重新计算每股风险和仓位；
- 目标价是预登记 R 倍 barrier，默认 1.5R，不是期望收益预测；
- 时间 barrier 由调用方用真实交易日历解析，不能直接加自然日。

如果用于撮合的 bar 已经触发 stop、target 或 time barrier，系统拒绝建立新仓。QUICK 的输入摘要、
策略版本、校准状态、价格和中间指标进入哈希事件流。

### 8.2 DEEP：成交后的多时间框架确认

DEEP 算法见 [deep_exit_planning.py](../src/gribuki_trade/features/deep_exit_planning.py)：

- 使用 1/5/15 分钟完整线；
- 每个时间框架计算 robust ATR、结构保护位和波动保护位；
- 用加权中位数融合，降低单一时间框架异常值影响；
- 吸收 baseline 与对抗语义分，但 LLM 只选择预登记 R 倍档位或缩短期限；
- 多头 DEEP 止损只能保持或上移，不能比 QUICK/上一版更松；
- 时间门只能保持或提前，不能延后；
- 两轨完整时计划可为 `CONFIRMED`，缺轨时明确为 `DEGRADED`；
- DEEP 失败时保留 QUICK，持仓不会因为模型故障失去保护。

### 8.3 barrier 观察语义

完成 bar 可触发 `STOP_LOSS`、`TAKE_PROFIT` 或 `TIME`。同一 OHLC bar 同时触发止损和止盈时固定
采用保守的 stop-first 顺序。T+1 可卖数为 0 时仍记录 barrier 和退出信号，但明确标记阻断，不伪造
卖出成交。生命周期服务本身不依赖 broker/order store，不创建订单。

PAPER-day 当前对 `REDUCE` 和退出 barrier 只写 journal、生成通知，不提交 PAPER 卖单；
`live-sync` 也只提醒用户，最终卖出必须由用户在券商完成后再同步事实。

## 9. A 股 PAPER 与跨日恢复

### 9.1 四层结构

1. [PAPER 账户服务](../src/gribuki_trade/services/ashare/paper_day/ashare_paper.py)：现金、持仓、均价、实现盈亏、费用、
   T+1 批次和 fill 幂等；
2. [日线保守撮合器](../src/gribuki_trade/services/ashare/paper_day/ashare_paper_matching.py)：六态限价单、NEXT_BAR/GTD、
   部分成交、价格带、成交量参与和保守滑点；当前主要是 Python API；
3. [durable 恢复层](../src/gribuki_trade/services/ashare/paper_day/ashare_paper_recovery.py)：订单事件、writer lease、
   跨库 fill saga；
4. [PAPER-day 编排器](../src/gribuki_trade/services/ashare/paper_day/ashare_paper_day.py)：单进程、按日隔离的盘前、盘中、
   撮合、保护、通知和报告流程。

### 9.2 PAPER-day 运行内容

`ashare-paper-day run` 经显式 `PAPER_DAY` 确认后：

- 验证上海当天交易日、BaoStock 日历和 NapCat 目标；
- 09:25 前执行始终降级的沪深盘前研究种子筛选，开盘后再用当前时段证据复核；
- 每 15 分钟维护全市场扫描，1 分钟主监控、5 分钟辅助监控；
- 对技术 `ENTER_CANDIDATE` 执行双轨 LLM、本会话异常、价格、资金、数量和风险门禁；
- 订单前持久化 QUICK，用下一根完整分钟 IOC 做保守撮合；
- 成交后绑定 fill、生成 DEEP，并持续观察已完成 bar；
- 写 journal、PAPER ledger、exit-plan store、outbox、sidecar 状态和 Markdown 日报；
- 收盘取消剩余待撮合项并生成最终投影。

默认初始资金为 20 万元。默认已取消固定“最多 5 个持仓”的硬上限；只有显式提供
`--maximum-positions N` 才启用持仓数量熔断。无论是否配置数量上限，以下门禁仍有效：

- 至少 20% 现金储备；
- 组合 gross 最高 80%；
- 单标的敞口最高 20%；
- 单笔初始止损风险预算最高 0.75%；
- 交易板块申报数量、费用、流动性和未决委托资金占用。

买入可接受价从严格高于技术失效位的首个合法 tick，到信号价加默认 0.1% 且不高于涨停价；
撮合 bar 触及或跌破失效位时不成交。卖出记录保存参考价减默认 0.1% 且不低于跌停价，到涨停价的
未来可接受走廊，但当前 runner 不据此自动卖出。

### 9.3 交易日历与时间 barrier

CLI 在运行准备阶段从 BaoStock 读取会话日前后自然日窗口，验证当前、上一交易日及足够的未来交易日，
把交易日序列和日历 SHA-256 冻结到 manifest。退出时间门从这些未来交易日中选择，节假日不会按普通
周一至周五错误推算。只有遗留/直接单元调用分支才有明确标记的 weekday 降级。

### 9.4 跨日账户连续性

[paper_account_chain.py](../src/gribuki_trade/runtime/paper_account_chain.py) 为每个交易日建立独立目录，
扫描同账户历史账本并验证：

- 新交易日准备先取得跨进程锁；
- 目录日期与账本投影日期一致；
- 历史事件哈希流前缀兼容，没有分叉；
- 当前交易日账本不存在时，才在 clone 前后复核来源并用 SQLite backup 原子复制最新有效账本；
- `ledger-lineage.json` 与 ledger 内嵌 immutable binding 共同保存来源日期、序列和哈希；
- sidecar 缺失但只有一个合法公共前缀时可恢复为 `RECOVERED_ORPHAN_CLONE`；
- 历史 seal 绑定完整事件数和尾哈希，封存来源日不得继续追加；
- 已存在当日账本不覆盖；binding 冲突、候选不唯一、符号链接、错误路径或并发准备均失败关闭。

现金、持仓、费用、T+1 批次和旧成交因此可跨日累计。按日目录共用父级
`runtime/paper/day/exit-plans.sqlite3`；启动会恢复 fill/退出计划之间的崩溃边界，并按账户与标的把
跨日持仓重新绑定到既有保护流。历史上没有 PIT 退出计划的旧持仓不会被后来数据补造保护计划。

### 9.5 崩溃与人工恢复

- 同一 run 有单 writer lease 和稳定 manifest；配置漂移不能借“恢复”绕过；
- journal 重放恢复候选、订单、fill、LLM 调用计数、双轨结果和保护计划；
- `DAY_ABORTED` 是终态，只有人工检查后显式加 `--recover-after-abort` 才可继续同一 run；
- 已写入 journal 的风险策略变化需要 `--confirm-risk-policy-change PAPER_RISK_POLICY_CHANGE`；
- PAPER `DAILY_REVIEW` Markdown 通过按日 `report-artifacts.sqlite3` 执行
  `PENDING → IN_FLIGHT → SENT/AMBIGUOUS`；claim 后异常、取消或崩溃禁止自动重发；
- 交易监控 `completed` 与日报双交付分开。CLI `ok` 还要求短文本无缺口且 Markdown `SENT`；
- `status.json` 持久保存附件状态、文本/总投递计数和 `daily_review_delivery_complete`；只读
  `ashare-paper-day status` 另给出 `operationally_complete`。这里顶层 `ok` 只表示 sidecar 查询成功，
  不能替代双交付结论；旧 sidecar 只能从 JSONL 保守恢复附件状态，无法证明文本完整时一律不报完成；
  无 notifier 是 `NOT_CONFIGURED` 缺口，不能误报完整；
- 历史 `REPORT_UPLOADED` 在文件名、生成哈希与冻结目标一致时迁移为 legacy `SENT`，不重传；
  旧 `REPORT_UPLOAD_FAILED` 因无法排除提供方已收件而迁移为 `AMBIGUOUS`；
- `AMBIGUOUS` 只能在提供方侧人工核验后，以
  `--confirm-report-artifact-recovery PAPER_REPORT_ARTIFACT_RECOVERY` 配合“确认已收件并补 SENT”或
  “确认未收件并重传一次”中的一个显式动作处理；授权本身写入不可变 journal，`RESEND` 还会在
  调用提供方前持久化单次消费事件，第二次歧义绝不复用旧授权；
- `status/report/summary` 只读 sidecar，不打开正在运行的 journal/ledger/outbox。

完整运行边界见 [A_SHARE_PAPER_TRADING.md](A_SHARE_PAPER_TRADING.md)。

## 10. `live-sync` 实盘观察链

`live-sync` 的关键代码为：

- [实盘观察领域模型](../src/gribuki_trade/domain/live_records.py)
- [入站成交服务](../src/gribuki_trade/services/live/live_trade_records.py)
- [实盘观察存储](../src/gribuki_trade/storage/live_records/live_records.py)
- [保护工作编排](../src/gribuki_trade/services/live/live_trade_orchestration.py)
- [单轮行情跟踪](../src/gribuki_trade/services/live/live_market_tracking.py)
- [真实保护输入装配](../src/gribuki_trade/services/live/live_protection_inputs.py)

### 10.1 两消息确认协议

入站只接受 OneBot v11 的普通好友私聊、白名单 QQ 发送者、合理时间窗和严格单行语法：

```text
GT-LIVE/1|account=...|command_id=...|side=BUY|symbol=600000.SH|quantity=100|price=10.00|instrument=STOCK|executed_at=...|commission=...|transfer_fee=...|stamp_tax=...|external_order_id=券商委托号|external_fill_id=券商成交执行号
GT-LIVE-CONFIRM/1|command_id=...|fingerprint=系统返回的指纹
```

第一条仅形成 proposal，不改变持仓。只有同一发送者提交正确 fingerprint，系统才原子确认“券商已经
成交”的事实。另有显式取消语法。新 proposal 强制提供 `external_fill_id`：`external_order_id` 表示
一张券商委托，`external_fill_id` 表示其中一次独立成交执行；跨命令唯一键使用执行号，所以同一委托
可安全记录多次部分成交，而相同执行号无论内容是否变化都只能确认一次。重复消息、相同 command ID
不同内容、错误费用、未来/过旧/非交易时段成交和卖出超过记录持仓均失败关闭。生产 CLI 使用 BaoStock
精确自然日日历，不按工作日猜测；供应商失败、载荷不完整和休市日都不会形成 proposal。

确认 BUY 会创建独立保护 identity 和 durable work；确认 SELL 会更新实盘观察持仓与相关批次，并在
最终数量为零时关闭保护。proposal、confirmation、position projection 与保护工作在同一实盘观察
边界中原子协调，崩溃重放不会生成第二套保护任务。full SELL 核销批次的同一事务会把尚未完成的
QUICK/DEEP 构建任务 fence 为终态；即使 DEEP 候选已写入另一个 SQLite，迟到 worker 也不能把它
切为 active。提醒入队在事务内复核 `remaining_quantity > 0` 和预期 `plan_stream_id`，计划切换或持仓
归零后不会新增旧提醒。

新 BUY 的确认事务提交后，`live-sync ingest` 还会执行一次有限的提交后钩子。它只按本次返回的
`protection_work_id` 领取 QUICK 工作，不会顺手领取更早的其他 BUY；QUICK 输入只依赖公开分钟行情
和 BaoStock 日历，不读取 LLM key。QUICK 原子公开并排队 DEEP 后，钩子只跟踪这一个
`protection_id` 一轮。若该轮完整 K 线已经触发 barrier，提醒会先以确认私聊 sender（或显式目标）
耐久写入本地 outbox，不要求 NapCat 此刻在线；`--base-url` 省略时读取 GUI 共享 OneBot 地址，
地址可用时追加一次有限网络派发。
钩子有 `--quick-timeout-seconds` 有界等待，任何行情、
日历、跟踪或 outbox 故障都不会回滚成交或已完成的 QUICK；顶层成交仍为成功，嵌套
`immediate_protection` 如实给出 `RETRY/PENDING` 与稳定错误码；NapCat 派发失败也只留在嵌套
`dispatch` 状态，不改变成交/QUICK 结论。DEEP 不在入站延迟路径调用，仍由
后续有限 `cycle` 使用 LLM 构建。

### 10.2 `cycle` 的实际工作

`live-sync cycle --confirm LIVE_SYNC_CYCLE` 是一次有限的应用级周期：

在打开账本或构建保护前，CLI 必须先验证通知目标、loopback OneBot URL、LLM provider/model、
对应 key 和精确确认词；任何一项缺失都使整轮失败关闭。后文“派发失败不回滚”仅指通过配置预检后
发生的网络/provider 结果故障，不表示未配置 OneBot 也能启动周期。

1. 先对所有 `plan_ready` 活动批次抓取已完成 1 分钟线、观察 barrier，并派发既有提醒；
2. 处理用户已确认 SELL 对应的关闭工作；
3. 对一个已经发生的 BUY 成交，以实际 fill 价建立并原子公开 QUICK；
4. 同一事务完成快速工作并排队独立、可恢复的 `BUILD_DEEP_PROTECTION`；
5. 使用真实公开分钟行情、BaoStock 日历和当前 provider 的生产双轨 LLM 构建一个 DEEP 候选；
6. 等待 DEEP 的有限时间内继续按配置间隔观察 barrier，新提醒立即进入目标限定 outbox 并派发；
7. 只有仍持有当前 work generation 的 worker 才能原子激活候选；随后本轮退出。

默认 DEEP 总等待为 600 秒，tracking pump 每 30 秒一次、最多 20 次，正好覆盖整个等待窗口；三个值
都可显式收紧，但配置必须保证 pump 覆盖 timeout。每个 DEEP attempt 使用独立候选物理流，
`live_protection_tracking.plan_stream_id` 才是生产 active 的唯一权威指针。旧 worker 在租约失效后迟到
写出的候选仍留在退出库供审计，却无法越过 live generation fence 切换该指针，也不会被观察或关闭
流程使用。

这里的 QUICK 时序与 PAPER 不同：实盘事实到达系统时，券商成交已经发生，因此无法做到券商下单前
保护；系统只能在确认成交后尽快建立临时 QUICK，再生成 DEEP。这正是 `live-sync` 仍属于观察链、
不是执行链的原因。

这里的第 3 步也承担即时钩子失败后的恢复；正常情况下，新 BUY 已在 `ingest` 返回前完成一次定向
QUICK 尝试和单轮跟踪。持续监看依然需要未来应用 runtime 周期调用 `cycle`，本实现没有创建任何
Windows 任务、服务或常驻监听器。

### 10.3 未包含的能力

- 没有 OneBot 反向 WebSocket/HTTP 常驻入站监听器；`ingest` 读取未来 runtime 转交或人工保存的事件；
- 没有持续循环；应用 runtime 需要周期调用 `cycle`；
- 没有券商查询对账，输入事实由白名单发送者双确认；
- 没有自动卖出。触发提醒后，用户在券商完成卖出，再同步 SELL 事实。

## 11. 六类报告契约

权威定义见 [contracts.py](../src/gribuki_trade/reporting/contracts.py)。

| 类型 | 中文名称 | 交付方式 | 必需章节 |
|---|---|---|---|
| `INTRADAY_ALERT` | 盘中交易告警 | 短文本 | 发生了什么、执行结果、关键价格、证据时点 |
| `EXECUTION_RECEIPT` | 成交与账户回执 | 短文本或 Markdown | 成交事实、费用与资金、持仓变化、后续保护计划 |
| `POSITION_REVIEW` | 持仓持续复核 | Markdown | 当前结论、保护计划、证据与反证、下一复核条件 |
| `DAILY_REVIEW` | 盘后日报与次日知识基线 | 短文本 + Markdown | 执行摘要、市场复盘、操作复盘、持仓深研、次日基线 |
| `INSTRUMENT_RESEARCH` | 标的深度研究 | 短文本或 Markdown（长研优先 Markdown） | 结论、技术结构、基本面与宏观、对抗观点、失效条件 |
| `SYSTEM_HEALTH` | 系统健康与数据质量 | 短文本或 Markdown | 总体状态、数据源、模型与通知、缺口与恢复动作 |

通用 renderer 校验标题、章节完整性、章节顺序和非空内容；自定义长报告也必须调用同一契约校验器。
稳定内部码在审计事件中原样保留，用户主报告通过中文映射解释；未知码显示为“尚未分类”并保留码值。

报告落地位置包括：

- PAPER-day 日报与增强摘要：
  [paper_day_summary.py](../src/gribuki_trade/reporting/paper_day/paper_day_summary.py)；
- 盘后日报和逐持仓复核：
  [post_close.py](../src/gribuki_trade/reporting/post_close.py)；
- 单标的深研：
  [ashare_close_analysis.py](../src/gribuki_trade/services/ashare/close/ashare_close_analysis.py)；
- Markdown/PNG 产物安全写入：
  [artifacts.py](../src/gribuki_trade/reporting/artifacts.py)。

盘后交付采用“一条可读短摘要 + 完整 Markdown 文件”，避免把长文切成没有上下文的 QQ 碎片。
分页 PNG 仍可作为单标的研究产物生成，但 Markdown 是完整文本的权威可读副本。

## 12. 盘后深度复盘

`ashare-post-close run/status/report` 复用当天已完成 PAPER sidecar 与哈希账本，对期末持仓串行深研，
形成日报与逐持仓报告，并投递到当天冻结的同一 NapCat 目标。

启动门槛包括：

- 上海时区同一天且 BaoStock 验证为交易日；
- 不早于 15:05；
- PAPER 生命周期已完成；
- 账户、配置和通知目标哈希与 manifest 一致；
- 单 writer lock 可获得；
- LLM、新闻和通知按显式配置可用或能如实降级。

每个持仓失败相互隔离。部分失败报告标记 `PARTIAL`；存在持仓但全部研究失败时，状态为可重试分析失败，
命令非零退出且不进入通知阶段。重复运行已完成 run 只返回幂等重放。

恢复分两类：

- 分析中断且尚未进入交付：人工或未来 runtime 可在相同冻结配置下加 `--recover-analysis`；
- provider 交付回执不确定：必须人工核对后加 `--recover-delivery`，因为可能造成重复消息。

不得通过删除 manifest、outbox 或账本“恢复”。详细状态机见
[A_SHARE_POST_CLOSE_AUTOMATION.md](A_SHARE_POST_CLOSE_AUTOMATION.md)。

## 13. 新闻、公告、官方来源与韧性

### 13.1 来源覆盖

当前证据族包括：

- 公共媒体线索：新浪、财联社、东方财富、同花顺及东方财富个股新闻；
- 公司公告：CNINFO 公告索引；
- 官方宏观/政策：国家统计局、人民银行、证监会、财政部、国家发改委、外汇局、上交所、
  Federal Reserve RSS；
- 人民币与利率：SAFE 中间价、Shibor、ChinaMoney FR/FDR、中债收益率曲线；
- 全球与跨市场：Cboe VIX、A/H/美/亚太指数和历史关系；
- A 股上下文：市场宽度、CFFEX IF、ETF 价/IOPV/份额、上交所期权风险参数；
- 可选发现：Tavily 或用户自管 SearXNG。

官方网页实现见 [official_macro.py](../src/gribuki_trade/ingest/official_macro.py)。HTML 解析后的每条 URL
再次通过来源白名单策略；不允许解析器输出越权域名或不安全跳转。Tavily/SearXNG 只是发现路由，
不是发布者：同一 URL 或同一注册发布域即使由多个 provider 返回也只产生 `HINT`，不能直接进入宏观
分。只有官方域，或规范化标题匹配且来自至少两个独立注册发布域，才能形成
`independent_publishers` 确认 basis；单一公共媒体线索仍需独立来源印证，才能被对抗轨作为事实引用。

### 13.2 失败隔离、退避与跨进程探针

[news_collection.py](../src/gribuki_trade/services/communications/news_collection.py) 对每个来源独立并发采集：

- `asyncio.gather(..., return_exceptions=True)` 防止单源异常取消整批；
- HTTP 层处理有限重试、`Retry-After` 和条件请求；
- 编排层对未知异常、输出不合法和本地落盘失败使用有上限指数退避与稳定错误码；
- cursor 和 `next_allowed_at` 持久化，重启不会忘记熔断状态；
- `news_source_probe_leases` 用 `BEGIN IMMEDIATE` 原子领取默认 5 分钟探针租约；
- lease token 对完成写入做 fencing，过期旧 owner 不能覆盖新探针的 cursor；
- 原文归档或事件库失败只影响该来源，不把成功写成成功游标。

这些机制提高可用性，但不证明新闻内容绝对准确。高影响结论仍应优先官方原文，并在报告中展示来源、
修订、可见时点和证据缺口。

## 14. GUI 与本机集成

GUI 入口是 `gribuki_trade gui`。集成页见
[integrations.py](../src/gribuki_trade/gui/integrations.py)，共享配置见
[integration_settings.py](../src/gribuki_trade/runtime/integration_settings.py)。

### 14.1 已实现控制

- DeepSeek/OpenAI provider 切换、各自模型 ID 保存和后台健康检查；
- 两个 provider 的 API key 写入 OS keyring，保存后清空输入框；
- NapCat runtime 目录、WebUI origin、OneBot origin 和 token 管理；
- OneBot 连通性、实现身份和 QQ 登录态检查；
- 显式启动本机 NapCat runtime、打开 WebUI；
- 只停止由当前 GUI 实例通过 `QProcess` 启动并持有的进程，不终止外部实例；
- 所有网络、keyring 和进程准备工作在后台 worker 中执行，避免阻塞 GUI 主线程；
- 错误信息脱敏，不显示 token、URL 查询参数、响应正文或敏感本机路径。

非秘密配置原子写入 `runtime/config/integrations.json`，schema 当前为 v2，支持 v1 DeepSeek-only 配置
读取迁移。OneBot/WebUI 只允许无凭据的 loopback HTTP(S) origin。PAPER、普通研究、盘后研究和
`live-sync cycle` 在 CLI 未覆盖时读取这套 provider/model/OneBot 默认值。

### 14.2 GUI 边界

GUI 的交易展示页仍是演示，不连接 A 股券商。GUI 不是常驻交易 runtime，也不负责交易日历调度、
PAPER 自动启动、盘后自动衔接或 OneBot 入站监听。NapCat 的安装、QQ 扫码登录和真实模型凭据仍需
操作者在本机完成一次人工验收。

## 15. strategy_lab 与退出策略评价器

`strategy_lab` 是纯离线研究层，不修改线上配置、PAPER 或实盘观察账户。

### 15.1 通用策略实验能力

- 冻结数据/策略 manifest、内容 SHA-256、字段、来源 revision 和代码版本；
- expanding walk-forward，训练与验证间 purge，验证后 embargo；
- 最终 holdout 只在候选选择锁定后打开；
- 多成本情景使用最差表现，避免低成本假设掩盖脆弱性；
- simplex 权重、宏观权重上限 40%、技术家族上限；
- 安全因子 DSL，只允许 OHLCVA、有限算术和 `lag/return/ma/vol/zscore`；
- 版本化、有预算的因子 grammar，拒绝 `eval`、属性访问、导入、动态窗口和非有限值；
- append-only 实验登记，同实验 ID 不同内容冲突。

单证券 A 股日线评价器支持下一开盘/保守限价、现金、T+1、整手、费用、滑点、价格带、停牌和成交量
门禁，但不是全市场横截面组合回测器。

### 15.2 退出策略 evaluator

新增的真实退出策略评价器见：

- [exit_policies.py](../src/gribuki_trade/strategy_lab/exit_policies.py)
- [exit_evaluator.py](../src/gribuki_trade/strategy_lab/exit_evaluator.py)
- [exit_experiment_io.py](../src/gribuki_trade/strategy_lab/exit_experiment_io.py)

它在冻结、按内容哈希的 episode 数据集上评估有限预登记参数空间：ATR 止损倍数、结构缓冲、R 倍目标、
最大持有交易日和可选 ATR trailing stop。候选数量超过显式上限时整批拒绝，不静默截断。

重放语义包括：

- A 股 T+1，入场日 bar 不可卖；
- 停牌和一字跌停无法卖出，记录 blocked sessions，等待后续可执行日；
- 同 bar 止损/止盈冲突采用 stop-first；
- trailing stop 只使用前一根已完成 bar，避免未来函数；
- 达到时间门、数据末端仍未成交时区分真实 `FILLED` 与 `MARKED_OPEN`；
- 显式佣金、最低佣金、税、过户费和保守卖出滑点；
- 输出复投净收益、最大回撤、平均收益、已成交胜率、profit factor、盈亏额和阻断交易日数。

walk-forward 先按入场交易日分组，再映射到 episode，防止同日样本跨集合泄漏；每个候选的每折结果、
拒绝原因和 validation 目标进入 trial registry。候选只按 validation 选择，锁定后才分别评价选中候选与
预登记 baseline 的 holdout。评价结果不会自动晋升 QUICK/DEEP 生产参数。

冻结实验已经有独立 CLI `strategy-exit-evaluate`。它在读取数据前要求显式
`--confirm RESEARCH_ONLY`，严格校验 `exit-policy-dataset@1` 与
`exit-policy-experiment-spec@1`，拒绝重复 JSON 键、浮点 JSON、未知字段、数据内容哈希漂移、
符号链接和覆盖已有产物。结果以 `exit-policy-experiment-artifact@1` 原子写入，包含输入文件哈希、
数据内容哈希、walk-forward 计划哈希和 trial registry 哈希，并固定
`research_only=true`、`promotion_authorized=false`、`execution_authority=false`。
它不替代 episode 数据构建：调用方仍需提供可信、完整、PIT 且覆盖完整标签窗口的数据集。

## 16. 通知与 durable outbox

OneBot 适配器见 [onebot.py](../src/gribuki_trade/adapters/notifiers/onebot.py)，outbox 见
[outbox.py](../src/gribuki_trade/storage/execution/outbox.py)。

出站语义是 at-least-once：

- enqueue 使用唯一 `idempotency_key`，同键不同 payload 冲突；
- worker 原子 claim 并增加 `attempt_count`，使用 lease 防止并发重复领取；
- 进程死亡后 lease 到期可重领；
- 临时错误按有上限退避重试，达到最大次数转 `dead`，TTL 到期转 `expired`；
- provider message ID 和发送时点持久化；
- dispatcher 可严格限定 `target_kind + target_id + channel`，不会因为共享 outbox 抢走其他目标消息；
- 错误只存稳定码，不保存可能含 token 或正文的异常文本。

at-least-once 仍意味着 provider 已收到但客户端未得到回执时可能重复。盘后交付歧义因此必须人工授权
`--recover-delivery`，不能把本地幂等键误当成 provider 端 exactly-once。

OneBot 只允许 loopback、Bearer token 和精确目标白名单。附件必须是报告根内的普通本地文件；拒绝
URL、UNC、路径穿越、符号链接和越过 artifact root 的路径。

## 17. 临时目录、运行目录与安全

### 17.1 目录约定

典型本机状态位于：

```text
runtime/
  config/                 非秘密集成设置
  tmp/                    统一临时根
  news/                   原始新闻与规范化事件
  research/               候选、研究、运行档案、market evidence
  paper/day/YYYY-MM-DD/   按交易日隔离的 PAPER journal/ledger/outbox/reports
  live/                   实盘观察 ledger、exit plans、outbox
  reports/                单标的研究 Markdown/PNG
  notifications/          通用通知 outbox
```

`runtime/`、`secrets/`、`vendor/`、数据库和本机数据文件由 `.gitignore` 排除。Git 忽略不等于自动清理；
任何清理程序仍必须验证归属和路径。

### 17.2 统一 temp root

[temp_root.py](../src/gribuki_trade/runtime/temp_root.py) 的解析优先级为：

```text
CLI --temp-dir > GRIBUKI_TRADE_TMP_DIR > runtime/tmp
```

解析器拒绝文件系统根和仓库根，不修改全局 `tempfile` 设置。pytest 的根
[conftest.py](../conftest.py) 会把测试临时文件放入进程隔离子目录。需要与目标文件原子替换的临时文件
仍应放在目标旁，而不是跨文件系统移动。

根目录历史 `.tmp*` 只能先由 [archive_root_temps.py](../scripts/archive_root_temps.py) 盘点、归档并生成
清单，再由 [cleanup_root_temps.ps1](../scripts/cleanup_root_temps.ps1) 在目标集合、归档证据和 reparse
point 校验通过后处理。不得用宽泛递归删除代替该流程。

### 17.3 凭据和明文文件

- 系统推荐通过 keyring 保存 DeepSeek/OpenAI key 和 OneBot token；
- 日志、异常、GUI 文本、状态文件和 `repr` 不得输出凭据；
- 用户自行放在 `secrets/` 或其他位置的明文 API 文件不得被 GUI、temp 清理、凭据迁移或安全扫描
  自动删除、移动、清空或覆盖；
- 本文和测试不得读取这些明文文件来“验证”凭据；
- 明文文件存在并不扩大研究、通知或 broker 权限，权限仍由具体 provider/运行入口控制。

## 18. 主要 CLI 工作流

以下示例只使用占位账户和目标，不包含凭据。

### 18.1 系统、临时目录和 GUI

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade sqlite-runtime-status
.\.venv\Scripts\python.exe -m gribuki_trade temp-root status
.\.venv\Scripts\python.exe -m gribuki_trade temp-root prepare --temp-dir runtime/tmp
.\.venv\Scripts\python.exe -m gribuki_trade gui
```

### 18.2 新闻、筛选、候选和研究

```powershell
# 新闻与来源状态的完整参数以 --help 为准
.\.venv\Scripts\python.exe -m gribuki_trade ashare-news --help
.\.venv\Scripts\python.exe -m gribuki_trade ashare-source-health --help

.\.venv\Scripts\python.exe -m gribuki_trade ashare-market-screen-once `
  --top-n 30 --factor-budget 300 `
  --candidate-db runtime/research/candidates.sqlite3 `
  --run-db runtime/research/runs.sqlite3

.\.venv\Scripts\python.exe -m gribuki_trade ashare-intraday-scan-once `
  --candidate-db runtime/research/candidates.sqlite3 `
  --run-db runtime/research/runs.sqlite3

.\.venv\Scripts\python.exe -m gribuki_trade ashare-candidates list
.\.venv\Scripts\python.exe -m gribuki_trade ashare-research-watch `
  --candidate-db runtime/research/candidates.sqlite3 --candidates-only --cycles 1

.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-once `
  --symbol 510300.SH --report-dir runtime/reports

.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-batch `
  --candidate-db runtime/research/candidates.sqlite3 --limit 10
```

实际新闻健康命令名称和 feed 选项应以 `python -m gribuki_trade --help` 及子命令 `--help` 为准；
不要根据旧设计文档猜测参数。

### 18.3 PAPER 账本与 PAPER-day

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper open `
  --account personal-paper --initial-cash 100000 --session-date 2026-08-14

.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper snapshot `
  --account personal-paper

$tradeDate = (Get-Date).ToString('yyyy-MM-dd')
.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper-day run `
  --session-date $tradeDate --initial-cash 200000 `
  --target-kind private --target-id "YOUR_QQ_ID" `
  --intraday-llm --intraday-llm-review-top-n 6 `
  --intraday-llm-review-ttl-minutes 20 `
  --intraday-llm-max-calls unlimited `
  --intraday-llm-events-db runtime/news/events.sqlite3 `
  --confirm PAPER_DAY

.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper-day status --session-date $tradeDate
.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper-day report --session-date $tradeDate
.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper-day summary --session-date $tradeDate
```

盘中 LLM 默认 required；使用 `--no-intraday-llm` 会形成明确 operator opt-out 审计，并把 runner
限制为持仓监控/卖出复核模式，所有新买入失败关闭，不会退回技术规则单独运行。事件库应在盘前
预填、关闭 writer 并 checkpoint；runner 以只读冻结快照启动，盘中不继续抓新闻。

### 18.4 盘后复盘

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-post-close run `
  --runtime-dir runtime/paper/day --session-date $tradeDate `
  --account ashare-paper-day `
  --target-kind private --target-id "YOUR_QQ_ID" `
  --macro --macro-provider deepseek `
  --dispatch-cycles 3 --confirm POST_CLOSE

.\.venv\Scripts\python.exe -m gribuki_trade ashare-post-close status --session-date $tradeDate
.\.venv\Scripts\python.exe -m gribuki_trade ashare-post-close report --session-date $tradeDate
```

### 18.5 实盘成交事实同步与单轮保护

```powershell
# 第一次 ingest 只生成 proposal；event.json 是 OneBot v11 私聊事件
.\.venv\Scripts\python.exe -m gribuki_trade live-sync ingest `
  --ledger-db runtime/live/observed-live.sqlite3 `
  --exit-plan-db runtime/live/exit-plans.sqlite3 `
  --outbox-path runtime/live/outbox.sqlite3 `
  --allowed-sender "YOUR_QQ_ID" --event-json .\runtime\live\event.json

# 保存第二条确认事件后再次 ingest；提交成交后自动做一次定向 QUICK/跟踪
.\.venv\Scripts\python.exe -m gribuki_trade live-sync ingest `
  --ledger-db runtime/live/observed-live.sqlite3 `
  --exit-plan-db runtime/live/exit-plans.sqlite3 `
  --outbox-path runtime/live/outbox.sqlite3 `
  --allowed-sender "YOUR_QQ_ID" --event-json .\runtime\live\confirm.json `
  --quick-timeout-seconds 45

.\.venv\Scripts\python.exe -m gribuki_trade live-sync status `
  --ledger-db runtime/live/observed-live.sqlite3 --account observed-account

.\.venv\Scripts\python.exe -m gribuki_trade live-sync cycle `
  --ledger-db runtime/live/observed-live.sqlite3 `
  --exit-plan-db runtime/live/exit-plans.sqlite3 `
  --outbox-path runtime/live/outbox.sqlite3 `
  --target-kind private --target-id "YOUR_QQ_ID" `
  --confirm LIVE_SYNC_CYCLE
```

### 18.6 NapCat 与报告交付

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade napcat-status
.\.venv\Scripts\python.exe -m gribuki_trade napcat-send-test `
  --target-kind private --target-id "YOUR_QQ_ID" --confirm SEND_TEST
.\.venv\Scripts\python.exe -m gribuki_trade napcat-dispatch `
  --target-kind private --target-id "YOUR_QQ_ID" --cycles 1
```

### 18.7 策略实验

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade strategy-factor-discover `
  --max-trials 100 --output runtime/strategy/factor-inventory.json

.\.venv\Scripts\python.exe -m gribuki_trade strategy-exit-evaluate `
  --dataset runtime/strategy/exit-policy-dataset.json `
  --specification runtime/strategy/exit-policy-experiment.json `
  --output runtime/strategy/exit-policy-result.json `
  --confirm RESEARCH_ONLY
```

退出策略实验入口只读取冻结 episode 数据并生成研究产物；它没有策略发布、PAPER 参数改写或实盘
执行权限。数据与配置格式、PIT 边界和失败关闭条件见 [STRATEGY_LAB.md](STRATEGY_LAB.md)。

## 19. 运行与故障恢复手册

| 现象 | 应检查的权威状态 | 合理动作 | 禁止动作 |
|---|---|---|---|
| `SQLITE_WAL_RESET_RUNTIME_UNSAFE` | `sqlite-runtime-status` 输出与 Python SQLite 版本 | 升级到认可版本；在此前禁止多进程同库 | 用业务 lease 掩盖 runtime 缺陷 |
| PAPER-day 启动失败 | 当日 `status.json`、manifest、journal phase | 修正日历、目标、配置或凭据；保持原数据库 | 删除当日目录重来 |
| `DAY_ABORTED` | journal 最后事件、账本/exit-plan 哈希链 | 人工核实后相同配置加 `--recover-after-abort` | 无审计地重开新 run 覆盖旧账 |
| 已有 run 配置不一致 | manifest/config SHA | 恢复原配置，或在新交易日创建新 run | 改写 manifest |
| PAPER 买前 QUICK 创建失败 | 完整 bar、失效位、最坏买价、日价格带 | 放弃该 PAPER 买入；修复数据源后等待新信号 | 先 PAPER 成交、事后补造 QUICK |
| live-sync 成交后 QUICK 创建失败 | 已确认成交、保护 work、行情/日历错误码 | 保留成交事实与 durable work，人工核对券商侧风险；修复后由 `cycle` 重试 | 回滚外部成交、删除 work 或谎报已有保护 |
| DEEP 失败 | exit-plan stream 与双轨审计 | 保留 QUICK，等待后续合法复核 | 删除 QUICK 或放松 stop |
| 盘中 LLM 无缓存 | LLM journal、PIT 快照、TTL | 当前买入失败关闭；修复后等下一次候选复核 | 在买入临界路径绕过 required 门 |
| 新闻来源熔断 | source cursor、`next_allowed_at`、probe lease | 等待退避或修复单源；其他来源继续 | 全局重试风暴或清空 cursor |
| NapCat 不在线 | GUI 登录态、OneBot health、outbox 状态 | GUI 显式启动/登录后重派发 | 隐式启动脚本或系统任务 |
| outbox `retry/dead/expired` | attempt、lease、TTL、稳定错误码 | 修复目标/token/provider；按语义重试 | 手改 `sent` 或删除整个库 |
| 盘后 `ANALYSIS_AMBIGUOUS` | post-close state/audit | 同配置人工授权 `--recover-analysis` | 新 run 混入不同配置 |
| 盘后 `DELIVERY_AMBIGUOUS` | provider 回执、outbox、目标 | 人工核对重复风险后决定 `--recover-delivery` | 自动无限重发 |
| live proposal 未改变持仓 | command state 与 fingerprint | 用同一白名单 sender 发送正确确认 | 把 proposal 当成交 |
| live barrier 提醒后无卖单 | live protection stream | 用户在券商执行后同步 SELL 事实 | 期待 `cycle` 自动卖出 |

恢复的共同原则是：先读 manifest、sidecar 和不可变事件，验证哈希与身份，再使用现有幂等入口；
不要通过删库、改 JSON、覆盖旧事件或换配置“修好”状态。

## 20. 测试与质量门槛

质量工作流见 [quality.yml](../.github/workflows/quality.yml)。交接前应在同一工作树运行：

```powershell
.\.venv\Scripts\python.exe -m ruff check conftest.py src tests
.\.venv\Scripts\python.exe -m mypy src
.\.venv\Scripts\python.exe -m pytest --temp-dir runtime/tmp -q
git diff --check
```

2026-08-15 对冻结工作树共收集 1,497 项测试，实际验收结果为：
`1492 passed, 5 skipped`；Ruff、`compileall`、200 个源码文件的 Mypy、全仓中文说明性注释守卫和
`git diff --check` 均通过。五项 skipped 均因当前 Windows 测试账户没有创建符号链接的权限，
对应 NapCat 配置目标、OneBot 附件、PAPER 两类 lineage 路径和退出实验输入的 symlink
失败关闭用例；不是业务测试失败，生产路径仍保留显式拒绝逻辑。README 与 `docs/**/*.md` 共核对
21 个 Markdown 文件、257 条本地链接引用（125 个唯一目标），缺失为零。

测试重点包括：

- provider schema、deadline、失败关闭和双轨 LLM 审计；
- PIT 事件 revision、未来数据拒绝和冻结证据；
- 收盘筛选、盘中异常、候选状态和研究门禁；
- QUICK/DEEP 单调约束、同 bar 冲突、T+1 和崩溃恢复；
- PAPER 费用、撮合、跨日 lineage、风险策略迁移和 sidecar 只读；
- live 两阶段确认、账本完整性、并发 proposal/confirmation、保护工作和提醒；
- outbox 并发 claim、target scope、lease、退避、TTL 和幂等冲突；
- 官方新闻 fixture、URL 白名单、跨进程 probe lease 和单源失败隔离；
- 六类报告契约、golden fixture、Markdown/PNG artifact 安全；
- GUI 后台 worker、provider/NapCat 配置和进程所有权；
- strategy_lab DSL、walk-forward、holdout、A 股日线与退出 evaluator。

离线测试不替代：真实交易日全天 soak、真实 DeepSeek/OpenAI 调用、真实 NapCat 私聊/群文件上传、
公开网页 provider 长期稳定性、休眠唤醒和应用常驻生命周期。最终交付说明应把本次实际命令结果另行
记录，不能只引用测试文件数量。

## 21. 其他市场模块

项目仍保留 Binance 与 Schwab 适配层：

- Binance Spot Testnet 已有状态、order test、普通模拟 cycle、durable OMS cycle/fill、公开历史、
  回测和本地 SHADOW。会改变 Testnet 远端状态的命令要求显式确认，不能使用 Live key；
- Schwab 具备 OAuth、Market Data、Trader REST 和简单限价适配器及离线 transport 测试，但没有
  用户侧 CLI、真实 Developer App 联调或生产执行编排。

它们不属于本轮 A 股 PAPER/live-sync 闭环，也不能被当作 A 股真实执行后门。详见
[BINANCE_SIMULATION_STATUS.md](BINANCE_SIMULATION_STATUS.md) 和
[BINANCE_SCHWAB_INTEGRATION.md](BINANCE_SCHWAB_INTEGRATION.md)。

## 22. 已知边界与下一位维护者必须保留的诚实表述

### 22.1 执行与行情边界

- A 股没有真实 broker adapter 的用户侧执行工作流；
- PAPER minute bar 不知道逐笔顺序、盘口、排队、价格笼子基准或真实滑点；
- 涨停封死只能保守视为无法证明成交，不能模拟排队成功；
- PAPER-day 目前只记录卖出信号，不提交 PAPER 卖单；
- live-sync 只提醒，不自动卖出；
- 北交所 PAPER 执行失败关闭，`.BJ` 分钟链不在当前承诺内；
- 公司行动、融资融券、多币种、真实冲击与组合容量未实现。

### 22.2 编排边界

- 没有常驻应用 runtime、OneBot 入站监听、自动开盘/收盘衔接或统一进程监督；
- `ashare-research-watch`、`live-sync cycle` 和 outbox dispatcher 都是有限轮；
- `ashare-paper-day run` 是阻塞单进程日运行，不是系统服务；
- `ashare-post-close run` 是幂等单次状态机，不自行按日触发；
- 仓库不提供或安装 Windows Task Scheduler、独立 PowerShell watchdog 或隐式 NapCat 启动器。

### 22.3 数据与统计边界

- 公开网页 provider 无交易级 SLA；字段、覆盖和页面结构可能变化；
- 新闻跨源印证降低单源误差，但不保证事实绝对正确；
- 技术分、宏观分、双轨 LLM 分均未概率校准；
- QUICK/DEEP 默认 ATR、R 倍和期限仍标记 `UNCALIBRATED`；
- strategy_lab 退出 evaluator 已能做真实 walk-forward，但没有现成大规模 PIT episode 数据集，
  也没有证明任何候选样本外优于 baseline；
- 当前 A 股评价器以单证券 long/cash 为主，不是历史全市场横截面组合回测器；
- holdout 不能被反复用于选择同一批候选。

### 22.4 集成与安全边界

- GUI 集成页不授予 provider“全部权限”，只保存本机配置和凭据；外部账户权限由 provider 决定；
- keyring 不是应用权限系统，明文 API 文件存在也不是授权依据；
- outbox 是 at-least-once，不是 provider exactly-once；
- GUI 只停止自己创建的 NapCat 进程；外部实例生命周期由用户负责；
- 真实 QQ 登录、模型网络、长报告文件上传仍需本机人工验收。

## 23. 交接检查清单

新维护者开始运行前应依次确认：

1. Python、SQLite 版本和依赖安装符合 [pyproject.toml](../pyproject.toml)；
2. `sqlite-runtime-status` 通过；
3. `runtime/tmp` 或显式 temp root 已准备，仓库根没有新增无归属 `.tmp*`；
4. 不读取、不移动、不删除 `secrets/` 下用户明文文件；
5. GUI 共享配置中的 provider/model/loopback URL 正确，秘密只从 keyring 读取；
6. NapCat 由 GUI 显式启动并确认 QQ 登录，目标 ID 与 run manifest 一致；
7. 新闻事件库在 PAPER-day 前完成写入和 checkpoint，没有活跃 writer；
8. PAPER 运行使用当天真实交易日和显式 `PAPER_DAY` 确认；
9. 不用 `--no-intraday-llm` 隐式掩盖 provider 故障；如主动关闭，接受审计中的 opt-out 与“禁止新买”边界；
10. 不为追求成交率绕过价格带、QUICK、风险预算、T+1 或数据完整性门禁；
11. 盘后恢复只在同 manifest 下使用明确恢复开关；
12. live-sync 的 proposal 与确认来自同一白名单 sender，且只描述券商已成交事实；
13. 运行 Ruff、Mypy、pytest 和 `git diff --check`，把实际结果写入最终交付记录；
14. 任何未来常驻能力接入统一应用 runtime，不恢复 OS 任务或第二套业务逻辑；
15. 任何生产参数晋升都先经过冻结数据、walk-forward、holdout、shadow 和人工发布。

## 24. 权威代码与配套文档索引

- 总览与常用命令：[README.md](../README.md)
- 历史需求实施评估：[DEVELOPMENT_PLAN_IMPLEMENTATION_260814.md](archive/DEVELOPMENT_PLAN_IMPLEMENTATION_260814.md)
- 交易策略体系说明：[TRADING_STRATEGY_GUIDE_260814.md](TRADING_STRATEGY_GUIDE_260814.md)
- 历史本轮更新详解：[ROUND_CHANGELOG_260814.md](archive/ROUND_CHANGELOG_260814.md)
- A 股 PAPER：[A_SHARE_PAPER_TRADING.md](A_SHARE_PAPER_TRADING.md)
- 盘后状态机：[A_SHARE_POST_CLOSE_AUTOMATION.md](A_SHARE_POST_CLOSE_AUTOMATION.md)
- A 股数据源矩阵：[A_SHARE_DATA_SOURCES.md](A_SHARE_DATA_SOURCES.md)
- 筛选与发现：[A_SHARE_SCREENING_AND_DISCOVERY.md](A_SHARE_SCREENING_AND_DISCOVERY.md)
- 策略实验室：[STRATEGY_LAB.md](STRATEGY_LAB.md)
- 本机研究与 NapCat：[LOCAL_RESEARCH_SETUP.md](LOCAL_RESEARCH_SETUP.md)
- CLI 装配：[cli.py](../src/gribuki_trade/cli.py)
- 六类报告契约：[contracts.py](../src/gribuki_trade/reporting/contracts.py)
- 双轨生产装配：[llm_production.py](../src/gribuki_trade/services/llm/llm_production.py)
- PAPER-day 编排：[ashare_paper_day.py](../src/gribuki_trade/services/ashare/paper_day/ashare_paper_day.py)
- 实盘观察编排：[live_trade_orchestration.py](../src/gribuki_trade/services/live/live_trade_orchestration.py)
- 退出计划 evaluator：[exit_evaluator.py](../src/gribuki_trade/strategy_lab/exit_evaluator.py)

当历史计划、旧文档和当前代码状态发生冲突时，应以领域不变量、CLI 当前 parser、持久化 schema
和测试为准，本交接文档仅作维护导航；仍需通过实际运行才能确认的事项，必须继续写成“待本机验收”或
“待真实交易日 soak”，不能为了完成状态表而改写为已完成。
