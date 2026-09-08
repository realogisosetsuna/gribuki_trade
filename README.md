# Gribuki Trade

Gribuki Trade 是一个面向个人研究的 Python 交易工作台，当前以 **A 股全市场发现、证据化深度研究、PAPER 仿真和受控通知** 为主线，同时维护 Binance Spot/Futures 的隔离适配层与 Charles Schwab API 适配层。

项目版本为 `0.1.0`，采用仓库根目录 [MIT License](LICENSE)。它不是已经投入生产的自动交易机器人，也不提供投资收益承诺：研究结论、LLM 评分、人工复核和 PAPER 成交都不等于真实委托授权。

## 当前结论

截至 2026-08-15，项目已经形成以下可运行主线：

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

调用方准备并冻结的 PIT 历史样本
              ↓（人工冻结、离线运行）
 walk-forward 策略实验与受控因子探索
```

最重要的边界是：**研究、PAPER 和实盘同步链都不会调用真实券商。**
`live-sync` 只接收并审计用户已经在券商完成的成交，再有限尝试构建保护计划、执行一次
行情跟踪并发送提醒；它没有下单权限。

| 能力 | 当前成熟度 | 准确边界 |
|---|---|---|
| A 股收盘全市场筛选 | 核心已实现 | 当前交易日盘后股票筛选；不是 ETF 全市场筛选，也不是买入清单 |
| A 股盘中异常发现 | 核心已实现 | 公开网页快照的线索发现；不是交易所 tick、L1/L2 或可执行报价 |
| 候选库、跟踪、深研、复核 | 核心已实现 / 编排部分实现 | 支持有限轮和有界批处理；尚无生产级常驻调度器 |
| 技术面 + 宏观/新闻融合 | 核心已实现 | 分数未经概率校准；LLM 不能单独把技术 `WATCH` 升级为入场 |
| A 股 PAPER | 跨日账本、单日盘中编排、QUICK/DEEP 保护计划与崩溃恢复已接入 CLI | 盘中撮合只用已完成分钟 bar，不是 tick、L1/L2 或盘口队列仿真；会话可能标记为 `PARTIAL_SESSION` |
| 保护性退出计划 | PAPER 已接买前 QUICK；`live-sync` 在收到券商已成交事实后立即有限尝试补建 QUICK，失败工作由后续 `cycle` 恢复；两者均接多时间框架 DEEP、哈希链和跨日观察 | 止盈是预注册 barrier，不是收益预测；只记录/提醒卖出时机，不创建真实或 PAPER 卖单；系统无权在券商成交前替实盘建计划 |
| 实盘成交观察账本 | `live-sync ingest/status/cycle` 已实现两阶段确认、原子账本、持久工作项、保护分析和单轮行情跟踪 | 只记录用户已在券商完成的成交；OneBot 常驻入站和循环生命周期由后续应用运行框架调用这些入口 |
| 策略实验与因子发现 | 成本感知 evaluator、purge/embargo walk-forward、holdout 与不可自动晋升 trial registry 已实现 | 仍不是全市场组合回测器，研究结果不会自动发布到线上参数 |
| PySide6 GUI | NapCat/LLM 集成管理已驱动生产默认值；交易页仍为演示 | 可显式启动/登录/监看 NapCat，保存 OneBot token，配置 DeepSeek/OpenAI keyring、provider 与模型；不连接券商 |
| Binance Spot/Futures LIVE | Spot LIVE durable OMS、USDⓈ-M Futures LIVE 受守卫执行入口已实现 | 必须显式 LIVE 确认、账户/IP/交易权限和真实网络验证；Futures 尚未接入 Spot SQLite durable OMS |
| Schwab | 适配代码与离线测试已实现 | 尚未用真实 Developer App 联调，无用户侧 CLI、streaming 和 durable 执行编排 |

“核心已实现”表示领域逻辑、持久化或 CLI 已存在并有测试，不表示已经完成连续运行、实盘或统计有效性验收。

## 六类研究功能是如何实现的

### 1. 收盘三层筛选与盘前降级种子

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

`ashare-paper-day` 另有一个只在 09:25 前使用的盘前适配层。它把当前网页快照保守地绑定到已经由交易日历核验的上一交易日，结果始终标记为 `DEGRADED`，排除北交所，并且只作为当日 research seed；开盘后仍必须由当前交易时段的 surveillance 与技术门禁重新确认。这个路径不能冒充历史收盘重放，也没有独立的“盘前自动选股”执行入口。

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

`ashare-research-watch` 是顺序、有界、多标的分钟研究轮询，每个标的故障隔离；它不是无限 daemon，也不是收盘深研。长期生命周期将由后续统一应用运行框架负责，仓库不再提供 OS 任务或独立看门脚本。候选库支持 `.BJ`，但现有分钟跟踪规范化仍只承诺 `.SH/.SZ`；北交所目前只承诺收盘日线深研，不承诺分钟链。

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
| 官方宏观/政策 | NBS、PBOC、CSRC、财政部、国家发改委、外汇局、上交所、Federal Reserve RSS | 事实主线；逐链接白名单、跨进程探测租约和持久退避使单源失败与其他源隔离 |
| 人民币与利率 | SAFE 中间价、官方 Shibor、ChinaMoney FR/FDR、中债收益率曲线 | 保留真实口径；FR/FDR 不冒充 R/DR |
| 全球风险与跨市场 | Cboe VIX EOD、A/港/美/亚太指数快照和历史关系 | 按各市场收盘可见性对齐；相关不写成因果 |
| A 股上下文 | 沪深京市场宽度、CFFEX IF、ETF 价/IOPV/份额、上交所期权风险参数 | 缺同日现货或不适用时不造基差、IV 或资金流 |
| 搜索发现 | 可选 Tavily、用户自管 SearXNG | 搜索供应商只是发现路由；同一 URL 或同一注册发布域即使被多个 provider 返回也仅为 `HINT`，只有官方域或标题匹配的不同独立发布域才能晋级 |

[原始文档与标准事件](src/gribuki_trade/domain/events.py) 分别记录 `published_at`、`first_seen_at`、`available_at`、内容哈希、解析版本和修订链。每个来源独立超时和降级；未来证据、过期证据、重复故事、低相关内容和提示注入不会进入模型。

[宏观研究服务](src/gribuki_trade/services/macro_research.py) 默认从最近 14 日选择最多 24 条、每源最多 6 条证据。官方与许可来源优先；单一公共媒体线索只有获得独立来源印证后才能被对抗轨作为事实引用。默认 [DeepSeek 适配器](src/gribuki_trade/adapters/llm/deepseek_chat.py) 使用 `deepseek-v4-flash`，也可由 GUI 选择 OpenAI。所有生产语义分析都在同一冻结 EvidencePack 上并行运行 baseline 与结构化对抗轨；生产选择优先采用对抗结果，任一必需角色失败、证据引用错误或跨轨实质冲突会失败关闭或降级。两轨结论、模型、证据覆盖和审计哈希同时进入报告。

#### 最终融合

[推荐门禁](src/gribuki_trade/policy/recommendation_gate.py) 默认技术 75%、宏观 25%，宏观权重上限为 40%。宏观结论为 `WATCH` 时其有效权重再减半；证据覆盖低于 10%、引用错配或模型弃权时不融合。强负面宏观分不高于 `-0.60` 时可以否决入场，但宏观永远不能把技术上未通过的 `WATCH` 升级为 `ENTER_CANDIDATE`。

报告将原始哈希转换为人类可读的证据编号，数值统一按可读精度展示；可原子导出 UTF-8 Markdown 和分页 PNG。六类报告都先通过固定契约校验；盘后交付只发送一条可读短摘要，再上传完整 Markdown，避免把长报告拆成失去上下文的 QQ 残片。

人工复核有 `PENDING_REVIEW/CONFIRMED/REJECTED/EXPIRED/CANCELLED` 五态及独立 append-only 审计。CLI 的 `confirm` 还必须显式输入 `RESEARCH_ONLY`；确认仅代表研究结论已复核，绝不产生委托。

### 5. A 股 PAPER、账本与保守撮合

A 股 PAPER 刻意拆成四层，避免把记账、两种撮合时间尺度和崩溃恢复混为一谈：

1. [持久成交账本](src/gribuki_trade/services/ashare_paper.py) 有 CLI：保存现金、持仓、均价、已实现盈亏、`today_buy/available_to_sell` T+1、佣金/最低佣金/过户费/卖出印花税。人工与模拟 fill 共用契约；`fill_id` 幂等，事件带逐账户 SHA-256 链并由同一投影重放。
2. [保守日线撮合器](src/gribuki_trade/services/ashare_paper_matching.py) 是 Python API：支持 `PENDING/PARTIALLY_FILLED/FILLED/CANCELLED/REJECTED/EXPIRED`、NEXT_TRADING_BAR/GTD、限价触及、100 股买入整手、卖出尾仓、FIFO、默认 1% bar 成交量参与、跨 bar 部分成交和不穿限价的保守滑点。调用方必须提供完整未复权 bar 和明确价格区间；停牌、缺 OHLC、零量、缺价格带或越界全部不成交。
3. [durable 恢复层](src/gribuki_trade/services/ashare_paper_recovery.py) 与 [订单事件库](src/gribuki_trade/storage/paper_orders.py) 保存完整 `RUN_STARTED` 输入、状态、fill 和完成事件，并使用 writer lease。资金账本和订单库是两个 SQLite 数据库，因此采用确定性 `fill_id` + 恢复 saga，而不宣称跨库 ACID；调用方必须先 `recover()`。
4. [PAPER-day 编排器](src/gribuki_trade/services/ashare_paper_day.py) 有 `ashare-paper-day run/status/report/summary` CLI。`run` 在一个进程内完成交易日历与通知预检、盘前全市场筛选、每 15 分钟全市场维护、1 分钟主监控与 5 分钟辅监控、风险评估、下一完整分钟 IOC、T+1 账本、通知 outbox 和收盘报告；journal、账本、outbox、`status.json` 与 Markdown 报告均按交易日隔离。`status/report/summary` 读取 sidecar，不打开正在运行的数据库。

PAPER-day 默认初始权益 20 万元；默认**没有固定持仓数量上限**，但每次买入仍受最低 20% 现金储备、最高 80% 组合 gross、最高 20% 单标的敞口和 0.75% 单笔止损风险预算约束。`--maximum-positions` 只是在需要时显式打开计数熔断器，不能绕过资金、费用、板块申报数量、流动性和待撮合资金占用。

每个买单在提交前必须先用当时已经完成的分钟线构建并持久化 QUICK 退出计划；
默认从合法技术失效位、结构支撑和 `2×robust ATR` 候选中取更宽松但仍低于
最低合法买价的止损，随后按“最坏许可买入限价－止损”重新计算风险和数量。
若信号后的撮合线已经触发 QUICK 止损、目标或时间门，买单直接失败关闭。成交后，
系统把 fill 绑定到保护流，使用 1/5/15 分钟已完成线并行取得 baseline/对抗语义评分，
再由确定性 DEEP 生成器更新止损、预注册 R 倍目标和期限；DEEP 只能维持或收紧
多头风险，不能下移止损或延长期限。模型失败时保留 QUICK，绝不留下无保护持仓。
保护账本跨重启、跨交易日恢复，历史上没有 PIT 计划的旧持仓不会被事后补造。

每个交易日使用独立 ledger/journal 目录，但退出保护流共享父级
`runtime/paper/day/exit-plans.sqlite3`。新日准备通过跨进程锁、ledger 内嵌 binding、
sidecar lineage 和历史 seal 核验公共事件前缀；只有唯一合法前缀可恢复孤儿 clone，
分叉、来源日续写或绑定冲突都会失败关闭。

买入可接受价下界是严格高于技术失效位的第一个合法价格刻度，上界是信号价加默认 0.1% 后、再受当日涨停价约束的限价；撮合分钟一旦触及或跌破失效位，整笔 IOC 不成交，不把“破位低价”解释成便宜买入。卖出记录保存“参考价减默认 0.1%、且不低于跌停价”到涨停价的未来可接受区间；`REDUCE` 会读取已持久化 DEEP 计划中的 baseline/对抗评分并连同价格走廊通知，但本日 runner **只写 journal，不提交卖单**。买入只检查信号之后第一个完整且已完成的 1 分钟区间，未成交或部分成交的剩余量当场取消，不跨分钟或午休延续；涨停封死视为排队不可证明而不成交，未来跌停卖出也必须按同一保守原则处理。

申报数量按板块冻结：沪深主板至少 100 股、按 100 股递增、限价单最多 100 万股；创业板至少 100 股、按 100 股递增、最多 30 万股；科创板至少 200 股、其后按 1 股递增、最多 10 万股；不足常规卖出门槛的余股必须作为届时全部余额一次卖出。北交所 PAPER 执行当前失败关闭。以上是 PAPER 规则契约，不是券商报盘合规证明。

PAPER-day 只消费公开源快照和分钟 OHLCV，不能观察逐笔成交顺序、盘口深度、委托排队或连续竞价价格笼子基准，因此不是 tick、L1/L2 或真实滑点仿真。晚启动、恢复或数据覆盖不完整的当天即使生命周期 `COMPLETED`，报告仍可诚实标记为 `PARTIAL_SESSION`。日线 durable matcher 及其恢复层目前仍没有用户侧撮合 CLI/GUI；这不再等同于“整个 PAPER 撮合没有 CLI”。公司行动、融资、多币种和真实冲击也仍未实现。

#### 盘中 LLM 门禁：离线工程验收完成，实盘时段 soak 待做

PAPER-day 当前 CLI 默认启用 required 模式的双轨盘中复核：每次全市场刷新在后台让同一 provider/model 分别运行原单分析器和结构化对抗角色，结果必须先写入 journal，买入门禁才可以引用。生产评分优先采用对抗轨；baseline 始终保留供报告对照。买入门禁只读本地已落盘缓存，不等待 provider，也不执行 I/O；复核尚未完成、审计未落盘或对抗轨失败时直接失败关闭该次买入。LLM 只能确认、降级或否决技术规则已经产生的 `ENTER_CANDIDATE`，不能从 `WATCH` 创造买点。`REDUCE` 不在分钟临界路径重新联网，而是读取成交后 DEEP 已保存的两轨评分，因此保护信号不会被模型延迟阻塞，也不再是“完全没有 LLM 分析”。

精确开关为 `--intraday-llm/--no-intraday-llm`，默认启用；显式关闭会把 operator opt-out 写入运行审计，并把当次 runner 限制为持仓监控/卖出复核模式，所有新买入以 `LLM_OPERATOR_DISABLED_NEW_BUY_BLOCKED` 失败关闭，绝不会退回技术规则单独买入。可调参数为 `--intraday-llm-provider`（DeepSeek/OpenAI）、`--intraday-llm-model`、`--intraday-llm-review-top-n`（默认 6）、`--intraday-llm-review-ttl-minutes`（默认 20）、`--intraday-llm-max-calls`（正整数或 `unlimited`，默认 `unlimited`）和 `--intraday-llm-events-db`（默认 `runtime/news/events.sqlite3`）。无限模式仍在重启时从 journal 恢复并累计已发起调用数，但不会因该计数停止新的后台复核；run manifest 用 JSON `null`、策略配置审计事件用 `UNLIMITED` 固定无限语义。启用时必须已有所选 provider 对应的 key；runner 在启动阶段以 `mode=ro&immutable=1` 和 `query_only` 只读加载 `first_seen_at/available_at` 均不晚于启动截点的 PIT 事件，随后冻结为内存快照，盘中刷新不再查询该库，也不会自行抓新闻。事件库不存在、无有效事件、格式无效或存在未 checkpoint 的非空 `-wal` 时，快照不可用，required 买入失败关闭。因此应由独立新闻任务在盘前预填并关闭 writer、完成 checkpoint 后再启动 runner；不得让新闻 writer 与快照读取并发访问同一路径。该接线已经通过全仓离线回归、故障与重启预算测试；尚未经历下一个真实交易日的全天 LLM soak，也未在当前环境中完成所选生产 provider 的网络冒烟，因此不能据此宣称生产可靠性。

生产双轨提供 `FAST/STANDARD/DEEP` 三档：盘中 FAST 单角色调用上限 15 秒、整个 case 24 秒，外层协调器 28 秒；普通研究和盘后深研使用更宽的固定上限。会话调用预算默认 unlimited，但每案角色、轮数和截止时间永远有界。每个角色轮次、证据/提示哈希、模型身份、token 用量、终止原因和最终选择都进入独立 SQLite 哈希链；对抗结论仍不能越过技术、价格、资金、数量、T+1 或执行门禁。

#### 本地验收实例：2026-08-14

本地 `runtime` 中保留了一次真实公开数据驱动的 PAPER 会话：生命周期 `COMPLETED`、覆盖口径 `PARTIAL_SESSION`，共写入 6,308 个 sidecar 事件；盘前名单 30 个标的，执行 15 次全市场盘中扫描，并对 93 个标的完成 5,536 次技术评估。当天出现 141 个技术买入候选，6 笔通过风控，5 笔成交、1 笔因未触及限价取消；203 个 `REDUCE` 只记录、不下单。20 万元初始现金最终投影为 40,225.47 元现金、168,551.00 元持仓市值和 208,776.47 元估算权益；这些只是当日 PAPER 标记结果，不代表可复现收益或真实成交质量。

当天交易信号完全由当时已落盘的技术规则驱动，没有盘中 LLM 参与。后续接入 LLM 不会追溯改写既有 journal、订单、成交或持仓；当日中途确认的风险策略变更也不反推旧订单的价格区间。本地验收摘要位于 `runtime/paper/day/2026-08-14/reports/ashare-paper-day-summary-2026-08-14-10e8f5980c.md`；`runtime/` 已被 Git 忽略，这一证据不会随仓库分发。

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

[退出策略评价器](src/gribuki_trade/strategy_lab/exit_evaluator.py) 进一步对冻结的已成交样本逐笔重放止损、追踪止损、R 倍目标和时间退出，计入 A 股 T+1、停牌、一字跌停、滑点、佣金、过户费和印花税；同一日线同时触发止损/止盈时固定按止损优先。它按入场交易日整组切分，使用 purge 与 embargo 防止标签窗口泄漏，只能用验证折选择参数，锁定后才读取最终 holdout。所有合法或非法候选都进入不可变 trial registry，且固定 `research_only=True`、`promotion_authorized=False`，因此任何看起来优秀的结果都不能自动改写 PAPER 或实盘配置。

[冻结实验入口](src/gribuki_trade/strategy_lab/exit_experiment_io.py) 进一步把数据集 JSON、预注册规格 JSON、文件 SHA-256、内容 SHA-256、walk-forward 计划和完整 trial registry 串成一个可复跑流程。输入拒绝未知/重复字段、浮点 JSON、符号链接和数据集哈希漂移；输出在同目录原子替换，并再次声明 `execution_authority=false`、`promotion_authorized=false`。这解决了“只有 evaluator、没有可运行实验入口”的问题，但不会把一次历史结果冒充已校准的生产参数。

完整的当前工程边界与后续验收路线见 [项目交接文档](docs/PROJECT_HANDOFF_260814.md)。历史计划与本轮实施记录已集中放入 [文档归档](docs/archive/README.md)。

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

策略、LLM、GUI 和复核服务都不能直接导入券商 SDK。`PAPER` 禁止访问真实 broker；`SHADOW` 只允许连接、查询和订阅；`LIVE` 守卫需要当前进程内精确确认短语、账户白名单和交易所白名单。Binance Spot/Futures 的用户侧 LIVE 入口已接入，但仍需操作者完成本机时钟、IP 白名单、余额和权限检查。

## 环境与安装

项目要求 Python `>=3.11,<3.13`，当前 CI 在 Windows 的 Python 3.11/3.12 上运行。Windows 是主要验收平台；Python 核心和 Qt 以跨平台为目标，但 macOS 尚未进入当前 CI。NapCat 本地运行物只由 GUI 显式启动和持有；仓库不再提供独立启动脚本。

```powershell
# 在仓库根目录创建独立环境
py -3.12 -m venv .venv

# 不必激活环境，直接用固定解释器安装
./.venv/Scripts/python.exe -m pip install --upgrade pip
./.venv/Scripts/python.exe -m pip install -e ".[dev]"

# 查看完整命令面
./.venv/Scripts/python.exe -m gribuki_trade --help

# 动态检查 SQLite shared-WAL 运行库安全性
./.venv/Scripts/python.exe -m gribuki_trade sqlite-runtime-status

# 运行质量门槛
./.venv/Scripts/python.exe -m ruff check conftest.py src tests
./.venv/Scripts/python.exe -m mypy src
./.venv/Scripts/python.exe -m pytest --temp-dir runtime/tmp -q

# 启动桌面 GUI；集成管理页可用，交易展示页仍为演示
./.venv/Scripts/python.exe -m gribuki_trade gui
```

依赖和版本区间以 [pyproject.toml](pyproject.toml) 为准。

## 常用工作流

### A 股发现与研究

以下命令会访问外部市场/新闻源；盘中、盘后时间门槛由代码检查。

```powershell
# 静态研究覆盖池：45 个标的（36 股 + 9 ETF），不是持仓或买入名单
./.venv/Scripts/python.exe -m gribuki_trade ashare-watchlist

# 15:05 后执行当前交易日股票三层筛选；默认同时写候选和运行档案
./.venv/Scripts/python.exe -m gribuki_trade ashare-market-screen-once `
  --top-n 30 --factor-budget 300 `
  --candidate-db runtime/research/candidates.sqlite3 `
  --run-db runtime/research/runs.sqlite3 `
  --output runtime/screening/latest.json

# 开市时段运行一次异常发现
./.venv/Scripts/python.exe -m gribuki_trade ashare-intraday-scan-once `
  --candidate-db runtime/research/candidates.sqlite3 `
  --run-db runtime/research/runs.sqlite3 `
  --output runtime/surveillance/latest.json

# 查看或维护候选
./.venv/Scripts/python.exe -m gribuki_trade ashare-candidates list

# 只跟踪 ACTIVE 候选一个有限周期
./.venv/Scripts/python.exe -m gribuki_trade ashare-research-watch `
  --candidate-db runtime/research/candidates.sqlite3 `
  --candidates-only --cycles 1

# 单标的收盘深研，并导出 Markdown + PNG
./.venv/Scripts/python.exe -m gribuki_trade ashare-close-research-once `
  --symbol 510300.SH --report-dir runtime/reports

# 有界批量深研 ACTIVE 候选
./.venv/Scripts/python.exe -m gribuki_trade ashare-close-research-batch `
  --candidate-db runtime/research/candidates.sqlite3 --limit 10

# 查看不可变筛选/扫描输出与 lineage
./.venv/Scripts/python.exe -m gribuki_trade ashare-research-runs list
```

### DeepSeek

API key 通过无回显提示写入 Windows Credential Manager/macOS Keychain，不写入仓库或命令历史。

```powershell
./.venv/Scripts/python.exe -m gribuki_trade deepseek-configure
./.venv/Scripts/python.exe -m gribuki_trade deepseek-status
```

默认模型是 `deepseek-v4-flash`。也可在收盘命令中显式指定 `--macro-provider openai`，但两种模型都受同一 EvidencePack 与推荐门禁约束。

GUI 的“集成管理”页可分别将 DeepSeek/OpenAI key 写入同一 OS keyring，选择默认 provider 与各自模型 ID，并把非秘密设置原子写入 `runtime/config/integrations.json`；PAPER、盘后、普通研究和实盘保护分析 CLI 在未显式覆盖时读取这些值。key 保存后立即从输入框清空。用户自行保存在 `secrets/` 下的明文 API 文件不会被 GUI、临时目录清理器或迁移流程删除。

### NapCatQQ

`vendor/` 被 Git 忽略，克隆项目不会自带 NapCat 源码、Shell 或 QQ 运行物。GUI 依赖已经安装/解压的受支持 Windows 运行目录；安装、扫码登录和端口配置见 [本机研究配置](docs/LOCAL_RESEARCH_SETUP.md)。

GUI 的“集成管理”页提供 loopback OneBot token 保存、QQ 登录态后台探测、WebUI 打开、本地运行目录选择和异步启动。OneBot 地址与运行目录写入上述共享配置；停止按钮只操作由当前 GUI 实例启动并持有的 PID tree，绝不会终止外部 NapCat；该页面不含券商或交易方法。

```powershell
# 启动和登录请在 GUI“集成管理”页显式完成；CLI 仅做只读检查
./.venv/Scripts/python.exe -m gribuki_trade napcat-status

# 显式发送一条固定测试消息
./.venv/Scripts/python.exe -m gribuki_trade napcat-send-test `
  --target-kind private --target-id <YOUR_QQ_ID> --confirm SEND_TEST

# 派发 durable outbox 中已入队的分段报告
./.venv/Scripts/python.exe -m gribuki_trade napcat-dispatch `
  --target-kind private --target-id <YOUR_QQ_ID> --cycles 1

# 校验报告契约后，耐久上传本地报告根内的 Markdown
./.venv/Scripts/python.exe -m gribuki_trade napcat-send-artifact `
  --target-kind private --target-id "YOUR_QQ_ID" `
  --artifact-kind file --report-kind INSTRUMENT_RESEARCH `
  --artifact-root runtime/reports `
  --artifact 510300.SH-示例.md --confirm SEND_ARTIFACT
```

OneBot 只允许 loopback HTTP、Bearer token 和精确私聊/群白名单。当前应用不自启常驻监听器；`live-sync ingest` 接收由未来应用运行框架转交或人工保存的 OneBot v11 私聊事件 JSON。当前用户侧附件 CLI 只接受报告根内、通过对应报告契约校验的 `.md/.markdown` 普通文件，并拒绝 URL、UNC、路径穿越和符号链接；底层适配器支持图片不等于该 CLI 对任意图片开放。

### 实盘成交同步与保护跟踪

`live-sync` 是“观察用户已完成成交”的独立账本，不是券商执行入口。BUY/SELL
消息采用 `GT-LIVE/1` 严格字段语法；第一条只建立 proposal，系统返回 24 位指纹，
同一白名单发送者必须再提交 `GT-LIVE-CONFIRM/1` 才会原子记账。BUY 确认同时
创建可恢复的 `BUILD_PROTECTION` 工作项；成交事务提交后，`ingest` 会只领取本次
`protection_work_id`；QUICK 阶段不读取 LLM key，也不以 NapCat 在线为前提，而是用公开行情和交易
日历尝试建立 QUICK，并对该批次执行一次有 timeout 的行情跟踪。QUICK 成功会原子标记
`plan_ready` 并排队 DEEP；首次跟踪若已经触发 barrier，提醒先耐久写入本地 outbox。
行情、日历、outbox 或跟踪故障都只进入 `immediate_protection` 嵌套回执和 durable retry，
不会撤销已经确认的成交；回执也不会把“已排队”谎称成“已完成”。每个新 proposal
必须同时给出 `external_order_id` 和
`external_fill_id`；前者是券商委托号，后者是每一次部分成交各不相同的成交执行号，
跨命令去重只绑定后者，因此同一委托的多次部分成交不会互相覆盖。成交时点还必须
通过 BaoStock 精确自然日历；日历不可用、返回结构不完整或该日休市均失败关闭。
未在即时尝试中完成的 QUICK 由后续 `cycle` 恢复；`cycle` 也会建立并原子公开 QUICK，同时排队独立的
`BUILD_DEEP_PROTECTION`；等待双轨 DEEP 时仍按有界间隔观察完成 bar，并立即派发
新出现的 NapCat 提醒。每个 DEEP attempt 写入独立候选物理流，只有仍持有当前
live lease generation 的 worker 才能原子切换 `plan_stream_id` 生产指针；旧 worker
即使迟到写入候选，也不会成为 active 保护。SELL 确认按买入批次原子核销，防止
并发超卖和同一外部成交重复记账；批次归零的同一事务会 fence 尚在构建的 QUICK/DEEP
任务，DEEP 完成和提醒入队也会再次核对剩余数量与当前 `plan_stream_id`。

```powershell
# event.json 是 OneBot v11 私聊事件；也可用 --event-json - 从标准输入读取
./.venv/Scripts/python.exe -m gribuki_trade live-sync ingest `
  --ledger-db runtime/live/observed-live.sqlite3 `
  --exit-plan-db runtime/live/exit-plans.sqlite3 `
  --outbox-path runtime/live/outbox.sqlite3 `
  --allowed-sender "YOUR_QQ_ID" --event-json .\event.json `
  --quick-timeout-seconds 45

./.venv/Scripts/python.exe -m gribuki_trade live-sync status `
  --ledger-db runtime/live/observed-live.sqlite3 --account live-main

# 单次、有限、可由后续应用运行框架重复调用；绝不创建订单
./.venv/Scripts/python.exe -m gribuki_trade live-sync cycle `
  --ledger-db runtime/live/observed-live.sqlite3 `
  --exit-plan-db runtime/live/exit-plans.sqlite3 `
  --outbox-path runtime/live/outbox.sqlite3 `
  --target-kind private --target-id "YOUR_QQ_ID" `
  --deep-timeout-seconds 600 `
  --tracking-pump-interval 30 --tracking-pump-limit 20 `
  --confirm LIVE_SYNC_CYCLE
```

`ingest` 和 `cycle` 使用的三个数据库必须是彼此不同的普通文件。`cycle` 会在打开这些数据库或构建保护前，先验证通知目标、loopback OneBot URL、LLM provider/model 以及对应 key；缺少任一项时整轮失败关闭。`ingest` 未显式给出
通知目标时，首次跟踪使用确认私聊的 sender 作为 private outbox 目标。`--base-url` 省略时读取 GUI
共享 OneBot 地址；地址可用时，本次入箱后还会尝试有限 NapCat 派发；派发失败不回滚成交、QUICK
或 durable outbox。持续实时
监看、DEEP LLM 和后续 outbox 派发仍由未来应用 runtime 重复调用 `cycle` 完成。保护工作、成交事件、批次分配
和通知均有幂等键或哈希链；通知工作进程按通道、目标类型和目标 ID 精确领取，
不会误领共享 outbox 中其他目标的消息。当前没有常驻应用主循环，也没有券商 API
调用；用户实际卖出后仍按同样的两阶段消息把成交同步回来。

### 推荐复核

```powershell
./.venv/Scripts/python.exe -m gribuki_trade ashare-review open `
  --recommendation-id <RECOMMENDATION_ID>
./.venv/Scripts/python.exe -m gribuki_trade ashare-review list
./.venv/Scripts/python.exe -m gribuki_trade ashare-review confirm `
  --case-id <CASE_ID> --reason MANUAL_EVIDENCE_REVIEWED `
  --confirm RESEARCH_ONLY
```

### A 股 PAPER 账本与单日盘中编排

`ashare-paper` 只记录显式账本动作，不会自动抓行情或撮合：

```powershell
./.venv/Scripts/python.exe -m gribuki_trade ashare-paper open `
  --account personal-paper --initial-cash 100000 `
  --session-date 2026-08-14

./.venv/Scripts/python.exe -m gribuki_trade ashare-paper fill `
  --account personal-paper --fill-id manual-20260814-001 `
  --symbol 600000.SH --side BUY --quantity 100 --price 10.00 `
  --instrument STOCK --source MANUAL --session-date 2026-08-14

./.venv/Scripts/python.exe -m gribuki_trade ashare-paper snapshot `
  --account personal-paper
```

`ashare-paper-day run` 则是明确确认后阻塞运行至收盘后的单进程 PAPER 编排；它从不连接真实券商。日期必须是上海时区当天且经 BaoStock 交易日历验证，通知目标应替换为自己的私聊或群白名单：

```powershell
$tradeDate = (Get-Date).ToString('yyyy-MM-dd')

# 查看 run/status/report/summary 及当前完整参数面
./.venv/Scripts/python.exe -m gribuki_trade ashare-paper-day --help

./.venv/Scripts/python.exe -m gribuki_trade ashare-paper-day run `
  --session-date $tradeDate --initial-cash 200000 `
  --target-kind private --target-id "YOUR_QQ_ID" `
  --intraday-llm --intraday-llm-review-top-n 6 `
  --intraday-llm-review-ttl-minutes 20 `
  --intraday-llm-max-calls unlimited `
  --intraday-llm-events-db runtime/news/events.sqlite3 `
  --confirm PAPER_DAY

# 以下三个动作只读按日 sidecar；summary 会原子写出增强 Markdown 摘要
./.venv/Scripts/python.exe -m gribuki_trade ashare-paper-day status `
  --session-date $tradeDate
./.venv/Scripts/python.exe -m gribuki_trade ashare-paper-day report `
  --session-date $tradeDate
./.venv/Scripts/python.exe -m gribuki_trade ashare-paper-day summary `
  --session-date $tradeDate
```

盘中 LLM 默认启用且是新买入的 required 门禁；`--no-intraday-llm` 只用于显式进入“持仓监控/卖出复核、禁止新买”模式，该 opt-out 会落审计，不存在技术规则单独买入的旁路。事件库须由盘前新闻任务预填，runner 只冻结快照、不在盘中抓新闻。`--maximum-positions <N>` 可显式启用持仓计数熔断；对已经写入 journal 的会话变更风险策略还需要 `--confirm-risk-policy-change PAPER_RISK_POLICY_CHANGE`。`DAY_ABORTED` 后只有人工核实并增加 `--recover-after-abort` 才会在同一不可变 Run 上恢复。

收盘 `DAILY_REVIEW` 的 Markdown 使用按日 `report-artifacts.sqlite3`：先绑定目标、文件名和 SHA-256，再原子 claim，且只调用一次 NapCat。调用后断连、取消或崩溃一律成为 `AMBIGUOUS`，后续运行不会自动重发；`PENDING` 会继续，`SENT` 只补 journal。CLI 中 `completed` 仅表示交易监控已终止，只有短摘要无缺口且 Markdown 为 `SENT` 时 `ok`/`daily_review_delivery_complete` 才为真；未配置附件通道会明确返回 `NOT_CONFIGURED` 和一个交付缺口。歧义状态只能在人工查看 QQ/NapCat 后选择以下一种恢复动作，并同时保留原 `run` 的其他参数：

```powershell
# 已在提供方确认文件存在：只补本地 SENT 回执，不再上传
--report-artifact-recovery-action MARK_SENT_AFTER_PROVIDER_VERIFICATION `
--report-artifact-provider-id "VERIFIED_PROVIDER_FILE_ID" `
--confirm-report-artifact-recovery PAPER_REPORT_ARTIFACT_RECOVERY

# 已在提供方确认文件不存在：显式授权一次重新上传
--report-artifact-recovery-action RESEND_AFTER_PROVIDER_NON_RECEIPT_VERIFICATION `
--confirm-report-artifact-recovery PAPER_REPORT_ARTIFACT_RECOVERY
```

`RESEND` 授权是单次消费的；若这一次人工重发仍断连或结果不确定，重启和重复传入同一参数都不会再次上传，必须重新核验提供方并以 `MARK_SENT` 收敛，不能把旧授权当作重试循环。

仓库已删除 Windows 任务和独立看门脚本；常驻、恢复和盘后衔接等待后续统一应用运行框架。

durable **日线**委托撮合仍只提供 Python API；PAPER-day **盘中**编排已有上述 CLI。完整边界见 [A 股 PAPER 文档](docs/A_SHARE_PAPER_TRADING.md)。

### A 股盘后深研

`ashare-post-close run/status/report` 在上海时区同日交易日 15:05 后复用已完成的 PAPER sidecar 与账本，逐持仓串行运行收盘深研，写出 Markdown，并经按日专用 outbox 向当天冻结的同一 NapCat 目标交付。DeepSeek 默认启用；全持仓深研失败时不发送且非零退出，部分失败会在报告中明确标为“部分完成”。Windows Task Scheduler/看门脚本及内部安装 action 已全部删除；下一版由应用内运行框架负责交易日历、互斥、恢复和生命周期。完整单次运行、幂等状态机与恢复边界见 [A 股盘后深研说明](docs/A_SHARE_POST_CLOSE_AUTOMATION.md)。

### 策略实验

```powershell
# 只扩展版本化、安全、有限的因子 grammar；不访问行情或 holdout
./.venv/Scripts/python.exe -m gribuki_trade strategy-factor-discover `
  --max-trials 100 --output runtime/strategy/factor-inventory.json

# 对冻结逐笔样本执行完整退出策略 walk-forward + holdout；结果只用于研究
./.venv/Scripts/python.exe -m gribuki_trade strategy-exit-evaluate `
  --dataset runtime/strategy/exit-dataset.json `
  --specification runtime/strategy/exit-experiment-spec.json `
  --output runtime/strategy/exit-trial-registry.json `
  --confirm RESEARCH_ONLY
```

完整实验协议见 [策略实验室](docs/STRATEGY_LAB.md)。

### Binance 与 Schwab

Binance Spot Testnet 已有 status、order test、普通虚拟 cycle、durable OMS cycle/fill、公开历史归档、回测和本地 SHADOW。Spot LIVE 与 USDⓈ-M Futures LIVE 也提供受 `LiveTradingGuard` 保护的状态、`order/test`、下单和撤单入口。带 `cycle/fill` 的命令会改变远端 Testnet 状态，LIVE 的 `submit` 会创建真实订单；两者都必须使用显式确认短语。

```bash
./.venv/Scripts/python.exe -m gribuki_trade binance-testnet-status
./.venv/Scripts/python.exe -m gribuki_trade binance-testnet-order-test
./.venv/Scripts/python.exe -m gribuki_trade binance-testnet-oms-cycle --confirm TESTNET
./.venv/Scripts/python.exe -m gribuki_trade binance-testnet-oms-fill --confirm TESTNET_FILL
./.venv/Scripts/python.exe -m gribuki_trade binance-live-status --confirm "ENABLE LIVE TRADING"
./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-status --confirm "ENABLE LIVE TRADING"
./.venv/Scripts/python.exe -m gribuki_trade binance-live-balance --asset USDT --confirm "ENABLE LIVE TRADING"
./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-balance --confirm "ENABLE LIVE TRADING"
./.venv/Scripts/python.exe -m gribuki_trade binance-history-sync --help
./.venv/Scripts/python.exe -m gribuki_trade binance-backtest --help
./.venv/Scripts/python.exe -m gribuki_trade binance-shadow-run --help
```

查询当前 Git Bash 网络代理的公网 IPv4（输出可直接复制到币安 API 白名单）：

```bash
bash ./scripts/current_ip.sh
bash ./scripts/current_ip.sh --verbose
```

该地址是本次 `curl` 请求的出口地址，不是 `192.168.*`、`10.*`、`198.18.*` 等本机或虚拟网卡地址。
如果交易进程和 Git Bash 使用了不同的 `HTTPS_PROXY`/`ALL_PROXY`，两者出口可能不同，应在同一网络环境下查询。

USDⓈ-M Futures 状态会返回 `position_mode`。单向持仓下单传
`--position-side BOTH`；双向持仓传 `--position-side LONG` 或 `SHORT`。客户端会在
order/test 和真实提交前查询账户模式，不会根据买卖方向猜测或自动修改账户设置。

Schwab 当前没有 CLI 或真实生产联调。OAuth、Market Data、Trader REST 和简单限价接口只完成了代码与离线 transport 测试；获得 Developer App 后，应先做真实网络下的只读授权、entitlement、限频和账户对账。当前接口边界见 [Binance/Schwab 接入说明](docs/BINANCE_SCHWAB_INTEGRATION.md)，2026-08-13 的本机 Testnet/SHADOW 数据见 [Binance 验收快照](docs/BINANCE_SIMULATION_STATUS.md)。

## 仓库结构

| 路径 | 职责 |
|---|---|
| `src/gribuki_trade/domain` | 订单、候选、推荐、复核、PAPER、退出计划与实盘观察等不可变领域模型 |
| `src/gribuki_trade/ports` | 行情、日历、LLM、通知、账本和 broker 协议 |
| `src/gribuki_trade/adapters` | AKShare/BaoStock、官方数据、DeepSeek/OpenAI、NapCat、Binance、Schwab 等外部边界 |
| `src/gribuki_trade/ingest` | 新闻、公告、官方宏观与搜索发现采集 |
| `src/gribuki_trade/features` | 盘中异常、收盘筛选、技术因子及 QUICK/DEEP 退出计划纯计算 |
| `src/gribuki_trade/services` | 筛选、研究、候选、复核、通知、PAPER、盘后与 live-sync 恢复编排 |
| `src/gribuki_trade/policy` | 技术/宏观融合与推荐发布门禁 |
| `src/gribuki_trade/storage` | SQLite 事件库、候选、研究、outbox、PAPER、退出计划、实盘观察、OMS 和运行档案 |
| `src/gribuki_trade/strategy` | 可复用的确定性策略基线 |
| `src/gribuki_trade/strategy_lab` | 冻结 manifest、walk-forward、因子 DSL/发现、单证券 A 股评价器与退出策略实验入口 |
| `src/gribuki_trade/backtest` | 成本与推荐事后评价 |
| `src/gribuki_trade/trading` | broker-neutral OMS 与执行状态 |
| `src/gribuki_trade/reporting` | 六类报告契约、Markdown/PNG 研究产物与 PAPER/盘后摘要 |
| `src/gribuki_trade/runtime` | PAPER/SHADOW/LIVE 模式、broker 守卫与集中临时目录解析 |

大型入口保留历史兼容路径，但内部职责已开始拆分：CLI 的无副作用参数转换在
`cli_parsing.py`，Binance Spot 的纯协议解析在
`adapters/binance/spot_parsing.py`，USDⓈ-M OMS 的 SQLite 编解码在
`trading/futures_oms_codec.py`。后续命令处理器和长工作流会沿同一规则逐步拆出；
具体边界见 [模块地图](docs/architecture/module-map.md)。
| `src/gribuki_trade/security` | OS keyring、token 和密钥边界 |
| `src/gribuki_trade/gui` | NapCat 与 DeepSeek/OpenAI 集成管理，以及仍使用占位数据的交易展示页 |
| `config` | 静态 A 股研究覆盖池 |
| `scripts` | 本地测试与受控临时目录归档清理；NapCat 启动、OS 看门和任务计划脚本均已移除 |
| `tests` | 离线契约、PIT、持久化、恢复、CLI 和 GUI smoke 测试 |
| `.github/workflows` | Windows Python 3.11/3.12 质量流水线 |

`runtime/`、`secrets/`、`vendor/`、`data/`、`logs/`、数据库、Parquet 和本地密钥文件均被 `.gitignore` 排除。

## 临时工作目录

可丢弃的测试与诊断工作区统一解析到一个安全根目录，优先级固定为：命令行显式 `--temp-dir` > 环境变量 `GRIBUKI_TRADE_TMP_DIR` > 仓库内默认 `runtime/tmp`。解析器拒绝文件系统根和仓库根，不修改进程全局临时目录；需要与目标文件原子替换的临时文件仍必须留在目标旁边。

```powershell
# 只解析并报告来源，不创建目录
./.venv/Scripts/python.exe -m gribuki_trade temp-root status

# 查看 status/prepare 与 --temp-dir 的当前帮助
./.venv/Scripts/python.exe -m gribuki_trade temp-root --help

# 显式准备一个集中目录
./.venv/Scripts/python.exe -m gribuki_trade temp-root prepare `
  --temp-dir runtime/tmp

# 根目录 conftest.py 会把 pytest 与标准库临时文件集中到进程隔离的子目录
./.venv/Scripts/python.exe -m pytest --temp-dir runtime/tmp -q
```

历史仓库根临时树只能先由 [归档清单工具](scripts/archive_root_temps.py) 建立并校验证据清单，再由 [受控清理脚本](scripts/cleanup_root_temps.ps1) 按完全一致的清单处理；脚本会拒绝目标集合漂移、reparse point 和未验证归档。这里不记录任何本机个人路径或具体清理目标。

```powershell
# 只盘点仓库根 .tmp*，不归档、不删除
./.venv/Scripts/python.exe .\scripts\archive_root_temps.py

# 查看归档、唯一证据保留与 manifest finalize 参数
./.venv/Scripts/python.exe .\scripts\archive_root_temps.py --help
```

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
./.venv/Scripts/python.exe -m gribuki_trade sqlite-runtime-status
```

当前安全策略只认可官方已修复线：`3.44.6`、`3.50.7` 和 `>=3.51.3`。若命令返回 `SQLITE_WAL_RESET_RUNTIME_UNSAFE`，单连接、本地、单 Store 流程仍可临时使用，但同一个数据库路径不得被两个 CLI、worker 或 Store 实例并发打开；常驻或多进程部署必须失败关闭。业务 writer lease 不能修复 SQLite checkpointer/writer 竞争。背景见 [SQLite WAL-reset 说明](https://www.sqlite.org/wal.html#walresetbug)。

## 质量门槛

[quality.yml](.github/workflows/quality.yml) 在 Windows、Python 3.11/3.12 上运行：

```powershell
./.venv/Scripts/python.exe -m ruff check conftest.py src tests
./.venv/Scripts/python.exe -m mypy src
./.venv/Scripts/python.exe -m pytest --temp-dir runtime/tmp -q
```

测试覆盖 provider schema/超时/降级、PIT 与 revision、筛选/候选、技术/宏观门禁、LLM 证据引用、通知 outbox、PAPER/OMS 恢复、策略实验、CLI 和 GUI smoke。网络集成与长时间 soak 不由离线单元测试替代。

## 文档索引

### 当前状态与实现

- [项目完整交接文档](docs/PROJECT_HANDOFF_260814.md)：面向维护交接，说明代码结构、运行边界与操作手册；具体行为仍以当前代码、CLI 和测试为准。
- [交易策略体系说明](docs/TRADING_STRATEGY_GUIDE_260814.md)：面向非金融背景读者，解释选股、买入、卖出、退出计划与双轨 LLM。
- [A 股数据源矩阵](docs/A_SHARE_DATA_SOURCES.md)：来源口径、发布时间、PIT、降级与许可层级。
- [全市场筛选、搜索发现与宏观融合](docs/A_SHARE_SCREENING_AND_DISCOVERY.md)：收盘筛选、盘前降级种子、搜索证据和融合边界。
- [A 股 PAPER 账本与撮合](docs/A_SHARE_PAPER_TRADING.md)：账本、matcher 和崩溃恢复边界。
- [A 股盘后深研与应用编排边界](docs/A_SHARE_POST_CLOSE_AUTOMATION.md)：15:05 gate、幂等 run 与 NapCat artifact；OS 任务方案已弃用。
- [策略实验室](docs/STRATEGY_LAB.md)：walk-forward、DSL、候选发现与评价器。
- [本机 A 股研究配置](docs/LOCAL_RESEARCH_SETUP.md)：DeepSeek、NapCat、扫码和附件发送。
- [Binance 与 Schwab 接入](docs/BINANCE_SCHWAB_INTEGRATION.md)：当前适配器、Testnet 执行与生产边界。
- [Binance 模拟交易验收快照](docs/BINANCE_SIMULATION_STATUS.md)：2026-08-13 的 Testnet、历史回放和 SHADOW 实测记录，不是实时状态页。

### 设计、调研与历史记录

- [历史文档归档](docs/archive/README.md)：旧需求、整改输入、旧路线图、MVP 选型稿和本轮变更记录；这些文件不再描述当前能力。
- [零注册依赖策略基线](docs/ZERO_REGISTRATION_STRATEGIES.md)：研究假设集合；只有文内明确标出的组件可视为已实现。
- [广发证券接入清单](docs/GF_SECURITIES_ONBOARDING.md)：未来 QMT 询问与开户信息清单，不是当前适配器说明。

## 下一阶段

优先级不是继续增加更多自由 LLM Agent，而是补齐可否证的数据与运行闭环：

1. 每日归档完整全市场股票池、排除原因、因子输入、公司行动和历史证券主数据，形成真正的 PIT 横截面数据集。
2. 在现有单交易日、单进程 PAPER-day 编排之上实现跨交易日常驻协调、背压、misfire 和休眠恢复，并在安全 SQLite 运行库上做连续 soak。
3. 建立横截面组合级 A 股回测器，覆盖退市股、历史 ST/停牌/涨跌停、T+1、成本、容量、行业/规模暴露和基准。
4. 为 durable **日线** PAPER matcher 增加显式恢复 CLI、GUI 和完整运行观测；盘中 PAPER-day CLI 继续补 soak 与故障演练，但两者都不伪造盘口队列。
5. 建立策略注册、统计校正、人工审核和 shadow 发布流程；未通过独立 holdout 的新因子不得进入生产配置。
6. 在已完成的 NapCat/DeepSeek 集成管理页之外，继续接入候选矩阵、证据、PAPER 账本和 dead-letter 管理；交易展示页在接入前仍明确标为演示。
7. 真实券商只在只读联调、对账、风险、kill switch 和长时间 PAPER/SHADOW 验收完成后考虑开放。
