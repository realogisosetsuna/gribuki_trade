# A 股全市场筛选、搜索发现与宏观融合边界

更新日期：2026-08-15

本文说明三项已经进入代码库、但成熟度不同的能力：全市场三层筛选漏斗、
Tavily/SearXNG 搜索发现，以及搜索事件进入宏观证据后的评分门禁。这里描述的是
当前代码的真实行为，不是未来产品功能表，也不是收益承诺。

## 1. 当前状态总览

| 能力 | 当前状态 | 已能完成 | 尚不能完成 |
|---|---|---|---|
| 三层全市场筛选漏斗 | 核心、单次 CLI、候选/运行档案和有界批量深研已实现；自动编排未接入 | 收盘后全市场硬过滤、有限候选历史因子补全、横截面排序、Top-N 候选入库，以及 ACTIVE 候选的串行深研、报告和 NapCat outbox | 尚无完整快照/全排名数据库、应用内交易日调度、GUI 页面或无人值守的每日闭环 |
| PAPER-day 盘前降级种子 | 已接入 `ashare-paper-day`，无独立执行入口 | 09:25 前把当前网页快照保守绑定到已核验的上一交易日，产生沪深研究种子 | 不是集合竞价、历史收盘重放或买入授权；北交所不在该路径，开盘后必须由当前时段证据复核 |
| Tavily/SearXNG 搜索发现 | 核心及收盘研究 CLI 已接入 | 个股、行业、宏观查询；并发、故障隔离、去重、线索事件归一化；SearXNG 可选 Keyring Bearer 鉴权 | 不保证搜索覆盖率、网页事实正确性或原文长期可访问 |
| 搜索证据与宏观融合门禁 | 已实现 | `discovery_hint` 拒绝进入宏观证据；满足晋级条件的 `discovery_confirmed` 才可继续经过 PIT/时效/注入等门禁 | `confirmed` 不代表事实已核实；宏观分不是概率，也不能把技术非入场信号升级为入场 |

三项能力都只生成研究数据或研究建议，不生成订单、目标数量或券商请求。

## 2. 三层全市场筛选漏斗

核心接口与实现分别位于：

- [全市场筛选端口](../src/gribuki_trade/ports/ashare_screening.py)
- [确定性硬过滤与因子排名](../src/gribuki_trade/features/ashare_screening.py)
- [三层编排服务](../src/gribuki_trade/services/ashare/research/ashare_screening.py)
- [AKShare 收盘快照与历史因子适配器](../src/gribuki_trade/adapters/ashare/screening/screening.py)
- [PAPER-day 盘前降级适配器](../src/gribuki_trade/adapters/ashare/screening/preopen_screening.py)
- [盘前种子编排服务](../src/gribuki_trade/services/ashare/research/ashare_preopen_screening.py)

当前数据流是：

```text
当前交易日 15:05 后的全 A 股网页收盘快照
    │
    ├─ L1：低成本、fail-closed 硬过滤
    │      └─ 按当日成交额排序，只把最多 300 个幸存者送入历史补全
    │
    ├─ L2：未复权日线、公司行为防护、横截面因子评分
    │      └─ 输出有完整因子审计行的确定性排名
    │
    └─ L3：截取默认 Top 30，写入候选事件库和筛选运行档案
           ├─ 可由有界串行批处理生成单标的报告并进入 NapCat outbox
           └─ 应用内自动调度、GUI 列表和失败重跑尚未接入
```

### 2.1 L1：全市场快照和硬过滤

适配器先尝试 AKShare/东方财富 `stock_zh_a_spot_em`，失败后尝试
AKShare/腾讯 `stock_zh_a_spot_tx`。腾讯成交额和总市值会按其上游单位显式换算为
人民币。一个快照少于默认 4,500 个标的会被视为覆盖异常，而不是继续生成排名。

网页接口没有权威 payload 时间戳，因此上述**收盘适配器**只允许查询“上海时区当前交易日”，
并保守地把 15:05 作为收盘数据最早可用时间。它不是交易所 feed，不支持盘中全市场
筛选，也不能用今天下载的页面重建过去某日的股票池。

默认硬过滤规则如下：

| 项目 | 默认值或行为 |
|---|---|
| 上市时间 | 至少 250 天；元数据未知时严格排除 |
| 交易状态 | 不可交易、停牌、ST/风险警示均排除；状态未知也排除 |
| 当日价格 | 至少 1 元；缺失、非正数排除 |
| 当日成交额 | 至少 2,000 万元 |
| 总市值 | 至少 20 亿元 |
| 板块 | 筛选核心默认允许沪深主板、创业板、科创板、北交所，调用方可收窄 |
| 历史补全预算 | 硬过滤后按当日成交额降序，最多 300 个标的 |

“筛选允许某板块”不等于 PAPER/LIVE 执行白名单允许该板块。筛选结果只是研究候选，
后续交易规则仍须单独限制。

每个被排除标的保留全部原因；缺字段不会用默认值猜测。超过历史补全预算的幸存者会
标记为 `FACTOR_BUDGET_DEFERRED`，这意味着当前排名刻意偏向流动性较高的候选，不能
把未进入 L2 解释为负面投资判断。

#### 2.1.1 PAPER-day 盘前降级种子

`ashare-paper-day` 在上海时区当前自然日不晚于 09:25 时，可以调用独立盘前适配器。该适配器仍抓取“现在”的东方财富/腾讯网页快照，但只在交易日历已经精确核验上一交易日后，才把它保守重标为上一交易日研究种子。行情与因子状态无条件标记为 `DEGRADED`，并显式记录当前抓取时点，不能用来声称拥有上一交易日的不可变原始快照。

这条路径只承诺沪深主板、创业板和科创板，北交所失败关闭。种子进入当日 watchlist 后，任何 PAPER 买入仍必须取得开盘后的当前 surveillance、技术、LLM、价格、数量、资金和 QUICK 保护证据；盘前结果本身不能创建订单。

### 2.2 L2：历史补全和横截面评分

L2 仅为预算内候选请求 AKShare `stock_zh_a_hist` 未复权日线，默认最少需要 201 个
交易日、单标的超时 18 秒、并发数 4。价格类因子只有在前收盘连续性检查覆盖足够且
未检测到疑似公司行为断点时才计算；当前复权口径重写历史数据会被 PIT 服务拒绝。

20 日平均成交额低于 5,000 万元的标的在评分前排除。默认评分因子是：

| 因子 | 权重 | 方向 |
|---|---:|---|
| 20 日动量 | 10% | 高为优 |
| 60 日动量 | 15% | 高为优 |
| 120 日动量、跳过最近 5 日 | 20% | 高为优 |
| MA20/MA60 趋势 | 15% | 高为优 |
| 20 日突破位置 | 10% | 高为优 |
| 20 日量比 | 10% | 高为优 |
| 60 日年化波动率 | 8% | 低为优 |
| 60 日最大回撤幅度 | 7% | 低为优 |
| 20 日 Amihud 非流动性 | 5% | 低为优 |

每个因子先在当日横截面做 2.5%/97.5% winsorize，再做处理并列值的百分位排名，
最后转换为 `[-1, 1]` 的方向分并乘权重。至少需要 20 个有效横截面观测；少于该数量
时整个因子标为不可用。

缺失因子不会被填成中位数、零或“中性分”。缺失权重直接从综合分中扣留；可用权重
低于 80% 的候选不参与排名。输出同时保留原始值、截尾值、百分位、方向分、权重、
贡献和横截面样本数。

当前综合分是同一截面内的相对排序量，不是上涨概率、预期收益或经过校准的置信度。
当前也没有做行业/市值中性化，没有基本面估值因子，L1/L2 不调用新闻或 LLM。

### 2.3 L3：Top-N 研究交接，而不是自动荐股

服务默认截取前 30 名，连同数据完整度、降级原因和逐因子贡献交给后续研究层。这里的
“第三层”当前是稳定的候选交接边界，不是已经完成的 30 只股票并发 DeepSeek 决策。

单次 CLI 已接入：

```powershell
# 当前交易日 15:05（Asia/Shanghai）后运行；输出 JSON 到 stdout
.\.venv\Scripts\python.exe -m gribuki_trade ashare-market-screen-once

# 默认同时保存 Top-N 候选和规范化运行档案；可另行原子写出 JSON
.\.venv\Scripts\python.exe -m gribuki_trade ashare-market-screen-once `
  --top-n 30 --factor-budget 300 `
  --candidate-db runtime/research/candidates.sqlite3 `
  --run-db runtime/research/runs.sqlite3 `
  --output runtime/screening/latest.json

# 对 ACTIVE 候选执行最多 10 个标的的串行收盘深研、报告和通知入箱
.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-batch `
  --candidate-db runtime/research/candidates.sqlite3 --limit 10 `
  --notify-target-kind private --notify-target-id "YOUR_QQ_ID"
```

CLI 支持修改 Top-N、因子补全预算、上市天数、当日成交额、20 日平均成交额和市值
门槛。15:05 前运行会返回结构化 `MARKET_NOT_CLOSED`，不会调用行情 provider。stdout
和可选 JSON 包含 source revision、漏斗计数、Top-N、逐因子贡献、排除原因汇总、
缺数候选和 warnings。

当前尚缺：

- 完整原始快照、全截面历史排名和全部因子审计行的数据库；当前候选库只保留 Top-N，运行档案
  保存规范化输出与 lineage，`--output` 仍只是一次性 JSON 投影；
- 应用内交易日历调度、GUI 列表、无人值守运行状态和失败重跑；
- 历史股票池归档及真正的 Point-in-Time 全市场 walk-forward 回测。

因此，目前不能声称软件已经会“每天自动从全市场选出并推送 30 只股票”。核心算法和
单次 CLI、候选/运行档案及有界批量深研已有离线测试，但连续自动运行和真实数据 soak test
仍未完成。

## 3. Tavily 与 SearXNG 搜索发现

实现位于 [search_discovery.py](../src/gribuki_trade/ingest/search_discovery.py)。搜索只用于
发现可能相关的 URL 和摘要，不替代交易所公告、监管机构原文或媒体全文抓取。

### 3.1 Tavily 配置

Tavily API key 的固定本机凭据名是：

```text
search.tavily.api_key
```

在 Windows Credential Manager/macOS Keychain 中通过无回显入口保存：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade secret-set search.tavily.api_key
.\.venv\Scripts\python.exe -m gribuki_trade secret-status
```

不要把 key 写入命令参数、`.env`、仓库文件、日志或聊天。适配器使用 Tavily 官方
`POST https://api.tavily.com/search` 和 Bearer header；key 不进入 URL、请求对象
`repr` 或持久化事件。

### 3.2 SearXNG 配置

当前收盘研究 CLI 只接受实例 URL：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-once `
  --symbol 510300.SH `
  --searxng-url https://search.example.com
```

实例必须：

- 使用 HTTPS 主机；字面量 localhost、私网 IP、含用户名/密码的 URL 和 HTTP 会在
  网络请求前被拒绝；
- 启用 SearXNG JSON 输出；适配器调用官方 `GET /search?...&format=json`；
- 能承受实例自身和底层搜索引擎的速率限制。

SearXNG 实例 URL 只通过 `--searxng-url` 传入，不作为秘密保存。若实例或反向代理需要
Bearer token，可把它通过无回显入口保存到可选 Keyring 名：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade secret-set search.searxng.bearer_token
```

CLI 只有在同时传入 `--searxng-url` 时才读取该 token，并以 Authorization header
传给核心 provider。不要把 token 塞进 URL。当前 endpoint 校验不做 DNS 解析结果固定
或 DNS rebinding 检测，因此只应配置用户控制、已知解析目标的主机名。

### 3.3 收盘研究中的启用行为

`ashare-close-research-once` 默认启用 `--search-discovery`，但“开关启用”不代表已经有
provider：

- Keyring 中存在 `search.tavily.api_key` 时启用 Tavily；
- 传入 `--searxng-url` 时启用 SearXNG，并按需读取
  `search.searxng.bearer_token`；
- 两者都没有时记录 `SEARCH_DISCOVERY_PROVIDERS_NOT_CONFIGURED`，其他官方/媒体新闻
  采集和收盘研究仍继续；
- `--no-search-discovery` 可完全关闭搜索发现；
- `--no-refresh-news` 会读取已留存事件，不执行本轮新搜索。

CLI 当前为每个标的生成个股、国内政策与流动性、美联储/美元/美债/美股、人民币/
港股/商品/能源/地缘风险四组查询；能取得行业元数据时再生成行业查询。个股与行业
查询最多 8 条，每组宏观查询最多 6 条。provider × query 并发执行，单 provider 的
401/403、429、超时、传输或 JSON schema 故障不会终止其他新闻源。

结果按 canonical URL 和规范化标题聚类，保留搜索 provider、匹配 URL、规范化发布者身份、
可解析的 `published_at`、本机首次观察时间、内容 SHA-256 和查询实体。发布者身份按注册域计算；
已知官方域映射到稳定的官方主体身份。provider ID 只表示发现路由，绝不作为独立发布者计数。
搜索 API 原始响应不进入原始文档档案；只保存长度受限、明确标记为搜索线索的事件。未提供或
无法解析发布时间时不会编造时间。

当前没有 Brave provider，也不会持久化 Brave 搜索结果。

## 4. `discovery_hint`、`discovery_confirmed` 与证据门禁

搜索事件采用显式二阶段 lineage：

| 情形 | 事件类型 | 能否进入宏观证据选择 |
|---|---|---|
| 普通媒体 URL，仅一个发布域 | `discovery_hint` | 否；即使多个搜索 provider 返回、entities 精确命中股票代码也拒绝 |
| URL 主机命中配置的官方域名 | `discovery_confirmed` | 可以继续参加后续门禁 |
| 多个 provider 发现同一 canonical URL | `discovery_hint` | 否；它们仍指向同一发布者页面 |
| 同一注册发布域的不同 URL 标题相同 | `discovery_hint` | 否；子域或栏目路由不能制造第二家媒体 |
| 不同独立注册发布域的规范化标题相同 | `discovery_confirmed` | 可以继续参加后续门禁 |

默认官方域名集合包括巨潮、证监会、国务院/政府、财政部、发改委、央行、外汇局、
上交所、统计局和深交所的主域名。子域名可匹配，形如 `sse.com.cn.evil.example` 的
后缀伪装不能匹配。

标题“同一故事”目前只做大小写、空白和标点归一化后的精确相等，不做 embedding 或
LLM 语义聚类；过短或已知通用标题不会用于独立发布域晋级。注册域使用保守的常见多标签公共
后缀规则，已知监管/交易所域使用稳定官方主体身份。它仍会遗漏改写标题，也不能解释为事实级
交叉验证。

两条重要边界：

1. `discovery_hint` 可以进入事件库，并通过 `hint_events` API 供展示层读取；宏观
   selector 在实体匹配之前直接拒绝它，不能因为带有股票代码而绕过确认门。当前
   收盘报告不保证逐条展示所有 hint，专用线索区仍需接线。
2. `discovery_confirmed` 的含义只是“官方域命中，或同标题由至少两个独立发布域承载”。事件仍为
   `SourceTier.PUBLIC_MEDIA`，不会冒充 `OFFICIAL`。多个搜索 provider 都发现一个页面不会晋级；
   即使不同发布域达到晋级条件，也不证明页面中的主张正确或彼此独立采编。

confirmed 事件进入宏观 EvidencePack 前仍必须通过：

- 决策时点可见性和未来信息拒绝；
- 发布时间/首次观察时间和时效窗口；
- 提示注入标记过滤；
- canonical URL/内容去重；
- 标的或宏观范围相关性；
- 每来源数量上限和证据多样性限制。

最佳实践仍是沿 confirmed URL 抓取原文；官方域名内容应转为独立官方 source 事件并
保留原文哈希、修订关系和抓取时间。搜索摘要只是发现层，不能替代该步骤。

## 5. 宏观评分上限和技术入场权

确定性门禁位于
[recommendation_gate.py](../src/gribuki_trade/policy/recommendation_gate.py)。原始技术分和
宏观 `macro_impact` 都在 `[-1, 1]`，它们是未校准的研究量表，不是概率。

收盘研究 CLI 的默认权重为：

- 技术面 75%；
- 宏观面 25%；
- `--macro-weight` 允许 `0` 到 `0.40`，大于 `0.40` 在参数解析时拒绝；
- 宏观模型只给出 `WATCH` 时，宏观有效权重再乘 0.5。默认即 12.5%，显式设为
  40% 时最多 20%。

实际融合为：

```text
effective_macro_weight = configured_macro_weight × decision_multiplier
combined_score = technical_score × (1 - effective_macro_weight)
               + macro_score × effective_macro_weight
```

宏观分只有在以下条件全部满足时才参与融合：模型未 `ABSTAIN`、存在证据引用、所有
引用都能在本地留存证据中找到、引用覆盖率达到默认 10%。否则综合分保持技术分，原本
的边缘技术入场还可能因宏观不可用或证据无效降为 `WATCH`。

宏观具有降级权，但没有创建入场的权力：

- 技术层已经给出 `ENTER_CANDIDATE` 时，强负面宏观可否决为 `WATCH`；
- 融合分低于入场阈值也可把技术候选降为 `WATCH`；
- 技术层给出 `WATCH`、`REDUCE` 或 `ABSTAIN` 时，即使宏观分为 `+1` 且融合分超过阈值，
  决策也不会升级为 `ENTER_CANDIDATE`，只记录 `TECHNICAL_ENTRY_GATE_NOT_MET`。

“宏观最多 40%”同时由收盘研究 CLI 和底层 `RecommendationGateConfig` 强制执行；
绕过 CLI 的新调用方也不能把宏观配置权重提高到 40% 以上。

## 6. 当前验收结论与后续工作

当前可以据此开展：

- 在单标的收盘研究中启用 Tavily 和/或 SearXNG，观察 hint/confirmed 数量、来源失败
  和宏观证据采用情况；
- 通过现有 CLI 运行当前交易日收盘后的全市场筛选和受控实验；
- 验证宏观缺失、模型弃权、线索未确认和负面宏观时系统是否按预期降级。

在宣称“全市场自动选股系统可用”前仍需完成：

1. 持久归档每日完整原始快照、全排名、所有排除原因和全部因子审计行，而不只保存规范化运行结果；
2. 归档每日完整股票池和 source revision，建立无幸存者偏差的历史回放；
3. 对因子做行业/规模暴露、换手、交易成本和 walk-forward 评估；
4. 对 Tavily/SearXNG 做连续可用性、费用、429 和字段漂移观测；
5. 对 confirmed URL 增加原文抓取、事件聚类、修订追踪和官方 source 升级；
6. 连续至少 20 个交易日运行筛选、证据、报告和通知 soak test。

完成这些工作之前，Top-N、技术分、宏观分和综合分都只能称为未校准研究输出。
