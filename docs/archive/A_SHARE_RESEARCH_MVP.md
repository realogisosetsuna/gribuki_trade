# A 股公开数据、资讯与研究建议 MVP

> **历史归档（2026-08-15）**：本文是早期选型与 MVP 设计快照。当前系统已经新增 PAPER-day、双轨 LLM、GUI 集成管理、Markdown 附件交付、更多官方来源和 `live-sync`；本文中的状态表与待办不再代表现状。请以 [README](../../README.md) 和 [项目交接文档](../PROJECT_HANDOFF_260814.md) 为准。

更新日期：2026-08-13

全市场三层筛选、Tavily/SearXNG 配置、搜索线索晋级和宏观融合的最新实现边界，见
[A 股全市场筛选、搜索发现与宏观融合边界](../A_SHARE_SCREENING_AND_DISCOVERY.md)。

## 1. 结论与当前边界

本阶段最可行的目标不是绕过券商接口做自动下单，而是构建一个可审计、可回放、默认拒绝不可靠输入的 A 股研究工作站：

1. AKShare 提供公开网页行情与资讯的研究入口；BaoStock 提供历史日线/EOD 交叉验证与回测底座。
2. 技术面只使用已经完成的 K 线；宏观面大模型只能读取本地构造的证据包，不能自行编造或补抓事实。
3. 技术面、宏观面和证据经过确定性门禁后，只生成 `ENTER_CANDIDATE / WATCH / REDUCE / ABSTAIN` 研究记录，不生成账户、数量、订单或券商请求。
4. NapCatQQ 只作为本机 OneBot HTTP 出站通知侧车，不接收 QQ 指令，更不能从聊天触发交易。
5. GUI 的“资讯 / 建议”页签当前是只读状态面板；在真实任务调度接入前，所有字段明确显示“尚未运行”或“未配置”。

必须特别澄清：AKShare 的网页快照、网页成交明细、1/5 分钟 K 线不是交易所原生逐笔行情。当前系统不拥有交易所序列号、逐笔委托、逐笔撤单、完整盘口深度或确定的端到端时延，因此不能据此做订单簿重建、微秒/毫秒级高频交易或成交质量证明。

## 2. 开源项目调研与取舍

下面的判断来自截至更新日期可见的官方文档、GitHub 仓库、许可证和本仓库的适配实验。它是工程选型记录，不等于对第三方项目安全性或收益能力的背书。

### 2.1 AKShare

来源：[AKShare GitHub](https://github.com/akfamily/akshare)、[官方介绍](https://github.com/akfamily/akshare/blob/main/docs/introduction.md)、[股票数据接口文档](https://github.com/akfamily/akshare/blob/main/docs/data/stock/stock.md)

可借鉴：

- Python 接口覆盖股票、基金、期货、期权、宏观和资讯，适合快速建立广覆盖研究入口。
- 返回 `DataFrame`，便于字段校验、归一化和落盘。
- `stock_zh_a_hist_min_em` 等接口可提供网页数据源聚合的分钟 K 线；个股新闻和多家财经快讯接口适合做“资讯线索源”。

不足与风险：

- 它是公开财经网页数据的采集/清洗层，不是交易所行情授权、行情专线或带 SLA 的商业 feed。
- 上游页面、字段名、单位和反爬策略都可能变化；“可调用”不代表长期稳定。
- 部分快照没有可验证的行情生成时间。HTTP 刚刚返回，只能证明“刚刚抓到”，不能证明价格刚刚产生。
- 网页“分笔/成交明细”通常缺少交易所序列号、撤单事件和全量盘口，不能称为交易所 tick。

本仓库决定：优先采用，但所有记录必须携带来源语义、抓取时间、上游时间、新鲜度、降级标记和警告；策略不能直接消费裸 `DataFrame`。

### 2.2 BaoStock

来源：[BaoStock 官网](https://www.baostock.com/)、[PyPI 包](https://pypi.org/project/baostock/)

可借鉴：

- 无需个人券商量化权限即可建立会话并查询 A 股历史行情、交易状态和部分基本面数据。
- 日线适合周度/波段策略、回测基准和 AKShare 日终数据交叉检查。
- 接口字段相对窄，易于做强类型适配和固定回归样本。

不足与风险：

- 主要价值在历史/EOD；不是低延迟盘中行情源，也不是逐笔数据源。
- 返回值以字符串和空值为主，需要显式检查错误码、停牌状态、复权参数、价格和成交量单位。
- 当日或分钟数据存在入库时间差，不能把“今天可查询”假定为“盘中可用”。

本仓库决定：作为历史日线与回测底座。停牌记录不前向填充，复权必须显式选择，失败必须结束会话并返回稳定错误。

### 2.3 Microsoft Qlib

来源：[Qlib GitHub](https://github.com/microsoft/qlib)、[官方文档目录](https://github.com/microsoft/qlib/blob/main/docs/index.rst)、[论文](https://arxiv.org/abs/2009.11189)

可借鉴：

- 数据层、特征、模型、组合、执行和实验工作流分层清楚。
- Point-in-Time 数据、离线/在线数据模式、可复现实验记录非常适合后续因子研究。
- Alpha158/Alpha360、滚动训练、模型比较和回测报告可作为中长期 ML 研究参考。

不足与风险：

- 对当前单机资讯 MVP 偏重，引入完整 Qlib 会增加数据转换、缓存格式、训练依赖和运维面。
- Qlib 不会替用户解决 A 股实时数据授权或券商交易准入；示例数据也不能自动满足实盘数据质量要求。
- 模型库丰富不代表模型在新的时间段、费用假设和 A 股交易规则下仍有效。

本仓库决定：首版不把 Qlib 作为运行时核心，但吸收其 Point-in-Time、实验版本化和数据/模型解耦原则。历史横截面因子阶段再评估独立 Qlib 研究子环境。

### 2.4 FinGPT

来源：[FinGPT GitHub](https://github.com/AI4Finance-Foundation/FinGPT)、[项目文档](https://fingpt.io/docs)、[论文](https://arxiv.org/abs/2306.06031)

可借鉴：

- 将新闻情绪、财报摘要、金融问答、预测和评测拆成任务，而不是用一个自由对话提示词包办一切。
- 数据整理、指令微调、RAG 和 benchmark 的思路适合建立 A 股宏观/事件分析评测集。
- 开放模型与托管 API 可以作为将来的成本、延迟和隐私对照组。

不足与风险：

- 训练语料、标签和演示更多面向英语/美股任务，不能直接推断 A 股事件效果。
- 自然语言情绪或“涨跌预测”不是经过成本、滑点和时间泄漏检验的交易策略。
- 本地部署金融模型会增加 GPU、模型版本、量化精度和安全更新负担。

本仓库决定：借鉴“受限任务 + 证据引用 + 单独评测”，不在首版引入其训练栈，也不把 LLM 输出直接映射为委托。

### 2.5 RQAlpha

来源：[RQAlpha GitHub](https://github.com/ricequant/rqalpha)、[许可证](https://github.com/ricequant/rqalpha/blob/master/LICENSE)

可借鉴：

- `Mod` 扩展点和账户、分析、风控、调度、模拟撮合、费用模块的职责划分成熟。
- 事件驱动回放、定时器和一致的回测/模拟接口值得用于后续 PAPER 引擎设计。
- 将交易成本、风险检查和分析报告做成独立模块，避免策略脚本内散落副作用。

不足与风险：

- 行情和真实执行仍依赖外部数据/交易接入，框架本身不提供券商权限。
- 当前许可证明确区分非商业与商业用途；即使当前个人自用，也不宜在未重新审查许可证时复制大段实现。
- 整体嵌入会和本仓库已有领域模型、PaperBroker、风控与 GUI 形成两套运行时。

本仓库决定：参考事件和 Mod 思想，不整体嵌入；如未来复用代码，先做许可证和边界审查。

### 2.6 VeighNa / vn.py

来源：[VeighNa GitHub](https://github.com/vnpy/vnpy)、[官方文档](https://www.vnpy.com/docs/cn/index.html)、[许可证](https://github.com/vnpy/vnpy/blob/master/LICENSE)

可借鉴：

- `gateway`、事件引擎、主引擎、策略应用、算法单和本地仿真的边界清晰。
- 对订单生命周期、异步回报、账户/持仓/成交事件的建模比“策略直接调用券商函数”稳健。
- PySide/Qt 桌面和多市场插件生态可为未来交易终端提供结构参考。

不足与风险：

- A 股 XTP、TORA、迅投等 gateway 仍要求对应机构、账户、客户端和权限；开源适配器不能消除准入流程。
- 全量框架和插件较重，且 C++ API、Windows DLL、插件版本之间存在兼容矩阵。
- 当前 MVP 不需要 CTA、期权、算法单等完整交易平台能力。

本仓库决定：保留自己的轻量 ports/adapters 架构；将来取得正式券商权限时，可评估把某个 VeighNa gateway 放在独立进程，通过窄协议接入，而不是让研究 GUI 直接持有交易对象。

### 2.7 Qbot

来源：[Qbot GitHub](https://github.com/UFund-Me/Qbot)、[许可证](https://github.com/UFund-Me/Qbot/blob/main/LICENSE)

可借鉴：

- 把数据、策略、回测、模拟、通知和 GUI 组合为完整个人投研产品，功能地图与本项目长期目标接近。
- 策略池和多通知渠道适合作为需求清单，尤其是研究、回测、可视化、告警分离。
- 将 Qlib、Backtrader、VeighNa 等作为可替换能力，而非自行重写所有算法，方向正确。

不足与风险：

- 仓库跨度很大，README 自述的部分环境仍以 Python 3.8/3.9 为主；直接安装会引入大量版本和平台耦合。
- 展示了很多策略不等于每个策略都具备无未来函数、费用完整、样本外稳定和实盘验收证据。
- 开源版、专业版和外部平台能力边界需要逐项核对，不能依据功能表假定代码、数据和实盘接口全部可用。

本仓库决定：学习产品信息架构和通知体验，不复制其大一统运行时；每个能力必须有窄接口、离线测试和独立验收。

### 2.8 NapCatQQ

来源：[NapCatQQ GitHub](https://github.com/NapNeko/NapCatQQ)、[OneBot 11 实现代码](https://github.com/NapNeko/NapCatQQ/tree/main/packages/napcat-onebot)、[许可证](https://github.com/NapNeko/NapCatQQ/blob/main/LICENSE)

可借鉴：

- 可把 NTQQ 侧能力暴露为 OneBot 11 HTTP 接口，Python 程序无需控制 QQ 界面。
- 私聊/群聊消息 API 简单，适合做本机告警侧车。
- 相比键鼠自动化，HTTP 状态码和响应体更容易做超时、重试、幂等和审计。

不足与风险：

- 它不是腾讯官方程序化接口；QQ/NTQQ 更新可能导致兼容中断、重新登录或账号风控。
- 运行一个非官方协议侧框架增加本机供应链、会话和端口暴露风险。
- 许可证是 NapCat 自定义的有限再分发/非商业许可证，不应把其源码打包进本项目或修改后公开分发。

本仓库决定：只使用用户自行安装的官方 NapCatQQ 发布物；本项目仅实现 OneBot 客户端。端点必须是 loopback，必须有 access token 和目标白名单，只发送纯文本段，不接收事件或命令。

### 2.9 近期 A 股 / LLM 项目

#### TradingAgents 与 TradingAgents-CN

来源：[TradingAgents](https://github.com/TauricResearch/TradingAgents)、[TradingAgents 论文](https://arxiv.org/abs/2412.20138)、[TradingAgents-CN](https://github.com/hsliuping/TradingAgents-CN)

可借鉴的是角色分工：技术、基本面、新闻、看多/看空、风险和组合角色分别产出结构化中间结果，再由门禁汇总；中文版本的数据源抽象、多 LLM provider、报告导出和分析进度 UI 也值得参考。

不能直接采用的部分是“多个 Agent 讨论后即得到可信买卖结论”。多个角色可能共享同一错误数据、同一模型偏差和同一提示上下文，文字更长并不会自动提高校准度。TradingAgents-CN 当前还包含 FastAPI、Vue、MongoDB、Redis、Docker 和混合许可证边界，对本机 MVP 过重。因此本仓库只保留证据化角色输出，不引入自动辩论链，也不复制其专有目录。

#### TradingAgents-A 股版

来源：[michaelyuancb/tradingagent_a](https://github.com/michaelyuancb/tradingagent_a)

该项目聚焦 A 股语境，默认使用 AKShare，并把分析师、研究员、交易员、风控和组合管理拆开。它适合检查 A 股提示词、CLI 和工具边界，但仓库说明不能替代 Point-in-Time 数据审计、长时间采集可用率、样本外回测和安全审计。本仓库只吸收其“研究流程可解释、模型 provider 可替换”的思想。

#### A-share MCP 工具层

来源：[24mlight/a-share-mcp-is-just-i-need](https://github.com/24mlight/a-share-mcp-is-just-i-need)

该项目把 BaoStock 的历史、财务、指数和宏观查询包装成 MCP 工具，说明“让 LLM 调用窄数据工具”比把数据库直接暴露给 Agent 更易理解。不过，工具返回仍必须经过时间戳、内容哈希、字段验证和证据 ID 归档；MCP 本身不会保证数据新鲜度或分析正确性。本仓库暂不需要 MCP 运行时，已有 Python ports 可以提供更窄的内部边界。

#### 对 LLM 收益能力的现实约束

[StockBench](https://arxiv.org/abs/2510.02209) 的多月连续交易评测显示，静态金融问答能力不能直接转化为交易能力，许多被测 LLM Agent 难以稳定超过简单持有基线。因此本项目把 LLM 定位为“宏观/事件证据整理器和反方检查器”，技术信号、数据新鲜度、证据存在性、TTL 和发布决定都由确定性代码控制。

### 2.10 daily_stock_analysis（重点代码审阅）

来源：[项目仓库与 README](https://github.com/ZhuLinsen/daily_stock_analysis)、[许可证](https://github.com/ZhuLinsen/daily_stock_analysis/blob/main/LICENSE)、[行情实现](https://github.com/ZhuLinsen/daily_stock_analysis/blob/main/data_provider/akshare_fetcher.py)、[新闻搜索实现](https://github.com/ZhuLinsen/daily_stock_analysis/blob/main/src/search_service.py)、[技术评分实现](https://github.com/ZhuLinsen/daily_stock_analysis/blob/main/src/stock_analyzer.py)、[LLM 分析实现](https://github.com/ZhuLinsen/daily_stock_analysis/blob/main/src/analyzer.py)、[回测实现](https://github.com/ZhuLinsen/daily_stock_analysis/blob/main/src/core/backtest_engine.py)、[本地调度器](https://github.com/ZhuLinsen/daily_stock_analysis/blob/main/src/scheduler.py)、[GitHub Actions](https://github.com/ZhuLinsen/daily_stock_analysis/blob/main/.github/workflows/00-daily-analysis.yml)、[通知派发](https://github.com/ZhuLinsen/daily_stock_analysis/blob/main/src/notification.py)、[通知降噪](https://github.com/ZhuLinsen/daily_stock_analysis/blob/main/src/notification_noise.py)

这是与本项目目标最接近的完整参考：它把多市场行情、搜索资讯、技术分析、LLM 决策报告、Web 工作台、事后验证、定时执行和多渠道推送连成了产品闭环。以下结论来自截至 2026-08-13 对 `main` 分支实际代码的审阅；分支会继续变化，不能只凭 README 的功能名称推断运行语义。

#### 实际行情与新闻来源

| 链路 | 代码中的实际行为 | 本仓库的判断 |
|---|---|---|
| A 股历史行情 | `AkshareFetcher` 依次尝试 AKShare 的东方财富 `stock_zh_a_hist`、新浪 `stock_zh_a_daily`、腾讯 `stock_zh_a_hist_tx`，三条路径均使用前复权 `qfq`。仓库还安装/支持 efinance、Tushare、Pytdx、BaoStock、TickFlow；YFinance、Longbridge 主要补充其他市场。 | 多源回退和统一列名值得借鉴，但回退后的 `provider`、复权口径、单位和异常必须进入逐条 provenance，不能只返回一张看似同构的表。 |
| A 股网页快照 | Actions 的默认优先级是 `tencent,akshare_sina,efinance,akshare_em`；AKShare 全市场股票/ETF 快照缓存 TTL 在代码中明确为 1,200 秒。 | 这适合日终报告和低频观察，不适合声称“实时”或触发日内快线。20 分钟缓存、腾讯/新浪网页接口和 AKShare 聚合都不是交易所 tick。 |
| 稳定性措施 | 有 2–5 秒随机限频、tenacity 退避、超时子进程和熔断器。需要注意，`_set_random_user_agent()` 在所审阅代码中只是选择并记录 UA，没有把它写入 AKShare session/header；直连新浪/腾讯的请求才显式带 header。 | 借鉴超时、退避和熔断；不把“随机 UA”当可用性保证，也不依赖规避上游限制来满足 SLA。每个源应有连续成功率、延迟和字段漂移指标。 |
| 新闻/公告线索 | 核心不是 AKShare 新闻接口或交易所公告 feed，而是 `SearchService` 查询 Anspire、Bocha、Tavily、Brave、SerpAPI、MiniMax、SearXNG。查询词覆盖最新消息、公告、机构分析、风险、业绩和行业；“上交所/深交所/cninfo”只是公告搜索词，不等于已经直连这些一手来源。 | 搜索 API 适合发现线索，关键事实仍要回链上交所、深交所、巨潮、监管机构或公司原文。 |
| 新闻时效 | 策略档位把 ultra-short/short/medium/long 映射为约 1/3/7/30 天并受最大天数限制；搜索缓存为进程内 10 分钟。严格维度会过滤日期未知、过旧和未来结果，但日期归一化最终只保留 `date`，分钟/小时级发布时间会丢失；部分分析维度允许 180 天窗口并保留未知日期。 | 可以借鉴“策略决定证据窗口”和未来日期过滤，但本仓库必须保留带时区的原始 `published_at`、`first_seen_at` 和 `available_at`，并让未知时间直接降低推荐等级。 |

`format_intel_report()` 会把标题、日期、摘要、搜索 provider 和关联度送入 LLM，但该格式化路径没有保留每条结果的原始 URL。也就是说，报告看起来有“来源”，模型结论却未必能逐条回溯到原文。这是本仓库不能复制的部分：EvidencePack 中每条输入都必须有稳定 `evidence_id`、canonical URL、内容哈希和首次观察时间，LLM 的每个事实 claim 必须引用这些 ID。

#### 技术策略与 LLM 评分并不是同一个分数

代码中实际存在两套 0–100 评分，展示时容易被混为一谈：

1. `signal_score` 是确定性技术分：趋势 30 分、相对 MA5 的乖离率 20 分、量能 15 分、MA5/MA10 支撑 10 分、MACD 15 分、RSI 10 分；随后再结合趋势状态映射成强买、买、等待或卖。它可审计、易单测，是值得借鉴的“规则特征层”。
2. `sentiment_score` 是 LLM 在阅读技术表格和新闻文本后生成的综合分，模型同时输出 `buy/hold/sell`、置信度、买卖点、止损和目标价。这个数是模型主观量表，不是根据历史频率校准出的上涨概率，也不能和技术分直接比较。

有价值的防线包括：提示词要求新闻日期、缺失数据不得编造、技术面矛盾要显式说明；部分确定性后处理还会在资金流不可用等情况下把买入降为观察，并约束分数区间。但输出契约仍偏宽松：完整 Pydantic schema 校验失败时只记录 warning 并继续，只要 JSON 含若干“最小字段”之一即可被接受；JSON 解析失败又会退化为正负关键词计数，甚至生成 `buy` 或 `sell`。此外，搜索文本直接嵌入 prompt，未形成逐条证据引用和不可信内容隔离。

本仓库的取舍是：借鉴其确定性指标拆分和“负面信息可降级”思想，不照搬固定权重、诸如“缩量回调即主力洗盘”的因果措辞或 LLM 自报分数。严格 schema、证据引用或时间校验任一失败都必须 `ABSTAIN`，不能用关键词猜测补出交易方向；新闻正文中的提示注入文本必须始终视为数据。

#### 回测与风险边界

该项目已经提供有用的“历史建议事后验证”：`BacktestEngine` 是 long-only 日线引擎，把自然语言建议映射为 long/cash 和 up/down/not_down/flat，读取未来若干日 OHLC，统计方向正确率、胜率、收益及止盈止损命中。同一根日 K 同时触及止损和止盈时会标记 `ambiguous` 并保守假定止损先发生，这一显式不确定性处理值得借鉴。

但它更接近对历史 LLM 报告的 outcome evaluation，而不是足以证明可交易性的组合回测。在所审阅纯引擎中没有显式佣金最低收费、滑点、印花税、T+1、涨跌停、停牌、成交容量、现金/仓位曲线、基准比较或 walk-forward 训练/验证；自然语言到仓位的关键词映射也会引入标签误差。技术规则权重与 LLM 分数尚不能因为存在该页面就视为完成样本外校准。

因此，本仓库会保留“推荐生成后的事后追踪”，但策略验收仍执行第 6.3 节的 Point-in-Time、成本、A 股规则、基线、滚动样本外和消融门槛。建议中给出止损价不等于实现了风险管理；没有新鲜度门禁、组合暴露约束和到期机制时，精确到分的点位反而会制造虚假精度。

#### 调度、推送与密钥

- GitHub Actions 默认工作日北京时间 18:00 运行，带 0–60 秒随机启动延迟、单任务并发锁、总超时、应用内交易日检查，并把报告和日志保存为 30 天 artifact。它是低成本日终方案，不是准点保证，也不能承担盘中实时通知；GitHub 托管 runner 还意味着行情、新闻、模型输入和日志离开本机。
- 本地 `schedule` 调度器支持每天多个 `HH:MM` 和后台周期任务，但主循环 30 秒轮询，小于 30 秒的周期会被钳制；任务状态、错过执行和互斥信息都只在内存中，进程重启后没有持久 lease、misfire 补偿或多实例领导者选举。
- 通知层覆盖企业微信、钉钉、飞书、Telegram、邮件、Discord、Slack、ntfy、Gotify、PushPlus、Server 酱、Pushover、自定义 webhook、AstrBot 等，并返回每渠道成功、失败、可重试和耗时，支持路由、静默时段、最低等级、去重和冷却。这些诊断结构值得借鉴。
- 统一派发的实际代码按渠道顺序循环；整体语义是“至少一个渠道成功即成功”。降噪模块明确声明只保存在进程内、无文件锁和跨 worker 协调。所审阅统一层没有持久 outbox、跨重启重投、到期丢弃或死信队列，因此部分失败可能只停留在一次运行的诊断里。单个 sender 即使有自己的超时/重试，也不能替代端到端可恢复投递。
- 它没有直接的 NapCatQQ/OneBot 适配。AstrBot 或自定义 webhook 不能被默认等同为 QQ 可靠投递；本仓库仍使用独立的 loopback OneBot 客户端和 SQLite outbox。
- 本地快速开始使用 `.env`，云端使用 GitHub Secrets/Variables；Actions 会把许多模型、搜索、行情和通知凭据同时注入一个分析 job，并可能上传含 prompt/错误诊断的日志。做个人单机系统时，应缩小每个进程的密钥集合，使用系统凭据库，日志统一脱敏，不把 `.env`、token 或完整 provider 响应归档。

#### 最终取舍

| 直接借鉴 | 必须重做或强化 |
|---|---|
| 多 provider 窄适配器、显式回退顺序、限频/退避/超时/熔断 | 每条记录保存真实 provider、源时间、抓取时间、语义、复权与降级状态 |
| 按交易周期决定新闻窗口、过滤过旧/未来结果、按维度组织情报 | 一手公告源直连；保存原文与 URL；分钟级时间；EvidencePack 与 claim 级证据 ID |
| 技术指标拆成可审计分项、缺失信息降级、冲突显式展示 | 权重必须样本外验证；LLM 分数不得冒充概率；严格 schema 失败即 `ABSTAIN` |
| 日终 Actions 与本地调度两种部署选择、每渠道派发诊断 | 交易日历感知、休眠恢复、持久 job lease；SQLite outbox、幂等、重试、TTL、死信 |
| 历史建议 outcome tracking、同日止盈止损歧义显式化 | 完整费用/A 股规则/组合回测、基准和 walk-forward；推荐账本与模型/证据版本 |

该仓库采用 MIT 许可证，可以作为工程参考；但其运行时范围远大于当前 MVP，直接整体嵌入会引入 Web、Bot、多市场、多模型和大量依赖。当前决定是不新增对它的运行时依赖，只吸收经过上述边界修正的设计，并用本仓库现有 ports/adapters、严格门禁和持久通知链实现。

本次 `ashare-close-research-once` 已落实其中三个优点：显式区分 postmarket、
区分 ETF/股票数据语义、技术指标先确定性计算再由 LLM 解释。没有照搬其历史数据
缺失时仍继续生成操作意见、固定 100 分权重、将“缩量”叙述为“主力洗盘”、单条
新闻优先返回、通配异常返回 `None` 或按本机工作日猜测交易日。当前实现改用真实
交易日历、typed failure code、不可变日线证据、严格 `ABSTAIN`、多源相关度过滤和
durable QQ outbox。

## 3. 本仓库的目标架构

数据和控制流如下：

```text
AKShare / BaoStock / 公开资讯
        │
        ▼
adapters + ingest（重试、限频、字段校验、来源语义）
        │
        ├──► raw archive（原文/原表、SHA-256、抓取与首次观察时间）
        ▼
canonical market data + normalized events
        │
        ├──► deterministic technical features
        └──► bounded EvidencePack ──► LLM macro/event analysis
                                      （严格 JSON、逐项 evidence_id）
        │
        ▼
deterministic recommendation gate
        │
        ├──► immutable research recommendation + expiry
        ├──► GUI 只读展示
        └──► SQLite outbox ──► loopback OneBot ──► NapCatQQ

不存在 recommendation ──► broker/order 的边。
```

### 3.1 已落地模块

| 层 | 当前实现 | 关键边界 |
|---|---|---|
| 行情端口 | `ports/market_data.py` | 强制携带 `SourceSemantics`、`FreshnessStatus` 和 provenance |
| AKShare 行情 | `adapters/akshare.py` | 快照、网页成交明细、完成的 1/5 分钟 K 线；重试、缓存、超时、降级标记 |
| BaoStock 日线 | `adapters/baostock.py` | 登录/登出、日线、复权参数、超时与重试；不静默填充停牌 |
| 资讯采集 | `ingest/http.py`、`ingest/rss.py`、`ingest/html.py`、`ingest/akshare_news.py`、`ingest/akshare_disclosures.py` | 媒体线索与巨潮公告；源白名单、条件请求、大小/类型限制、退避；401/403 不规避 |
| 源健康 | `storage/source_health.py` | append-only 运行观测；成功/失败/降级/陈旧率、p50/p95 延迟和最近故障，用于 20 日 soak |
| 事件流水线 | `pipeline/normalize.py`、`pipeline/dedupe.py` | URL 规范化、稳定 ID、重复/修订区分 |
| 证据与研究存储 | `storage/raw_store.py`、`storage/event_store.py`、`storage/research_store.py` | 内容寻址原始档案、SQLite 事件与游标、不可变推荐、追加式结果观察 |
| 技术研究 | `features/technical.py` | 仅完成 K 线；MA、突破、量能、ATR；过期/不足数据自动 `ABSTAIN` |
| 宏观分析 | `adapters/llm/openai_responses.py`、`services/macro_research.py` | Point-in-Time EvidencePack、Responses API、严格 JSON Schema、`store=false`、无外部工具、证据引用验证；失败即 `ABSTAIN` |
| 发布门禁 | `policy/recommendation_gate.py` | 无证据不发布入场候选；负面宏观可降级；结论有 TTL |
| 研究编排 | `services/ashare_research.py`、`services/research_watch.py` | 完成 K 线、证据和可选宏观分析到推荐；一次运行或有界多标的周期；单股失败隔离；无 broker/account/order 依赖 |
| 推荐评估 | `backtest/recommendation_outcomes.py` | 从下一可交易日开盘开始评估，区分 pending/unevaluable，计入佣金、印花税和滑点；不把同日未来收盘价当入场价 |
| 通知 | `adapters/notifiers/onebot.py`、`storage/execution/outbox.py`、`services/notification_dispatch.py` | loopback、token、目标白名单、纯文本、幂等、租约、退避、TTL、死信和有限轮询派发 |
| GUI | `gui/main_window.py` | PAPER 壳与只读“资讯 / 建议”页；尚未绑定后台调度器 |

### 3.2 数据语义合同

| 语义值 | 实际含义 | 允许用途 | 禁止用途 |
|---|---|---|---|
| `PUBLIC_WEB_QUOTE_SNAPSHOT` | 某公开网页/聚合接口在抓取时返回的单标的快照 | GUI、研究状态、粗粒度价格参考 | 证明交易所当前价、低延迟触发、成交保证 |
| `PUBLIC_WEB_TIME_AND_SALES` | 网页展示的成交明细/分笔表 | 研究成交方向和活跃度、人工核验 | 称为交易所逐笔、订单簿重建、撤单/队列分析 |
| `AGGREGATED_MINUTE_BAR` | 数据提供方聚合的 1/5 分钟 OHLCV | 完成 K 线技术指标、盘中研究 | 未完成 bar 决策、tick 策略、撮合时延推断 |
| `PROVIDER_HISTORICAL_DAILY_BAR` | BaoStock 等提供方的历史日线/EOD | 波段/周度回测、日终交叉验证 | 盘中信号和实时交易 |

`fetched_at` 是本进程取得记录的时间，`provider_timestamp` 是上游标注时间，`first_seen_at` 是本系统首次观察到资讯的时间。三者不能互换。上游没有可靠时间戳时，新鲜度必须是 `UNKNOWN`；缓存回退必须标记 `degraded=True` 或 `STALE`，不能伪装成当前行情。

### 3.3 当前资讯源与扩展顺序

当前代码通过 AKShare 支持：

- 巨潮资讯指定 A 股公告；
- 东方财富个股新闻；
- 东方财富全市场快讯；
- 财联社快讯；
- 新浪财经 7×24；
- 同花顺实时新闻。

巨潮公告属于一手披露入口，其余接口主要是“发现线索”的聚合/媒体层。下一步继续增加一手宏观与监管来源，并保留媒体层用于时效与交叉验证：

当前默认链路还直接接入四个隔离的一手栏目：国家统计局数据发布、中国人民银行公开
市场公告、中国证监会政策解读和美联储货币政策 RSS。每个来源使用独立 HTTPS 主机
白名单、有限超时和单次尝试；某一来源失败只产生来源级诊断，不中断其余资讯。筛选同时
要求 `available_at <= as_of`，并按原始 `published_at` 检查 14 日内容时效，避免把今天
首次抓到的旧公告误当成近期事件。无关公司的股价异动、业绩预告和协议转让等个股快讯
不会作为宽基 ETF 的宏观证据。

1. 交易所与公告：[上交所披露](https://www.sse.com.cn/disclosure/listedinfo/announcement/)、[深交所公告](https://www.szse.cn/disclosure/listed/notice/)、[巨潮资讯](https://www.cninfo.com.cn/new/index)。
2. 监管与政策：[中国证监会](https://www.csrc.gov.cn/)、[中国人民银行](https://www.pbc.gov.cn/)、[国家统计局](https://www.stats.gov.cn/)、[财政部](https://www.mof.gov.cn/)、[国家发改委](https://www.ndrc.gov.cn/)。
3. 公司官网与投资者关系页：只针对自选股配置，避免全网无界抓取。
4. 主流财经媒体与聚合快讯：用于发现事件，关键事实应回链到公告、监管文件或公司原文。

提高可用性的手段是源隔离、有限并发、超时、指数退避、`Retry-After`、ETag/Last-Modified、内容哈希、游标、字段漂移测试和多源回退。验证码、登录墙、付费墙或 401/403 不通过规避技术继续抓取；发生时记录源状态并切换到合法可访问的替代源。

## 4. 收盘多因子方法

本节定义盘后日线研究的当前方法、后续周线目标和验收边界。它是可复现的研究规范，
不是收益声明，也不表示下列候选因子已经在 A 股样本外获得统计显著性。新增指标
的目的应是增加相互独立的证据维度，而不是把同一价格趋势换成多个名称后重复计分。

### 4.1 当前实现及其解释边界

当前 `features/close_analysis.py` 的 `close-multifactor@2` 已实现以下确定性盘后能力：

- 使用真实交易日历明确最新完成交易日和下一交易日，不按自然工作日猜测；
- 仅消费已完成且严格按日期排序的未复权日线；BaoStock 覆盖不足时可切换至
  AKShare 独立日线源，只有在至少 20 个重叠交易日的 OHLC 完全一致后才允许用
  较新来源补齐滞后一日的尾部；拒绝未来、重复、乱序和未经核验的静默拼接；
- 区分股票与 ETF，检查停牌、ST、数据缺口和未复权价格不连续；
- 至少使用 201 个完成交易日，计算 MA5/20/60/120/200、ATR 归一化趋势斜率、
  5/20/60/120（跳过最近 5 日）/200 日风险调整动量、MACD、Donchian 20/55 日
  通道、Bollinger `%b`/带宽状态、简单窗口 RSI14/ATR14、随机指标 K14、方向化
  成交量确认、上涨/下跌成交额比、Amihud 20/60 日非流动性、下行与隔夜跳空
  波动以及 60 日最大回撤；
- 以 Wilder 递推计算 ADX14、`+DI14` 和 `-DI14`，只作为趋势强度诊断，不进入
  方向家族分；简单窗口 ATR14/RSI14 仍按原名称和公式明确展示；
- 将方向证据封装为趋势结构、多周期动量、突破与波动释放、顺势回撤、量价确认
  五个有上限的家族；相对强弱与市场广度家族在缺少可审计成分/基准数据时显式为
  零权重缺口，不用中性值伪装覆盖；
- 把 ATR 占比、最大回撤和跳空波动作为风险门禁，风险过高只阻止入选或降低资格，
  不把负方向翻转成正方向；
- 生成 `ENTER_CANDIDATE / WATCH / REDUCE / ABSTAIN`，保留指标、理由码、策略
  版本、目标交易日和不可变行情证据；
- 在确定性技术判断完成后，才允许受限 LLM 阅读行情证据和筛选后的新闻证据；
- 研究记录和 QQ 通知不包含 broker、account 或 order 调用。

这些能力构成可审计基线，但趋势、动量、突破和振荡指标仍大量来自同一 OHLCV 序列，
相关家族不能视为完全独立确认。MACD 只在动量家族计分，换手和 Amihud 只进入风险
诊断；当前 ATR/RSI 采用窗口简单平均，报告会明确标注而不冒充 Wilder 递推版本；
只有 ADX/+DI/-DI 使用 Wilder 递推。
家族固定权重尚未经过 A 股 walk-forward 校准，相对强弱、指数成分广度、ETF 份额和
NAV 折溢价仍在建设中。当前已能计算九组指数的 PIT 对齐历史相关、EWMA 相关和单因子
beta/t 值，但它们只作为描述性证据，不进入方向分，也不声称因果或预测能力；因此置信度
继续保持 `UNCALIBRATED`。

### 4.2 双周期输出和六个信号家族

盘后报告已经输出两个诊断周期，不再用一个分数解释短线和波段用途：

- 短线：下一交易日至 5 个交易日；
- 波段：2 至 8 周；当前复用同一次日线指标计算中的 60/120/200 日结构与长周期动量，
  尚未实现独立周 K 聚合和已结束周确认，报告会明确提示这一边界。

每个信号家族独立返回 `score [-1, 1]`、指标名、中文解释和加权贡献。两个
`CloseHorizonView` 只复用这些家族分并按周期重新加权，不重复计算指标；各视角保存
分数、家族贡献、覆盖率和用途说明。缺失或零权重家族先剔除，再在已覆盖家族中归一化。
两个视角分都不是概率，也不会各自生成一套可执行结论；原有确定性决策仍只有一套。

| 信号家族 | 候选确定性指标 | A 股 / ETF 适配 | 评分边界 |
|---|---|---|---|
| 趋势结构 | 收盘相对 SMA20/60/120/200、均线排列、MA20/60 的简单 ATR 归一化斜率；Wilder ADX/+DI/-DI 另作强度诊断 | 当前使用截至时点可知的单标的未复权日线；复权总回报序列和独立周 K 尚未接入 | MACD 不在此家族重复计分；ADX/+DI/-DI 完全不进入方向分 |
| 多周期动量 | 5/20/60 日风险调整收益、120 日动量跳过最近 5 日、200 日收益、MACD 柱 | A 股周/月个股动量证据不如成熟市场一致；涨跌停日仍需单独可成交性数据 | 时间序列动量与横截面排名分开；没有完整同时点股票池时不得声称横截面动量 |
| 突破与压缩释放 | Donchian 20/55 日通道位置、Bollinger BandWidth 相对状态及方向化释放 | 当前尚未实现连续多日确认、完整涨跌停与一字板可成交性门禁 | 触及上轨或接近新高不自动等于买入；释放必须与通道方向一致 |
| 均值回归与入场位置 | 相对 MA20/简单 ATR 的距离、简单窗口 RSI14、随机 K14、Bollinger `%b` | 只在日线中期上升趋势中把超卖解释为顺势回撤；T+1 会限制快速反转交易 | 下跌趋势中的低 RSI 不会反向升级为看多分 |
| 量价与流动性 | 20 日量比、上涨/下跌成交额比和末日收益方向；换手状态和 Amihud 另作风险诊断 | 股票使用成交额和可用换手；ETF 份额、NAV 折溢价和申赎状态尚未接入 | 量能用于确认价格方向；换手和 Amihud 不制造方向 |
| 相对强弱与市场广度 | 标的相对沪深 300/中证全指/行业指数的 5/20/60 日超额收益，行业分位，成分股上涨率、均线上方比例、新高新低、上涨成交额占比 | 股票需历史行业归属；宽基 ETF 应分析跟踪指数成分股广度、行业权重和集中度 | 无历史成分和当日覆盖率时不得回填今天的股票池；覆盖不足必须降级或 `ABSTAIN` |

下列初始权重只用于形成不依赖收益调参的工程基线，不是经验最优参数：

| 信号家族 | 短线初始权重 | 波段初始权重 |
|---|---:|---:|
| 趋势结构 | 15% | 35% |
| 多周期动量 | 20% | 30% |
| 突破与压缩释放 | 25% | 20% |
| 均值回归与入场位置 | 20% | 5% |
| 量价与流动性 | 20% | 10% |
| 相对强弱与市场广度 | 当前零权重，不纳入视角 | 当前零权重，不纳入视角 |

表中五个已实现方向家族各周期合计为 100%。若其中某个家族因数据缺失或配置为零权重，
`coverage` 保留缺失前的覆盖比例，剩余家族权重归一化后再计算诊断分。

后续如果样本外结果不支持某个家族，应删除或降低其作用，而不是继续增加条件来保护
历史表现。

### 4.3 风险覆盖层与决策门禁

波动、流动性和事件风险首先决定“是否有资格给方向”和“最多承担多少风险”，而不是
独立的看多因子。建议的数据结构把三个量完全分开：

```text
directional_score = Σ family_weight × family_score
confidence = 数据覆盖率 × 家族一致性 × 证据新鲜度 × 校准状态
risk_multiplier = 波动、流动性、回撤、相关性和事件风险的上限函数
```

风险覆盖候选包括：Yang-Zhang 20/60 日波动、ATR14 百分比、隔夜跳空波动、下行
波动、60/252 日最大回撤、Expected Shortfall、20/120 日波动状态比、相对基准 beta、
相关性和跟踪误差。高波动可以降低置信度和建议风险预算，但不能把负趋势改成正趋势。

建议初始门禁为：

- 复权链、交易日历、行情、元数据或市场覆盖不完整时 `ABSTAIN`；
- `ENTER_CANDIDATE` 需要方向家族一致、趋势为正、数据覆盖充分、无停牌/ST/不可成交
  等硬风险；具体数值阈值在完成样本外校准前只作为版本化研究参数；
- 已持有标的出现通道破位或中期趋势失效时可生成 `REDUCE`；
- 其余冲突、临近突破但未确认、量能不足或风险状态过高的情况保持 `WATCH`；
- LLM 的宏观影响分不能越过确定性数据和技术门禁。模型负责证据归纳、情景和反证，
  不负责生成技术分、概率、仓位或订单。

### 4.4 A 股交易制度和数据处理

所有回放和报告必须显式处理下列制度差异：

- A 股股票和境内股票 ETF 的 T+1；同日新买仓位不能假设可按收盘信号卖出；
- 主板、科创板、创业板、风险警示证券和新股的价格限制不同，规则应按证券和日期解析，
  不能只使用固定 9.5% 阈值；
- 涨跌停、停牌和一字板可能让理论信号无法成交，未成交必须进入结果统计；
- ST、退市整理、上市不足 250 个交易日、长期停牌和极低流动性应作为股票池门禁；
- 企业行动采用“双序列”：未复权价格用于参考价和成交，Point-in-Time 复权/总回报
  序列用于收益和趋势，并归档当时可知的调整因子；
- 不能用今天的指数成分、行业分类或证券状态回填历史；股票池、成分权重和行业归属
  必须带生效日期；
- 佣金最低收费、印花税、滑点、价格冲击、整手和最小价格单位进入净值回放。

上交所公开说明了主板竞价、价格优先/时间优先和价格限制等交易机制；股票 ETF 为
T+1，最小价格变动单位为 0.001 元。实现时以适用日期的交易所正式规则为准，不能把
当前网页摘要永久硬编码。来源：[上交所交易机制](https://english.sse.com.cn/start/trading/mechanism/)、
[上交所 ETF 问答](https://www.sse.com.cn/assortment/fund/etf/question/)。

### 4.5 标的背景与 ETF 特有分析

收盘报告不能只展示证券代码。基础档案至少应包含名称、资产类型、交易所、板块、行业、
风格、角色、风险标签、上市日期和元数据来源/生效时间。静态 watchlist 可作为人工核验的
降级来源，但名称、行业和基金信息需要定期重新验证。

股票还应增加申万/中证行业、主营业务、规模层级、相对行业基准和直接公告。ETF 不宜
强行套用单一行业，至少增加基金管理人、跟踪指数、投资范围、成立/上市日期、规模、份额
变化、NAV 折溢价、跟踪误差、行业权重、前十大成分和集中度。宽基 ETF 的“市场广度”
应由其跟踪指数的历史成分计算。沪深 300 的自由流通市值和分级靠档方法以中证指数的
[官方方法文件](https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/en/000300_Index_Methodology_en.pdf)
为准。

### 4.6 当前尚缺的数据

六家族完整版不能仅依靠当前单标的 BaoStock 日线。按优先级仍缺：

1. 至少 300 个完成交易日的稳定 OHLCV/成交额/换手历史，以及 Point-in-Time 企业行动
   和复权因子；
2. 带生效日期的证券名称、板块、行业、上市状态、ST/退市/价格限制元数据；
3. 沪深 300、中证全指、行业指数和 ETF 跟踪指数的同期行情；
4. 无幸存者偏差的历史 A 股股票池、指数成分及权重，用于广度和横截面排名；
5. ETF 的历史 NAV/IOPV、份额、规模、折溢价、跟踪误差、指数行业权重和成分；
6. 已接入沪深300、恒指/国企指数、标普500、纳斯达克100、道琼斯、日经、韩国和
   台湾指数的严格映射历史；仍缺稳定的 VIX、美元指数、人民币离岸汇率、国内短端利率、
   债券、商品、股指期货/期权及 ETF 份额/NAV，并且每项仍须具有真实可用时间；
7. 逐次保存的行情、元数据和新闻证据快照，保证在线评估与历史回放使用同一输入合同。

在这些数据到位前，可以展示当前单标的技术基线和描述性跨市场关系，但不能声称已经
完成行业相对强弱、横截面选股、市场广度、ETF 资金流或跨市场预测。

### 4.7 避免相关性重复和过拟合

- 每个信号先进入家族，家族总贡献封顶；每月检查家族分数相关矩阵，长期绝对相关性高于
  0.80 的信号应合并、替换或降权；
- 初版固定常见的 5/20/60/120/200 和 20/55 日窗口，不为单只证券寻找最佳参数；
- 参数稳定性应表现为较宽的可接受区间，而不是一个尖锐的历史最优点；
- 同一确定性函数用于在线盘后评估和历史回放；决策在收盘后形成，成交不得早于下一
  可交易时点；
- 使用真实历史成分、退市股、停牌、ST、上市日和企业行动，避免幸存者偏差和未来函数；
- 对重叠持有期使用时间顺序 walk-forward 和必要的 purge/embargo，最终测试集不得参与
  阈值选择；
- 记录全部策略与参数试验，使用 White Reality Check、CSCV/PBO 或同类多重试验控制，
  不只保存表现最好的版本；
- 与持有、指数、简单均线和简单动量基线比较，并做家族消融；某家族没有稳定样本外
  增量时应移除；
- 同时报告毛收益、成本后收益、最大回撤、Expected Shortfall、换手、容量、未成交率、
  数据覆盖率和 `ABSTAIN` 比例，不选择性展示累计收益；
- 分主板/科创板/创业板、规模、行业及趋势/震荡/高波动状态报告结果；不足样本明确标记；
- 在足够样本完成概率校准前，任何 LLM 自报置信度和技术总分都不解释为上涨概率。

关于数据窥探和多重试验的主要方法参考：
[Sullivan、Timmermann 与 White](https://www.fmg.ac.uk/publications/discussion-papers/data-snooping-technical-trading-rule-performance-and-bootstrap)、
[Probability of Backtest Overfitting](https://papers.ssrn.com/sol3/Papers.cfm?abstract_id=2326253)、
[Harvey、Liu 与 Zhu](https://www.nber.org/papers/w20592)。

### 4.8 方法依据与克制性解释

| 主题 | 原始论文或官方来源 | 本仓库采用的有限结论 |
|---|---|---|
| 时间序列趋势 | [Moskowitz、Ooi、Pedersen](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2089463)；[Brock、Lakonishok、LeBaron](https://onlinelibrary.wiley.com/doi/abs/10.1111/j.1540-6261.1992.tb04681.x) | 趋势和区间突破值得作为候选基线，不代表在当前 A 股、成本和样本外仍有收益 |
| 横截面与行业动量 | [Jegadeesh、Titman](https://onlinelibrary.wiley.com/doi/abs/10.1111/j.1540-6261.1993.tb04702.x)；[Moskowitz、Grinblatt](https://onlinelibrary.wiley.com/doi/pdf/10.1111/0022-1082.00146) | 个股、行业和时间序列动量必须分开；完整股票池到位后再验证 |
| A 股日频动量 | [Gao、Jiang、Xiong、Xiong](https://www.nber.org/papers/w31839) | A 股日频存在值得研究的延续现象，但周/月结果不同，且注意力、涨跌停、T+1 和成本影响可交易性 |
| 量价与流动性 | [Lee、Swaminathan](https://onlinelibrary.wiley.com/doi/10.1111/0022-1082.00280)；[Amihud](https://www.sciencedirect.com/science/article/pii/S1386418101000246) | 成交量可帮助区分动量状态；日收益/成交额可作流动性代理，均需在 A 股重新验证 |
| 波动管理 | [Moreira、Muir](https://www.nber.org/papers/w22208)；[Yang、Zhang](https://ideas.repec.org/a/ucp/jnlbus/v73y2000i3p477-91.html) | 波动适合作为风险覆盖和仓位约束；OHLC 有助于估计隔夜与日内波动，不作为看多理由 |
| 波动压缩与突破 | [Bollinger 官方方法手册](https://www.bollingerbands.com/_files/ugd/58be43_d09c50b6e8ea4afd9af0523ef94de876.pdf) | `%b`、BandWidth 与趋势强度联合解释；不采用“碰上轨即买/卖”的简化规则 |
| 市场广度 | [Zaremba 等](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3444882) | 广度可能提供增量状态信息；本项目必须先取得可审计的历史成分和覆盖率 |
| A 股因子适配 | [Liu、Stambaugh、Yuan](https://www.nber.org/papers/w24458)；[Hanauer 等](https://www.sciencedirect.com/science/article/pii/S105752192300491X) | 不直接复制美股因子；小微壳价值、行业、规模和投资者结构需要单独控制 |
| 交易成本 | [Frazzini、Israel、Moskowitz](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2294498) | 不同策略容量和成本差异很大；本项目只接受包含 A 股实际约束的成本后评估 |
| 组合风险 | [Ledoit、Wolf](https://www.ledoit.net/Honey_2004.pdf)；[Maillard、Roncalli、Teiletche](https://www.thierry-roncalli.com/download/erc.pdf) | 如后续形成多标的组合，收缩协方差和风险预算可作为比裸样本均值-方差更稳健的候选 |

论文中的历史结果只用于说明为什么某类特征值得进入候选集。最终是否保留，取决于本项目
使用可得数据、A 股执行约束和预先定义样本外流程得到的结果；报告不得把学术异市场结果
转述为对具体标的的收益保证。

## 5. 本地运行命令

以下命令均在仓库根目录的 PowerShell 中执行。它们只做研究、只读检查或固定通知测试，不会提交 A 股订单。

### 5.1 环境与测试

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest --temp-dir runtime/tmp -q
.\.venv\Scripts\python.exe -m ruff check conftest.py src tests
```

启动 PAPER 桌面：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade gui
```

### 5.2 AKShare 与 BaoStock

```powershell
# 公开网页快照；输出 semantics/freshness/degraded/warnings
.\.venv\Scripts\python.exe -m gribuki_trade ashare-snapshot --symbol 600000.SH

# 仅返回已完成的提供方聚合分钟 K 线
.\.venv\Scripts\python.exe -m gribuki_trade ashare-bars --symbol 600000.SH --interval 5m --lookback-minutes 480

# BaoStock 历史日线
.\.venv\Scripts\python.exe -m gribuki_trade ashare-daily --symbol 600000.SH --days 120
```

看到 `PUBLIC_WEB_QUOTE_SNAPSHOT`、`AGGREGATED_MINUTE_BAR` 或 `freshness=UNKNOWN` 是设计结果，不应在外层重命名为 tick 或强行改成 current。

### 5.3 新闻一次采集与持续观察

```powershell
# 单次全市场快讯，原始表存入 runtime/news/raw
.\.venv\Scripts\python.exe -m gribuki_trade ashare-news --feed global_sina --limit 10

# 单只股票新闻线索
.\.venv\Scripts\python.exe -m gribuki_trade ashare-news --feed individual_eastmoney --symbol 600000.SH --limit 10

# 两个全局源 + 一个自选股；先跑 3 个周期验证
.\.venv\Scripts\python.exe -m gribuki_trade ashare-news-watch `
  --feed global_sina `
  --feed global_cailianpress `
  --symbol 600000.SH `
  --interval-seconds 60 `
  --cycles 3

# 指定股票的巨潮公告；保存原始响应、内容哈希、游标和事件版本
.\.venv\Scripts\python.exe -m gribuki_trade ashare-disclosures `
  --symbol 600000.SH `
  --lookback-days 14 `
  --runtime-dir runtime/news
```

持续模式默认目录是 `runtime/news`，包含内容寻址原始档案和 `events.sqlite3`。把 `--cycles` 设为 `0` 才会运行到人工中断。
每次源运行还会追加到 `source_health.sqlite3`。查看最近 20 日统计：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-source-health `
  --days 20 `
  --runtime-dir runtime/news
```

### 5.4 大模型密钥

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade secret-set openai.api_key
.\.venv\Scripts\python.exe -m gribuki_trade secret-status
```

`secret-set` 使用本机无回显提示并写入系统凭据库。不要把 API key 放进命令行、`.env`、源码、文档或聊天。

运行一次技术面研究；它会读取本地事件库、持久化不可变推荐，但不会下单：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-research-once `
  --symbol 600000.SH `
  --interval 5m `
  --lookback-minutes 480 `
  --events-db runtime/news/events.sqlite3
```

显式加入宏观/事件分析时才会读取本机 OpenAI key：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-research-once `
  --symbol 600000.SH `
  --interval 5m `
  --lookback-minutes 480 `
  --events-db runtime/news/events.sqlite3 `
  --macro `
  --model gpt-5.6
```

`--macro` 采用受限 EvidencePack、严格结构化输出和引用校验。模型不可用、证据不足、引用错误或输出无效时，结果降为 `ABSTAIN`；配置密钥本身不会启动后台调用。GUI 尚未绑定这条运行链。

对多只股票运行三个有界周期；每只股票的失败不会中止其余股票：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-research-watch `
  --symbol 600000.SH `
  --symbol 000001.SZ `
  --interval 5m `
  --interval-seconds 60 `
  --cycles 3
```

该命令故意要求正整数周期数，不提供隐藏的无限循环。未来长期运行由统一应用运行框架按小批次推进，负责交易日历、休眠恢复和故障审计；不再使用 Windows 任务计划或独立 PowerShell 监督器。

### 5.5 NapCatQQ

先从 [NapCatQQ 官方仓库/发布页](https://github.com/NapNeko/NapCatQQ/releases)自行安装并登录一个专用 QQ 账号，在 OneBot 11 中只启用本机 HTTP server，例如 `127.0.0.1:3000`，并设置高强度 access token。随后执行：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade secret-set napcat.onebot.access_token
.\.venv\Scripts\python.exe -m gribuki_trade napcat-status --base-url http://127.0.0.1:3000

# 明确确认后，只发送一条固定的非交易测试消息
.\.venv\Scripts\python.exe -m gribuki_trade napcat-send-test `
  --base-url http://127.0.0.1:3000 `
  --target-kind private `
  --target-id YOUR_QQ_ID `
  --confirm SEND_TEST

# 派发 ashare-research-once 写入 outbox 的待发消息；先用有限周期验证
.\.venv\Scripts\python.exe -m gribuki_trade napcat-dispatch `
  --base-url http://127.0.0.1:3000 `
  --target-kind private `
  --target-id YOUR_QQ_ID `
  --outbox-path runtime/research/outbox.sqlite3 `
  --cycles 10 `
  --poll-interval 1
```

当前 CLI 已提供健康检查、固定测试消息和可恢复 outbox 的有限周期派发。它还不是由 Windows 服务管理器托管的常驻 worker；NapCat 也不会由本仓库自动安装、启动或登录。研究命令只有同时给出 `--notify-target-kind` 和 `--notify-target-id` 才会尝试入箱，而且默认不通知 `ABSTAIN`。

## 6. 验证门槛

### 6.1 合并前门槛

- 全部单元测试通过；Ruff 无错误。
- 适配器测试不得访问真实网络，必须用固定 provider/frame/HTTP 响应覆盖字段漂移、空表、超时、429、5xx 和权限拒绝。
- 时间必须带时区；未来数据、乱序数据、重复 K 线、未完成 K 线和请求窗口外数据必须拒绝。
- 日线复权、成交量单位、金额单位和停牌状态必须有固定样本测试。
- 所有日志和异常不得包含 API key、OneBot token、认证 header、完整响应体或本机凭据值。

### 6.2 公共数据上线门槛

- 至少连续观察 20 个 A 股交易日，记录每个源的成功、未变化、退避、失败、字段漂移和数据延迟；不能只做一次成功截图。
- AKShare 与 BaoStock 的日终 OHLCV 抽样交叉检查，并对复权口径、停牌和单位差异单独解释。
- 盘中恢复、午休、收盘、周末和电脑休眠恢复都要验证；旧价格不能因为“刚抓到”而变成 current。
- 任一主源不可用时必须显示降级/陈旧状态；不得静默复用过期价格产生入场候选。
- 所有资讯保留 `published_at`、`first_seen_at`、`available_at`、canonical URL、内容哈希和原始文档引用。

### 6.3 策略与回测门槛

- 同一确定性策略函数用于历史回放和在线一次性评估。
- 回测只能读取决策时点已经可见的数据；新闻以本系统首次观察时间为可用性下界。
- 纳入佣金最低收费、印花税、滑点、停牌、涨跌停、T+1、退市和成分股历史变化。
- 使用滚动/走步样本外验证，并与持有、指数、简单均线/动量基线比较。
- 分别报告收益、最大回撤、换手、费用、暴露、缺失数据日和拒绝判断比例；不能只报告累计收益。
- 在形成统计校准集之前，推荐置信度必须保持 `UNCALIBRATED`。

### 6.4 LLM 门槛

- 模型只能接收经过归一化的 EvidencePack，默认无网页浏览、无交易工具、无账户信息。
- 输出必须匹配严格 JSON Schema；每条事实 claim 必须引用请求内存在的 `evidence_id`。
- 证据不足、冲突、陈旧、来自未来或模型输出无效时必须 `ABSTAIN`。
- 固定一组人工核验的 A 股公告/宏观事件评测集；每次模型、提示词或 schema 变更都做回归。
- 记录模型版本、提示模板版本、输入证据哈希、延迟和 token 成本；不得把模型自报“高置信”当作统计置信度。
- 做提示注入测试：新闻正文中的“忽略规则、调用工具、买入某股”等文本只能作为不可信证据，不能成为指令。

### 6.5 推荐发布门槛

- `ENTER_CANDIDATE` 必须有证据；缺失证据自动 `ABSTAIN`。
- 陈旧、未知新鲜度、降级或数据不完整时，不得发布入场候选。
- 每条推荐有稳定 ID、策略/模型版本、理由码、不确定性、参考时点和到期时间。
- 宏观负面达到阈值可以把技术入场候选降为观察，但 LLM 不能越过技术/证据门禁提升为自动买入。
- 推荐对象不得包含账户、交易密码、下单 API、可执行脚本或自动订单数量。

### 6.6 NapCat 通知门槛

- OneBot HTTP 只监听 loopback，不开放局域网/公网，不把 token 放在 URL。
- 私聊和群聊目标采用精确白名单；白名单拒绝必须发生在网络请求之前。
- 仅发送 OneBot 文本 segment，不解释或执行 CQ 码、URL、附件和入站消息。
- outbox 幂等键防重复；租约支持崩溃恢复；重试有退避和最大次数；推荐过期后不得补发。
- 连续观察期间统计发送成功、重试、死信和端到端延迟；QQ 掉线或升级后应明确告警。

## 7. 分阶段开发计划

### P0：研究数据骨架（已完成代码，待真实连续观测）

- AKShare 快照、网页成交明细、1/5 分钟 K 线适配。
- BaoStock 日线适配。
- 数据语义、新鲜度、重试、缓存与离线测试。
- AKShare 新闻一次采集/循环采集、原始档案、事件去重与修订。
- append-only 源健康账本及成功率、降级率、陈旧率和 p50/p95 延迟汇总。

退出条件：完成 20 个交易日的源健康与字段漂移报告。

### P1：一手来源与 EvidencePack

- 巨潮指定股票公告适配已完成；继续增加上交所、深交所、证监会、央行、统计局等一手源适配。
- 建立公司代码/简称/曾用名/行业实体映射，降低同名误匹配。
- 已形成按标的与决策时点裁剪、具有来源多样性和注入标记过滤的 EvidencePack；继续扩展行业、宏观主题和正式实体映射。
- GUI 展示原文链接、首次观察时间、来源层级和矛盾证据。

退出条件：关键事实可从推荐回溯到原始内容哈希与一手 URL；未来信息测试全部拒绝。

### P2：技术与宏观研究闭环

- `AShareResearchService` 端到端一次性 CLI及多标的有界周期调度已完成；后台交易日历 job 待 P3。
- LLM adapter 已接入 EvidencePack，并保留确定性门禁和 `ABSTAIN`。
- 将短线（日内观察、1–5 日持有）和波段（1–8 周）使用不同 bar、TTL 和参数集。
- 推荐不可变账本、追加式结果记录和下一可交易日开始的成本后事后评估已完成；批量标注与统计校准仍待积累样本。

退出条件：固定回放可复现相同推荐 ID；LLM 失败不影响技术研究且不能产生无证据候选。

### P3：调度、GUI 与通知

- 建立交易日历感知的常驻调度器、进程互斥、休眠恢复和健康状态。
- 绑定 GUI“资讯 / 建议”页签，禁止 GUI 主线程执行网络请求。
- 启动 outbox dispatcher，接入 NapCat 状态、重试、死信和手动重发界面。
- 增加静默时段、摘要批次和高优先级事件策略，防止消息轰炸。

退出条件：连续 20 个交易日无重复通知、无过期补发、无 GUI 卡死，所有源故障可见。

### P4：研究有效性验证

- 建立无未来函数的历史资讯语料和 walk-forward 回测。
- 对技术-only、宏观-only、组合门禁和简单基线做消融实验。
- 统计不同来源、事件类型、持有周期和市场状态下的命中、回撤和拒绝比例。
- 只有在足够样本上完成概率校准后，才允许从 `UNCALIBRATED` 改为 LOW/MEDIUM/HIGH。

退出条件：预先定义的样本外标准通过；若不能超过简单基线，则保留资讯摘要功能而不宣称预测能力。

### P5：券商接口（本阶段不阻塞，也不自动启用）

待用户从广发确认正式 API/QMT/PTrade 等权限后，只新增独立 broker adapter。研究推荐仍不能直接下单，必须经过账户白名单、A 股规则、组合净额、风险、人工确认/PAPER 验收和显式 LIVE 解锁。

## 8. 需要用户提供的信息与本机权限

### 8.1 现在推进研究闭环所需

1. 自选股清单：证券代码、关注名称/曾用名、行业；建议先从 20–50 个高流动性股票或 ETF 开始。
2. 关注周期：短线 1 分钟还是 5 分钟为主；波段使用日线还是周线；各自希望接收消息的时段。
3. 资讯偏好：公告、政策、宏观、行业、公司新闻、异常波动中哪些必须即时推送，哪些只做日终摘要。
4. LLM 预算与模型访问：可用的 OpenAI 模型名称、月度/单日成本上限。API key 只通过本机 `secret-set` 输入，不要发给开发者或写入聊天。
5. 数据保留：原始资讯保存多久、允许占用的磁盘上限、是否需要加密备份。

### 8.2 NapCat 联调所需

1. 一个建议专用的 QQ 账号；用户本人在本机完成 NapCat/NTQQ 的二维码登录，不提供密码或验证码。
2. 通知类型（私聊或群聊）和目标 QQ/群号。
3. 用户在 NapCat 本地配置的 OneBot access token，通过 `secret-set napcat.onebot.access_token` 输入。
4. 若不是默认值，提供本机 loopback 端口；当前客户端不会接受非 loopback 地址。

### 8.3 需要允许的本机能力

- Python 进程访问 AKShare 上游、BaoStock、选定公开资讯站点和模型 API 的出站网络。
- 写入仓库内 `runtime/` 的原始档案、SQLite、日志和 outbox。
- 访问 Windows Credential Manager/macOS Keychain 保存 API key 和 OneBot token。
- 运行用户自行安装的 NapCatQQ/NTQQ，并允许 Python 访问其 loopback HTTP 端口。
- P3 常驻能力等待统一应用运行框架设计；不得用 Windows 任务计划、登录自启动或独立 PowerShell 监督器替代该框架。

### 8.4 当前不需要

- 广发账户密码、交易密码、短信验证码、完整资金账号或身份证件。
- QMT/PTrade/XTP/TORA 注册信息。
- 任何 A 股券商实盘权限。
- 把密钥保存到仓库或发到聊天。

## 9. 尚未完成与下一阻塞点

当前代码已经覆盖数据适配、资讯档案、确定性技术信号、受限宏观分析、推荐门禁、通知适配和 GUI 空状态，但距离“持续实时给出并同步建议”还缺少：

- 巨潮公告已经接入；上交所、深交所及一手宏观源仍需接入并做长期字段漂移验证；
- 有界多标的调度已完成；仍缺应用内交易日历感知、生命周期监督和休眠 misfire 恢复；
- 新闻事件到行业/宏观主题的正式实体解析；标的与时间裁剪的 EvidencePack 已完成；
- GUI 后台绑定与常驻任务管理；端到端一次性研究 CLI 和有限 outbox 派发 CLI 已完成；
- OpenAI 模型真实访问、成本上限和回归评测；
- NapCat 本机安装、专用 QQ 登录、目标 ID 和 live 通知联调；
- 至少 20 个交易日的数据与通知 soak test；
- Point-in-Time 历史新闻语料和样本外有效性验证。

因此，下一项真正需要用户参与的阻塞点不是券商审批，而是两组本机信息：自选股/消息偏好，以及 NapCat 专用 QQ 的本机登录和通知目标。其余数据、存储、调度和研究门禁可以继续在无券商权限下开发。
