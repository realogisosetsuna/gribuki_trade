# A 股 PAPER 账本、日线撮合与单日盘中编排

本模块是本地、无券商委托副作用的研究系统。持久账本解决“已经确定的一笔成交如何记账、重放和审计”；保守日线撮合器只执行明确的日线假设；持久 wrapper 以 append-only 事件和恢复 saga 保存委托及 bar-run；PAPER-day 再以独立的分钟级、单日 CLI 编排公开行情、技术信号、风险、一次性 IOC、通知和报告。任何一层都不声称还原 tick、L1/L2、盘口排队或真实成交。

## 模块边界

- `domain/paper_trading.py`：成交、费用、持仓、账户快照和账本事件值对象。
- `ports/paper_ledger.py`：服务所依赖的最小持久化协议。
- `storage/paper/paper_ledger.py`：SQLite WAL、append-only、逐账户哈希链实现。
- `services/ashare/paper_day/ashare_paper.py`：开户、成交录入、交易日 rollover、状态重放。
- `domain/paper_orders.py`：六态限价委托、显式价格区间、撮合 bar 和结果值对象。
- `services/ashare/paper_day/ashare_paper_matching.py`：FIFO、成交量参与率和 next-bar 保守撮合。
- `storage/paper/paper_orders.py`：append-only 委托/run 事件、hash chain、run identity 和 writer lease。
- `services/ashare/paper_day/ashare_paper_recovery.py`：跨订单库与资金账本的可恢复 saga。
- `services/ashare/intraday/ashare_intraday_paper.py`：盘中价格接受区间、板块数量、组合风险与下一完整分钟 IOC。
- `domain/paper_day.py`、`storage/paper/paper_day.py`：单日 manifest、append-only journal、事件哈希链和单 writer lease。
- `services/ashare/paper_day/ashare_paper_day.py`：盘前、盘中、收盘、outbox 与 sidecar 的单进程全天编排。
- `reporting/paper_day/paper_day_summary.py`：只从 sidecar 生成可审计增强摘要，不打开运行中的数据库。
- `backtest/costs.py`：模拟盘与回测共用的费用计算器；现已包含可配置过户费。

账本和日线 matcher 服务没有导入行情或券商适配器；PAPER-day 由 CLI 组合公开行情、交易日历与 OneBot 通知适配器，但没有真实 broker 依赖。人工成交和模拟成交都使用 `ASharePaperFill`，仅用 `source=MANUAL/SIMULATED` 区分来源。人工账单允许录入实际费用覆盖值；模拟成交必须使用配置费率。

## 第二阶段撮合能力

- 委托状态为 `PENDING/PARTIALLY_FILLED/FILLED/CANCELLED/REJECTED/EXPIRED`；
- 只处理决策日之后、调用方明确提供的未复权完整日线；
- 缺 OHLC、零成交量、停牌、缺价格区间或 bar 超出区间均不成交；
- 买入为 100 股整手，卖出允许清理零股尾仓；
- 限价必须被 bar 触及，成交价使用不突破限价的保守方向滑点；
- 默认最多使用 bar 成交股数的 1%，按委托时间 FIFO 分配，可跨 bar 部分成交；
- 确定性 `fill_id` 写入上述持久账本，重复 bar 精确幂等，修订冲突失败关闭；
- 部分成交按同一订单累计最低佣金，避免每个 bar 重复收取一遍最低佣金。

纯日线撮合器仍是确定性内存引擎；需要持久恢复时应使用第三阶段 durable wrapper。它可恢复未完成提交、订单状态和 bar-run，并依赖资金账本的确定性 `fill_id` 防止跨库崩溃后重复扣款。该**日线** matcher/wrapper 尚无用户侧撮合 CLI/GUI，调用方必须显式 `recover()` 后再使用；这不影响后述独立 PAPER-day **盘中** CLI。

## 已实现不变量

1. 资金不得为负，失败成交不会追加事件。
2. 持仓满足 `quantity = available_to_sell + today_buy`。
3. 买入计入 `today_buy`，同一交易日不可卖；显式 rollover 到下一个已确认交易日后才变为 `available_to_sell`。
4. 卖出数量不得超过 `available_to_sell`，因此不会裸卖或超卖。
5. `fill_id` 是账户内成交幂等键：同内容重放不重复记账，不同内容复用同一编号会失败。
6. 每笔成交保存最终成交额、佣金、过户费、印花税、现金变化和已实现盈亏，后续修改默认费率不会改变历史结果。
7. SQLite 使用 WAL 和 `synchronous=FULL`；数据库触发器禁止 UPDATE/DELETE。
8. 账户快照只由事件重放生成。进程重启、历史审计和日常查询走同一投影代码。
9. 每个账户的事件带前序哈希；读取时验证 payload 摘要和哈希链。
10. rollover 只接受更晚日期。服务不把自然日误当交易日，调用方必须提供经交易所日历确认的交易日。

## 费用假设

默认个人佣金是假设值 `0.03%`、每笔最低 5 元，必须按实际券商合同修改。股票过户费默认按成交额 `0.001%` 双向，股票卖出印花税默认 `0.05%`；ETF 默认不收过户费和印花税。所有费率均可配置，人工账单也可逐笔覆盖实际费用。

参考依据：

- 中国结算自 2022-04-29 将股票交易过户费统一调整为成交金额 `0.01‰` 双向收取：<https://www.xinhuanet.com/2022-04/28/c_1128605983.htm>
- 财政部、税务总局公告自 2023-08-28 将证券交易印花税减半征收：<https://fgk.chinatax.gov.cn/zcfgk/c102416/c5211343/content.html>

这些是默认建模依据，不替代券商实际交割单；费率发生变化时应更新配置，不应回写历史事件。

## 使用示例

CLI 已提供显式、有限动作入口；它只维护账本，不会连接行情或自动撮合：

```powershell
python -m gribuki_trade ashare-paper open --account personal-paper `
  --initial-cash 100000 --session-date 2026-08-14

python -m gribuki_trade ashare-paper fill --account personal-paper `
  --fill-id manual-20260814-001 `
  --symbol 600000.SH --side BUY --quantity 100 --price 10.00 `
  --instrument STOCK --source MANUAL --session-date 2026-08-14

python -m gribuki_trade ashare-paper snapshot --account personal-paper
```

`rollover` 必须传入已经由交易日历确认的下一交易日；`fills` 用于读取不可变成交历史。模拟成交和人工成交使用同一个账本契约，但 CLI 不会把推荐自动转换为成交。

## PAPER-day 单进程全天编排

`ashare-paper-day` 是与 durable 日线 matcher 分开的盘中入口：

- `run` 只允许上海时区当天，经 BaoStock 完整交易日历验证并要求上一交易日存在；还会预检 PAPER 账本与 NapCat/OneBot，必须显式输入 `--confirm PAPER_DAY`。
- 09:25 前执行盘前降级筛选并建立初始关注名单；该路径把当前网页快照保守绑定到已核验的上一交易日，始终标记 `DEGRADED`，只承诺沪深主板、创业板和科创板并排除北交所。开盘后每 15 分钟扫描一次全市场，维护有界关注名单与当日异动 TTL；任何买入都必须获得当前交易时段 corroboration。
- 名单内标的以 1 分钟为主、5 分钟为辅生成确定性技术决策。只有 `ENTER_CANDIDATE` 才进入横截面异常、跨源价格一致性、组合风险和申报规则门禁。
- 获批买入只使用“信号完全可知之后开始的第一个完整、已收盘 1 分钟区间”做一次 IOC。部分成交立即取消余量，未成交不会滚到下一分钟，也不会跨午休延续。
- `REDUCE` 无论是否有 T+1 可卖数量，本次单日测试都只把信号、可接受卖价和未来数量计划写入不可变 journal 并通知，不创建卖单；当天买入另外明确记为 `today_buy`，可卖数量为零。
- 每个交易日隔离到 `runtime/paper/day/<YYYY-MM-DD>/`，包含 manifest、`journal.sqlite3`、`ledger.sqlite3`、`outbox.sqlite3`、`report-artifacts.sqlite3`、`session.log.jsonl`、`status.json` 与原子生成的 Markdown 报告。跨日退出计划使用共同父级的 `runtime/paper/day/exit-plans.sqlite3`，不是每个日期各建一套。文本 outbox、附件 outbox、成交应用和恢复均使用稳定事件键/`fill_id`，不靠重抓未来行情修补历史。
- `status`、`report`、`summary` 都以 sidecar 为读取边界，不打开运行中的 journal、账本或 outbox；`summary` 会原子写出增强 Markdown 投影。生命周期完成不等于覆盖完整：晚启动、人工恢复或资料缺口会保留 `PARTIAL_SESSION`。

新交易日准备使用跨进程锁，并在克隆前后复核来源账本、公共事件前缀和 lineage。每个按日 ledger 同时保存不可变 binding 与历史 seal；sidecar 缺失但只有一个合法公共前缀时，可以确定性恢复为 `RECOVERED_ORPHAN_CLONE`。任何 binding 冲突、历史分叉、来源日被继续追加或候选不唯一都会失败关闭。启动时还会恢复成交与退出计划之间的崩溃边界，并按 `account_id + symbol` 把跨日持仓重新绑定到既有保护流；历史上不存在的 PIT 保护不会用今天的数据补造。

```powershell
$tradeDate = (Get-Date).ToString('yyyy-MM-dd')

python -m gribuki_trade ashare-paper-day --help

python -m gribuki_trade ashare-paper-day run `
  --session-date $tradeDate --account ashare-paper-day `
  --initial-cash 200000 `
  --target-kind private --target-id "YOUR_QQ_ID" `
  --intraday-llm --intraday-llm-review-top-n 6 `
  --intraday-llm-review-ttl-minutes 20 `
  --intraday-llm-max-calls unlimited `
  --intraday-llm-events-db runtime/news/events.sqlite3 `
  --confirm PAPER_DAY

python -m gribuki_trade ashare-paper-day status --session-date $tradeDate
python -m gribuki_trade ashare-paper-day report --session-date $tradeDate
python -m gribuki_trade ashare-paper-day summary --session-date $tradeDate
```

`DAY_ABORTED` 是人工处置边界。确认可以在同一 Run 上继续后，原命令必须显式增加 `--recover-after-abort`；恢复会从不可变事件重建名单、最后处理分钟、待撮合订单及未完成 fill saga，不创建第二本历史。仓库不再提供 Windows Task Scheduler 或独立 PowerShell 看门入口；后续统一应用运行框架也不得自行越过 `DAY_ABORTED`。

### 日报附件的持久交付与人工恢复

`DAILY_REVIEW` 是“短摘要 **且** Markdown”的双交付契约。Markdown 先验证固定报告章节，再以 `run_id`、冻结目标、文件名和 SHA-256 写入 `report-artifacts.sqlite3`，随后执行 `PENDING → IN_FLIGHT → SENT`。NapCat 没有端到端幂等键，因此一旦已经 claim 并调用上传，任何异常、取消或进程中断都会落为 `AMBIGUOUS`；运行器绝不在不确定提供方是否收件时盲重试。

- 完成事件后、上传前中断：`PENDING` 在同一 run 重启时继续；
- `mark_sent` 后、journal 前中断：`SENT` 只补不可变事件，不再次上传；
- 历史 `REPORT_UPLOADED` 且文件名/SHA/目标一致：迁移为 legacy `SENT`，不重传；
- 历史 `REPORT_UPLOAD_FAILED` 无法证明提供方未收件：保守迁移为 `AMBIGUOUS`；
- 未配置附件 notifier：状态为 `NOT_CONFIGURED`，总交付缺口增加 1，不能宣称日报完整；
- `PaperDayResult.completed` 只表示交易监控到达终态；CLI `ok` 和 `daily_review_delivery_complete` 还要求所有短文本无缺口且 Markdown 为 `SENT`。

只有操作员已经在 QQ/NapCat 核验结果后，才可在原 `ashare-paper-day run` 命令上增加一个恢复动作：

```powershell
# 文件确已存在：记录核验到的 provider file ID，只补 SENT，不上传
--report-artifact-recovery-action MARK_SENT_AFTER_PROVIDER_VERIFICATION `
--report-artifact-provider-id "VERIFIED_PROVIDER_FILE_ID" `
--confirm-report-artifact-recovery PAPER_REPORT_ARTIFACT_RECOVERY

# 文件确实不存在：显式授权重新入队并只尝试上传一次
--report-artifact-recovery-action RESEND_AFTER_PROVIDER_NON_RECEIPT_VERIFICATION `
--confirm-report-artifact-recovery PAPER_REPORT_ARTIFACT_RECOVERY
```

两种动作互斥；没有强确认、核验结果冲突或仍处于未知状态时继续失败关闭。`RESEND` 授权先写入不可变 journal；执行时先把歧义记录重新入队，再在调用提供方前写入单次消费事件。若在“重新入队→消费事件”之间崩溃，恢复逻辑会依据原歧义事件把记录重新隔离而不会上传；如果该次上传再次成为 `AMBIGUOUS`，重启或重复同一恢复参数也不会再次发送，只能重新检查提供方并在确认已收件后用 `MARK_SENT` 收敛。

### 风险、价格接受区间与成交边界

默认初始权益为 200,000 元。默认 `maximum_positions=None`，即不设固定持仓数量硬上限；真正逐单生效的上限仍包括：

| 约束 | 默认值 | 实际含义 |
|---|---:|---|
| 最低现金储备 | 20% | 执行后现金不得低于初始权益的 20% |
| 组合总 gross | 80% | 持仓标记市值合计不得超过初始权益的 80% |
| 单标的敞口 | 20% | 同一证券的标记市值不得超过初始权益的 20% |
| 单笔止损风险 | 0.75% | 风险预算为初始权益的 0.75%；止损距离取真实距离与入场价 1.5% 两者较大值 |
| 分钟成交量参与 | 1% | 只使用撮合分钟成交股数的 1%，再按板块单位向下取整 |
| 最晚新开仓 | 14:55 | 超时不再创建买入订单 |

`--maximum-positions <N>` 只是在需要时打开额外的计数熔断，不能替代上述资金边界。已经 journal 化的会话若改变风险策略，还必须输入 `--confirm-risk-policy-change PAPER_RISK_POLICY_CHANGE`，并保留前后策略哈希；既有成交与持仓不重算。

价格接受区间是执行失败关闭契约，不是预测的目标价区间：

- **买入**：下界是严格高于技术失效位的第一个合法 tick；上界/限价是信号参考价加默认 0.1%，并封顶于当日涨停价。若完整撮合分钟的最低价触及或跌破失效位，整笔 IOC 不成交；系统不会把已经破坏技术前提的低价当作更优成交。
- **卖出记录**：下界/未来限价是参考价减默认 0.1%，并托底于当日跌停价；上界为涨停价，代表更高的卖价仍可接受。当前 runner 不提交卖单；未来执行若下一价格低于下界、分钟最高价未触及限价、可验证量不足或跌停封死，均不得假设成交。
- **日价格带**：仅对已支持的非 ST 普通阶段使用保守标准带——沪深主板 ±10%，创业板/科创板 ±20%。ST 和北交所执行不支持；缺少 IPO 特殊阶段元数据时不会擅自放宽价格带。
- **队列**：分钟 OHLCV 不能证明排队优先级。开高低收都锁在涨停价的买入分钟直接不成交；未来跌停封死的卖出同样应失败关闭。连续竞价价格笼子也没有 L1 基准价验证，因此这里不是报盘合规检查。

分板块限价申报数量冻结如下；单笔上限是交易申报形状上限，不是组合风险额度：

| 板块 | 买入 | 限价单单笔上限 | 常规卖出与余股 | PAPER 部分成交单位 |
|---|---|---:|---|---:|
| 沪市主板 | 至少 100 股，其后按 100 股递增 | 1,000,000 股 | 至少 100 股、按 100 股递增；不足 100 股余股届时一次性全卖 | 100 股 |
| 深市主板 | 至少 100 股，其后按 100 股递增 | 1,000,000 股 | 至少 100 股、按 100 股递增；不足 100 股余股届时一次性全卖 | 100 股 |
| 创业板 | 至少 100 股，其后按 100 股递增 | 300,000 股 | 至少 100 股、按 100 股递增；不足 100 股余股届时一次性全卖 | 100 股 |
| 科创板 | 至少 200 股，其后按 1 股递增 | 100,000 股 | 至少 200 股、按 1 股递增；不足 200 股余股届时一次性全卖 | 1 股 |
| 北交所 | 不支持 | — | 不支持 | — |

### 买前 QUICK 与成交后 DEEP 退出计划

PAPER-day 的买入链现在把退出计划当作前置条件，而不是成交后的可选备注：

1. 技术信号和盘中 LLM 门通过后，先用当时已经完成、PIT 可见的分钟线构建
   QUICK。候选来自技术失效位、突破支撑、确认摆动低点和 `2×robust ATR`；
   系统取更宽松但仍低于最低合法买价的止损，并把目标表示为预注册 R 倍 barrier，
   不伪称上涨概率或预期收益。
2. QUICK 先写入独立 append-only `exit-plans.sqlite3` 哈希链，订单 payload 再绑定
   `protection_id/plan_id`。随后用“最坏许可买入限价－QUICK 止损”重新计算每股
   风险、组合预算和申报数量；缺计划或风险重算失败都不允许提交订单。
3. 信号后的完整撮合分钟若已经触发止损、止盈或时间门，整笔订单失败关闭；
   旧 pending 订单缺 QUICK 也会在恢复时终止，不能偷偷套用新规则。
4. 成交后把 fill 持久绑定到保护流，并立即登记可恢复的 DEEP 请求。DEEP 使用
   1/5/15 分钟已完成线、确定性 robust ATR/结构指标，以及同一生产双轨 LLM 的
   baseline/对抗评分。LLM 只能影响预登记 R 档或缩短期限；最终价格仍由确定性
   规则映射，止损不得下移、时间门不得延长。模型或数据失败时继续使用 QUICK。
5. 只有已附着成交的计划才能观察退出 barrier。同一 OHLC 同时触发止损和止盈时
   固定按止损优先；T+1 可卖数为零仍记录并通知，但不会创建卖单。保护流可在
   进程恢复和下一交易日重新发现，历史无 PIT 计划的旧持仓不会被事后补造。

这些默认参数仍标记为 `UNVALIDATED/UNCALIBRATED`。离线 strategy_lab evaluator
能够计入 T+1、停牌/一字跌停、费用和滑点做 purge/embargo walk-forward，但结果
固定为 research-only，不能自动晋升生产策略。

### 盘中双轨 LLM 复核（离线工程验收完成，实盘时段 soak 待做）

盘中 LLM 不是 2026-08-14 历史会话的组成部分；当前用户侧 CLI 已接生产双轨并通过离线回归、故障注入和重启测试，但尚未完成下一个真实交易日的全天 soak。`--intraday-llm/--no-intraday-llm` 默认选择前者；provider 与模型未显式指定时读取 GUI 共享配置。`--no-intraday-llm` 是显式 operator opt-out，会落入运行审计，并把 runner 收紧为“持仓监控/卖出复核、禁止新买”；技术候选不能绕过双轨门禁单独成交。默认后台复核当前 Top 6，可通过 `--intraday-llm-review-top-n` 修改；复核 TTL 默认 20 分钟。`--intraday-llm-max-calls` 接受正整数或 `unlimited`，默认 `unlimited`；无限模式仍从 journal 恢复并累计已发起调用数，但计数不触发会话预算耗尽。manifest 中的 `maximum_reviews_per_session` 以 JSON `null` 固定无限语义，`LLM_INTRADAY_POLICY_CONFIGURED` 审计事件以 `UNLIMITED` 固定可读语义。

`--intraday-llm-events-db` 默认指向 `runtime/news/events.sqlite3`。runner 在启动阶段使用 `mode=ro&immutable=1` 与 `query_only`，只读加载 `first_seen_at/available_at` 均不晚于启动截点的 PIT 事件并冻结为内存快照；盘中刷新不再访问该数据库，也不会自行采集新闻。事件库不存在、无有效事件、格式无效或存在未 checkpoint 的非空 `-wal` 时，快照不可用，required 买入失败关闭。因此应由独立新闻任务在盘前预填并关闭 writer、完成 checkpoint 后再启动 runner，不得让新闻 writer 与这次只读快照加载并发打开同一路径。

原单分析器和结构化对抗角色在同一冻结 EvidencePack 上并行运行，生产评分优先采用对抗轨；baseline 始终进入报告供对照。角色必须引用原始 evidence ID 并给出证伪条件；对抗失败、审计落盘失败或跨轨实质冲突会失败关闭或降级。盘中单角色调用上限 15 秒、FAST case 24 秒、外层协调器 28 秒，避免相同 deadline 嵌套；会话预算可无限，但每案角色和轮次永远有限。

结果必须先写 journal，之后买入门禁才可以引用；最终买入门只读本地缓存，不等待 provider，也不执行 I/O。复核未就绪、过期、上下文/模型身份不一致或失败时，required 买入失败关闭。LLM 只能确认、降级或 veto 技术规则已经给出的 `ENTER_CANDIDATE`，绝不能把 `WATCH` 升级为买入。`REDUCE` 不在分钟临界路径重新调用模型，而是读取成交后 DEEP 已持久化的 baseline/对抗评分；模型排队或失败不能阻断止损观察。

### 本地验收实例：2026-08-14

当前工作区保留了一次公开行情驱动的实际 PAPER-day：生命周期 `COMPLETED`，但覆盖口径诚实记录为 `PARTIAL_SESSION`；写入 6,308 个 sidecar 事件，盘前名单 30 个标的，15 次全市场盘中扫描，对 93 个标的完成 5,536 次技术评估（`WATCH=5161`、`REDUCE=203`、`ENTER_CANDIDATE=141`、`ABSTAIN=31`）。141 个买入候选中 6 个通过风控，5 个成交，1 个因未触及限价取消；203 个卖出信号全部只记录。20 万元初始现金最终投影为 40,225.47 元现金、168,551.00 元持仓市值、208,776.47 元估算权益和 5 个持仓。

这次会话的交易信号由当时已落盘的技术规则驱动，没有盘中 LLM。后续实现不会追溯重评或改写 journal、订单、成交和持仓；当日中途确认的持仓计数上限移除、价格接受区间与分板块数量策略，也不会反推早先订单不存在的字段。完整详情见 [本机 PAPER-day 增强摘要](../runtime/paper/day/2026-08-14/reports/ashare-paper-day-summary-2026-08-14-10e8f5980c.md)。`runtime/` 被 Git 忽略，因此这是当前工作区的本地验收证据，不是 clone 后自带的仓库资产；其中的估算权益也不是收益承诺或真实成交验证。

Python API 示例：

```python
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from gribuki_trade.domain import (
    ASharePaperFill,
    PaperFillSource,
    PaperInstrumentType,
    Side,
)
from gribuki_trade.services import ASharePaperTradingService
from gribuki_trade.storage import SQLitePaperLedger

shanghai = ZoneInfo("Asia/Shanghai")

with SQLitePaperLedger("runtime/research/ashare-paper.sqlite3") as ledger:
    service = ASharePaperTradingService(ledger)
    service.open_account(
        "personal-paper",
        initial_cash=Decimal("100000"),
        session_date=date(2026, 8, 14),
        opened_at=datetime(2026, 8, 14, 8, 30, tzinfo=shanghai),
    )
    receipt = service.record_fill(
        ASharePaperFill(
            account_id="personal-paper",
            fill_id="manual-20260814-001",
            symbol="600000.SH",
            side=Side.BUY,
            quantity=100,
            price=Decimal("10.00"),
            instrument_type=PaperInstrumentType.STOCK,
            trading_date=date(2026, 8, 14),
            executed_at=datetime(2026, 8, 14, 10, 0, tzinfo=shanghai),
            source=PaperFillSource.MANUAL,
        )
    )
```

## 运行数据与临时目录

`runtime/paper/day/` 是 PAPER-day 的按日权威运行根，不属于可丢弃测试目录。测试与诊断临时文件统一按“显式 `--temp-dir` > `GRIBUKI_TRADE_TMP_DIR` > `runtime/tmp`”解析；可先用 `python -m gribuki_trade temp-root status` 查看，用 `temp-root prepare --temp-dir <PATH>` 显式创建。根目录 `conftest.py` 会再建立进程隔离子目录，避免并发测试互相清理；放在仓库根也确保不带显式测试路径的完整 `pytest --temp-dir ...` 调用能在初始参数解析阶段识别该选项。

历史仓库根临时树只能先通过 [归档清单工具](../scripts/archive_root_temps.py) 生成并验证清单，再由 [受控清理脚本](../scripts/cleanup_root_temps.ps1) 执行；PAPER-day 权威目录、reparse point、目标集合漂移或归档不一致都会被拒绝。文档不保存本机个人路径或具体清理目标。

`python scripts/archive_root_temps.py` 只做盘点，不归档也不删除；用 `python scripts/archive_root_temps.py --help` 查看受控归档、唯一证据保留和 manifest finalize 参数。清理脚本是另一步显式破坏性操作，不由盘点命令自动触发。

## 持久订单与崩溃恢复

第三阶段新增 `SQLitePaperOrderStore` 与
`DurableASharePaperOrderMatcher`。它们不改变保守日线撮合规则，只把订单状态和
bar-run 运行协议持久化：

1. 提交先写 `ORDER_SUBMISSION_STARTED`，验证/预留后写
   `ORDER_STATE_APPLIED`；未完成提交会阻止 bar run，恢复时可重建。
2. 撮合前写 `RUN_STARTED`，其中保留完整原始 bar、价格限制、来源版本、撮合
   配置以及所有订单的运行前快照。
3. 确定性 `fill_id` 先通过资金账本幂等记账，再写
   `ORDER_FILL_APPLIED`，最后写订单状态和 `RUN_COMPLETED`。
4. 若进程在两个 SQLite 数据库之间崩溃，重启扫描 incomplete run 并从
   `RUN_STARTED` 重算；账本已存在的同一 `fill_id` 不会重复扣款。
5. SQLite `BEGIN IMMEDIATE`、全局单个未完成 run、run lease 与全局 durable-wrapper
   writer lease 防止并发撮合和并发预算预留。另一实例须待全局 lease 到期后接管，
   并先执行完整恢复；未完成 run 存在时也不能提交或变更订单。
6. 同一 `symbol / trade_date / config` 只能绑定一个 bar/source revision 和运行前
   订单快照。精确重放返回 `applied_new=False` 和容量摘要，但 `outcomes=()`：历史
   receipt 含账户投影，系统不会伪造 receipt；完整结果仍可从事件与账本审计。

订单事件和运行身份有禁止 `UPDATE/DELETE` 的触发器及 SHA-256 hash chain。lease
表是唯一可变表，只承担单写者协调，不承载业务事实。订单库和资金账本不是同一
数据库，因此这里明确采用 saga，不宣称跨库 ACID。

运行库门槛：本次本机 `sqlite3.sqlite_version` 为 3.50.4，属于 SQLite 官方 2026 年披露的
WAL-reset 缺陷可能影响范围。writer lease 只能处理业务并发，不能修复 checkpointer/writer
竞争。升级至 3.50.7、3.44.6 或 >=3.51.3 前，同一订单库或资金账本路径必须保持单进程、
单 Store 实例；不得把 durable wrapper 放入多 worker 部署。官方说明：
<https://www.sqlite.org/wal.html#walresetbug>。

## 明确未实现

- durable **日线** wrapper 尚未接入撮合 CLI/GUI；调用方必须显式执行 `recover()`。已有 CLI 的是独立 PAPER-day 盘中编排，不能拿它替代日线恢复入口。
- PAPER-day 盘中 LLM 已有默认 required 的用户侧 CLI，并通过全仓离线回归，但真实交易时段全天 soak 与当前所选生产 provider 的网络冒烟尚未完成；不能据此宣称生产可靠性。
- 自动推导完整历史 ST/涨跌停、停复牌、IPO 特殊阶段、集合竞价、连续竞价价格笼子基准或盘口路径；日线 matcher 的价格区间仍必须由调用方明确提供。
- 盘口冲击、真实排队位置、L1/L2 和逐笔成交模拟；日线与一分钟模型都只能给出明示的保守假设。
- T+0 ETF/债券/跨境品种的差异化交收；第一阶段账本按 T+1 管理支持的股票和 ETF。
- 分红、送股、配股、拆并股等公司行动。
- 现金存取、融资融券和多币种；PAPER-day 会保守预留待撮合买入容量，但这不是完整券商资金冻结模型。
- GUI 账户页、账单导入和持仓图表。账本 CLI 只执行明确动作；PAPER-day 只有在显式确认 `run` 后才自动推进当日研究与模拟撮合，且永不连接真实 broker。

在上述撮合与公司行动数据没有可靠来源前，系统应继续拒绝伪造“接近实盘”的成交结果。
