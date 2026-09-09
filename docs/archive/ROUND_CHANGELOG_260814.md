# 260814 本轮完整变更记录

> **历史归档（2026-08-15）**：本文冻结本轮开发结束时的变化和测试快照，不是持续更新的现状页。后续提交、测试数量和能力边界以代码、[README](../../README.md) 与 [项目交接文档](../PROJECT_HANDOFF_260814.md) 为准。

计划日期：2026-08-14；最终冻结日期：2026-08-15

本文对应 [RE 后续开发计划](后续开发计划260814.md)
列出的 16 项整改要求，记录本轮相对仓库既有版本形成的代码、持久化、运行编排、测试和文档变化。
它不是 Git 提交日志，也不把尚未经过真实交易日或真实网络验证的能力写成已经验收。

## 1. 本轮修改原则

本轮采用以下共同边界：

1. 研究、PAPER、实盘成交同步三条链保持隔离；任何新服务都没有真实券商下单权限。
2. 买入前必须先形成可审计的 QUICK 保护计划，成交后才允许观察退出 barrier 并生成 DEEP 版本。
3. LLM 只解释冻结证据、评分和降级，不计算交易所价格带、数量、费用、T+1 或订单。
4. 所有生产语义 LLM 同时保留 baseline 与对抗轨；生产优先对抗轨，失败时失败关闭，不把 baseline
   偷偷冒充为对抗结果。
5. “LLM 预算不限”表示会话调用总数可以没有硬上限，不表示无限角色、无限轮次、无限等待或无限重试。
6. 退出策略实验只能产生研究产物，不能自动改写 PAPER、实盘或生产配置。
7. 实盘同步只记录用户已经在券商完成的成交，系统观察并提醒卖出时机，不代替用户执行卖单。
8. 常驻运行框架留给用户后续定义；不恢复 Windows Task Scheduler、独立 PowerShell watchdog 或
   隐式 NapCat 启动脚本。
9. 用户明文保存的 API 文件不属于临时文件，不读取、不迁移、不删除、不清空、不覆盖。

## 2. 16 项要求的解决结果

| 编号 | 原问题 | 本轮解决结果 | 仍需真实环境验证的事项 |
|---:|---|---|---|
| 1 | 退出计划未接真实主链 | PAPER 已接买前 QUICK；`live-sync` 对券商已成交事实立即补建 QUICK；两者均接成交附着、DEEP、恢复和 barrier 观察 | 下一交易日全天 soak |
| 2 | 没有 DEEP 生成器 | 新增多时间框架确定性 DEEP 生成器，吸收 baseline/对抗评分但不允许 LLM 放宽风险 | ATR/R 参数仍标记未校准 |
| 3 | 生产 LLM 仍为单分析器 | 所有生产语义入口改为 baseline+对抗并行，生产优先对抗，报告保留两轨 | 真实 provider 多日延迟、弃权率统计 |
| 4 | 实盘确认回复不真实 | BUY 先原子确认成交，再只为本次 work 尝试无需 LLM key 的 QUICK 和定向单轮跟踪；成功/重试及 outbox 入箱均如实回执 | 常驻 OneBot 入站与持续循环等待未来应用 runtime |
| 5 | GUI 配置不驱动生产 | GUI 与 CLI 共用版本化 integration settings 和 keyring 命名，PAPER/研究/盘后/live 读取同一默认值 | 本机扫码、真实模型、重启人工验收 |
| 6 | 报告契约未被实际采用 | 六类报告在生产发送前验证；日报短摘要+Markdown，其他类型按明确 OR/AND 交付语义 | 多日真实 NapCat 交付统计 |
| 7 | 新闻只少重试、不够准确 | 扩大官方源、严格链接域、按注册发布域而非搜索 provider 计算独立佐证、单域未证实标记、持久退避与跨进程探测 fencing | 连续交易日覆盖率与修订率统计 |
| 8 | PAPER lineage 并发/绑定缺口 | 新日准备加锁，当前账本与 lineage 重新绑定校验，克隆与 manifest 可审计，分叉失败关闭 | 多交易日故障注入 soak |
| 9 | 实盘账本并发/真实性缺口 | 原子库存与批次、券商执行号去重、同委托多次部分成交、真实交易日日历、full SELL 构建 fence、工作 lease generation 与 active 指针 fencing 已落地 | 与用户券商对账仍由用户提供事实 |
| 10 | 退出条件可在成交前被观察 | QUICK 在买单前落盘；若当前线或撮合线已触发退出条件则拒买；barrier 只有附着成交后可观察 | 无 |
| 11 | 对抗 LLM 有延迟/恢复风险 | 角色、轮次和 case deadline 有界；盘中 28 秒外层；会话预算可设无限；慢分析不阻塞已有保护观察 | 真实网络尾延迟统计 |
| 12 | GUI 只是孤立原型 | NapCat runtime/token/登录态与 DeepSeek/OpenAI key/provider/model 已形成共享生产配置控制面 | GUI 不承担整个交易 runtime，符合用户后续重构安排 |
| 13 | 中文代码注释不完整 | 全仓说明性 docstring/comment 中文化，并加入自动扫描回归门 | 机器指令、错误码和协议标识保留原文 |
| 14 | OS 编排残骸与文档矛盾 | 注册脚本、watchdog、隐式 NapCat 启动器及公开入口删除；文档统一为应用内 runtime | 用户未来运行框架尚未定义 |
| 15 | 无提交边界 | 按用户要求不作为阻塞；以当前工作树、哈希链、测试和三份交接文档为本轮基线 | 未擅自创建 Git 提交 |
| 16 | “预期盈利”被改写 | 采用更诚实的 reward/risk barrier；不声称止盈价是收益概率或价格预测 | 参数需样本外校准 |

表中“需真实环境验证”不是缺少代码占位，而是离线测试无法替代的市场、网络或统计验收。

## 3. 买前 QUICK 与成交后 DEEP

### 3.1 新增领域模型和哈希链

新增：

- [`domain/exit_plans.py`](../../src/gribuki_trade/domain/exit_plans.py)
- [`storage/exit_plans.py`](../../src/gribuki_trade/storage/execution/exit_plans.py)
- [`services/exit_plan_lifecycle.py`](../../src/gribuki_trade/services/exit/exit_plan_lifecycle.py)

每个保护流采用 append-only 事件序列。计划创建、成交附着、DEEP 请求、版本替换、分析失败、barrier
观察与退出信号都有稳定幂等键、连续序号、前序哈希和事件哈希。SQLite trigger 禁止 UPDATE/DELETE；
读取时验证完整链。相同幂等键重放返回原事件，不同内容复用同键会失败关闭。

DEEP 替换必须保持同一账户、标的、成交基准、价格 tick 和初始风险。多头 stop 只能保持或上移，
时间 barrier 只能保持或提前；模型不能借“更乐观”扩大已经承诺的风险。

### 3.2 QUICK 算法

[`features/exit_planning.py`](../../src/gribuki_trade/features/exit_planning.py) 只读取决策时点前已经完成、
顺序严格、时效合格的 bar。它组合：

- 原技术信号失效位；
- 已确认 swing low；
- 突破结构支撑；
- robust ATR，即中位数/MAD 与 EWMA 波动估计中较保守的一个。

按用户要求，QUICK 不再机械选择最紧 stop，而是在合法候选中采用较松的保护，并默认保留 2 ATR
空间。目标价是预注册 R 倍 barrier，不称为“预期收益”。计划先于订单落盘；若当前 bar 已经触发
stop、目标或时间条件，候选直接拒绝。

### 3.3 DEEP 算法

新增 [`features/deep_exit_planning.py`](../../src/gribuki_trade/features/deep_exit_planning.py)：

- 联合 1、5、15 分钟已完成 bar；
- 分别校验 PIT、顺序、时效与覆盖；
- 计算多尺度 robust ATR、结构位和趋势状态；
- 同时接收 baseline 与对抗轨的结构化评分；
- 生产优先采用对抗轨，但 LLM 只能选择预登记 R 档或缩短时间门；
- 价格最终由确定性 Decimal/tick 规则映射；
- 任何结果仍必须通过 stop 不下移、时间不延长的替换验证。

若 LLM、证据或多时间框架不可用，系统生成明确降级状态或保留 QUICK，不会假装完成了模型深研。

### 3.4 PAPER 主链

[`services/ashare/paper_day/ashare_paper_day.py`](../../src/gribuki_trade/services/ashare/paper_day/ashare_paper_day.py) 的顺序改为：

```text
技术候选与双轨买入复核
  → 价格、数量、资金和风险初筛
  → 按最坏买入限价创建并持久化 QUICK
  → 复核当前 bar 未触发退出条件
  → 使用 QUICK stop 重新计算风险数量
  → 提交 PAPER IOC
  → 首个合资格完整分钟撮合前再次检查退出条件
  → FILL_APPLIED
  → 附着成交并登记一次 DEEP 请求
  → 构建/应用 DEEP，失败则保留 QUICK
  → 后续完整 bar 观察 barrier
```

当日买入的 A 股持仓 `available_to_sell=0`，触发后只写观察和 T+1 阻断通知，不生成卖单。重启会从
PAPER journal 与退出哈希链恢复未完成 follow-up；旧待撮合订单缺少 QUICK 时安全终止，不在成交后
补造不存在的买前证据。

## 4. 价格、数量、持仓与风险改进

买入可接受区间固定为“失效位上方第一个合法 tick”到“信号价加预登记 markup 与日涨停价中的较低者”。
撮合 bar 只要触及失效位，整笔 IOC 以信号先失效终止；封死涨停不假设 PAPER 拥有排队优先权。

卖出复核记录最低接受价、标准非 ST 日价格带、跳空、跌停队列、成交量和 T+1 条件。当前 PAPER
仍不生成卖单，但报告把未来何时不可成交写清楚。

板块数量规则按交易所区分：主板/创业板通常为 100 股整手；科创板买入最低 200 股，超过 200 后
可按 1 股递增，并保留余股卖出边界。北交所不在当前 PAPER 执行白名单。

固定五持仓上限改为可选配置，默认不设数量硬上限；20% 现金储备、80% 总敞口、20% 单标的、
0.75% 单笔风险和待撮合容量门仍保留。风险距离统一用“最坏许可买入限价减 stop”，不再用较低
信号价低估滑点风险。

## 5. 双轨生产 LLM

新增/扩展：

- [`services/adversarial_macro.py`](../../src/gribuki_trade/services/macro/adversarial_macro.py)
- [`services/llm_production.py`](../../src/gribuki_trade/services/llm/llm_production.py)
- [`storage/adversarial_audit.py`](../../src/gribuki_trade/storage/execution/adversarial_audit.py)
- [`ports/llm_analyzer.py`](../../src/gribuki_trade/ports/llm_analyzer.py)

对抗轨角色覆盖催化支持、风险挑战、证据审计、市场状态和执行风险。首轮角色彼此不可见，使用同一
冻结 EvidencePack；后续只接收规范化并明确标为不可信的同伴摘要。每条主张必须引用已知 evidence id
并给出证伪条件；未知引用、角色缺失、schema 错误、超时或关键分歧会 ABSTAIN/WATCH，而不是强迫一致。

生产双轨会并行执行原单分析器和对抗系统。对抗结论是生产优先轨；跨轨实质冲突会降级。角色请求
哈希、证据哈希、模型身份、轮次、usage、终止原因、选择结果写入独立 SQLite 哈希链，不保存秘密、
原始异常或思维链。

生产装配已经覆盖 PAPER 盘中复核、普通研究、收盘研究、盘后深研和实盘保护。PAPER journal、盘中
提醒、单标的研究、持仓复核和日报都保留两轨结论、评分、模型、采用轨和审计摘要；历史记录缺字段
时明确写“未携带”，不补造。

盘前冻结上下文从单一 selected 结果升级为完整双轨：新上下文必须同时验证 baseline、adversarial、
生产采用轨、模型身份、时点和审计 SHA，缺任一项即失败关闭；旧 journal/manifest 只做原样兼容恢复，
不会补造旧结论。退出 DEEP 与 barrier 的即时通知也增加了双轨完整性检查：只有两轨和采用结果一致
才可用于生产；单轨失败时仍分别展示已取得分数与“不可用”，并明确不采用不完整双轨。

## 6. 实盘成交同步与持续保护

新增/扩展：

- [`domain/live_records.py`](../../src/gribuki_trade/domain/live_records.py)
- [`storage/live_records/live_records.py`](../../src/gribuki_trade/storage/live_records/live_records.py)
- [`services/live/live_trade_records.py`](../../src/gribuki_trade/services/live/live_trade_records.py)
- [`services/live/live_trade_orchestration.py`](../../src/gribuki_trade/services/live/live_trade_orchestration.py)
- [`services/live/live_protection_inputs.py`](../../src/gribuki_trade/services/live/live_protection_inputs.py)
- [`services/live/live_market_tracking.py`](../../src/gribuki_trade/services/live/live_market_tracking.py)

`live-sync ingest` 只接受白名单 OneBot 私聊、合理消息时窗和严格 `GT-LIVE/1` 语法。新 proposal
必须包含券商委托号 `external_order_id` 与独立成交执行号 `external_fill_id`；同一委托的多个部分成交
使用不同执行号分别记账，跨命令重放相同执行号则拒绝。成交日期由 BaoStock 精确自然日日历验证，
日历不可用、载荷不完整或休市均失败关闭。第一条仅形成 proposal；同一发送者必须回传 24 位指纹
确认。确认事务同时处理：

- 外部成交 ID 去重；
- BUY/SELL 批次与库存；
- 手续费和已实现盈亏；
- append-only 事件；
- BUY 的 durable `BUILD_PROTECTION` 工作项。

因此不会再出现“回复说已生成保护、实际上只有内存 boolean”的不真实状态。进程在确认后崩溃，
保护工作仍可恢复。

为彻底落实“确认 BUY 后立刻分析和跟踪”，确认事务现在先独立提交成交事实，再启动一次有限的
提交后钩子：只 claim 本次 `protection_work_id`，用无需 LLM key 的公开行情/日历输入创建 QUICK，
原子标记可跟踪并排队 DEEP，然后只观察本次 `protection_id` 一轮。首轮若触发 barrier，提醒先
耐久写入 private sender（或显式目标）的本地 outbox；这里不要求 NapCat 网络在线，命令行或 GUI
共享地址可用时才追加一次有限派发。QUICK、跟踪和
outbox 写入都受有限 timeout/持久重试约束，任何下游失败都不会撤销成交，也不会撤销已经公开的
QUICK。顶层回执保持成交成功，`immediate_protection` 嵌套状态明确报告是否 `plan_ready`、DEEP
是否排队、首轮跟踪及 outbox 入箱结果。持续跟踪和 DEEP 仍由应用 runtime 重复调用 `cycle`，没有
增加 OS 常驻编排。

full SELL 在批次归零事务中同时 fence 未完成的 QUICK/DEEP 构建工作；DEEP 完成入口仍复核剩余数量，
提醒入队仍复核剩余数量和当前 `plan_stream_id`。候选已经写入退出计划库但尚未切换 live 指针时发生
full SELL 的故障注入已验证：迟到候选保留供审计但不能激活，关闭工作只关闭原 active 流。

工作租约使用持久 attempt generation 作为 fencing token。续租、完成、失败、QUICK-ready 和生产
指针切换都必须携带当前 generation。每个 DEEP attempt 写独立候选物理流；live 账本中的
`plan_stream_id` 是唯一 active 指针。旧 worker 超时后即使恢复并写出候选，也不能覆盖新 owner 或
让迟到候选进入观察链。

`live-sync cycle` 的固定顺序为：

1. 先跟踪所有 `plan_ready` 持仓；
2. 把退出提醒写入 NapCat outbox 并有限派发；
3. 处理成交关闭工作；
4. 最多领取一个新建保护工作；
5. 冻结行情并原子创建/附着 QUICK、标记可跟踪，同时排队独立 DEEP 工作；
6. 领取一个 DEEP generation，在有限 timeout 内构建候选；
7. 等待期间由有界 tracking pump 持续观察，新提醒立即入箱并派发；
8. 当前 generation 成功完成时才原子切换 active 指针，超时则释放为可重试。

默认 timeout 为 600 秒，pump 每 30 秒一次、最多 20 次；配置校验要求 pump 窗口覆盖整个 timeout。
因此慢 DEEP 不会暂停已有持仓的 stop/target 观察或 NapCat 派发。退出提醒同时展示 baseline 与对抗
评分及生产采用轨。
整条链固定 `execution_authority=false`，没有 broker 端口。

## 7. 报告契约与交付

[`reporting/contracts.py`](../../src/gribuki_trade/reporting/contracts.py) 定义并验证六类生产报告：

- 盘中交易告警；
- 成交与账户回执；
- 持仓持续复核；
- 盘后日报；
- 标的深度研究；
- 系统健康与数据质量。

交付语义被拆清：`DAILY_REVIEW` 要求短摘要和 Markdown 同时存在；成交回执、标的研究和系统健康
按场景选择短文本或 Markdown，因此使用 `SHORT_TEXT_OR_MARKDOWN`，不再用枚举名夸大已交付内容。

实际 PAPER、live、研究、盘后和 NapCat 健康输出均在发送前调用契约 renderer/validator。盘后日报
只发送一条结构完整的短摘要，再上传完整 Markdown；不再把长文切成失去标题和证据上下文的片段。
稳定内部码在审计中保留，用户正文显示中文解释。

PAPER 日报附件不再直接调用 NapCat 并盲重试。按日 `report-artifacts.sqlite3` 先冻结
`DAILY_REVIEW`、目标、文件名与 SHA-256，再事务 claim 并只调用一次上传；调用后的异常、取消和
崩溃进入 `AMBIGUOUS`，重启不得自动重发。`PENDING` 可继续，`SENT` 可在 journal 写入前崩溃后
幂等补事件。旧 `REPORT_UPLOADED` 被绑定为 legacy `SENT` 而不重传，旧失败回执保守迁移为歧义。
CLI 现在分别输出交易监控终态与 `daily_review_delivery_complete`，附件未配置、待处理或歧义时
`ok=false` 且总缺口包含 Markdown。人工核验提供方收件情况后，必须用强确认选择补记 `SENT` 或
只重传一次；授权会持久化，不能被普通重启隐式越过。

只读状态与增强摘要也完成了同一迁移：`status.json` 持久保存附件状态、文本/总投递计数和日报
双交付结论；`ashare-paper-day status` 的顶层 `ok` 仅表示 sidecar 读取成功，另以
`operationally_complete` 表示日报是否真正交付完整。旧 sidecar 只能从 JSONL 保守投影附件状态，
缺少文本终态证据时不会误报完成。

## 8. 新闻、证据与数据源

新闻默认官方源扩展到统计局、央行、证监会、财政部、国家发改委、外汇局、上交所和美联储。
HTML 内解析出的详情链接必须重新通过协议、主机和路径边界校验。公共媒体事件会标记独立来源数量，
未获第二来源或官方材料印证时只能作为“未证实线索”，不能成为对抗轨的硬事实。

搜索发现的 Tavily/SearXNG 只属于采集路由，不计作发布者。lineage 现在稳定保存排序后的 provider、
匹配 URL 和规范化发布者身份：同一 canonical URL 或同一注册发布域的不同 URL 即使被多个 provider
返回也保持 `discovery_hint`；只有官方域，或标题规范化匹配且来自至少两个独立注册发布域时，才形成
`independent_publishers` 确认 basis 并允许继续进入证据门禁。

来源 cursor 保存错误类别、退避窗口和确定性 jitter。half-open 探测通过 SQLite 即时事务和 lease
保证跨进程只有一个 owner，过期 token 不能覆盖新结果。来源 adapter、raw store、事件库或 cursor
写入失败都隔离在单来源，不让 `asyncio.gather` 取消健康来源。

行情继续坚持显式 fallback 身份和时效：分钟线主源失败可回退 Sina，快照可回退 Tencent；陈旧缓存
只能以 degraded 身份使用，不能被包装成新鲜主源。

## 9. GUI 与共享生产配置

新增 [`gui/integrations.py`](../../src/gribuki_trade/gui/integrations.py) 与版本化
[`runtime/integration_settings.py`](../../src/gribuki_trade/runtime/integration_settings.py)。集成管理页可以：

- 选择、校验并启动本地 NapCat runtime；
- 查看 OneBot 连通、NapCat 版本和 QQ 登录态；
- 保存 OneBot token；
- 保存 DeepSeek/OpenAI key；
- 选择默认 provider 和各自模型；
- 后台执行模型健康检查；
- 只停止当前 GUI 自己启动的进程。

秘密进入 keyring，非秘密设置原子写入共享 JSON。PAPER、普通研究、盘后研究和 `live-sync cycle`
在 CLI 未显式覆盖时读取同一份 provider/model/OneBot 配置。GUI 不连接券商，也不负责未来整个交易
runtime，符合用户将另行给出运行框架的决定。

## 10. 跨日 PAPER 与并发

[`runtime/paper_account_chain.py`](../../src/gribuki_trade/runtime/paper_account_chain.py) 为新交易日加准备锁，
扫描同账户历史账本、验证公共前缀和每日日期，再通过 SQLite backup + 原子替换克隆最新有效账本。
`ledger-lineage.json` 和账本内不可变 binding 共同绑定账户、session、来源日期、来源事件数和尾哈希；
历史 seal 再绑定该日实际完整事件数和尾哈希。每个历史候选都重放完整哈希链，并用自己的 lineage、
binding、seal、账户和投影日期复核，不再直接相信 JSON。sidecar 删除可从 binding 恢复，clone 后崩溃
留下的无 lineage 账本可由唯一的最近已验证前缀以 `RECOVERED_ORPHAN_CLONE` 显式恢复；损坏或矛盾的
lineage、历史 seal 变化、分叉、符号链接、账户不一致或并发冲突均失败关闭。seal 提交同时启用 SQLite
追加拦截；克隆提交新日 binding 后才恢复新日写入，避免已验真的旧日又被 runner 追加。首个旧账本
只允许走带明确 origin 的可审计迁移规则。

T+1 批次随账本跨日：昨日买入在下一有效交易日转为可卖；当日新买仍不可卖。PAPER 日 manifest
保存 lineage 与交易日历 revision，恢复不能静默换策略或换账户。

## 11. 退出策略离线评价与冻结入口

新增：

- [`strategy_lab/exit_policies.py`](../../src/gribuki_trade/strategy_lab/exit_policies.py)
- [`strategy_lab/exit_evaluator.py`](../../src/gribuki_trade/strategy_lab/exit_evaluator.py)
- [`strategy_lab/exit_experiment_io.py`](../../src/gribuki_trade/strategy_lab/exit_experiment_io.py)

评价器重放 A 股 T+1、停牌、一字跌停、同 bar stop-first、追踪止损、时间退出、滑点、佣金、最低
佣金、印花税和过户费。样本按入场交易日整组切分，purge/embargo 覆盖标签窗口；只用 validation
选择参数，锁定后才读取 holdout。非法候选也进入 trial registry，防止只展示成功结果。

新增 `strategy-exit-evaluate` CLI 后，冻结数据和实验规范可以重复执行而不写临时代码。入口拒绝
未确认、重复 JSON 键、未知字段、float JSON、内容哈希漂移、右删失、符号链接和隐式覆盖；输出
绑定数据、配置、walk-forward 计划与 registry 哈希，并固定无发布、无执行权限。

## 12. OS 编排、临时目录与秘密文件

本轮删除了 Windows Task Scheduler 安装路径、PAPER watchdog、相关启动/测试文档和隐式 NapCat
PowerShell 启动器。公开 CLI 不再解析 schedule install/status，当前系统没有对应任务。应用未来只
接收用户定义的统一 runtime。

根目录历史 `.tmp*` 已盘点；唯一有价值迁移证据进入带清单和 SHA-256 的 runtime archive，其余可再生
测试目录安全清理。默认临时根为 `runtime/tmp`，可通过环境或显式参数覆盖；根 `/tmp/`、`/.tmp*/`、
`/runtime/`、`/secrets/` 均被 Git 忽略。清理脚本只能处理自己精确枚举和验证的临时对象。

`secrets/` 下用户明文文件从未纳入 temp resolver、清理脚本、GUI 凭据迁移或测试扫描。本轮没有读取、
移动、删除、清空或覆盖这些文件。

## 13. 中文注释与工程质量

本轮把 `src/gribuki_trade` 中说明性的模块、类、函数 docstring 和行注释统一为中文。错误码、协议名、
schema、第三方 API 名、类型指令和机器检查指令保持稳定，避免为了翻译破坏兼容性。自动回归会使用
AST 与 tokenize 发现“不含中文但包含英文解释”的新增注释，防止后续回退。

测试覆盖新增领域不变量、SQLite 篡改/并发/崩溃恢复、PAPER 真 runner 链、真实 transport fake、
GUI 离屏 worker、报告 golden、新闻多源隔离、跨日 lineage、live generation fencing 和冻结实验 CLI。
最终质量结果以本文第 15 节为准。

## 14. 兼容性和迁移边界

1. 历史 PAPER 持仓若当时从未创建退出计划，系统不会用今天的数据倒填一个伪 PIT 计划；报告会明确
   标出“未监控”。
2. 历史 LLM 事件缺少双轨字段时按旧记录恢复，但报告明确写未携带；不补造模型结论。
3. 风险策略从固定五仓迁移到无数量硬上限时，要求无待撮合、无未应用成交、账本与 journal 成交一致，
   并写人工策略变更事件；已有成交不重算。
4. PAPER `DAY_ABORTED` 仍要求显式恢复授权；不能删除旧目录或启动第二个 run 绕过终态。
5. live proposal/confirm 协议、外部成交 ID 和工作 generation 属于持久兼容面，未来 runtime 必须复用，
   不能另建一套无审计状态。

## 15. 验证记录

本轮最终交付前执行以下门槛：

```powershell
.\.venv\Scripts\python.exe -m compileall -q src/gribuki_trade
.\.venv\Scripts\ruff.exe check conftest.py src tests
.\.venv\Scripts\mypy.exe src
.\.venv\Scripts\python.exe -m pytest --temp-dir runtime/tmp/final -q
git diff --check
```

此外单独核对：

- 六类报告生产调用与 golden；
- 所有语义 LLM 构造点均经过生产双轨工厂；
- PAPER QUICK→订单→成交→DEEP→T+1 barrier→重启；
- live proposal→confirm→QUICK-ready→慢 DEEP→跟踪→提醒→SELL；
- 旧 work lease 被新 owner 接管后，迟到 worker 最多留下不可激活的候选物理流；权威
  `plan_stream_id` 指针、work 终态和生产观察状态均由当前 generation fence；
- 跨进程新闻探测、账本准备和 outbox claim；
- 全仓中文说明性注释扫描；
- 文档相对链接；
- 根目录 `.tmp*` 数量为零；
- 仓库无任务注册/watchdog/隐式 NapCat 启动代码；
- `git status -- secrets` 无本轮改动。

2026-08-15 冻结工作树的最终结果：

- `pytest -ra --temp-dir runtime/tmp/frozen-full-regression-260815-r4/full-scratch`：共收集
  1,497 项，`1492 passed, 5 skipped`；
- 五项 skipped 均来自当前 Windows 测试账户不能创建符号链接，分别覆盖 NapCat 配置目标、
  OneBot 附件、PAPER 两类 lineage 路径和退出实验输入；相关生产代码仍显式拒绝 symlink；
- `compileall`、Ruff、`mypy src`（200 个源码文件）、全仓中文注释守卫和
  `git diff --check` 全部通过；
- README 与 `docs/**/*.md` 共核对 20 个 Markdown 文件、236 条本地链接引用（121 个唯一目标），
  缺失为零；
- 根目录 `.tmp*`/`tmp` 为零，系统相关计划任务为零，`git status -- secrets` 无本轮改动。

## 16. 本轮结束后仍然诚实保留的限制

以下不是未接线占位，而是不能用离线代码伪造的外部验收：

- QUICK/DEEP 的 ATR、R 倍和持有期参数还没有足够 A 股样本外证据，必须继续标记未校准；
- 公开网页行情不是交易所 tick、L1/L2 或真实盘口队列；PAPER 成交不能解释为真实可成交性；
- 多角色共享 provider/model 时仍可能产生相关错误，多角色数量不等于真值；
- NapCat 扫码、真实 QQ、真实模型网络和多日文件交付需要用户本机验收；
- OneBot 常驻入站、跨交易日常驻循环和应用生命周期等待用户给出的统一运行框架；
- live 观察链没有券商接口，卖出提醒后仍由用户在券商操作并同步成交事实；
- 策略实验没有生产晋升权限，优秀 holdout 结果也必须经过独立复核、shadow 和人工发布。

项目总体结构与运维边界见 [项目交接文档](../PROJECT_HANDOFF_260814.md)，交易逻辑的通俗说明见
[交易策略指南](../TRADING_STRATEGY_GUIDE_260814.md)。
