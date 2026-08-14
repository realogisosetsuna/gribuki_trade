# 后续开发计划 260814：工程评估、落地状态与验收路线

> **历史归档（2026-08-15）**：本文保留一次实施评估及其更早研究基线，内部有意混合了两个时期的状态，因此不再是“权威状态页”。当前行为以代码、测试、[README](../../README.md) 和 [项目交接文档](../PROJECT_HANDOFF_260814.md) 为准。

评估日期：2026-08-14

原始需求：[后续开发计划260814.md](%E5%90%8E%E7%BB%AD%E5%BC%80%E5%8F%91%E8%AE%A1%E5%88%92260814.md)

既有交易系统路线：[ROADMAP.md](ROADMAP.md)

本文不覆盖原始需求，而是把其中六类能力转换为可实施、可测试、可审计的工程计划，并记录
当前代码的真实成熟度。文中的“研究候选”“异常”“评分”都不是收益概率、订单或下单授权。

## 0. 当前增量状态（本文件的权威状态页）

本章反映 2026-08-14 当前工作树的真实状态；后文保留的是较早的长篇研究基线。若二者发生冲突，
以本章为准。尤其是后文 R5 中曾提出的 Windows 任务计划/PowerShell watchdog 路线已经废止，
不得重新安装或作为运行前提。未来常驻编排只进入用户稍后给出的应用运行框架。

状态术语：

- **核心已落地**：领域模型、服务或持久化已经实现并有离线测试；不表示已完成真实交易日验收。
- **部分接入**：可复用能力已经存在，但还没有贯通当前 PAPER 日运行、通知或 GUI 的完整调用链。
- **待运行框架**：业务边界已经确定，但进程生命周期、入站网络监听或常驻调度刻意不在操作系统层实现。

| 原始计划的六项需求 | 合理性判断 | 当前状态 | 下一道验收门 |
|---|---|---|---|
| 1. 买卖信号与两阶段退出计划 | 条件合理；保护线应是可审计 barrier，不是“预测收益” | 已接入 PAPER 与实盘同步生产链 | 下一真实交易日做全天 soak；参数继续保持未校准标识 |
| 2. 多角色 LLM 对抗分析 | 可提高反证覆盖，不保证真值或收益 | 所有生产语义 LLM 已双轨并行；对抗轨优先、baseline 留档 | 用真实 provider/真实交易日持续统计延迟、弃权率和分歧率 |
| 3. 报告结构与 Markdown 推送 | 合理；长报告和即时告警应分流 | 六类契约已接入实际报告、事件通知与盘后文件交付 | 扩充长期 golden 样本与真实 NapCat 多日交付统计 |
| 4. 行情/新闻来源稳定性 | 必须做，但重试不能掩盖陈旧或错误数据 | 官方源扩展、媒体独立佐证、持久退避及跨进程探测租约已接入 | 连续交易日统计覆盖率、延迟、修订率和 stale 拒绝率 |
| 5. NapCat 与 LLM 配置 GUI | 合理；网络和凭据操作必须离开 GUI 主线程 | GUI 可管理 NapCat、DeepSeek/OpenAI，生产 CLI 消费同一共享配置 | 本机扫码登录、真实模型和 GUI 重启恢复仍需人工验收 |
| 6. 跨日 PAPER 与独立实盘记录 | 必须隔离；QQ 只能同步已成交事实 | 跨日 lineage/并发已加固；`live-sync` 已接成交、保护分析、单轮跟踪和提醒 | 等用户给出应用 runtime 后调用现有入口形成常驻生命周期 |

### 0.1 两阶段退出计划：合理，但必须修正“预测收益”的表述

原计划中“快速计算预期止盈/止损、成交后再深度更新”的时序是合理的，前提是把止盈价理解为
**策略预先注册的价格 barrier**，而不是可靠的期望收益、到达概率或价格预测。技术指标可以定义
风险几何和失效条件，却不能单独证明未来收益。

已经落地的边界如下：

1. **下单前 QUICK**：使用已完成、满足 point-in-time 的 bar，组合技术失效位、最近 swing low 和
   robust ATR；按最坏可成交买价（通常是买入限价）计算每股初始风险，再以离散的 R 倍数形成
   take-profit barrier。时间 barrier 由具备交易日历的调用方提供，算法不会把自然日冒充交易日。
   代码位于 [exit_planning.py](../../src/gribuki_trade/features/exit_planning.py)。
2. **成交后 DEEP**：不可变版本模型已支持成交后的确认/降级计划以及旧版本替换。DEEP 可以吸收
   更多技术面和经验证据约束的 LLM 结论，但 LLM 不能自由生成订单或取消确定性保护。
   对多头仓位，DEEP **不得下调 stop（放宽风险）**，也不得延后时间 barrier；深度分析失败时应
   继续沿用 QUICK，而不是使持仓失去保护。领域约束见
   [exit_plans.py](../../src/gribuki_trade/domain/exit_plans.py)。
3. **审计存储**：计划创建、替换、附着成交、barrier 观察和退出信号采用 append-only 哈希链，
   见 [exit_plans.py（存储）](../../src/gribuki_trade/storage/exit_plans.py)。
4. **离线优化**：`strategy_lab` 已增加有限、预注册的退出参数空间和 PIT trace；观察到 barrier
   与实际可执行成交被分开记录，T+1 阻断不会被伪装为成交，同一 OHLC bar 同时触碰止盈/止损时
   采用保守的 stop-first。见
   [exit_policies.py](../../src/gribuki_trade/strategy_lab/exit_policies.py)。
5. **生命周期服务**：broker-free 服务已经把 QUICK 创建、成交附着、单次 DEEP 请求、单调替换、
   深研失败留用旧计划以及完成 bar 的 barrier/退出信号串成幂等流程。T+1 可卖数为零仍完整记录，
   但服务没有 broker/order-store 依赖且永不自行创建订单。见
   [exit_plan_lifecycle.py](../../src/gribuki_trade/services/exit_plan_lifecycle.py)。

上述生命周期已经接入 PAPER 日运行和 `live-sync`：买单前必须先落 QUICK，撮合线若已触发
barrier 就拒买；fill 后附着成交并登记/执行 DEEP，多时间框架输入与双轨 LLM 评分共同进入
确定性映射；失败保留 QUICK；T+1 当日只记录和提醒，不生成卖单。计划可跨重启、跨交易日恢复。
`strategy_lab` 也已加入成本感知退出 evaluator、purge/embargo walk-forward、最终 holdout 和不可变
trial registry。但当前仍没有足够样本证明任何 R 倍数或 ATR 参数有样本外优势，实验结果固定为
research-only，不会在线调参或自动发布策略。

冻结实验可通过 `strategy-exit-evaluate --confirm RESEARCH_ONLY` 重复运行。入口在读取输入前验证
确认词，严格绑定 episode 数据内容哈希、实验规范、完整 walk-forward 计划与 trial registry，结果
采用原子文件写入并明确 `promotion_authorized=false`、`execution_authority=false`。因此这一入口
解决了“评价器只能靠临时代码调用”的问题，但不会把研究表现自动带入 PAPER 或实盘。输入契约与
命令示例见 [STRATEGY_LAB.md](../STRATEGY_LAB.md)。

此外，盘中 PAPER 风控已经用“买入限价到失效价”的最坏距离做头寸风险预算，避免用较低的信号价
低估滑点后的每股风险；买卖两侧仍各自保留交易所价格带和可接受成交走廊。实现见
[ashare_intraday_paper.py](../../src/gribuki_trade/services/ashare_intraday_paper.py)。

### 0.2 多角色 LLM：先 SHADOW，不能因为预算不限就取消边界

参考 `ai-berkshire` 的角色分工、但不照抄其投资委员会叙事是合理的。多角色对抗能强制暴露反证、
来源依赖、市场状态和执行风险，但多个角色仍可能共享同一模型偏差；辩论轮数增加也会放大延迟、
费用和错误共识。因此“所有语义 LLM 调用采用同一证据约束协议”是迁移方向，健康检查、解析、
风险计算等确定性函数则不应为了形式统一而调用 LLM。

[adversarial_macro.py](../../src/gribuki_trade/services/adversarial_macro.py) 已实现兼容现有
`MacroAnalyzer` 的有界协议：首轮各角色只看同一冻结 `EvidencePack`，后续只能看到经过规范化的
不可信同伴论点；引用必须指向已知 evidence id，并提供可证伪条件。当前深度档位为：

| 档位 | 角色/轮次上限 | 单角色超时 | 适用场景 |
|---|---:|---:|---|
| `FAST` | 2 角色 / 1 轮 | 15 秒；case 24 秒 | 盘中高价值候选的低延迟复核 |
| `STANDARD` | 3 角色 / 2 轮 | 60 秒 | 持仓复核、普通深研 |
| `DEEP` | 5 角色 / 3 轮 | 180 秒 | 收盘后深度研究 |

“无 LLM 预算上限”只允许把会话调用预算设为 `None`，**不会取消固定角色、轮数、超时、证据和
失败关闭边界**。兼容层仍保留三种发布模式供研究比较：

- `BASELINE`：只返回原单分析器结果，用于对照测试。
- `SHADOW`：同时运行对抗协议但只返回 baseline；慢对抗任务不会阻塞 baseline。
- `ENFORCE`：单独使用对抗聚合结果的兼容模式。

生产命令没有直接使用上述单轨发布开关，而是使用 `ProductionDualTrackMacroAnalyzer`：baseline 与
对抗轨在同一 EvidencePack 上并行，生产选择优先采用对抗轨；对抗失败时失败关闭，跨轨实质冲突
降级，baseline 始终保留供报告对照。盘中外层为 28 秒，FAST case 为 24 秒，底层单调用为 15 秒。
每轮角色的 prompt/evidence/model/usage/终止原因和最终选择写入独立 SQLite 哈希链；PAPER journal
也保存双轨投影并能在恢复时重建。当前尚需真实 provider 和真实交易日的持续统计，不能用一次
主观观感证明多角色一定优于 baseline。

### 0.3 报告：六类稳定契约，长文 Markdown 优先

[contracts.py](../../src/gribuki_trade/reporting/contracts.py) 已把报告收敛为六类稳定契约：

| 类型 | 中文用途 | 默认交付 |
|---|---|---|
| `INTRADAY_ALERT` | 盘中交易告警 | 短文本 |
| `EXECUTION_RECEIPT` | 成交与账户回执 | 短文本或 Markdown（按场景选择） |
| `POSITION_REVIEW` | 持仓持续复核 | Markdown |
| `DAILY_REVIEW` | 盘后日报与次日知识基线 | 短文本 + Markdown |
| `INSTRUMENT_RESEARCH` | 标的深度研究 | 短文本或 Markdown（长研优先 Markdown） |
| `SYSTEM_HEALTH` | 系统健康与数据质量 | 短文本或 Markdown（按场景选择） |

这一区分保留了盘中消息的时效性，同时让长报告优先以 Markdown 文件承载结构化证据。内部稳定码
仍保留在不可变事件和审计附件中，主报告通过 `humanize_internal_code()` 转成中文解释；未知码
显示为“尚未分类、请查看审计事件”，而不是直接把大写 token 倾倒给用户。盘后报告已开始使用
该转换，见 [post_close.py](../../src/gribuki_trade/reporting/post_close.py)。

只有 `DAILY_REVIEW` 的契约明确要求短摘要和完整 Markdown 同时交付；成交回执、标的研究与
系统健康允许调用方依据时效和篇幅选择一种格式。该语义由 `SHORT_TEXT_OR_MARKDOWN` 明确表达，
避免把“支持两种格式”误报为“每次都已发送两份附件”。

六类契约现已进入实际生产输出：PAPER/live 盘中告警、成交/账户回执、盘后逐持仓复核、
PAPER 与盘后日报、单标的深研、PAPER/NapCat 健康消息均在发送前验证类型。盘后只发一条
`DAILY_REVIEW` 短摘要，再上传完整 Markdown，不再把长报告拆成失去上下文的文本片段。报告同时
展示 baseline/对抗结论、评分、证据覆盖和审计哈希；旧历史缺字段时明确写“未携带”，不补造。
剩余验收是长期维护六类 golden 样本并统计真实 NapCat 多日交付，不是生产调用尚未接入。

### 0.4 数据与新闻韧性：持久退避、来源隔离和明确降级

稳定性改进必须保持 fail-closed：重试成功率不能以继续使用陈旧、字段漂移或来源身份错误的数据
为代价。当前能力分两层：

- 通用 HTTP 采集已经处理条件请求、有限重试、`Retry-After`/退避和持久 cursor，见
  [http.py](../../src/gribuki_trade/ingest/http.py)。
- 新闻编排对每个来源独立运行；adapter 以外的异常、非法输出或来源 ID 不匹配会写入可跨重启的
  cooling cursor，采用有上限的指数退避和来源特定 jitter。熔断窗口内直接跳过，窗口到期只允许
  新一轮探测；SQLite `BEGIN IMMEDIATE` 探测租约保证不同进程也只有一个 half-open owner，
  token fence 禁止过期 owner 覆盖新状态，不让一个失败来源阻断其他来源，见
  [news_collection.py](../../src/gribuki_trade/services/news_collection.py)。
- 默认官方源已经扩展到统计局、央行、证监会、财政部、国家发改委、外汇局、上交所和美联储；
  所有解析出来的链接都重新经过来源域名/协议白名单。公共媒体事实只有获得独立来源印证后才可被
  对抗轨引用；单一媒体保持“未证实线索”。
- 行情侧已有主源失败后的明确降级：分钟线可回退 Sina，全市场/单标的快照可回退 Tencent，
  受时间上限约束的旧缓存只能以 degraded 身份使用，见
  [akshare.py](../../src/gribuki_trade/adapters/akshare.py)。
- 原始文档、内容哈希、事件去重/修订链继续用于追踪同一新闻的重复与变更；fallback 的来源身份
  必须进入审计，不能把备用源伪装成主源。

这些能力不能证明新闻“精确无误”，也尚未给所有市场数据 adapter 配置统一的跨进程 circuit。
下一步应做连续交易日 soak，统计每源覆盖率、延迟、修订率、fallback 比例和 stale 拒绝次数；
高价值结论应优先采用交易所、公司公告等官方原文，并把聚合转载视为线索而非独立证据。

### 0.5 NapCat 与 LLM GUI：显式管理已实现，后台运行框架仍待定义

GUI 已新增“集成管理”页，接入位置见 [main_window.py](../../src/gribuki_trade/gui/main_window.py)，
具体实现见 [integrations.py](../../src/gribuki_trade/gui/integrations.py)。当前页面提供：

- NapCat/OneBot 本机地址、令牌状态、连通性和 QQ 登录信息检查；
- 选择并校验本地 NapCat runtime，启动/停止**仅由当前窗口创建**的进程，不会终止外部实例；
- DeepSeek/OpenAI API Key 写入系统凭据库、默认 provider、各自模型选择和模型健康检查；
- 所有网络、凭据和进程准备工作通过后台 worker 执行，避免阻塞 Qt 主线程；错误文本经过净化，
  不把 token、URL 查询参数或本机路径回显到界面。

该页面只负责可见配置与健康管理，不连接券商、不发交易委托，也不是常驻交易调度器。非秘密配置
原子写入 `runtime/config/integrations.json`，PAPER、普通研究、盘后深研和 `live-sync cycle`
在命令行未显式覆盖时读取同一 provider/model/OneBot 地址；GUI token/key 与 CLI 使用同一 keyring
名称。仍需在用户本机完成 NapCat 扫码/登录、真实 provider 和应用重启后的人工验收。

### 0.6 跨日 PAPER 与独立实盘观察账本

#### 0.6.1 PAPER 账户连续性

[paper_account_chain.py](../../src/gribuki_trade/runtime/paper_account_chain.py) 已采用“每日目录隔离、
累计账本克隆”的方式延续同一 PAPER 账户。新交易日前会在跨进程锁内逐日重放同一账户的完整
事件哈希链，并核对每一天自己的账户、目录日期、账本投影日期、lineage 来源日期/事件数/尾哈希，
以及账本内不可变的 lineage binding 和历史 event-count/tail seal。缺失 sidecar 只有在账本内 binding
或最近已验真历史前缀能唯一证明来源时才会恢复；损坏、互相矛盾、历史分叉和 seal 变化一律失败关闭。
真正没有同账户先前候选的旧账本只可通过 `LEGACY_EMPTY_SESSION_LEDGER` 或
`LEGACY_SESSION_LOCAL` 做一次显式、可审计迁移，不能成为后续日期绕过来源校验的旁路。
seal 与 SQLite 追加拦截触发器在同一事务内生效：旧日一经封存就不能再由普通 PAPER writer 追加；
克隆账本只有提交新日 binding 后才恢复新日写入能力，从存储层关闭“验证后、克隆前后又写旧日”的窗口。

克隆采用 SQLite backup + 原子替换，并按“账本内 binding 先提交、JSON sidecar 后提交”的次序落盘。
即使进程在 clone 后或两次提交之间崩溃，下一次启动也会保留 orphan clone，并由已验证的唯一历史
前缀以 `RECOVERED_ORPHAN_CLONE` 明示恢复提交，无需人工删除账本；当前 sidecar 被删除会从 binding
恢复，被篡改则与 binding 冲突并拒绝运行。真实双子进程互斥、clone 后强制崩溃恢复、历史 lineage
缺失/损坏/矛盾、历史 seal 变化和符号链接拒绝均有故障注入测试。

该准备步骤已经接入 PAPER-day CLI，lineage 进入 run manifest，因而现金、持仓、费用、T+1 批次
和历史成交能够跨交易日累计，同时避免盘中 writer 与盘后 reader 共用一个可写 SQLite 文件。
下一验收是连续多个真实交易日的 rollover/重启/盘后重放 soak；结构性的分叉、残缺账本、崩溃和
重复启动已由自动化测试验证失败关闭或确定性恢复。这仍不是券商资金账户。

#### 0.6.2 QQ 同步实盘成交事实

实盘记录与 PAPER 采用独立编排和独立哈希链 SQLite 账本：

- [live_records.py（领域）](../../src/gribuki_trade/domain/live_records.py)
- [live_trade_records.py](../../src/gribuki_trade/services/live_trade_records.py)
- [live_records.py（存储）](../../src/gribuki_trade/storage/live_records.py)

`live-sync ingest` 会把 OneBot 私聊事件规范化，只接受白名单发送者、私聊 friend 消息、合理时间窗和严格的
`GT-LIVE/1` 结构化字段。第一条消息仅形成 proposal；同一发送者必须用系统返回的指纹发送第二条
`GT-LIVE-CONFIRM/1`，确认后才把“券商已经成交”的事实写入账本。取消、消息重放、实际费用、
平均成本、已实现盈亏和卖出超过已记录持仓均有明确处理。新确认的 BUY 在成交事务中写入原子
库存/批次、外部成交去重约束和 durable `BUILD_PROTECTION` 工作项；事务提交后，`ingest` 只领取
本次工作号，用无需 LLM key 的公开行情/日历输入尝试建立 QUICK，并对本次保护批次执行一轮有限
跟踪。首轮 barrier 提醒先耐久进入本地 outbox；行情、跟踪或通知故障只留下嵌套失败回执和可恢复
任务，绝不回滚成交或已完成 QUICK。新 proposal 强制使用
独立 `external_fill_id` 标识每一次券商成交执行，`external_order_id` 只表示共同委托；因此同一委托
的多次部分成交可分别确认，相同执行号跨命令只能落账一次。CLI 通过 BaoStock 精确自然日日历验证
成交日期，供应商异常、结构不完整和休市日均失败关闭。

`live-sync cycle` 已完成一次有限应用编排：领取保护工作，使用真实公开行情、交易日历和生产双轨
LLM 构建 QUICK/DEEP，恢复退出计划，观察完成 bar 的 barrier，把可卖时机以契约化短文本写入
专用 NapCat outbox，并派发到精确目标；SELL 确认会按买入批次原子核销并关闭保护。全链没有 broker
接口或订单权限。每轮固定先观察已经具备 QUICK 的持仓并派发提醒，再关闭成交、最后最多领取一个
新建保护任务；QUICK 在同一事务中标记可跟踪并排队独立 DEEP 工作。等待 DEEP 的完整有界窗口内，
tracking pump 继续观察并派发新提醒。每个 DEEP generation 写独立候选物理流，只有当前 live lease
owner 才能原子切换 `plan_stream_id` 权威指针；过期 worker 的迟到候选不会成为 active。full SELL
会在核销事务里 fence 尚未完成的 QUICK/DEEP 构建；DEEP 完成与提醒入队再次复核剩余数量，提醒还
必须匹配当前 `plan_stream_id`。候选写入与 active 指针切换之间强制卖出的故障注入测试证明迟到候选
不会复活零持仓。尚未接入的是 OneBot 反向 WebSocket/HTTP 常驻监听与循环生命周期；用户已明确
稍后提供运行框架，所以当前只提供可重复调用的 `ingest/status/cycle`，不安装系统服务。
即时钩子不会在线调用 DEEP；NapCat 未配置时只耐久入箱，命令行或 GUI 共享地址可用时追加一次
有限派发，失败
不改变成交/QUICK 结论。DEEP、持续监看和后续 outbox 网络派发仍由该未来 runtime 周期调用
`cycle` 完成。

### 0.7 明确废止项和不可破坏约束

1. **不得重新安装 Windows Task Scheduler 任务。**此前的系统任务已取消，PowerShell watchdog、
   任务安装器、NapCat 隐式启动脚本及其测试/操作文档已经从当前树删除。未来由应用内部
   `RunCoordinator`/用户指定 runtime 管理交易日历、午休、恢复、互斥和停止。
2. **不得自动清理明文 API 文件。**仓库中的 `secrets/deepseek API.txt` 以及用户以后明文保存的
   API 文件不得被临时目录清理、凭据迁移或安全扫描自动删除、移动、清空或覆盖。GUI 写入系统
   凭据库只是新增一种配置方式，不改变这条约束。
3. **临时目录治理与 secrets 完全分离。**专用 temp root 只能管理自己创建并有明确归属的临时
   产物；不得把 `secrets/`、用户文档或未知根目录文件纳入清理范围。

### 0.8 下一轮统一验收顺序

1. 在下一个真实交易日运行 PAPER 双轨与退出计划全天 soak，核对 QUICK 永不缺席、DEEP 失败保留
   QUICK、恢复/午休/跨日不重复工作、T+1 只记录不伪造成交。
2. 用冻结历史数据运行退出策略 walk-forward，记录成本敏感性、样本外回撤和候选稳定性；任何结果
   都必须经过人工发布流程，trial registry 禁止自动晋升。
3. 对双轨 LLM 持续统计 baseline/对抗分歧、弃权率、schema 失败、延迟和 token；生产门保持技术、
   价格、资金和执行规则优先。
4. 对新闻、官方源和行情 fallback 做连续交易日 soak；没有 source revision、PIT 时点和覆盖率的
   输出不得进入 DEEP 结论。
5. 在本机验收 GUI 的 NapCat 登录、DeepSeek/OpenAI 健康、进程所有权、共享配置和重启行为。
6. 等用户给出应用 runtime 后，让它循环调用现有 `live-sync ingest/status/cycle` 并提供 OneBot 入站
   transport；全过程不借助 Windows Task Scheduler 或 PowerShell watchdog。

### 0.9 `RE后续开发计划260814.md` 的 16 项最终对应关系

下表按用户的二次整改文档逐项核对。这里的“完成”表示代码已经进入真实生产调用路径，并具备相称的
持久化、恢复和离线回归；不表示 ATR/R 参数已经具备统计显著性，也不表示真实 QQ、公开网络或下一个
交易日已经被离线测试替代。

| 编号 | 状态 | 当前代码事实 |
|---:|---|---|
| 1 | 已完成 | PAPER 已强制买前 QUICK；`live-sync` 对外部已成交事实立即补建 QUICK；两者均接 DEEP、恢复和成交后 barrier 观察 |
| 2 | 已完成 | 多时间框架 DEEP 生成器已实现并接入；LLM 只选择登记档位，确定性规则映射价格且不放宽 stop |
| 3 | 已完成 | PAPER、普通研究、收盘/盘后研究和 live 保护均经生产双轨工厂；生产优先对抗轨，报告保留两轨 |
| 4 | 已完成可调用链 | 两阶段实盘确认原子记录成交、批次与 durable 工作；`cycle` 构建保护、跟踪并发 NapCat 提醒，不下单 |
| 5 | 已完成 | GUI 管理 NapCat 与 DeepSeek/OpenAI；PAPER、研究、盘后和 live 在 CLI 未覆盖时读取同一共享配置 |
| 6 | 已完成 | 六类报告已有生产 renderer/validator；日报短摘要加 Markdown，其他类型使用明确的 OR 交付语义 |
| 7 | 已完成工程改进 | 官方源扩展、媒体独立佐证、URL 白名单、逐源隔离、持久退避和跨进程 half-open fencing 已接入 |
| 8 | 已完成 | PAPER 新日准备使用真实跨进程锁；逐日 lineage/binding/seal 与账户、session、完整事件链复核；orphan clone、sidecar 删除可确定性恢复，矛盾/分叉/符号链接失败关闭 |
| 9 | 已完成 | live 原子库存/批次、外部成交去重、持久工作和 lease generation fencing 阻止并发超卖与旧 worker 回写 |
| 10 | 已完成 | QUICK 必须在订单前落盘；当前线或撮合线已触发退出条件会拒买；未附着成交不能观察 barrier |
| 11 | 已完成 | baseline/对抗并行；会话调用预算可为无限，但角色、轮次、每调用和 case deadline 有界；live 等待慢 DEEP 时由有界 tracking pump 持续观察并派发提醒 |
| 12 | 已完成当前约定范围 | GUI 完整负责 LLM/NapCat 控制面，不承担用户尚未给出的整个交易 runtime |
| 13 | 已完成 | `src/gribuki_trade`、`tests`、Python 脚本和根 `conftest.py` 的说明性 docstring/注释统一中文，并由 AST/tokenize 回归审计防止回退 |
| 14 | 已完成 | Task Scheduler、独立 watchdog、隐式 NapCat 启动脚本及可达安装入口删除，文档统一为应用内 runtime |
| 15 | 按用户要求忽略 | 未擅自提交；当前工作树、SQLite/事件哈希链、测试与三份交接文档构成本轮审计基线 |
| 16 | 已完成合理改写 | “预期盈利”改为未校准 reward/risk barrier，不声称目标价是收益概率或价格预测 |

第 4 项中的“可调用链”特意区别于“已经有常驻监听器”：OneBot 反向入站与无限生命周期等待用户后续
提供统一应用运行框架，这是明确架构决定，不是用 OS 任务补齐的隐藏缺口。第 7 项也不宣称新闻已经
绝对正确；工程上已经提高官方材料优先级和独立佐证，统计效果仍需连续交易日测量。完整逐项变更和
代码文件索引见 [ROUND_CHANGELOG_260814.md](ROUND_CHANGELOG_260814.md)。

---

以下章节保留此前的长篇研究与路线基线，便于追溯设计理由；其状态判断和调度建议如与第 0 章冲突，
均以第 0 章为准。

## 1. 结论

原计划方向合理且有必要，建议继续实施，但必须按依赖顺序推进，不能从公开行情直接跳到
“自动优化权重”或“机器学习挖因子”。合理的顺序是：

```text
可审计的时点数据
    -> 收盘筛选 / 盘中发现
    -> 统一候选事件库
    -> 有界跟踪 / 单标的深研 / 通知
    -> 可重放 PAPER 与统一回测
    -> 离线策略实验和显式发布
    -> 受控因子探索
```

当前已经形成了这条链路的前半段，以及后半段的安全研究骨架：

- 收盘后三层全市场筛选核心、单次 CLI，以及 Top-N 写入候选库已经落地；
- 盘中全市场异常扫描核心、单次 CLI，以及短 TTL 候选写入已经落地；
- 手工、收盘筛选和盘中异动可在统一 append-only 候选事件库中合并；静态自选池与 ACTIVE
  候选可在有限轮跟踪入口合并，也可用 `--candidates-only` 明确只跟踪候选；
- `ashare-research-watch` 可显式合并 ACTIVE 候选并做有限轮、多标的、逐标的失败隔离的研究；
- Top-N/显式标的有界批量收盘深研、动态证券画像和沪深京收盘日线链路已经落地；
- 推荐人工复核已经具备独立的 append-only 状态机；确认研究结论不会创建 PAPER/LIVE 委托；
- 收盘筛选和盘中扫描具有输出与 lineage 运行档案，但完整原始输入快照仍需单独归档；
- A 股 PAPER 的成交账本、T+1 可卖数量、费用、幂等与重放核心及 CLI 已落地；限价委托与
  bar-run 另有 append-only 事件库、单写者租约和跨库恢复 saga；
- `strategy_lab` 已实现冻结 manifest、purge/embargo walk-forward、独立最终留出集、
  有约束的权重网格、安全因子 DSL、受控候选生成/冗余筛除、单证券 A 股日线评价器和
  append-only 实验存储。

仍未完成的关键闭环是：完整原始市场快照和排名历史归档、交易日历感知的
常驻编排、横截面组合级 A 股回测器，以及从实验结果到生产
配置的人工审核发布流程。因此目前适合称为“可运行的研究基础设施”，还不能称为已经完成的
自动进化交易系统。

## 2. 架构判断

### 2.1 原计划与既有 ROADMAP 的关系

原计划重点是研究闭环，现有 [ROADMAP.md](ROADMAP.md) 重点是交易核心、OMS、风险和未来券商
适配，两者互补而不互相替代：

```text
研究数据 -> 筛选 -> 候选 -> 深研 -> 推荐 -> 事后评价
                                      |
                                      v
                              人工复核 / PAPER
                                      |
                                      v
未来 LIVE：TargetPosition -> Risk -> OMS -> BrokerAdapter
```

研究模块不得直接导入券商 SDK，也不得绕过 `Risk -> OMS` 边界。PAPER 结果可以成为策略评估
数据，但不能反向触发在线自动调参，更不能因为历史表现较好而自动进入 LIVE。

### 2.2 建议保留的工程原则

1. **端口/适配器隔离**：AKShare、BaoStock、新闻、LLM、通知和未来券商都在适配器侧；因子、
   候选生命周期、账本和策略实验保持纯领域逻辑。
2. **Point-in-Time 优先**：每份数据至少保留 `available_at`、`observed_at/first_seen_at`、
   source revision；回放只能读取当时已可见内容。
3. **append-only 审计**：候选、推荐、研究运行输出、人工复核、PAPER 成交、实验结果都追加，
   不原地改写历史。
4. **失败关闭**：陈旧、覆盖不足、未来数据、字段缺失或模型证据无效时降级或弃权，不能把
   缺失值补成“中性”后继续发布入场信号。
5. **研究、PAPER、LIVE 三态隔离**：研究评分不是委托；PAPER fill 不是券商成交；LIVE 必须
   经过既有 OMS、风控和显式解锁。
6. **确定性先于 LLM**：便宜的规则和技术因子先筛选，LLM 只处理已裁剪证据和高价值候选。
   这样可控制延迟、费用、非确定性和消息轰炸。
7. **离线优化、显式发布**：权重和因子只能在冻结数据上离线实验，经独立留出集、shadow 和
   人工审核后发布版本；不做根据当天盈亏自动改权重的在线学习。

## 3. 功能落地状态总览

状态定义：

- **核心已实现**：领域逻辑、持久化或 CLI 已存在，并有离线测试；不等于生产验收完成。
- **部分实现**：已有可复用能力，但关键编排、数据或验证缺失。
- **未实现**：只有设计方向，尚无可用闭环。

| 原计划能力 | 当前状态 | 已落地的核心 | 主要缺口 |
|---|---|---|---|
| 收盘全市场筛选 | 核心已实现 | 三层漏斗、硬过滤、历史因子、横截面排名、Top-N、候选库写入 | 全量历史排名/因子审计库、PIT 历史股票池、定时任务 |
| 盘中全市场发现 | 核心已实现 | 当前交易时段单次异常扫描、覆盖/时效门禁、Top-N、短 TTL 候选 | 周期编排、历史快照归档、运行去重/背压、公开网页源的长期稳定性验证 |
| 统一候选名单 | 核心已实现 | 多来源合并、优先级、TTL、ACTIVE/COOLING/EXPIRED/REMOVED、PIT 重放 | GUI、运行队列、交易日 TTL；北交所分钟链仍不在覆盖承诺内 |
| 候选实时跟踪 | 部分实现 | 有限轮多标的研究、逐标的失败隔离、ACTIVE 候选接入、outbox/QQ 通知能力 | 常驻调度、优先级调度、休眠恢复、宏观调用预算、端到端运行指标 |
| 特定股票深度分析 | 核心已实现 | 单标的盘中研究、单/多标的有界收盘深研、动态画像、沪深京收盘日线、技术/宏观证据门禁、报告与通知 | 动态元数据长期稳定性、复核与通知/批处理自动接线、常驻任务编排 |
| 推荐人工复核 | 核心已实现 | append-only 状态机、PIT 查询、过期投影、证据与候选 provenance、幂等/冲突保护 | 与报告/通知 callback 的自动接线、GUI、运行监控；确认不会自动下单 |
| 研究运行档案 | 部分实现 | 收盘筛选/盘中扫描的规范化输出、完整规范化配置及哈希、来源 revision、状态/时间、append-only 幂等存储与查询 CLI | 完整原始输入、完整漏斗/历史因子快照、批量深研接线、无网络重放 |
| A 股 PAPER 与保守撮合 | 核心已实现（分层） | 持久账本/T+1/费用/重放/CLI；六态限价委托、next-bar、部分成交；append-only order/run、单写者租约与跨库崩溃恢复 saga | durable 撮合 CLI/GUI、公司行动、停复牌/价格限制档案、盘口/集合竞价模型 |
| A 股统一回测 | 部分实现 | 成本模型、推荐事后评价、保守日线撮合、冻结单证券 long/cash A 股评价器 | 横截面组合回放、PIT 股票池、真实成分/退市、公司行动和容量模型 |
| 权重优化 | 研究骨架已实现 | 冻结 manifest、walk-forward、purge/embargo、holdout、simplex 约束、多成本情景、单证券 A 股评价器 | 冻结数据集构建器、横截面/组合评价、统计校正、模型注册/审核/发布、长期 shadow 数据 |
| 技术面探索引擎 | 研究骨架已实现 | 安全 DSL、受控模板候选生成、搜索预算/复杂度门禁、规范化去重、开发集相关性筛除、拒绝留痕、可接 A 股评价器 | 行业/规模中性化、IC/换手冗余治理、PBO/DSR、横截面样本外复验 |

## 4. 逐项可行性与实现边界

### 4.1 收盘全市场筛选

可行性：**高**。日频策略允许使用低频公开数据，且横截面筛选比对全部股票逐只调用 LLM
更稳定、更便宜。当前实现位于：

- [筛选数据端口](../../src/gribuki_trade/ports/ashare_screening.py)
- [硬过滤和横截面因子](../../src/gribuki_trade/features/ashare_screening.py)
- [三层编排服务](../../src/gribuki_trade/services/ashare_screening.py)
- [AKShare 筛选适配器](../../src/gribuki_trade/adapters/ashare_screening.py)

当前漏斗：

1. L1 对全市场快照做 fail-closed 硬过滤：交易状态、ST/停牌、上市时间、价格、当日成交额、
   市值和板块；默认覆盖不足时拒绝运行。
2. 按当日成交额取最多 300 个幸存者补历史，控制上游压力。
3. L2 使用 20/60/120 日动量、MA20/MA60、20 日突破位置、量比、波动率、最大回撤和
   Amihud 非流动性做 winsorize、横截面百分位与加权排名。
4. 可用因子权重不足 80% 时不参与排名；缺失因子不填零。
5. L3 输出默认 Top 30，并写入统一候选库；L3 仍是“深研输入边界”，不是自动买入。

合理性改进：当前成交额预算会偏向高流动性股票，这是有意的工程取舍，但不能把预算外标的
解释为负面观点。下一版本应同时保存“L1 幸存但因预算延后”的集合，并在历史回测中固定预算
规则。增加行业/规模中性化前，应先保存完整横截面，否则无法判断排名增益是否只是行业或
小盘暴露。

当前可运行入口：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-market-screen-once `
  --top-n 30 `
  --factor-budget 300 `
  --candidate-db runtime/research/candidates.sqlite3 `
  --run-db runtime/research/runs.sqlite3 `
  --output runtime/screening/latest.json
```

生产验收门槛：

- 连续至少 20 个交易日记录源覆盖、延迟、字段漂移和 Top-N 稳定性；
- 同一 source revision、同一配置重复运行得到相同排名和逐因子贡献；
- 保存每日完整股票池、排除原因、预算延后集合、历史因子输入和策略版本；
- 建立包含退市股、历史 ST/停牌、上市日和公司行动的 PIT 回放，不使用今天的股票池替代历史；
- 与简单流动性、指数成分、均线和动量基线比较，成本后样本外增益不足则不升级。

### 4.2 盘中全市场异常发现

可行性：**中等**。用于分钟级“发现线索”可行；把公开网页快照称为交易所实时行情或直接据此
下单不可行。当前实现位于：

- [盘中快照端口](../../src/gribuki_trade/ports/ashare_surveillance.py)
- [盘中异常因子](../../src/gribuki_trade/features/ashare_surveillance.py)
- [盘中编排服务](../../src/gribuki_trade/services/ashare_surveillance.py)
- [AKShare/东方财富与腾讯适配器](../../src/gribuki_trade/adapters/ashare_surveillance.py)

当前单次扫描只在 09:30–11:30、13:00–15:00 运行，默认要求至少 4,500 个标的且快照不超过
3 分钟。它按涨跌强度、当日成交额、日内区间位置、开盘后延续、量比和换手率做横截面排名，
输出 `MOMENTUM_EXPANSION`、`ACTIVE_STRENGTH` 或 `OBSERVATION_ONLY` 候选。结果始终标明
`CURRENT_SESSION_SNAPSHOT_ONLY`，不会直接成为交易信号。

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-intraday-scan-once `
  --top-n 30 `
  --candidate-db runtime/research/candidates.sqlite3 `
  --run-db runtime/research/runs.sqlite3 `
  --output runtime/surveillance/latest.json
```

下一步不应简单地把命令放进无限 `while`。需要一个交易日历感知的 `RunCoordinator`，提供：

- 进程互斥、run id、超时、取消、午休和收盘切换；
- 上一轮未结束时不重入，超出数据源预算时背压或跳过；
- 休眠恢复后的 misfire 记录，而不是补跑一串过期扫描；
- source health、扫描耗时、覆盖率、候选数量、重复率和陈旧拒绝次数；
- 只把新晋级、显著变更或接近到期的候选交给深研，避免每分钟对全部标的调用 LLM。

生产验收门槛：连续 20 个交易日无陈旧数据晋级、无重叠运行、无午休误判；源失败时明确降级；
保存盘中快照修订以便复盘；验证扫描候选在下一可交易时点后的表现，而不是用当根未完成 K 线。

### 4.3 统一候选名单与实时跟踪

可行性：**高**。当前候选领域模型、SQLite 事件存储和编排服务已落地：

- [候选领域模型](../../src/gribuki_trade/domain/candidates.py)
- [候选事件存储](../../src/gribuki_trade/storage/candidate_store.py)
- [候选编排服务](../../src/gribuki_trade/services/candidate_universe.py)
- [有限轮研究调度](../../src/gribuki_trade/services/research_watch.py)

候选来源包含手工、收盘筛选、盘中异动、策略和复核；同一代码的多来源会合并但不丢失
provenance。默认 TTL 为收盘筛选 4 天、盘中异动 8 小时、策略 2 天、复核 7 天，手工候选
默认不过期。`COOLING` 可临时暂停跟踪，`REMOVED` 需要显式重新激活；所有状态都从不可变事件
按 `as_of` 重放。

```powershell
# 查看候选
.\.venv\Scripts\python.exe -m gribuki_trade ashare-candidates list `
  --candidate-db runtime/research/candidates.sqlite3

# 手工加入
.\.venv\Scripts\python.exe -m gribuki_trade ashare-candidates add `
  --symbol 600000.SH `
  --reason MANUAL_RESEARCH_SELECTION

# 仅跟踪 ACTIVE 候选，执行三个有限周期
.\.venv\Scripts\python.exe -m gribuki_trade ashare-research-watch `
  --candidate-db runtime/research/candidates.sqlite3 `
  --candidates-only `
  --cycles 3 `
  --interval-seconds 60
```

当前“实时”应准确解释为有限轮、顺序执行的研究轮询。它有逐标的失败隔离，避免单个 provider
故障中止整个周期，但尚无优先级队列、全局 API 预算、常驻服务、交易日历、休眠恢复或 SLA。
北交所代码已经能进入候选库和收盘日线深研：BaoStock 使用 `bj.xxxxxx`，不可用时可由现有
路由降级至明确覆盖沪深京的 AKShare 日线。分钟研究链仍未承诺北交所覆盖，因此 `.BJ` 候选
目前只能进入收盘深研，不能被当作已支持盘中跟踪。

建议把跟踪分成三档：

| 档位 | 输入 | 频率和成本 | 处理 |
|---|---|---|---|
| 快速监视 | 全部 ACTIVE 候选 | 高频、纯确定性 | 价格/量能/陈旧检查和状态变化 |
| 事件复核 | 技术阈值穿越或重要新闻 | 中频 | 重取完整技术特征与 EvidencePack |
| 深度分析 | 新晋级、高优先级或人工请求 | 低频、可调用 LLM | 生成版本化报告和通知 |

这样可避免把“持续宏观分析”误解为每分钟对每只股票调用一次模型。

### 4.4 特定股票深度分析与复核 callback

可行性：**高**，但应将“分析函数”和“确认信号的状态机”分开。

现有 `ashare-research-once` 用已完成 1/5 分钟 K 线做短周期研究；
`ashare-close-research-once` 用已完成日线、新闻、跨市场/宏观证据做下一交易日深研，并可生成
Markdown/PNG 报告和通知。推荐经过确定性门禁：宏观最多占 40%，可以降级技术候选，但不能
越过技术入场门直接创建入场信号。

当前已实现的批量与复核协议包括：

1. Top-N 批处理接收显式 symbol 或统一候选库中的 ACTIVE 标的，设置最大数量并逐标的失败隔离；
2. 批处理严格顺序访问公开上游，避免无界并发；stdout 只返回摘要，完整建议和报告走既有持久化；
3. 动态证券画像按“静态自选配置优先、当前 provider 按需回退”解析；名称/行业等必需字段缺失时
   结构化失败，不伪造档案；
4. 沪深京普通股票均支持收盘日线深研；BaoStock 的北交所代码使用 `bj.xxxxxx`，失败时可回退到
   明确覆盖沪深京的 AKShare 日线。该结论不外推到分钟数据；
5. 独立复核状态机支持 `PENDING_REVIEW -> CONFIRMED/REJECTED/CANCELLED`，到期查询投影为
   `EXPIRED`；保存 recommendation、证据、候选 provenance、操作主体和原因；
6. 复核确认只表示研究结论获批，领域与服务层均不导入订单、账户、券商或执行模块。

相关实现见 [动态证券画像适配器](../../src/gribuki_trade/adapters/instrument_profile.py)、
[复核领域模型](../../src/gribuki_trade/domain/review_cases.py)、
[复核服务](../../src/gribuki_trade/services/recommendation_review.py) 和
[复核事件存储](../../src/gribuki_trade/storage/review_case_store.py)。

人工复核的有限 CLI 已实现，`confirm` 需要显式 `RESEARCH_ONLY` 哨兵：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-review open `
  --recommendation-id <RECOMMENDATION_ID>
.\.venv\Scripts\python.exe -m gribuki_trade ashare-review list
.\.venv\Scripts\python.exe -m gribuki_trade ashare-review confirm `
  --case-id <CASE_ID> `
  --reason MANUAL_EVIDENCE_REVIEWED `
  --confirm RESEARCH_ONLY
```

`get/reject/cancel` 使用同一 append-only 存储；确认后仍需另一个明确、独立的 PAPER 动作，系统
不会将 review case 翻译为订单。

Top-N 到收盘深研的有界批处理入口现已实现：它可接收重复 `--symbol`，也可合并统一候选库中
当前 ACTIVE 的标的；严格顺序执行以保护公开上游，每个标的失败隔离，并只在 stdout 保留精简
结果，完整建议和报告仍写入既有持久化。示例：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-close-research-batch `
  --candidate-db runtime/research/candidates.sqlite3 `
  --limit 10 `
  --output runtime/research/close-batch-latest.json
```

该命令不是常驻任务，也不会自动下单。当前剩余缺口是把复核 case 与报告/通知 callback 自动
接线、把批处理纳入交易日历感知的运行编排，以及对动态画像 provider 做长期稳定性验证。

### 4.5 A 股 PAPER 交易与回测

可行性：**分三层判断**。

- 对“人工或模拟成交已经确定后，可靠记账、重放和评价”：可行性高，第一阶段已实现。
- 对“用明确的未复权日线与价格区间做可重复的保守撮合”：可行性中等，第二阶段纯撮合核心已实现。
- 对“仅凭免费网页行情，准确模拟盘口排队、集合竞价和市场冲击”：可行性低，当前明确不建模。

当前 A 股 PAPER 模块见：

- [领域模型](../../src/gribuki_trade/domain/paper_trading.py)
- [账本端口](../../src/gribuki_trade/ports/paper_ledger.py)
- [SQLite 账本](../../src/gribuki_trade/storage/paper_ledger.py)
- [PAPER 服务](../../src/gribuki_trade/services/ashare_paper.py)
- [持久委托事件库](../../src/gribuki_trade/storage/paper_orders.py)
- [崩溃恢复 saga](../../src/gribuki_trade/services/ashare_paper_recovery.py)
- [详细边界](../A_SHARE_PAPER_TRADING.md)

已实现现金非负、`quantity = available_to_sell + today_buy`、买入当日不可卖、下一交易日显式
rollover、禁止超卖、账户内 `fill_id` 幂等、实际费用固化、事件哈希链、SQLite WAL 和事件重放。
人工与模拟成交共享同一 fill 契约，区别只在来源和费用覆盖权限。

第二阶段 [PAPER 委托模型](../../src/gribuki_trade/domain/paper_orders.py) 与
[保守日线撮合器](../../src/gribuki_trade/services/ashare_paper_matching.py) 已提供：

- `PENDING/PARTIALLY_FILLED/FILLED/CANCELLED/REJECTED/EXPIRED` 六态限价委托；
- 只处理决策日之后、明确 `PriceAdjustment.NONE` 的已完成日线；停牌、OHLC 缺失、零成交量、
  缺失或不一致的显式价格区间均不成交；价格区间由调用方提供，撮合器不猜测板块涨跌停规则；
- 限价触及、默认 1% bar 成交量参与率、FIFO 容量分配、跨 bar 部分成交、买入 100 股整手及
  卖出尾仓；成交价应用不突破委托限价的保守方向滑点；
- 同一 bar revision 重放幂等，revision 冲突或时间倒退失败；确定 fill 通过既有持久账本记账。

第三阶段已在纯撮合器外增加持久 wrapper：订单提交/状态、完整 bar/config/source revision 和
run 前订单投影进入 append-only 事件流；确定性 `fill_id` 先写资金账本，再追加订单成交与完成事件。
两个 SQLite 文件之间不宣称跨库 ACID，而以可重算 saga 恢复：崩溃后同一 fill 由账本幂等拦截，
不会重复扣款。全局 writer lease、单个未完成 run 和 `BEGIN IMMEDIATE` 防止两个 wrapper 并发预留
同一资金。精确完成态重放返回 receipt-less 摘要，避免伪造历史账户投影。

重要边界：持久 wrapper 仍是 Python API，尚无撮合 CLI/GUI；调用方必须先 `recover()`。价格限制、
停牌、公司行动及交易日仍必须由 PIT 数据明确提供。它是可恢复的本地研究撮合器，不是模拟真实
盘口的券商。

```powershell
# 开户
.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper open `
  --account personal-paper `
  --initial-cash 100000 `
  --session-date 2026-08-14

# 录入一笔已确认的人工成交；不会推测是否能成交
.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper fill `
  --account personal-paper `
  --fill-id manual-20260814-001 `
  --symbol 600000.SH `
  --side BUY `
  --quantity 100 `
  --price 10.00 `
  --instrument STOCK `
  --source MANUAL `
  --session-date 2026-08-14

.\.venv\Scripts\python.exe -m gribuki_trade ashare-paper snapshot `
  --account personal-paper
```

下一阶段应把持久 wrapper 接入显式、有限的撮合命令和运行编排，补订单/账本一致性巡检与恢复
告警，再让同一撮合器服务历史回放与 PAPER。仍缺的模型是公司行动、退市、集合竞价、T+0
品种差异和盘口冲击；其中盘口队列在没有逐笔数据时不应实现。

模型名称、参数、行情 revision 和费用版本必须进入回测 manifest。未获得逐笔队列数据时，不实现
声称能还原真实排队位置的模型。

统一回测器的最低验收要求：

- 决策信号和成交之间至少隔开一个真实可交易时点，禁止使用产生信号的收盘价回填成交；
- 覆盖佣金最低收费、过户费、印花税、滑点、T+1、涨跌停、停牌、退市和公司行动；
- 同一策略函数服务于历史回放、PAPER 研究和一次性在线评估；
- 结果同时报告毛收益、成本后收益、最大回撤、Expected Shortfall、换手、容量、未成交率、
  缺数率和 `ABSTAIN` 比例；
- 事件重放得到相同资金、持仓、成交和指标，重复输入不会重复记账。

### 4.6 策略权重优化

可行性：**中等**。可以寻找更稳健的候选权重，不能从有限回测中求出永久“最优权重”。

当前 [strategy_lab/experiments.py](../../src/gribuki_trade/strategy_lab/experiments.py) 已实现：

- 数据和策略 manifest 的 SHA-256 冻结；
- expanding walk-forward；训练/验证之间 purge，验证后 embargo；
- 与开发折完全隔离的最终 test holdout；
- 技术家族和宏观权重组成 simplex，宏观上限 40%，单技术家族有上限；
- 基于验证集选择，最终测试只在选择锁定后读取；
- 多种成本情景，按最差情景的验证目标选择；
- 基线、逐折指标、家族贡献、试验次数与过拟合警告；
- append-only SQLite 实验存储。

这套框架有意不做在线自动调参。[A 股日线评价器](../../src/gribuki_trade/strategy_lab/ashare_evaluator.py)
已经实现单证券 long/cash 的冻结 PIT 评价：逐观察来源 revision、技术/宏观 `known_at`、价格限制、
次日开盘/保守限价、T+1、整手、费用、滑点、停牌和量能均进入确定性 trace。它已经能作为
walk-forward 的真实 `StrategyEvaluator`，但不能冒充横截面组合回测或收益归因。下一步是冻结
数据集构建器、组合层和发布注册表：

```text
DRAFT -> VALIDATED -> HOLDOUT_EVALUATED -> SHADOW -> APPROVED -> RETIRED
```

任何状态跃迁都要保存审核人、代码 revision、数据 manifest、基线差异、样本外指标和理由。
最终测试集只允许用于一次版本决策；同一测试集被反复查看后，应视为开发数据并重新取得未见
shadow 期。

最低验收门槛：

- 预先登记目标、成本情景、候选权重空间和停止规则；
- 至少与等权、当前生产权重、简单技术基线及不使用宏观的基线比较；
- 分市场状态、行业、规模和板块报告，不只展示总体最优结果；
- 报告 trial 数，并使用 PBO、deflated Sharpe、White Reality Check 或同类方法处理多重试验；
- 权重变化只有在多个 walk-forward 折、成本压力和 shadow 期均稳定时才允许发布；
- 发布后只监测漂移和触发重新研究，不能根据单日 PAPER 盈亏即时更新。

### 4.7 技术面探索引擎

可行性：**中等到低**，主要瓶颈不是算法，而是可靠的 PIT 数据、标签、交易约束和多重试验控制。

当前 [strategy_lab/factors.py](../../src/gribuki_trade/strategy_lab/factors.py) 提供安全因子 DSL，仅允许
OHLCV/成交额、算术及 `lag`、`return/ret`、`ma`、`vol`、`zscore`；不使用 `eval`，拒绝任意
Python、动态窗口、除零和非有限结果，并显式给出预热长度。

[strategy_lab/discovery.py](../../src/gribuki_trade/strategy_lab/discovery.py) 已在 DSL 之上实现第一阶段
受控发现器：版本化模板 grammar、窗口/列白名单、AST 深度/节点/预热门禁、全搜索空间预算、
规范化表达式去重、经济家族标签、稳定 candidate/attempt/inventory ID，以及对拒绝原因和多重试验
警告的完整保留。可选冗余过滤只消费调用方提供的开发集数值，并按相关性失败关闭；它不会读取
holdout、计算收益、调用 LLM 或发布策略。这是“可审计候选生成”，不是已经发现可交易 alpha。

候选表达式现在可以由调用方在训练/验证切片上求值后交给单证券 A 股评价器，并进入既有 trial
registry；发现器本身仍不偷看收益或 holdout。横截面 IC、行业/规模中性化和组合换手尚未实现。

建议按风险从低到高逐步增加：

1. **基线扩展**：继续审查现有趋势、反转、量价、波动、流动性模板，不因模板更多就默认更好；
2. **冗余治理**：在已实现的开发集数值相关性筛除之上，补 PIT 横截面 IC 相关、换手和家族
   归属去重，不因公式不同就算独立假设；
3. **中性化和稳健性**：行业、规模、价格和流动性暴露；不同板块、状态和成本压力测试；
4. **受控符号搜索**：把已实现的候选 inventory 与实验 trial registry/evaluator 接通，保留失败结果；
5. **机器学习组合**：只使用时间顺序训练，嵌套验证选择超参数；模型输出仍经过风险和证据门禁；
6. **状态空间/市场状态**：状态只能由当时已知数据识别，转移概率和窗口也要在训练折内估计。

LLM 可以提出因子假设、解释经济含义和生成 DSL 草案，但不能直接执行任意代码、查看 holdout
结果后继续改公式，或自动发布到在线策略。因子必须经过相同的成本、样本外、多重试验和
shadow 门槛。

### 4.8 研究运行档案与重放边界

可行性：**高**，但“保存运行输出”和“保存完整原始输入”必须分开表述。

[研究运行存储](../../src/gribuki_trade/storage/research_runs.py) 已为收盘全市场筛选和盘中异常扫描
保存 append-only 运行记录：确定性 run ID、逻辑键、策略版本、状态、起止时间、规范化完整配置
及 SHA-256、来源 revision，以及规范化输出文档和摘要。同一逻辑运行精确重放保持幂等，复用
身份但配置、lineage 或输出不同都会报冲突。旧预览表迁移时不会伪造缺失配置。只读 CLI 支持
`ashare-research-runs list|get`，且数据库不存在时不会意外创建。SQLite 使用 WAL、
`synchronous=FULL` 和禁止 UPDATE/DELETE 的触发器。

这份档案只证明“某配置和来源 revision 产生了什么输出”，**不包含 provider 的完整原始响应、
完整 L1 排除集合、预算延后集合或逐标的历史因子输入**，因此不能单独满足无网络重放。后续应
用独立的不可变 raw/PIT 数据归档保存输入，再由 run lineage 引用其内容摘要；不能把输出摘要
反向描述成完整数据快照。

## 5. 数据和统计风险清单

| 风险 | 当前影响 | 必须采取的控制 |
|---|---|---|
| 公开网页快照不是交易所 feed | 时间戳、覆盖和字段可能漂移 | 明确语义、3 分钟时效、覆盖门槛、双源降级、20 日 soak |
| 幸存者偏差 | 用今天股票池回测历史会高估效果 | 每日归档全股票池、退市/ST/停牌/上市历史 |
| 未来函数 | 复权重写、新闻发布时间和当根 K 线易泄漏 | `available_at/first_seen_at`、未完成 bar 拒绝、next-bar 成交 |
| 公司行动 | 未复权价格可出现伪动量/伪突破 | 保存原始和公司行动，使用 PIT 可重建调整因子 |
| 横截面暴露 | 排名可能只是行业、小盘或流动性暴露 | 行业/规模中性化、分组报告、容量约束 |
| 选择性深研 | 只保留 Top-N 的结果会产生条件选择偏差 | 保存完整漏斗、排除项和预算延后项 |
| 新闻修订与重复 | 同一故事多次出现会放大情绪 | canonical URL、内容哈希、事件聚类、修订链、来源上限 |
| LLM 非确定性和版本漂移 | 同证据可能得到不同评分 | 模型/提示/schema 版本、输入证据哈希、固定回归集、弃权门禁 |
| 重叠标签 | 多日持有标签让训练/验证互相泄漏 | purge/embargo 至少覆盖标签周期 |
| 多重试验 | 因子和权重越搜越容易偶然“优秀” | 全量 trial registry、预登记、PBO/DSR/Reality Check、独立 holdout |
| SQLite WAL 运行库缺陷 | 本次本机 `sqlite3.sqlite_version=3.50.4`，处于官方 WAL-reset 缺陷影响范围；同一 DB 多连接并发 checkpoint/write 有低概率损坏风险 | 升级到官方修复版（3.50.7、3.44.6 或 >=3.51.3）；升级前同一 DB 路径只运行一个进程/Store 实例，不把业务 writer lease 误当 SQLite 修复 |

可用 `python -m gribuki_trade sqlite-runtime-status` 做无副作用 preflight；长期 worker 或多进程
部署应调用 `gribuki_trade.sqlite_runtime.require_safe_shared_wal()` 并在不安全版本上失败关闭。
| 执行高估 | 忽略最小佣金、涨跌停和未成交 | 保守撮合、多成本情景、容量和未成交率 |
| 反馈漂移 | PAPER/人工成交不是随机样本 | 记录人工是否执行及原因，区分信号效果和执行选择偏差 |

## 6. 推荐的后续阶段

### R0：数据归档和运行契约

优先级：最高。

- 保留已实现的收盘筛选/盘中扫描输出与 lineage 运行档案，并扩展到批量深研；
- 冻结每日原始全市场快照、完整漏斗、预算延后集合和历史因子输入；
- 引入交易日历端口，统一收盘、盘中、rollover 和次日标签；
- 在现有 run 表补完整输入归档引用和稳定错误码，避免把输出档案当输入快照；
- 长期验证动态证券画像；北交所当前只承诺收盘日线，分钟链需单独验收。

退出条件：任一收盘/盘中结果能从输出追溯到不可变输入，并能在无网络环境重放。

### R1：候选到深研的完整闭环

优先级：最高。

- 将已实现的 Top-N 有界批量收盘深研纳入常驻编排和运行审计；
- 实现候选优先级队列、变更触发和 API/LLM 预算；
- 将已实现的复核 case 状态机接入报告/通知 callback，并补报告幂等和通知去重；
- 对接 NapCat outbox，但保留有限批次和过期拒绝。

退出条件：连续 20 个交易日无重复报告/通知，单标的失败不影响其余标的，过期候选不会补发。

### R2：统一 PAPER 委托与事件驱动回测

优先级：高。

- 把已实现的 append-only 委托/run、全局 writer lease 和崩溃恢复 wrapper 接入有限 CLI/worker，
  增加恢复告警、完整性巡检与故障注入；
- 补公司行动、退市与交易日历数据；不在无逐笔数据时声称还原盘口队列；
- 以已实现的单证券 A 股评价器为基线，补横截面组合回放，让回测、PAPER 和一次性在线分析调用
  同一确定性策略/成交契约；
- 把推荐 ID、候选 provenance、委托、fill 和结果串成可审计 lineage。

退出条件：固定事件回放结果完全一致；边界测试覆盖 T+1、费用、停牌、涨跌停、部分成交和退市。

### R3：策略评估与发布治理

优先级：高，依赖 R0/R2。

- 扩展已实现的单证券 `StrategyEvaluator`，实现冻结横截面数据集构建器和组合 evaluator；
- 接通推荐事后结果、PAPER 和历史回放，但分别报告三类样本；
- 增加实验/模型注册表和显式发布状态机；
- 补 PBO、deflated Sharpe、基线/消融、状态分层和容量报告。

退出条件：候选策略只凭 validation 选择，holdout 只在锁定后评估，发布配置可追溯且可回滚。

### R4：受控因子探索

优先级：中，依赖 R3，不能提前替代数据建设。

- 扩展已实现的版本化 DSL grammar 和因子家族目录；
- 把现有搜索预算、表达式复杂度、规范化去重和相关性筛除批量接入 evaluator/trial registry；
- 完成受控符号搜索的样本外评估后，再判断是否值得引入树模型、稀疏模型或状态空间方法；
- 每一轮探索使用新的未见数据或 shadow 窗口。

退出条件：新因子在多个样本外折、成本情景和状态中提供稳定增量；未通过时保留负结果而不发布。

### R5：常驻运行、GUI 和可观测性

优先级：与 R1–R3 并行，但不改变研究算法。

- 等用户给出统一应用运行框架后，在应用内实现交易日历、进程互斥、健康检查和休眠恢复；
- 部署 preflight 校验 SQLite 运行库；修复版就绪前对每个 WAL DB 强制单进程单实例；
- GUI 展示 run、候选来源、TTL、报告、PAPER 账本和实验版本；
- 指标包括源健康、扫描耗时、候选晋级/过期、模型调用/费用、通知延迟、死信和回测运行；
- 所有长期任务可停止、可重启、可恢复，不在 GUI 主线程执行网络请求。

退出条件：连续 20 个交易日运行无重复任务、无过期补发、无不可解释状态和 GUI 卡死。

## 7. 当前最合理的开发优先级

下一轮不应优先投入复杂 ML。建议顺序如下：

1. 冻结完整筛选/盘中原始输入和漏斗快照；现有运行档案继续负责输出与 lineage，补交易日历；
2. 把已实现的批量收盘深研和复核状态机接入运行编排、通知 callback 与端到端去重；
3. 完成动态标的元数据长期稳定性验证，并单独验收北交所分钟数据边界；
4. 将已实现的 PAPER 持久恢复 wrapper 接入运行编排，并补统一组合回测、公司行动与价格限制档案；
5. 扩展已接通的单证券 evaluator，完成冻结横截面数据集、实验注册和显式发布；
6. 用冻结开发集评估已实现的受控因子候选；数据和基线通过后再启动机器学习实验。

这一路线保留了原计划的所有目标，同时把“选股”“分析”“交易模拟”“策略优化”和“因子探索”
拆成了可独立测试、可审计、可降级的组件。它也避免最危险的失败方式：用当前股票池回测历史、
用同一数据反复选权重、把 LLM 分数当概率、或让一次回测结果自动改变在线交易行为。

## 8. 方法参考

- 止损规则在不同收益状态下的边界：[Kaminski 与 Lo，When Do Stop-Loss Rules Stop Losses?](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=968338)
- 波动率管理的资产配置证据：[Moreira 与 Muir，Volatility Managed Portfolios](https://www.nber.org/papers/w22208)
- 目标/止损/时间 barrier 的联合表达：[Bañós 等，Trading Strategies with Target, Stop-Loss, and Time-Out](https://arxiv.org/abs/2003.10502)
- 参考项目及其角色编排：[ai-berkshire](https://github.com/xbtlin/ai-berkshire)；本项目只吸收角色分工与反方审计，不采用其结果宣称
- 多智能体辩论的正面结果：[Du 等，Improving Factuality and Reasoning through Multiagent Debate](https://proceedings.mlr.press/v235/du24e.html)
- 等调用预算下的限制与敏感性：[Smit 等，Should We Be Going MAD?](https://proceedings.mlr.press/v235/smit24a.html)
- 自我修正/辩论的反例：[Huang 等，Large Language Models Cannot Self-Correct Reasoning Yet](https://arxiv.org/abs/2310.01798)
- 多模型多样性与协作：[ReConcile](https://aclanthology.org/2024.acl-long.381/)
- 时间顺序验证与 `gap`：[scikit-learn TimeSeriesSplit](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html)
- 回测过拟合概率：[Bailey 等，The Probability of Backtest Overfitting](https://papers.ssrn.com/sol3/Papers.cfm?abstract_id=2326253)
- 多重因子检验：[Harvey、Liu、Zhu，… and the Cross-Section of Expected Returns](https://www.nber.org/papers/w20592)
- 技术规则数据窥探：[Sullivan、Timmermann、White](https://www.fmg.ac.uk/publications/discussion-papers/data-snooping-technical-trading-rule-performance-and-bootstrap)
- SQLite WAL 语义：[SQLite Write-Ahead Logging](https://www.sqlite.org/wal.html)
- SQLite 2026 WAL-reset 缺陷与修复版本：[The WAL-Reset Bug](https://www.sqlite.org/wal.html#walresetbug)

这些方法只用于建立实验和审计门槛，不证明当前任何因子或评分已经具有可交易的样本外收益。
