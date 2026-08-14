# A 股盘后深研与应用编排边界

> **当前决定（2026-08-14）**：不采用 Windows Task Scheduler 或独立
> PowerShell watchdog 作为交易系统编排层。相关安装、看门和测试脚本已经从当前树移除，
> 不存在可恢复的隐藏入口。`ashare-post-close run/status/report` 的单次运行与
> 可恢复状态机继续有效；交易日调度、进程监督和生命周期将在后续统一应用运行框架中接入。

`ashare-post-close` 是 broker-free 的生产边界：它只读取当天已经完成的
PAPER journal、sidecar 与哈希链账本，为期末持仓逐一运行收盘深研，写出本地
Markdown，再通过一个按交易日隔离的 OneBot outbox 和冻结目标交付。它没有真实
券商依赖，也不会提交订单。

## 单次运行

运行必须满足全部条件：上海时区同一天、真实交易日历已验证、时间不早于
15:05、当天 PAPER 生命周期为 `COMPLETED`，且通知目标与 PAPER run 中冻结的
目标哈希完全一致。BaoStock 日历读取有有限重试；非交易日或 15:05 前只产生明确
的 skipped 结果，不会分析或发送。

```powershell
$tradeDate = (Get-Date).ToString('yyyy-MM-dd')

.\.venv\Scripts\python.exe -m gribuki_trade ashare-post-close run `
  --runtime-dir runtime/paper/day --session-date $tradeDate `
  --account ashare-paper-day `
  --target-kind private --target-id "YOUR_QQ_ID" `
  --base-url http://127.0.0.1:3000 `
  --candidate-db runtime/research/candidates.sqlite3 `
  --history-days 540 `
  --news-feed global_eastmoney `
  --news-feed global_cailianpress `
  --news-feed global_sina `
  --news-feed global_10jqka `
  --refresh-news --search-discovery `
  --macro --macro-provider deepseek --model deepseek-v4-flash `
  --macro-weight 0.25 `
  --dispatch-cycles 3 --dispatch-poll-interval 2 `
  --confirm POST_CLOSE
```

DeepSeek 盘后分析默认启用。API key 与 NapCat token 由运行该命令的同一 OS
用户从 keyring 解析；CLI、status 和日志不得包含 secret 值。用户自行保存的明文 API
文件不属于自动清理或迁移范围。盘中 PAPER LLM 的默认调用预算仍是 `unlimited`，与
`ashare-paper-day --intraday-llm-max-calls unlimited` 的文档和 manifest 语义一致。

动态证券画像不可得时，盘后适配器不会猜测行业、规模或板块。它只使用不可变
PAPER session 中 WATCHLIST、SURVEILLANCE 与 FILL 已明确归档且相互一致的
symbol/name/board/instrument type，来源标记为 `PAPER_SESSION_ARCHIVE`；industry 与
size 固定为 `unknown-not-provided`，并在画像中披露 `DEGRADED_PROFILE`。日线必须
精确覆盖分析日；Sina 未复权降级也不能用前一日数据冒充当日收盘。

Windows 上 AKShare 的部分 Sina 路由使用 MiniRacer。生产接线将持仓、单持仓新闻
源、跨市场 fallback 和跨市场历史调用全部确定性串行，避免多个 V8 runtime 并发
导致原生进程终止。此限制优先于盘后吞吐。

## 状态、产物与恢复

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade ashare-post-close status `
  --session-date $tradeDate
.\.venv\Scripts\python.exe -m gribuki_trade ashare-post-close report `
  --session-date $tradeDate
```

run manifest 根据日期和完整研究策略生成稳定 `run_id`，已有 manifest 的配置、账户
或目标哈希发生变化时失败关闭。`run.lock` 保证单 writer；重复执行已经完成的 run
只返回 idempotent replay，不会重新分析或推送。

每个持仓失败相互隔离，但结果不会被粉饰：部分失败的 Markdown 明确标为
`PARTIAL`；只要存在持仓且全部深研失败，状态写为
`ANALYSIS_FAILED_RETRYABLE`，命令非零退出，并且不会进入通知阶段。文本按 `(i/n)`
编号顺序进入专用 `delivery-outbox.sqlite3`，Markdown 文件另行上传；outbox、状态和
`audit.jsonl` 都保留有限重试及稳定 idempotency key。目标只允许同一冻结的
private/group ID。

正常的确定性失败可由未来应用运行框架按冻结策略有限重试。只有进程在分析或发送期间中断，
留下 `ANALYSIS_IN_PROGRESS`、`ANALYSIS_AMBIGUOUS`、
`DELIVERY_IN_PROGRESS` 或 `DELIVERY_AMBIGUOUS` 时，才需要人工检查并用原命令、原
配置、原目标追加恢复授权：

```powershell
# 分析阶段尚未进入交付；允许在同一 run_id 下重新分析
<the-identical-run-command> --recover-analysis

# provider receipt 不确定；明确接受一次可能重复交付的人工恢复
<the-identical-run-command> --recover-delivery
```

未来运行框架只能自动附加 `--recover-analysis`：冻结 manifest 且尚未进入交付时，原生崩溃、断电或
进程终止留下的 `ANALYSIS_IN_PROGRESS`/`ANALYSIS_AMBIGUOUS` 可以在同一个 `run_id`
下重新分析，并在 `audit.jsonl` 写入显式恢复授权。框架永远不自动附加
`--recover-delivery`；一旦交付回执有歧义，仍须人工核对后决定是否接受重复发送风险。
不要删除 manifest、ledger 或 outbox 来“恢复”；配置漂移和未知 phase 都会失败关闭。

NapCat 未在线时，run 不会启动任何脚本或后台进程。本地报告与 outbox 保留，命令非零
退出，并写明需要操作者在 GUI“集成管理”页完成 NapCat 启动和 QQ 登录；它不会启动真实
broker。

## 编排迁移说明

操作系统层安装方案已经删除。CLI 内部和公开 parser 均不再接受
`schedule-install`/`schedule-status`，仓库也不保留任务注册或看门脚本。
未来应用运行框架至少需要统一负责：上海交易日历、单实例租约、心跳、取消、阶段恢复、
NapCat 生命周期、LLM/数据源资源预算和收盘后的盘中日志交接。框架必须调用同一个
`ashare-post-close run` 状态机，而不是复制研究和交付逻辑。

可选 `--searxng-url` 在创建 post-close 目录及写 manifest 前验证：只接受结构完整的
HTTP(S) endpoint，并拒绝 userinfo、query 与 fragment。运行时使用验证后的 URL；持久
manifest 与 `run_id` 只保存规范化 origin 和 path 的 SHA-256，不保存可能敏感的 path
原文。失败只返回稳定码 `SEARXNG_URL_INVALID`。
