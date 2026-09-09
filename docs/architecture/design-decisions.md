# Design decisions recorded in the current code

This page records reasons that are already expressed by implementation and
tests, rather than proposing new architecture.

## Keep broker access behind a guard

The mode guard centralizes authorization so services and GUI code cannot grant
themselves broker authority. The three-mode behavior and process-local LIVE
unlock are implemented in `runtime/guard.py` and tested in
`test_runtime_guard.py`.

## Preserve evidence provenance and fail closed

Provider disagreement, missing history, stale/future bars, malformed payloads,
and ambiguous revisions are represented as typed failures or degraded results.
This prevents a fallback from looking like complete evidence. See the adapter,
evidence and technical-signal tests named in `verification-map.md`.

## Separate durable stores and sidecars

Candidate/research/review data, execution ledgers, notification outboxes and
report artifacts have different idempotency and recovery lifecycles, so they
are stored separately. PAPER-day additionally isolates state by trading day.
The store implementations and recovery tests enforce this separation.

## Keep strategy-lab output research-only

Factor parsing, walk-forward evaluation, costs and trial registries are kept
offline and immutable. The evaluator output carries `research_only` and
`promotion_authorized` constraints; strategy-lab tests cover rejection of
unsafe expressions and promotion bypasses.

Evidence paths: `src/gribuki_trade/strategy_lab/`,
`src/gribuki_trade/backtest/`, and `tests/unit/strategy_lab/test_strategy_lab_factors.py`.

## Keep research artifact codecs separate from evaluators

退出策略评估器中的数据集、滚动计划和登记簿需要稳定摘要与归档格式，但这些
格式化操作不应携带模拟状态或文件 I/O。`strategy_lab/exit_serialization.py`
因此只接收结构化值并产生确定性的 JSON 文档和 SHA-256 摘要；历史
`exit_evaluator` 导入路径继续作为兼容门面。

通用权重实验也遵循同一边界：`strategy_lab/experiment_serialization.py`
只负责清单、折叠、指标、试验和留出集的确定性编码。它通过
`TYPE_CHECKING` 引用不可变模型，运行时不依赖评价器或存储；
`experiments.py` 继续保留历史导入路径和研究约束，避免已归档实验的
内容摘要因拆分而改变。
