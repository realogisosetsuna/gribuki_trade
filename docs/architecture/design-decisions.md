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
`src/gribuki_trade/backtest/`, and `tests/unit/test_strategy_lab_factors.py`.
