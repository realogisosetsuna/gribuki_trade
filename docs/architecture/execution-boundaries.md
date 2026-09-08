# Execution boundaries and safety invariants

These are current contracts, verified by source and tests.

## Modes

- `PAPER` cannot access a real broker.
- `SHADOW` permits connection, queries and subscriptions but rejects operations
  that change orders.
- `LIVE` requires the exact process-local confirmation phrase and allowlisted
  account and exchange. Confirmation is not loaded from environment/config.

Source: `src/gribuki_trade/runtime/mode.py` and `runtime/guard.py`.
Verification: `tests/unit/test_runtime_guard.py`, broker adapter tests, and
CLI tests covering confirmation errors.

Binance Spot execution uses the durable SQLite OMS in
`services/binance_execution.py`. `BinanceSpotTestnetExecutionService` remains
TESTNET-only; `BinanceSpotExecutionService` is the explicit TESTNET/LIVE entry
point and requires a `LiveTradingGuard` for every connect, query, subscribe,
submit, and cancel operation. Public market monitoring is isolated in
`services/binance_monitor.py` and never requires credentials or order access;
its snapshots report receive latency percentiles and clock-skew samples.

USD-M/COIN-M Futures REST execution is exposed through
`services/binance_futures_execution.py`. It supports account and position
queries, open/order history and trade reconciliation, order submission, single
order cancellation, and cancel-all. LIVE clients must be created with
`allow_live=True` and a `LiveTradingGuard`; every query and order-changing
operation is checked against the guard. SHADOW may query but cannot submit or
cancel, while PAPER is rejected before any broker request. The adapter keeps
the official product-specific `/fapi` and `/dapi` routes and never falls back
from DEMO to LIVE.

The LIVE balance CLI commands only connect, synchronize time, and query
account data under the same guards. Spot reports per-asset free and locked
balances; USD-M Futures reports selected account and asset balance fields.
Decimal amounts remain strings, assets are never added across currencies,
and account payloads are filtered to balance fields before output.

## Research and LLM gates

Research services persist evidence and provenance before recommendation/review.
The recommendation gate combines technical and macro inputs and can abstain;
LLM services consume the supplied evidence contract and cannot bypass the
technical decision boundary. Adversarial review and production dual-track
behavior are covered by `test_adversarial_macro.py`,
`test_production_dual_track_llm.py`, `test_recommendation_gate.py`, and
`test_recommendation_evaluation_service.py`.

## PAPER and live-sync

PAPER execution is local ledger/matcher state. Live-sync records broker facts
and protection/review state; it does not turn observed fills into an implicit
broker order authority. Inspect `services/ashare_paper*`,
`services/live_trade*`, `runtime/paper_account_chain.py`, and the corresponding
`test_paper_*`/`test_live_*` files.

## Secrets and generated state

Credentials are delegated to `security/secrets.py` and OS keyring adapters.
Runtime databases, reports, caches and temporary files belong under
`runtime/` and are not source-of-truth code. `test_security_secrets.py`,
`test_integration_settings.py`, and `test_temp_root.py` verify these rules.
