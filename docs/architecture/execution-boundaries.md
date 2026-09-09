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

For USDⓈ-M unattended execution, `services/binance_futures_unattended.py`
combines the REST client with the routed private user stream and
`trading/futures/futures_oms.py`. Startup and every stream epoch change reconcile
balances, Hedge/One-way positions, normal orders, and Algo orders before order
changes resume. Raw events, fills, protection identities, command outcomes,
owner leases, and fencing tokens are durable in SQLite; a transport timeout or
process restart leaves a command `UNKNOWN` until REST evidence resolves it.
The service also refuses order changes when the private stream is disconnected
or degraded. Public market streams feed `services/binance_orderbook.py`, which exposes only
neutral `LocalOrderBookView` snapshots. A gap or reconnect moves the book to
`DESYNCED`, clears unsafe levels, and requires a REST snapshot bridge before
strategies can consume it.

The Futures service also exposes guarded risk configuration and protection
interfaces. `set_leverage` is per symbol; `set_margin_type` is `ISOLATED` or
`CROSSED`; position mode and USD-M multi-assets mode are account settings and
therefore use the `CHANGE_RISK` guard operation. Structured protection requests
cover fixed `STOP_MARKET`/`TAKE_PROFIT_MARKET`, legacy
`TRAILING_STOP_MARKET`, and the official `/fapi/v1/algoOrder` conditional-order
family. Legacy trailing orders validate `callbackRate` at 0.1–5 percent while
Algo trailing orders validate 0.1–10 percent. Dynamic strategy changes use
cancel-and-recreate with an explicit result for reconciliation; the service
does not claim an atomic amendment for conditional orders.

Spot advanced trading is exposed by the gateway as structured
`BinanceSpotOrderLeg` and order-list requests. The adapter supports native
stop/take-profit orders, integer-BIPS `trailingDelta`, OCO, OTO, OTOCO,
order-list cancellation, cancel-replace, and cancel-all-open-orders. Spot and
Futures trailing parameters are deliberately separate types and are never
converted implicitly.

Spot conditional order lists have a separate durable projection in
`trading/spot_order_lists.py`. `SQLiteSpotOrderListStore` stores the list
status and an independent member-leg table with raw event idempotency. The
Spot execution service writes `listStatus` before refreshing member orders, and
startup reconciliation merges `openOrderLists` with `allOrderList` REST
snapshots. A missing list route is treated as a configuration error when the
independent store is enabled; the service never treats a list event as a
single child-order fill.

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
`KeyringSecretProvider` also keeps a user-bound encrypted fallback on Windows
using DPAPI under `%LOCALAPPDATA%/gribuki-trade/secrets.json`. The fallback is
written atomically, never contains plaintext values, and is used only when the
same-user keyring is unavailable or has been reset after a profile migration.
Runtime databases, reports, caches and temporary files belong under
`runtime/` and are not source-of-truth code. `test_security_secrets.py`,
`test_integration_settings.py`, and `test_temp_root.py` verify these rules.
