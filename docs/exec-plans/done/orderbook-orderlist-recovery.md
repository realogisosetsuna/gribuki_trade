# Order book and Spot order-list recovery

Status: complete

## Objective

Add restart-safe local order-book snapshot/diff recovery for Spot and USDⓈ-M,
and persist Spot order-list identity and member legs for unattended recovery.

## Scope and decisions

- Keep Binance payload parsing and REST/WebSocket details in adapters.
- Keep the local book recovery state in the adapter/service boundary and rebuild
  it from a fresh REST snapshot after restart; keep Spot list state durable in
  the trading store. Strategies consume neutral snapshots and do not import
  Binance protocol types.
- A depth gap, malformed snapshot, or stale first update fails closed and
  requires a fresh snapshot before publishing a book.
- Spot order-list writes are idempotent by account/environment/list identity;
  list events trigger atomic member-leg reconciliation before strategy use.
- PAPER/SHADOW/LIVE boundaries remain unchanged; tests stay offline.

## Work status

- [x] Inspect existing Spot/Futures streams, depth REST routes, and OMS schemas.
- [x] Implement snapshot/diff recovery and focused tests.
- [x] Implement durable Spot order-list store and reconciliation.
- [x] Update services, architecture docs, and capability audit.
- [x] Run repository quality gates and push the completed commit.

## Validation

Focused tests cover first snapshot bracketing, stale/gap recovery, sequence
monotonicity, duplicate list events, restart reconstruction, and member-leg
reconciliation. Full gates are the repository readiness check, Ruff, mypy, and
pytest commands in `AGENTS.md`.

The repository-wide pytest run retains the pre-existing time-dependent failure
in `test_alert_outbox_boundary_recovers_without_duplicate_notification`; all
Binance-focused tests and static quality gates pass.
