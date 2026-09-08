# Binance LIVE Spot and USDⓈ-M execution

Status: complete

## Objective

Expose guarded, user-facing LIVE Spot and USDⓈ-M Futures read-only checks,
order validation, submit, cancel, and reconciliation while keeping PAPER,
SHADOW, TESTNET, and DEMO boundaries explicit.

## Decisions

- LIVE operations require the existing `LiveTradingGuard` and explicit
  account/exchange allowlists.
- Credentials remain in the OS keyring; output must redact keys, signatures,
  balances, and order payload secrets.
- Read-only health and permission checks run before any order operation.
- Ambiguous network results are reconciled before retrying; no blind resubmit.
- CLI defaults remain non-LIVE and existing testnet/demo commands remain
  unchanged.

## Implementation

- Add a guarded LIVE Spot CLI status/permission check and execution wrapper.
- Extend USDⓈ-M Futures with live order/cancel/query operations and guarded
  service orchestration.
- Add regression tests for environment selection, guard enforcement, request
  paths, idempotency and redaction.
- Update architecture and operational documentation.

## Validation

- Run focused Binance tests.
- Run repository readiness, Ruff, mypy, and full pytest.
- Run the user's configured LIVE credentials only for signed read-only account,
  permission, server-time, exchange-info and order-test checks; do not submit
  a live order.
- The final full suite remains at 1514 passed, 5 skipped, 1 pre-existing
  time-sensitive live-orchestration failure; the Binance-focused suite passes.
