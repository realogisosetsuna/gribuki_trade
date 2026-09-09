# Storage rules

Read [`ARCHITECTURE.md`](../../../ARCHITECTURE.md) and
[`docs/architecture/data-lineage.md`](../../../docs/architecture/data-lineage.md)
before changing a store.

- SQLite stores are durable boundaries; preserve transactions, idempotency,
  monotonic status, provenance and restart/recovery behavior.
- Schema changes must be derived from store code and tests, never generated
  databases under `runtime/`.
- Preserve writer/lease and shared-WAL safety checks in `sqlite_runtime.py`.
- Update the store's focused tests whenever a record or transition changes.

Useful contracts include `tests/unit/storage/test_*store.py`,
`tests/unit/storage/test_notification_outbox.py`, `tests/unit/trading/test_trading_oms.py`,
and `tests/unit/storage/test_paper_day_store.py`. The OMS in `trading/oms.py` is a
separate durable boundary; run its tests when changing OMS behavior.
