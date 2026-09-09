# PAPER storage map

Read [`ARCHITECTURE.md`](../../../../ARCHITECTURE.md), [`docs/architecture/data-lineage.md`](../../../../docs/architecture/data-lineage.md), and tests under `tests/unit/storage/`.

Owns PAPER-day, ledger and order stores. Preserve atomic transactions, idempotent events, lease behavior and cross-session continuity.
