# Live-record storage map

Read [`ARCHITECTURE.md`](../../../../ARCHITECTURE.md), [`docs/architecture/execution-boundaries.md`](../../../../docs/architecture/execution-boundaries.md), and tests under `tests/unit/storage/` and `tests/unit/services/live/`.

Preserve append-only records, leases, fencing, confirmation fingerprints, hash integrity and restart reconciliation. Keep pure codecs/policies separate from SQLite transactions.
