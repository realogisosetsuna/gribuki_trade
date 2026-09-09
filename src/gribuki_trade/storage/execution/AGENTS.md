# Execution storage map

Read [`ARCHITECTURE.md`](../../../../ARCHITECTURE.md), [`docs/architecture/execution-boundaries.md`](../../../../docs/architecture/execution-boundaries.md), and tests under `tests/unit/storage/`.

Owns exit plans, outboxes, execution audits and experiment records. Preserve durable delivery, monotonic status and restart/recovery semantics.
