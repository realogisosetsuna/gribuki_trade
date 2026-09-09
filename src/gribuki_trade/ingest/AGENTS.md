# Ingest map

Read [`ARCHITECTURE.md`](../../../ARCHITECTURE.md), [`docs/architecture/data-lineage.md`](../../../docs/architecture/data-lineage.md), and tests under `tests/unit/ingest/`.

Ingest validates, normalizes and records source metadata. Preserve publication/observation timestamps, revisions and degradation; durable writes belong in storage/services.
