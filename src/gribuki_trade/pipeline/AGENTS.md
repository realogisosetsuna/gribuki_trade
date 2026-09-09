# Pipeline agent map

Read repository `AGENTS.md`, `ARCHITECTURE.md` and `docs/architecture/module-map.md` before editing.
Normalization and deduplication must preserve point-in-time provenance, revision and degradation metadata. Keep pipeline code provider-neutral. Tests live in `tests/unit/ingest/` or `tests/unit/pipeline/` when present. Run focused tests, then repository gates.
