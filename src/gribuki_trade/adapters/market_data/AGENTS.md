# Market-data adapter map

Read [`ARCHITECTURE.md`](../../../../ARCHITECTURE.md), [`docs/architecture/data-lineage.md`](../../../../docs/architecture/data-lineage.md), and tests under `tests/unit/adapters/market_data/`.

This package owns AKShare, BaoStock, archived daily and cross-market provider boundaries. Preserve source identity, revision, timestamps, fallback and degradation semantics. Do not add broker or strategy orchestration here.
