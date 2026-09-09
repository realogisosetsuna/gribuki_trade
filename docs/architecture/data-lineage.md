# Data lineage and durable state

External payloads enter through `adapters/` or `ingest/`. They are validated,
normalized and deduplicated before services consume them. The event and raw
stores preserve source identity, timestamps, revision/content hashes and
health/degradation metadata. Evidence and research stores then bind analysis
to a reproducible run.

The point-in-time rule is visible in `domain/events.py`,
`storage/event_store.py`, `adapters/archived_daily.py`, and the tests
`test_event_store_as_of.py`, `test_archived_daily_adapter.py`,
`test_market_evidence.py`, `test_official_rates_evidence.py`, and
`test_cross_market_evidence.py`.

SQLite stores are explicit resource boundaries. Candidate/research/review
stores are separate from notification/report outboxes. Trading OMS and PAPER
stores keep idempotency keys, monotonic transitions and restart/reconciliation
state; see `storage/*.py`, `trading/core/oms.py`, and their `test_*store.py`,
`test_trading_oms.py`, and `test_paper_*` tests.

Shared WAL use is gated by the runtime SQLite version check in
`sqlite_runtime.py`; `test_sqlite_runtime.py` is the contract. Test and
standard-library scratch paths are resolved by `runtime/temp_root.py` and
`conftest.py`, with process-isolation tests in `test_temp_root.py`.

When changing a durable record, update its store tests, idempotency/recovery
tests, and this lineage map. Do not infer a schema from a generated database in
`runtime/`; use the store implementation and fixtures.

Evidence paths: `src/gribuki_trade/domain/events.py`,
`src/gribuki_trade/storage/event_store.py`, and
`tests/unit/test_event_store_as_of.py`.
