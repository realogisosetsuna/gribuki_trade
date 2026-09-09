# A-share service map

Read [`ARCHITECTURE.md`](../../../../ARCHITECTURE.md), [`docs/architecture/execution-boundaries.md`](../../../../docs/architecture/execution-boundaries.md), and tests under `tests/unit/ashare/` and `tests/unit/services/`.

- `paper_day/`: PAPER-day runner, ledger, matching, recovery and reports.
- `intraday/`: intraday PAPER and LLM gates/policies.
- `close/`: close analysis, sessions and post-close workflow.
- `evidence/`: evidence bundles; `research/`: screening, surveillance and research orchestration.

PAPER must remain broker-free and all durable writes stay in storage boundaries.
