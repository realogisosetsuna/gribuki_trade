# Repository modularization

Status: active

## Objective

Reduce the cognitive load of the largest application modules across the whole
repository while preserving public entry points, broker safety boundaries,
durable state behavior, and existing test contracts. The first increment
targets the Binance adapter, CLI, and execution orchestration because they are
the most operationally sensitive and have clear ownership boundaries.

## Baseline

The current package has a healthy top-level dependency direction, but several
files have grown beyond practical review size:

- `cli.py` is roughly 445 KiB and contains parser construction, command
  handlers, service wiring, and presentation formatting.
- `adapters/binance/gateway.py` is roughly 85 KiB and combines transport,
  signing, market data, account queries, order commands, and response parsing.
- `trading/futures_oms.py`, `services/binance_execution.py`, and several
  A-share workflows also combine persistence, state transitions, and runtime
  orchestration.

## Decisions

1. Refactor in vertical slices. Each slice keeps the old import path as a
   compatibility facade so callers and downstream sessions do not need a
   flag-day migration.
2. Move code according to responsibility, not file size alone. Pure parsing,
   request construction, state transitions, and orchestration should have
   separate homes and tests.
3. Preserve dependency direction: adapters know provider protocols; services
   compose ports; strategies and GUI do not import broker adapters.
4. Do not change live-trading authorization, retry, reconciliation,
   idempotency, or persistence semantics during a structural move.
5. Every extracted module gets a narrow test route. Full repository gates run
   after all parallel slices are reviewed.

## Increment 1 scope (completed)

- Extract one or more cohesive internal modules from the Binance Spot gateway,
  retaining `adapters.binance.gateway` exports.
- Extract one low-risk CLI concern behind `gribuki_trade.cli:main`.
- Extract one low-risk execution/OMS concern without importing adapter code
  into broker-neutral modules.
  The broker-neutral OMS slice is now `trading/oms_codec.py`, which owns pure
  SQLite row codecs, JSON/Decimal/time conversion, identifiers, and status
  projection helpers; `oms.py` retains all connections, transactions, leases,
  and durable state transitions.
- Update the architecture module map with the new ownership boundaries.

## Repository-wide follow-up slices

The remaining oversized files are handled by responsibility groups rather than
by arbitrary line ranges:

- A-share PAPER-day and post-close workflows: pure projections/configuration,
  calendar/session resolution, persistence, and orchestration.
- Storage and broker-neutral OMS: SQLite codecs, schema creation, leases,
  projections, and monotonic state transitions.
- Research and strategy-lab modules: dataset/manifest IO, pure evaluators,
  cost models, and report projections. The first research slice extracts
  deterministic exit-policy document codecs into
  `strategy_lab/exit_serialization.py`, while `exit_evaluator.py` remains the
  compatibility facade for the evaluator and historical private helper names.
- CLI: command registration, Binance handlers, A-share handlers, and output
  formatting behind the stable `gribuki_trade.cli:main` entry point.
- Reporting and GUI integrations: artifact serialization, provider adapters,
  and presentation-only code.

The storage slice now includes `storage/paper_day_codec.py`. It owns pure
SQLite row decoding, hash-chain digest construction, identifier validation,
and lease-argument normalization for `SQLitePaperDayStore`; the facade keeps
all connections, transactions, leases, and append-only transitions.

The A-share PAPER-day slice now includes
`services/ashare_paper_day_projection.py`. It owns the deterministic LLM gate
and DEEP exit audit/notification projections; the runner retains compatibility
wrappers while continuing to own scheduling, persistence, and side effects.

The AKShare daily-history slice now includes
`adapters/akshare_daily_parsing.py`. It owns provider symbol/date normalization,
DataFrame row extraction, column alias resolution, numeric/OHLC validation, and
`DailyBar` decoding. `adapters/akshare_daily.py` remains the compatibility
facade for client calls, timeout handling, source fallback, and routing; it
re-exports the historical exception and enum names through imports.

The reporting slice now includes `reporting/paper_day_codec.py`. It owns the
sidecar JSON object reader, JSONL event decoder, timestamp/date validation, and
scalar coercion helpers. `reporting/paper_day_summary.py` keeps the historical
private helper names as small compatibility wrappers and continues to own
projection assembly, Markdown rendering, and atomic report writing.

The same reporting slice also includes `reporting/paper_day_formatting.py` for
pure stable-code, timestamp, money, percentage, and Markdown-cell formatting.
The summary facade delegates those historical helper names without changing
the generated report contract.

Each slice must remove a real responsibility from its original file, retain a
compatibility facade while callers migrate, and add a focused test for the
new boundary. A module is not considered split merely because it was renamed
or wrapped by another equally large module.

## Validation

Run the focused Binance/CLI/OMS tests first, then:

```bash
python scripts/check_repo_agent_readiness.py
python -m ruff check conftest.py src tests
python -m mypy src
python -m pytest --temp-dir runtime/tmp -q
```

The existing live integration and long-running soak limitations remain
verification gaps; this refactor must not claim to prove them.
