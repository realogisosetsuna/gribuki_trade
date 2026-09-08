# Architecture map

This document is a verified map of the current repository. Claims below are
anchored to source files and tests; it is not a proposal for a future rewrite.

## System shape

The package is a Python 3.11–3.12 application with one console entry point,
`gribuki_trade.cli:main`, and a module entry point in
`src/gribuki_trade/__main__.py`. The dependency and quality configuration is in
`pyproject.toml`; CI repeats the Ruff, mypy and pytest gates in
`.github/workflows/quality.yml`.

The repository also runs `scripts/check_repo_agent_readiness.py`, a standard
library structural check for documentation routing, active ExecPlans and a
small set of dependency-direction rules. Its contract is tested by
`tests/unit/test_repo_agent_readiness.py`.

The implementation follows this direction:

```text
external APIs/files
        ↓
adapters + ingest → pipeline normalization/dedupe
        ↓
ports ← services/application orchestration → storage/outboxes
        ↓                         ↓
features/policy/strategy      reporting/gui/notifications
        ↓
PAPER ledger/matcher or guarded broker boundary
```

`domain` supplies shared models and invariants. `ports` define boundary
protocols. Pure calculations live in `features`, `strategy`, `backtest`,
`analysis`, and `strategy_lab`. `services` compose those pieces, enforce
failure/approval policy, and select adapters. `storage` is the durable side of
the application. This layering is visible from imports and package contents;
the principal module map is expanded in `docs/architecture/module-map.md`.

## Execution boundaries

`TradingMode` has `PAPER`, `SHADOW`, and `LIVE` values
(`runtime/mode.py`). `LiveTradingGuard` rejects real-broker operations in
PAPER, rejects order-changing operations in SHADOW, and requires a
process-local confirmation phrase plus account/exchange allowlists in LIVE
(`runtime/guard.py`). Tests in `test_runtime_guard.py` and the broker adapter
tests exercise these branches.

The A-share research/PAPER path is exposed through subcommands registered in
`cli.py`. The CLI composes screening, surveillance, research, candidate,
recommendation, PAPER-day, post-close, notification, and live-sync services;
tests named `test_cli.py`, `test_ashare_*`, `test_paper_*`, and
`test_live_*` provide the executable coverage map.

Binance code is split between `adapters/binance` and services for execution,
PAPER and SHADOW. The CLI labels execution entry points TESTNET-only. Schwab
code is an adapter layer with offline transport/OAuth tests; the README and
`docs/BINANCE_SCHWAB_INTEGRATION.md` record that production/user-facing
workflow is not verified here.

## Evidence and data flow

Ingest adapters validate provider payloads, normalize them through
`pipeline/normalize.py`, and deduplicate through `pipeline/dedupe.py`. Domain
events retain publication/observation/availability timestamps and source
revision. Research services persist raw/evidence/research records before
producing reports or recommendations. Candidate, review, notification and
report-artifact stores use SQLite and explicit idempotency/status transitions.

Market-data fallback, point-in-time selection, source identity, malformed data,
and degradation are tested in the adapter and evidence test families. The
details and file-level index are in `docs/architecture/data-lineage.md`.

## Durable state and runtime safety

`storage/` contains event, raw, source-health, candidate, research, review,
outbox, PAPER, live-record, exit-plan and strategy-experiment stores. Trading
OMS behavior is in `trading/oms.py` and tested by `test_trading_oms.py`.
PAPER-day uses per-session journal/ledger/outbox/report sidecars, with
cross-day continuity helpers in `runtime/paper_account_chain.py`.

Temporary paths are centralized by `runtime/temp_root.py` and root
`conftest.py`; tests in `test_temp_root.py` verify process isolation and path
validation. SQLite shared-WAL approval is explicit in `sqlite_runtime.py` and
covered by `test_sqlite_runtime.py`.

## Verified limits

Offline tests do not prove provider availability, long-running soak behavior,
real broker execution, or production LLM reliability. The README and existing
integration documents explicitly mark those limits. Keep such statements out
of architecture as capabilities; record them as verification gaps instead.

For the progressive-read path, start with `docs/architecture/module-map.md`,
then open the boundary document matching the task, then the named source/test
files. `docs/architecture/verification-map.md` maps behavior to tests.
