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

The storage slice now includes `storage/paper_day_codec.py` and `storage/live_record_codec.py`. The latter owns pure live-record scalar validation, canonical JSON, event/protection/work identifiers, and event hashes; `storage/live_records.py` retains SQLite transactions, leases, and orchestration boundaries. It owns pure
SQLite row decoding, hash-chain digest construction, identifier validation,
and lease-argument normalization for `SQLitePaperDayStore`; the facade keeps
all connections, transactions, leases, and append-only transitions.

The A-share PAPER-day slice now includes
`services/ashare_paper_day_projection.py`. It owns the deterministic LLM gate
and DEEP exit audit/notification projections; the runner retains compatibility
wrappers while continuing to own scheduling, persistence, and side effects.

The nested PAPER-day runner also delegates pure K-line/technical-bar codecs,
exit-barrier helpers, UTC normalization, canonical hashing, and event JSONL/file
primitives to `services/ashare/ashare_paper_day_serialization.py`. Historical
private helper names remain aliases in the runner module. Focused validation is
covered by `tests/unit/test_ashare_paper_day_serialization.py` and the existing
PAPER-day compatibility suite.

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

## Runtime PAPER continuity slice

`runtime/paper_account_manifest.py` now owns the side-effect-free PAPER
lineage contract: manifest parsing, canonical JSON, account and seal hashes,
source-prefix validation, and projected-ledger compatibility checks.
`runtime/paper_account_chain.py` remains the compatibility facade and owns
cross-process locks, SQLite backup, metadata tables, filesystem atomicity, and
recovery orchestration. The historical public `PaperAccountChainError`,
`PaperAccountLedgerPreparation`, and `prepare_paper_day_ledger` imports remain
unchanged; the old private helper names are aliases for the pure module during
the migration. Regression coverage is in
`tests/unit/test_paper_account_manifest.py` and
`tests/unit/test_paper_account_chain.py`.

## Provider and command directory grouping

The implementation now has explicit navigation roots for A-share, cross-market,
macro, simulated, Binance, and CLI command families. This increment also extracts
Binance Spot order parameter encoding, cross-market payload parsing, reporting
Markdown rendering, adversarial macro audit serialization, strategy exit simulation,
A-share quantity/configuration policy, and close-analysis indicators into focused
modules. The durable-state follow-up also extracts live-record row models, OMS schema
initialization, and A-share screening factors. Binance workflows and read-only A-share market/research handlers are grouped
under `cli_commands/handlers/` alongside the parser families. Compatibility aliases at
the former flat paths are intentionally thin and use the implementation module
object so existing imports and monkeypatch-based operational tests retain their
semantics. This grouping is structural; it does not alter broker permissions,
order state transitions, or durable schemas.

The next verified boundary slices extract Binance REST request encoding,
live-record schema/migration DDL, and A-share PAPER-day session-time calculations.
These modules remain pure or connection-scoped; transport, transaction, calendar
I/O, and runner side effects stay in their original facades.

The following increment adds post-close CLI result contracts, adversarial macro
policy projections, and Futures OMS schema DDL. Their facades retain compatibility
names while provider calls, transaction boundaries, and durable state transitions
remain at the original boundaries.

This increment also adds AKShare delayed-history stitching, intraday LLM audit
serialization, and NapCat process lifecycle boundaries. Provider access, LLM
execution, and GUI settings remain in their original facades.

The current increment additionally separates PAPER-day LLM sidecar projections,
intraday execution policy, and Binance CLI result shaping. These are pure data or
policy modules; file loading, account matching, network calls, and LIVE guards stay
in their existing owners.

The latest increment separates search-discovery result policy from provider HTTP
transport and extracts Futures OMS order-status monotonicity, protection-plan
revision checks, and restart-recovery query construction. The facades retain their
historical private names while storage, network, transaction, and event-generation
ownership remain unchanged. Focused validation covered 39 discovery/Futures OMS,
module-layout, and source-comment tests; the full suite then passed 1771 tests with
five environment-skipped tests, 41 subtests, and one recurring Windows pytest-cache
warning. Readiness, Ruff, mypy (340 source files), and compileall also passed.

The broker-neutral OMS now delegates pure fill-to-position projection to
`trading/oms_position_policy.py`; the facade still owns row reads and transaction
writes. The projection is covered by dedicated opening, partial-close, and reversal
tests.

The same follow-up extracts AKShare provider payload decoding into
`adapters/market_data/akshare_payload.py` and exit-strategy trading-day walk-forward
planning into `strategy_lab/exit_walk_forward.py`. Focused tests for these slices and
the OMS projection passed; the subsequent full suite passed 1778 tests with five
environment-skipped tests, 41 subtests, and the same Windows pytest-cache warning.
Ruff and mypy passed for 343 source files.

The durable live-record store now delegates A-share T+1/FIFO protection-lot
quantity policy and retry/dead-work projection to pure policy modules. The
intraday LLM facade also delegates stable context/review identities and scalar
validation to `ashare_intraday_llm_policy.py`. Focused live-record, intraday LLM,
module-layout, and source-comment tests passed 46 tests; Ruff and mypy passed for
345 source files.

AKShare payload ownership now also includes symbol, numeric, timestamp, volume-unit,
and trade-direction normalization, reducing the market-data facade below 900 lines
while preserving its provider fallback and cache boundaries. The payload contract
tests passed 25 cases in the current verification route; full-suite validation is
still required after the in-flight Binance adapter slice.

The latest increment separates live-sync result payloads from the CLI facade,
strict PAPER-day LLM audit payload recovery from the nested runner, Binance error
and rate-limit protocol helpers from the REST gateway, and live-record work-queue
lease/query policy from the SQLite store. These modules are pure or connection-
independent; command dispatch, HTTP transport, transactions, and state changes
remain in their original owners.

Focused validation for this increment passed 260 tests. The full suite passed 1730
tests with five environment-skipped tests and 41 subtests; Ruff, mypy (325 source
files), readiness, compileall, and the Chinese-source check passed.

The next increment isolates close-research CLI payloads, Futures order parameters,
and PAPER-day notification/report projections while preserving command, transport,
outbox, and persistence ownership in their facades.

Focused validation for this increment passed 271 tests; full-suite validation is
run with an isolated temporary root because old shared Windows pytest basetemps can
be locked by prior symbolic-link tests.

After restoring direct-call compatibility for the extracted Futures parameter
helpers, the full suite passed 1742 tests with five environment-skipped tests and
41 subtests. Ruff, mypy (328 source files), readiness, compileall, and the
Chinese-source check passed.

Validation evidence for this increment: CLI and parser tests 195 passed; the
Binance Spot, cross-market, reporting, adversarial macro, exit simulation, A-share
quantity/configuration, and close-analysis suites passed together (398 focused
tests). The live-record, OMS, screening, and module-layout follow-up passed 82
focused tests; Ruff, mypy (310 source files), readiness, compileall, and
Chinese-source checks passed. The full suite passed 1682 tests, with five
environment-skipped tests and 41 subtests. Full quality gates remain required after
the next orchestration slice is merged.

The current follow-up focused route passed 101 tests, including the Chinese-source
check, module-layout checks, Binance gateway/request-builder tests, live-record
schema/model/store tests, and PAPER-day schedule/runner tests. Full quality gates
remain required after this follow-up is staged.

## Futures REST parsing boundary (this increment)

Status: implementation complete; parent full-repository validation complete.

`adapters/binance/futures_parsing.py` now owns pure Futures response decoding and
scalar validation. It contains the USD-M/COIN-M Ticker shape adapter, order-book
level/snapshot parser, and symbol/enum/listen-key checks. `futures.py` remains a
compatibility facade and still owns HTTP signing, credentials, transport, clock
synchronization, runtime authority, and all order-changing endpoints.

Focused evidence:

```bash
python -m pytest tests/unit/test_binance_futures_parsing.py \
  tests/unit/test_binance_futures_order_params.py \
  tests/unit/test_binance_orderbook.py -q
```

Result: 28 passed (with the recurring Windows pytest-cache permission warning).
Ruff and mypy passed for the changed source and test files. The parent agent
must run the repository-wide readiness, Ruff, mypy, compileall, source-comment,
and full pytest gates after combining parallel slices.

The live-sync/PAPER-day payload slice and the gateway/live-work policy slice are
now pushed. Focused payload and storage/provider routes passed; the repository
gates then passed readiness, Ruff, mypy (333 source files), compileall, and the
full suite with 1758 passed, 5 environment-skipped tests, and 41 subtests.

The next reporting/execution slice is also prepared: PAPER-day account, fill,
order, and notification projections now live beside their immutable sidecar
models, while Binance execution record conversions live outside the execution
orchestrator. Focused reporting and execution routes passed 39 tests; the
facades retain their historical private helper names and durable side effects.

The A-share screening payload slice now follows the same boundary: provider row
decoding, security/date/number validation, coverage checks, and revision hashes
are isolated in `adapters/ashare/screening_payload.py`; the adapter facade keeps
AKShare calls, timeout/concurrency control, fallback selection, and degradation
reporting.

After preserving the screening facade exports used by preopen and cross-market
adapters, the full repository gate passed again: readiness, Ruff, mypy (337
source files), compileall, and 1765 tests passed, with five environment skips
and 41 subtests.

The adversarial macro boundary slice now isolates role-request construction,
failure-safe ``ABSTAIN`` analysis, and sanitized failed-call audit documents;
provider concurrency, session budgets, and conservative aggregation remain in
the service facade.

The adversarial slice passed the full repository gate as well: readiness, Ruff,
mypy (338 source files), compileall, and 1765 tests passed, with five
environment skips and 41 subtests.

The live-record protection slice now isolates A-share T+1 sellable-quantity and
FIFO buy-lot allocation in `storage/live_record_protection_policy.py`; the
work-policy module also exposes a pure retry/dead-state projection. The store
facade continues to own SQLite reads, allocation writes, lease fencing, and
transaction boundaries. Focused validation covers the new policy tests together
with live-record, module-layout, and Chinese-source checks.

The current provider/UI follow-up passed 106 focused tests, then the full suite
passed 1718 tests with five environment-skipped tests and 41 subtests. Ruff,
mypy (322 source files), readiness, compileall, and the Chinese-source check all
passed after the final compatibility fixes.

The next focused route passed 212 behavioral tests before the final Chinese-source
comment cleanup; the full route will be rerun before staging this increment.

The completed combined increment adds the expanded AKShare payload normalization
boundary and Futures REST response parsing boundary described above. Repository-wide
validation then passed readiness, Ruff, mypy (346 source files), compileall, and the
full pytest route with 1808 passed, five environment-skipped tests, 41 subtests,
and one recurring Windows pytest-cache permission warning.
