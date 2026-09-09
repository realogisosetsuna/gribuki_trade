# Repository modularization roadmap

This document records the current physical layout and the remaining work. The
canonical paths are authoritative; historical facade paths were removed during
the repository-wide move and are retained only in archived changelogs.

## Current ownership model

```text
domain / ports
    ↓
adapters + ingest → pipeline normalization
    ↓
services / policy / strategy
    ↓
trading / storage / outboxes
    ↓
runtime guards → PAPER / SHADOW / LIVE
```

Provider wire code stays in `adapters`. Services compose ports, retries and
reconciliation. SQLite transactions and monotonic state transitions stay in
`storage` or `trading`. Pure models, codecs and projections do not import
network clients, broker SDKs, Qt or database connections.

## Canonical source groups

| Group | Canonical path | Scope |
|---|---|---|
| Binance adapters | `src/gribuki_trade/adapters/binance/{auth,transport,spot,futures,market_data}/` | Credentials/environment, signed HTTP, Spot/Futures REST and streams, local-book recovery |
| Other adapters | `src/gribuki_trade/adapters/{ashare,market_data,macro,llm,schwab,notifiers,simulated}/` | Provider boundaries and broker-free simulation |
| Binance services | `src/gribuki_trade/services/binance/` | Monitoring, Spot/Futures execution, PAPER, SHADOW and unattended recovery |
| A-share services | `src/gribuki_trade/services/ashare/{paper_day,intraday,close,evidence,research}/` | Research, PAPER-day, intraday and post-close workflows |
| Other services | `src/gribuki_trade/services/{live,macro,research,communications,llm,exit}/` | Live records/protection, macro and research orchestration, notifications, LLM and exit lifecycle |
| Durable state | `src/gribuki_trade/storage/{live_records,paper,research,execution}/` | SQLite schemas, append-only records, leases and outboxes |
| Trading state | `src/gribuki_trade/trading/{core,futures,spot}/` | Broker-neutral and Binance Futures/Spot OMS state transitions |
| CLI/presentation | `src/gribuki_trade/cli_commands/`, `reporting/`, `gui/` | Parsers, handlers, artifacts and UI integration boundaries |

## Operational slices completed

- Binance Spot and USDⓈ-M Futures protocol code is grouped by auth,
  transport, product and market-data responsibility.
- Spot order-list persistence and Spot/USDⓈ-M local order-book snapshot
  recovery have dedicated durable boundaries.
- LIVE execution services persist order/fill/protection outcomes and reconcile
  after reconnect or restart; PAPER/SHADOW/LIVE guards remain unchanged.
- `binance-time-sync` measures Binance server time and applies the measured offset
  before signed requests. Credentials are read through `security.secrets` and
  persist in OS keyring plus a same-user Windows DPAPI fallback.
- Unit tests mirror source domains under `tests/unit/`; no tests remain directly
  under `tests/unit/`.
- CLI, GUI, ingest, reporting, storage and strategy-lab concerns are grouped by
  responsibility with local maps and focused test routes.

## Remaining work

1. Add account-scoped Binance rate limiting and circuit breaking for unattended
   streams and signed REST commands.
2. Collect multi-hour authenticated network soak and restart evidence for
   public/private streams before enabling unattended strategy automation.
3. Continue splitting only when a module crosses a clear responsibility
   boundary; keep transaction and broker authority in their existing owners.
4. Keep architecture maps and nearest `AGENTS.md` files synchronized whenever a
   package is added or moved.

## Change and verification workflow

Before a structural move, read `AGENTS.md`, `ARCHITECTURE.md`, the matching
architecture page and the nearest local map. Move implementation and tests in
one change, update imports and documentation, then run:

```bash
python scripts/check_repo_agent_readiness.py
python -m ruff check conftest.py src tests
python -m mypy src
python -m compileall -q src tests
python -m pytest --temp-dir runtime/tmp -q
```

Use `python -m pytest --collect-only -q` before and after test moves to confirm
collection is unchanged. Do not commit runtime output, credentials or local
logs.
