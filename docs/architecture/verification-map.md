# Verification map

The checked source tree is `src/gribuki_trade/`; test modules are grouped under
`tests/unit/` by provider, service and durable boundary. The last complete local
run for the clock-calibration and credential-persistence update reported
`1859 passed, 5 skipped, 41 subtests passed` on 2026-09-09. The five skips require
Windows symbolic-link permission; a pytest-cache permission warning is local
runtime state, not a source failure.

CI runs Windows with Python 3.11 and 3.12. Use Git Bash for local commands:

```bash
python scripts/check_repo_agent_readiness.py
python -m ruff check conftest.py src tests
python -m mypy src
python -m compileall -q src tests
python -m pytest --temp-dir runtime/tmp -q
```

Use the smallest matching test family first:

| Boundary | Test route |
|---|---|
| Binance protocols, clock, guards and execution | `tests/unit/binance/` |
| A-share adapters and workflows | `tests/unit/ashare/`, `tests/unit/adapters/ashare/` |
| Other provider adapters | `tests/unit/adapters/` |
| Application services | `tests/unit/services/` |
| SQLite stores and durable records | `tests/unit/storage/` |
| Broker-neutral/Spot/Futures OMS | `tests/unit/trading/` |
| Pure research and calculations | `tests/unit/analysis/`, `tests/unit/strategy_lab/` |
| Runtime, secrets and continuity | `tests/unit/runtime/` |
| CLI, GUI, reporting and ingest | corresponding directories under `tests/unit/` |
| Agent maps and source-layout invariants | `tests/unit/meta/` |

Tests are predominantly deterministic and offline. They prove behavior and
failure contracts, not provider uptime, indefinite soak behavior, real fill
quality or complete production permission coverage. LIVE `order/test` results
prove signed request/parameter acceptance without creating an exchange order.
Long-running private streams and actual execution still require separate
integration/soak evidence.

`scripts/check_repo_agent_readiness.py` checks required maps, local Markdown
links, architecture evidence references, active-plan shape, and selected import
directions. Generated `runtime/` state is excluded. See
[`source-layout.md`](source-layout.md) for canonical paths and
[`module-map.md`](module-map.md) for owners.
