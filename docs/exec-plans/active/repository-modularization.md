# Repository modularization

Status: active

## Objective

Reduce the cognitive load of the largest application modules while preserving
the public entry points, broker safety boundaries, durable state behavior, and
existing test contracts. The first increment targets the Binance adapter,
CLI, and execution orchestration because they are the most operationally
sensitive and have clear ownership boundaries.

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

## Increment 1 scope

- Extract one or more cohesive internal modules from the Binance Spot gateway,
  retaining `adapters.binance.gateway` exports.
- Extract one low-risk CLI concern behind `gribuki_trade.cli:main`.
- Extract one low-risk execution/OMS concern without importing adapter code
  into broker-neutral modules.
- Update the architecture module map with the new ownership boundaries.

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
