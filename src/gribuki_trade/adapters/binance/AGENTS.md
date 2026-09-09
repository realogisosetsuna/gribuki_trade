# Binance adapter rules

Read [`ARCHITECTURE.md`](../../../../ARCHITECTURE.md) and
[`docs/architecture/execution-boundaries.md`](../../../../docs/architecture/execution-boundaries.md)
before changing this adapter.

- Environment and stage selection must be explicit; never silently fall back
  from LIVE-like settings to another environment.
- Keep gateway, stream and user-stream access behind the runtime guard; tests
  and local PAPER/SHADOW paths must not acquire production order authority.
- Preserve Decimal precision and exchange-filter validation in `rules.py`.
- Do not expose API keys, secrets or signatures in errors or representations.
- Uncertain execution must reconcile before retrying.

Canonical implementation routes are `auth/`, `transport/`, `spot/`,
`futures/`, and `market_data/`. Root-level flat modules are no longer import
paths. Server-clock calibration lives in `transport/time_sync.py`; it adjusts
client signing timestamps in memory and must not set the operating-system
clock or persist credentials.

Run `python -m pytest --temp-dir runtime/binance-tests -q tests/unit/binance`
after changes. For credential persistence also run
`tests/unit/runtime/test_security_secrets.py`.
