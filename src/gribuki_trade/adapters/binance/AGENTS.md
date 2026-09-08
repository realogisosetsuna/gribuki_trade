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

Run `python -m pytest tests/unit -k binance` after changes; this avoids shell
glob differences while covering the Binance test family.
