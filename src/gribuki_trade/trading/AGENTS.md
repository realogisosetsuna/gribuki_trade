# Trading OMS rules

Read [`ARCHITECTURE.md`](../../../ARCHITECTURE.md) and
[`docs/architecture/execution-boundaries.md`](../../../docs/architecture/execution-boundaries.md)
before changing the broker-neutral OMS.

- `trading/oms.py` is a durable boundary for commands, broker events, fills,
  balances and positions.
- Preserve account scoping, atomic create/submit behavior, idempotent fills,
  monotonic terminal states, and restart reconciliation of in-flight work.
- Keep broker-specific protocol code in `adapters/`; the OMS consumes ports and
  domain models.

Run `tests/unit/test_trading_oms.py` and the relevant broker adapter tests after
changes.
