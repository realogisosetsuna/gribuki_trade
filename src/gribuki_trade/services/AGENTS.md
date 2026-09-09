# Service map

Read [`ARCHITECTURE.md`](../../../ARCHITECTURE.md), [`docs/architecture/execution-boundaries.md`](../../../docs/architecture/execution-boundaries.md), and tests under `tests/unit/services/` before editing.

Services compose ports and adapters, enforce failure policy, guards, recovery and durable transaction boundaries. Keep provider protocol code in adapters and strategy calculations in features/strategy.
