# Runtime rules

Read [`ARCHITECTURE.md`](../../../ARCHITECTURE.md) and
[`docs/architecture/execution-boundaries.md`](../../../docs/architecture/execution-boundaries.md)
before changing this package.

- `TradingMode` and `LiveTradingGuard` are the broker safety boundary.
- LIVE confirmation is process-local and must never be loaded from env/config.
- PAPER/SHADOW/LIVE behavior must remain fail-closed.
- Temporary paths must go through `TempRootResolver`; do not treat generated
  `runtime/` files as schema or source-of-truth.
- PAPER account continuity and cross-process recovery are runtime contracts.

Run the focused contracts after changes:
`tests/unit/test_runtime_guard.py`, `tests/unit/test_temp_root.py`, and
`tests/unit/test_paper_account_chain.py`.
