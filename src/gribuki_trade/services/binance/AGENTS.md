# Binance service map

Read [`ARCHITECTURE.md`](../../../../ARCHITECTURE.md), [`docs/architecture/execution-boundaries.md`](../../../../docs/architecture/execution-boundaries.md), and tests under `tests/unit/binance/`.

This package owns Binance monitoring, Spot/Futures execution, PAPER, SHADOW, reconciliation and unattended private-stream orchestration. LIVE operations require the runtime guard, account allowlist and explicit confirmation. Preserve UNKNOWN/reconciliation behavior on uncertain transport results.
