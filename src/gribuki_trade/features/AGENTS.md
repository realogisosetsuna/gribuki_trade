# Feature map

Read [`ARCHITECTURE.md`](../../../ARCHITECTURE.md), [`docs/architecture/data-lineage.md`](../../../docs/architecture/data-lineage.md), and feature tests under `tests/unit/analysis/`, `tests/unit/ashare/` and `tests/unit/trading/`.

Feature calculations are deterministic and broker-free. Preserve point-in-time inputs and avoid network, storage or execution side effects.
