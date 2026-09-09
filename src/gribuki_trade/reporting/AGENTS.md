# Reporting map

Read [`ARCHITECTURE.md`](../../../ARCHITECTURE.md), [`docs/architecture/source-layout.md`](../../../docs/architecture/source-layout.md), and tests under `tests/unit/reporting/`.

- `paper_day/`: sidecar decoding, projections and deterministic rendering.
- Root modules: artifact and report contracts.

Reporting is presentation-only and must not write broker, ledger or OMS state.
