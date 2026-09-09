# A-share adapter map

Read [`ARCHITECTURE.md`](../../../../ARCHITECTURE.md), [`docs/architecture/data-lineage.md`](../../../../docs/architecture/data-lineage.md), and tests under `tests/unit/ashare/` and `tests/unit/adapters/ashare/`.

- `market/`: breadth, context, derivatives and surveillance providers/parsers.
- `screening/`: universe/history screening, factor and payload validation.
- `profile/`: point-in-time instrument profiles.

Keep network/retry behavior in adapters and pure payload rules side-effect free.
