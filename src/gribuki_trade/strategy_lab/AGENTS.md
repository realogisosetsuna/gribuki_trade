# Strategy lab rules

Read [`ARCHITECTURE.md`](../../../ARCHITECTURE.md) and
[`docs/architecture/design-decisions.md`](../../../docs/architecture/design-decisions.md)
before changing experiments or evaluators.

- This package is offline research only.
- Preserve frozen dataset/source hashes, point-in-time boundaries,
  walk-forward purge/embargo and holdout locking.
- Keep trial outputs `research_only=True` and
  `promotion_authorized=False`; never promote results to PAPER or LIVE.
- Keep the factor DSL bounded and reject arbitrary Python execution.

Run `tests/unit/strategy_lab/test_strategy_lab_*.py` and the relevant backtest tests.
