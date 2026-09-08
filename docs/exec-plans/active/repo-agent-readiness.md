# ExecPlan: agent-readable repository map

Status: active (documentation-only refactor)

## Objective

Create a small root map and evidence-backed architecture index so a new agent
can read `AGENTS.md → ARCHITECTURE.md → task-specific architecture → code/tests`.

## Scope

- Add root `AGENTS.md` with navigation, boundaries, workflow and quality gates.
- Add root `ARCHITECTURE.md` with verified dependency direction and limits.
- Add `docs/architecture/` module, execution-boundary, lineage, verification,
  and design-decision pages.
- Establish the active ExecPlan convention for future significant refactors.

## Evidence used

`pyproject.toml`, `.github/workflows/quality.yml`, `conftest.py`,
`src/gribuki_trade/{cli.py,runtime,domain,ports,features,services,adapters,ingest,pipeline,storage,reporting,security,gui,strategy_lab,backtest}`,
and the 1,497 collected tests under `tests/unit`.

## Validation

- `python -m pytest --collect-only -q` (collection baseline: 1,497 tests).
- Full pytest was run locally: 1,491 passed, 5 skipped, 1 failed. The existing
  failure is the fixture-clock issue documented in
  `docs/architecture/verification-map.md`.
- Ruff and mypy remain the required quality gates and should be run before
  moving this plan to `done/`.

## Completion criteria

All architecture claims link to source/test paths; no generated runtime data is
treated as code; the root AGENTS file remains a map rather than an encyclopedia.
After the quality gates are run, move the plan to `docs/exec-plans/done/` and
retain it as the audit record.
