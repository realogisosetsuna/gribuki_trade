# ExecPlan: agent workflow hardening

Status: complete (2026-09-08)

## Objective

Apply the share conversation's repository-centric development practices where
they fit this Python trading workstation, while keeping the system's existing
runtime and trading boundaries unchanged.

## Delivered

- Added local maps for `runtime`, `storage`, `trading`, `strategy_lab`, and
  `adapters/binance`.
- Added `scripts/check_repo_agent_readiness.py` and its regression test.
- Added the check to the Windows CI quality job.
- Added `docs/agent-workflows.md` with task-context, review, subagent and
  worktree guidance.
- Updated architecture and root routing documents.

## Non-goals

No production Python modules, import APIs, trading-mode policy or external
broker authority were changed. Worktree and review-agent execution remains a
host capability; the repository documents how to use it without claiming an
automatic manager.

## Evidence and validation

The changes are grounded in the existing source tree and tests. The readiness
check passes; Ruff and mypy pass; focused structural, comment, runtime and OMS
tests pass (22 tests, 3 subtests). The full suite reports 1,492 passed, 5
skipped and one pre-existing time-sensitive failure in
`tests/unit/test_live_trade_orchestration.py`.

## Follow-up

The recorded clock-dependent failure remains in
`docs/architecture/verification-map.md`; fix it in a separate ExecPlan rather
than hiding it in this documentation refactor.
