# Gribuki Trade agent map

## Read this first

在本仓库的操作说明、验证命令和排障流程中，默认使用 Git Bash；除非用户
明确要求 PowerShell，否则不要把 PowerShell 作为主要命令格式。

For every task, read in this order: `AGENTS.md` → `ARCHITECTURE.md` → the
relevant file in `docs/architecture/` → the task's module and its tests. Do not
read the whole repository unless the task crosses those boundaries.

`README.md` is the user-facing guide. It contains operational examples and
historical status; architecture facts belong in `ARCHITECTURE.md` and
`docs/architecture/`.

For copyable planning, implementation, review and subagent prompts, see
`docs/agent-workflows.md`.

Before entering a subtree, check for its nearest `AGENTS.md`; deeper files
override this map for that subtree.

## Repository shape

- `src/gribuki_trade/domain`: value objects, state and event language.
- `src/gribuki_trade/ports`: protocols at infrastructure boundaries.
- `src/gribuki_trade/features`, `strategy`, `backtest`, `strategy_lab`,
  `analysis`, `policy`: calculations, experiments and decision rules.
- `src/gribuki_trade/services`: application orchestration and failure policy.
- `src/gribuki_trade/adapters`, `ingest`: external APIs, files and protocols.
- `src/gribuki_trade/storage`: SQLite stores, append-only records and outboxes.
- `src/gribuki_trade/runtime`, `security`: mode guards, process/runtime safety,
  credentials and temporary paths.
- `src/gribuki_trade/reporting`, `gui`: report artifacts and presentation.
- `tests/unit`: the executable contract for the modules above.
- `runtime/`: local generated state; it is intentionally not a source artifact.

## Non-negotiable boundaries

- `PAPER` must never access a real broker. `SHADOW` may connect, query and
  subscribe, but may not change real orders. `LIVE` requires process-local
  confirmation plus account and exchange allowlists. See
  `src/gribuki_trade/runtime/guard.py` and its tests.
- Strategy, LLM, GUI and review code must depend on ports/services, not broker
  SDKs. Keep external protocol code in adapters.
- Treat market and news evidence as point-in-time data. Preserve source,
  revision, timestamps and degradation; fail closed on ambiguity.
- SQLite stores are durable boundaries. Preserve idempotency, monotonic status,
  audit fields and restart behavior when changing schemas or writers.
- Do not put secrets in source, JSON settings, fixtures or logs. Use the
  security/keyring abstractions and their tests.
- Research and strategy-lab output is `research_only`; it cannot silently
  promote PAPER or LIVE configuration.

## Change workflow

1. Identify the module from `ARCHITECTURE.md` and locate its nearest tests.
2. For a significant refactor, create an ExecPlan in
   `docs/exec-plans/active/` before editing; keep status, decisions and
   validation commands there. Move completed plans to `docs/exec-plans/done/`.
3. Make the smallest change that preserves the documented boundaries.
4. Add or update a regression test for a bug pattern or contract change.
5. Run the narrow tests first, then the repository quality gates:
   `python scripts/check_repo_agent_readiness.py`,
   `python -m ruff check conftest.py src tests`, `python -m mypy src`, and
   `python -m pytest --temp-dir runtime/tmp -q`.
6. Update architecture docs when a dependency, boundary, durable state shape,
   or operational invariant changes. Do not add transient exploration notes.

## Verification and review

The CI contract is in `.github/workflows/quality.yml` and runs on Windows with
Python 3.11 and 3.12. Keep tests deterministic and offline unless a test is
explicitly an integration boundary. Use fixtures under `tests/fixtures` for
provider payloads. Before handing off, report commands run and any unverified
network, soak, or production claims.

When a deeper directory gains special rules, add a small local `AGENTS.md`
there and state which root rule it narrows or overrides. Keep all AGENTS files
as maps; put explanations in architecture or design-decision documents.
