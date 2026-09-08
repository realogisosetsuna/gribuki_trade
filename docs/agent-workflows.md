# Agent workflow reference

This is a copyable operating method for Codex sessions. It does not grant
permissions or replace the repository rules in `AGENTS.md`.

## Task-context prompt

```text
Read AGENTS.md first.
Task: <specific request>
Identify the relevant subsystem from ARCHITECTURE.md and its nearest local
AGENTS.md. Read only the linked architecture/design pages, implementation files
and tests needed for this task. Before editing, report current behavior,
dependency boundaries, invariants, hidden dependencies, risks and validation.
For a significant refactor, create or update docs/exec-plans/active/<name>.md.
```

## Session roles

Use separate sessions when the task is large enough to benefit from clean
context: planning/exploration, implementation, independent review, and final
validation. A review session should receive `AGENTS.md`, the relevant
architecture pages, the active ExecPlan, the diff, and test results. It should
question the implementation's assumptions instead of relying on the authoring
conversation.

## Bounded subagents

Delegate one bounded question at a time. Ask each worker to return only:

```text
Relevant files
Current behavior
Important invariants
Hidden dependencies
Risks
Recommended change boundary
Required tests
```

Keep raw exploration, long logs and intermediate searches out of the main
context. The main agent owns the final scope and decisions.

## Worktrees

Use separate worktrees only when changes have clear file boundaries and can be
validated independently. Keep a single checkout for tightly coupled changes;
the repository does not provide an automatic worktree manager. Before merging,
rerun the readiness check, relevant tests and the full quality gates.

## Evidence handoff

At handoff, record changed files, commands and results, known failures, and
unverified network/soak claims in the ExecPlan or review output. Temporary
exploration that does not change a durable rule, architecture fact, regression
test or long-running plan stays in the conversation.
