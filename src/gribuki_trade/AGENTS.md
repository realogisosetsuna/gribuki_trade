# Package agent map

Read repository `AGENTS.md`, `ARCHITECTURE.md`, then the relevant page in
`docs/architecture/` before editing. Use Git Bash commands by default.

Canonical package routing:
- `adapters/`: external API, WebSocket, file and simulation boundaries.
- `services/`: application workflows, retries, recovery and approval policy.
- `storage/` and `trading/`: durable SQLite transactions and OMS transitions.
- `domain/`, `ports/`, `features/`, `strategy/`, `policy/`: pure contracts and rules.
- `runtime/`, `security/`: mode guards, process safety, clock calibration and secrets.
- `ingest/`, `pipeline/`: source collection, normalization and deduplication.
- `reporting/`, `gui/`, `cli_commands/`: artifacts, presentation and CLI boundaries.

Tests mirror these areas under `tests/unit/`. Keep providers out of strategies,
keep secrets out of logs/source, and preserve durable idempotency and restart
semantics. Run the narrow area tests before the repository quality gates.
