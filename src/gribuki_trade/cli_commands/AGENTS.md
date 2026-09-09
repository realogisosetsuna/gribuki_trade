# CLI commands agent map

Read repository `AGENTS.md`, `ARCHITECTURE.md` and `docs/architecture/module-map.md` before editing.
Keep parsers and handlers as a presentation boundary: call services and ports, never broker SDKs directly; preserve confirmation and secret-redaction behavior. Tests live in `tests/unit/cli/`. Run focused CLI tests, then repository gates.
