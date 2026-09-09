# Policy agent map

Read repository `AGENTS.md`, `ARCHITECTURE.md` and `docs/architecture/module-map.md` before editing.
Policies enforce safety and recommendation gates; they must not call broker SDKs or silently promote research output to PAPER/LIVE. Tests live in `tests/unit/policy/` or the nearest policy-focused suite. Run focused tests, then repository gates.
