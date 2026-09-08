# Binance unattended Futures execution

Status: active

## Objective

Implement USD-M private stream event adaptation, durable normal/algo order
management and guarded recovery; audit Spot/USD-M market and execution
capabilities against the current official documentation. Preserve PAPER/SHADOW/
LIVE boundaries and never claim production readiness from offline tests alone.

## Scope and decisions

- Use Git Bash for commands. Do not submit real orders or change account risk
  settings during verification.
- Keep Binance protocol parsing in adapters and durable normalized state in
  trading; application services own recovery and protection failure policy.
- Futures positions are keyed by account, environment, product, symbol and
  position side. Algo identity and triggered matching-engine order identity
  are distinct. Existing Spot durable schemas remain compatible.
- Any ambiguous submission/cancellation becomes UNKNOWN and requires broker
  reconciliation before retry. Reconnect does not imply reconciled state.
- Private streams use current USD-M /private endpoints. Native conditions use
  the Algo API. Unknown event/schema variants degrade trading health.
- Full official coverage is assessed explicitly; missing unrelated product
  families and unverified operational claims cannot be marked implemented.

## Work status

- [x] Read architecture, subtree rules, existing Futures and Spot boundaries.
- [x] Verify official contracts and record endpoint/feature coverage.
- [x] Implement Futures events and renewable/reconnecting private stream.
- [x] Implement durable order/algo commands, events, positions and recovery.
- [x] Integrate guarded runner, protection revisions, health and CLI.
- [x] Test failures, restart, duplicates, gaps, unknown outcomes and guards.
- [x] Run repository quality gates and applicable read-only network checks.
- [x] Update architecture, operational instructions and capability audit.

## Validation

Narrow: `.venv/Scripts/python.exe -m pytest tests/unit -k binance -q` plus
the new Futures store/service tests.

Gates: `scripts/check_repo_agent_readiness.py`, `ruff check conftest.py src
tests`, `mypy src`, `pytest --temp-dir runtime/tmp -q` using the venv Python.

Long-running soak, network, real matching-engine order execution and account
permissions require separate evidence and will be reported honestly.
