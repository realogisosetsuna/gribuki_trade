# Execution boundaries and safety invariants

These are current contracts, verified by source and tests.

## Modes

- `PAPER` cannot access a real broker.
- `SHADOW` permits connection, queries and subscriptions but rejects operations
  that change orders.
- `LIVE` requires the exact process-local confirmation phrase and allowlisted
  account and exchange. Confirmation is not loaded from environment/config.

Source: `src/gribuki_trade/runtime/mode.py` and `runtime/guard.py`.
Verification: `tests/unit/test_runtime_guard.py`, broker adapter tests, and
CLI tests covering confirmation errors.

## Research and LLM gates

Research services persist evidence and provenance before recommendation/review.
The recommendation gate combines technical and macro inputs and can abstain;
LLM services consume the supplied evidence contract and cannot bypass the
technical decision boundary. Adversarial review and production dual-track
behavior are covered by `test_adversarial_macro.py`,
`test_production_dual_track_llm.py`, `test_recommendation_gate.py`, and
`test_recommendation_evaluation_service.py`.

## PAPER and live-sync

PAPER execution is local ledger/matcher state. Live-sync records broker facts
and protection/review state; it does not turn observed fills into an implicit
broker order authority. Inspect `services/ashare_paper*`,
`services/live_trade*`, `runtime/paper_account_chain.py`, and the corresponding
`test_paper_*`/`test_live_*` files.

## Secrets and generated state

Credentials are delegated to `security/secrets.py` and OS keyring adapters.
Runtime databases, reports, caches and temporary files belong under
`runtime/` and are not source-of-truth code. `test_security_secrets.py`,
`test_integration_settings.py`, and `test_temp_root.py` verify these rules.
