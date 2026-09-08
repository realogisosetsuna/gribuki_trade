# Verification map

The repository currently collects 1,497 pytest cases (`python -m pytest
--collect-only -q` on 2026-09-08). A full local run on that date reported
1,491 passed, 5 skipped, and 1 failed; the failure is the time-sensitive
`test_alert_outbox_boundary_recovers_without_duplicate_notification` in
`tests/unit/test_live_trade_orchestration.py` (its retry clock uses the real
current time while the fixture uses 2026-08-17). Treat this as an existing
verification gap, not evidence that the architecture is green. CI runs the
following on Windows for Python
3.11 and 3.12 (`.github/workflows/quality.yml`):

```powershell
python -m ruff check conftest.py src tests
python -m mypy src
python scripts/check_repo_agent_readiness.py
python -m pytest --temp-dir runtime/tmp/ci -q
```

Use these narrower families while working:

- adapters and evidence: `test_akshare_*`, `test_baostock_adapter.py`,
  `test_official_*`, `test_cross_market_*`, `test_cboe_vix_adapter.py`;
- research/decision: `test_ashare_*`, `test_macro_*`,
  `test_recommendation_*`, `test_adversarial_macro.py`;
- durable execution: `test_paper_*`, `test_live_*`, `test_trading_oms.py`,
  `test_notification_*`, `test_*store.py`;
- strategy research: `test_strategy_lab_*`, `test_*backtest*`,
  `test_weekly_trend.py`, `test_crypto_trend.py`;
- boundaries/presentation: `test_runtime_guard.py`, `test_security_secrets.py`,
  `test_temp_root.py`, `test_cli.py`, `test_gui_*`, `test_report_*`.

Tests are predominantly offline and fixture-driven. They establish behavior and
failure contracts, but do not establish external provider uptime, production
broker permissions, or long-running soak. Any change claiming those properties
needs a separately documented integration or soak result.

The checked source tree is `src/gribuki_trade/`; all current test modules are
under `tests/unit/`.

`scripts/check_repo_agent_readiness.py` checks required maps, local Markdown
links, architecture evidence references, active-plan shape, and selected import
direction rules. It uses only the standard library and excludes generated
`runtime/` state.
