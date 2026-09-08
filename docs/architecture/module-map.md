# Module map

| Area | Responsibility | Start here | Representative tests |
|---|---|---|---|
| `domain` | Shared models, events, orders, candidates, recommendations, PAPER and live state | `src/gribuki_trade/domain/__init__.py` | `test_orders.py`, `test_candidate_store.py`, `test_live_trade_records.py` |
| `ports` | Protocols for market data, news, LLM, notifier, broker and ledgers | `src/gribuki_trade/ports/__init__.py` | adapter protocol tests, `test_trading_oms.py` |
| `features` / `strategy` | Deterministic technical, screening, surveillance, cross-market and trend calculations | `features/*.py`, `strategy/*.py` | `test_technical_signals.py`, `test_ashare_screening.py`, `test_weekly_trend.py` |
| `services` | Application workflows, orchestration, gates and recovery | `services/*.py` | `test_*service*`, `test_live_trade_orchestration.py`, `test_ashare_paper_day.py` |
| `adapters` / `ingest` | Provider/API/file boundaries and payload validation | `adapters/`, `ingest/` | `test_akshare_*`, `test_official_*`, `test_binance_*`, Schwab tests |
| `pipeline` | Normalization and deduplication of incoming evidence | `pipeline/normalize.py` | `test_news_parsers.py`, event/evidence tests |
| `storage` | SQLite durable records, projections, leases and outboxes | `storage/*.py` | `test_*store.py`, `test_notification_outbox.py`, `test_trading_oms.py` |
| `runtime` / `security` | Modes, guards, temp roots, settings, credentials and continuity | `runtime/`, `security/` | `test_runtime_guard.py`, `test_temp_root.py`, `test_security_secrets.py` |
| `reporting` / `gui` | Report contracts/artifacts and PySide6 presentation | `reporting/`, `gui/` | `test_report_contract*`, `test_report_artifacts.py`, `test_gui_*` |
| `strategy_lab` / `backtest` | Offline experiments, factor DSL, walk-forward evaluation and costs | `strategy_lab/`, `backtest/` | `test_strategy_lab_*`, `test_crypto_backtest.py`, `test_recommendation_outcomes.py` |

Use the row matching the task, then follow its representative tests before
opening neighboring modules.

All representative tests in this table live under `tests/unit/`; the table
uses filename patterns to keep the routing map compact.

Local rules are deliberately limited to `src/gribuki_trade/runtime/`,
`storage/`, `trading/`, `strategy_lab/`, and `adapters/binance/`. Check the
nearest local `AGENTS.md` before changing one of those subtrees.
