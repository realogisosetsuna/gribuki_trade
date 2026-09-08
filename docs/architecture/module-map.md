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

The A-share PAPER-day runner keeps scheduling and durable side effects in
`services/ashare_paper_day.py`. Its deterministic audit and notification
projections live in `services/ashare_paper_day_projection.py`; the runner
retains compatibility wrappers for historical private helper names. The
projection module has no storage, network, scheduler, or broker dependency and
is tested independently in `test_ashare_paper_day_projection.py`.

## Large-module ownership boundaries

The repository keeps compatibility facades at historical import paths while
moving cohesive, side-effect-free concerns into small modules. The current
first increment is:

| Facade | Extracted responsibility | Boundary |
|---|---|---|
| `cli.py` | `cli_commands/parsers/` owns command registration; `cli_commands/handlers/binance.py` and `cli_commands/handlers/ashare.py` own provider workflows; `cli_parsing.py` and `cli_output.py` own pure helpers | `cli.py` remains the stable facade and compatibility surface; handlers resolve runtime dependencies through the facade so existing monkeypatch and import contracts remain valid |
| `adapters/binance/gateway.py` | `adapters/binance/spot_parsing.py` owns Spot wire parsing and scalar validation; `adapters/binance/spot_order_params.py` owns Spot order/OCO/OTO/OTOCO parameter validation and encoding | Pure protocol functions have no network, credential, or gateway state; gateway retains transport and compatibility wrappers |
| `trading/futures_oms.py` | `trading/futures_oms_codec.py` owns SQLite row codecs, JSON/Decimal conversion, timestamps, and event identities | No transactions or broker imports |
| `trading/oms.py` | `trading/oms_codec.py` owns broker-neutral SQLite row codecs, JSON/Decimal/time conversion, identifiers, and order status projection rules | No connections, transactions, or broker imports; `oms.py` remains the transaction facade |
| `storage/live_records.py` + `storage/live_record_codec.py` | Live observation ledger transactions and pure hash/JSON identifiers | `test_live_trade_records.py`, `test_live_trade_orchestration.py` |
| `storage/paper_day.py` | `storage/paper_day_codec.py` owns PAPER-day row decoding, event digests, identifier validation, and lease argument normalization | No connections, transactions, or mutable store state |
| `strategy_lab/exit_evaluator.py` | `strategy_lab/exit_serialization.py` owns deterministic documents; `strategy_lab/exit_simulation.py` owns daily replay, costs, slippage, metrics, and objective scoring | Pure codecs and simulation have no broker or storage access; evaluator facade retains experiment orchestration and compatibility helpers |
| `strategy_lab/experiments.py` | `strategy_lab/experiment_serialization.py` owns strategy/data manifests, trial folds, metrics, and holdout JSON plus SHA-256 serialization | Type-check-only model imports; no simulation, I/O, broker, storage, or promotion authority |
| `services/ashare_paper_day.py` | `services/ashare_paper_day_projection.py` owns LLM gate and DEEP exit audit/notification projections | Pure projections only; no storage, network, scheduler, or broker imports |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_serialization.py` owns K-line/technical-bar codecs, exit-barrier helpers, UTC normalization, canonical hashes, and event JSONL/file writes | Pure market/exit serialization and durable text primitives; the runner facade retains scheduling, state transitions, and side effects while re-exporting historical private names |
| `gui/integrations.py` | `gui/integration_validation.py` owns provider/model/token validation and safe error text | Pure configuration validation; Qt widgets, processes, and network probes remain in the GUI facade |
| `cli.py` | `cli_output.py` owns Decimal formatting and atomic JSON output | Pure output helpers; command dispatch remains in the CLI facade |
| `reporting/paper_day_summary.py` | `reporting/paper_day_codec.py` owns sidecar JSON/object and event-line decoding | Pure UTF-8/JSON decoding and scalar validation; summary facade retains historical private helper names and report semantics |
| `reporting/paper_day_summary.py` | `reporting/paper_day_formatting.py` owns stable-code/value formatting; `reporting/paper_day_renderer.py` owns deterministic Markdown rendering and audit sections | Pure formatting/rendering has no file, network, SQLite, or Qt dependency; summary facade retains sidecar loading and projection assembly |
| `reporting/paper_day_summary.py` | `reporting/paper_day_llm_projection.py` owns pure LLM sidecar data classes and projection statistics | Reads immutable sidecar event values only; summary facade retains file loading and full report assembly |
| `adapters/akshare_daily.py` | `adapters/akshare_daily_parsing.py` owns symbol/date normalization, frame column resolution, scalar validation, and `DailyBar` decoding | Pure payload parser; no AKShare client, network, timeout, or mutable adapter state |
| `adapters/market_data/cross_market.py` | `adapters/market_data/cross_market_payload.py` owns DataFrame/quote/date/number parsing and exact-universe validation | Pure payload functions; adapter facade retains provider calls, timeout, fallback, and degradation policy |
| `adapters/market_data/akshare_daily.py` | `adapters/market_data/akshare_daily_stitch.py` owns delayed-history tail stitching and overlap diagnostics | Pure bar-sequence policy; adapter retains provider calls, parsing, and fallback error routing |
| `features/close_analysis.py` | `features/close_analysis_indicators.py` owns pure indicator/score calculations | No storage, network, scheduler, or broker dependency; close-analysis facade retains data collection and report assembly |
| `services/ashare_close_analysis.py` | `services/ashare_close_models.py` owns request, market-data collection, and run result contracts; `services/ashare_close_projection.py` owns pure assessment/evidence projections; `services/ashare_close_notifications.py` owns report rendering and message splitting | The facade retains market-data/model orchestration and notification enqueue policy; extracted modules have no broker, network, storage, or scheduler dependency |
| `services/adversarial_macro.py` | `services/adversarial_macro_serialization.py` owns canonical request, identity, analysis documents, hashes, and scalar normalization | Pure audit encoding only; analyzer orchestration and provider calls stay in the service facade |
| `services/adversarial_macro.py` | `services/adversarial_macro_policy.py` owns role-output validation, peer envelopes, conservative aggregation, and round stability | Pure policy projection; analyzer facade retains provider calls, budgets, and shadow publication |
| `trading/futures_oms.py` | `trading/futures_oms_schema.py` owns Futures OMS SQLite DDL and indexes | Schema helper uses caller-owned connection; OMS retains transaction, lease, and state-transition logic |
| `services/ashare/ashare_intraday_paper.py` | `services/ashare/ashare_intraday_quantity.py` owns lot rules and sell-quantity planning | Pure quantity policy has no storage, network, scheduler, or broker dependency; the runner facade retains matching and ledger transitions |
| `services/ashare/ashare_intraday_paper.py` | `services/ashare/ashare_intraday_policy.py` owns immutable risk config, price acceptance, and board price bands | Pure execution policy; PAPER runner retains account, matching, and ledger transitions |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_config.py` owns frozen schedule/risk configuration and policy manifest projections | Pure configuration and policy documents; runner retains storage, scheduling, event, and notification side effects |
| `storage/live_records.py` | `storage/live_record_models.py` owns durable-row dataclasses and SQLite row-to-domain decoding | Pure row decoding; store retains transactions, leases, hash-chain writes, and state transitions |
| `storage/live_records.py` | `storage/live_record_schema.py` owns live-record DDL, append-only triggers, and idempotent `plan_stream_id` migration | Schema helper accepts the caller connection; store retains transaction orchestration and projection semantics |
| `storage/live_records.py` | `storage/live_record_work_policy.py` owns work-claim query construction and lease/failure argument normalization | Pure policy objects and parameterized SQL construction; store retains transaction, lease ownership, row projection, and state transitions |
| `trading/oms.py` | `trading/oms_schema.py` owns SQLite DDL and idempotent schema migration | Schema setup accepts the caller-owned connection; OMS retains transaction boundaries and order state transitions |
| `adapters/ashare/screening.py` | `adapters/ashare/screening_factors.py` owns historical-bar models and pure factor/corporate-action calculations | No provider, storage, or network dependency; screening facade retains universe/history fetching and degradation policy |
| `adapters/binance/gateway.py` | `adapters/binance/request_builder.py` owns deterministic REST query/form encoding and HMAC request assembly | No transport, clock, credentials lookup, or retry policy; gateway retains network and error handling |
| `adapters/binance/gateway.py` | `adapters/binance/errors.py` owns the shared Binance error hierarchy; `adapters/binance/rate_limit.py` owns response-header usage parsing | Errors and rate-limit parsing are transport-independent; gateway retains signing, clock, retries, HTTP calls, and compatibility exports |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_schedule.py` owns Shanghai session datetime, phase, and sleep calculations | Pure time calculations; runner retains calendar I/O, persistence, scheduling, and side effects |
| `services/ashare/ashare_intraday_llm.py` | `services/ashare/ashare_intraday_llm_serialization.py` owns safe audit documents, stable JSON normalization, and hashes | Pure audit encoding; LLM calls, timeouts, and plan gates remain in the service facade |
| `gui/integrations.py` | `gui/napcat_process.py` owns NapCat command validation and process lifecycle control | Qt process ownership stays in the lifecycle module; integration facade retains settings, tokens, and UI wiring |
| `cli_commands/handlers/binance.py` | `cli_commands/binance_results.py` owns balance validation, testnet deltas, and reconciliation JSON payloads | Pure result shaping; handlers retain network orchestration and LIVE guards |
| `cli.py` | `cli_commands/close_research_payloads.py` owns close-research archive/profile payload projections | Pure result shaping; CLI retains command dispatch, service calls, and artifact I/O |
| `adapters/binance/futures.py` | `adapters/binance/futures_order_params.py` owns Futures order/protection parameter validation and encoding | Pure protocol parameters; Futures gateway retains signing, transport, and response handling |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_notifications.py` owns stable notification kind, artifact identity, and report text projections | Pure event projection; runner retains outbox, notifier, and persistence side effects |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_llm_payloads.py` owns strict LLM audit document recovery, type validation, and gate/text projections | Pure immutable-payload decoding and projection; runner retains analyzer calls, storage, scheduling, and side effects |
| `cli.py` | `cli_commands/live_sync_payloads.py` owns live-sync status, ingest, protection, cycle, and receipt payload projections | Pure result shaping; CLI facade retains event ingestion, service orchestration, LIVE guards, and notification dispatch |
| `runtime/paper_account_chain.py` | `runtime/paper_account_manifest.py` owns pure lineage manifests, canonical JSON, account/seal hashes, source-prefix validation, and ledger projection compatibility | No filesystem, SQLite, lock, scheduler, or broker side effects; chain facade retains recovery and atomic persistence |

These modules are intentionally narrow. The old facade names remain available
so CLI entry points, services, and external integrations can migrate in later
increments without a flag-day change. The next planned slices are separation of the A-share PAPER/post-close workflows,
remaining sidecar loading, and execution orchestration helpers.

When adding a new slice, keep parser/codec/state-transition code pure where
possible, put provider protocol code under `adapters`, keep durable writes in
`storage` or `trading`, and add a focused test route before moving callers.

The repository-wide sequence and ownership targets are recorded in
[`modularization-roadmap.md`](modularization-roadmap.md).

Use the row matching the task, then follow its representative tests before
opening neighboring modules.

All representative tests in this table live under `tests/unit/`; the table
uses filename patterns to keep the routing map compact.

Local rules are deliberately limited to `src/gribuki_trade/runtime/`,
`storage/`, `trading/`, `strategy_lab/`, and `adapters/binance/`. Check the
nearest local `AGENTS.md` before changing one of those subtrees.

## Directory layout for provider and service code

The provider boundary is grouped by business role. `adapters/binance/` and
`adapters/schwab/` remain platform-specific protocol packages; A-share and
cross-market data live under `adapters/ashare/` and
`adapters/market_data/`, macro feeds under `adapters/macro/`, and broker-free
simulation under `adapters/simulated/`. The former flat adapter paths are
small compatibility aliases and contain no implementation logic.

Application services use the same grouping: `services/ashare/` owns A-share
research, close analysis, paper-day and paper matching workflows, while
`services/binance/` owns Binance monitoring, execution, paper and shadow
flows. The top-level service paths remain aliases so existing integrations
continue to resolve while new code can navigate by domain.

The CLI command tree is assembled in `src/gribuki_trade/cli_commands/parser.py`.
Each command family has a registration module under
`src/gribuki_trade/cli_commands/parsers/`; Binance execution, balance, stream,
backtest, shadow, and testnet OMS handlers live in
`src/gribuki_trade/cli_commands/handlers/binance.py` and
`src/gribuki_trade/cli_commands/handlers/ashare.py`. `cli.py` remains the stable
process entry point and compatibility facade.
