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
| `cli.py` | `cli_commands/runtime.py` owns integration-setting default application, terminal encoding, secret access/status, SQLite diagnostics, and temp-root operations | Runtime helpers are independently importable and resolve replaceable dependencies through the facade; `cli.py` retains historical private names and dispatch behavior |
| `adapters/binance/gateway.py` | `adapters/binance/spot_parsing.py` owns Spot wire parsing, order snapshots/status mapping, and scalar validation; `adapters/binance/spot_order_params.py` owns Spot order/OCO/OTO/OTOCO parameter validation and encoding | Pure protocol functions have no network, credential, or gateway state; gateway retains transport and compatibility wrappers |
| `trading/futures_oms.py` | `trading/futures_oms_codec.py` owns SQLite row codecs, JSON/Decimal conversion, timestamps, and event identities | No transactions or broker imports |
| `trading/oms.py` | `trading/oms_codec.py` owns broker-neutral SQLite row codecs, JSON/Decimal/time conversion, identifiers, and order status projection rules | No connections, transactions, or broker imports; `oms.py` remains the transaction facade |
| `storage/live_records.py` + `storage/live_record_codec.py` | Live observation ledger transactions and pure hash/JSON identifiers | `test_live_trade_records.py`, `test_live_trade_orchestration.py` |
| `storage/live_records.py` | `storage/live_record_confirmation_policy.py` owns two-phase live-fill command, sender, and fingerprint identity checks | Pure confirmation validation has no SQLite access; the store retains the confirmation transaction and all durable state transitions |
| `storage/paper_day.py` | `storage/paper_day_codec.py` owns PAPER-day row decoding, event digests, identifier validation, and lease argument normalization | No connections, transactions, or mutable store state |
| `strategy_lab/exit_evaluator.py` | `strategy_lab/exit_serialization.py` owns deterministic documents; `strategy_lab/exit_simulation.py` owns daily replay, costs, slippage, metrics, and objective scoring; `strategy_lab/exit_walk_forward.py` owns trading-day fold construction and episode-index mapping | Pure codecs, simulation, and walk-forward planning have no broker or storage access; evaluator facade retains experiment orchestration and compatibility helpers |
| `strategy_lab/exit_evaluator.py` | `strategy_lab/exit_models.py` owns frozen bars/episodes/datasets, costs, outcomes, metrics, and trial registry value objects | Model validation and research-only registry invariants are independent of replay and fold orchestration; the evaluator facade re-exports historical identities |
| `strategy_lab/ashare_evaluator.py` | `strategy_lab/ashare_evaluator_models.py` owns A-share execution/action enums, PIT scores, completed bars, observations, evaluator configuration, and result records | Model validation is independent of matching and performance calculations; the evaluator facade retains simulation and serialization helpers while re-exporting historical identities |
| `strategy_lab/experiments.py` | `strategy_lab/experiment_serialization.py` owns strategy/data manifests, trial folds, metrics, and holdout JSON plus SHA-256 serialization | Type-check-only model imports; no simulation, I/O, broker, storage, or promotion authority |
| `strategy_lab/experiments.py` | `strategy_lab/experiment_models.py` owns immutable manifests, walk-forward configuration/folds, weight constraints, costs, metrics, evaluation records, and research experiment results | Models are research-only and broker-independent; the facade retains fold generation, candidate enumeration, and experiment execution |
| `services/ashare_paper_day.py` | `services/ashare_paper_day_projection.py` owns LLM gate and DEEP exit audit/notification projections | Pure projections only; no storage, network, scheduler, or broker imports |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_serialization.py` owns K-line/technical-bar codecs, exit-barrier helpers, UTC normalization, canonical hashes, and event JSONL/file writes | Pure market/exit serialization and durable text primitives; the runner facade retains scheduling, state transitions, and side effects while re-exporting historical private names |
| `gui/integrations.py` | `gui/integration_validation.py` owns provider/model/token validation and safe error text | Pure configuration validation; Qt widgets, processes, and network probes remain in the GUI facade |
| `cli.py` | `cli_output.py` owns Decimal formatting and atomic JSON output | Pure output helpers; command dispatch remains in the CLI facade |
| `cli.py` | `cli_commands/handlers/napcat.py` owns NapCat configuration, status, finite outbox dispatch, health-test messages, and durable Markdown artifact delivery | The handler uses a lazy CLI facade for secret/configuration hooks, so historical imports and monkeypatch points remain stable; notifier and receipt-store boundaries stay inside the handler |
| `services/ashare/ashare_close_notifications.py` | `services/ashare/ashare_close_notification_splitting.py` owns pure contractual report splitting, payload chunking, and dual-track summary lines | The splitter has no storage, network, notifier, or service state; the notification facade retains historical private helper aliases and report assembly |
| `services/live_trade_orchestration.py` | `services/live_trade_orchestration_models.py` owns frozen protection inputs, input-provider protocol, work-run summaries, tracking observations, and stable input errors | Models have no storage, notifier, or broker access; the orchestration facade retains work claiming, exit lifecycle calls, and outbox delivery |
| `services/exit_plan_lifecycle.py` | `services/exit_plan_lifecycle_models.py` owns lifecycle errors, append-only event-store protocol, QUICK/DEEP application results, and barrier observations | Contracts and results have no storage implementation or broker access; the lifecycle facade retains event replay, idempotent append, and exit-plan orchestration |
| `services/macro_research.py` | `services/macro_evidence_selection.py` owns point-in-time evidence filtering, relevance ranking, injection rejection, publisher identity, and corroboration policy | Selection is pure apart from input event values; the macro facade retains request hashing, analyzer calls, and fail-closed execution |
| `cli.py` | `cli_commands/handlers/ashare_news.py` owns one-shot A-share news, continuous news polling, and disclosure-index ingestion | Provider collection and source-health persistence stay in the handler; the CLI facade keeps historical function names and dispatch semantics through lazy runtime hooks |
| `reporting/paper_day_summary.py` | `reporting/paper_day_codec.py` owns sidecar JSON/object and event-line decoding | Pure UTF-8/JSON decoding and scalar validation; summary facade retains historical private helper names and report semantics |
| `reporting/paper_day_summary.py` | `reporting/paper_day_sidecar_codec.py` owns deterministic string/integer tuple, counter, and final-result sidecar codecs | Small pure value codecs and result identity checks stay independent of projection assembly and report file writes |
| `reporting/paper_day_summary.py` | `reporting/paper_day_formatting.py` owns stable-code/value formatting; `reporting/paper_day_renderer.py` owns deterministic Markdown rendering and audit sections | Pure formatting/rendering has no file, network, SQLite, or Qt dependency; summary facade retains sidecar loading and projection assembly |
| `reporting/paper_day_summary.py` | `reporting/paper_day_llm_projection.py` owns pure LLM sidecar data classes and projection statistics | Reads immutable sidecar event values only; summary facade retains file loading and full report assembly |
| `reporting/paper_day_summary.py` | `reporting/paper_day_projection_models.py` owns immutable sidecar/projection value objects; `reporting/paper_day_account_projection.py` owns account, fill, order, and notification event projections | Pure sidecar projections; summary facade retains file loading, report assembly, and rendering |
| `reporting/paper_day_summary.py` | `reporting/paper_day_summary_models.py` owns executive, watchlist, source-state, risk-policy, price/quantity, and execution summary data classes | Immutable model boundary has no file, SQLite, or projection side effects; the summary facade re-exports the same type identities for existing callers |
| `reporting/paper_day_summary.py` | `reporting/paper_day_execution_projection.py` owns pure watchlist, risk-policy, price/quantity, order-acceptance, stable-reason, and source-transition projections | Event interpretation is isolated from sidecar loading and Markdown rendering; the facade keeps historical private helper names as delegating aliases |
| `adapters/market_data/akshare_daily.py` | `adapters/market_data/akshare_daily_router.py` owns provider protocols, primary/fallback selection, sync/async diagnostics, and controlled tail-stitch routing | Router coordinates normalized `DailyBar` providers only; the AKShare facade retains endpoint calls, timeout handling, row parsing, and public compatibility exports |
| `adapters/binance/user_stream.py` | `adapters/binance/user_stream_parsing.py` owns Spot user-data event models, signature payload encoding, envelope/frame decoding, and strict event-field validation | Parsing is pure and broker-connection-free; `user_stream.py` retains WebSocket authentication, reconnect, buffering, rotation, and compatibility re-exports |
| `services/ashare/ashare_intraday_llm.py` | `services/ashare/ashare_intraday_llm_models.py` owns immutable LLM config, point-in-time context/review, journal acceptance, schedule, and gate result models | Model validation is independent of background coordination and network calls; the coordinator facade re-exports historical types and document helpers |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_events.py` owns the narrow event store protocol, event publication, sidecar recovery/heartbeat, and notification outbox reconciliation | Journal writes remain authoritative and sidecar failures remain nonfatal; the runner retains trading-day orchestration and historical publisher/store exports |
| `adapters/akshare_daily.py` | `adapters/akshare_daily_parsing.py` owns symbol/date normalization, frame column resolution, scalar validation, and `DailyBar` decoding | Pure payload parser; no AKShare client, network, timeout, or mutable adapter state |
| `adapters/market_data/cross_market.py` | `adapters/market_data/cross_market_payload.py` owns DataFrame/quote/date/number parsing and exact-universe validation | Pure payload functions; adapter facade retains provider calls, timeout, fallback, and degradation policy |
| `adapters/market_data/akshare_daily.py` | `adapters/market_data/akshare_daily_stitch.py` owns delayed-history tail stitching and overlap diagnostics | Pure bar-sequence policy; adapter retains provider calls, parsing, and fallback error routing |
| `adapters/market_data/akshare.py` | `adapters/market_data/akshare_payload.py` owns record decoding, symbol/number/time normalization, volume-unit conversion, required-column checks, and provider payload exception types | Pure payload boundary; the adapter retains network requests, retry/cache policy, and market-domain projection while re-exporting historical exception/helper names |
| `features/close_analysis.py` | `features/close_analysis_indicators.py` owns pure indicator/score calculations | No storage, network, scheduler, or broker dependency; close-analysis facade retains data collection and report assembly |
| `features/close_analysis.py` | `features/close_analysis_models.py` owns close-analysis configuration, signal-family, horizon, and technical-assessment value objects | Immutable model validation is independent of indicator calculations and report orchestration; the facade re-exports historical identities |
| `features/cross_market_relations.py` | `features/cross_market_models.py` owns cross-market risk enums, PIT observations, relation metrics, reports, and internal pair records | Models are pure value objects; the relation facade retains input validation, Pearson/EWMA/OLS calculations, and failure projection |
| `services/ashare_close_analysis.py` | `services/ashare_close_models.py` owns request, market-data collection, and run result contracts; `services/ashare_close_projection.py` owns pure assessment/evidence projections; `services/ashare_close_notifications.py` owns report rendering and message splitting | The facade retains market-data/model orchestration and notification enqueue policy; extracted modules have no broker, network, storage, or scheduler dependency |
| `services/adversarial_macro.py` | `services/adversarial_macro_serialization.py` owns canonical request, identity, analysis documents, hashes, and scalar normalization | Pure audit encoding only; analyzer orchestration and provider calls stay in the service facade |
| `services/adversarial_macro.py` | `services/adversarial_macro_policy.py` owns role-output validation, peer envelopes, conservative aggregation, and round stability | Pure policy projection; analyzer facade retains provider calls, budgets, and shadow publication |
| `services/adversarial_macro.py` | `services/adversarial_macro_boundaries.py` owns role-request construction, failure-safe ABSTAIN analysis, and sanitized failure-call documents | Pure protocol boundary; analyzer facade retains concurrency, provider calls, budgets, and aggregation |
| `services/adversarial_macro.py` | `services/adversarial_macro_models.py` owns adversarial roles, immutable configuration, round/opinion/run records, and audit value objects | Model validation and audit projection are independent of provider calls and round orchestration; the facade re-exports historical identities |
| `ingest/search_discovery.py` | `ingest/search_discovery_policy.py` owns safe-hit normalization, official-host/publisher identity, and deterministic discovery clustering | Pure search-result policy; provider HTTP calls, backoff, event creation, and source orchestration remain in the facade |
| `trading/futures_oms.py` | `trading/futures_oms_schema.py` owns Futures OMS SQLite DDL and indexes | Schema helper uses caller-owned connection; OMS retains transaction, lease, and state-transition logic |
| `trading/futures_oms.py` | `trading/futures_oms_policy.py` owns pure order-status monotonicity, protection-plan revision checks, and restart-recovery query construction | Pure policy has no database writes or broker calls; the facade retains transactions and historical `_STATUS_RANK`/`_should_apply` names |
| `services/ashare/ashare_intraday_paper.py` | `services/ashare/ashare_intraday_quantity.py` owns lot rules and sell-quantity planning | Pure quantity policy has no storage, network, scheduler, or broker dependency; the runner facade retains matching and ledger transitions |
| `services/ashare/ashare_intraday_paper.py` | `services/ashare/ashare_intraday_policy.py` owns immutable risk config, price acceptance, and board price bands | Pure execution policy; PAPER runner retains account, matching, and ledger transitions |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_config.py` owns frozen schedule/risk configuration and policy manifest projections | Pure configuration and policy documents; runner retains storage, scheduling, event, and notification side effects |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_risk.py` owns runtime risk-policy migration validation, price/quantity audit projections, and sell-plan explanations | Pure risk transition and document policy; runner retains event writes, recovery, scheduling, and transaction side effects |
| `storage/live_records.py` | `storage/live_record_models.py` owns durable-row dataclasses and SQLite row-to-domain decoding | Pure row decoding; store retains transactions, leases, hash-chain writes, and state transitions |
| `storage/live_records.py` | `storage/live_record_schema.py` owns live-record DDL, append-only triggers, and idempotent `plan_stream_id` migration | Schema helper accepts the caller connection; store retains transaction orchestration and projection semantics |
| `storage/live_records.py` | `storage/live_record_work_policy.py` owns work-claim query construction and lease/failure argument normalization | Pure policy objects and parameterized SQL construction; store retains transaction, lease ownership, row projection, and state transitions |
| `storage/live_records.py` | `storage/live_record_protection_policy.py` owns A-share T+1 sellable-quantity and FIFO buy-lot allocation projections | Pure quantity policy has no SQLite access; store retains lot queries, allocation writes, and protection-work fencing updates |
| `storage/live_records.py` | `storage/live_record_integrity.py` owns legacy JSON object validation and append-only event hash-chain verification; `storage/live_record_errors.py` owns shared store error types | Pure integrity functions have no SQLite access; `live_records.py` retains migration, transaction, lease, and compatibility exports |
| `trading/oms.py` | `trading/oms_schema.py` owns SQLite DDL and idempotent schema migration | Schema setup accepts the caller-owned connection; OMS retains transaction boundaries and order state transitions |
| `trading/oms.py` | `trading/oms_position_policy.py` owns pure fill-to-position projection, including partial close and reversal math | No SQLite or broker access; OMS retains row reads, transaction scope, and durable writes |
| `trading/oms.py` | `trading/oms_command_policy.py` owns command scope normalization, lease/claim validation, and UNKNOWN recovery projections | Pure command policy; OMS retains SQLite transactions, outbox leases, order events, and durable recovery writes |
| `adapters/ashare/screening.py` | `adapters/ashare/screening_factors.py` owns historical-bar models and pure factor/corporate-action calculations | No provider, storage, or network dependency; screening facade retains universe/history fetching and degradation policy |
| `adapters/ashare/screening.py` | `adapters/ashare/screening_payload.py` owns provider row decoding, symbol/date/number validation, coverage checks, and revision hashes | Pure provider payload boundary; screening facade retains AKShare calls, timeout, concurrency, fallback, and degradation policy |
| `adapters/ashare/context.py` | `adapters/ashare/context_parsing.py` owns provider field resolution, ETF/security symbol normalization, scalar/date parsing, and source metadata mappings | Pure parsing boundary; context facade retains AKShare calls, timeout/thread orchestration, and degradation/missing-source projection |
| `adapters/binance/gateway.py` | `adapters/binance/request_builder.py` owns deterministic REST query/form encoding and HMAC request assembly | No transport, clock, credentials lookup, or retry policy; gateway retains network and error handling |
| `adapters/binance/gateway.py` | `adapters/binance/errors.py` owns the shared Binance error hierarchy; `adapters/binance/rate_limit.py` owns response-header usage parsing | Errors and rate-limit parsing are transport-independent; gateway retains signing, clock, retries, HTTP calls, and compatibility exports |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_schedule.py` owns Shanghai session datetime, phase, and sleep calculations | Pure time calculations; runner retains calendar I/O, persistence, scheduling, and side effects |
| `services/ashare/ashare_intraday_llm.py` | `services/ashare/ashare_intraday_llm_serialization.py` owns safe audit documents, stable JSON normalization, and hashes | Pure audit encoding; LLM calls, timeouts, and plan gates remain in the service facade |
| `services/ashare/ashare_intraday_llm.py` | `services/ashare/ashare_intraday_llm_policy.py` owns stable context/review identities and scalar/time validation | Pure policy has no analyzer, scheduler, storage, or broker access; the facade retains review lifecycle and gate orchestration |
| `gui/integrations.py` | `gui/napcat_process.py` owns NapCat command validation and process lifecycle control | Qt process ownership stays in the lifecycle module; integration facade retains settings, tokens, and UI wiring |
| `cli_commands/handlers/binance.py` | `cli_commands/binance_results.py` owns balance validation, testnet deltas, and reconciliation JSON payloads | Pure result shaping; handlers retain network orchestration and LIVE guards |
| `cli_commands/handlers/binance.py` | `cli_commands/handlers/binance_live.py` owns LIVE guard/service construction, Spot LIVE and USDⓈ-M Futures LIVE status, balance, order-test, submit/cancel, and private-stream workflows | LIVE facade re-exports the historical handler names; both modules use a lazy CLI facade so direct imports avoid cycles while safety guards and test seams remain centralized |
| `cli.py` | `cli_commands/close_research_payloads.py` owns close-research archive/profile payload projections | Pure result shaping; CLI retains command dispatch, service calls, and artifact I/O |
| `adapters/binance/futures.py` | `adapters/binance/futures_order_params.py` owns Futures order/protection parameter validation and encoding | Pure protocol parameters; Futures gateway retains signing, transport, and response handling |
| `adapters/binance/futures.py` | `adapters/binance/futures_parsing.py` owns Futures Ticker/order-book response decoding plus symbol, enum, and listen-key validation | Pure response boundary; Futures gateway retains HTTP signing, credentials, transport, and LIVE authority |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_notifications.py` owns stable notification kind, artifact identity, and report text projections | Pure event projection; runner retains outbox, notifier, and persistence side effects |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_llm_payloads.py` owns strict LLM audit document recovery, type validation, and gate/text projections | Pure immutable-payload decoding and projection; runner retains analyzer calls, storage, scheduling, and side effects |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_documents.py` owns watchlist/candidate/order/fill document codecs and symbol/quantity validation | Pure in-memory/document conversion; runner retains recovery orchestration, scheduling, ledger transactions, and notification side effects |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_risk.py` owns exact risk-policy migration authorization, incomplete-fill/pending-order checks, and price/quantity policy documents | Pure policy and codecs read immutable events/configuration only; runner retains event writes, transactions, scheduling, and side effects |
| `services/binance/binance_execution.py` | `services/binance/binance_execution_records.py` owns pure Binance snapshot, fill, balance, timestamp, and order-list projections | Pure record conversion; execution facade retains OMS transactions, broker calls, user-stream processing, reconciliation, and runtime guards |
| `services/binance/binance_execution.py` | `services/binance/binance_execution_models.py` owns execution gateway and private-stream protocols, testnet guard error, and startup reconciliation result | These contracts have no network or persistence state; the facade retains OMS transactions, broker calls, user-stream processing, reconciliation, and runtime guards |
| `services/binance/binance_execution.py` | `services/binance/binance_execution_policy.py` owns environment, clock, order allow-list, and exchange-snapshot merge policies | Pure execution safety policy; execution facade retains broker calls, OMS transactions, user-stream processing, reconciliation, and runtime guards |
| `cli.py` | `cli_commands/live_sync_payloads.py` owns live-sync status, ingest, protection, cycle, and receipt payload projections | Pure result shaping; CLI facade retains event ingestion, service orchestration, LIVE guards, and notification dispatch |
| `cli_commands/parsers/*.py` | `cli_commands/parsers/ashare_common.py` owns shared A-share news-feed choices and optional notification-target argument registration | Pure argparse registration; command-family modules retain command-specific defaults, execution dispatch, and runtime validation |
| `cli.py` | `cli_commands/handlers/ashare.py` owns A-share source-health/watchlist read-only handlers and screening/intraday result projections | Read-only adapter/store reads and pure result shaping stay behind the handler boundary; `cli.py` retains command dispatch, provider orchestration, persistence, and compatibility exports |
| `runtime/paper_account_chain.py` | `runtime/paper_account_manifest.py` owns pure lineage manifests, canonical JSON, account/seal hashes, source-prefix validation, and ledger projection compatibility | No filesystem, SQLite, lock, scheduler, or broker side effects; chain facade retains recovery and atomic persistence |
| `storage/paper_orders.py` | `storage/paper_orders_codec.py` owns immutable paper-order event/run models, SQLite row decoding, canonical JSON, hash-chain verification, and scalar normalization | Codec has no connections or transactions; the store facade retains schema, WAL, leases, idempotent appends, and recovery |

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
