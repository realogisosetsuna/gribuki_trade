# Repository modularization roadmap

This repository uses compatibility facades while large files are split by
responsibility. The goal is to make the path from an external input to a
durable state change visible to a human reader without changing trading
authority or recovery semantics.

## Current ownership model

```text
domain / ports
    ↓
adapters + ingest → pipeline normalization
    ↓
services / policy / strategy
    ↓
trading / storage / outboxes
    ↓
runtime guards → PAPER / SHADOW / LIVE
```

Provider protocol code stays in `adapters`. Pure transformations stay in
small modules beside the facade that uses them. SQLite transaction boundaries,
leases, and monotonic transitions stay in `trading` or `storage`. Services
compose these pieces and own retry, reconciliation, and failure policy.

## Completed slices

| Original facade | New cohesive module | Responsibility |
|---|---|---|
| `cli.py` | `cli_commands/parsers/`, `cli_commands/handlers/binance.py`, `cli_commands/handlers/ashare.py`, `cli_commands/runtime.py`, `cli_parsing.py`, `cli_output.py` | Command-family registration, Binance and read-only A-share workflow handlers, shared runtime/default handling, argparse converters, Decimal formatting, and atomic JSON output |
| `adapters/binance/gateway.py` | `adapters/binance/spot_parsing.py`, `adapters/binance/spot_order_params.py` | Spot wire parsing, order snapshot/status mapping, scalar validation, signing/redaction, and pure Spot/OCO/OTO/OTOCO parameter encoding |
| `trading/futures_oms.py` | `trading/futures_oms_codec.py` | Futures SQLite codecs, JSON/Decimal/time conversion, event identity |
| `trading/futures_oms.py` | `trading/futures_oms_schema.py` | Futures OMS SQLite DDL and indexes with caller-owned transaction scope |
| `trading/futures_oms.py` | `trading/futures_oms_policy.py` | Pure order-status projection, protection-plan version policy, and restart-recovery query parameters |
| `trading/oms.py` | `trading/oms_codec.py` | Broker-neutral OMS SQLite row codecs, JSON/Decimal/time conversion, identifiers, and status projection |
| `trading/oms.py` | `trading/oms_position_policy.py` | Pure fill-to-position quantity, average-price, reversal, and realized-P&L projection |
| `trading/oms.py` | `trading/oms_command_policy.py` | Command scope normalization, lease validation, and UNKNOWN recovery projection |
| `storage/paper_day.py` | `storage/paper_day_codec.py` | PAPER-day row decoding, event digests, identifiers, and lease argument validation |
| `storage/live_records.py` | `storage/live_record_codec.py` | Live-record scalar validation, canonical JSON, event/protection/work identifiers, and event hashes |
| `strategy_lab/exit_evaluator.py` | `strategy_lab/exit_serialization.py`, `strategy_lab/exit_simulation.py`, `strategy_lab/exit_walk_forward.py` | Exit documents, pure daily replay/cost metrics, and trading-day walk-forward fold/index planning |
| `strategy_lab/experiments.py` | `strategy_lab/experiment_serialization.py` | Strategy/data manifests, trial folds, metrics and holdout JSON plus SHA-256 serialization |
| `services/ashare_paper_day.py` | `services/ashare_paper_day_projection.py` | LLM gate and DEEP exit audit/notification projections |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_serialization.py` | K-line/technical-bar codecs, exit-barrier/time helpers, UTC normalization, canonical hashes, and event JSONL/file primitives |
| `reporting/paper_day_summary.py` | `reporting/paper_day_codec.py` | Sidecar JSON and JSONL event decoding for reports |
| `reporting/paper_day_summary.py` | `reporting/paper_day_sidecar_codec.py` | Deterministic sidecar tuple/counter codecs and final-result identity filtering |
| `adapters/akshare_daily.py` | `adapters/akshare_daily_parsing.py` | Symbol/date normalization, frame parsing, and DailyBar validation |
| `adapters/market_data/cross_market.py` | `adapters/market_data/cross_market_payload.py` | DataFrame/quote/date/number parsing and exact-universe validation |
| `adapters/market_data/akshare_daily.py` | `adapters/market_data/akshare_daily_stitch.py` | Delayed-history tail stitching and overlap diagnostics |
| `adapters/market_data/akshare.py` | `adapters/market_data/akshare_payload.py` | Provider record decoding, symbol/number/time normalization, volume-unit conversion, JSONP/Eastmoney payload parsing, and required-column validation |
| `gui/integrations.py` | `gui/integration_validation.py` | Provider/model/token validation and safe UI error text |
| `features/close_analysis.py` | `features/close_analysis_indicators.py` | ATR/RSI/ADX, volatility, liquidity, trend, and score calculations |
| `services/ashare_close_analysis.py` | `services/ashare_close_models.py`, `services/ashare_close_projection.py`, `services/ashare_close_notifications.py` | Point-in-time request/result contracts, pure evidence/technical projections, and deterministic report rendering/message splitting |
| `services/adversarial_macro.py` | `services/adversarial_macro_serialization.py` | Canonical request/identity/analysis documents, hashes, and scalar normalization |
| `services/adversarial_macro.py` | `services/adversarial_macro_policy.py` | Role validation, peer envelopes, conservative aggregation, and round stability |
| `services/adversarial_macro.py` | `services/adversarial_macro_boundaries.py` | Role request construction, failure-safe ABSTAIN analysis, and sanitized failure-call documents |
| `ingest/search_discovery.py` | `ingest/search_discovery_policy.py` | Pure result sanitization, publisher identity, discovery clustering, and confirmation basis selection |
| `services/ashare/ashare_intraday_paper.py` | `services/ashare/ashare_intraday_quantity.py` | Pure lot/quantity rules and sell-quantity planning for A-share intraday PAPER execution |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_config.py` | Frozen schedule/risk configuration and policy manifest projections |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_risk.py` | Runtime risk-policy migration validation and price/quantity audit projections |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_llm_payloads.py` | Strict LLM audit payload recovery, type validation, and pure gate/text projections |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_documents.py` | Watchlist/candidate/order/fill document codecs, A-share symbol resolution, and strict positive-integer validation |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_risk.py` | Pure risk-policy migration authorization, incomplete-fill checks, and price/quantity execution-policy documents |
| `services/binance/binance_execution.py` | `services/binance/binance_execution_records.py` | Pure snapshot/fill/balance/order-list record projections and timestamp normalization |
| `services/binance/binance_execution.py` | `services/binance/binance_execution_policy.py` | Environment, clock, order allow-list, and exchange-snapshot merge policy |
| `services/ashare/ashare_intraday_llm.py` | `services/ashare/ashare_intraday_llm_serialization.py` | Safe audit documents, stable JSON normalization, and hashes |
| `services/ashare/ashare_intraday_llm.py` | `services/ashare/ashare_intraday_llm_policy.py` | Pure context/review identity, scalar validation, score bounds, and UTC normalization |
| `services/ashare/ashare_intraday_paper.py` | `services/ashare/ashare_intraday_policy.py` | Risk config, price acceptance, board price bands, and validation |
| `reporting/paper_day_summary.py` | `reporting/paper_day_llm_projection.py` | LLM sidecar data classes and pure projection statistics |
| `reporting/paper_day_summary.py` | `reporting/paper_day_projection_models.py`, `reporting/paper_day_account_projection.py` | Immutable sidecar models and pure account/order/fill/notification projections |
| `reporting/paper_day_summary.py` | `reporting/paper_day_summary_models.py` | Immutable executive, risk, price/quantity, execution, watchlist, and source-state summary models |
| `reporting/paper_day_summary.py` | `reporting/paper_day_execution_projection.py` | Pure watchlist, risk-policy, price/quantity, execution-acceptance, stable-reason, and source-transition event projections |
| `adapters/market_data/akshare_daily.py` | `adapters/market_data/akshare_daily_router.py` | Provider protocol, fallback routing, source diagnostics, and controlled tail-stitch coordination |
| `adapters/binance/user_stream.py` | `adapters/binance/user_stream_parsing.py` | Spot user-data event models, signature encoding, strict frame/event parsing, and field validation |
| `services/ashare/ashare_intraday_llm.py` | `services/ashare/ashare_intraday_llm_models.py` | Immutable configuration, PIT review context, journal acceptance, schedule, and gate outcome models |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_events.py` | Event store protocol, publisher, sidecar heartbeat/recovery, and notification outbox reconciliation |
| `cli_commands/handlers/binance.py` | `cli_commands/binance_results.py` | Balance validation, testnet deltas, and reconciliation payload shaping |
| `cli_commands/handlers/binance.py` | `cli_commands/handlers/binance_live.py` | LIVE Spot/Futures guards, account queries, order tests, execution, and private stream orchestration |
| `cli.py` | `cli_commands/close_research_payloads.py` | Close-research archive/profile payload projections |
| `adapters/binance/futures.py` | `adapters/binance/futures_order_params.py` | Futures order/protection parameter validation and encoding |
| `adapters/binance/futures.py` | `adapters/binance/futures_parsing.py` | Futures Ticker and local-order-book response decoding, plus symbol/enum/listen-key validation |
| `adapters/binance/gateway.py` | `adapters/binance/errors.py`, `adapters/binance/rate_limit.py` | Shared error hierarchy and pure rate-limit response-header parsing |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_notifications.py` | Stable notification kind, artifact identity, and report text projections |
| `gui/integrations.py` | `gui/napcat_process.py` | NapCat launch-command validation and owned process lifecycle |
| `reporting/paper_day_summary.py` | `reporting/paper_day_renderer.py` | Pure Markdown rendering and audit sections from immutable projections |
| `storage/live_records.py` | `storage/live_record_models.py` | Durable-row dataclasses and pure SQLite row-to-domain decoding |
| `storage/live_records.py` | `storage/live_record_schema.py` | Live-record DDL, append-only triggers, and idempotent schema migration |
| `trading/oms.py` | `trading/oms_schema.py` | SQLite DDL and idempotent schema migration, with connection ownership left to the OMS facade |
| `adapters/ashare/screening.py` | `adapters/ashare/screening_factors.py` | Historical-bar model, raw factor calculations, and corporate-action guards |
| `adapters/ashare/screening.py` | `adapters/ashare/screening_payload.py` | Provider row decoding, validation, coverage checks, and deterministic revision hashes |
| `adapters/ashare/context.py` | `adapters/ashare/context_parsing.py` | Provider field resolution, ETF/security symbol normalization, scalar/date parsing, and source metadata mappings |
| `adapters/binance/gateway.py` | `adapters/binance/request_builder.py` | Deterministic REST query/form encoding and signed request assembly |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_schedule.py` | Session timezone, phase boundaries, and scheduler sleep calculations |
| `cli.py` | `cli_commands/live_sync_payloads.py` | Pure live-sync status, ingest, protection, cycle, and receipt result documents |
| `cli_commands/parsers/*.py` | `cli_commands/parsers/ashare_common.py` | Shared A-share news-feed choices and optional notification-target argument registration |
| `storage/live_records.py` | `storage/live_record_work_policy.py` | Work-claim SQL construction and lease/failure parameter normalization |
| `storage/live_records.py` | `storage/live_record_protection_policy.py` | T+1 sellable-quantity and FIFO protection-lot allocation projections |
| `storage/live_records.py` | `storage/live_record_integrity.py`, `storage/live_record_errors.py` | Legacy JSON recovery validation, append-only event hash-chain verification, and shared durable-store error types |

## Next slices

The next large files are grouped by the responsibilities they mix:

| Area | Large files | Extraction order |
|---|---|---|
| A-share execution | `services/ashare_paper_day.py`, remaining `services/ashare_intraday_paper.py` orchestration | projections/configuration → calendar/session logic → orchestration |
| Durable state | `storage/live_records.py` | row models/schema/codecs, T+1/FIFO protection policy, and lease policy are split; transaction methods remain |
| Broker-neutral execution | `trading/oms.py` | schema/migration is split; command/outbox/fill transaction methods remain |
| A-share screening | `adapters/ashare/screening.py` | pure factor calculations are split; provider calls and degradation policy remain |
| Research | remaining `services/adversarial_macro.py` orchestration and other strategy/research facades | pure calculations → dataset/manifest IO → orchestration |
| Presentation | remaining `reporting/paper_day_summary.py`, `gui/integrations.py` | sidecar loading/projection assembly → provider boundary → UI wiring |
| CLI | `cli.py` | remaining A-share workflow handlers and orchestration → compatibility migration |

The original import path remains a facade until all in-repository callers have
migrated. The broker-neutral OMS slice now has
`trading/oms_codec.py`; it contains no SQLite connections or transaction code, and
historical private helper names remain available through aliases in `oms.py`. Every slice requires a focused test and the repository quality gates
before it is committed.

## Evidence

Current source evidence includes `src/gribuki_trade/cli_parsing.py`,
`src/gribuki_trade/cli_output.py`,
`src/gribuki_trade/gui/integration_validation.py`,
`src/gribuki_trade/adapters/binance/spot_parsing.py`,
`src/gribuki_trade/adapters/binance/spot_order_params.py`,
`src/gribuki_trade/adapters/market_data/cross_market_payload.py`,
`src/gribuki_trade/trading/futures_oms_codec.py`,
`src/gribuki_trade/trading/oms_codec.py`,
`src/gribuki_trade/services/ashare_paper_day_projection.py`,
`src/gribuki_trade/services/ashare_close_models.py`,
`src/gribuki_trade/services/ashare_close_projection.py`,
`src/gribuki_trade/services/ashare_close_notifications.py`,
`src/gribuki_trade/storage/paper_day_codec.py`, and
`src/gribuki_trade/strategy_lab/exit_serialization.py`,
`src/gribuki_trade/strategy_lab/exit_simulation.py`,
`src/gribuki_trade/strategy_lab/exit_walk_forward.py`,
`src/gribuki_trade/services/adversarial_macro_serialization.py`,
`src/gribuki_trade/reporting/paper_day_renderer.py`,
`src/gribuki_trade/services/ashare/ashare_intraday_quantity.py`,
`src/gribuki_trade/services/ashare/ashare_paper_day_config.py`,
`src/gribuki_trade/services/ashare/ashare_paper_day_llm_payloads.py`,
`src/gribuki_trade/adapters/binance/errors.py`,
`src/gribuki_trade/adapters/binance/rate_limit.py`,
`src/gribuki_trade/storage/live_record_work_policy.py`,
`src/gribuki_trade/services/binance/binance_execution_records.py`,
`src/gribuki_trade/adapters/ashare/screening_payload.py`,
`src/gribuki_trade/services/adversarial_macro_boundaries.py`,
`src/gribuki_trade/features/close_analysis_indicators.py`, and
`src/gribuki_trade/storage/live_record_models.py`,
`src/gribuki_trade/trading/oms_schema.py`,
`src/gribuki_trade/adapters/ashare/screening_factors.py`, and
`src/gribuki_trade/strategy_lab/experiment_serialization.py`. Focused verification is
covered by `tests/unit/test_cli_parsing.py`,
`tests/unit/test_cli_output.py`, `tests/unit/test_binance_spot_parsing.py`,
`tests/unit/test_futures_oms_codec.py`,
`tests/unit/test_trading_oms_codec.py`,
`tests/unit/test_ashare_paper_day_projection.py`,
`tests/unit/test_ashare_close_components.py`,
`tests/unit/test_paper_day_store.py`, and
`tests/unit/test_strategy_lab_exit_serialization.py`, with strategy experiment
serialization coverage in `tests/unit/test_strategy_lab_experiment_serialization.py`
and `tests/unit/test_strategy_lab_experiments.py`.

## Directory grouping completed in this increment

Provider implementations are now organized under
`src/gribuki_trade/adapters/ashare/`, `market_data/`, `macro/`, and
`simulated/`; A-share and Binance application services are under
`src/gribuki_trade/services/ashare/` and `services/binance/`. Historical flat
module paths are compatibility aliases that point at the implementation module,
so private monkeypatch and import behavior used by existing integrations stays
stable. The CLI parser is split into eleven command-family modules under
`src/gribuki_trade/cli_commands/parsers/`; Binance workflows and read-only A-share
market/research handlers are extracted to `cli_commands/handlers/`. Shared
integration defaults, terminal encoding, local-secret access, SQLite diagnostics,
and temp-root operations are isolated in `cli_commands/runtime.py`; this module
lazy-loads the facade so it can be imported independently without changing
monkeypatch hooks.

Representative routing tests include `tests/unit/test_akshare_market_data.py`,
`tests/unit/test_ashare_paper_day.py`, `tests/unit/test_binance_execution.py`,
and `tests/unit/test_cli.py`. The remaining oversized orchestration facades
(`ashare_paper_day.py`, `storage/live_records.py`, the remaining sidecar loading in
`reporting/paper_day_summary.py`, and the A-share workflow branches in `cli.py`)
are the next vertical slices; each must first
extract pure projections or codecs before moving transaction and dispatch
logic.

The Futures adapter parsing slice now places deterministic Ticker and
local-order-book decoding in `adapters/binance/futures_parsing.py`. It accepts
the documented USD-M object and COIN-M single-element-array Ticker forms,
preserves Decimal precision and snapshot sequence identity, and centralizes
symbol, enum, and listen-key validation. The `futures.py` facade retains signed
HTTP, credential handling, runtime authority, and order-changing operations.
The focused parser, order-parameter, and order-book route passed 28 tests; the
known Windows pytest-cache permission warning remains environment-only.

The CLI A-share handler slice now moves source-health and watchlist read-only
commands, along with close-screening and intraday-surveillance result projections,
into `cli_commands/handlers/ashare.py`. The facade still owns command dispatch,
provider orchestration, candidate/research persistence, and compatibility exports;
the moved handlers resolve clock/path hooks through the facade where tests and
embedded callers historically replaced them.

The CLI notification slice now places NapCat/OneBot configuration, health checks,
finite outbox polling, explicit test messages, and durable Markdown artifact
delivery in `cli_commands/handlers/napcat.py`. The handler is directly
importable and resolves secret/configuration hooks through a lazy CLI facade, so
the historical `gribuki_trade.cli` functions and monkeypatch points remain
compatible while command dispatch stays in the facade.
