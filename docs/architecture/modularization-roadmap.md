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
| `cli.py` | `cli_parsing.py`, `cli_output.py` | Argparse converters, Decimal formatting, and atomic JSON output |
| `adapters/binance/gateway.py` | `adapters/binance/spot_parsing.py` | Spot wire parsing, scalar validation, signing, and redaction |
| `trading/futures_oms.py` | `trading/futures_oms_codec.py` | Futures SQLite codecs, JSON/Decimal/time conversion, event identity |
| `trading/oms.py` | `trading/oms_codec.py` | Broker-neutral OMS SQLite row codecs, JSON/Decimal/time conversion, identifiers, and status projection |
| `storage/paper_day.py` | `storage/paper_day_codec.py` | PAPER-day row decoding, event digests, identifiers, and lease argument validation |
| `strategy_lab/exit_evaluator.py` | `strategy_lab/exit_serialization.py` | Exit dataset, plan, outcome, registry JSON and SHA-256 serialization |
| `strategy_lab/experiments.py` | `strategy_lab/experiment_serialization.py` | Strategy/data manifests, trial folds, metrics and holdout JSON plus SHA-256 serialization |
| `services/ashare_paper_day.py` | `services/ashare_paper_day_projection.py` | LLM gate and DEEP exit audit/notification projections |
| `services/ashare/ashare_paper_day.py` | `services/ashare/ashare_paper_day_serialization.py` | K-line/technical-bar codecs, exit-barrier/time helpers, UTC normalization, canonical hashes, and event JSONL/file primitives |
| `reporting/paper_day_summary.py` | `reporting/paper_day_codec.py` | Sidecar JSON and JSONL event decoding for reports |
| `adapters/akshare_daily.py` | `adapters/akshare_daily_parsing.py` | Symbol/date normalization, frame parsing, and DailyBar validation |
| `gui/integrations.py` | `gui/integration_validation.py` | Provider/model/token validation and safe UI error text |
| `services/ashare_close_analysis.py` | `services/ashare_close_models.py`, `services/ashare_close_projection.py`, `services/ashare_close_notifications.py` | Point-in-time request/result contracts, pure evidence/technical projections, and deterministic report rendering/message splitting |

## Next slices

The next large files are grouped by the responsibilities they mix:

| Area | Large files | Extraction order |
|---|---|---|
| A-share execution | `services/ashare_paper_day.py`, `services/ashare_intraday_paper.py` | projections/configuration → calendar/session logic → orchestration |
| Durable state | `storage/live_records.py` | codecs/projections → schema/lease helpers → transaction methods |
| Research | `strategy_lab/exit_evaluator.py`, `services/adversarial_macro.py` | pure calculations → dataset/manifest IO → orchestration |
| Presentation | `reporting/paper_day_summary.py`, `gui/integrations.py` | value formatting/artifacts → provider boundary → UI wiring |
| CLI | `cli.py` | command registration → Binance handlers → A-share workflow handlers → output formatting |

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
`src/gribuki_trade/trading/futures_oms_codec.py`,
`src/gribuki_trade/trading/oms_codec.py`,
`src/gribuki_trade/services/ashare_paper_day_projection.py`,
`src/gribuki_trade/services/ashare_close_models.py`,
`src/gribuki_trade/services/ashare_close_projection.py`,
`src/gribuki_trade/services/ashare_close_notifications.py`,
`src/gribuki_trade/storage/paper_day_codec.py`, and
`src/gribuki_trade/strategy_lab/exit_serialization.py`, and
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
`src/gribuki_trade/cli_commands/parsers/`.

Representative routing tests include `tests/unit/test_akshare_market_data.py`,
`tests/unit/test_ashare_paper_day.py`, `tests/unit/test_binance_execution.py`,
and `tests/unit/test_cli.py`. The remaining oversized orchestration facades
(`cli.py`, `ashare_paper_day.py`, `storage/live_records.py`) are the next
vertical slices; each must first extract pure projections or codecs before
moving transaction and dispatch logic.
