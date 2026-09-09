# Module map

This map describes the current canonical paths. Root-level adapter and service
implementation aliases were removed; imports should use the paths below.

| Area | Responsibility | Start here | Representative tests |
|---|---|---|---|
| `domain` | Shared value objects, events, orders, candidates and account state | `src/gribuki_trade/domain/` | `tests/unit/ashare/`, `tests/unit/trading/` |
| `ports` | Broker, market-data, news, LLM, notifier and ledger protocols | `src/gribuki_trade/ports/` | `tests/unit/adapters/`, `tests/unit/trading/` |
| `features` / `strategy` / `policy` | Pure indicators, screening, cross-market and decision rules | `src/gribuki_trade/features/` | `tests/unit/analysis/`, `tests/unit/ashare/` |
| `backtest` / `strategy_lab` | Offline replay, costs, walk-forward and research-only experiments | `src/gribuki_trade/backtest/`, `src/gribuki_trade/strategy_lab/` | `tests/unit/strategy_lab/` |
| `adapters/binance` | Binance REST/WebSocket protocol boundaries | `src/gribuki_trade/adapters/binance/` | `tests/unit/binance/`, `tests/unit/adapters/` |
| `adapters/ashare` | SSE/A-share provider adapters grouped by market, screening and profile | `src/gribuki_trade/adapters/ashare/` | `tests/unit/adapters/ashare/` |
| `adapters/market_data` | AKShare, BaoStock, archive and cross-market data | `src/gribuki_trade/adapters/market_data/` | `tests/unit/adapters/market_data/` |
| `adapters/macro` / `llm` / `schwab` / `notifiers` / `simulated` | External provider and broker-free simulation adapters | `src/gribuki_trade/adapters/<area>/` | matching `tests/unit/adapters/<area>/` |
| `ingest` / `pipeline` | News/search/document ingestion, normalization and deduplication | `src/gribuki_trade/ingest/` | `tests/unit/ingest/` |
| `services/binance` | Binance monitoring, Spot/Futures execution, PAPER and SHADOW orchestration | `src/gribuki_trade/services/binance/` | `tests/unit/binance/` |
| `services/live` | Live observations, protection inputs, records and recovery orchestration | `src/gribuki_trade/services/live/` | `tests/unit/services/live/` |
| `services/ashare` | A-share research, PAPER-day, intraday, close and evidence workflows | `src/gribuki_trade/services/ashare/` | `tests/unit/ashare/`, `tests/unit/services/` |
| `services/macro` / `research` / `communications` / `llm` / `exit` | Macro analysis, research, notifications, production LLM and exit lifecycle | `src/gribuki_trade/services/<area>/` | matching `tests/unit/services/<area>/` |
| `trading` | Broker-neutral and Futures/Spot OMS state transitions | `src/gribuki_trade/trading/` | `tests/unit/trading/` |
| `storage` | Durable SQLite stores, leases, append-only events and outboxes | `src/gribuki_trade/storage/` | `tests/unit/storage/` |
| `runtime` / `security` | PAPER/SHADOW/LIVE guards, clock/runtime safety and encrypted credentials | `src/gribuki_trade/runtime/`, `security/` | `tests/unit/runtime/` |
| `reporting` / `gui` / `cli_commands` | Artifacts, presentation, command parsing and handlers | `src/gribuki_trade/reporting/`, `gui/`, `cli_commands/` | corresponding `tests/unit/` directories |

## Binance operational entry points

- Spot and USDⓈ-M Futures transport: `adapters/binance/transport/`,
  `adapters/binance/spot/`, `adapters/binance/futures/`.
- Credentials and environment selection: `adapters/binance/auth/credentials.py`
  and `adapters/binance/auth/envs.py`; secrets are provided by
  `security.secrets.KeyringSecretProvider`.
- Fast server-clock calibration: `cli_commands/handlers/binance.py` dispatches
  `binance-time-sync`; the gateway/client maintain the measured offset for
  signed requests.
- LIVE status, balances, order tests and guarded commands: `cli_commands/handlers/binance_live.py`.
- Long-running Futures private-stream recovery: `services/binance/binance_futures_unattended.py`.
- Local order-book recovery: `adapters/binance/market_data/orderbook.py` and
  `services/binance/binance_orderbook.py`.

## Navigation rules

1. Start with this map, then read the nearest `AGENTS.md` and the matching
   architecture boundary page.
2. Keep provider wire code in `adapters`; compose providers and retries in
   `services`; keep SQLite transactions in `storage` or `trading`.
3. Pure parsing, codecs, models and policy modules must not import network,
   broker, Qt or SQLite connections.
4. Add tests under the matching `tests/unit/<area>/` directory and preserve
   fixture paths. Run `python -m pytest --collect-only -q` before and after a
   structural move to confirm collection is unchanged.

See [`source-layout.md`](source-layout.md),
[`execution-boundaries.md`](execution-boundaries.md), and
[`verification-map.md`](verification-map.md) for detailed boundaries and gates.
