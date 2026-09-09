# Adapter map

Read [`ARCHITECTURE.md`](../../../ARCHITECTURE.md), [`docs/architecture/source-layout.md`](../../../docs/architecture/source-layout.md), and the nearest provider map before editing. Provider protocols stay here; application orchestration belongs in services and tests live under `tests/unit/adapters/`.

- `binance/`: Binance REST/WebSocket, auth, transport and market-data boundaries.
- `ashare/`: A-share market, screening and instrument-profile providers.
- `market_data/`, `macro/`, `simulated/`, `llm/`, `notifiers/`, `schwab/`: other external boundaries.
- Never log secrets; preserve mode guards and point-in-time source metadata.
