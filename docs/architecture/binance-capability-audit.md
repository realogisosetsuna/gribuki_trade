# Binance Spot and USDⓈ-M capability audit

**Audit date:** 2026-09-09  
**Scope:** unattended LIVE market data and execution surfaces used by this
repository. This is a code audit, not a claim that every Binance API is part of
the product. The status below is intentionally conservative.

Implementation evidence: `src/gribuki_trade/adapters/binance/`,
`src/gribuki_trade/services/binance_futures_unattended.py`, and
`src/gribuki_trade/trading/futures_oms.py`,
`src/gribuki_trade/adapters/binance/orderbook.py`,
`src/gribuki_trade/services/binance_orderbook.py`, and
`src/gribuki_trade/trading/spot_order_lists.py`. Verification evidence:
`tests/unit/binance/test_binance_futures_stream.py`,
`tests/unit/binance/test_binance_futures_user_stream.py`,
`tests/unit/binance/test_binance_futures_unattended.py`,
`tests/unit/trading/test_futures_oms.py`, `tests/unit/binance/test_binance_orderbook.py`,
`tests/unit/test_spot_order_list_store.py`, and the Spot Binance adapter tests
under `tests/unit/`.

Official references used for this audit:

- [Binance API catalogue](https://developers.binance.com/en/docs/catalog)
- [Spot REST API](https://developers.binance.com/en/docs/products/spot/rest-api)
- [Spot trade REST endpoints](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/trade)
- [Spot WebSocket user-data API](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-api/user-data-stream)
- [Spot WebSocket account methods](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-api/account)
- [Spot local order-book procedure (Binance-maintained API specification)](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly)
- [USDⓈ-M WebSocket market streams](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)
- [USDⓈ-M local order-book procedure](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)
- [USDⓈ-M WebSocket user-data API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/user-data-streams)

## Status vocabulary

* **Implemented** means the adapter has the route/event parser and tests for
  the shape, but production readiness still depends on runtime guards and
  reconciliation.
* **Partial** means only a subset is implemented or an important recovery or
  persistence invariant is missing.
* **Missing** means strategies cannot use the surface through a repository
  port.
* **Out of scope** means a valid Binance product surface that is deliberately
  not part of this Spot/USDⓈ-M phase (for example SOR or margin).

## Spot public market data

| Binance surface | Repository implementation | Status | LIVE implication / follow-up |
| --- | --- | --- | --- |
| REST `ping`, `time`, `exchangeInfo`, price ticker, depth, klines | `adapters/binance/gateway.py` | Implemented | Time offset and symbol rules are available. REST polling is not a substitute for a low-latency stream during volatility. |
| WebSocket `@trade` | `adapters/binance/stream.py` (`BinanceTradeEvent`) | Implemented | Parser checks symbol and trade-id monotonicity; reconnect does not prove that no messages were missed. |
| WebSocket `@bookTicker` | `BinanceBookTickerEvent` | Implemented | Best bid/ask only; update-id regression is rejected. Full depth strategies use the local order-book recovery service. |
| WebSocket `@kline_<interval>` | `BinanceKlineEvent` | Implemented | Candle events are validated; no sequence continuity or REST backfill is coupled to reconnect. |
| WebSocket `@depth` / `@depth@100ms` | `BinanceDepthEvent` + `BinanceSpotOrderBook` + `BinanceOrderBookRecoveryService` | Implemented | Buffers diffs, applies `/api/v3/depth`, enforces `U <= lastUpdateId + 1 <= u`, deletes zero levels, and marks gaps/overflow `DESYNCED` until a fresh snapshot bridges the stream. |
| WebSocket `@aggTrade`, `@miniTicker`, `@ticker`, rolling statistics | No parser/subscription | Missing | Add only if a strategy needs the feed; `@trade` and `@bookTicker` are currently the smallest supported set. |
| SBE market data | No SBE transport/schema | Out of scope | SBE is a lower-latency option but requires a binary schema/decoder and a separate operational test. |
| 24-hour rotation, 10 msg/s inbound limit, 1024 streams/connection | URL stream connector enforces stream count; reconnect is bounded | Partial | The connector rotates before 24h and uses bounded jittered reconnect. A per-account rate limiter and a soak test are still required before unattended LIVE. |

The Spot local-order-book requirement is not optional: the official procedure
requires buffering depth events, taking `/api/v3/depth`, discarding old updates,
checking the first `[U,u]` range, and restarting when the next update is not
contiguous. `bookTicker` cannot provide those guarantees.

## Spot REST account and execution

| Surface | Implementation | Status | Notes |
| --- | --- | --- | --- |
| New single order and test order (`POST /api/v3/order`, `/order/test`) | `BinanceSpotGateway.submit_spot_order`, `test_order` | Implemented | Supports market/limit, stop/take-profit variants, `quoteOrderQty`, iceberg, STP fields, and `trailingDelta` where supplied. Symbol filters still have to be loaded before a strategy submits. |
| OCO, OTO, OTOCO order lists | `submit_oco`, `submit_oto`, `submit_otoco` + `SQLiteSpotOrderListStore` | Implemented | List projection, independent member-leg table, raw event idempotency, startup REST reconciliation, and `listStatus` refresh are durable. |
| OPO / OPOCO and SOR routes | No adapter | Out of scope | Add only after defining allocation/working-floor semantics and durable reconciliation. |
| Cancel order/list, cancel all, cancel-replace | Gateway methods | Implemented | A transport timeout can leave either cancel or replacement accepted. Persist both outcomes and reconcile by order/list query before retrying. |
| Amend keep-priority | `amend_order_keep_priority` | Implemented | Binance permits quantity reduction only; strategies must not model this as an arbitrary price amendment. |
| Query order, open orders, all orders, trades, account, commission | Gateway methods | Implemented | Reconciliation reads exist for single orders and account state. Spot WebSocket account equivalents are not wired into the gateway. |
| Order-list query (`allOrderLists`, `openOrderLists`) and order-list status | `gateway.py`, `binance_spot_advanced.py`, `binance_execution.py`, and `trading/spot_order_lists.py` | Implemented | REST open/history snapshots and `listStatus` are persisted atomically with member references; restart reconciliation runs before pending commands resume. |
| Rate-limit and order-count headers | Request metadata is captured by the gateway | Partial | No proactive per-account limiter/circuit breaker is exposed to a strategy. |

## Spot private WebSocket stream

`adapters/binance/user_stream.py` uses the current signed Spot WebSocket API
subscription and parses `executionReport`, `outboundAccountPosition`,
`balanceUpdate`, and `listStatus`. A list event is persisted first, then triggers
a REST reconciliation boundary in `BinanceSpotExecutionService`; the independent
`spot_order_lists` projection stores list identity and member legs before strategy
commands resume. Every reconnect remains a reconciliation boundary.

The current user stream connector has bounded reconnect, credential guards, and
scheduled rotation. Treat every reconnect as a reconciliation boundary, not as
proof that no events were lost.

## USDⓈ-M public market data

| Surface | Implementation | Status | LIVE implication / follow-up |
| --- | --- | --- | --- |
| REST `exchangeInfo`, price, depth, klines | `adapters/binance/futures.py` | Implemented | Useful for bootstrap and validation only. |
| Public routed WebSocket (`/public`) `@depth`, `@aggTrade`, `@trade` | `adapters/binance/futures_stream.py` | Partial | Typed transport events and sequence/gap fail-closed checks are implemented; REST snapshot application and durable book state remain above the adapter. |
| Market routed WebSocket (`/market`) `@markPrice`, ticker/miniTicker, index and funding feeds | `FuturesMarkPriceEvent` / `FuturesTickerEvent` | Partial | Mark price, funding rate and next funding time are parsed. Index/mini-ticker variants and persistence are still missing. |
| Futures local order-book recovery | `BinanceFuturesOrderBook` + `BinanceOrderBookRecoveryService` + `FuturesRestClient.order_book` | Implemented | Uses `U <= snapshot.lastUpdateId <= u`, then requires `pu == previous final id`; gaps and buffer overflow clear the view and require a new REST snapshot. |
| 24-hour rotation, ping/pong, 10 msg/s, 1024 streams | `adapters/binance/futures_stream.py` + `services/binance_orderbook.py` | Partial | Routed connectors, rotation, reconnect, sequence checks, and REST bootstrap are implemented. Per-account rate limiting and long-duration soak evidence remain. |

USDⓈ-M now has routed endpoints: `wss://fstream.binance.com/public` for
high-frequency public data, `/market` for regular market data, and `/private`
for user data. An unrouted URL may silently deliver only public streams, so a
generic Spot-style URL builder would be unsafe.

## USDⓈ-M REST account, risk and execution

| Surface | Implementation | Status | Notes |
| --- | --- | --- | --- |
| Account, position risk, exchange info, orders, trades | `adapters/binance/futures.py` and futures execution service | Implemented | Position mode/side, leverage and margin-type controls are represented. All LIVE writes still require runtime confirmation. |
| Leverage and margin type | Futures gateway methods | Implemented | Must be applied and confirmed before an entry; position-side semantics differ in Hedge and One-way modes. |
| Native conditional/protection orders | Futures protection/algo methods | Partial | REST submission/cancel/query exists in the current branch; activation and terminal status still require private-stream reconciliation. |
| Current USDⓈ-M Algo API (`/fapi/v1/algoOrder` family) | Check the futures adapter implementation and tests | Partial | Binance has migrated conditional/TP/SL workflows to the Algo API. Keep legacy conditional routes behind an explicit capability check and persist `algoId` separately from `orderId`. |
| Cancel/replace, modify, cancel-all for algo orders | Gateway subset | Partial | A failed or timed-out cancel must be reconciled by querying the algo order before a retry. |
| REST rate limits and response headers | Request metadata only | Partial | No strategy-facing limiter or durable retry budget. |

## USDⓈ-M private events

`adapters/binance/futures_user_stream.py` provides a guarded parser and
reconnecting transport for `ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`,
`ACCOUNT_CONFIG_UPDATE`, `MARGIN_CALL`, `ALGO_UPDATE`, `TRADE_LITE`, strategy
and grid events, trigger rejects, and stream-expiry events. `futures.py`
exposes listen-key start/keepalive/close and uses the routed private URL.
`BinanceFuturesUnattendedExecutionService` persists raw events, order/fill/
position/balance/config projections, owner leases, command outcomes, and
startup/reconnect REST reconciliation. Unknown outcomes stay UNKNOWN and block
blind retries. This is **partial** until a durable protection-plan reconciler
and a long-running rate-limited network soak is completed. The local-book
snapshot component is now available through `BinanceOrderBookRecoveryService`.

## Readiness decision

The existing guarded paths are suitable for development and explicitly
confirmed LIVE probes. The repository is **not yet ready for unattended LIVE** strategy automation
until operational rate-limit, soak, restart, and account-acceptance evidence is
recorded, even though local order-book and Spot order-list recovery are now
implemented.
The remaining production-blocking evidence set is:

1. Add account-scoped rate limiting/circuit breaking.
2. Complete a multi-hour authenticated network soak for private and public
   streams, including reconnect and process restart.
3. Run a controlled account-level acceptance test without enabling strategy
   automation.

The USDⓈ-M private event lifecycle, durable OMS, restart reconciliation,
owner fencing, and explicit 24-hour rotation are now present. They still need
an authenticated network soak and a controlled account-level acceptance test;
those cannot be inferred from offline unit tests.

These items should remain behind the existing PAPER/SHADOW/LIVE guards. New
strategy code should depend on ports/services and never import Binance
transport classes directly.
