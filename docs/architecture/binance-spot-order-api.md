# Binance Spot order execution API

`BinanceSpotGateway` keeps Spot order construction in typed request objects while
`BinanceSpotAdvancedExecutionService` exposes the same operations to strategies
without making them depend on the HTTP adapter. Every write method is available
only after `connect()` and still passes through the enclosing runtime LIVE/SHADOW
guard.

## Structured requests

- `BinanceSpotOrderLeg` models one leg. `order_type` supports Binance Spot
  `MARKET`, `LIMIT`, `STOP_LOSS`, `STOP_LOSS_LIMIT`, `TAKE_PROFIT`,
  `TAKE_PROFIT_LIMIT`, and `LIMIT_MAKER`. Conditional legs can use either an
  absolute `stop_price`, a `trailing_delta` in BIPS, or both.
- `BinanceSpotOcoRequest` represents an OCO pair. `above` and `below` use the
  same quantity and side; Binance cancels the remaining leg after one executes.
- `BinanceSpotOtoRequest` represents a working order followed by a pending order
  that is activated after the working order fully fills.
- `BinanceSpotOtocoRequest` represents a working order followed by a pending
  OCO pair, which is the native Spot bracket-order shape.

The adapter returns `BinanceOrderSnapshot` for a single order and
`BinanceOrderListSnapshot` for an order list. Decimal amounts are serialized
without binary floating point. Invalid type/price/stop/trailing combinations
fail locally before a signed request.

## Gateway operations

| Strategy operation | Gateway method | Binance REST route |
| --- | --- | --- |
| Single market/limit/conditional/trailing order | `submit_spot_order` | `POST /api/v3/order` |
| OCO take-profit/stop-loss pair | `submit_oco` | `POST /api/v3/orderList/oco` |
| Entry then exit | `submit_oto` | `POST /api/v3/orderList/oto` |
| Entry then TP/SL OCO | `submit_otoco` | `POST /api/v3/orderList/otoco` |
| Dynamic price/stop update | `cancel_replace` | `POST /api/v3/order/cancelReplace` |
| Reduce quantity and keep queue priority | `amend_order_keep_priority` | `PUT /api/v3/order/amend/keepPriority` |
| Cancel one list | `cancel_order_list` | `DELETE /api/v3/orderList` |
| Cancel all symbol orders | `cancel_all_open_orders` | `DELETE /api/v3/openOrders` |

`cancel_replace` returns both the cancel and new-order outcomes. Strategies must
persist those results and reconcile when the transport result is uncertain;
`ALLOW_FAILURE` is available but should only be used when the strategy's risk
policy explicitly accepts a possible overlap.

Official reference: [Spot Trade REST API](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/trade).

Implementation: `src/gribuki_trade/adapters/binance/gateway.py` and
`src/gribuki_trade/adapters/binance/models.py`. Verification:
`tests/unit/binance/test_binance_gateway.py` and the Binance adapter test family under
`tests/unit/`.
