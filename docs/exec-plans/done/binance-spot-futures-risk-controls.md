# Binance Spot and USDⓈ-M risk controls

Status: complete

## Objective

Provide the first structured Spot and USDⓈ-M Futures risk-control surface.

Active. This plan covers the first structured risk-control surface for Binance
Spot and USDⓈ-M Futures: product-specific order parameters, leverage and
margin configuration, conditional exits, and strategy-facing service methods.

## Decisions

- Keep Binance protocol details in `adapters/binance`; strategies call services.
- Preserve the existing `PAPER`/`SHADOW`/`LIVE` guard for every query and write.
- Treat exchange-native conditional orders as the primary protection mechanism.
- Represent trailing exits with explicit activation/callback fields and expose
  cancel-and-replace helpers because conditional order amendment support differs
  by product and order type.
- Keep all quantities/prices/rates as `Decimal` at the service boundary.

## Validation

- Binance unit tests and CLI tests.
- Readiness, Ruff, mypy, and the full pytest suite.
- Official Binance REST/WebSocket documentation comparison recorded in the
  integration guide.

## Verification gaps

- Offline tests cannot prove live matching-engine acceptance or production
  WebSocket soak behavior. Any live smoke test remains read-only/order-test
  unless the operator explicitly uses the existing LIVE confirmation.
