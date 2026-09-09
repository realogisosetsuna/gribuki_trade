import json
from collections import deque
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest import IsolatedAsyncioTestCase

from gribuki_trade.adapters.binance import parse_stream_message
from gribuki_trade.adapters.simulated.paper import PaperBroker
from gribuki_trade.adapters.simulated.paper_account import PaperSpotAccount, SpotSymbolAssets
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.services.binance.binance_paper import BinancePaperEngine, PaperRiskLimits
from gribuki_trade.trading import SQLiteOrderManagementStore, TradingCommandStatus

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)


def kline(*, closed: bool = True) -> object:
    return parse_stream_message(
        json.dumps(
            {
                "e": "kline",
                "E": int(NOW.timestamp() * 1_000),
                "s": "BTCUSDT",
                "k": {
                    "t": int(NOW.timestamp() * 1_000) - 60_000,
                    "T": int(NOW.timestamp() * 1_000) - 1,
                    "s": "BTCUSDT",
                    "i": "1m",
                    "f": 1,
                    "L": 2,
                    "o": "100",
                    "c": "101",
                    "h": "102",
                    "l": "99",
                    "v": "10",
                    "n": 2,
                    "x": closed,
                    "q": "1005",
                    "V": "4",
                    "Q": "402",
                },
            }
        )
    )


def book(*, event_time_ms: int | None = None) -> object:
    payload: dict[str, object] = {
        "u": 2,
        "s": "BTCUSDT",
        "b": "100",
        "B": "1",
        "a": "101",
        "A": "1",
    }
    if event_time_ms is not None:
        payload["E"] = event_time_ms
    return parse_stream_message(json.dumps(payload))


class FakeMarket:
    def __init__(self, *events: object) -> None:
        self._events = deque(events)

    async def events(self):  # type: ignore[no-untyped-def]
        while self._events:
            yield self._events.popleft()


def order(order_id: str = "paper-1", *, notional: str = "10") -> OrderIntent:
    price = Decimal("101")
    return OrderIntent(
        client_order_id=order_id,
        account_id="paper-account",
        strategy_id="unit",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal(notional) / price,
        limit_price=price,
        created_at=NOW,
    )


class BinancePaperEngineTests(IsolatedAsyncioTestCase):
    async def test_engine_persists_before_submit_then_matches_and_recovers(self) -> None:
        events = (kline(), book())

        def decide(_event: object, _snapshot: object) -> tuple[OrderIntent, ...]:
            return (order(),)

        path = Path(self._testMethodName + ".sqlite3")
        self.addCleanup(path.unlink, missing_ok=True)
        account = PaperSpotAccount(
            account_id="paper-account",
            initial_balances={"USDT": "1000", "BTC": "0"},
            symbol_assets={"BTCUSDT": SpotSymbolAssets("BTC", "USDT")},
        )
        with SQLiteOrderManagementStore(path) as oms:
            engine = BinancePaperEngine(
                FakeMarket(*events),
                PaperBroker(),
                oms,
                decide,
                account=account,
                clock=lambda: NOW,
            )

            snapshot = await engine.run(maximum_events=2)

            persisted = oms.require_order("paper-1")
            self.assertIs(persisted.status, OrderStatus.FILLED)
            self.assertEqual(persisted.filled_quantity, order().quantity)
            self.assertEqual(len(oms.fills(client_order_id="paper-1")), 1)
            self.assertEqual(oms.fills()[0].fee_asset, "USDT")
            self.assertGreater(oms.fills()[0].fee_amount, 0)
            self.assertEqual(account.balance("USDT").locked, 0)
            self.assertGreater(account.balance("BTC").free, 0)
            self.assertEqual(oms.balances("paper-account")[0].asset, "BTC")
            self.assertEqual(
                oms.positions(account_id="paper-account")[0].quantity,
                order().quantity,
            )
            self.assertIs(oms.commands()[0].status, TradingCommandStatus.SENT)
            self.assertEqual(snapshot.submitted_orders, 1)
            self.assertEqual(snapshot.fill_count, 1)

        with SQLiteOrderManagementStore(path) as reopened:
            self.assertIs(reopened.require_order("paper-1").status, OrderStatus.FILLED)
            self.assertEqual(len(reopened.fills()), 1)

    async def test_engine_uses_only_closed_fresh_bars_and_enforces_risk(self) -> None:
        attempts = 0

        def decide(_event: object, _snapshot: object) -> tuple[OrderIntent, ...]:
            nonlocal attempts
            attempts += 1
            return (order("too-large", notional="21"),)

        stale_time = int(NOW.timestamp() * 1_000) - 5_000
        path = Path(self._testMethodName + ".sqlite3")
        self.addCleanup(path.unlink, missing_ok=True)
        with SQLiteOrderManagementStore(path) as oms:
            engine = BinancePaperEngine(
                FakeMarket(kline(closed=False), book(event_time_ms=stale_time), kline()),
                PaperBroker(),
                oms,
                decide,
                limits=PaperRiskLimits(maximum_order_notional=Decimal("20")),
                clock=lambda: NOW,
            )

            snapshot = await engine.run(maximum_events=3)

            self.assertEqual(attempts, 1)
            self.assertEqual(oms.orders_requiring_reconciliation(), ())
            self.assertEqual(snapshot.processed_closed_bars, 1)
            self.assertEqual(snapshot.stale_market_events, 1)
            self.assertEqual(snapshot.rejected_signals, 1)
