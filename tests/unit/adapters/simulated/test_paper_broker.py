import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase

from gribuki_trade.adapters.simulated.paper import (
    ORDER_FILL_EVENT,
    ORDER_STATUS_EVENT,
    PaperBroker,
    PaperFill,
    PaperOrderUpdate,
)
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side


def make_order(client_order_id: str = "paper-1", *, quantity: int = 100) -> OrderIntent:
    return OrderIntent(
        client_order_id=client_order_id,
        account_id="paper",
        strategy_id="smoke",
        symbol="600000.SH",
        side=Side.BUY,
        quantity=quantity,
        limit_price=Decimal("10.01"),
        created_at=datetime(2026, 8, 13, 1, 30, tzinfo=UTC),
    )


class PaperBrokerTests(IsolatedAsyncioTestCase):
    async def test_connected_broker_accepts_limit_order(self) -> None:
        broker = PaperBroker()
        await broker.connect()

        order = make_order()
        await broker.submit_order(order)
        event = await anext(broker.events())

        self.assertEqual(event.event_type, ORDER_STATUS_EVENT)
        self.assertIsInstance(event.payload, PaperOrderUpdate)
        self.assertEqual(event.payload.order, order)
        self.assertIs(event.payload.status, OrderStatus.ACCEPTED)
        self.assertIsNone(event.payload.reason)

    async def test_duplicate_client_order_id_is_idempotent(self) -> None:
        broker = PaperBroker()
        await broker.connect()
        order = make_order()

        await broker.submit_order(order)
        await broker.submit_order(order)
        stream = broker.events()

        self.assertIs((await anext(stream)).payload.status, OrderStatus.ACCEPTED)
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(anext(stream), timeout=0.01)

    async def test_conflicting_reuse_of_client_order_id_is_rejected(self) -> None:
        broker = PaperBroker()
        await broker.connect()
        await broker.submit_order(make_order(quantity=100))

        with self.assertRaisesRegex(ValueError, "already used for a different order"):
            await broker.submit_order(make_order(quantity=200))

    async def test_disconnected_submission_publishes_rejection(self) -> None:
        broker = PaperBroker()
        order = make_order()

        await broker.submit_order(order)
        event = await anext(broker.events())

        self.assertIs(event.payload.status, OrderStatus.BROKER_REJECTED)
        self.assertEqual(event.payload.reason, "paper broker is not connected")
        self.assertIs(broker.order_update(order.client_order_id), event.payload)

    async def test_cancel_publishes_canceled_status_and_is_idempotent(self) -> None:
        broker = PaperBroker()
        await broker.connect()
        order = make_order()
        await broker.submit_order(order)
        stream = broker.events()
        await anext(stream)

        await broker.cancel_order(order.client_order_id)
        event = await anext(stream)
        await broker.cancel_order(order.client_order_id)

        self.assertIs(event.payload.status, OrderStatus.CANCELED)
        self.assertIs(broker.order_update(order.client_order_id), event.payload)
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(anext(stream), timeout=0.01)

    async def test_cancel_requires_connection_and_known_accepted_order(self) -> None:
        broker = PaperBroker()
        await broker.connect()
        order = make_order()
        await broker.submit_order(order)
        await broker.disconnect()

        with self.assertRaisesRegex(ConnectionError, "not connected"):
            await broker.cancel_order(order.client_order_id)
        with self.assertRaisesRegex(KeyError, "unknown client_order_id"):
            await broker.cancel_order("missing")

    async def test_partial_and_final_fills_update_weighted_average(self) -> None:
        broker = PaperBroker()
        await broker.connect()
        order = make_order(quantity=100)
        await broker.submit_order(order)
        stream = broker.events()
        await anext(stream)

        first = await broker.record_fill(
            order.client_order_id,
            quantity="40",
            price="10.00",
            fill_id="fill-1",
        )
        fill_event = await anext(stream)
        partial_event = await anext(stream)
        self.assertEqual(fill_event.event_type, ORDER_FILL_EVENT)
        self.assertIsInstance(fill_event.payload, PaperFill)
        self.assertEqual(first, fill_event.payload)
        self.assertIs(partial_event.payload.status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(partial_event.payload.filled_quantity, Decimal("40"))

        duplicate = await broker.record_fill(
            order.client_order_id,
            quantity="40",
            price="10.00",
            fill_id="fill-1",
        )
        self.assertEqual(duplicate, first)

        await broker.record_fill(
            order.client_order_id,
            quantity="60",
            price="11.00",
            fill_id="fill-2",
        )
        await anext(stream)
        final_event = await anext(stream)
        self.assertIs(final_event.payload.status, OrderStatus.FILLED)
        self.assertEqual(final_event.payload.average_fill_price, Decimal("10.60"))
        self.assertEqual(len(broker.fills()), 2)

    async def test_match_quote_fills_only_marketable_limit_orders(self) -> None:
        broker = PaperBroker()
        await broker.connect()
        marketable = make_order("buy", quantity=100)
        resting = OrderIntent(
            client_order_id="resting",
            account_id="paper",
            strategy_id="smoke",
            symbol="600000.SH",
            side=Side.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("9.99"),
            created_at=datetime(2026, 8, 13, 1, 30, tzinfo=UTC),
        )
        await broker.submit_order(marketable)
        await broker.submit_order(resting)

        fills = await broker.match_quote("600000.SH", bid="10.00", ask="10.01")

        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].client_order_id, "buy")
        self.assertIs(broker.order_update("buy").status, OrderStatus.FILLED)
        self.assertIs(broker.order_update("resting").status, OrderStatus.ACCEPTED)
