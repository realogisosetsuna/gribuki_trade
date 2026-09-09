from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase

from gribuki_trade.adapters.binance.envs import BinanceProduct, BinanceStage
from gribuki_trade.adapters.binance.futures_user_stream import parse_futures_user_event
from gribuki_trade.runtime.guard import (
    LIVE_CONFIRMATION_PHRASE,
    LiveTradingGuard,
    LiveTradingNotConfirmed,
    OperationNotAllowed,
)
from gribuki_trade.runtime.mode import TradingMode
from gribuki_trade.services.binance_futures_unattended import (
    BinanceFuturesUnattendedExecutionService,
)
from gribuki_trade.trading.futures_oms import FuturesOrderManagementStore


class FakeStream:
    def __init__(self, *events: object, epoch: int = 1) -> None:
        self._events = events
        self.connection_epoch = epoch
        self.connected = False
        self.healthy = True
        self.closed = False

    async def connect(self) -> None:
        self.connected = True

    async def aclose(self) -> None:
        self.closed = True

    async def events(self) -> AsyncIterator[object]:
        for event in self._events:
            yield event


class FakeClient:
    def __init__(self, *, live: bool = False) -> None:
        self.profile = SimpleNamespace(is_live=live)
        self.stage = BinanceStage.LIVE if live else BinanceStage.TESTNET
        self.product = BinanceProduct.USDS_FUTURES
        self.calls: list[tuple[str, object]] = []
        self.submit_calls = 0
        self.fail_submit = False

    async def ping(self) -> None:
        self.calls.append(("ping", None))

    async def synchronize_time(self) -> int:
        self.calls.append(("time", None))
        return 0

    async def account(self) -> dict[str, object]:
        self.calls.append(("account", None))
        return {
            "assets": [
                {
                    "asset": "USDT",
                    "walletBalance": "100",
                    "availableBalance": "90",
                    "crossWalletBalance": "95",
                }
            ]
        }

    async def position_risk(self) -> tuple[dict[str, object], ...]:
        self.calls.append(("position", None))
        return ()

    async def open_orders(self) -> tuple[dict[str, object], ...]:
        self.calls.append(("open", None))
        return ()

    async def open_algo_orders(self) -> tuple[dict[str, object], ...]:
        self.calls.append(("open_algo", None))
        return ()

    async def all_orders(self, symbol: str, *, limit: int = 1_000) -> tuple[dict[str, object], ...]:
        self.calls.append(("history", (symbol, limit)))
        return ()

    async def all_algo_orders(
        self, symbol: str, *, limit: int = 1_000
    ) -> tuple[dict[str, object], ...]:
        self.calls.append(("history_algo", (symbol, limit)))
        return ()

    async def submit_order(self, **kwargs: object) -> dict[str, object]:
        self.submit_calls += 1
        self.calls.append(("submit", kwargs))
        if self.fail_submit:
            raise TimeoutError("request outcome is unknown")
        return {
            "symbol": str(kwargs["symbol"]),
            "orderId": 123,
            "clientOrderId": kwargs.get("new_client_order_id", "entry-1"),
            "status": "NEW",
            "side": kwargs.get("side", "BUY"),
            "positionSide": kwargs.get("position_side", "LONG"),
            "origQty": kwargs.get("quantity", "0.001"),
        }

    async def submit_algo_order(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(("submit_algo", kwargs))
        return {"symbol": str(kwargs["symbol"]), "algoId": 456, "algoStatus": "NEW"}

    async def cancel_algo_order(self, symbol: str, **kwargs: object) -> dict[str, object]:
        self.calls.append(("cancel_algo", (symbol, kwargs)))
        return {"symbol": symbol, "algoId": kwargs.get("algo_id"), "algoStatus": "CANCELED"}


class BinanceFuturesUnattendedTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "futures.sqlite3"
        self.oms = FuturesOrderManagementStore(self.path)
        self.addCleanup(self.oms.close)

    def service(
        self,
        client: FakeClient | None = None,
        stream: FakeStream | None = None,
        *,
        guard: LiveTradingGuard | None = None,
    ) -> BinanceFuturesUnattendedExecutionService:
        return BinanceFuturesUnattendedExecutionService(
            client or FakeClient(),
            self.oms,
            account_id="acct",
            symbols=("BTCUSDT",),
            user_stream=stream or FakeStream(),  # type: ignore[arg-type]
            guard=guard,
            # OMS 在启动时取得实时所有权租约；让服务时钟使用同一时间域，避免命令
            # 认领时被误判为过期。
            clock=lambda: datetime.now(UTC),
        )

    async def test_start_reconciles_before_connecting_private_stream(self) -> None:
        client = FakeClient()
        stream = FakeStream()
        service = self.service(client, stream)

        result = await service.start()

        self.assertTrue(service.started)
        self.assertTrue(stream.connected)
        self.assertEqual(result.balances, 1)
        self.assertEqual(
            [name for name, _ in client.calls][:4], ["ping", "time", "account", "position"]
        )

    async def test_order_update_preserves_hedge_side_and_records_fill(self) -> None:
        service = self.service()
        event = parse_futures_user_event(
            '{"e":"ORDER_TRADE_UPDATE","E":1000,"T":999,"o":{'
            '"s":"BTCUSDT","c":"entry-1","S":"BUY","o":"LIMIT",'
            '"q":"0.01","ap":"70010","x":"TRADE","X":"PARTIALLY_FILLED",'
            '"i":123,"l":"0.001","z":"0.001","L":"70010","t":456,'
            '"rp":"1.2","N":"USDT","n":"0.01","ps":"LONG",'
            '"R":false,"cp":false,"wt":"CONTRACT_PRICE"}}',
            received_time_ms=1001,
            connection_epoch=1,
        )

        self.assertTrue(await service.consume_user_event(event))
        orders = self.oms.orders(account_id="acct", environment="TESTNET", product="USDS_FUTURES")
        fills = self.oms.fills(account_id="acct", environment="TESTNET", product="USDS_FUTURES")
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].position_side, "LONG")
        self.assertEqual(orders[0].exchange_order_id, "123")
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].trade_id, "456")

    async def test_account_update_preserves_hedge_positions(self) -> None:
        service = self.service()
        event = parse_futures_user_event(
            '{"e":"ACCOUNT_UPDATE","E":1000,"T":999,"a":{'
            '"B":[{"a":"USDT","wb":"100","cw":"90","bc":"0"}],'
            '"P":[{"s":"BTCUSDT","pa":"0.01","ep":"70000",'
            '"bep":"70001","cr":"1","up":"2","mt":"isolated",'
            '"iw":"10","ps":"LONG"}]}}',
            received_time_ms=1001,
            connection_epoch=1,
        )

        self.assertTrue(await service.consume_user_event(event))
        positions = self.oms.positions(
            account_id="acct", environment="TESTNET", product="USDS_FUTURES"
        )
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].position_side, "LONG")
        self.assertEqual(str(positions[0].quantity), "0.01")

    async def test_trade_lite_is_deduplicated_by_event_identity(self) -> None:
        service = self.service()
        raw = '{"e":"TRADE_LITE","E":1000,"T":999,"s":"BTCUSDT",' \
            '"q":"0.001","p":"70000","L":"70000","l":"0.001",' \
            '"S":"BUY","i":123,"t":789,"c":"entry-1"}'
        event = parse_futures_user_event(raw, received_time_ms=1001, connection_epoch=1)

        self.assertTrue(await service.consume_user_event(event))
        self.assertFalse(await service.consume_user_event(event))
        fills = self.oms.fills(account_id="acct", environment="TESTNET", product="USDS_FUTURES")
        self.assertEqual(len(fills), 1)

    async def test_unknown_event_marks_stream_degraded(self) -> None:
        service = self.service()
        event = parse_futures_user_event(
            '{"e":"UNRECOGNIZED_EVENT","E":1000}', received_time_ms=1001, connection_epoch=1
        )

        self.assertTrue(await service.consume_user_event(event))
        health = self.oms.health(account_id="acct", environment="TESTNET", product="USDS_FUTURES")
        self.assertIsNotNone(health)
        self.assertEqual(health.state, "DEGRADED")  # type: ignore[union-attr]

    async def test_unknown_submit_outcome_is_persisted_without_retry(self) -> None:
        client = FakeClient()
        client.fail_submit = True
        service = self.service(client)
        await service.start()

        with self.assertRaises(TimeoutError):
            await service.submit_order(
                symbol="BTCUSDT",
                side="BUY",
                type="LIMIT",
                quantity="0.001",
                new_client_order_id="entry-1",
            )

        self.assertEqual(client.submit_calls, 1)
        commands = self.oms.commands(
            account_id="acct", environment="TESTNET", product="USDS_FUTURES"
        )
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].status.value, "UNKNOWN")

        # 重用相同客户端编号前必须先解决持久化 UNKNOWN 命令；盲目再次提交可能
        # 创建重复仓位。
        with self.assertRaises((RuntimeError, ValueError)):
            await service.submit_order(
                symbol="BTCUSDT",
                side="BUY",
                type="LIMIT",
                quantity="0.001",
                new_client_order_id="entry-1",
            )
        self.assertEqual(client.submit_calls, 1)

    async def test_stop_closes_stream_and_clears_started_state(self) -> None:
        stream = FakeStream()
        service = self.service(stream=stream)
        await service.start()

        await service.stop()

        self.assertFalse(service.started)
        self.assertTrue(stream.closed)

    async def test_unhealthy_private_stream_fails_closed_before_submit(self) -> None:
        client = FakeClient()
        stream = FakeStream()
        service = self.service(client, stream)
        await service.start()
        stream.healthy = False

        with self.assertRaises(RuntimeError):
            await service.submit_order(symbol="BTCUSDT", side="BUY", type="MARKET")
        self.assertEqual(client.submit_calls, 0)

    async def test_shadow_guard_blocks_submit_after_query_startup(self) -> None:
        guard = LiveTradingGuard(
            TradingMode.SHADOW,
            allowed_accounts=("acct",),
            allowed_exchanges=("BINANCE",),
        )
        client = FakeClient()
        service = self.service(client, guard=guard)
        await service.start()

        with self.assertRaises(OperationNotAllowed):
            await service.submit_order(symbol="BTCUSDT", side="BUY", type="MARKET")
        self.assertEqual(client.submit_calls, 0)

    async def test_live_guard_requires_confirmation_before_start(self) -> None:
        guard = LiveTradingGuard(
            TradingMode.LIVE,
            allowed_accounts=("acct",),
            allowed_exchanges=("BINANCE",),
        )
        client = FakeClient(live=True)
        service = self.service(client, guard=guard)

        with self.assertRaises(LiveTradingNotConfirmed):
            await service.start()
        guard.confirm_live_trading(LIVE_CONFIRMATION_PHRASE)
        result = await service.start()
        self.assertEqual(result.balances, 1)
