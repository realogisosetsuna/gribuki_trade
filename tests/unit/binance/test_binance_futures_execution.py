from __future__ import annotations

from unittest import IsolatedAsyncioTestCase

from gribuki_trade.adapters.binance.auth.envs import BinanceStage
from gribuki_trade.adapters.binance.transport.gateway import BinanceConfigurationError
from gribuki_trade.runtime.guard import (
    LIVE_CONFIRMATION_PHRASE,
    LiveTradingGuard,
    LiveTradingNotConfirmed,
    OperationNotAllowed,
)
from gribuki_trade.runtime.mode import TradingMode
from gribuki_trade.services.binance.binance_futures_execution import (
    BinanceFuturesExecutionService,
)


class FakeFuturesClient:
    stage = BinanceStage.LIVE

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def ping(self) -> None:
        self.calls.append(("ping", None))

    async def synchronize_time(self) -> int:
        self.calls.append(("time", None))
        return 3

    async def account(self) -> dict[str, object]:
        self.calls.append(("account", None))
        return {"assets": []}

    async def position_risk(self, symbol: str | None = None) -> tuple[dict[str, object], ...]:
        self.calls.append(("position", symbol))
        return ()

    async def position_side_mode(self) -> bool:
        self.calls.append(("position_mode", None))
        return False

    async def open_orders(self, symbol: str | None = None) -> tuple[dict[str, object], ...]:
        self.calls.append(("open", symbol))
        return ()

    async def get_order(self, symbol: str, **kwargs: object) -> dict[str, object]:
        self.calls.append(("get", (symbol, kwargs)))
        return {"symbol": symbol}

    async def submit_order(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(("submit", kwargs))
        return {"orderId": 1}

    async def cancel_order(self, symbol: str, **kwargs: object) -> dict[str, object]:
        self.calls.append(("cancel", (symbol, kwargs)))
        return {"status": "CANCELED"}

    async def cancel_all_orders(self, symbol: str) -> dict[str, object]:
        self.calls.append(("cancel_all", symbol))
        return {"code": 200}

    async def all_orders(self, symbol: str) -> tuple[dict[str, object], ...]:
        self.calls.append(("history", symbol))
        return ()

    async def account_trades(self, symbol: str) -> tuple[dict[str, object], ...]:
        self.calls.append(("trades", symbol))
        return ()


class BinanceFuturesExecutionServiceTests(IsolatedAsyncioTestCase):
    async def test_live_requires_guard_and_confirmation_for_order_changes(self) -> None:
        client = FakeFuturesClient()
        with self.assertRaises(BinanceConfigurationError):
            BinanceFuturesExecutionService(client, account_id="acct")

        guard = LiveTradingGuard(
            TradingMode.LIVE,
            allowed_accounts=("acct",),
            allowed_exchanges=("BINANCE",),
        )
        service = BinanceFuturesExecutionService(client, account_id="acct", guard=guard)
        with self.assertRaises(LiveTradingNotConfirmed):
            await service.submit_order(symbol="BTCUSDT", side="BUY", type="MARKET")
        guard.confirm_live_trading(LIVE_CONFIRMATION_PHRASE)
        result = await service.submit_order(symbol="BTCUSDT", side="BUY", type="MARKET")
        self.assertEqual(result["orderId"], 1)

    async def test_shadow_can_query_but_cannot_change_orders(self) -> None:
        client = FakeFuturesClient()
        guard = LiveTradingGuard(
            TradingMode.SHADOW,
            allowed_accounts=("acct",),
            allowed_exchanges=("BINANCE",),
        )
        service = BinanceFuturesExecutionService(client, account_id="acct", guard=guard)
        self.assertEqual(await service.account(), {"assets": []})
        with self.assertRaises(OperationNotAllowed):
            await service.cancel_order("BTCUSDT", order_id=1)

    async def test_position_side_mode_is_query_guarded(self) -> None:
        client = FakeFuturesClient()
        guard = LiveTradingGuard(
            TradingMode.SHADOW,
            allowed_accounts=("acct",),
            allowed_exchanges=("BINANCE",),
        )
        service = BinanceFuturesExecutionService(client, account_id="acct", guard=guard)
        self.assertFalse(await service.position_side_mode())
        self.assertIn(("position_mode", None), client.calls)

    async def test_reconcile_collects_account_and_symbol_history(self) -> None:
        client = FakeFuturesClient()
        guard = LiveTradingGuard(
            TradingMode.LIVE,
            allowed_accounts=("acct",),
            allowed_exchanges=("BINANCE",),
        )
        guard.confirm_live_trading(LIVE_CONFIRMATION_PHRASE)
        service = BinanceFuturesExecutionService(client, account_id="acct", guard=guard)
        result = await service.reconcile(("BTCUSDT", "ETHUSDT"))
        self.assertEqual(result["account"], {"assets": []})
        self.assertEqual([name for name, _ in client.calls], [
            "account", "position", "open", "history", "trades", "history", "trades"
        ])
