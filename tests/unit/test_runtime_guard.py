from datetime import UTC, datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, Mock

from gribuki_trade.domain.orders import OrderIntent, Side
from gribuki_trade.runtime import (
    LIVE_CONFIRMATION_PHRASE,
    AccountNotAllowed,
    BrokerOperation,
    ExchangeNotAllowed,
    GuardedBrokerAdapter,
    LiveTradingGuard,
    LiveTradingNotConfirmed,
    OperationNotAllowed,
    TradingMode,
    prompt_for_live_confirmation,
)


def make_order(account_id: str = "approved-account") -> OrderIntent:
    return OrderIntent(
        client_order_id="guard-1",
        account_id=account_id,
        strategy_id="guard-test",
        symbol="600000.SH",
        side=Side.BUY,
        quantity=100,
        limit_price=Decimal("10.01"),
        created_at=datetime(2026, 8, 13, tzinfo=UTC),
    )


class TradingModeTests(TestCase):
    def test_modes_are_distinct_string_values(self) -> None:
        self.assertEqual(
            {mode.value for mode in TradingMode},
            {"PAPER", "SHADOW", "LIVE"},
        )

    def test_default_guard_denies_real_submit(self) -> None:
        guard = LiveTradingGuard()

        self.assertIs(guard.mode, TradingMode.PAPER)
        with self.assertRaisesRegex(OperationNotAllowed, "PAPER"):
            guard.assert_broker_operation("SSE", "account", "submit_order")

    def test_shadow_allows_read_operations_but_denies_order_changes(self) -> None:
        guard = LiveTradingGuard(TradingMode.SHADOW)

        guard.assert_broker_operation("SSE", "account", BrokerOperation.QUERY)
        for operation in (
            BrokerOperation.SUBMIT_ORDER,
            BrokerOperation.CANCEL_ORDER,
            BrokerOperation.REPLACE_ORDER,
        ):
            with (
                self.subTest(operation=operation),
                self.assertRaisesRegex(OperationNotAllowed, "SHADOW"),
            ):
                guard.assert_broker_operation("SSE", "account", operation)

    def test_live_requires_confirmation_account_and_exchange_allowlists(self) -> None:
        guard = LiveTradingGuard(
            TradingMode.LIVE,
            allowed_accounts={"approved-account"},
            allowed_exchanges={"sse"},
        )

        with self.assertRaises(LiveTradingNotConfirmed):
            guard.assert_broker_operation("SSE", "approved-account", "submit_order")
        with self.assertRaises(LiveTradingNotConfirmed):
            guard.confirm_live_trading("almost right")

        guard.confirm_live_trading(LIVE_CONFIRMATION_PHRASE)
        with self.assertRaises(AccountNotAllowed):
            guard.assert_broker_operation("SSE", "other-account", "submit_order")
        with self.assertRaises(ExchangeNotAllowed):
            guard.assert_broker_operation("SZSE", "approved-account", "submit_order")

        guard.assert_broker_operation("SSE", "approved-account", "submit_order")

    def test_failed_confirmation_relocks_an_unlocked_guard(self) -> None:
        guard = LiveTradingGuard(
            TradingMode.LIVE,
            allowed_accounts={"approved-account"},
            allowed_exchanges={"SSE"},
        )
        guard.confirm_live_trading(LIVE_CONFIRMATION_PHRASE)

        with self.assertRaises(LiveTradingNotConfirmed):
            guard.confirm_live_trading("wrong")

        self.assertFalse(guard.live_confirmed)

    def test_prompt_uses_hidden_reader_and_does_not_store_phrase_in_repr(self) -> None:
        guard = LiveTradingGuard(TradingMode.LIVE)
        hidden_reader = Mock(return_value=LIVE_CONFIRMATION_PHRASE)

        self.assertTrue(prompt_for_live_confirmation(guard, getpass_fn=hidden_reader))
        self.assertTrue(guard.live_confirmed)
        self.assertNotIn(LIVE_CONFIRMATION_PHRASE, repr(guard))


class GuardedBrokerAdapterTests(IsolatedAsyncioTestCase):
    async def test_shadow_submit_never_calls_wrapped_real_adapter(self) -> None:
        broker = Mock()
        broker.submit_order = AsyncMock()
        guarded = GuardedBrokerAdapter(
            broker,
            LiveTradingGuard(TradingMode.SHADOW),
            exchange="SSE",
            account_id="approved-account",
        )

        with self.assertRaises(OperationNotAllowed):
            await guarded.submit_order(make_order())

        broker.submit_order.assert_not_awaited()

    async def test_live_submit_calls_adapter_only_after_all_checks_pass(self) -> None:
        broker = Mock()
        broker.submit_order = AsyncMock()
        guard = LiveTradingGuard(
            TradingMode.LIVE,
            allowed_accounts={"approved-account"},
            allowed_exchanges={"SSE"},
        )
        guarded = GuardedBrokerAdapter(
            broker,
            guard,
            exchange="SSE",
            account_id="approved-account",
        )
        order = make_order()

        guard.confirm_live_trading(LIVE_CONFIRMATION_PHRASE)
        await guarded.submit_order(order)

        broker.submit_order.assert_awaited_once_with(order)

    async def test_order_account_must_match_guarded_account(self) -> None:
        broker = Mock()
        broker.submit_order = AsyncMock()
        guard = LiveTradingGuard(
            TradingMode.LIVE,
            allowed_accounts={"approved-account", "other-account"},
            allowed_exchanges={"SSE"},
        )
        guard.confirm_live_trading(LIVE_CONFIRMATION_PHRASE)
        guarded = GuardedBrokerAdapter(
            broker,
            guard,
            exchange="SSE",
            account_id="approved-account",
        )

        with self.assertRaises(AccountNotAllowed):
            await guarded.submit_order(make_order("other-account"))

        broker.submit_order.assert_not_awaited()
