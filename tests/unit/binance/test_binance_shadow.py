import asyncio
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase

from gribuki_trade.adapters.binance import BinanceEnvironment
from gribuki_trade.adapters.binance.spot.stream import (
    BinanceBookTickerEvent,
    BinanceKlineEvent,
    BinanceMarketEvent,
)
from gribuki_trade.adapters.simulated.paper_account import PaperSpotAccount, SpotSymbolAssets
from gribuki_trade.backtest.crypto import CryptoBar
from gribuki_trade.domain.orders import OrderIntent, Side
from gribuki_trade.services.binance.binance_shadow import (
    BinanceShadowConfig,
    BinanceShadowSession,
    ShadowMarketIntegrityError,
    ShadowTermination,
)
from gribuki_trade.strategy.crypto_trend import CryptoTrendConfig
from gribuki_trade.trading import BalanceValue, SQLiteOrderManagementStore

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
BASE = NOW - timedelta(minutes=4)


class FakeMarket:
    def __init__(self, *events: BinanceMarketEvent) -> None:
        self._events = deque(events)

    async def events(self):  # type: ignore[no-untyped-def]
        while self._events:
            yield self._events.popleft()


class BlockingMarket:
    async def events(self):  # type: ignore[no-untyped-def]
        await asyncio.Event().wait()
        if False:  # pragma: no cover - make this an async generator
            yield _book(1)


def _book(update_id: int, *, bid: str = "100", ask: str = "101") -> BinanceBookTickerEvent:
    return BinanceBookTickerEvent(
        stream="btcusdt@bookTicker",
        symbol="BTCUSDT",
        update_id=update_id,
        bid_price=Decimal(bid),
        bid_quantity=Decimal("10"),
        ask_price=Decimal(ask),
        ask_quantity=Decimal("10"),
        event_time_ms=int(NOW.timestamp() * 1_000),
    )


def _bar(index: int, close: str, *, stale: bool = False) -> BinanceKlineEvent:
    start = BASE + timedelta(minutes=index)
    start_ms = int(start.timestamp() * 1_000)
    close_value = Decimal(close)
    event_time = NOW - timedelta(seconds=10) if stale else NOW
    return BinanceKlineEvent(
        stream="btcusdt@kline_1m",
        symbol="BTCUSDT",
        event_time_ms=int(event_time.timestamp() * 1_000),
        start_time_ms=start_ms,
        close_time_ms=start_ms + 59_999,
        interval="1m",
        first_trade_id=index * 2,
        last_trade_id=index * 2 + 1,
        open=close_value,
        close=close_value,
        high=close_value + 1,
        low=close_value - 1,
        volume=Decimal("10"),
        trade_count=2,
        is_closed=True,
        quote_volume=close_value * 10,
        taker_buy_base_volume=Decimal("4"),
        taker_buy_quote_volume=close_value * 4,
    )


def _config() -> BinanceShadowConfig:
    return BinanceShadowConfig(
        initial_balances={"BTC": "0", "ETH": "0", "USDT": "100"},
        minimum_order_quantity=Decimal("0.001"),
        quantity_step=Decimal("0.001"),
        minimum_order_notional=Decimal("1"),
        maximum_order_notional=Decimal("100"),
        aggressive_limit_offset_bps=Decimal("1"),
        maximum_market_age_seconds=Decimal("5"),
        history_capacity=10,
    )


def _trend() -> CryptoTrendConfig:
    return CryptoTrendConfig(
        fast_window=1,
        slow_window=2,
        minimum_history=2,
        target_position_fraction=Decimal("0.5"),
        quantity_step=Decimal("0.001"),
        minimum_order_quantity=Decimal("0.001"),
    )


def _seed(index: int, close: str) -> CryptoBar:
    event = _bar(index, close)
    return CryptoBar(
        symbol=event.symbol,
        open_time=datetime.fromtimestamp(event.start_time_ms / 1_000, tz=UTC),
        close_time=datetime.fromtimestamp((event.close_time_ms + 1) / 1_000, tz=UTC),
        available_at=datetime.fromtimestamp((event.close_time_ms + 1) / 1_000, tz=UTC),
        open=event.open,
        high=event.high,
        low=event.low,
        close=event.close,
        volume=event.volume,
    )


class BinanceShadowSessionTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._temporary = TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)

    def _store(self, name: str) -> SQLiteOrderManagementStore:
        store = SQLiteOrderManagementStore(Path(self._temporary.name) / name)
        self.addCleanup(store.close)
        return store

    async def test_closed_bar_bound_runs_strategy_and_local_fill(self) -> None:
        oms = self._store("bounded.sqlite3")
        session = BinanceShadowSession(
            FakeMarket(_book(1), _bar(0, "100"), _bar(1, "102"), _book(2), _bar(2, "103")),
            oms,
            environment=BinanceEnvironment.LIVE,
            config=_config(),
            trend_config=_trend(),
            clock=lambda: NOW,
        )

        report = await session.run(maximum_closed_bars=3)

        self.assertIs(report.termination, ShadowTermination.MAXIMUM_CLOSED_BARS)
        self.assertFalse(report.watermark.remote_order_submission_enabled)
        self.assertIn("NO_REMOTE_ORDERS", report.watermark.label)
        self.assertEqual(report.paper_engine.submitted_orders, 1)
        self.assertEqual(report.paper_engine.fill_count, 1)
        self.assertEqual(len(oms.fills()), 1)
        self.assertGreater(session.account.balance("BTC").free, 0)
        self.assertEqual(session.account.balance("USDT").locked, 0)

    async def test_gap_and_stale_data_fail_closed(self) -> None:
        gap_session = BinanceShadowSession(
            FakeMarket(_book(1), _bar(0, "100"), _bar(2, "102")),
            self._store("gap.sqlite3"),
            config=_config(),
            trend_config=_trend(),
            clock=lambda: NOW,
        )
        with self.assertRaisesRegex(ShadowMarketIntegrityError, "closed-kline gap"):
            await gap_session.run()
        assert gap_session.statistics is not None
        self.assertIs(gap_session.statistics.termination, ShadowTermination.FAILED_CLOSED)
        self.assertEqual(gap_session.statistics.detected_bar_gaps, 1)

        stale_session = BinanceShadowSession(
            FakeMarket(_bar(0, "100", stale=True)),
            self._store("stale.sqlite3"),
            config=_config(),
            trend_config=_trend(),
            clock=lambda: NOW,
        )
        with self.assertRaisesRegex(ShadowMarketIntegrityError, "stale"):
            await stale_session.run()
        assert stale_session.statistics is not None
        self.assertEqual(stale_session.statistics.stale_market_events, 1)

    async def test_restart_restores_open_order_and_account_reservation(self) -> None:
        database = Path(self._temporary.name) / "restart.sqlite3"
        first_oms = SQLiteOrderManagementStore(database)
        first = BinanceShadowSession(
            FakeMarket(_book(1), _bar(0, "100"), _bar(1, "102")),
            first_oms,
            config=_config(),
            trend_config=_trend(),
            clock=lambda: NOW,
        )
        first_report = await first.run()
        self.assertEqual(first_report.paper_engine.submitted_orders, 1)
        self.assertGreater(first.account.balance("USDT").locked, 0)
        first_oms.close()

        second_oms = SQLiteOrderManagementStore(database)
        self.addCleanup(second_oms.close)
        second = BinanceShadowSession(
            FakeMarket(_book(2)),
            second_oms,
            config=_config(),
            trend_config=_trend(),
            clock=lambda: NOW,
        )

        second_report = await second.run()

        self.assertEqual(second_report.recovered_open_orders, 1)
        self.assertEqual(len(second_oms.fills()), 1)
        self.assertEqual(second.account.balance("USDT").locked, 0)
        self.assertGreater(second.account.balance("BTC").free, 0)

    async def test_external_stop_interrupts_blocked_market_source(self) -> None:
        stop = asyncio.Event()
        session = BinanceShadowSession(
            BlockingMarket(),
            self._store("stop.sqlite3"),
            config=_config(),
            trend_config=_trend(),
            clock=lambda: NOW,
        )

        running = asyncio.create_task(session.run(stop_event=stop))
        await asyncio.sleep(0)
        stop.set()
        report = await asyncio.wait_for(running, timeout=1)

        self.assertIs(report.termination, ShadowTermination.EXTERNAL_STOP)
        self.assertEqual(report.paper_engine.processed_market_events, 0)

    async def test_rest_seed_duplicate_is_ignored_before_next_closed_bar(self) -> None:
        session = BinanceShadowSession(
            FakeMarket(_book(1), _bar(0, "100"), _bar(1, "102")),
            self._store("seed.sqlite3"),
            config=_config(),
            trend_config=_trend(),
            initial_history=(_seed(0, "100"),),
            clock=lambda: NOW,
        )

        report = await session.run(maximum_closed_bars=1)

        self.assertEqual(report.ignored_duplicate_closed_bars, 1)
        self.assertEqual(report.paper_engine.processed_closed_bars, 1)
        self.assertIs(report.termination, ShadowTermination.MAXIMUM_CLOSED_BARS)

    async def test_in_flight_outbox_is_hydrated_and_reconciled_after_restart(self) -> None:
        database = Path(self._temporary.name) / "unknown.sqlite3"
        order = OrderIntent(
            client_order_id="crash-before-paper-submit",
            account_id="binance-shadow",
            strategy_id="crypto-trend-shadow-v1",
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=Decimal("0.1"),
            limit_price=Decimal("100"),
            created_at=NOW,
        )
        account = PaperSpotAccount(
            account_id="binance-shadow",
            initial_balances={"BTC": "0", "ETH": "0", "USDT": "100"},
            symbol_assets={"BTCUSDT": SpotSymbolAssets("BTC", "USDT")},
        )
        account.reserve_order(order)
        before = SQLiteOrderManagementStore(database)
        before.record_balance_snapshot(
            account.account_id,
            tuple(
                BalanceValue(item.asset, item.free, item.locked)
                for item in account.balances()
            ),
            event_id="locked-before-crash",
            occurred_at=NOW,
        )
        before.create_order(order)
        before.claim_command(f"submit:{order.client_order_id}", now=NOW)
        before.close()

        after = SQLiteOrderManagementStore(database)
        self.addCleanup(after.close)
        session = BinanceShadowSession(
            FakeMarket(_book(1, bid="98", ask="99")),
            after,
            config=_config(),
            trend_config=_trend(),
            clock=lambda: NOW,
        )

        report = await session.run()

        self.assertEqual(report.recovered_open_orders, 1)
        self.assertEqual(after.require_order(order.client_order_id).status.value, "FILLED")
        self.assertEqual(len(after.fills()), 1)
