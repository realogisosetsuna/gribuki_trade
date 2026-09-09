from collections import deque
from decimal import Decimal
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase

from gribuki_trade.adapters.binance import (
    BinanceEnvironment,
    BinanceKlineArchive,
    BinanceKlineCollector,
    Kline,
    KlineArchiveError,
)


def bar(open_time_ms: int, close: str = "101") -> Kline:
    return Kline(
        open_time_ms=open_time_ms,
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=Decimal(close),
        volume=Decimal("10"),
        close_time_ms=open_time_ms + 59_999,
        quote_volume=Decimal("1005"),
        trade_count=7,
        taker_buy_base_volume=Decimal("4"),
        taker_buy_quote_volume=Decimal("402"),
    )


class FakeKlineSource:
    def __init__(self, *pages: tuple[Kline, ...]) -> None:
        self.pages = deque(pages)
        self.calls: list[tuple[str, str, int, int | None, int | None]] = []

    async def klines(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> tuple[Kline, ...]:
        self.calls.append((symbol, interval, limit, start_time_ms, end_time_ms))
        return self.pages.popleft() if self.pages else ()


class BinanceKlineArchiveTests(TestCase):
    def test_archive_is_immutable_replayable_and_fingerprinted(self) -> None:
        path = Path(self._testMethodName + ".sqlite3")
        self.addCleanup(path.unlink, missing_ok=True)
        self.addCleanup(path.with_suffix(".sqlite3-wal").unlink, missing_ok=True)
        self.addCleanup(path.with_suffix(".sqlite3-shm").unlink, missing_ok=True)
        bars = (bar(0), bar(60_000, "102"))

        with BinanceKlineArchive(path) as archive:
            self.assertEqual(archive.append("testnet", "btcusdt", "1m", bars), 2)
            first = archive.dataset("TESTNET", "BTCUSDT", "1m")
            self.assertEqual(archive.append("TESTNET", "BTCUSDT", "1m", bars), 0)
            self.assertTrue(archive.integrity("TESTNET", "BTCUSDT", "1m").complete)

        with BinanceKlineArchive(path) as reopened:
            second = reopened.dataset(BinanceEnvironment.TESTNET, "BTCUSDT", "1m")
            self.assertEqual(first, second)
            self.assertEqual(reopened.load("TESTNET", "BTCUSDT", "1m"), bars)

    def test_conflicting_revision_and_gap_are_visible(self) -> None:
        path = Path(self._testMethodName + ".sqlite3")
        self.addCleanup(path.unlink, missing_ok=True)
        with BinanceKlineArchive(path) as archive:
            archive.append("TESTNET", "BTCUSDT", "1m", (bar(0), bar(120_000)))
            report = archive.integrity("TESTNET", "BTCUSDT", "1m")

            self.assertFalse(report.complete)
            self.assertEqual(report.gaps[0].expected_open_time_ms, 60_000)
            with self.assertRaisesRegex(KlineArchiveError, "revision"):
                archive.append("TESTNET", "BTCUSDT", "1m", (bar(0, "100.5"),))


class BinanceKlineCollectorTests(IsolatedAsyncioTestCase):
    async def test_collector_paginates_and_never_archives_open_bar(self) -> None:
        path = Path(self._testMethodName + ".sqlite3")
        self.addCleanup(path.unlink, missing_ok=True)
        source = FakeKlineSource(
            (bar(0), bar(60_000)),
            (bar(120_000), bar(180_000)),
        )
        with BinanceKlineArchive(path) as archive:
            collector = BinanceKlineCollector(
                source,
                archive,
                environment="TESTNET",
                clock_ms=lambda: 200_000,
            )

            result = await collector.sync(
                "btcusdt",
                "1m",
                start_time_ms=0,
                batch_size=2,
            )

            self.assertEqual(result.requested_pages, 2)
            self.assertEqual(result.received_rows, 4)
            self.assertEqual(result.inserted_rows, 3)
            self.assertEqual(result.skipped_open_rows, 1)
            self.assertEqual(result.dataset.row_count, 3)
            self.assertTrue(result.integrity.complete)
            self.assertEqual(source.calls[1][3], 120_000)

    async def test_collector_rejects_out_of_order_provider_page(self) -> None:
        path = Path(self._testMethodName + ".sqlite3")
        self.addCleanup(path.unlink, missing_ok=True)
        source = FakeKlineSource((bar(60_000), bar(0)))
        with BinanceKlineArchive(path) as archive:
            collector = BinanceKlineCollector(
                source,
                archive,
                environment="TESTNET",
                clock_ms=lambda: 1_000_000,
            )

            with self.assertRaisesRegex(KlineArchiveError, "ordered"):
                await collector.sync("BTCUSDT", "1m", start_time_ms=0)
