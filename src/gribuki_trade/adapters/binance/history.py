"""Point-in-time-safe Binance kline collection and immutable local archive."""

from __future__ import annotations

import hashlib
import sqlite3
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .models import BinanceEnvironment, Kline
from .stream import KLINE_INTERVALS, normalize_symbol


class BinanceKlineSource(Protocol):
    async def klines(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> tuple[Kline, ...]: ...


class KlineArchiveError(RuntimeError):
    """Historical market data violated an immutability or ordering invariant."""


@dataclass(frozen=True, slots=True)
class KlineGap:
    expected_open_time_ms: int
    next_open_time_ms: int


@dataclass(frozen=True, slots=True)
class KlineIntegrityReport:
    row_count: int
    first_open_time_ms: int | None
    last_close_time_ms: int | None
    gaps: tuple[KlineGap, ...]

    @property
    def complete(self) -> bool:
        return self.row_count > 0 and not self.gaps


@dataclass(frozen=True, slots=True)
class KlineDataset:
    environment: BinanceEnvironment
    symbol: str
    interval: str
    row_count: int
    first_open_time_ms: int | None
    last_close_time_ms: int | None
    sha256: str


@dataclass(frozen=True, slots=True)
class KlineSyncResult:
    requested_pages: int
    received_rows: int
    inserted_rows: int
    skipped_open_rows: int
    dataset: KlineDataset
    integrity: KlineIntegrityReport


_FIXED_INTERVAL_MS = {
    "1s": 1_000,
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
}


class BinanceKlineArchive:
    """SQLite WAL archive that never overwrites conflicting historical bars."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self._path)
        self._connection.row_factory = sqlite3.Row
        self._closed = False
        self._initialize()

    def _initialize(self) -> None:
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS binance_klines (
                environment TEXT NOT NULL,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                open_time_ms INTEGER NOT NULL,
                open TEXT NOT NULL,
                high TEXT NOT NULL,
                low TEXT NOT NULL,
                close TEXT NOT NULL,
                volume TEXT NOT NULL,
                close_time_ms INTEGER NOT NULL,
                quote_volume TEXT NOT NULL,
                trade_count INTEGER NOT NULL,
                taker_buy_base_volume TEXT NOT NULL,
                taker_buy_quote_volume TEXT NOT NULL,
                PRIMARY KEY (environment, symbol, interval, open_time_ms)
            ) WITHOUT ROWID
            """
        )
        self._connection.commit()

    def append(
        self,
        environment: BinanceEnvironment | str,
        symbol: str,
        interval: str,
        bars: Sequence[Kline],
    ) -> int:
        """Atomically append bars, accepting exact replays but rejecting revisions."""

        selected = _environment(environment)
        normalized_symbol = normalize_symbol(symbol)
        normalized_interval = _interval(interval)
        _validate_page(bars)
        inserted = 0
        with self._connection:
            for bar in bars:
                values = _bar_values(selected, normalized_symbol, normalized_interval, bar)
                cursor = self._connection.execute(
                    """
                    INSERT OR IGNORE INTO binance_klines VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    values,
                )
                if cursor.rowcount == 1:
                    inserted += 1
                    continue
                existing = self._connection.execute(
                    """
                    SELECT environment, symbol, interval, open_time_ms, open, high, low,
                           close, volume, close_time_ms, quote_volume, trade_count,
                           taker_buy_base_volume, taker_buy_quote_volume
                    FROM binance_klines
                    WHERE environment = ? AND symbol = ? AND interval = ? AND open_time_ms = ?
                    """,
                    (selected.value, normalized_symbol, normalized_interval, bar.open_time_ms),
                ).fetchone()
                if existing is None or tuple(existing) != values:
                    raise KlineArchiveError(
                        f"historical kline revision for {normalized_symbol} "
                        f"{normalized_interval} at {bar.open_time_ms}"
                    )
        return inserted

    def load(
        self,
        environment: BinanceEnvironment | str,
        symbol: str,
        interval: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> tuple[Kline, ...]:
        selected = _environment(environment)
        normalized_symbol = normalize_symbol(symbol)
        normalized_interval = _interval(interval)
        clauses = ["environment = ?", "symbol = ?", "interval = ?"]
        params: list[object] = [selected.value, normalized_symbol, normalized_interval]
        if start_time_ms is not None:
            clauses.append("open_time_ms >= ?")
            params.append(_non_negative_time(start_time_ms, "start_time_ms"))
        if end_time_ms is not None:
            clauses.append("open_time_ms <= ?")
            params.append(_non_negative_time(end_time_ms, "end_time_ms"))
        if (
            start_time_ms is not None
            and end_time_ms is not None
            and end_time_ms < start_time_ms
        ):
            raise ValueError("end_time_ms must not precede start_time_ms")
        rows = self._connection.execute(
            """
            SELECT open_time_ms, open, high, low, close, volume, close_time_ms,
                   quote_volume, trade_count, taker_buy_base_volume,
                   taker_buy_quote_volume
            FROM binance_klines
            WHERE """
            + " AND ".join(clauses)
            + " ORDER BY open_time_ms",
            tuple(params),
        ).fetchall()
        return tuple(_row_to_kline(row) for row in rows)

    def latest_open_time_ms(
        self,
        environment: BinanceEnvironment | str,
        symbol: str,
        interval: str,
    ) -> int | None:
        selected = _environment(environment)
        row = self._connection.execute(
            """
            SELECT MAX(open_time_ms)
            FROM binance_klines
            WHERE environment = ? AND symbol = ? AND interval = ?
            """,
            (selected.value, normalize_symbol(symbol), _interval(interval)),
        ).fetchone()
        value = None if row is None else row[0]
        return int(value) if value is not None else None

    def integrity(
        self,
        environment: BinanceEnvironment | str,
        symbol: str,
        interval: str,
    ) -> KlineIntegrityReport:
        normalized_interval = _interval(interval)
        bars = self.load(environment, symbol, normalized_interval)
        gaps: list[KlineGap] = []
        fixed_ms = _FIXED_INTERVAL_MS.get(normalized_interval)
        if fixed_ms is not None:
            for previous, current in zip(bars, bars[1:], strict=False):
                expected = previous.open_time_ms + fixed_ms
                if current.open_time_ms != expected:
                    gaps.append(KlineGap(expected, current.open_time_ms))
        return KlineIntegrityReport(
            row_count=len(bars),
            first_open_time_ms=bars[0].open_time_ms if bars else None,
            last_close_time_ms=bars[-1].close_time_ms if bars else None,
            gaps=tuple(gaps),
        )

    def dataset(
        self,
        environment: BinanceEnvironment | str,
        symbol: str,
        interval: str,
    ) -> KlineDataset:
        selected = _environment(environment)
        normalized_symbol = normalize_symbol(symbol)
        normalized_interval = _interval(interval)
        bars = self.load(selected, normalized_symbol, normalized_interval)
        digest = hashlib.sha256()
        for bar in bars:
            digest.update("\x1f".join(_canonical_bar(bar)).encode("ascii"))
            digest.update(b"\n")
        return KlineDataset(
            environment=selected,
            symbol=normalized_symbol,
            interval=normalized_interval,
            row_count=len(bars),
            first_open_time_ms=bars[0].open_time_ms if bars else None,
            last_close_time_ms=bars[-1].close_time_ms if bars else None,
            sha256=digest.hexdigest(),
        )

    def close(self) -> None:
        if not self._closed:
            self._connection.close()
            self._closed = True

    def __enter__(self) -> BinanceKlineArchive:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


class BinanceKlineCollector:
    """Paginate completed REST klines into an immutable archive."""

    def __init__(
        self,
        source: BinanceKlineSource,
        archive: BinanceKlineArchive,
        *,
        environment: BinanceEnvironment | str,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._source = source
        self._archive = archive
        self._environment = _environment(environment)
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    async def sync(
        self,
        symbol: str,
        interval: str,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        batch_size: int = 1_000,
        max_pages: int = 10_000,
    ) -> KlineSyncResult:
        normalized_symbol = normalize_symbol(symbol)
        normalized_interval = _interval(interval)
        cursor = _non_negative_time(start_time_ms, "start_time_ms")
        if end_time_ms is not None:
            _non_negative_time(end_time_ms, "end_time_ms")
            if end_time_ms < cursor:
                raise ValueError("end_time_ms must not precede start_time_ms")
        if not 1 <= batch_size <= 1_000:
            raise ValueError("batch_size must be between 1 and 1000")
        if max_pages <= 0:
            raise ValueError("max_pages must be positive")

        pages = received = inserted = skipped_open = 0
        stopped_by_source = False
        as_of_ms = self._clock_ms()
        while pages < max_pages and (end_time_ms is None or cursor <= end_time_ms):
            page = await self._source.klines(
                normalized_symbol,
                normalized_interval,
                limit=batch_size,
                start_time_ms=cursor,
                end_time_ms=end_time_ms,
            )
            pages += 1
            if not page:
                stopped_by_source = True
                break
            _validate_page(page)
            received += len(page)
            completed = tuple(bar for bar in page if bar.close_time_ms < as_of_ms)
            skipped_open += len(page) - len(completed)
            if completed:
                inserted += self._archive.append(
                    self._environment,
                    normalized_symbol,
                    normalized_interval,
                    completed,
                )
            next_cursor = page[-1].close_time_ms + 1
            if next_cursor <= cursor:
                raise KlineArchiveError("Binance kline pagination made no progress")
            cursor = next_cursor
            if len(page) < batch_size or skipped_open:
                stopped_by_source = True
                break
        if (
            pages == max_pages
            and not stopped_by_source
            and (end_time_ms is None or cursor <= end_time_ms)
        ):
            raise KlineArchiveError("Binance kline pagination limit exhausted")

        return KlineSyncResult(
            requested_pages=pages,
            received_rows=received,
            inserted_rows=inserted,
            skipped_open_rows=skipped_open,
            dataset=self._archive.dataset(
                self._environment, normalized_symbol, normalized_interval
            ),
            integrity=self._archive.integrity(
                self._environment, normalized_symbol, normalized_interval
            ),
        )


def _environment(value: BinanceEnvironment | str) -> BinanceEnvironment:
    if isinstance(value, BinanceEnvironment):
        return value
    try:
        return BinanceEnvironment(str(value).upper())
    except ValueError:
        raise ValueError(f"unsupported Binance environment: {value!r}") from None


def _interval(value: str) -> str:
    if value not in KLINE_INTERVALS:
        raise ValueError(f"unsupported Binance kline interval: {value!r}")
    return value


def _non_negative_time(value: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _validate_page(bars: Sequence[Kline]) -> None:
    opens = [bar.open_time_ms for bar in bars]
    if opens != sorted(opens) or len(opens) != len(set(opens)):
        raise KlineArchiveError("Binance klines must be strictly ordered and unique")
    for bar in bars:
        if bar.open_time_ms < 0 or bar.close_time_ms < bar.open_time_ms:
            raise KlineArchiveError("Binance kline timestamps are invalid")
        if min(bar.open, bar.high, bar.low, bar.close) <= 0:
            raise KlineArchiveError("Binance kline prices must be positive")
        if bar.high < max(bar.open, bar.close) or bar.low > min(bar.open, bar.close):
            raise KlineArchiveError("Binance kline OHLC values are inconsistent")
        if min(
            bar.volume,
            bar.quote_volume,
            bar.taker_buy_base_volume,
            bar.taker_buy_quote_volume,
        ) < 0:
            raise KlineArchiveError("Binance kline volumes must be non-negative")


def _canonical_bar(bar: Kline) -> tuple[str, ...]:
    return (
        str(bar.open_time_ms),
        format(bar.open, "f"),
        format(bar.high, "f"),
        format(bar.low, "f"),
        format(bar.close, "f"),
        format(bar.volume, "f"),
        str(bar.close_time_ms),
        format(bar.quote_volume, "f"),
        str(bar.trade_count),
        format(bar.taker_buy_base_volume, "f"),
        format(bar.taker_buy_quote_volume, "f"),
    )


def _bar_values(
    environment: BinanceEnvironment,
    symbol: str,
    interval: str,
    bar: Kline,
) -> tuple[object, ...]:
    return (
        environment.value,
        symbol,
        interval,
        bar.open_time_ms,
        format(bar.open, "f"),
        format(bar.high, "f"),
        format(bar.low, "f"),
        format(bar.close, "f"),
        format(bar.volume, "f"),
        bar.close_time_ms,
        format(bar.quote_volume, "f"),
        bar.trade_count,
        format(bar.taker_buy_base_volume, "f"),
        format(bar.taker_buy_quote_volume, "f"),
    )


def _row_to_kline(row: sqlite3.Row) -> Kline:
    from decimal import Decimal

    return Kline(
        open_time_ms=int(row["open_time_ms"]),
        open=Decimal(str(row["open"])),
        high=Decimal(str(row["high"])),
        low=Decimal(str(row["low"])),
        close=Decimal(str(row["close"])),
        volume=Decimal(str(row["volume"])),
        close_time_ms=int(row["close_time_ms"]),
        quote_volume=Decimal(str(row["quote_volume"])),
        trade_count=int(row["trade_count"]),
        taker_buy_base_volume=Decimal(str(row["taker_buy_base_volume"])),
        taker_buy_quote_volume=Decimal(str(row["taker_buy_quote_volume"])),
    )
