"""Binance Spot/USDⓈ-M 本地订单簿快照与增量恢复。

本模块实现官方 ``depth`` 流的快照接入规则。WebSocket 断线、序列缺口或快照
无法与缓冲事件衔接时，订单簿会失效并回到等待快照状态；调用方必须重新读取
REST ``depth`` 快照后才能继续把本地价格作为策略输入。
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Literal, Protocol

from .models import OrderBookLevel, OrderBookSnapshot


class OrderBookRecoveryState(StrEnum):
    """本地订单簿的同步状态。"""

    WAITING_SNAPSHOT = "WAITING_SNAPSHOT"
    BUFFERING = "BUFFERING"
    SYNCED = "SYNCED"
    DESYNCED = "DESYNCED"


class DepthEvent(Protocol):
    symbol: str
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int | None
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]


@dataclass(frozen=True, slots=True)
class OrderBookRecoveryResult:
    """应用一帧增量后的结果，供监控器记录缺口和恢复次数。"""

    state: OrderBookRecoveryState
    applied: bool
    buffered: bool
    gap_detected: bool
    last_update_id: int | None


@dataclass(frozen=True, slots=True)
class LocalOrderBookView:
    """策略读取的不可变订单簿视图。"""

    symbol: str
    state: OrderBookRecoveryState
    last_update_id: int | None
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]


SnapshotFetcher = Callable[[str], Awaitable[OrderBookSnapshot]]


class BinanceLocalOrderBook:
    """按 Spot 或 USD-M 官方序列规则维护单个交易对订单簿。

    ``venue="SPOT"`` 使用 ``U <= last + 1 <= u``；``venue="USDS_FUTURES"``
    在第一帧使用 ``U <= snapshot_id <= u``，后续帧还必须满足 ``pu == last``。
    """

    def __init__(
        self,
        symbol: str,
        *,
        venue: Literal["SPOT", "USDS_FUTURES"],
        max_buffered_events: int = 10_000,
    ) -> None:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must not be blank")
        if venue not in {"SPOT", "USDS_FUTURES"}:
            raise ValueError("venue must be SPOT or USDS_FUTURES")
        if max_buffered_events <= 0:
            raise ValueError("max_buffered_events must be positive")
        self.symbol = symbol.upper()
        self.venue = venue
        self._max_buffered_events = max_buffered_events
        self._state = OrderBookRecoveryState.WAITING_SNAPSHOT
        self._last_update_id: int | None = None
        self._needs_first_event = False
        self._buffer_overflowed = False
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._buffer: OrderedDict[tuple[int, int], DepthEvent] = OrderedDict()

    @property
    def state(self) -> OrderBookRecoveryState:
        return self._state

    @property
    def last_update_id(self) -> int | None:
        return self._last_update_id

    @property
    def synced(self) -> bool:
        return self._state is OrderBookRecoveryState.SYNCED

    def view(self) -> LocalOrderBookView:
        return LocalOrderBookView(
            symbol=self.symbol,
            state=self._state,
            last_update_id=self._last_update_id,
            bids=tuple(
                OrderBookLevel(price=price, quantity=quantity)
                for price, quantity in sorted(self._bids.items(), reverse=True)
                if quantity > 0
            ),
            asks=tuple(
                OrderBookLevel(price=price, quantity=quantity)
                for price, quantity in sorted(self._asks.items())
                if quantity > 0
            ),
        )

    def reset(self) -> None:
        """丢弃本地状态，等待下一次 REST 快照。"""

        self._state = OrderBookRecoveryState.WAITING_SNAPSHOT
        self._last_update_id = None
        self._needs_first_event = False
        self._buffer_overflowed = False
        self._bids.clear()
        self._asks.clear()
        self._buffer.clear()

    async def bootstrap(self, fetch_snapshot: SnapshotFetcher) -> bool:
        """读取并应用 REST 快照；返回是否成功衔接此前缓冲事件。"""

        if self._buffer_overflowed:
            self.reset()
        snapshot = await fetch_snapshot(self.symbol)
        return self.apply_snapshot(snapshot)

    def ingest(self, event: DepthEvent) -> OrderBookRecoveryResult:
        """接收一帧深度增量，缺口时立即使本地簿失效。"""

        self._validate_event(event)
        if self._state is not OrderBookRecoveryState.SYNCED:
            self._buffer_event(event)
            if self._state is OrderBookRecoveryState.WAITING_SNAPSHOT:
                self._state = OrderBookRecoveryState.BUFFERING
            return OrderBookRecoveryResult(
                self._state, False, True, False, self._last_update_id
            )

        assert self._last_update_id is not None
        if self._needs_first_event:
            contiguous = self._is_first_snapshot_event(event, self._last_update_id)
        else:
            contiguous = self._is_contiguous(event, self._last_update_id)
        if not contiguous:
            self._state = OrderBookRecoveryState.DESYNCED
            self._bids.clear()
            self._asks.clear()
            self._buffer_event(event)
            return OrderBookRecoveryResult(
                self._state, False, True, True, self._last_update_id
            )
        self._apply_event(event)
        return OrderBookRecoveryResult(
            self._state, True, False, False, self._last_update_id
        )

    def apply_snapshot(self, snapshot: OrderBookSnapshot) -> bool:
        """应用 REST 快照并回放事件缓冲区。"""

        if self._buffer_overflowed:
            self._state = OrderBookRecoveryState.DESYNCED
            self._bids.clear()
            self._asks.clear()
            return False
        if snapshot.symbol.upper() != self.symbol:
            raise ValueError("snapshot symbol does not match local order book")
        if snapshot.last_update_id < 0:
            raise ValueError("snapshot last_update_id must be non-negative")
        bids, asks = self._levels_from_snapshot(snapshot)
        self._bids, self._asks = bids, asks
        self._last_update_id = snapshot.last_update_id
        self._needs_first_event = True
        self._state = OrderBookRecoveryState.SYNCED
        buffered = tuple(self._buffer.values())
        self._buffer.clear()
        first_event = True
        for event in buffered:
            if event.final_update_id <= self._last_update_id:
                continue
            valid = (
                self._is_first_snapshot_event(event, self._last_update_id)
                if first_event
                else self._is_contiguous(event, self._last_update_id)
            )
            if not valid:
                self._state = OrderBookRecoveryState.DESYNCED
                self._bids.clear()
                self._asks.clear()
                self._buffer_event(event)
                return False
            self._apply_event(event)
            first_event = False
        return True

    def _validate_event(self, event: DepthEvent) -> None:
        if event.symbol.upper() != self.symbol:
            raise ValueError("depth event symbol does not match local order book")
        if event.first_update_id < 0 or event.final_update_id < event.first_update_id:
            raise ValueError("depth update sequence is malformed")
        if (
            self.venue == "USDS_FUTURES"
            and event.previous_final_update_id is not None
            and event.previous_final_update_id >= event.final_update_id
        ):
            raise ValueError("Futures depth previous sequence is malformed")
        for levels in (event.bids, event.asks):
            for price, quantity in levels:
                if price <= 0 or quantity < 0:
                    raise ValueError(
                        "depth levels must have positive prices and non-negative quantities"
                    )

    def _buffer_event(self, event: DepthEvent) -> None:
        key = (event.first_update_id, event.final_update_id)
        self._buffer[key] = event
        while len(self._buffer) > self._max_buffered_events:
            self._buffer_overflowed = True
            self._state = OrderBookRecoveryState.DESYNCED
            self._bids.clear()
            self._asks.clear()
            self._buffer.clear()
            return

    def _is_first_snapshot_event(self, event: DepthEvent, snapshot_id: int) -> bool:
        if self.venue == "SPOT":
            return event.first_update_id <= snapshot_id + 1 <= event.final_update_id
        return event.first_update_id <= snapshot_id <= event.final_update_id

    def _is_contiguous(self, event: DepthEvent, previous: int) -> bool:
        if not event.first_update_id <= previous + 1 <= event.final_update_id:
            return False
        if self.venue == "USDS_FUTURES":
            return event.previous_final_update_id == previous
        return True

    def _apply_event(self, event: DepthEvent) -> None:
        for price, quantity in event.bids:
            if quantity == 0:
                self._bids.pop(price, None)
            else:
                self._bids[price] = quantity
        for price, quantity in event.asks:
            if quantity == 0:
                self._asks.pop(price, None)
            else:
                self._asks[price] = quantity
        self._last_update_id = event.final_update_id
        self._needs_first_event = False

    @staticmethod
    def _levels_from_snapshot(
        snapshot: OrderBookSnapshot,
    ) -> tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]]:
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        for level in snapshot.bids:
            if level.price <= 0 or level.quantity < 0:
                raise ValueError("snapshot bids contain invalid levels")
            if level.quantity > 0:
                bids[level.price] = level.quantity
        for level in snapshot.asks:
            if level.price <= 0 or level.quantity < 0:
                raise ValueError("snapshot asks contain invalid levels")
            if level.quantity > 0:
                asks[level.price] = level.quantity
        return bids, asks


class BinanceSpotOrderBook(BinanceLocalOrderBook):
    """现货订单簿恢复器。"""

    def __init__(self, symbol: str, *, max_buffered_events: int = 10_000) -> None:
        super().__init__(symbol, venue="SPOT", max_buffered_events=max_buffered_events)


class BinanceFuturesOrderBook(BinanceLocalOrderBook):
    """USDⓈ-M 合约订单簿恢复器。"""

    def __init__(self, symbol: str, *, max_buffered_events: int = 10_000) -> None:
        super().__init__(symbol, venue="USDS_FUTURES", max_buffered_events=max_buffered_events)
