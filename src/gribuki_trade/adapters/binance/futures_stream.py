"""USDⓈ-M 合约公共 WebSocket 市场数据流。

合约公共流按官方路由分为 ``/public``（高频成交/深度）和 ``/market``（标记价、
行情等）。本模块只负责连接与严格事件规范化；本地 order book 的 REST 快照、持久化
和策略计算由上层负责。序列倒退、深度缺口和不合法载荷均失败关闭。
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, TypeAlias

from websockets.asyncio.client import connect as websocket_connect
from websockets.exceptions import ConnectionClosed

from .envs import BinanceEnvironmentProfile, BinanceProduct, BinanceStage, binance_environment
from .gateway import BinanceConfigurationError, BinanceProtocolError, BinanceTransportError

_SYMBOL = re.compile(r"[a-z0-9]{2,32}", re.ASCII)
_STREAM = re.compile(
    r"(?P<symbol>[a-z0-9]{2,32})@(?P<kind>aggTrade|trade|depth(?:@100ms)?|markPrice(?:@1s)?|bookTicker|ticker)",
    re.ASCII,
)
_MAX_STREAMS = 1024
_MARKET_KINDS = frozenset({"markPrice", "markPrice@1s", "ticker"})


class BinanceFuturesStreamConnectionError(BinanceTransportError):
    """合约公共流无法安全建立或恢复。"""


class FuturesWebSocketConnection(Protocol):
    async def recv(self) -> str | bytes: ...
    async def close(self) -> None: ...


FuturesWebSocketConnector: TypeAlias = Callable[
    ...,
    AbstractAsyncContextManager[FuturesWebSocketConnection],
]
Sleep: TypeAlias = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class FuturesAggTradeEvent:
    stream: str
    symbol: str
    event_time_ms: int
    trade_time_ms: int
    aggregate_trade_id: int
    first_trade_id: int
    last_trade_id: int
    price: Decimal
    quantity: Decimal
    buyer_is_market_maker: bool


@dataclass(frozen=True, slots=True)
class FuturesTradeEvent:
    stream: str
    symbol: str
    event_time_ms: int
    trade_time_ms: int
    trade_id: int
    price: Decimal
    quantity: Decimal
    buyer_is_market_maker: bool


@dataclass(frozen=True, slots=True)
class FuturesDepthEvent:
    stream: str
    symbol: str
    event_time_ms: int
    transaction_time_ms: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int | None
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]


@dataclass(frozen=True, slots=True)
class FuturesMarkPriceEvent:
    stream: str
    symbol: str
    event_time_ms: int
    mark_price: Decimal
    index_price: Decimal
    estimated_settle_price: Decimal
    funding_rate: Decimal
    next_funding_time_ms: int


@dataclass(frozen=True, slots=True)
class FuturesBookTickerEvent:
    stream: str
    symbol: str
    event_time_ms: int
    update_id: int
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal


@dataclass(frozen=True, slots=True)
class FuturesTickerEvent:
    stream: str
    symbol: str
    event_time_ms: int
    last_price: Decimal
    price_change_percent: Decimal
    weighted_average_price: Decimal
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    volume: Decimal
    quote_volume: Decimal


FuturesMarketEvent: TypeAlias = (
    FuturesAggTradeEvent
    | FuturesTradeEvent
    | FuturesDepthEvent
    | FuturesMarkPriceEvent
    | FuturesBookTickerEvent
    | FuturesTickerEvent
)


def normalize_futures_symbol(symbol: str) -> str:
    if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol.lower()) is None:
        raise ValueError("symbol must contain 2-32 ASCII letters, digits or underscore")
    return symbol.upper()


def futures_stream(symbol: str, kind: str) -> str:
    normalized = normalize_futures_symbol(symbol).lower()
    value = f"{normalized}@{kind}"
    if _STREAM.fullmatch(value) is None:
        raise ValueError(f"unsupported USD-M Futures stream kind: {kind!r}")
    return value


def validate_futures_stream(stream: str) -> str:
    if not isinstance(stream, str) or _STREAM.fullmatch(stream) is None:
        raise ValueError("invalid USD-M Futures stream name")
    return stream


def futures_stream_url(
    streams: Sequence[str],
    *,
    profile: BinanceEnvironmentProfile | None = None,
    stage: BinanceStage | str = BinanceStage.DEMO,
    combined: bool | None = None,
    route: str | None = None,
    allow_live: bool = False,
) -> str:
    selected = profile or binance_environment(BinanceProduct.USDS_FUTURES, stage)
    if selected.stage is BinanceStage.LIVE and not allow_live:
        raise BinanceConfigurationError("USD-M LIVE stream requires allow_live=True")
    if selected.market_ws_base_url is None:
        raise BinanceConfigurationError("USD-M market stream endpoint is unavailable")
    canonical = tuple(validate_futures_stream(item) for item in streams)
    if not canonical:
        raise ValueError("at least one Futures stream is required")
    if len(canonical) > _MAX_STREAMS or len(set(canonical)) != len(canonical):
        raise ValueError("Futures streams must be unique and contain at most 1024 entries")
    inferred = (
        "market" if all(item.split("@", 1)[1] in _MARKET_KINDS for item in canonical) else "public"
    )
    selected_route = route or inferred
    if selected_route not in {"public", "market"}:
        raise ValueError("route must be public or market")
    if any(
        (item.split("@", 1)[1] in _MARKET_KINDS) != (selected_route == "market")
        for item in canonical
    ):
        raise ValueError("public and market Futures streams require separate connections")
    use_combined = len(canonical) > 1 if combined is None else combined
    if not use_combined and len(canonical) != 1:
        raise ValueError("raw Futures WebSocket URL accepts exactly one stream")
    base = selected.market_ws_base_url.rstrip("/") + f"/{selected_route}"
    if use_combined:
        return f"{base}/stream?streams={'/'.join(canonical)}"
    return f"{base}/ws/{canonical[0]}"


def parse_futures_stream_message(
    message: str | bytes, *, stream: str | None = None
) -> FuturesMarketEvent:
    if isinstance(message, bytes):
        try:
            message = message.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            raise BinanceProtocolError("Futures stream frame is not UTF-8") from None
    if not isinstance(message, str):
        raise BinanceProtocolError("Futures stream frame must be text or bytes")
    try:
        decoded = json.loads(message)
    except (json.JSONDecodeError, UnicodeError):
        raise BinanceProtocolError("Futures stream frame is not valid JSON") from None
    if not isinstance(decoded, Mapping):
        raise BinanceProtocolError("Futures stream payload must be an object")
    envelope = decoded.get("stream")
    payload = decoded.get("data", decoded)
    if not isinstance(payload, Mapping):
        raise BinanceProtocolError("Futures stream data must be an object")
    if envelope is not None and (
        not isinstance(envelope, str) or _STREAM.fullmatch(envelope) is None
    ):
        raise BinanceProtocolError("Futures stream envelope is malformed")
    selected = stream or envelope
    symbol = _symbol(payload.get("s"))
    event_type = payload.get("e")
    if not isinstance(event_type, str):
        raise BinanceProtocolError("Futures event type is malformed")
    if selected is None:
        selected = _derive_stream(symbol, event_type)
    validate_futures_stream(selected)
    if selected.split("@", 1)[0] != symbol.lower():
        raise BinanceProtocolError("Futures stream symbol does not match payload")
    kind = selected.split("@", 1)[1]
    expected = (
        "markPriceUpdate"
        if kind.startswith("markPrice")
        else (
            "24hrTicker"
            if kind == "ticker"
            else ("depthUpdate" if kind.startswith("depth") else kind)
        )
    )
    if event_type != expected and not (kind == "bookTicker" and event_type is None):
        raise BinanceProtocolError("Futures stream event type does not match stream")
    if kind == "aggTrade":
        return FuturesAggTradeEvent(
            selected,
            symbol,
            _int(payload, "E"),
            _int(payload, "T"),
            _int(payload, "a"),
            _int(payload, "f"),
            _int(payload, "l"),
            _dec(payload, "p", positive=True),
            _dec(payload, "q", positive=True),
            _bool(payload, "m"),
        )
    if kind == "trade":
        return FuturesTradeEvent(
            selected,
            symbol,
            _int(payload, "E"),
            _int(payload, "T"),
            _int(payload, "t"),
            _dec(payload, "p", positive=True),
            _dec(payload, "q", positive=True),
            _bool(payload, "m"),
        )
    if kind.startswith("depth"):
        return FuturesDepthEvent(
            selected,
            symbol,
            _int(payload, "E"),
            _int(payload, "T"),
            _int(payload, "U"),
            _int(payload, "u"),
            _optional_int(payload, "pu"),
            _levels(payload, "b"),
            _levels(payload, "a"),
        )
    if kind.startswith("markPrice"):
        return FuturesMarkPriceEvent(
            selected,
            symbol,
            _int(payload, "E"),
            _dec(payload, "p", positive=True),
            _dec(payload, "i", positive=True),
            _dec(payload, "P"),
            _dec(payload, "r"),
            _int(payload, "T"),
        )
    if kind == "bookTicker":
        return FuturesBookTickerEvent(
            selected,
            symbol,
            _int(payload, "E"),
            _int(payload, "u"),
            _dec(payload, "b", positive=True),
            _dec(payload, "B", positive=False),
            _dec(payload, "a", positive=True),
            _dec(payload, "A", positive=False),
        )
    return FuturesTickerEvent(
        selected,
        symbol,
        _int(payload, "E"),
        _dec(payload, "c", positive=True),
        _dec(payload, "P"),
        _dec(payload, "w", positive=True),
        _dec(payload, "o", positive=True),
        _dec(payload, "h", positive=True),
        _dec(payload, "l", positive=True),
        _dec(payload, "v", positive=False),
        _dec(payload, "q", positive=False),
    )


class BinanceFuturesMarketStream:
    """带 24 小时轮换、有限重连和序列缺口拒绝的合约公共流。"""

    def __init__(
        self,
        streams: Sequence[str],
        *,
        profile: BinanceEnvironmentProfile | None = None,
        stage: BinanceStage | str = BinanceStage.DEMO,
        allow_live: bool = False,
        connector: FuturesWebSocketConnector | None = None,
        sleep: Sleep = asyncio.sleep,
        jitter: Callable[[float], float] | None = None,
        max_reconnect_attempts: int = 5,
        reconnect_delay_seconds: float = 0.5,
        rotation_seconds: float = 23 * 60 * 60,
    ) -> None:
        if max_reconnect_attempts < 0 or reconnect_delay_seconds < 0 or rotation_seconds <= 0:
            raise ValueError("invalid reconnect or rotation settings")
        self._streams = tuple(validate_futures_stream(item) for item in streams)
        self._profile = profile or binance_environment(BinanceProduct.USDS_FUTURES, stage)
        self._url = futures_stream_url(self._streams, profile=self._profile, allow_live=allow_live)
        self._connector = connector or websocket_connect
        self._sleep, self._jitter = (
            sleep,
            jitter or (lambda value: random.uniform(0.8, 1.2) * value),
        )
        self._max_reconnect, self._delay, self._rotation = (
            max_reconnect_attempts,
            reconnect_delay_seconds,
            rotation_seconds,
        )
        self._stop = False
        self._connection: FuturesWebSocketConnection | None = None
        self._last: dict[str, int] = {}

    @property
    def streams(self) -> tuple[str, ...]:
        return self._streams

    @property
    def url(self) -> str:
        return self._url

    async def aclose(self) -> None:
        self._stop = True
        if self._connection is not None:
            await self._connection.close()

    async def events(self) -> AsyncIterator[FuturesMarketEvent]:
        retries = 0
        while not self._stop:
            try:
                context = self._connector(
                    self._url, ping_interval=20, ping_timeout=20, max_size=1_048_576
                )
                async with context as connection:
                    self._connection = connection
                    started = time.monotonic()
                    retries = 0
                    while not self._stop and time.monotonic() - started < self._rotation:
                        event = parse_futures_stream_message(
                            await connection.recv(),
                            stream=self._streams[0] if len(self._streams) == 1 else None,
                        )
                        self._validate_sequence(event)
                        yield event
            except BinanceProtocolError:
                raise
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, ConnectionError, TimeoutError, EOFError, OSError) as exc:
                if self._stop:
                    break
                if retries >= self._max_reconnect:
                    raise BinanceFuturesStreamConnectionError(
                        "Futures market stream reconnect limit exhausted"
                    ) from exc
                retries += 1
                await self._sleep(self._jitter(self._delay * 2 ** (retries - 1)))
            finally:
                self._connection = None
        self._stop = False

    def _validate_sequence(self, event: FuturesMarketEvent) -> None:
        if isinstance(event, FuturesDepthEvent):
            previous = self._last.get(event.stream)
            if previous is not None and (
                event.previous_final_update_id is not None
                and event.previous_final_update_id != previous
                or event.first_update_id > previous + 1
            ):
                raise BinanceProtocolError(
                    "Futures depth update gap detected; snapshot recovery required"
                )
            sequence = event.final_update_id
        elif isinstance(event, FuturesAggTradeEvent):
            sequence = event.aggregate_trade_id
        elif isinstance(event, FuturesTradeEvent):
            sequence = event.trade_id
        elif isinstance(event, FuturesBookTickerEvent):
            sequence = event.update_id
        else:
            return
        previous = self._last.get(event.stream)
        if previous is not None and sequence <= previous:
            raise BinanceProtocolError("Futures stream sequence did not advance")
        self._last[event.stream] = sequence


def _symbol(value: object) -> str:
    if (
        not isinstance(value, str)
        or _SYMBOL.fullmatch(value.lower()) is None
        or value != value.upper()
    ):
        raise BinanceProtocolError("Futures symbol is malformed")
    return value


def _derive_stream(symbol: str, event_type: str) -> str:
    mapping = {
        "aggTrade": "aggTrade",
        "trade": "trade",
        "depthUpdate": "depth",
        "markPriceUpdate": "markPrice",
        "24hrTicker": "ticker",
        "bookTicker": "bookTicker",
    }
    try:
        return f"{symbol.lower()}@{mapping[event_type]}"
    except KeyError:
        raise BinanceProtocolError("unsupported Futures event type") from None


def _int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BinanceProtocolError(f"Futures field {key!r} is malformed")
    return value


def _optional_int(payload: Mapping[str, Any], key: str) -> int | None:
    return None if key not in payload else _int(payload, key)


def _dec(payload: Mapping[str, Any], key: str, *, positive: bool = False) -> Decimal:
    value = payload.get(key)
    if not isinstance(value, str):
        raise BinanceProtocolError(f"Futures field {key!r} is malformed")
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise BinanceProtocolError(f"Futures field {key!r} is malformed") from None
    if not number.is_finite() or (number <= 0 if positive else number < 0):
        raise BinanceProtocolError(f"Futures field {key!r} is out of range")
    return number


def _bool(payload: Mapping[str, Any], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise BinanceProtocolError(f"Futures field {key!r} is malformed")
    return value


def _levels(payload: Mapping[str, Any], key: str) -> tuple[tuple[Decimal, Decimal], ...]:
    values = payload.get(key)
    if not isinstance(values, list):
        raise BinanceProtocolError(f"Futures depth field {key!r} is malformed")
    result = []
    for level in values:
        if (
            not isinstance(level, list)
            or len(level) != 2
            or not all(isinstance(item, str) for item in level)
        ):
            raise BinanceProtocolError(f"Futures depth field {key!r} is malformed")
        result.append((_dec({"v": level[0]}, "v", positive=True), _dec({"v": level[1]}, "v")))
    return tuple(result)
