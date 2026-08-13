"""Public Binance Spot WebSocket market-data streams.

Only public streams are supported here.  API credentials and private user-data
listen keys deliberately do not form part of this module's interface.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, TypeAlias

from websockets.asyncio.client import connect as websocket_connect
from websockets.exceptions import ConnectionClosed

from .gateway import (
    BinanceConfigurationError,
    BinanceProtocolError,
    BinanceTransportError,
)
from .models import BinanceEnvironment

LIVE_WS_BASE_URL = "wss://stream.binance.com:9443"
TESTNET_WS_BASE_URL = "wss://stream.testnet.binance.vision"

KLINE_INTERVALS = frozenset(
    {
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    }
)

_SYMBOL_RE = re.compile(r"[A-Za-z0-9]{2,32}", re.ASCII)
_STREAM_RE = re.compile(
    r"(?P<symbol>[a-z0-9]{2,32})@"
    r"(?:(?P<book>bookTicker)|(?P<trade>trade)|kline_(?P<interval>"
    + "|".join(re.escape(value) for value in sorted(KLINE_INTERVALS, key=len, reverse=True))
    + r"))",
    re.ASCII,
)
_MAX_COMBINED_STREAMS = 1024


class BinanceStreamConnectionError(BinanceTransportError):
    """A public WebSocket could not be established or restored safely."""


class WebSocketConnection(Protocol):
    """Minimum socket surface used by :class:`BinanceSpotMarketStream`."""

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


WebSocketConnector: TypeAlias = Callable[
    ...,
    AbstractAsyncContextManager[WebSocketConnection],
]
Sleep: TypeAlias = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BinanceBookTickerEvent:
    stream: str
    symbol: str
    update_id: int
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal
    event_time_ms: int | None = None


@dataclass(frozen=True, slots=True)
class BinanceTradeEvent:
    stream: str
    symbol: str
    event_time_ms: int
    trade_id: int
    price: Decimal
    quantity: Decimal
    buyer_order_id: int | None
    seller_order_id: int | None
    trade_time_ms: int
    buyer_is_market_maker: bool


@dataclass(frozen=True, slots=True)
class BinanceKlineEvent:
    stream: str
    symbol: str
    event_time_ms: int
    start_time_ms: int
    close_time_ms: int
    interval: str
    first_trade_id: int
    last_trade_id: int
    open: Decimal
    close: Decimal
    high: Decimal
    low: Decimal
    volume: Decimal
    trade_count: int
    is_closed: bool
    quote_volume: Decimal
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal


BinanceMarketEvent: TypeAlias = (
    BinanceBookTickerEvent | BinanceTradeEvent | BinanceKlineEvent
)


def normalize_symbol(symbol: str) -> str:
    """Validate a Binance symbol and return its canonical upper-case form."""

    if not isinstance(symbol, str) or _SYMBOL_RE.fullmatch(symbol) is None:
        raise ValueError("symbol must contain 2-32 ASCII letters or digits")
    return symbol.upper()


def book_ticker_stream(symbol: str) -> str:
    return f"{normalize_symbol(symbol).lower()}@bookTicker"


def trade_stream(symbol: str) -> str:
    return f"{normalize_symbol(symbol).lower()}@trade"


def kline_stream(symbol: str, interval: str) -> str:
    normalized = normalize_symbol(symbol).lower()
    if not isinstance(interval, str) or interval not in KLINE_INTERVALS:
        raise ValueError(f"unsupported Binance kline interval: {interval!r}")
    return f"{normalized}@kline_{interval}"


def validate_stream_name(stream: str) -> str:
    """Return a canonical public stream name or reject it.

    Requiring lower-case symbols matches Binance's wire format and prevents a
    caller from injecting another path or query parameter into a stream URL.
    """

    if not isinstance(stream, str) or _STREAM_RE.fullmatch(stream) is None:
        raise ValueError(
            "stream must be a canonical bookTicker, trade, or supported kline stream"
        )
    return stream


def websocket_base_url(environment: BinanceEnvironment | str) -> str:
    try:
        selected = (
            environment
            if isinstance(environment, BinanceEnvironment)
            else BinanceEnvironment(str(environment).upper())
        )
    except ValueError:
        raise BinanceConfigurationError(
            f"unknown Binance environment: {environment!r}"
        ) from None
    if selected is BinanceEnvironment.LIVE:
        return LIVE_WS_BASE_URL
    return TESTNET_WS_BASE_URL


def build_stream_url(
    streams: Sequence[str],
    *,
    environment: BinanceEnvironment | str = BinanceEnvironment.TESTNET,
    allow_live: bool = False,
    combined: bool | None = None,
) -> str:
    """Build a raw or combined public WebSocket URL from validated streams."""

    try:
        selected = (
            environment
            if isinstance(environment, BinanceEnvironment)
            else BinanceEnvironment(str(environment).upper())
        )
    except ValueError:
        raise BinanceConfigurationError(
            f"unknown Binance environment: {environment!r}"
        ) from None
    if selected is BinanceEnvironment.LIVE and not allow_live:
        raise BinanceConfigurationError(
            "Binance LIVE WebSocket is disabled; pass allow_live=True explicitly to enable it"
        )

    canonical = tuple(validate_stream_name(stream) for stream in streams)
    if not canonical:
        raise ValueError("at least one Binance public stream is required")
    if len(canonical) > _MAX_COMBINED_STREAMS:
        raise ValueError(f"at most {_MAX_COMBINED_STREAMS} streams may be combined")
    if len(set(canonical)) != len(canonical):
        raise ValueError("duplicate Binance streams are not allowed")

    base_url = websocket_base_url(selected)
    use_combined = len(canonical) > 1 if combined is None else combined
    if not use_combined and len(canonical) != 1:
        raise ValueError("a raw Binance WebSocket URL supports exactly one stream")
    if use_combined:
        # Stream names are grammar-validated above, so joining cannot inject a
        # path, fragment, or second query parameter.
        return f"{base_url}/stream?streams={'/'.join(canonical)}"
    return f"{base_url}/ws/{canonical[0]}"


def parse_stream_message(
    message: str | bytes,
    *,
    expected_streams: Sequence[str] | None = None,
) -> BinanceMarketEvent:
    """Parse one raw or combined Binance market-data frame strictly."""

    if isinstance(message, bytes):
        try:
            text = message.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            raise BinanceProtocolError("Binance WebSocket frame is not UTF-8") from None
    elif isinstance(message, str):
        text = message
    else:
        raise BinanceProtocolError("Binance WebSocket frame must be text or bytes")

    try:
        decoded = json.loads(text)
    except (json.JSONDecodeError, UnicodeError):
        raise BinanceProtocolError("Binance WebSocket frame is not valid JSON") from None
    if not isinstance(decoded, Mapping):
        raise BinanceProtocolError("Binance WebSocket payload must be an object")

    stream_from_envelope: str | None = None
    payload: Mapping[str, Any]
    if "stream" in decoded or "data" in decoded:
        stream_value = decoded.get("stream")
        data_value = decoded.get("data")
        if not isinstance(stream_value, str):
            raise BinanceProtocolError("combined Binance stream name is malformed")
        try:
            stream_from_envelope = validate_stream_name(stream_value)
        except (TypeError, ValueError):
            raise BinanceProtocolError("combined Binance stream name is malformed") from None
        if not isinstance(data_value, Mapping):
            raise BinanceProtocolError("combined Binance stream data must be an object")
        payload = data_value
    else:
        payload = decoded

    allowed: tuple[str, ...] | None = None
    if expected_streams is not None:
        try:
            allowed = tuple(validate_stream_name(value) for value in expected_streams)
        except (TypeError, ValueError):
            raise ValueError("expected_streams contains an invalid stream name") from None
        if not allowed:
            raise ValueError("expected_streams must not be empty")
        if stream_from_envelope is not None and stream_from_envelope not in allowed:
            raise BinanceProtocolError("received an event for an unsubscribed Binance stream")

    event_type = payload.get("e")
    symbol = _wire_symbol(payload.get("s"))
    stream = stream_from_envelope or _derive_stream(event_type, symbol, payload)
    if allowed is not None and stream not in allowed:
        raise BinanceProtocolError("received an event for an unsubscribed Binance stream")

    _validate_envelope_matches_payload(stream, event_type, symbol, payload)
    if stream.endswith("@bookTicker"):
        return _parse_book_ticker(stream, symbol, payload)
    if stream.endswith("@trade"):
        return _parse_trade(stream, symbol, payload)
    return _parse_kline(stream, symbol, payload)


class BinanceSpotMarketStream:
    """Async iterator over public Binance Spot market events.

    The WebSocket library supplies protocol ping/pong heartbeats.  A network
    loss is retried a bounded number of times; malformed data and sequence
    regressions are raised immediately and are never treated as reconnectable.
    """

    def __init__(
        self,
        streams: Sequence[str],
        *,
        environment: BinanceEnvironment | str = BinanceEnvironment.TESTNET,
        allow_live: bool = False,
        combined: bool | None = None,
        connector: WebSocketConnector | None = None,
        ping_interval_seconds: float = 20.0,
        ping_timeout_seconds: float = 20.0,
        open_timeout_seconds: float = 10.0,
        close_timeout_seconds: float = 10.0,
        max_reconnect_attempts: int = 3,
        reconnect_delay_seconds: float = 0.5,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if ping_interval_seconds <= 0 or ping_timeout_seconds <= 0:
            raise ValueError("WebSocket heartbeat intervals must be positive")
        if open_timeout_seconds <= 0 or close_timeout_seconds <= 0:
            raise ValueError("WebSocket timeouts must be positive")
        if (
            isinstance(max_reconnect_attempts, bool)
            or not isinstance(max_reconnect_attempts, int)
            or max_reconnect_attempts < 0
        ):
            raise ValueError("max_reconnect_attempts must be a non-negative integer")
        if reconnect_delay_seconds < 0:
            raise ValueError("reconnect_delay_seconds must not be negative")

        self._streams = tuple(validate_stream_name(stream) for stream in streams)
        self._url = build_stream_url(
            self._streams,
            environment=environment,
            allow_live=allow_live,
            combined=combined,
        )
        try:
            self._environment = (
                environment
                if isinstance(environment, BinanceEnvironment)
                else BinanceEnvironment(str(environment).upper())
            )
        except ValueError:  # pragma: no cover - validated by build_stream_url
            raise BinanceConfigurationError("unknown Binance environment") from None
        self._connector = connector if connector is not None else websocket_connect
        self._ping_interval_seconds = ping_interval_seconds
        self._ping_timeout_seconds = ping_timeout_seconds
        self._open_timeout_seconds = open_timeout_seconds
        self._close_timeout_seconds = close_timeout_seconds
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._sleep = sleep
        self._connected = False
        self._iterating = False
        self._stop_requested = False
        self._connection: WebSocketConnection | None = None
        self._last_sequence: dict[str, int] = {}

    def __repr__(self) -> str:
        # Deliberately omit ``url``: combined URLs use a query component and
        # retaining it in repr would be an unsafe precedent for future streams.
        return (
            f"BinanceSpotMarketStream(environment={self.environment.value!r}, "
            f"stream_count={len(self.streams)!r}, connected={self.connected!r})"
        )

    @property
    def environment(self) -> BinanceEnvironment:
        return self._environment

    @property
    def streams(self) -> tuple[str, ...]:
        return self._streams

    @property
    def url(self) -> str:
        return self._url

    @property
    def connected(self) -> bool:
        return self._connected

    def __aiter__(self) -> AsyncIterator[BinanceMarketEvent]:
        return self.events()

    async def aclose(self) -> None:
        """Stop iteration and close the active socket, if one exists."""

        self._stop_requested = True
        connection = self._connection
        if connection is not None:
            await connection.close()

    async def events(self) -> AsyncIterator[BinanceMarketEvent]:
        if self._iterating:
            raise RuntimeError("a Binance market stream supports only one active consumer")
        self._iterating = True
        self._stop_requested = False
        reconnects = 0
        try:
            while not self._stop_requested:
                received_valid_event = False
                try:
                    context = self._connector(
                        self._url,
                        ping_interval=self._ping_interval_seconds,
                        ping_timeout=self._ping_timeout_seconds,
                        open_timeout=self._open_timeout_seconds,
                        close_timeout=self._close_timeout_seconds,
                        max_size=1_048_576,
                    )
                    async with context as connection:
                        self._connection = connection
                        self._connected = True
                        while not self._stop_requested:
                            frame = await connection.recv()
                            event = parse_stream_message(
                                frame,
                                expected_streams=self._streams,
                            )
                            self._validate_sequence(event)
                            received_valid_event = True
                            reconnects = 0
                            yield event
                except asyncio.CancelledError:
                    raise
                except BinanceProtocolError:
                    raise
                except (ConnectionClosed, ConnectionError, TimeoutError, EOFError, OSError) as exc:
                    if self._stop_requested:
                        break
                    if reconnects >= self._max_reconnect_attempts:
                        raise BinanceStreamConnectionError(
                            "Binance public WebSocket reconnect limit exhausted"
                        ) from exc
                    reconnects += 1
                    delay = self._reconnect_delay_seconds * (2 ** (reconnects - 1))
                    await self._sleep(delay)
                    continue
                finally:
                    self._connection = None
                    self._connected = False

                if self._stop_requested:
                    break
                # A conforming WebSocket normally exits through ConnectionClosed.
                # Treat an unexplained clean context exit as a bounded disconnect.
                if received_valid_event:
                    reconnects = 0
                if reconnects >= self._max_reconnect_attempts:
                    raise BinanceStreamConnectionError(
                        "Binance public WebSocket closed without a close exception"
                    )
                reconnects += 1
                delay = self._reconnect_delay_seconds * (2 ** (reconnects - 1))
                await self._sleep(delay)
        finally:
            self._connection = None
            self._connected = False
            self._iterating = False

    def _validate_sequence(self, event: BinanceMarketEvent) -> None:
        sequence: int | None = None
        if isinstance(event, BinanceBookTickerEvent):
            sequence = event.update_id
        elif isinstance(event, BinanceTradeEvent):
            sequence = event.trade_id
        if sequence is None:
            return
        previous = self._last_sequence.get(event.stream)
        if previous is not None and sequence <= previous:
            raise BinanceProtocolError(
                f"Binance {event.stream.rsplit('@', 1)[-1]} sequence did not advance"
            )
        self._last_sequence[event.stream] = sequence


def _wire_symbol(value: object) -> str:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise BinanceProtocolError("Binance WebSocket symbol is malformed")
    if value != value.upper():
        raise BinanceProtocolError("Binance WebSocket symbol is not canonical upper-case")
    return value


def _derive_stream(event_type: object, symbol: str, payload: Mapping[str, Any]) -> str:
    prefix = symbol.lower()
    if event_type == "bookTicker" or (
        event_type is None and all(key in payload for key in ("u", "b", "B", "a", "A"))
    ):
        return f"{prefix}@bookTicker"
    if event_type == "trade":
        return f"{prefix}@trade"
    if event_type == "kline":
        kline = payload.get("k")
        if not isinstance(kline, Mapping):
            raise BinanceProtocolError("Binance kline payload is malformed")
        interval = kline.get("i")
        if not isinstance(interval, str) or interval not in KLINE_INTERVALS:
            raise BinanceProtocolError("Binance kline interval is malformed")
        return f"{prefix}@kline_{interval}"
    raise BinanceProtocolError("unsupported Binance WebSocket event type")


def _validate_envelope_matches_payload(
    stream: str,
    event_type: object,
    symbol: str,
    payload: Mapping[str, Any],
) -> None:
    stream_symbol, stream_kind = stream.split("@", maxsplit=1)
    if stream_symbol != symbol.lower():
        raise BinanceProtocolError("Binance stream symbol does not match its payload")
    expected_type = "kline" if stream_kind.startswith("kline_") else stream_kind
    # Binance's bookTicker wire payload omits ``e`` on some endpoints; the
    # canonical envelope name plus its required field set identifies it.
    if event_type != expected_type and not (
        expected_type == "bookTicker" and event_type is None
    ):
        raise BinanceProtocolError("Binance stream type does not match its payload")
    if expected_type == "kline":
        kline = payload.get("k")
        if not isinstance(kline, Mapping):
            raise BinanceProtocolError("Binance kline payload is malformed")
        if kline.get("s") != symbol:
            raise BinanceProtocolError("Binance kline symbol does not match its payload")
        if stream_kind != f"kline_{kline.get('i')}":
            raise BinanceProtocolError("Binance kline interval does not match its stream")


def _parse_book_ticker(
    stream: str,
    symbol: str,
    payload: Mapping[str, Any],
) -> BinanceBookTickerEvent:
    return BinanceBookTickerEvent(
        stream=stream,
        symbol=symbol,
        update_id=_integer(payload, "u", minimum=0),
        bid_price=_decimal(payload, "b", positive=True),
        bid_quantity=_decimal(payload, "B", positive=False),
        ask_price=_decimal(payload, "a", positive=True),
        ask_quantity=_decimal(payload, "A", positive=False),
        event_time_ms=_optional_integer(payload, "E", minimum=0),
    )


def _parse_trade(
    stream: str,
    symbol: str,
    payload: Mapping[str, Any],
) -> BinanceTradeEvent:
    return BinanceTradeEvent(
        stream=stream,
        symbol=symbol,
        event_time_ms=_integer(payload, "E", minimum=0),
        trade_id=_integer(payload, "t", minimum=0),
        price=_decimal(payload, "p", positive=True),
        quantity=_decimal(payload, "q", positive=True),
        # The current public Spot trade stream doesn't guarantee order IDs.
        # Some environments/older payloads include them; preserve them when
        # present without rejecting the documented minimal event shape.
        buyer_order_id=_optional_integer(payload, "b", minimum=0),
        seller_order_id=_optional_integer(payload, "a", minimum=0),
        trade_time_ms=_integer(payload, "T", minimum=0),
        buyer_is_market_maker=_boolean(payload, "m"),
    )


def _parse_kline(
    stream: str,
    symbol: str,
    payload: Mapping[str, Any],
) -> BinanceKlineEvent:
    raw = payload.get("k")
    if not isinstance(raw, Mapping):
        raise BinanceProtocolError("Binance kline payload is malformed")
    first_trade_id = _integer(raw, "f", minimum=-1)
    last_trade_id = _integer(raw, "L", minimum=-1)
    trade_count = _integer(raw, "n", minimum=0)
    if (first_trade_id == -1) != (last_trade_id == -1):
        raise BinanceProtocolError("Binance kline trade-ID sentinels are inconsistent")
    if trade_count > 0 and first_trade_id == -1:
        raise BinanceProtocolError("Binance non-empty kline is missing trade IDs")
    if first_trade_id >= 0 and last_trade_id < first_trade_id:
        raise BinanceProtocolError("Binance kline trade-ID range is malformed")
    return BinanceKlineEvent(
        stream=stream,
        symbol=symbol,
        event_time_ms=_integer(payload, "E", minimum=0),
        start_time_ms=_integer(raw, "t", minimum=0),
        close_time_ms=_integer(raw, "T", minimum=0),
        interval=_required_string(raw, "i"),
        first_trade_id=first_trade_id,
        last_trade_id=last_trade_id,
        open=_decimal(raw, "o", positive=True),
        close=_decimal(raw, "c", positive=True),
        high=_decimal(raw, "h", positive=True),
        low=_decimal(raw, "l", positive=True),
        volume=_decimal(raw, "v", positive=False),
        trade_count=trade_count,
        is_closed=_boolean(raw, "x"),
        quote_volume=_decimal(raw, "q", positive=False),
        taker_buy_base_volume=_decimal(raw, "V", positive=False),
        taker_buy_quote_volume=_decimal(raw, "Q", positive=False),
    )


def _integer(mapping: Mapping[str, Any], key: str, *, minimum: int) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise BinanceProtocolError(f"Binance WebSocket field {key!r} is malformed")
    return value


def _optional_integer(
    mapping: Mapping[str, Any],
    key: str,
    *,
    minimum: int,
) -> int | None:
    if key not in mapping:
        return None
    return _integer(mapping, key, minimum=minimum)


def _decimal(
    mapping: Mapping[str, Any],
    key: str,
    *,
    positive: bool,
) -> Decimal:
    value = mapping.get(key)
    if not isinstance(value, str):
        raise BinanceProtocolError(f"Binance WebSocket field {key!r} is malformed")
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise BinanceProtocolError(f"Binance WebSocket field {key!r} is malformed") from None
    if not number.is_finite() or (number <= 0 if positive else number < 0):
        raise BinanceProtocolError(f"Binance WebSocket field {key!r} is out of range")
    return number


def _boolean(mapping: Mapping[str, Any], key: str) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise BinanceProtocolError(f"Binance WebSocket field {key!r} is malformed")
    return value


def _required_string(mapping: Mapping[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise BinanceProtocolError(f"Binance WebSocket field {key!r} is malformed")
    return value
