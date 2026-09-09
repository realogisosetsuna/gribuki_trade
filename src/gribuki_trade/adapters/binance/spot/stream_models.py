"""Binance 现货公共流的事件模型与 URL/标的校验。"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import TypeAlias

from ..models import BinanceEnvironment
from ..transport.errors import BinanceConfigurationError

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
    r"(?:(?P<book>bookTicker)|(?P<trade>trade)|(?P<depth>depth(?:@100ms)?)|kline_(?P<interval>"
    + "|".join(re.escape(value) for value in sorted(KLINE_INTERVALS, key=len, reverse=True))
    + r"))",
    re.ASCII,
)
_MAX_COMBINED_STREAMS = 1024


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


@dataclass(frozen=True, slots=True)
class BinanceDepthEvent:
    """现货差分深度更新。

    ``bids`` and ``asks`` contain absolute quantities for each price level.  The
    event is only a transport value; callers must apply Binance's REST snapshot
    and update-id recovery procedure before treating a local book as valid.
    """

    stream: str
    symbol: str
    event_time_ms: int
    first_update_id: int
    final_update_id: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    previous_final_update_id: int | None = None


BinanceMarketEvent: TypeAlias = (
    BinanceBookTickerEvent | BinanceTradeEvent | BinanceKlineEvent | BinanceDepthEvent
)


def normalize_symbol(symbol: str) -> str:
    """校验 Binance 标的代码并返回规范的大写形式。"""

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


def depth_stream(symbol: str, *, update_speed_ms: int | None = None) -> str:
    """返回现货差分深度流名称，默认 1000 毫秒，也支持 100 毫秒。"""

    normalized = normalize_symbol(symbol).lower()
    if update_speed_ms not in (None, 100):
        raise ValueError("update_speed_ms must be 100 for the accelerated depth stream")
    return f"{normalized}@depth" + ("@100ms" if update_speed_ms == 100 else "")


def validate_stream_name(stream: str) -> str:
    """返回规范的公共数据流名称，无法规范化时拒绝。

    要求标的代码使用小写，既符合 Binance 线上格式，也可阻止调用方把其他路径或
    查询参数注入数据流 URL。
    """

    if not isinstance(stream, str) or _STREAM_RE.fullmatch(stream) is None:
        raise ValueError(
            "stream must be a canonical bookTicker, trade, depth, or supported kline stream"
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
    """根据已校验的数据流构建原始或组合公共 WebSocket URL。"""

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
        # 数据流名称已经过语法校验，因此拼接不会注入路径、片段或第二个查询参数。
        return f"{base_url}/stream?streams={'/'.join(canonical)}"
    return f"{base_url}/ws/{canonical[0]}"
