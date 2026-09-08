"""Binance USDⓈ-M/COIN-M 私有用户数据流适配器。

本模块只负责协议、连接和事件规范化；持久化与恢复策略属于上层服务。事件解析
对新增事件保持向前兼容，但不接受缺少时间戳或事件类型的模糊载荷。
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias
from urllib.parse import quote

from websockets.asyncio.client import connect as websocket_connect
from websockets.exceptions import ConnectionClosed

from .futures import BinanceFuturesRestClient
from .gateway import (
    BinanceConfigurationError,
    BinanceError,
    BinanceProtocolError,
    BinanceTransportError,
)

if TYPE_CHECKING:
    from gribuki_trade.runtime.guard import LiveTradingGuard


class BinanceFuturesUserStreamConnectionError(BinanceTransportError):
    """合约私有数据流无法建立或恢复。"""


class FuturesUserStreamConnection(Protocol):
    """合约数据流需要的最小 WebSocket 接口。"""

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


FuturesUserStreamConnector: TypeAlias = Callable[
    ...,
    AbstractAsyncContextManager[FuturesUserStreamConnection],
]
Sleep: TypeAlias = Callable[[float], Awaitable[None]]
LifecycleCallback: TypeAlias = Callable[["FuturesStreamLifecycle"], Awaitable[None] | None]


@dataclass(frozen=True, slots=True)
class FuturesUserEvent:
    """所有合约用户事件共享的安全、可持久化外观。"""

    event_type: str
    event_time_ms: int
    transaction_time_ms: int | None
    received_time_ms: int
    payload: Mapping[str, Any]
    connection_epoch: int = 0
    known: bool = True


class FuturesOrderUpdate(FuturesUserEvent):
    """普通订单或成交状态事件。"""


class FuturesAlgoUpdate(FuturesUserEvent):
    """条件 Algo 订单状态事件。"""


class FuturesAccountUpdate(FuturesUserEvent):
    """余额和仓位变化事件。"""


class FuturesConfigUpdate(FuturesUserEvent):
    """杠杆或多资产模式变化事件。"""


class FuturesMarginCall(FuturesUserEvent):
    """保证金风险提示事件。"""


class FuturesTradeLite(FuturesUserEvent):
    """低延迟成交事件。"""


class FuturesTriggerReject(FuturesUserEvent):
    """条件单触发失败事件。"""


class FuturesStrategyUpdate(FuturesUserEvent):
    """策略订单状态事件。"""


class FuturesGridUpdate(FuturesUserEvent):
    """旧版网格策略状态事件。"""


class FuturesListenKeyExpired(FuturesUserEvent):
    """监听密钥过期事件。"""


class FuturesUnknownEvent(FuturesUserEvent):
    """文档尚未识别但结构完整的事件。"""


FuturesUserEventType: TypeAlias = FuturesUserEvent


@dataclass(frozen=True, slots=True)
class FuturesStreamLifecycle:
    """连接生命周期通知，不包含密钥。"""

    kind: str
    connection_epoch: int
    observed_time_ms: int
    reason: str | None = None


_KNOWN = {
    "ACCOUNT_UPDATE": FuturesAccountUpdate,
    "ACCOUNT_CONFIG_UPDATE": FuturesConfigUpdate,
    "ORDER_TRADE_UPDATE": FuturesOrderUpdate,
    "ALGO_UPDATE": FuturesAlgoUpdate,
    "TRADE_LITE": FuturesTradeLite,
    "CONDITIONAL_ORDER_TRIGGER_REJECT": FuturesTriggerReject,
    "MARGIN_CALL": FuturesMarginCall,
    "STRATEGY_UPDATE": FuturesStrategyUpdate,
    "GRID_UPDATE": FuturesGridUpdate,
    "listenKeyExpired": FuturesListenKeyExpired,
}


def parse_futures_user_event(
    message: str | bytes,
    *,
    received_time_ms: int | None = None,
    connection_epoch: int = 0,
) -> FuturesUserEvent:
    """严格解析合约事件，未知事件保留为降级事件。"""

    if isinstance(message, bytes):
        try:
            text = message.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            raise BinanceProtocolError("Binance Futures user frame is not UTF-8") from None
    elif isinstance(message, str):
        text = message
    else:
        raise BinanceProtocolError("Binance Futures user frame must be text or bytes")
    try:
        decoded = json.loads(text)
    except (json.JSONDecodeError, UnicodeError):
        raise BinanceProtocolError("Binance Futures user frame is not valid JSON") from None
    if not isinstance(decoded, Mapping):
        raise BinanceProtocolError("Binance Futures user payload must be an object")
    payload: Mapping[str, Any] = decoded
    # 兼容组合流信封和新的订阅信封。
    if isinstance(decoded.get("data"), Mapping):
        payload = decoded["data"]
    elif isinstance(decoded.get("event"), Mapping):
        payload = decoded["event"]
    event_type = payload.get("e")
    if not isinstance(event_type, str) or not event_type:
        raise BinanceProtocolError("Binance Futures user event type is malformed")
    event_time = _integer(payload, "E")
    transaction = payload.get("T")
    if transaction is not None:
        transaction = _integer_value(transaction, "T")
    _validate_event_shape(event_type, payload)
    safe_payload = MappingProxyType(_sanitize_mapping(payload))
    event_class = _KNOWN.get(event_type, FuturesUnknownEvent)
    return event_class(
        event_type=event_type,
        event_time_ms=event_time,
        transaction_time_ms=transaction,
        received_time_ms=received_time_ms if received_time_ms is not None else _now_ms(),
        payload=safe_payload,
        connection_epoch=connection_epoch,
        known=event_type in _KNOWN,
    )


def normalize_futures_user_event(event: FuturesUserEvent) -> dict[str, Any]:
    """生成供服务层使用的中立字典，不复制密钥。"""

    raw = dict(event.payload)
    nested = raw.get("o")
    if not isinstance(nested, Mapping):
        nested = {}
    result: dict[str, Any] = {
        "event_type": event.event_type,
        "event_time_ms": event.event_time_ms,
        "transaction_time_ms": event.transaction_time_ms,
        "received_time_ms": event.received_time_ms,
        "connection_epoch": event.connection_epoch,
        "known": event.known,
        "kind": _event_kind(event.event_type),
        "payload": raw,
    }
    for source, target in {
        "s": "symbol",
        "c": "client_order_id",
        "i": "order_id",
        "x": "execution_type",
        "X": "status",
        "S": "side",
        "ps": "position_side",
        "o": "order_type",
        "q": "quantity",
        "z": "cumulative_filled_quantity",
        "l": "last_filled_quantity",
        "L": "last_filled_price",
        "n": "commission",
        "N": "commission_asset",
        "t": "trade_id",
        "r": "reason",
        "rm": "reason",
        "caid": "client_algo_id",
        "aid": "algo_id",
        "ai": "actual_order_id",
        "tp": "trigger_price",
        "tt": "trigger_time",
    }.items():
        value = nested.get(source)
        if value is None:
            value = raw.get(source)
        if value is not None:
            result[target] = value
    return result


class BinanceFuturesUserDataStream:
    """带守卫、轮换、保活和有限缓冲的合约私有流。"""

    def __init__(
        self,
        client: BinanceFuturesRestClient,
        *,
        account_id: str,
        guard: LiveTradingGuard | None = None,
        exchange: str = "BINANCE",
        connector: FuturesUserStreamConnector | None = None,
        clock_ms: Callable[[], int] | None = None,
        monotonic: Callable[[], float] | None = None,
        sleep: Sleep = asyncio.sleep,
        jitter: Callable[[float], float] | None = None,
        on_lifecycle: LifecycleCallback | None = None,
        keepalive_interval_seconds: float = 30 * 60,
        rotation_interval_seconds: float = 23 * 60 * 60,
        queue_capacity: int = 1024,
        max_reconnect_attempts: int = 8,
        reconnect_delay_seconds: float = 0.5,
    ) -> None:
        if not account_id.strip():
            raise ValueError("account_id must not be blank")
        if client.profile.is_live and guard is None:
            raise BinanceConfigurationError("LIVE Futures user stream requires a guard")
        if keepalive_interval_seconds <= 0 or rotation_interval_seconds <= 0:
            raise ValueError("stream intervals must be positive")
        if queue_capacity < 1 or max_reconnect_attempts < 0 or reconnect_delay_seconds < 0:
            raise ValueError("invalid stream buffering/reconnect options")
        self._client = client
        self._account_id = account_id
        self._exchange = exchange
        self._guard = guard
        self._connector = connector or websocket_connect
        self._clock_ms = clock_ms or _now_ms
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep
        self._jitter = jitter or (lambda value: random.uniform(0.8, 1.2) * value)
        self._on_lifecycle = on_lifecycle
        self._keepalive_interval = keepalive_interval_seconds
        self._rotation_interval = rotation_interval_seconds
        self._queue: asyncio.Queue[FuturesUserEvent | None] = asyncio.Queue(maxsize=queue_capacity)
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_delay = reconnect_delay_seconds
        self._connected = False
        self._healthy = True
        self._stopping = False
        self._epoch = 0
        self._listen_key: str | None = None
        self._connection: FuturesUserStreamConnection | None = None
        self._runner: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._terminal_error: BaseException | None = None
        self._last_event_order: dict[tuple[str, str], tuple[int, int]] = {}

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def healthy(self) -> bool:
        return self._healthy

    @property
    def connection_epoch(self) -> int:
        return self._epoch

    @property
    def listen_key(self) -> str | None:
        return None

    async def connect(self) -> None:
        """启动后台连接；事件通过 :meth:`events` 读取。"""

        if self._guard is not None:
            from gribuki_trade.runtime.guard import BrokerOperation

            self._guard.assert_broker_operation(
                self._exchange,
                self._account_id,
                BrokerOperation.SUBSCRIBE,
            )
        if self._runner is None or self._runner.done():
            self._stopping = False
            self._terminal_error = None
            self._runner = asyncio.create_task(self._run())

    async def wait_until_ready(self, timeout_seconds: float = 10.0) -> None:
        """等待首个 WebSocket 连接建立，避免服务在断线状态发单。"""

        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        deadline = self._monotonic() + timeout_seconds
        while not self._connected:
            if self._terminal_error is not None:
                error = self._terminal_error
                self._terminal_error = None
                raise error
            if self._monotonic() >= deadline:
                raise BinanceFuturesUserStreamConnectionError(
                    "Binance Futures user stream did not become ready"
                )
            await asyncio.sleep(0.01)

    async def disconnect(self) -> None:
        """停止连接并安全关闭监听密钥。"""

        self._stopping = True
        connection = self._connection
        if connection is not None:
            await connection.close()
        if self._runner is not None:
            await self._runner
        if self._listen_key is not None:
            with suppress(Exception):
                await self._client.close_user_data_stream(self._listen_key)
            self._listen_key = None

    aclose = disconnect

    async def events(self) -> AsyncIterator[FuturesUserEvent]:
        """迭代事件；连接缺口和协议错误不会被静默吞掉。"""

        await self.connect()
        while True:
            if self._terminal_error is not None:
                error = self._terminal_error
                self._terminal_error = None
                raise error
            event = await self._queue.get()
            if event is None:
                if self._terminal_error is not None:
                    error = self._terminal_error
                    self._terminal_error = None
                    raise error
                return
            yield event

    async def _run(self) -> None:
        attempts = 0
        try:
            while not self._stopping:
                try:
                    self._listen_key = await self._client.start_user_data_stream()
                    self._epoch += 1
                    await self._notify("connecting")
                    url = self._stream_url(self._listen_key)
                    context = self._connector(
                        url,
                        ping_interval=20,
                        ping_timeout=20,
                        open_timeout=10,
                        close_timeout=10,
                        max_size=1_048_576,
                    )
                    async with context as connection:
                        self._connection = connection
                        self._connected = True
                        self._healthy = True
                        attempts = 0
                        started = self._monotonic()
                        await self._notify("connected")
                        self._keepalive_task = asyncio.create_task(self._keepalive())
                        while not self._stopping:
                            if self._monotonic() - started >= self._rotation_interval:
                                await self._notify("rotating", "scheduled")
                                break
                            try:
                                frame = await asyncio.wait_for(
                                    connection.recv(),
                                    timeout=min(60.0, self._keepalive_interval / 4),
                                )
                            except TimeoutError:
                                # 套接字库负责协议层 ping/pong；没有业务事件时继续
                                # 等待，不能把正常的安静市场误判成断线。
                                continue
                            event = parse_futures_user_event(
                                frame,
                                received_time_ms=self._clock_ms(),
                                connection_epoch=self._epoch,
                            )
                            event_order = (
                                event.event_time_ms,
                                event.transaction_time_ms
                                if event.transaction_time_ms is not None
                                else -1,
                            )
                            previous_order = self._last_event_order.get(_event_order_key(event))
                            if previous_order is not None and event_order < previous_order:
                                self._healthy = False
                                self._terminal_error = BinanceProtocolError(
                                    "Binance Futures user events arrived out of order"
                                )
                                await self._notify("degraded", "out_of_order")
                                self._stopping = True
                                break
                            self._last_event_order[_event_order_key(event)] = event_order
                            if not event.known:
                                self._healthy = False
                                await self._notify("degraded", "unknown_event")
                            self._enqueue(event)
                            if event.event_type == "listenKeyExpired":
                                self._healthy = False
                                await self._notify("degraded", "listenKeyExpired")
                                break
                except asyncio.CancelledError:
                    raise
                except BinanceProtocolError as exc:
                    self._healthy = False
                    self._terminal_error = exc
                    await self._notify("degraded", "malformed_event")
                    break
                except BinanceError as exc:
                    self._healthy = False
                    self._terminal_error = exc
                    await self._notify("degraded", "api_error")
                    break
                except (
                    BinanceTransportError,
                    ConnectionClosed,
                    ConnectionError,
                    TimeoutError,
                    EOFError,
                    OSError,
                ):
                    if self._stopping:
                        break
                    await self._notify("disconnected", "transport")
                    if attempts >= self._max_reconnect_attempts:
                        self._terminal_error = BinanceFuturesUserStreamConnectionError(
                            "Binance Futures user stream reconnect limit exhausted"
                        )
                        break
                    attempts += 1
                    await self._notify("reconnecting", "transport")
                    await self._sleep(self._jitter(self._reconnect_delay * (2 ** (attempts - 1))))
                finally:
                    if self._keepalive_task is not None:
                        self._keepalive_task.cancel()
                        await asyncio.gather(self._keepalive_task, return_exceptions=True)
                        self._keepalive_task = None
                    self._connection = None
                    self._connected = False
                if not self._stopping and self._listen_key is not None:
                    with suppress(Exception):
                        await self._client.close_user_data_stream(self._listen_key)
                    self._listen_key = None
                if self._stopping:
                    break
        finally:
            self._connected = False
            await self._notify("stopped")
            self._offer(None)

    async def _keepalive(self) -> None:
        while not self._stopping:
            await self._sleep(self._keepalive_interval)
            if self._listen_key is None or self._stopping:
                return
            try:
                await self._client.keepalive_user_data_stream(self._listen_key)
            except Exception:
                self._healthy = False
                await self._notify("degraded", "keepalive")
                connection = self._connection
                if connection is not None:
                    with suppress(Exception):
                        await connection.close()
                return

    def _stream_url(self, listen_key: str) -> str:
        # 当前私有端点使用查询参数，避免把密钥暴露到路径型日志中。
        events = ",".join(sorted(_KNOWN))
        return (
            f"{self._client.user_data_stream_base_url}?listenKey={quote(listen_key)}"
            f"&events={quote(events)}"
        )

    def _enqueue(self, event: FuturesUserEvent) -> None:
        self._offer(event)

    def _offer(self, event: FuturesUserEvent | None) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._healthy = False
            self._terminal_error = BinanceFuturesUserStreamConnectionError(
                "Binance Futures user stream event buffer is full"
            )

    async def _notify(self, kind: str, reason: str | None = None) -> None:
        if self._on_lifecycle is None:
            return
        callback = self._on_lifecycle(
            FuturesStreamLifecycle(kind, self._epoch, self._clock_ms(), reason)
        )
        if callback is not None:
            await callback


def _event_kind(event_type: str) -> str:
    return {
        "ORDER_TRADE_UPDATE": "order",
        "ALGO_UPDATE": "algo",
        "TRADE_LITE": "trade",
        "ACCOUNT_UPDATE": "account",
        "ACCOUNT_CONFIG_UPDATE": "config",
        "MARGIN_CALL": "margin_call",
        "CONDITIONAL_ORDER_TRIGGER_REJECT": "trigger_reject",
        "listenKeyExpired": "expired",
        "STRATEGY_UPDATE": "strategy",
        "GRID_UPDATE": "grid",
    }.get(event_type, "unknown")


def _event_order_key(event: FuturesUserEvent) -> tuple[str, str]:
    """按标的分开检查顺序，避免多标的事件的合法交错被误判。"""

    nested = event.payload.get("o")
    if isinstance(nested, Mapping) and isinstance(nested.get("s"), str):
        return event.event_type, nested["s"]
    symbol = event.payload.get("s")
    return event.event_type, symbol if isinstance(symbol, str) else "*"


def _validate_event_shape(event_type: str, payload: Mapping[str, Any]) -> None:
    """校验已知事件的外层结构，防止半截载荷进入 OMS。"""

    required_mapping = {
        "ORDER_TRADE_UPDATE": "o",
        "ALGO_UPDATE": "o",
        "ACCOUNT_UPDATE": "a",
        "CONDITIONAL_ORDER_TRIGGER_REJECT": "or",
        "STRATEGY_UPDATE": "su",
        "GRID_UPDATE": "gu",
    }
    key = required_mapping.get(event_type)
    if key is not None and not isinstance(payload.get(key), Mapping):
        raise BinanceProtocolError(
            f"Binance Futures {event_type} payload field {key!r} is malformed"
        )
    if event_type == "ACCOUNT_CONFIG_UPDATE" and not any(
        isinstance(payload.get(key), Mapping) for key in ("ac", "ai")
    ):
        raise BinanceProtocolError("Binance Futures ACCOUNT_CONFIG_UPDATE payload is malformed")
    if event_type == "MARGIN_CALL" and not isinstance(payload.get("p"), list):
        raise BinanceProtocolError("Binance Futures MARGIN_CALL payload is malformed")
    if event_type == "TRADE_LITE" and any(key not in payload for key in ("s", "i", "c")):
        raise BinanceProtocolError("Binance Futures TRADE_LITE payload is malformed")


def _integer(mapping: Mapping[str, Any], key: str) -> int:
    return _integer_value(mapping.get(key), key)


def _integer_value(value: object, key: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BinanceProtocolError(f"Binance Futures user field {key!r} is malformed")
    return value


def _sanitize_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in mapping.items():
        if key in {"apiKey", "secretKey", "signature", "listenKey"}:
            continue
        if isinstance(value, Mapping):
            output[str(key)] = _sanitize_mapping(value)
        elif isinstance(value, list):
            output[str(key)] = [
                _sanitize_mapping(item) if isinstance(item, Mapping) else item for item in value
            ]
        else:
            output[str(key)] = value
    return output


def _now_ms() -> int:
    return time.time_ns() // 1_000_000
