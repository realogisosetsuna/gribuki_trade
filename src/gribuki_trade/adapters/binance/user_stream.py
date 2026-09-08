"""经 WebSocket API 认证的 Binance 现货用户数据流。

当前 Binance WebSocket API 通过 ``userDataStream.subscribe.signature``
认证用户数据订阅，不使用旧版 REST ``listenKey`` 生命周期。本模块特意不提供
交易方法：套接字只能订阅并接收账户或订单通知。
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, TypeAlias

from websockets.asyncio.client import connect as websocket_connect
from websockets.exceptions import ConnectionClosed

from gribuki_trade.domain.orders import OrderStatus, Side

from .gateway import (
    BinanceConfigurationError,
    BinanceProtocolError,
    BinanceTransportError,
    sign_hmac_sha256,
)
from .models import BinanceCredentials, BinanceEnvironment

LIVE_USER_WS_API_URL = "wss://ws-api.binance.com:443/ws-api/v3"
TESTNET_USER_WS_API_URL = "wss://ws-api.testnet.binance.vision/ws-api/v3"

_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)\b(api[-_ ]?key|secret(?:[-_ ]?key)?|signature)\b\s*[:=]\s*[^\s,;&]+"
)


class BinanceUserStreamConnectionError(BinanceTransportError):
    """无法安全建立或恢复私有 WebSocket 连接。"""


class BinanceUserStreamAPIError(BinanceTransportError):
    """Binance 拒绝订阅，且错误信息中的凭据已脱敏。"""

    def __init__(self, *, status: int, code: int | None, message: str) -> None:
        self.status = status
        self.code = code
        self.message = message
        code_text = "unknown" if code is None else str(code)
        super().__init__(
            f"Binance User Data Stream error (status {status}, code {code_text}): {message}"
        )


class UserWebSocketConnection(Protocol):
    """数据流使用的最小认证套接字接口。"""

    async def send(self, message: str | bytes) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


UserWebSocketConnector: TypeAlias = Callable[
    ...,
    AbstractAsyncContextManager[UserWebSocketConnection],
]
Sleep: TypeAlias = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BinanceExecutionReport:
    """来自现货账户的类型化 ``executionReport`` 事件。"""

    subscription_id: int
    event_time_ms: int
    transaction_time_ms: int
    symbol: str
    client_order_id: str
    original_client_order_id: str | None
    side: Side
    order_type: str
    time_in_force: str
    original_quantity: Decimal
    order_price: Decimal
    stop_price: Decimal
    iceberg_quantity: Decimal
    order_list_id: int
    execution_type: str
    status: OrderStatus
    exchange_order_status: str
    reject_reason: str
    order_id: int
    last_executed_quantity: Decimal
    cumulative_filled_quantity: Decimal
    last_executed_price: Decimal
    commission_amount: Decimal
    commission_asset: str | None
    trade_id: int
    prevented_match_id: int | None
    execution_id: int
    is_order_on_book: bool
    is_maker_side: bool
    order_creation_time_ms: int
    cumulative_quote_quantity: Decimal
    last_quote_quantity: Decimal
    quote_order_quantity: Decimal
    working_time_ms: int | None
    self_trade_prevention_mode: str | None


@dataclass(frozen=True, slots=True)
class BinanceListStatus:
    """现货 OCO、OTO、OTOCO 订单列表状态事件。"""

    subscription_id: int
    event_time_ms: int
    transaction_time_ms: int
    order_list_id: int
    list_client_order_id: str
    contingency_type: str
    list_status_type: str
    list_order_status: str
    reject_reason: str
    symbol: str
    order_ids: tuple[int, ...]
    client_order_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BinanceUserBalance:
    asset: str
    free: Decimal
    locked: Decimal


@dataclass(frozen=True, slots=True)
class BinanceOutboundAccountPosition:
    subscription_id: int
    event_time_ms: int
    last_account_update_ms: int
    balances: tuple[BinanceUserBalance, ...]


@dataclass(frozen=True, slots=True)
class BinanceBalanceUpdate:
    subscription_id: int
    event_time_ms: int
    asset: str
    balance_delta: Decimal
    clear_time_ms: int


@dataclass(frozen=True, slots=True)
class BinanceEventStreamTerminated:
    subscription_id: int
    event_time_ms: int


BinanceUserDataEvent: TypeAlias = (
    BinanceExecutionReport
    | BinanceListStatus
    | BinanceOutboundAccountPosition
    | BinanceBalanceUpdate
    | BinanceEventStreamTerminated
)


def user_websocket_api_url(environment: BinanceEnvironment | str) -> str:
    """返回指定环境的私有 WebSocket API 端点。"""

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
        return LIVE_USER_WS_API_URL
    return TESTNET_USER_WS_API_URL


def signature_payload(params: Mapping[str, object]) -> str:
    """生成采用 ASCII 键顺序的规范 Binance WebSocket-API 载荷。

    始终排除 ``signature``。调用方用 HMAC-SHA256 对返回字符串签名，再把十六进制
    结果追加到请求参数。
    """

    keys: list[str] = []
    for key in params:
        if not isinstance(key, str):
            raise TypeError("Binance signing parameter names must be strings")
        try:
            key.encode("ascii")
        except UnicodeEncodeError:
            raise ValueError("Binance signing parameter names must be ASCII") from None
        keys.append(key)

    pairs: list[tuple[str, str]] = []
    for key in sorted(keys, key=lambda value: value.encode("ascii")):
        if key == "signature":
            continue
        value = params[key]
        if isinstance(value, bool):
            text = "true" if value else "false"
        elif isinstance(value, (str, int, Decimal)) and not isinstance(value, bool):
            text = str(value)
        else:
            raise TypeError(
                f"unsupported Binance signing parameter type: {type(value).__name__}"
            )
        pairs.append((key, text))
    return "&".join(f"{key}={value}" for key, value in pairs)


def parse_user_data_event(message: str | bytes) -> BinanceUserDataEvent:
    """解析一个文档规定的 ``{subscriptionId,event}`` 通知。"""

    decoded = _decode_frame(message)
    if "id" in decoded:
        raise BinanceProtocolError("Binance response frame is not a user-data event")
    subscription_id = _integer(decoded, "subscriptionId", minimum=0)
    event = decoded.get("event")
    if not isinstance(event, Mapping):
        raise BinanceProtocolError("Binance user-data event envelope is malformed")
    event_type = event.get("e")
    if event_type == "executionReport":
        return _parse_execution_report(subscription_id, event)
    if event_type == "listStatus":
        return _parse_list_status(subscription_id, event)
    if event_type == "outboundAccountPosition":
        return _parse_outbound_account_position(subscription_id, event)
    if event_type == "balanceUpdate":
        return _parse_balance_update(subscription_id, event)
    if event_type == "eventStreamTerminated":
        return BinanceEventStreamTerminated(
            subscription_id=subscription_id,
            event_time_ms=_integer(event, "E", minimum=0),
        )
    raise BinanceProtocolError("unsupported Binance user-data event type")


class BinanceSpotUserDataStream:
    """迭代已认证 Binance 现货账户或订单事件的异步迭代器。

    订阅请求与所有通知共用一个 WebSocket API 连接。含 ``id`` 的数据帧会路由到
    匹配的订阅请求；通知信封若先到达则进入缓冲区。重连次数受限，且每次重连都会
    重新执行签名订阅。
    """

    def __init__(
        self,
        credentials: BinanceCredentials,
        *,
        environment: BinanceEnvironment | str = BinanceEnvironment.TESTNET,
        allow_live: bool = False,
        connector: UserWebSocketConnector | None = None,
        clock_ms: Callable[[], int] | None = None,
        recv_window_ms: int = 5_000,
        ping_interval_seconds: float = 20.0,
        ping_timeout_seconds: float = 20.0,
        open_timeout_seconds: float = 10.0,
        close_timeout_seconds: float = 10.0,
        rotation_seconds: float = 23 * 60 * 60,
        monotonic: Callable[[], float] | None = None,
        max_reconnect_attempts: int = 3,
        reconnect_delay_seconds: float = 0.5,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
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
                "Binance LIVE User Data Stream is disabled; pass allow_live=True explicitly"
            )
        if not isinstance(credentials, BinanceCredentials):
            raise TypeError("credentials must be BinanceCredentials")
        if not 1 <= recv_window_ms <= 60_000:
            raise ValueError("recv_window_ms must be between 1 and 60000")
        if ping_interval_seconds <= 0 or ping_timeout_seconds <= 0:
            raise ValueError("WebSocket heartbeat intervals must be positive")
        if open_timeout_seconds <= 0 or close_timeout_seconds <= 0:
            raise ValueError("WebSocket timeouts must be positive")
        if rotation_seconds <= 0:
            raise ValueError("rotation_seconds must be positive")
        if (
            isinstance(max_reconnect_attempts, bool)
            or not isinstance(max_reconnect_attempts, int)
            or max_reconnect_attempts < 0
        ):
            raise ValueError("max_reconnect_attempts must be a non-negative integer")
        if reconnect_delay_seconds < 0:
            raise ValueError("reconnect_delay_seconds must not be negative")

        self._credentials = credentials
        self._environment = selected
        self._url = user_websocket_api_url(selected)
        self._connector = connector if connector is not None else websocket_connect
        self._clock_ms = clock_ms if clock_ms is not None else lambda: time.time_ns() // 1_000_000
        self._recv_window_ms = recv_window_ms
        self._ping_interval_seconds = ping_interval_seconds
        self._ping_timeout_seconds = ping_timeout_seconds
        self._open_timeout_seconds = open_timeout_seconds
        self._close_timeout_seconds = close_timeout_seconds
        self._rotation_seconds = rotation_seconds
        self._monotonic = monotonic if monotonic is not None else time.monotonic
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._sleep = sleep
        self._next_request_id = 1
        self._subscription_id: int | None = None
        self._connection_epoch = 0
        self._connected = False
        self._iterating = False
        self._stop_requested = False
        self._connection: UserWebSocketConnection | None = None

    def __repr__(self) -> str:
        return (
            f"BinanceSpotUserDataStream(environment={self.environment.value!r}, "
            f"connected={self.connected!r}, "
            f"subscription_id={self.subscription_id!r}, "
            "credentials=<redacted>)"
        )

    @property
    def environment(self) -> BinanceEnvironment:
        return self._environment

    @property
    def url(self) -> str:
        return self._url

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def subscription_id(self) -> int | None:
        return self._subscription_id

    @property
    def connection_epoch(self) -> int:
        """供下游缺口恢复使用的单调递增成功订阅计数。"""

        return self._connection_epoch

    def __aiter__(self) -> AsyncIterator[BinanceUserDataEvent]:
        return self.events()

    async def aclose(self) -> None:
        self._stop_requested = True
        connection = self._connection
        if connection is not None:
            await connection.close()

    async def events(self) -> AsyncIterator[BinanceUserDataEvent]:
        if self._iterating:
            raise RuntimeError("a Binance user-data stream supports only one active consumer")
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
                        connected_at = self._monotonic()
                        subscription_id, buffered = await self._subscribe(connection)
                        self._subscription_id = subscription_id
                        self._connection_epoch += 1
                        for event in buffered:
                            self._validate_subscription(event, subscription_id)
                            received_valid_event = True
                            reconnects = 0
                            yield event
                            if isinstance(event, BinanceEventStreamTerminated):
                                self._stop_requested = True
                                break
                        while not self._stop_requested:
                            if self._monotonic() - connected_at >= self._rotation_seconds:
                                break
                            frame = await connection.recv()
                            decoded = _decode_frame(frame)
                            if "id" in decoded:
                                raise BinanceProtocolError(
                                    "unexpected Binance response after user-data subscription"
                                )
                            event = _parse_decoded_user_event(decoded)
                            self._validate_subscription(event, subscription_id)
                            received_valid_event = True
                            reconnects = 0
                            yield event
                            if isinstance(event, BinanceEventStreamTerminated):
                                self._stop_requested = True
                                break
                except asyncio.CancelledError:
                    raise
                except (BinanceProtocolError, BinanceUserStreamAPIError):
                    raise
                except (ConnectionClosed, ConnectionError, TimeoutError, EOFError, OSError):
                    if self._stop_requested:
                        break
                    if reconnects >= self._max_reconnect_attempts:
                        raise BinanceUserStreamConnectionError(
                            "Binance user-data WebSocket reconnect limit exhausted"
                        ) from None
                    reconnects += 1
                    await self._sleep(
                        self._reconnect_delay_seconds * (2 ** (reconnects - 1))
                    )
                    continue
                finally:
                    self._connection = None
                    self._connected = False
                    self._subscription_id = None

                if self._stop_requested:
                    break
                if received_valid_event:
                    reconnects = 0
                if reconnects >= self._max_reconnect_attempts:
                    raise BinanceUserStreamConnectionError(
                        "Binance user-data WebSocket closed without a close exception"
                    )
                reconnects += 1
                await self._sleep(
                    self._reconnect_delay_seconds * (2 ** (reconnects - 1))
                )
        finally:
            self._connection = None
            self._connected = False
            self._subscription_id = None
            self._iterating = False

    async def _subscribe(
        self,
        connection: UserWebSocketConnection,
    ) -> tuple[int, tuple[BinanceUserDataEvent, ...]]:
        request_id = self._next_request_id
        self._next_request_id += 1
        params: dict[str, object] = {
            "apiKey": self._credentials.api_key,
            "recvWindow": self._recv_window_ms,
            "timestamp": self._clock_ms(),
        }
        signature = sign_hmac_sha256(
            self._credentials.secret_key,
            signature_payload(params),
        )
        params["signature"] = signature
        request = {
            "id": request_id,
            "method": "userDataStream.subscribe.signature",
            "params": params,
        }
        try:
            await connection.send(json.dumps(request, separators=(",", ":")))
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, ConnectionError, TimeoutError, EOFError, OSError):
            raise
        except Exception:
            # 注入的套接字可能在异常中包含已签名数据帧。
            raise BinanceUserStreamConnectionError(
                "Binance user-data subscription send failed"
            ) from None

        buffered: list[BinanceUserDataEvent] = []
        while True:
            frame = await connection.recv()
            decoded = _decode_frame(frame)
            if "id" not in decoded:
                buffered.append(_parse_decoded_user_event(decoded))
                continue
            if decoded.get("id") != request_id:
                raise BinanceProtocolError(
                    "Binance user-data response id does not match its request"
                )
            subscription_id = self._parse_subscription_response(
                decoded,
                signature=signature,
            )
            for event in buffered:
                self._validate_subscription(event, subscription_id)
            return subscription_id, tuple(buffered)

    def _parse_subscription_response(
        self,
        response: Mapping[str, Any],
        *,
        signature: str,
    ) -> int:
        status = response.get("status")
        if isinstance(status, bool) or not isinstance(status, int):
            raise BinanceProtocolError("Binance subscription response status is malformed")
        if not 200 <= status < 300:
            error = response.get("error")
            code: int | None = None
            message: object = "subscription rejected"
            if isinstance(error, Mapping):
                raw_code = error.get("code")
                if isinstance(raw_code, int) and not isinstance(raw_code, bool):
                    code = raw_code
                message = error.get("msg", message)
            safe = _sanitize_message(
                message,
                (self._credentials.api_key, self._credentials.secret_key, signature),
            )
            raise BinanceUserStreamAPIError(
                status=status,
                code=code,
                message=safe,
            )
        result = response.get("result")
        if not isinstance(result, Mapping):
            raise BinanceProtocolError("Binance subscription result is malformed")
        return _integer(result, "subscriptionId", minimum=0)

    @staticmethod
    def _validate_subscription(
        event: BinanceUserDataEvent,
        subscription_id: int,
    ) -> None:
        if event.subscription_id != subscription_id:
            raise BinanceProtocolError(
                "Binance event subscriptionId does not match the active subscription"
            )


def _decode_frame(message: str | bytes) -> Mapping[str, Any]:
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
    return decoded


def _parse_decoded_user_event(decoded: Mapping[str, Any]) -> BinanceUserDataEvent:
    # 避免重新编码，使 Decimal 来源字符串与发送值完全一致。
    if "id" in decoded:
        raise BinanceProtocolError("Binance response frame is not a user-data event")
    subscription_id = _integer(decoded, "subscriptionId", minimum=0)
    event = decoded.get("event")
    if not isinstance(event, Mapping):
        raise BinanceProtocolError("Binance user-data event envelope is malformed")
    event_type = event.get("e")
    if event_type == "executionReport":
        return _parse_execution_report(subscription_id, event)
    if event_type == "listStatus":
        return _parse_list_status(subscription_id, event)
    if event_type == "outboundAccountPosition":
        return _parse_outbound_account_position(subscription_id, event)
    if event_type == "balanceUpdate":
        return _parse_balance_update(subscription_id, event)
    if event_type == "eventStreamTerminated":
        return BinanceEventStreamTerminated(
            subscription_id=subscription_id,
            event_time_ms=_integer(event, "E", minimum=0),
        )
    raise BinanceProtocolError("unsupported Binance user-data event type")


def _parse_list_status(
    subscription_id: int,
    event: Mapping[str, Any],
) -> BinanceListStatus:
    orders_value = event.get("O")
    if not isinstance(orders_value, list):
        raise BinanceProtocolError("Binance listStatus orders are malformed")
    order_ids: list[int] = []
    client_order_ids: list[str] = []
    for item in orders_value:
        if not isinstance(item, Mapping):
            raise BinanceProtocolError("Binance listStatus order is malformed")
        order_ids.append(_integer(item, "i", minimum=0))
        client_order_ids.append(_required_string(item, "c"))
    return BinanceListStatus(
        subscription_id=subscription_id,
        event_time_ms=_integer(event, "E", minimum=0),
        transaction_time_ms=_integer(event, "T", minimum=0),
        order_list_id=_integer(event, "g", minimum=-1),
        list_client_order_id=_required_string(event, "c"),
        contingency_type=_required_string(event, "l"),
        list_status_type=_required_string(event, "L"),
        list_order_status=_required_string(event, "J"),
        reject_reason=_required_string(event, "r"),
        symbol=_symbol(event, "s"),
        order_ids=tuple(order_ids),
        client_order_ids=tuple(client_order_ids),
    )


def _parse_execution_report(
    subscription_id: int,
    event: Mapping[str, Any],
) -> BinanceExecutionReport:
    try:
        side = Side(_required_string(event, "S"))
    except ValueError:
        raise BinanceProtocolError("Binance executionReport side is malformed") from None
    exchange_status = _required_string(event, "X")
    return BinanceExecutionReport(
        subscription_id=subscription_id,
        event_time_ms=_integer(event, "E", minimum=0),
        transaction_time_ms=_integer(event, "T", minimum=0),
        symbol=_symbol(event, "s"),
        client_order_id=_required_string(event, "c"),
        original_client_order_id=_optional_string(event, "C", blank_as_none=True),
        side=side,
        order_type=_required_string(event, "o"),
        time_in_force=_required_string(event, "f"),
        original_quantity=_decimal(event, "q", minimum=Decimal("0")),
        order_price=_decimal(event, "p", minimum=Decimal("0")),
        stop_price=_decimal(event, "P", minimum=Decimal("0")),
        iceberg_quantity=_decimal(event, "F", minimum=Decimal("0")),
        order_list_id=_integer(event, "g", minimum=-1),
        execution_type=_required_string(event, "x"),
        status=_map_order_status(exchange_status),
        exchange_order_status=exchange_status,
        reject_reason=_required_string(event, "r"),
        order_id=_integer(event, "i", minimum=0),
        last_executed_quantity=_decimal(event, "l", minimum=Decimal("0")),
        cumulative_filled_quantity=_decimal(event, "z", minimum=Decimal("0")),
        last_executed_price=_decimal(event, "L", minimum=Decimal("0")),
        commission_amount=_decimal(event, "n", minimum=Decimal("0")),
        commission_asset=_nullable_string(event, "N"),
        trade_id=_integer(event, "t", minimum=-1),
        prevented_match_id=_optional_integer(event, "v", minimum=0),
        execution_id=_integer(event, "I", minimum=0),
        is_order_on_book=_boolean(event, "w"),
        is_maker_side=_boolean(event, "m"),
        order_creation_time_ms=_integer(event, "O", minimum=0),
        cumulative_quote_quantity=_decimal(event, "Z", minimum=Decimal("0")),
        last_quote_quantity=_decimal(event, "Y", minimum=Decimal("0")),
        quote_order_quantity=_decimal(event, "Q", minimum=Decimal("0")),
        working_time_ms=_optional_integer(event, "W", minimum=0),
        self_trade_prevention_mode=_optional_string(event, "V"),
    )


def _parse_outbound_account_position(
    subscription_id: int,
    event: Mapping[str, Any],
) -> BinanceOutboundAccountPosition:
    raw_balances = event.get("B")
    if not isinstance(raw_balances, list):
        raise BinanceProtocolError("Binance outboundAccountPosition balances are malformed")
    balances: list[BinanceUserBalance] = []
    for raw in raw_balances:
        if not isinstance(raw, Mapping):
            raise BinanceProtocolError(
                "Binance outboundAccountPosition balance is malformed"
            )
        balances.append(
            BinanceUserBalance(
                asset=_asset(raw, "a"),
                free=_decimal(raw, "f", minimum=Decimal("0")),
                locked=_decimal(raw, "l", minimum=Decimal("0")),
            )
        )
    return BinanceOutboundAccountPosition(
        subscription_id=subscription_id,
        event_time_ms=_integer(event, "E", minimum=0),
        last_account_update_ms=_integer(event, "u", minimum=0),
        balances=tuple(balances),
    )


def _parse_balance_update(
    subscription_id: int,
    event: Mapping[str, Any],
) -> BinanceBalanceUpdate:
    return BinanceBalanceUpdate(
        subscription_id=subscription_id,
        event_time_ms=_integer(event, "E", minimum=0),
        asset=_asset(event, "a"),
        balance_delta=_decimal(event, "d", minimum=None),
        clear_time_ms=_integer(event, "T", minimum=0),
    )


def _map_order_status(value: str) -> OrderStatus:
    return {
        "NEW": OrderStatus.ACCEPTED,
        "PENDING_NEW": OrderStatus.SUBMITTING,
        "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
        "FILLED": OrderStatus.FILLED,
        "PENDING_CANCEL": OrderStatus.CANCEL_PENDING,
        "CANCELED": OrderStatus.CANCELED,
        "REJECTED": OrderStatus.BROKER_REJECTED,
        "EXPIRED": OrderStatus.EXPIRED,
        "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
    }.get(value, OrderStatus.UNKNOWN)


def _integer(mapping: Mapping[str, Any], key: str, *, minimum: int) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
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
    minimum: Decimal | None,
) -> Decimal:
    value = mapping.get(key)
    if not isinstance(value, str):
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed") from None
    if not number.is_finite() or (minimum is not None and number < minimum):
        raise BinanceProtocolError(f"Binance user-data field {key!r} is out of range")
    return number


def _boolean(mapping: Mapping[str, Any], key: str) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _required_string(mapping: Mapping[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _optional_string(
    mapping: Mapping[str, Any],
    key: str,
    *,
    blank_as_none: bool = False,
) -> str | None:
    if key not in mapping:
        return None
    value = mapping.get(key)
    if not isinstance(value, str):
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    if not value and blank_as_none:
        return None
    if not value:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _nullable_string(mapping: Mapping[str, Any], key: str) -> str | None:
    if key not in mapping:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    value = mapping.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _symbol(mapping: Mapping[str, Any], key: str) -> str:
    value = _required_string(mapping, key)
    if not value.isascii() or not value.isalnum() or value != value.upper():
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _asset(mapping: Mapping[str, Any], key: str) -> str:
    value = _required_string(mapping, key)
    if not value.isascii() or not value.isalnum() or value != value.upper():
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _sanitize_message(message: object, secrets: Sequence[str]) -> str:
    text = str(message).replace("\r", " ").replace("\n", " ")
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    text = _SENSITIVE_ASSIGNMENT.sub(
        lambda match: f"{match.group(1)}=<redacted>",
        text,
    )
    return text[:500] or "subscription rejected"
