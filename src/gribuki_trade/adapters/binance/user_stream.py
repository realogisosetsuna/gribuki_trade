"""经 WebSocket API 认证的 Binance 现货用户数据流。

当前 Binance WebSocket API 通过 ``userDataStream.subscribe.signature``
认证用户数据订阅，不使用旧版 REST ``listenKey`` 生命周期。本模块特意不提供
交易方法：套接字只能订阅并接收账户或订单通知。
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol, TypeAlias

from websockets.asyncio.client import connect as websocket_connect
from websockets.exceptions import ConnectionClosed

from .gateway import (
    BinanceConfigurationError,
    BinanceProtocolError,
    BinanceTransportError,
    sign_hmac_sha256,
)
from .models import BinanceCredentials, BinanceEnvironment
from .user_stream_parsing import (
    LIVE_USER_WS_API_URL,
    TESTNET_USER_WS_API_URL,
    BinanceBalanceUpdate,
    BinanceEventStreamTerminated,
    BinanceExecutionReport,
    BinanceListStatus,
    BinanceOutboundAccountPosition,
    BinanceUserBalance,
    BinanceUserDataEvent,
    _decode_frame,
    _integer,
    _parse_decoded_user_event,
    _sanitize_message,
    parse_user_data_event,
    signature_payload,
    user_websocket_api_url,
)

__all__ = (
    "LIVE_USER_WS_API_URL",
    "TESTNET_USER_WS_API_URL",
    "BinanceBalanceUpdate",
    "BinanceEventStreamTerminated",
    "BinanceExecutionReport",
    "BinanceListStatus",
    "BinanceOutboundAccountPosition",
    "BinanceSpotUserDataStream",
    "BinanceUserBalance",
    "BinanceUserDataEvent",
    "BinanceUserStreamAPIError",
    "BinanceUserStreamConnectionError",
    "UserWebSocketConnection",
    "UserWebSocketConnector",
    "parse_user_data_event",
    "signature_payload",
    "user_websocket_api_url",
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
