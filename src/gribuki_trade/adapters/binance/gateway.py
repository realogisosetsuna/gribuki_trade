"""Binance 现货公共行情与签名测试/生产 REST 网关。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.ports.broker import BrokerEvent

from .errors import (
    BinanceAPIError as _BinanceAPIError,
)
from .errors import (
    BinanceConfigurationError as _BinanceConfigurationError,
)
from .errors import (
    BinanceError as _BinanceError,
)
from .errors import (
    BinanceProtocolError as _BinanceProtocolError,
)
from .errors import (
    BinanceTransportError as _BinanceTransportError,
)
from .errors import (
    BinanceUncertainResultError as _BinanceUncertainResultError,
)
from .http import (
    AsyncHttpTransport,
    HttpRequest,
    HttpResponse,
    UrllibAsyncHttpTransport,
)
from .models import (
    ORDER_STATUS_EVENT,
    BinanceAccount,
    BinanceBalance,
    BinanceCancelReplaceResult,
    BinanceCommissionComponent,
    BinanceCommissionDiscount,
    BinanceCommissionRate,
    BinanceCredentials,
    BinanceEnvironment,
    BinanceOrderListSnapshot,
    BinanceOrderSnapshot,
    BinanceOrderUpdate,
    BinanceRateLimitUsage,
    BinanceSpotOcoRequest,
    BinanceSpotOrderLeg,
    BinanceSpotOtocoRequest,
    BinanceSpotOtoRequest,
    BinanceTrade,
    Kline,
    OrderBookLevel,
    OrderBookSnapshot,
    TickerPrice,
)
from .rate_limit import parse_rate_limit_usage
from .request_builder import encode_request
from .rules import (
    BinanceValidationError,
    SymbolRules,
    decimal_from_api,
    decimal_to_fixed,
)
from .spot_order_params import (
    CLIENT_ORDER_ID as _CLIENT_ORDER_ID,
)
from .spot_order_params import (
    append_optional as _append_optional,
)
from .spot_order_params import (
    list_leg_params as _list_leg_params,
)
from .spot_order_params import (
    optional_text as _optional_text,
)
from .spot_order_params import (
    order_type as _order_type,
)
from .spot_order_params import (
    positive_decimal as _positive_decimal,
)
from .spot_order_params import (
    prefixed_leg_params as _prefixed_leg_params,
)
from .spot_order_params import (
    require_client_order_id as _require_client_order_id,
)
from .spot_order_params import (
    spot_order_params as _spot_order_params,
)
from .spot_order_params import (
    validate_leg as _validate_leg,
)
from .spot_order_params import (
    validate_list_response_type as _validate_list_response_type,
)
from .spot_parsing import (
    api_code as _api_code_value,
)
from .spot_parsing import (
    map_order_status,
    parse_commission_component,
    parse_kline,
    parse_levels,
    parse_order_snapshot,
    parse_trade,
    sanitize_message,
)
from .spot_parsing import (
    normalize_symbol as _normalize_symbol_value,
)
from .spot_parsing import (
    optional_integer as _optional_integer_value,
)
from .spot_parsing import (
    parameter_text as _parameter_text_value,
)
from .spot_parsing import (
    sign_hmac_sha256 as _sign_hmac_sha256,
)

# 保留旧网关导出的错误类和签名函数，避免下游适配器因拆分而改变导入路径。
BinanceAPIError = _BinanceAPIError
BinanceConfigurationError = _BinanceConfigurationError
BinanceError = _BinanceError
BinanceProtocolError = _BinanceProtocolError
BinanceTransportError = _BinanceTransportError
BinanceUncertainResultError = _BinanceUncertainResultError


def sign_hmac_sha256(secret_key: str, payload: str) -> str:
    """兼容旧网关导出的 Binance HMAC-SHA256 签名函数。"""

    return _sign_hmac_sha256(secret_key, payload)


@dataclass(slots=True)
class _TrackedOrder:
    order: OrderIntent
    update: BinanceOrderUpdate
    submission_attempted: bool = False
    cancellation_attempted: bool = False


class BinanceSpotGateway:
    """异步 Binance 现货 REST 适配器。

    公共方法从不需要凭据。签名方法使用 HMAC-SHA256 和 ``X-MBX-APIKEY``
    请求头。除非明确提供 ``allow_live=True``，否则拒绝构造生产环境实例。

    网关绝不自动重试提交或取消请求。传输失败、HTTP 5xx 或 Binance ``-1007``
    会产生 ``UNKNOWN`` 订单事件；调用方必须通过 :meth:`query_order` 对账。
    """

    def __init__(
        self,
        *,
        environment: BinanceEnvironment | str = BinanceEnvironment.TESTNET,
        credentials: BinanceCredentials | None = None,
        transport: AsyncHttpTransport | None = None,
        allow_live: bool = False,
        recv_window_ms: int = 5_000,
        timeout_seconds: float = 10.0,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        try:
            selected_environment = (
                environment
                if isinstance(environment, BinanceEnvironment)
                else BinanceEnvironment(str(environment).upper())
            )
        except ValueError:
            raise BinanceConfigurationError(
                f"unknown Binance environment: {environment!r}"
            ) from None
        if selected_environment is BinanceEnvironment.LIVE and not allow_live:
            raise BinanceConfigurationError(
                "Binance LIVE is disabled; pass allow_live=True explicitly to enable it"
            )
        if not 1 <= recv_window_ms <= 60_000:
            raise ValueError("recv_window_ms must be between 1 and 60000")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._environment = selected_environment
        self._credentials = credentials
        self._transport = transport if transport is not None else UrllibAsyncHttpTransport()
        self._recv_window_ms = recv_window_ms
        self._timeout_seconds = timeout_seconds
        self._clock_ms = clock_ms if clock_ms is not None else lambda: time.time_ns() // 1_000_000
        self._server_time_offset_ms = 0
        self._last_time_sync_rtt_ms: int | None = None
        self._connected = False
        self._events: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._orders: dict[str, _TrackedOrder] = {}
        self._standalone_cancel_results: dict[tuple[str, str], BinanceOrderSnapshot] = {}
        self._symbol_rules: dict[str, SymbolRules] = {}
        self._rate_limit_usage = BinanceRateLimitUsage()
        self._lock = asyncio.Lock()

    def __repr__(self) -> str:
        return (
            f"BinanceSpotGateway(environment={self.environment.value!r}, "
            f"base_url={self.base_url!r}, connected={self.connected!r}, "
            f"credentials_configured={self._credentials is not None!r})"
        )

    @property
    def environment(self) -> BinanceEnvironment:
        return self._environment

    @property
    def base_url(self) -> str:
        return self._environment.base_url

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def rate_limit_usage(self) -> BinanceRateLimitUsage:
        """返回最近一次交易所限流计数器。

        Binance 按 IP 应用请求权重限制，按账户应用订单限制。持续暴露这些计数器
        可让引擎在 HTTP 429/418 之前关闭失败，而不是把限流视为普通瞬态错误。
        """

        return self._rate_limit_usage

    async def connect(self) -> None:
        """启用订单变更，但不发起网络请求。"""

        async with self._lock:
            self._connected = True

    async def disconnect(self) -> None:
        """禁用新的订单变更，同时保留对账状态。"""

        async with self._lock:
            self._connected = False

    async def events(self) -> AsyncIterator[BrokerEvent]:
        while True:
            yield await self._events.get()

    def order_update(self, client_order_id: str) -> BinanceOrderUpdate | None:
        tracked = self._orders.get(client_order_id)
        return None if tracked is None else tracked.update

    async def ping(self) -> None:
        payload = await self._request_json("GET", "/api/v3/ping")
        if not isinstance(payload, Mapping):
            raise BinanceProtocolError("Binance ping response must be an object")

    async def server_time(self) -> int:
        payload = await self._request_json("GET", "/api/v3/time")
        mapping = self._require_mapping(payload, "server time")
        try:
            return int(mapping["serverTime"])
        except (KeyError, TypeError, ValueError):
            raise BinanceProtocolError("Binance server time response is malformed") from None

    @property
    def server_time_offset_ms(self) -> int:
        """最近测得的 ``服务器 - 本地`` 时钟偏移，单位为毫秒。"""

        return self._server_time_offset_ms

    @property
    def last_time_sync_rtt_ms(self) -> int | None:
        """最近一次服务器时钟采样的往返延迟，单位为毫秒。"""

        return self._last_time_sync_rtt_ms

    async def synchronize_time(self) -> int:
        """使用本地请求中点测量 Binance 时钟偏移。

        这样无需修改操作系统时钟，就能使签名请求保持在 ``recvWindow`` 内。
        可在启动时以及 Binance 明确以 ``-1021`` 拒绝时间戳后安全调用。
        """

        started_ms = self._clock_ms()
        exchange_ms = await self.server_time()
        finished_ms = self._clock_ms()
        self._last_time_sync_rtt_ms = max(0, finished_ms - started_ms)
        local_midpoint_ms = started_ms + (finished_ms - started_ms) // 2
        self._server_time_offset_ms = exchange_ms - local_midpoint_ms
        return self._server_time_offset_ms

    async def exchange_info(self, symbol: str | None = None) -> dict[str, Any]:
        params: tuple[tuple[str, object], ...] = ()
        normalized_symbol: str | None = None
        if symbol is not None:
            normalized_symbol = self._normalize_symbol(symbol)
            params = (("symbol", normalized_symbol),)
        payload = await self._request_json("GET", "/api/v3/exchangeInfo", params=params)
        mapping = self._require_mapping(payload, "exchangeInfo")
        result = dict(mapping)
        self.cache_exchange_info(result)
        if normalized_symbol is not None and normalized_symbol not in self._symbol_rules:
            raise BinanceProtocolError(
                f"exchangeInfo response did not contain requested symbol {normalized_symbol}"
            )
        return result

    def cache_exchange_info(self, payload: Mapping[str, Any]) -> tuple[SymbolRules, ...]:
        """解析并缓存交易代码过滤器，供启动和离线测试使用。"""

        symbols = payload.get("symbols")
        if not isinstance(symbols, list):
            raise BinanceProtocolError("exchangeInfo symbols must be a list")
        parsed: list[SymbolRules] = []
        for value in symbols:
            if not isinstance(value, dict):
                raise BinanceProtocolError("exchangeInfo contains a malformed symbol")
            try:
                rules = SymbolRules.from_exchange_info(value)
            except BinanceValidationError as exc:
                raise BinanceProtocolError(str(exc)) from None
            self._symbol_rules[rules.symbol] = rules
            parsed.append(rules)
        return tuple(parsed)

    async def symbol_rules(self, symbol: str, *, refresh: bool = False) -> SymbolRules:
        normalized = self._normalize_symbol(symbol)
        if refresh or normalized not in self._symbol_rules:
            await self.exchange_info(normalized)
        return self._symbol_rules[normalized]

    async def ticker_price(self, symbol: str) -> TickerPrice:
        normalized = self._normalize_symbol(symbol)
        payload = await self._request_json(
            "GET", "/api/v3/ticker/price", params=(("symbol", normalized),)
        )
        mapping = self._require_mapping(payload, "ticker price")
        try:
            response_symbol = str(mapping["symbol"])
            price = decimal_from_api(mapping["price"], "price")
        except (KeyError, BinanceValidationError):
            raise BinanceProtocolError("Binance ticker price response is malformed") from None
        return TickerPrice(symbol=response_symbol, price=price)

    get_ticker_price = ticker_price

    async def order_book(self, symbol: str, *, limit: int = 100) -> OrderBookSnapshot:
        if limit not in {5, 10, 20, 50, 100, 500, 1_000, 5_000}:
            raise ValueError("unsupported Binance order-book limit")
        normalized = self._normalize_symbol(symbol)
        payload = await self._request_json(
            "GET",
            "/api/v3/depth",
            params=(("symbol", normalized), ("limit", limit)),
        )
        mapping = self._require_mapping(payload, "order book")
        try:
            update_id = int(mapping["lastUpdateId"])
            bids = self._parse_levels(mapping["bids"], "bids")
            asks = self._parse_levels(mapping["asks"], "asks")
        except (KeyError, TypeError, ValueError, BinanceValidationError):
            raise BinanceProtocolError("Binance order book response is malformed") from None
        return OrderBookSnapshot(
            symbol=normalized,
            last_update_id=update_id,
            bids=bids,
            asks=asks,
        )

    get_order_book = order_book

    async def klines(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> tuple[Kline, ...]:
        if not interval or len(interval) > 4:
            raise ValueError("invalid Binance kline interval")
        if not 1 <= limit <= 1_000:
            raise ValueError("kline limit must be between 1 and 1000")
        params: list[tuple[str, object]] = [
            ("symbol", self._normalize_symbol(symbol)),
            ("interval", interval),
            ("limit", limit),
        ]
        if start_time_ms is not None:
            params.append(("startTime", start_time_ms))
        if end_time_ms is not None:
            params.append(("endTime", end_time_ms))
        payload = await self._request_json("GET", "/api/v3/klines", params=tuple(params))
        if not isinstance(payload, list):
            raise BinanceProtocolError("Binance klines response must be a list")
        try:
            return tuple(self._parse_kline(item) for item in payload)
        except (TypeError, ValueError, IndexError, BinanceValidationError):
            raise BinanceProtocolError("Binance klines response is malformed") from None

    get_klines = klines

    async def validate_order_on_exchange(self, order: OrderIntent) -> None:
        """通过 Binance 验证限价单，但不实际下单。

        Binance 的 ``/api/v3/order/test`` 端点会检查签名、账户、交易代码过滤器
        和订单架构，但绝不进入撮合引擎。此方法刻意不在本地订单管理系统中
        预留客户端订单编号。
        """

        if not self._connected:
            raise ConnectionError("Binance gateway is not connected")
        self._require_credentials()
        if not _CLIENT_ORDER_ID.fullmatch(order.client_order_id):
            raise BinanceValidationError("client_order_id must be 1-36 Binance-safe characters")
        rules = await self.symbol_rules(order.symbol)
        rules.validate_limit_order(quantity=order.quantity, price=order.limit_price)
        await self._request_json(
            "POST",
            "/api/v3/order/test",
            params=(
                ("symbol", rules.symbol),
                ("side", order.side.value),
                ("type", "LIMIT"),
                ("timeInForce", "GTC"),
                ("quantity", decimal_to_fixed(order.quantity)),
                ("price", decimal_to_fixed(order.limit_price)),
                ("newClientOrderId", order.client_order_id),
                ("newOrderRespType", "RESULT"),
            ),
            signed=True,
            execution_sensitive=False,
        )

    test_order = validate_order_on_exchange

    async def submit_spot_order(
        self,
        *,
        symbol: str,
        side: Side,
        order_type: str,
        quantity: Decimal | None = None,
        quote_order_quantity: Decimal | None = None,
        price: Decimal | None = None,
        stop_price: Decimal | None = None,
        trailing_delta: int | None = None,
        time_in_force: str | None = None,
        client_order_id: str | None = None,
        iceberg_quantity: Decimal | None = None,
        strategy_id: int | None = None,
        strategy_type: int | None = None,
        self_trade_prevention_mode: str | None = None,
        response_type: str = "RESULT",
    ) -> BinanceOrderSnapshot:
        """提交任意 Binance Spot 单笔订单，包括止损、止盈和跟踪止盈。

        该接口保留交易所的原生 ``STOP_LOSS``/``TAKE_PROFIT`` 与
        ``trailingDelta`` 语义，策略层只需要构造结构化参数即可；返回值始终
        归一化为 :class:`BinanceOrderSnapshot`，便于后续对账。
        """

        self._require_order_connection()
        normalized = self._normalize_symbol(symbol)
        params = self._spot_order_params(
            symbol=normalized,
            side=side,
            order_type=order_type,
            quantity=quantity,
            quote_order_quantity=quote_order_quantity,
            price=price,
            stop_price=stop_price,
            trailing_delta=trailing_delta,
            time_in_force=time_in_force,
            client_order_id=client_order_id,
            iceberg_quantity=iceberg_quantity,
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            self_trade_prevention_mode=self_trade_prevention_mode,
            response_type=response_type,
        )
        payload = await self._request_json(
            "POST",
            "/api/v3/order",
            params=tuple(params),
            signed=True,
            execution_sensitive=True,
        )
        return self._snapshot_from_payload(payload, fallback_symbol=normalized)

    async def submit_oco(self, request: BinanceSpotOcoRequest) -> BinanceOrderListSnapshot:
        """提交 OCO（止盈/止损互斥）订单列表。"""

        self._require_order_connection()
        symbol = self._normalize_symbol(request.symbol)
        self._validate_list_response_type(request.response_type)
        quantity = self._positive_decimal(request.quantity, "quantity")
        if request.above.side is not request.side or request.below.side is not request.side:
            raise ValueError("OCO legs must use the request side")
        params: list[tuple[str, object]] = [
            ("symbol", symbol),
            ("side", request.side.value),
            ("quantity", decimal_to_fixed(quantity)),
            ("aboveType", self._order_type(request.above.order_type)),
            ("belowType", self._order_type(request.below.order_type)),
        ]
        params.extend(self._list_leg_params("above", request.above, quantity=quantity))
        params.extend(self._list_leg_params("below", request.below, quantity=quantity))
        self._append_optional(params, "listClientOrderId", request.list_client_order_id)
        params.append(("newOrderRespType", request.response_type.upper()))
        payload = await self._request_json(
            "POST",
            "/api/v3/orderList/oco",
            params=tuple(params),
            signed=True,
            execution_sensitive=True,
        )
        return self._order_list_from_payload(payload, fallback_symbol=symbol)

    async def submit_oto(self, request: BinanceSpotOtoRequest) -> BinanceOrderListSnapshot:
        """提交 OTO：working 成交后自动激活 pending。"""

        self._require_order_connection()
        symbol = self._normalize_symbol(request.symbol)
        self._validate_list_response_type(request.response_type)
        params: list[tuple[str, object]] = [("symbol", symbol)]
        params.extend(self._prefixed_leg_params("working", request.working, required=True))
        params.extend(self._prefixed_leg_params("pending", request.pending, required=True))
        self._append_optional(params, "listClientOrderId", request.list_client_order_id)
        params.append(("newOrderRespType", request.response_type.upper()))
        payload = await self._request_json(
            "POST",
            "/api/v3/orderList/oto",
            params=tuple(params),
            signed=True,
            execution_sensitive=True,
        )
        return self._order_list_from_payload(payload, fallback_symbol=symbol)

    async def submit_otoco(self, request: BinanceSpotOtocoRequest) -> BinanceOrderListSnapshot:
        """提交 OTOCO：working 成交后自动激活一组 OCO 止盈/止损。"""

        self._require_order_connection()
        symbol = self._normalize_symbol(request.symbol)
        self._validate_list_response_type(request.response_type)
        if request.pending_above.side is not request.pending_below.side:
            raise ValueError("OTOCO pending legs must use the same side")
        params: list[tuple[str, object]] = [("symbol", symbol)]
        params.extend(self._prefixed_leg_params("working", request.working, required=True))
        if request.pending_above.side is not request.pending_below.side:
            raise ValueError("OTOCO pending legs must use the same side")
        if request.pending_above.quantity != request.pending_below.quantity:
            raise ValueError("OTOCO pending legs must use the same quantity")
        params.extend(
            (
                ("pendingSide", request.pending_above.side.value),
                ("pendingQuantity", decimal_to_fixed(request.pending_above.quantity)),
                ("pendingAboveType", self._order_type(request.pending_above.order_type)),
                ("pendingBelowType", self._order_type(request.pending_below.order_type)),
            )
        )
        params.extend(
            self._prefixed_leg_params(
                "pendingAbove",
                request.pending_above,
                required=False,
                include_type=False,
                include_side=False,
                include_quantity=False,
            )
        )
        params.extend(
            self._prefixed_leg_params(
                "pendingBelow",
                request.pending_below,
                required=False,
                include_type=False,
                include_side=False,
                include_quantity=False,
            )
        )
        self._append_optional(params, "listClientOrderId", request.list_client_order_id)
        params.append(("newOrderRespType", request.response_type.upper()))
        payload = await self._request_json(
            "POST",
            "/api/v3/orderList/otoco",
            params=tuple(params),
            signed=True,
            execution_sensitive=True,
        )
        return self._order_list_from_payload(payload, fallback_symbol=symbol)

    async def cancel_order_list(
        self,
        symbol: str,
        *,
        order_list_id: int | None = None,
        list_client_order_id: str | None = None,
        new_client_order_id: str | None = None,
    ) -> BinanceOrderListSnapshot:
        """取消整组现货 OCO/OTO/OTOCO 订单。"""

        self._require_order_connection()
        if (order_list_id is None) == (list_client_order_id is None):
            raise ValueError("provide exactly one of order_list_id or list_client_order_id")
        normalized = self._normalize_symbol(symbol)
        params: list[tuple[str, object]] = [("symbol", normalized)]
        if order_list_id is not None:
            if order_list_id < 0:
                raise ValueError("order_list_id must be non-negative")
            params.append(("orderListId", order_list_id))
        else:
            self._require_client_order_id(list_client_order_id)
            params.append(("listClientOrderId", list_client_order_id))
        if new_client_order_id is not None:
            self._require_client_order_id(new_client_order_id)
        self._append_optional(params, "newClientOrderId", new_client_order_id)
        payload = await self._request_json(
            "DELETE",
            "/api/v3/orderList",
            params=tuple(params),
            signed=True,
            execution_sensitive=True,
        )
        return self._order_list_from_payload(payload, fallback_symbol=normalized)

    async def cancel_all_open_orders(self, symbol: str) -> tuple[BinanceOrderSnapshot, ...]:
        """取消标的全部未结订单（包括订单列表成员）。"""

        self._require_order_connection()
        normalized = self._normalize_symbol(symbol)
        payload = await self._request_json(
            "DELETE",
            "/api/v3/openOrders",
            params=(("symbol", normalized),),
            signed=True,
            execution_sensitive=True,
        )
        return self._parse_order_list(payload, fallback_symbol=normalized)

    async def open_order_lists(self) -> tuple[BinanceOrderListSnapshot, ...]:
        """查询当前打开的现货订单列表，用于 OCO/OTO/OTOCO 恢复。"""

        self._require_order_connection()
        payload = await self._request_json("GET", "/api/v3/openOrderLists", signed=True)
        if not isinstance(payload, list):
            raise BinanceProtocolError("Binance open order lists response must be a list")
        if any(not isinstance(item, Mapping) for item in payload):
            raise BinanceProtocolError("Binance open order list item is malformed")
        return tuple(
            self._order_list_from_payload(item, fallback_symbol=str(item.get("symbol", "")))
            for item in payload
        )

    async def all_order_lists(
        self,
        *,
        from_id: int | None = None,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> tuple[BinanceOrderListSnapshot, ...]:
        """查询现货订单列表历史，供重启对账使用。"""

        self._require_order_connection()
        if not 1 <= limit <= 1_000:
            raise ValueError("limit must be between 1 and 1000")
        params: list[tuple[str, object]] = [("limit", limit)]
        if from_id is not None:
            if from_id < 0:
                raise ValueError("from_id must be non-negative")
            params.append(("fromId", from_id))
        if start_time_ms is not None:
            params.append(("startTime", start_time_ms))
        if end_time_ms is not None:
            params.append(("endTime", end_time_ms))
        payload = await self._request_json(
            "GET", "/api/v3/allOrderList", params=tuple(params), signed=True
        )
        if not isinstance(payload, list):
            raise BinanceProtocolError("Binance all order lists response must be a list")
        if any(not isinstance(item, Mapping) for item in payload):
            raise BinanceProtocolError("Binance order list history item is malformed")
        return tuple(
            self._order_list_from_payload(item, fallback_symbol=str(item.get("symbol", "")))
            for item in payload
        )

    async def cancel_replace(
        self,
        *,
        symbol: str,
        cancel_order_id: int | None = None,
        cancel_client_order_id: str | None = None,
        new_order: BinanceSpotOrderLeg,
        cancel_replace_mode: str = "STOP_ON_FAILURE",
        cancel_restrictions: str | None = None,
        response_type: str = "RESULT",
    ) -> BinanceCancelReplaceResult:
        """原子地撤销并替换订单，适合策略动态移动止盈/止损。"""

        self._require_order_connection()
        if (cancel_order_id is None) == (cancel_client_order_id is None):
            raise ValueError("provide exactly one cancel_order_id or cancel_client_order_id")
        if cancel_order_id is not None and cancel_order_id < 0:
            raise ValueError("cancel_order_id must be non-negative")
        if cancel_client_order_id is not None:
            self._require_client_order_id(cancel_client_order_id)
        mode = cancel_replace_mode.upper()
        if mode not in {"STOP_ON_FAILURE", "ALLOW_FAILURE"}:
            raise ValueError("cancel_replace_mode must be STOP_ON_FAILURE or ALLOW_FAILURE")
        self._validate_list_response_type(response_type)
        normalized = self._normalize_symbol(symbol)
        params = self._spot_order_params(
            symbol=normalized,
            side=new_order.side,
            order_type=new_order.order_type,
            quantity=new_order.quantity,
            quote_order_quantity=new_order.quote_order_quantity,
            price=new_order.price,
            stop_price=new_order.stop_price,
            trailing_delta=new_order.trailing_delta,
            time_in_force=new_order.time_in_force,
            client_order_id=new_order.client_order_id,
            iceberg_quantity=new_order.iceberg_quantity,
            strategy_id=new_order.strategy_id,
            strategy_type=new_order.strategy_type,
            self_trade_prevention_mode=new_order.self_trade_prevention_mode,
            response_type=response_type,
        )
        params.extend(
            (
                ("cancelReplaceMode", mode),
                ("newOrderRespType", response_type.upper()),
            )
        )
        if cancel_order_id is not None:
            params.append(("cancelOrderId", cancel_order_id))
        else:
            params.append(("cancelOrigClientOrderId", cancel_client_order_id))
        self._append_optional(params, "cancelRestrictions", cancel_restrictions)
        payload = await self._request_json(
            "POST",
            "/api/v3/order/cancelReplace",
            params=tuple(params),
            signed=True,
            execution_sensitive=True,
        )
        mapping = self._require_mapping(payload, "cancel-replace")
        cancel_payload = mapping.get("cancelResponse")
        new_payload = mapping.get("newOrderResponse")
        return BinanceCancelReplaceResult(
            cancel_result=self._optional_text(mapping.get("cancelResult")),
            new_order_result=self._optional_text(mapping.get("newOrderResult")),
            cancel_response=(
                self._snapshot_from_payload(cancel_payload, fallback_symbol=normalized)
                if isinstance(cancel_payload, Mapping)
                else None
            ),
            new_order_response=(
                self._snapshot_from_payload(new_payload, fallback_symbol=normalized)
                if isinstance(new_payload, Mapping)
                else None
            ),
        )

    async def amend_order_keep_priority(
        self,
        *,
        symbol: str,
        new_quantity: Decimal,
        order_id: int | None = None,
        client_order_id: str | None = None,
        new_client_order_id: str | None = None,
    ) -> BinanceOrderSnapshot:
        """仅减少未成交数量并保持订单队列优先级。"""

        self._require_order_connection()
        if (order_id is None) == (client_order_id is None):
            raise ValueError("provide exactly one order_id or client_order_id")
        if order_id is not None and order_id < 0:
            raise ValueError("order_id must be non-negative")
        if client_order_id is not None:
            self._require_client_order_id(client_order_id)
        normalized = self._normalize_symbol(symbol)
        quantity = self._positive_decimal(new_quantity, "new_quantity")
        params: list[tuple[str, object]] = [
            ("symbol", normalized),
            ("newQty", decimal_to_fixed(quantity)),
        ]
        self._append_optional(params, "orderId", order_id)
        self._append_optional(params, "origClientOrderId", client_order_id)
        if new_client_order_id is not None:
            self._require_client_order_id(new_client_order_id)
        self._append_optional(params, "newClientOrderId", new_client_order_id)
        payload = await self._request_json(
            "PUT",
            "/api/v3/order/amend/keepPriority",
            params=tuple(params),
            signed=True,
            execution_sensitive=True,
        )
        mapping = self._require_mapping(payload, "amended order")
        amended = mapping.get("amendedOrder", mapping)
        return self._snapshot_from_payload(amended, fallback_symbol=normalized)

    async def submit_order(self, order: OrderIntent) -> None:
        """验证并提交一笔幂等的撤销前有效限价单。"""

        async with self._lock:
            existing = self._orders.get(order.client_order_id)
            if existing is not None:
                if existing.order != order:
                    raise ValueError(
                        f"client_order_id {order.client_order_id!r} is already used "
                        "for a different order"
                    )
                # 包括 UNKNOWN：必须先对账，绝不能盲目重新提交。
                return
            if not self._connected:
                update = BinanceOrderUpdate(
                    order=order,
                    status=OrderStatus.BROKER_REJECTED,
                    reason="Binance gateway is not connected",
                )
                self._orders[order.client_order_id] = _TrackedOrder(order, update)
                self._publish(update)
                return
            self._require_credentials()

            if not _CLIENT_ORDER_ID.fullmatch(order.client_order_id):
                update = BinanceOrderUpdate(
                    order=order,
                    status=OrderStatus.LOCAL_REJECTED,
                    reason="client_order_id must be 1-36 Binance-safe characters",
                )
                self._orders[order.client_order_id] = _TrackedOrder(order, update)
                self._publish(update)
                return

            rules = await self.symbol_rules(order.symbol)
            try:
                rules.validate_limit_order(quantity=order.quantity, price=order.limit_price)
            except (BinanceValidationError, TypeError) as exc:
                update = BinanceOrderUpdate(
                    order=order,
                    status=OrderStatus.LOCAL_REJECTED,
                    reason=str(exc),
                )
                self._orders[order.client_order_id] = _TrackedOrder(order, update)
                self._publish(update)
                return

            initial = BinanceOrderUpdate(order=order, status=OrderStatus.SUBMITTING)
            tracked = _TrackedOrder(order=order, update=initial, submission_attempted=True)
            self._orders[order.client_order_id] = tracked
            params: tuple[tuple[str, object], ...] = (
                ("symbol", rules.symbol),
                ("side", order.side.value),
                ("type", "LIMIT"),
                ("timeInForce", "GTC"),
                ("quantity", decimal_to_fixed(order.quantity)),
                ("price", decimal_to_fixed(order.limit_price)),
                ("newClientOrderId", order.client_order_id),
                ("newOrderRespType", "RESULT"),
            )
            try:
                payload = await self._request_json(
                    "POST",
                    "/api/v3/order",
                    params=params,
                    signed=True,
                    execution_sensitive=True,
                )
                snapshot = self._snapshot_from_payload(payload, fallback_symbol=rules.symbol)
            except (BinanceUncertainResultError, BinanceTransportError, BinanceProtocolError):
                self._record_update(
                    tracked,
                    BinanceOrderUpdate(
                        order=order,
                        status=OrderStatus.UNKNOWN,
                        reason="submission outcome unknown; reconcile with query_order",
                    ),
                )
                return
            except BinanceAPIError as exc:
                self._record_update(
                    tracked,
                    BinanceOrderUpdate(
                        order=order,
                        status=OrderStatus.BROKER_REJECTED,
                        reason=str(exc),
                        error_code=exc.code,
                    ),
                )
                return

            self._record_snapshot(tracked, snapshot)

    async def cancel_order(self, client_order_id: str) -> None:
        """取消本地跟踪的订单，且不自动重试。"""

        async with self._lock:
            tracked = self._orders.get(client_order_id)
            if tracked is None:
                raise KeyError(f"unknown client_order_id: {client_order_id!r}")
            await self._cancel_order_by_client_id_locked(
                tracked.order.symbol,
                client_order_id,
            )

    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> BinanceOrderSnapshot:
        """按持久交易所身份取消订单，即使进程已经重启也可执行。

        与 :meth:`cancel_order` 不同，此方法不要求订单由当前网关实例提交。
        它只执行一次签名 DELETE。结果不确定的传输/协议响应会以 ``UNKNOWN``
        返回并被记住，因此在 :meth:`get_order` 对账前，重复调用不能盲目发送
        第二次取消。
        """

        async with self._lock:
            return await self._cancel_order_by_client_id_locked(symbol, client_order_id)

    async def _cancel_order_by_client_id_locked(
        self,
        symbol: str,
        client_order_id: str,
    ) -> BinanceOrderSnapshot:
        normalized = self._normalize_symbol(symbol)
        if not _CLIENT_ORDER_ID.fullmatch(client_order_id):
            raise ValueError("client_order_id must be 1-36 Binance-safe characters")
        tracked = self._orders.get(client_order_id)
        if tracked is not None:
            tracked_symbol = self._normalize_symbol(tracked.order.symbol)
            if tracked_symbol != normalized:
                raise ValueError(
                    f"client_order_id {client_order_id!r} belongs to {tracked_symbol}, "
                    f"not {normalized}"
                )
            if tracked.update.status in {
                OrderStatus.CANCELED,
                OrderStatus.FILLED,
                OrderStatus.EXPIRED,
                OrderStatus.BROKER_REJECTED,
                OrderStatus.LOCAL_REJECTED,
            }:
                return self._snapshot_from_tracked(tracked)
            if tracked.cancellation_attempted:
                # UNKNOWN 取消必须先查询，才能再次尝试。
                return self._snapshot_from_tracked(tracked)
            if tracked.update.status is OrderStatus.UNKNOWN:
                raise ValueError(
                    f"order {client_order_id!r} is UNKNOWN; query it before cancellation"
                )

        cancellation_key = (normalized, client_order_id)
        previous = self._standalone_cancel_results.get(cancellation_key)
        if previous is not None:
            return previous
        if not self._connected:
            raise ConnectionError("Binance gateway is not connected")
        self._require_credentials()

        if tracked is not None:
            tracked.cancellation_attempted = True
        try:
            payload = await self._request_json(
                "DELETE",
                "/api/v3/order",
                params=(
                    ("symbol", normalized),
                    ("origClientOrderId", client_order_id),
                ),
                signed=True,
                execution_sensitive=True,
            )
            snapshot = self._snapshot_from_payload(payload, fallback_symbol=normalized)
        except (BinanceUncertainResultError, BinanceTransportError, BinanceProtocolError):
            snapshot = BinanceOrderSnapshot(
                symbol=normalized,
                client_order_id=client_order_id,
                order_id=(tracked.update.exchange_order_id if tracked is not None else None),
                status=OrderStatus.UNKNOWN,
                exchange_status=None,
                side=(tracked.order.side if tracked is not None else None),
                price=(tracked.order.limit_price if tracked is not None else None),
                original_quantity=(tracked.order.quantity if tracked is not None else None),
                executed_quantity=(
                    tracked.update.executed_quantity if tracked is not None else Decimal("0")
                ),
            )
            self._standalone_cancel_results[cancellation_key] = snapshot
            if tracked is not None:
                self._record_update(
                    tracked,
                    BinanceOrderUpdate(
                        order=tracked.order,
                        status=OrderStatus.UNKNOWN,
                        exchange_order_id=tracked.update.exchange_order_id,
                        executed_quantity=tracked.update.executed_quantity,
                        reason="cancellation outcome unknown; reconcile with query_order",
                    ),
                )
            return snapshot
        except BinanceAPIError:
            # 已知拒绝并未执行。这里没有隐藏重试；向上暴露，让调用方决定后续。
            if tracked is not None:
                tracked.cancellation_attempted = False
            raise

        if snapshot.symbol != normalized or (
            snapshot.client_order_id is not None and snapshot.client_order_id != client_order_id
        ):
            # DELETE 已经到达 Binance，因此身份不匹配或格式错误仍属于执行结果
            # 不确定。应像传输失败一样隔离，不能允许盲目发送第二次请求。
            snapshot = BinanceOrderSnapshot(
                symbol=normalized,
                client_order_id=client_order_id,
                order_id=None,
                status=OrderStatus.UNKNOWN,
                exchange_status=None,
                side=(tracked.order.side if tracked is not None else None),
                price=(tracked.order.limit_price if tracked is not None else None),
                original_quantity=(tracked.order.quantity if tracked is not None else None),
                executed_quantity=(
                    tracked.update.executed_quantity if tracked is not None else Decimal("0")
                ),
            )
            self._standalone_cancel_results[cancellation_key] = snapshot
            if tracked is not None:
                self._record_update(
                    tracked,
                    BinanceOrderUpdate(
                        order=tracked.order,
                        status=OrderStatus.UNKNOWN,
                        exchange_order_id=tracked.update.exchange_order_id,
                        executed_quantity=tracked.update.executed_quantity,
                        reason="cancellation response identity unknown; reconcile with query_order",
                    ),
                )
            return snapshot
        self._standalone_cancel_results[cancellation_key] = snapshot
        if tracked is not None:
            self._record_snapshot(tracked, snapshot)
        return snapshot

    async def query_order(
        self,
        client_order_id: str,
        *,
        symbol: str | None = None,
    ) -> BinanceOrderSnapshot:
        """按客户端编号对账订单，包括 UNKNOWN 提交。"""

        tracked = self._orders.get(client_order_id)
        if symbol is None:
            if tracked is None:
                raise KeyError("symbol is required when client_order_id is not locally tracked")
            symbol = tracked.order.symbol
        return await self.get_order(symbol, client_order_id=client_order_id)

    async def get_order(
        self,
        symbol: str,
        *,
        client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> BinanceOrderSnapshot:
        """仅使用交易所订单编号或客户端订单编号之一直接查询 Binance。"""

        if (client_order_id is None) == (order_id is None):
            raise ValueError("provide exactly one of client_order_id or order_id")
        self._require_credentials()
        normalized = self._normalize_symbol(symbol)
        params: list[tuple[str, object]] = [("symbol", normalized)]
        if client_order_id is not None:
            params.append(("origClientOrderId", client_order_id))
        else:
            params.append(("orderId", order_id))
        try:
            payload = await self._request_json(
                "GET",
                "/api/v3/order",
                params=tuple(params),
                signed=True,
                execution_sensitive=True,
            )
            snapshot = self._snapshot_from_payload(payload, fallback_symbol=normalized)
        except (BinanceUncertainResultError, BinanceTransportError, BinanceProtocolError):
            snapshot = BinanceOrderSnapshot(
                symbol=normalized,
                client_order_id=client_order_id,
                order_id=order_id,
                status=OrderStatus.UNKNOWN,
                exchange_status=None,
                side=None,
                price=None,
                original_quantity=None,
                executed_quantity=Decimal("0"),
            )
            tracked = self._find_tracked(client_order_id=client_order_id, order_id=order_id)
            if tracked is not None:
                self._record_update(
                    tracked,
                    BinanceOrderUpdate(
                        order=tracked.order,
                        status=OrderStatus.UNKNOWN,
                        exchange_order_id=tracked.update.exchange_order_id,
                        executed_quantity=tracked.update.executed_quantity,
                        reason="order query outcome unknown; reconciliation is still required",
                    ),
                )
            return snapshot

        tracked = self._find_tracked(
            client_order_id=snapshot.client_order_id or client_order_id,
            order_id=snapshot.order_id,
        )
        if client_order_id is not None and snapshot.status in {
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
        }:
            # 查询成功证明此前独立 UNKNOWN 取消后订单仍为未结，因此允许再明确
            # 发起一次取消。
            self._standalone_cancel_results.pop((normalized, client_order_id), None)
        if tracked is not None:
            self._record_snapshot(tracked, snapshot)
            if snapshot.status in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED}:
                # 查询成功证明此前 UNKNOWN 取消没有使订单进入终态，因此可以安全地
                # 再明确发起一次取消。
                tracked.cancellation_attempted = False
        return snapshot

    async def open_orders(
        self,
        symbol: str | None = None,
    ) -> tuple[BinanceOrderSnapshot, ...]:
        """返回当前所有未结现货订单，可选限定单个交易代码。"""

        self._require_credentials()
        params: tuple[tuple[str, object], ...] = ()
        fallback_symbol = ""
        if symbol is not None:
            fallback_symbol = self._normalize_symbol(symbol)
            params = (("symbol", fallback_symbol),)
        payload = await self._request_json(
            "GET",
            "/api/v3/openOrders",
            params=params,
            signed=True,
            execution_sensitive=False,
        )
        return self._parse_order_list(payload, fallback_symbol=fallback_symbol)

    async def all_orders(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> tuple[BinanceOrderSnapshot, ...]:
        """返回供启动和定期对账使用的现货订单历史。"""

        if not 1 <= limit <= 1_000:
            raise ValueError("order-history limit must be between 1 and 1000")
        self._require_credentials()
        normalized = self._normalize_symbol(symbol)
        params: list[tuple[str, object]] = [("symbol", normalized), ("limit", limit)]
        if order_id is not None:
            if order_id < 0:
                raise ValueError("order_id must be non-negative")
            params.append(("orderId", order_id))
        if start_time_ms is not None:
            if start_time_ms < 0:
                raise ValueError("start_time_ms must be non-negative")
            params.append(("startTime", start_time_ms))
        if end_time_ms is not None:
            if end_time_ms < 0:
                raise ValueError("end_time_ms must be non-negative")
            params.append(("endTime", end_time_ms))
        if start_time_ms is not None and end_time_ms is not None and end_time_ms < start_time_ms:
            raise ValueError("end_time_ms must not precede start_time_ms")
        payload = await self._request_json(
            "GET",
            "/api/v3/allOrders",
            params=tuple(params),
            signed=True,
            execution_sensitive=False,
        )
        return self._parse_order_list(payload, fallback_symbol=normalized)

    async def account_trades(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        from_id: int | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> tuple[BinanceTrade, ...]:
        """返回用于恢复遗漏用户事件的不可变账户成交。"""

        if not 1 <= limit <= 1_000:
            raise ValueError("trade-history limit must be between 1 and 1000")
        normalized = self._normalize_symbol(symbol)
        self._require_credentials()
        params: list[tuple[str, object]] = [("symbol", normalized), ("limit", limit)]
        for name, value in (
            ("orderId", order_id),
            ("fromId", from_id),
            ("startTime", start_time_ms),
            ("endTime", end_time_ms),
        ):
            if value is not None:
                if value < 0:
                    raise ValueError(f"{name} must be non-negative")
                params.append((name, value))
        if start_time_ms is not None and end_time_ms is not None and end_time_ms < start_time_ms:
            raise ValueError("end_time_ms must not precede start_time_ms")
        payload = await self._request_json(
            "GET",
            "/api/v3/myTrades",
            params=tuple(params),
            signed=True,
            execution_sensitive=False,
        )
        if not isinstance(payload, list):
            raise BinanceProtocolError("Binance account trades response must be a list")
        try:
            return tuple(self._parse_trade(item, fallback_symbol=normalized) for item in payload)
        except (KeyError, TypeError, ValueError, BinanceValidationError):
            raise BinanceProtocolError("Binance account trades response is malformed") from None

    async def commission_rate(self, symbol: str) -> BinanceCommissionRate:
        """查询账户针对某一交易代码的现货佣金率。"""

        normalized = self._normalize_symbol(symbol)
        self._require_credentials()
        payload = await self._request_json(
            "GET",
            "/api/v3/account/commission",
            params=(("symbol", normalized),),
            signed=True,
            execution_sensitive=False,
        )
        mapping = self._require_mapping(payload, "commission rate")
        try:
            discount_value = mapping["discount"]
            if not isinstance(discount_value, Mapping):
                raise TypeError("discount")
            return BinanceCommissionRate(
                symbol=str(mapping.get("symbol", normalized)).upper(),
                standard=self._parse_commission_component(mapping["standardCommission"]),
                tax=self._parse_commission_component(mapping["taxCommission"]),
                special=self._parse_commission_component(mapping["specialCommission"]),
                discount=BinanceCommissionDiscount(
                    enabled_for_account=bool(discount_value.get("enabledForAccount", False)),
                    enabled_for_symbol=bool(discount_value.get("enabledForSymbol", False)),
                    asset=str(discount_value.get("discountAsset", "")),
                    discount=decimal_from_api(discount_value.get("discount", "0"), "discount"),
                ),
            )
        except (KeyError, TypeError, BinanceValidationError):
            raise BinanceProtocolError("Binance commission response is malformed") from None

    get_open_orders = open_orders
    get_all_orders = all_orders
    get_account_trades = account_trades
    get_commission_rate = commission_rate

    async def account(self) -> BinanceAccount:
        """返回经签名查询的现货账户余额和权限。"""

        self._require_credentials()
        payload = await self._request_json(
            "GET", "/api/v3/account", signed=True, execution_sensitive=False
        )
        mapping = self._require_mapping(payload, "account")
        balances_value = mapping.get("balances")
        if not isinstance(balances_value, list):
            raise BinanceProtocolError("Binance account balances are malformed")
        try:
            balances = tuple(
                BinanceBalance(
                    asset=str(item["asset"]),
                    free=decimal_from_api(item["free"], "free"),
                    locked=decimal_from_api(item["locked"], "locked"),
                )
                for item in balances_value
                if isinstance(item, Mapping)
            )
            update_time_value = mapping.get("updateTime")
            update_time = int(update_time_value) if update_time_value is not None else None
            permissions_value = mapping.get("permissions", [])
            if not isinstance(permissions_value, list):
                raise TypeError("permissions")
            return BinanceAccount(
                can_trade=bool(mapping.get("canTrade", False)),
                can_withdraw=bool(mapping.get("canWithdraw", False)),
                can_deposit=bool(mapping.get("canDeposit", False)),
                account_type=str(mapping.get("accountType", "")),
                balances=balances,
                update_time_ms=update_time,
                permissions=tuple(str(value).upper() for value in permissions_value),
                uid=self._optional_integer(mapping.get("uid"), "uid"),
                maker_commission=self._optional_integer(
                    mapping.get("makerCommission"), "makerCommission"
                ),
                taker_commission=self._optional_integer(
                    mapping.get("takerCommission"), "takerCommission"
                ),
                buyer_commission=self._optional_integer(
                    mapping.get("buyerCommission"), "buyerCommission"
                ),
                seller_commission=self._optional_integer(
                    mapping.get("sellerCommission"), "sellerCommission"
                ),
                brokered=bool(mapping.get("brokered", False)),
                require_self_trade_prevention=bool(
                    mapping.get("requireSelfTradePrevention", False)
                ),
                prevent_sor=bool(mapping.get("preventSor", False)),
            )
        except (KeyError, TypeError, ValueError, BinanceValidationError):
            raise BinanceProtocolError("Binance account response is malformed") from None

    get_account = account

    @staticmethod
    def _positive_decimal(value: Decimal, label: str) -> Decimal:
        """兼容旧网关导出的正数校验辅助方法。"""

        return _positive_decimal(value, label)

    @staticmethod
    def _order_type(value: str) -> str:
        """兼容旧网关导出的订单类型校验辅助方法。"""

        return _order_type(value)

    @staticmethod
    def _validate_list_response_type(value: str) -> None:
        """兼容旧网关导出的订单列表响应类型校验方法。"""

        _validate_list_response_type(value)

    @staticmethod
    def _append_optional(params: list[tuple[str, object]], key: str, value: object | None) -> None:
        _append_optional(params, key, value)

    @staticmethod
    def _require_client_order_id(value: str | None) -> None:
        _require_client_order_id(value)

    @staticmethod
    def _optional_text(value: object) -> str | None:
        return _optional_text(value)

    @staticmethod
    def _validate_leg(leg: BinanceSpotOrderLeg, *, require_quantity: bool) -> None:
        _validate_leg(leg, require_quantity=require_quantity)

    @staticmethod
    def _spot_order_params(
        *,
        symbol: str,
        side: Side,
        order_type: str,
        quantity: Decimal | None,
        quote_order_quantity: Decimal | None,
        price: Decimal | None,
        stop_price: Decimal | None,
        trailing_delta: int | None,
        time_in_force: str | None,
        client_order_id: str | None,
        iceberg_quantity: Decimal | None,
        strategy_id: int | None,
        strategy_type: int | None,
        self_trade_prevention_mode: str | None,
        response_type: str,
    ) -> list[tuple[str, object]]:
        return _spot_order_params(
            symbol=symbol,
            side=side,
            order_type_value=order_type,
            quantity=quantity,
            quote_order_quantity=quote_order_quantity,
            price=price,
            stop_price=stop_price,
            trailing_delta=trailing_delta,
            time_in_force=time_in_force,
            client_order_id=client_order_id,
            iceberg_quantity=iceberg_quantity,
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            self_trade_prevention_mode=self_trade_prevention_mode,
            response_type=response_type,
        )

    @staticmethod
    def _list_leg_params(
        prefix: str, leg: BinanceSpotOrderLeg, *, quantity: Decimal
    ) -> list[tuple[str, object]]:
        return _list_leg_params(prefix, leg, quantity=quantity)

    @staticmethod
    def _prefixed_leg_params(
        prefix: str,
        leg: BinanceSpotOrderLeg,
        *,
        required: bool,
        include_type: bool = True,
        include_side: bool = True,
        include_quantity: bool = True,
    ) -> list[tuple[str, object]]:
        return _prefixed_leg_params(
            prefix,
            leg,
            required=required,
            include_type=include_type,
            include_side=include_side,
            include_quantity=include_quantity,
        )

    def _order_list_from_payload(
        self, payload: object, *, fallback_symbol: str
    ) -> BinanceOrderListSnapshot:
        mapping = self._require_mapping(payload, "order list")
        reports = mapping.get("orderReports", mapping.get("orders", []))
        if not isinstance(reports, list):
            raise BinanceProtocolError("Binance order-list reports are malformed")
        orders = tuple(
            self._snapshot_from_payload(item, fallback_symbol=fallback_symbol)
            for item in reports
            if isinstance(item, Mapping)
        )
        try:
            list_id = None if mapping.get("orderListId") is None else int(mapping["orderListId"])
            transaction = (
                None if mapping.get("transactionTime") is None else int(mapping["transactionTime"])
            )
        except (TypeError, ValueError):
            raise BinanceProtocolError("Binance order-list response is malformed") from None
        return BinanceOrderListSnapshot(
            order_list_id=list_id,
            contingency_type=None
            if mapping.get("contingencyType") is None
            else str(mapping["contingencyType"]),
            list_status_type=None
            if mapping.get("listStatusType") is None
            else str(mapping["listStatusType"]),
            list_order_status=None
            if mapping.get("listOrderStatus") is None
            else str(mapping["listOrderStatus"]),
            list_client_order_id=None
            if mapping.get("listClientOrderId") is None
            else str(mapping["listClientOrderId"]),
            symbol=str(mapping.get("symbol", fallback_symbol)).upper(),
            orders=orders,
            transaction_time_ms=transaction,
        )

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: tuple[tuple[str, object], ...] = (),
        signed: bool = False,
        execution_sensitive: bool = False,
        retry_timestamp_rejection: bool = True,
    ) -> Any:
        credentials = self._require_credentials() if signed else None
        encoded = encode_request(
            method=method,
            base_url=self.base_url,
            path=path,
            params=params,
            credentials=credentials,
            recv_window_ms=self._recv_window_ms if signed else None,
            timestamp_ms=(self._clock_ms() + self._server_time_offset_ms) if signed else None,
            parameter_encoder=self._parameter_text,
            signer=sign_hmac_sha256,
        )
        request = HttpRequest(
            method=method,
            url=encoded.url,
            headers=encoded.headers,
            body=encoded.body,
            timeout_seconds=self._timeout_seconds,
        )
        signature = encoded.signature
        try:
            response = await self._transport.request(request)
        except Exception:
            # 注入的传输层可能在自身异常中包含完整请求；将其替换为稳定且不含
            # 凭据的错误。
            raise BinanceTransportError("Binance HTTP request failed") from None
        if not isinstance(response, HttpResponse):
            raise BinanceProtocolError("HTTP transport returned an invalid response")
        self._update_rate_limit_usage(response.headers)
        try:
            response_payload = response.json()
        except (UnicodeDecodeError, ValueError):
            raise BinanceProtocolError(
                f"Binance returned non-JSON data (HTTP {response.status_code})"
            ) from None

        if 200 <= response.status_code < 300:
            if isinstance(response_payload, Mapping) and self._api_code(response_payload) == -1007:
                raise self._api_error(
                    response.status_code,
                    response_payload,
                    uncertain=True,
                    signature=signature,
                )
            return response_payload

        api_code = self._api_code(response_payload)
        if signed and api_code == -1021 and retry_timestamp_rejection:
            await self.synchronize_time()
            return await self._request_json(
                method,
                path,
                params=params,
                signed=True,
                execution_sensitive=execution_sensitive,
                retry_timestamp_rejection=False,
            )

        uncertain = api_code == -1007 or (execution_sensitive and response.status_code >= 500)
        raise self._api_error(
            response.status_code,
            response_payload,
            uncertain=uncertain,
            signature=signature,
        )

    def _api_error(
        self,
        status_code: int,
        payload: object,
        *,
        uncertain: bool,
        signature: str,
    ) -> BinanceAPIError:
        code: int | None = None
        message: object = "request rejected"
        if isinstance(payload, Mapping):
            code = self._api_code(payload)
            message = payload.get("msg", message)
        secrets = [signature]
        if self._credentials is not None:
            secrets.extend((self._credentials.api_key, self._credentials.secret_key))
        error_type = BinanceUncertainResultError if uncertain else BinanceAPIError
        return error_type(
            status_code=status_code,
            code=code,
            message=sanitize_message(message, secrets),
        )

    @staticmethod
    def _api_code(payload: Mapping[object, object]) -> int | None:
        return _api_code_value(payload)

    def _require_credentials(self) -> BinanceCredentials:
        if self._credentials is None:
            raise BinanceConfigurationError(
                "Binance credentials are required for this signed endpoint"
            )
        return self._credentials

    def _require_order_connection(self) -> None:
        if not self._connected:
            raise ConnectionError("Binance gateway is not connected")

    @staticmethod
    def _parameter_text(value: object) -> str:
        return _parameter_text_value(value)

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return _normalize_symbol_value(symbol)

    @staticmethod
    def _require_mapping(payload: object, label: str) -> Mapping[str, Any]:
        if not isinstance(payload, Mapping):
            raise BinanceProtocolError(f"Binance {label} response must be an object")
        return payload

    @staticmethod
    def _parse_levels(value: object, label: str) -> tuple[OrderBookLevel, ...]:
        return parse_levels(value, label)

    @staticmethod
    def _parse_kline(item: object) -> Kline:
        return parse_kline(item)

    def _parse_order_list(
        self,
        payload: object,
        *,
        fallback_symbol: str,
    ) -> tuple[BinanceOrderSnapshot, ...]:
        if not isinstance(payload, list):
            raise BinanceProtocolError("Binance order-list response must be a list")
        try:
            return tuple(
                self._snapshot_from_payload(item, fallback_symbol=fallback_symbol)
                for item in payload
            )
        except BinanceProtocolError:
            raise
        except (TypeError, ValueError, BinanceValidationError):
            raise BinanceProtocolError("Binance order-list response is malformed") from None

    @staticmethod
    def _parse_trade(item: object, *, fallback_symbol: str) -> BinanceTrade:
        return parse_trade(item, fallback_symbol=fallback_symbol)

    @staticmethod
    def _parse_commission_component(value: object) -> BinanceCommissionComponent:
        return parse_commission_component(value)

    @staticmethod
    def _optional_integer(value: object, label: str) -> int | None:
        return _optional_integer_value(value, label)

    def _update_rate_limit_usage(self, headers: Mapping[str, str]) -> None:
        self._rate_limit_usage = parse_rate_limit_usage(headers, prior=self._rate_limit_usage)

    def _snapshot_from_payload(
        self,
        payload: object,
        *,
        fallback_symbol: str,
    ) -> BinanceOrderSnapshot:
        try:
            return parse_order_snapshot(payload, fallback_symbol=fallback_symbol)
        except (TypeError, ValueError, BinanceValidationError):
            raise BinanceProtocolError("Binance order response is malformed") from None

    @staticmethod
    def _map_order_status(exchange_status: str | None) -> OrderStatus:
        return map_order_status(exchange_status)

    def _record_snapshot(
        self,
        tracked: _TrackedOrder,
        snapshot: BinanceOrderSnapshot,
    ) -> None:
        self._record_update(
            tracked,
            BinanceOrderUpdate(
                order=tracked.order,
                status=snapshot.status,
                exchange_order_id=snapshot.order_id,
                executed_quantity=snapshot.executed_quantity,
            ),
        )

    @staticmethod
    def _snapshot_from_tracked(tracked: _TrackedOrder) -> BinanceOrderSnapshot:
        return BinanceOrderSnapshot(
            symbol=tracked.order.symbol,
            client_order_id=tracked.order.client_order_id,
            order_id=tracked.update.exchange_order_id,
            status=tracked.update.status,
            exchange_status=None,
            side=tracked.order.side,
            price=tracked.order.limit_price,
            original_quantity=tracked.order.quantity,
            executed_quantity=tracked.update.executed_quantity,
        )

    def _record_update(self, tracked: _TrackedOrder, update: BinanceOrderUpdate) -> None:
        if tracked.update == update:
            return
        tracked.update = update
        self._publish(update)

    def _find_tracked(
        self,
        *,
        client_order_id: str | None,
        order_id: int | None,
    ) -> _TrackedOrder | None:
        if client_order_id is not None:
            tracked = self._orders.get(client_order_id)
            if tracked is not None:
                return tracked
        if order_id is not None:
            for value in self._orders.values():
                if value.update.exchange_order_id == order_id:
                    return value
        return None

    def _publish(self, update: BinanceOrderUpdate) -> None:
        occurred_at = datetime.now(UTC)
        payload = BinanceOrderUpdate(
            order=update.order,
            status=update.status,
            exchange_order_id=update.exchange_order_id,
            executed_quantity=update.executed_quantity,
            reason=update.reason,
            error_code=update.error_code,
            occurred_at=occurred_at,
        )
        tracked = self._orders.get(update.order.client_order_id)
        if tracked is not None:
            tracked.update = payload
        self._events.put_nowait(
            BrokerEvent(
                event_id=str(uuid4()),
                event_type=ORDER_STATUS_EVENT,
                occurred_at=occurred_at,
                payload=payload,
            )
        )
