"""Binance USD-M 与 COIN-M 合约的最小异步 REST 边界。

默认连接 Binance 模拟交易环境。公开请求以及签名后的账户、测试订单请求均保持
精简且可注入，以便完全离线测试。客户端不会重试变更操作，也不会把不受支持的
环境切换到实盘地址。
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Literal
from urllib.parse import urlencode

from .envs import (
    BinanceCapability,
    BinanceEnvironmentProfile,
    BinanceProduct,
    BinanceStage,
    UnsupportedBinanceEnvironment,
    binance_environment,
)
from .gateway import (
    BinanceAPIError,
    BinanceConfigurationError,
    BinanceProtocolError,
    BinanceTransportError,
    sign_hmac_sha256,
)
from .http import (
    AsyncHttpTransport,
    HttpRequest,
    HttpTransportError,
    UrllibAsyncHttpTransport,
)
from .models import BinanceCredentials

_SYMBOL = re.compile(r"^[A-Z0-9_]{1,30}$")
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)\b(api[-_ ]?key|secret(?:[-_ ]?key)?|signature)\b\s*[:=]\s*[^\s,;&]+"
)


@dataclass(frozen=True, slots=True)
class BinanceFuturesTicker:
    symbol: str
    price: Decimal
    time_ms: int | None = None


@dataclass(frozen=True, slots=True)
class BinanceFuturesProtectionOrder:
    """策略层使用的结构化保护单请求。"""

    kind: Literal["stop_loss", "take_profit", "trailing_stop"]
    symbol: str
    side: str
    position_side: str | None = None
    quantity: Decimal | str | None = None
    stop_price: Decimal | str | None = None
    callback_rate: Decimal | str | None = None
    activation_price: Decimal | str | None = None
    close_position: bool = True
    working_type: str = "MARK_PRICE"
    price_protect: bool | None = None
    client_order_id: str | None = None

    def validate(self) -> None:
        if self.kind in {"stop_loss", "take_profit"} and self.stop_price is None:
            raise ValueError("stop_price is required for fixed protection orders")
        if self.kind == "trailing_stop" and self.callback_rate is None:
            raise ValueError("callback_rate is required for trailing_stop")
        if self.kind == "trailing_stop" and self.close_position:
            raise ValueError("trailing_stop requires quantity and cannot use close_position")


class BinanceFuturesRestClient:
    """面向单一 USD-M 或 COIN-M 环境的 REST 客户端。

    按当前官方文档，``DEMO`` 是唯一受支持的非生产合约阶段。``LIVE`` 还要求
    显式启用 ``allow_live``；传入 ``TESTNET`` 会直接报错，而不会替调用方选择
    模拟或实盘环境。
    """

    def __init__(
        self,
        *,
        product: BinanceProduct | str = BinanceProduct.USDS_FUTURES,
        stage: BinanceStage | str = BinanceStage.DEMO,
        credentials: BinanceCredentials | None = None,
        transport: AsyncHttpTransport | None = None,
        allow_live: bool = False,
        recv_window_ms: int = 5_000,
        timeout_seconds: float = 10.0,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        try:
            profile = binance_environment(product, stage)
        except UnsupportedBinanceEnvironment as exc:
            raise BinanceConfigurationError(str(exc)) from None
        if not profile.supports(BinanceCapability.FUTURES) or profile.product not in {
            BinanceProduct.USDS_FUTURES,
            BinanceProduct.COIN_FUTURES,
        }:
            raise BinanceConfigurationError(
                "BinanceFuturesRestClient supports only USD-M or COIN-M Futures"
            )
        if profile.is_live and not allow_live:
            raise BinanceConfigurationError(
                "Binance Futures LIVE is disabled; pass allow_live=True explicitly"
            )
        if not 1 <= recv_window_ms <= 60_000:
            raise ValueError("recv_window_ms must be between 1 and 60000")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._profile = profile
        self._credentials = credentials
        self._transport = transport if transport is not None else UrllibAsyncHttpTransport()
        self._recv_window_ms = recv_window_ms
        self._timeout_seconds = timeout_seconds
        self._clock_ms = clock_ms if clock_ms is not None else lambda: time.time_ns() // 1_000_000
        self._server_time_offset_ms = 0
        self._last_time_sync_rtt_ms: int | None = None

    def __repr__(self) -> str:
        return (
            f"BinanceFuturesRestClient(product={self.product.value!r}, "
            f"stage={self.stage.value!r}, base_url={self.base_url!r}, "
            f"credentials_configured={self._credentials is not None!r})"
        )

    @property
    def profile(self) -> BinanceEnvironmentProfile:
        return self._profile

    @property
    def product(self) -> BinanceProduct:
        return self._profile.product

    @property
    def stage(self) -> BinanceStage:
        return self._profile.stage

    @property
    def base_url(self) -> str:
        return self._profile.rest_base_url

    @property
    def server_time_offset_ms(self) -> int:
        return self._server_time_offset_ms

    @property
    def last_time_sync_rtt_ms(self) -> int | None:
        """最近一次服务器时钟采样的往返延迟，单位为毫秒。"""

        return self._last_time_sync_rtt_ms

    @property
    def _api_prefix(self) -> str:
        return self._profile.api_prefix

    async def ping(self) -> None:
        payload = await self._request_json("GET", self._v1("ping"))
        if not isinstance(payload, Mapping):
            raise BinanceProtocolError("Binance Futures ping response must be an object")

    async def start_user_data_stream(self) -> str:
        """创建或续期合约用户数据流密钥（USER_STREAM）。

        此接口只使用 API key 请求头，不使用签名；密钥不会进入异常或对象表示。
        """

        payload = await self._request_json("POST", self._v1("listenKey"), api_key_only=True)
        mapping = self._require_mapping(payload, "user data stream")
        value = mapping.get("listenKey")
        if not isinstance(value, str) or not value:
            raise BinanceProtocolError("Binance Futures listenKey response is malformed")
        return value

    async def keepalive_user_data_stream(self, listen_key: str) -> str | None:
        """把合约用户数据流密钥再延长 60 分钟。"""

        self._validate_listen_key(listen_key)
        payload = await self._request_json(
            "PUT", self._v1("listenKey"), params=(("listenKey", listen_key),), api_key_only=True
        )
        if isinstance(payload, Mapping):
            value = payload.get("listenKey")
            if value is None:
                return None
            if not isinstance(value, str) or not value:
                raise BinanceProtocolError("Binance Futures listenKey response is malformed")
            return value
        if payload in ({}, None):
            return None
        raise BinanceProtocolError("Binance Futures listenKey response is malformed")

    async def close_user_data_stream(self, listen_key: str) -> None:
        """关闭合约用户数据流密钥。"""

        self._validate_listen_key(listen_key)
        await self._request_json(
            "DELETE", self._v1("listenKey"), params=(("listenKey", listen_key),), api_key_only=True
        )

    @property
    def user_data_stream_base_url(self) -> str:
        """返回当前产品和环境的私有 WebSocket 主机。"""

        base = self._profile.market_ws_base_url
        if not base:
            raise BinanceConfigurationError("Binance Futures user stream is unavailable")
        return base.rstrip("/") + "/private"

    async def server_time(self) -> int:
        payload = await self._request_json("GET", self._v1("time"))
        mapping = self._require_mapping(payload, "server time")
        try:
            return int(mapping["serverTime"])
        except (KeyError, TypeError, ValueError):
            raise BinanceProtocolError(
                "Binance Futures server time response is malformed"
            ) from None

    async def synchronize_time(self) -> int:
        started_ms = self._clock_ms()
        exchange_ms = await self.server_time()
        finished_ms = self._clock_ms()
        self._last_time_sync_rtt_ms = max(0, finished_ms - started_ms)
        midpoint_ms = started_ms + (finished_ms - started_ms) // 2
        self._server_time_offset_ms = exchange_ms - midpoint_ms
        return self._server_time_offset_ms

    async def exchange_info(self) -> dict[str, Any]:
        payload = await self._request_json("GET", self._v1("exchangeInfo"))
        return dict(self._require_mapping(payload, "exchangeInfo"))

    async def ticker_price(self, symbol: str) -> BinanceFuturesTicker:
        normalized = self._normalize_symbol(symbol)
        payload = await self._request_json(
            "GET",
            self._v1("ticker/price"),
            params=(("symbol", normalized),),
        )
        # 当前 COIN-M 即使提供 ``symbol`` 仍返回单元素数组，而 USD-M 返回对象；
        # 在适配器边界保留这一有文档依据的产品差异。
        if isinstance(payload, list):
            if len(payload) != 1 or not isinstance(payload[0], Mapping):
                raise BinanceProtocolError(
                    "Binance Futures ticker response must contain exactly one symbol"
                )
            mapping = payload[0]
        else:
            mapping = self._require_mapping(payload, "ticker price")
        try:
            response_symbol = self._normalize_symbol(str(mapping["symbol"]))
            price = Decimal(str(mapping["price"]))
            time_value = mapping.get("time")
            time_ms = None if time_value is None else int(time_value)
        except (KeyError, InvalidOperation, TypeError, ValueError):
            raise BinanceProtocolError("Binance Futures ticker response is malformed") from None
        return BinanceFuturesTicker(symbol=response_symbol, price=price, time_ms=time_ms)

    async def klines(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> tuple[tuple[Any, ...], ...]:
        if not interval.strip():
            raise ValueError("interval must not be blank")
        if not 1 <= limit <= 1_500:
            raise ValueError("limit must be between 1 and 1500")
        params: list[tuple[str, object]] = [
            ("symbol", self._normalize_symbol(symbol)),
            ("interval", interval.strip()),
            ("limit", limit),
        ]
        if start_time_ms is not None:
            params.append(("startTime", start_time_ms))
        if end_time_ms is not None:
            params.append(("endTime", end_time_ms))
        payload = await self._request_json("GET", self._v1("klines"), params=params)
        if not isinstance(payload, list) or any(not isinstance(row, list) for row in payload):
            raise BinanceProtocolError("Binance Futures klines response must be a list")
        return tuple(tuple(row) for row in payload)

    async def account(self) -> dict[str, Any]:
        path = (
            "/fapi/v3/account"
            if self.product is BinanceProduct.USDS_FUTURES
            else "/dapi/v1/account"
        )
        payload = await self._request_json("GET", path, signed=True)
        return dict(self._require_mapping(payload, "account"))

    async def position_risk(self, symbol: str | None = None) -> tuple[dict[str, Any], ...]:
        path = (
            "/fapi/v3/positionRisk"
            if self.product is BinanceProduct.USDS_FUTURES
            else "/dapi/v1/positionRisk"
        )
        params: tuple[tuple[str, object], ...] = ()
        if symbol is not None:
            params = (("symbol", self._normalize_symbol(symbol)),)
        payload = await self._request_json("GET", path, params=params, signed=True)
        if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
            raise BinanceProtocolError("Binance Futures position response must be a list")
        return tuple(dict(item) for item in payload)

    async def position_side_mode(self) -> bool:
        """返回账户是否启用双向持仓模式。

        ``True`` 表示 Hedge Mode，``False`` 表示 One-way Mode。该查询使用
        账户签名接口；响应缺失或类型不符合官方布尔字段时直接失败，避免在
        下单前猜测账户模式。
        """

        payload = await self._request_json("GET", self._v1("positionSide/dual"), signed=True)
        mapping = self._require_mapping(payload, "position side mode")
        value = mapping.get("dualSidePosition")
        if not isinstance(value, bool):
            raise BinanceProtocolError("Binance Futures position side mode response is malformed")
        return value

    async def position_mode(self) -> bool:
        """``position_side_mode`` 的简短兼容别名。"""

        return await self.position_side_mode()

    async def set_position_mode(self, dual_side_position: bool) -> dict[str, Any]:
        """设置账户持仓模式（单向或双向）。

        这是账户级别的风险设置，Binance 要求没有未平仓仓位和挂单时才可切换。
        调用方必须通过运行时守卫授权；客户端不会在失败后重试。
        """

        if not isinstance(dual_side_position, bool):
            raise TypeError("dual_side_position must be a bool")
        payload = await self._request_json(
            "POST",
            self._v1("positionSide/dual"),
            params=(("dualSidePosition", str(dual_side_position).lower()),),
            signed=True,
        )
        return dict(self._require_mapping(payload, "position side mode update"))

    async def set_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        """设置单个合约的杠杆倍数（交易所仍会按风险档位限制上限）。"""

        if isinstance(leverage, bool) or not isinstance(leverage, int):
            raise TypeError("leverage must be an integer")
        if not 1 <= leverage <= 125:
            raise ValueError("leverage must be between 1 and 125")
        payload = await self._request_json(
            "POST",
            self._v1("leverage"),
            params=(
                ("symbol", self._normalize_symbol(symbol)),
                ("leverage", leverage),
            ),
            signed=True,
        )
        return dict(self._require_mapping(payload, "leverage update"))

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        """设置单个合约的保证金模式（ISOLATED 或 CROSSED）。"""

        normalized = self._enum_value(margin_type, "margin_type")
        if normalized not in {"ISOLATED", "CROSSED"}:
            raise ValueError("margin_type must be ISOLATED or CROSSED")
        payload = await self._request_json(
            "POST",
            self._v1("marginType"),
            params=(
                ("symbol", self._normalize_symbol(symbol)),
                ("marginType", normalized),
            ),
            signed=True,
        )
        return dict(self._require_mapping(payload, "margin type update"))

    async def multi_assets_mode(self) -> bool:
        """查询 USDⓈ-M 多资产保证金模式。COIN-M 不支持此账户设置。"""

        if self.product is not BinanceProduct.USDS_FUTURES:
            raise BinanceConfigurationError("multi-assets mode is only supported by USD-M Futures")
        payload = await self._request_json("GET", self._v1("multiAssetsMargin"), signed=True)
        value = self._require_mapping(payload, "multi-assets mode").get("multiAssetsMargin")
        if not isinstance(value, bool):
            raise BinanceProtocolError("Binance Futures multi-assets mode response is malformed")
        return value

    async def set_multi_assets_mode(self, multi_assets_margin: bool) -> dict[str, Any]:
        """设置 USDⓈ-M 多资产保证金模式。"""

        if self.product is not BinanceProduct.USDS_FUTURES:
            raise BinanceConfigurationError("multi-assets mode is only supported by USD-M Futures")
        if not isinstance(multi_assets_margin, bool):
            raise TypeError("multi_assets_margin must be a bool")
        payload = await self._request_json(
            "POST",
            self._v1("multiAssetsMargin"),
            params=(("multiAssetsMargin", str(multi_assets_margin).lower()),),
            signed=True,
        )
        return dict(self._require_mapping(payload, "multi-assets mode update"))

    async def open_orders(self, symbol: str | None = None) -> tuple[dict[str, Any], ...]:
        """返回当前未完成的 USD-M/COIN-M 合约订单。"""

        path = self._order_path("openOrders")
        params: tuple[tuple[str, object], ...] = ()
        if symbol is not None:
            params = (("symbol", self._normalize_symbol(symbol)),)
        payload = await self._request_json("GET", path, params=params, signed=True)
        if not isinstance(payload, list) or any(not isinstance(item, Mapping) for item in payload):
            raise BinanceProtocolError("Binance Futures open orders response must be a list")
        return tuple(dict(item) for item in payload)

    async def get_order(
        self,
        symbol: str,
        *,
        order_id: int | str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """按交易所订单号或客户端订单号查询单笔订单。"""

        if (order_id is None) == (client_order_id is None):
            raise ValueError("provide exactly one of order_id or client_order_id")
        params: list[tuple[str, object]] = [("symbol", self._normalize_symbol(symbol))]
        if order_id is not None:
            params.append(("orderId", order_id))
        else:
            params.append(("origClientOrderId", client_order_id))
        payload = await self._request_json(
            "GET", self._order_path("order"), params=params, signed=True
        )
        return dict(self._require_mapping(payload, "order"))

    async def all_orders(
        self, symbol: str, *, limit: int = 500, order_id: int | str | None = None
    ) -> tuple[dict[str, Any], ...]:
        """返回指定合约的历史订单，用于启动对账。"""

        if not 1 <= limit <= 1_000:
            raise ValueError("limit must be between 1 and 1000")
        params: list[tuple[str, object]] = [
            ("symbol", self._normalize_symbol(symbol)),
            ("limit", limit),
        ]
        if order_id is not None:
            params.append(("orderId", order_id))
        payload = await self._request_json(
            "GET", self._order_path("allOrders"), params=params, signed=True
        )
        if not isinstance(payload, list) or any(not isinstance(item, Mapping) for item in payload):
            raise BinanceProtocolError("Binance Futures all orders response must be a list")
        return tuple(dict(item) for item in payload)

    async def account_trades(
        self, symbol: str, *, limit: int = 500, from_id: int | str | None = None
    ) -> tuple[dict[str, Any], ...]:
        """返回账户成交，用于填充和校验本地成交记录。"""

        if not 1 <= limit <= 1_000:
            raise ValueError("limit must be between 1 and 1000")
        params: list[tuple[str, object]] = [
            ("symbol", self._normalize_symbol(symbol)),
            ("limit", limit),
        ]
        if from_id is not None:
            params.append(("fromId", from_id))
        payload = await self._request_json(
            "GET", self._account_trades_path(), params=params, signed=True
        )
        if not isinstance(payload, list) or any(not isinstance(item, Mapping) for item in payload):
            raise BinanceProtocolError("Binance Futures account trades response must be a list")
        return tuple(dict(item) for item in payload)

    async def submit_order(self, **kwargs: object) -> dict[str, Any]:
        """提交真实订单；调用方必须先通过运行时交易守卫。"""

        values = dict(kwargs)
        if str(values.get("type", "")).upper() in {
            "STOP",
            "TAKE_PROFIT",
            "STOP_MARKET",
            "TAKE_PROFIT_MARKET",
            "TRAILING_STOP_MARKET",
        }:
            raise ValueError(
                "conditional Futures orders must use submit_algo_order after Binance Algo migration"
            )
        values["position_side"] = await self._checked_position_side(
            values.get("position_side"),
            values.get("reduce_only"),
        )
        params = self._order_params(values)
        payload = await self._request_json(
            "POST", self._order_path("order"), params=params, signed=True
        )
        return dict(self._require_mapping(payload, "order"))

    async def submit_stop_loss(
        self,
        *,
        symbol: str,
        side: str,
        stop_price: Decimal | str,
        position_side: str | None = None,
        quantity: Decimal | str | None = None,
        close_position: bool = True,
        working_type: str = "MARK_PRICE",
        price_protect: bool | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """提交止损保护单；默认 STOP_MARKET 全平对应方向仓位。"""

        if close_position and quantity is not None:
            raise ValueError("close_position cannot be combined with quantity")

        if self.product is not BinanceProduct.USDS_FUTURES:
            return await self.submit_order(
                symbol=symbol,
                side=side,
                type="STOP_MARKET",
                quantity=quantity,
                stop_price=stop_price,
                position_side=position_side,
                close_position=close_position,
                working_type=working_type,
                price_protect=price_protect,
                client_order_id=client_order_id,
            )
        return await self.submit_algo_order(
            algo_type="CONDITIONAL",
            symbol=symbol,
            side=side,
            type="STOP_MARKET",
            quantity=quantity,
            trigger_price=stop_price,
            position_side=position_side,
            close_position=close_position,
            working_type=working_type,
            price_protect=price_protect,
            client_algo_id=client_order_id,
        )

    async def submit_take_profit(
        self,
        *,
        symbol: str,
        side: str,
        stop_price: Decimal | str,
        position_side: str | None = None,
        quantity: Decimal | str | None = None,
        close_position: bool = True,
        working_type: str = "MARK_PRICE",
        price_protect: bool | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """提交止盈保护单；默认 TAKE_PROFIT_MARKET 全平对应方向仓位。"""

        if close_position and quantity is not None:
            raise ValueError("close_position cannot be combined with quantity")

        if self.product is not BinanceProduct.USDS_FUTURES:
            return await self.submit_order(
                symbol=symbol,
                side=side,
                type="TAKE_PROFIT_MARKET",
                quantity=quantity,
                stop_price=stop_price,
                position_side=position_side,
                close_position=close_position,
                working_type=working_type,
                price_protect=price_protect,
                client_order_id=client_order_id,
            )
        return await self.submit_algo_order(
            algo_type="CONDITIONAL",
            symbol=symbol,
            side=side,
            type="TAKE_PROFIT_MARKET",
            quantity=quantity,
            trigger_price=stop_price,
            position_side=position_side,
            close_position=close_position,
            working_type=working_type,
            price_protect=price_protect,
            client_algo_id=client_order_id,
        )

    async def submit_trailing_stop(
        self,
        *,
        symbol: str,
        side: str,
        callback_rate: Decimal | str,
        position_side: str | None = None,
        quantity: Decimal | str | None = None,
        activation_price: Decimal | str | None = None,
        close_position: bool = False,
        working_type: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """提交移动止损单（TRAILING_STOP_MARKET）。"""

        if close_position:
            raise ValueError("TRAILING_STOP_MARKET requires quantity and cannot use close_position")
        if quantity is None:
            raise ValueError("TRAILING_STOP_MARKET requires quantity")
        rate = Decimal(str(callback_rate))
        maximum = Decimal("10") if self.product is BinanceProduct.USDS_FUTURES else Decimal("5")
        if not Decimal("0.1") <= rate <= maximum:
            raise ValueError(f"callback_rate must be between 0.1 and {maximum} percent")
        if self.product is not BinanceProduct.USDS_FUTURES:
            return await self.submit_order(
                symbol=symbol,
                side=side,
                type="TRAILING_STOP_MARKET",
                quantity=quantity,
                activation_price=activation_price,
                callback_rate=rate,
                position_side=position_side,
                close_position=close_position,
                working_type=working_type,
                client_order_id=client_order_id,
            )
        return await self.submit_algo_order(
            algo_type="CONDITIONAL",
            symbol=symbol,
            side=side,
            type="TRAILING_STOP_MARKET",
            quantity=quantity,
            activate_price=activation_price,
            callback_rate=rate,
            position_side=position_side,
            close_position=close_position,
            working_type=working_type,
            client_algo_id=client_order_id,
        )

    async def submit_protection_order(
        self, request: BinanceFuturesProtectionOrder
    ) -> dict[str, Any]:
        """按结构化保护单意图分派到官方条件单类型。"""

        request.validate()
        if request.kind == "stop_loss":
            return await self.submit_stop_loss(
                symbol=request.symbol,
                side=request.side,
                stop_price=request.stop_price,  # type: ignore[arg-type]
                position_side=request.position_side,
                quantity=request.quantity,
                close_position=request.close_position,
                working_type=request.working_type,
                price_protect=request.price_protect,
                client_order_id=request.client_order_id,
            )
        if request.kind == "take_profit":
            return await self.submit_take_profit(
                symbol=request.symbol,
                side=request.side,
                stop_price=request.stop_price,  # type: ignore[arg-type]
                position_side=request.position_side,
                quantity=request.quantity,
                close_position=request.close_position,
                working_type=request.working_type,
                price_protect=request.price_protect,
                client_order_id=request.client_order_id,
            )
        return await self.submit_trailing_stop(
            symbol=request.symbol,
            side=request.side,
            callback_rate=request.callback_rate,  # type: ignore[arg-type]
            position_side=request.position_side,
            quantity=request.quantity,
            activation_price=request.activation_price,
            close_position=request.close_position,
            working_type=request.working_type if request.working_type else None,
            client_order_id=request.client_order_id,
        )

    async def submit_algo_order(self, **kwargs: object) -> dict[str, Any]:
        """提交 Binance 条件算法订单（``/fapi/v1/algoOrder``）。"""

        values = dict(kwargs)
        if "symbol" not in values or "side" not in values:
            raise ValueError("submit_algo_order requires symbol and side")
        values["position_side"] = await self._checked_position_side(
            values.get("position_side"), values.get("reduce_only")
        )
        if values.get("trigger_price") is not None and values.get("stop_price") is not None:
            raise ValueError("provide trigger_price or stop_price, not both")
        params: list[tuple[str, object]] = []
        aliases = {
            "algo_type": "algoType",
            "symbol": "symbol",
            "side": "side",
            "type": "type",
            "quantity": "quantity",
            "price": "price",
            "trigger_price": "triggerPrice",
            "stop_price": "triggerPrice",
            "position_side": "positionSide",
            "close_position": "closePosition",
            "working_type": "workingType",
            "price_protect": "priceProtect",
            "callback_rate": "callbackRate",
            "activation_price": "activatePrice",
            "activate_price": "activatePrice",
            "client_algo_id": "clientAlgoId",
            "reduce_only": "reduceOnly",
        }
        for key, api_name in aliases.items():
            value = values.get(key)
            if value is None:
                continue
            if key in {"side", "type", "position_side", "working_type", "algo_type"}:
                value = self._enum_value(str(value), key)
            elif key in {"close_position", "price_protect", "reduce_only"}:
                value = str(value).lower()
            params.append((api_name, value))
        if not any(name == "algoType" for name, _ in params):
            params.append(("algoType", "CONDITIONAL"))
        payload = await self._request_json(
            "POST", self._v1("algoOrder"), params=params, signed=True
        )
        return dict(self._require_mapping(payload, "algo order"))

    async def submit_algo_trailing_stop(
        self,
        *,
        symbol: str,
        side: str,
        callback_rate: Decimal | str,
        position_side: str | None = None,
        quantity: Decimal | str | None = None,
        activation_price: Decimal | str | None = None,
        close_position: bool = False,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """提交算法移动止盈止损；算法接口允许 callbackRate 0.1–10%。"""

        rate = Decimal(str(callback_rate))
        if not Decimal("0.1") <= rate <= Decimal("10"):
            raise ValueError("callback_rate must be between 0.1 and 10 percent")
        if close_position:
            raise ValueError("algorithm trailing stop cannot use close_position")
        if quantity is None:
            raise ValueError("algorithm trailing stop requires quantity")
        return await self.submit_algo_order(
            algo_type="CONDITIONAL",
            symbol=symbol,
            side=side,
            type="TRAILING_STOP_MARKET",
            quantity=quantity,
            callback_rate=rate,
            activation_price=activation_price,
            position_side=position_side,
            close_position=close_position,
            client_algo_id=client_algo_id,
        )

    async def get_algo_order(
        self,
        symbol: str,
        *,
        algo_id: int | str | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        if (algo_id is None) == (client_algo_id is None):
            raise ValueError("provide exactly one of algo_id or client_algo_id")
        params: list[tuple[str, object]] = [("symbol", self._normalize_symbol(symbol))]
        params.append(
            ("algoId" if algo_id is not None else "clientAlgoId", algo_id or client_algo_id)
        )
        payload = await self._request_json("GET", self._v1("algoOrder"), params=params, signed=True)
        return dict(self._require_mapping(payload, "algo order"))

    async def open_algo_orders(self, symbol: str | None = None) -> tuple[dict[str, Any], ...]:
        params: tuple[tuple[str, object], ...] = ()
        if symbol is not None:
            params = (("symbol", self._normalize_symbol(symbol)),)
        payload = await self._request_json(
            "GET", self._v1("openAlgoOrders"), params=params, signed=True
        )
        if not isinstance(payload, list) or any(not isinstance(item, Mapping) for item in payload):
            raise BinanceProtocolError("Binance Futures open algo orders response must be a list")
        return tuple(dict(item) for item in payload)

    async def all_algo_orders(
        self,
        symbol: str,
        *,
        algo_id: int | str | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> tuple[dict[str, Any], ...]:
        """按官方分页参数读取指定合约的 Algo 历史和活动订单。"""

        if not 1 <= limit <= 1_000:
            raise ValueError("limit must be between 1 and 1000")
        if start_time_ms is not None and start_time_ms < 0:
            raise ValueError("start_time_ms must be non-negative")
        if end_time_ms is not None and end_time_ms < 0:
            raise ValueError("end_time_ms must be non-negative")
        if start_time_ms is not None and end_time_ms is not None and end_time_ms < start_time_ms:
            raise ValueError("end_time_ms must not precede start_time_ms")
        params: list[tuple[str, object]] = [
            ("symbol", self._normalize_symbol(symbol)),
            ("limit", limit),
        ]
        if algo_id is not None:
            params.append(("algoId", algo_id))
        if start_time_ms is not None:
            params.append(("startTime", start_time_ms))
        if end_time_ms is not None:
            params.append(("endTime", end_time_ms))
        payload = await self._request_json(
            "GET", self._v1("allAlgoOrders"), params=params, signed=True
        )
        if not isinstance(payload, list) or any(not isinstance(item, Mapping) for item in payload):
            raise BinanceProtocolError("Binance Futures all algo orders response must be a list")
        return tuple(dict(item) for item in payload)

    async def cancel_algo_order(
        self,
        symbol: str,
        *,
        algo_id: int | str | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        if (algo_id is None) == (client_algo_id is None):
            raise ValueError("provide exactly one of algo_id or client_algo_id")
        params: list[tuple[str, object]] = [("symbol", self._normalize_symbol(symbol))]
        params.append(
            ("algoId" if algo_id is not None else "clientAlgoId", algo_id or client_algo_id)
        )
        payload = await self._request_json(
            "DELETE", self._v1("algoOrder"), params=params, signed=True
        )
        return dict(self._require_mapping(payload, "cancel algo order"))

    async def cancel_all_algo_orders(self, symbol: str) -> dict[str, Any]:
        payload = await self._request_json(
            "DELETE",
            self._v1("algoOpenOrders"),
            params=(("symbol", self._normalize_symbol(symbol)),),
            signed=True,
        )
        return dict(self._require_mapping(payload, "cancel all algo orders"))

    async def cancel_order(
        self,
        symbol: str,
        *,
        order_id: int | str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """撤销一笔订单；必须提供交易所订单号或客户端订单号之一。"""

        if (order_id is None) == (client_order_id is None):
            raise ValueError("provide exactly one of order_id or client_order_id")
        params: list[tuple[str, object]] = [("symbol", self._normalize_symbol(symbol))]
        if order_id is not None:
            params.append(("orderId", order_id))
        else:
            params.append(("origClientOrderId", client_order_id))
        payload = await self._request_json(
            "DELETE", self._order_path("order"), params=params, signed=True
        )
        return dict(self._require_mapping(payload, "cancel order"))

    async def cancel_all_orders(self, symbol: str) -> dict[str, Any]:
        """撤销指定合约的全部未完成订单。"""

        payload = await self._request_json(
            "DELETE",
            self._order_path("allOpenOrders"),
            params=(("symbol", self._normalize_symbol(symbol)),),
            signed=True,
        )
        return dict(self._require_mapping(payload, "cancel all orders"))

    async def validate_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal | str | None = None,
        price: Decimal | str | None = None,
        time_in_force: str | None = None,
        position_side: str | None = None,
        reduce_only: bool | None = None,
        stop_price: Decimal | str | None = None,
        close_position: bool | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """调用官方合约测试订单端点，但不实际创建订单。"""

        normalized_position_side = await self._checked_position_side(position_side, reduce_only)

        params: list[tuple[str, object]] = [
            ("symbol", self._normalize_symbol(symbol)),
            ("side", self._enum_value(side, "side")),
            ("type", self._enum_value(order_type, "order_type")),
        ]
        optional: tuple[tuple[str, object | None], ...] = (
            ("quantity", quantity),
            ("price", price),
            (
                "timeInForce",
                None if time_in_force is None else self._enum_value(time_in_force, "time_in_force"),
            ),
            (
                "positionSide",
                normalized_position_side,
            ),
            ("reduceOnly", None if reduce_only is None else str(reduce_only).lower()),
            ("stopPrice", stop_price),
            ("closePosition", None if close_position is None else str(close_position).lower()),
            ("newClientOrderId", client_order_id),
        )
        params.extend((name, value) for name, value in optional if value is not None)
        payload = await self._request_json(
            "POST", self._v1("order/test"), params=params, signed=True
        )
        return dict(self._require_mapping(payload, "test order"))

    async def _checked_position_side(
        self,
        position_side: object,
        reduce_only: object,
    ) -> str:
        """在任何订单请求前核验账户模式并返回规范化持仓方向。"""

        hedge_mode = await self.position_side_mode()
        if hedge_mode:
            if position_side is None:
                raise ValueError("position_side must be LONG or SHORT in Hedge Mode")
            normalized = self._enum_value(str(position_side), "position_side")
            if normalized not in {"LONG", "SHORT"}:
                raise ValueError("position_side must be LONG or SHORT in Hedge Mode")
            if reduce_only is not None:
                raise ValueError("reduce_only is not allowed in Hedge Mode")
            return normalized

        if position_side is None:
            return "BOTH"
        normalized = self._enum_value(str(position_side), "position_side")
        if normalized != "BOTH":
            raise ValueError("position_side must be BOTH in One-way Mode")
        return normalized

    def _v1(self, suffix: str) -> str:
        return f"/{self._api_prefix}/v1/{suffix}"

    def _order_path(self, suffix: str) -> str:
        return self._v1(suffix)

    def _account_trades_path(self) -> str:
        return self._v1("userTrades")

    def _order_params(self, values: Mapping[str, object]) -> list[tuple[str, object]]:
        order_type = values.get("type", values.get("order_type"))
        if "symbol" not in values or "side" not in values or order_type is None:
            raise ValueError("submit_order requires symbol, side and type")
        params: list[tuple[str, object]] = [
            ("symbol", self._normalize_symbol(str(values["symbol"]))),
            ("side", self._enum_value(str(values["side"]), "side")),
            ("type", self._enum_value(str(order_type), "order_type")),
        ]
        aliases = {
            "quantity": "quantity",
            "price": "price",
            "time_in_force": "timeInForce",
            "position_side": "positionSide",
            "reduce_only": "reduceOnly",
            "stop_price": "stopPrice",
            "close_position": "closePosition",
            "client_order_id": "newClientOrderId",
            "new_client_order_id": "newClientOrderId",
            "working_type": "workingType",
            "price_protect": "priceProtect",
            "activation_price": "activationPrice",
            "callback_rate": "callbackRate",
            "price_match": "priceMatch",
            "self_trade_prevention_mode": "selfTradePreventionMode",
        }
        for key, api_name in aliases.items():
            value = values.get(key)
            if value is None:
                continue
            if key in {"time_in_force", "position_side", "working_type"}:
                value = self._enum_value(str(value), key)
            elif key in {"reduce_only", "close_position", "price_protect"}:
                value = str(value).lower()
            params.append((api_name, value))
        return params

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Sequence[tuple[str, object]] = (),
        signed: bool = False,
        api_key_only: bool = False,
    ) -> Any:
        if signed and api_key_only:
            raise ValueError("signed and api_key_only are mutually exclusive")
        request_params = list(params)
        headers: dict[str, str] = {"Accept": "application/json"}
        if signed:
            credentials = self._require_credentials()
            request_params.extend(
                (
                    ("recvWindow", self._recv_window_ms),
                    ("timestamp", self._clock_ms() + self._server_time_offset_ms),
                )
            )
            unsigned_query = urlencode(request_params)
            request_params.append(
                ("signature", sign_hmac_sha256(credentials.secret_key, unsigned_query))
            )
            headers["X-MBX-APIKEY"] = credentials.api_key
        elif api_key_only:
            headers["X-MBX-APIKEY"] = self._require_credentials().api_key
        query = urlencode(request_params)
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = HttpRequest(
            method=method,
            url=url,
            headers=headers,
            timeout_seconds=self._timeout_seconds,
        )
        try:
            response = await self._transport.request(request)
        except HttpTransportError:
            raise BinanceTransportError("Binance Futures HTTP request failed") from None
        try:
            payload = response.json()
        except (TypeError, ValueError):
            raise BinanceProtocolError(
                f"Binance Futures returned invalid JSON (HTTP {response.status_code})"
            ) from None
        if response.status_code >= 400:
            mapping = payload if isinstance(payload, Mapping) else {}
            code_value = mapping.get("code")
            try:
                code = None if code_value is None else int(code_value)
            except (TypeError, ValueError):
                code = None
            message = self._safe_message(mapping.get("msg", "request rejected"))
            raise BinanceAPIError(
                status_code=response.status_code,
                code=code,
                message=message,
            )
        return payload

    def _require_credentials(self) -> BinanceCredentials:
        if self._credentials is None:
            raise BinanceConfigurationError(
                "Binance Futures credentials are required for signed endpoints"
            )
        return self._credentials

    def _safe_message(self, message: object) -> str:
        text = str(message).replace("\r", " ").replace("\n", " ")
        if self._credentials is not None:
            text = text.replace(self._credentials.api_key, "<redacted>")
            text = text.replace(self._credentials.secret_key, "<redacted>")
        text = _SENSITIVE_ASSIGNMENT.sub(lambda match: f"{match.group(1)}=<redacted>", text)
        return text[:500] or "request rejected"

    @staticmethod
    def _require_mapping(payload: Any, description: str) -> Mapping[str, Any]:
        if not isinstance(payload, Mapping):
            raise BinanceProtocolError(f"Binance Futures {description} response must be an object")
        return payload

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not _SYMBOL.fullmatch(normalized):
            raise ValueError(f"invalid Binance Futures symbol: {symbol!r}")
        return normalized

    @staticmethod
    def _validate_listen_key(listen_key: str) -> str:
        if not isinstance(listen_key, str) or not listen_key.strip():
            raise ValueError("listen_key must not be blank")
        # 密钥是透明字符串，但路径字符必须受限，防止误把查询参数拼进请求。
        allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        if any(char not in allowed for char in listen_key):
            raise ValueError("listen_key contains invalid characters")
        return listen_key

    @staticmethod
    def _enum_value(value: str, name: str) -> str:
        normalized = value.strip().upper()
        if not normalized or not normalized.replace("_", "").isalnum():
            raise ValueError(f"invalid Binance Futures {name}: {value!r}")
        return normalized
