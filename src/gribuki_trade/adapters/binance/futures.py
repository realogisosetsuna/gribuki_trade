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
from typing import Any
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


@dataclass(frozen=True, slots=True)
class BinanceFuturesTicker:
    symbol: str
    price: Decimal
    time_ms: int | None = None


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
    def _api_prefix(self) -> str:
        return self._profile.api_prefix

    async def ping(self) -> None:
        payload = await self._request_json("GET", self._v1("ping"))
        if not isinstance(payload, Mapping):
            raise BinanceProtocolError("Binance Futures ping response must be an object")

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
            raise BinanceProtocolError(
                "Binance Futures ticker response is malformed"
            ) from None
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
                None
                if time_in_force is None
                else self._enum_value(time_in_force, "time_in_force"),
            ),
            (
                "positionSide",
                None
                if position_side is None
                else self._enum_value(position_side, "position_side"),
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

    def _v1(self, suffix: str) -> str:
        return f"/{self._api_prefix}/v1/{suffix}"

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Sequence[tuple[str, object]] = (),
        signed: bool = False,
    ) -> Any:
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
        return text[:500] or "request rejected"

    @staticmethod
    def _require_mapping(payload: Any, description: str) -> Mapping[str, Any]:
        if not isinstance(payload, Mapping):
            raise BinanceProtocolError(
                f"Binance Futures {description} response must be an object"
            )
        return payload

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not _SYMBOL.fullmatch(normalized):
            raise ValueError(f"invalid Binance Futures symbol: {symbol!r}")
        return normalized

    @staticmethod
    def _enum_value(value: str, name: str) -> str:
        normalized = value.strip().upper()
        if not normalized or not normalized.replace("_", "").isalnum():
            raise ValueError(f"invalid Binance Futures {name}: {value!r}")
        return normalized
