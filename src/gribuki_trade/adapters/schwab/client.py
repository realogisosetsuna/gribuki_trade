"""Async Charles Schwab Market Data and Trader API client skeleton."""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from enum import StrEnum
from typing import TypeAlias
from urllib.parse import quote, urlencode, urlsplit

from .errors import (
    SchwabAuthenticationError,
    SchwabHttpError,
    SchwabLiveAccessError,
    SchwabPermissionError,
    SchwabProtocolError,
    SchwabRateLimitError,
    SchwabServerError,
)
from .oauth import SchwabOAuthClient
from .transport import AsyncHttpTransport, HttpRequest, HttpResponse

JsonObject: TypeAlias = dict[str, object]
QueryScalar: TypeAlias = str | int | float | bool | date | datetime
QueryValue: TypeAlias = QueryScalar | Sequence[QueryScalar] | None


class SchwabEnvironment(StrEnum):
    LIVE = "LIVE"
    OFFLINE = "OFFLINE"


@dataclass(frozen=True, slots=True)
class SchwabEndpoints:
    """Endpoint set with an explicit production-network safety interlock."""

    environment: SchwabEnvironment = SchwabEnvironment.LIVE
    market_data_base_url: str = "https://api.schwabapi.com/marketdata/v1"
    trader_base_url: str = "https://api.schwabapi.com/trader/v1"
    allow_live: bool = field(default=False, repr=False)

    def __post_init__(self) -> None:
        try:
            environment = SchwabEnvironment(self.environment)
        except ValueError as error:
            raise ValueError("unknown Schwab environment") from error
        object.__setattr__(self, "environment", environment)
        urls = (self.market_data_base_url, self.trader_base_url)
        if any(not _is_absolute_http_url(url) for url in urls):
            raise ValueError("Schwab API base URLs must be absolute HTTP(S) URLs")
        production_host_used = any(
            urlsplit(url).hostname == "api.schwabapi.com" for url in urls
        )
        live_access = environment is SchwabEnvironment.LIVE or production_host_used
        if live_access and not self.allow_live:
            raise SchwabLiveAccessError(
                "Schwab production access is disabled; pass allow_live=True explicitly"
            )

    @classmethod
    def offline(
        cls,
        *,
        market_data_base_url: str = "https://marketdata.schwab.invalid/v1",
        trader_base_url: str = "https://trader.schwab.invalid/v1",
    ) -> SchwabEndpoints:
        return cls(
            environment=SchwabEnvironment.OFFLINE,
            market_data_base_url=market_data_base_url,
            trader_base_url=trader_base_url,
        )

    @classmethod
    def production(cls, *, allow_live: bool = False) -> SchwabEndpoints:
        return cls(allow_live=allow_live)


@dataclass(frozen=True, slots=True, repr=False)
class AccountNumberHash:
    """Raw-to-hash association returned by ``/accounts/accountNumbers``."""

    account_number: str
    hash_value: str

    def __repr__(self) -> str:
        return "AccountNumberHash(account_number=<redacted>, hash_value=<redacted>)"


@dataclass(frozen=True, slots=True)
class PlacedOrder:
    """The identifiers from a successful Schwab order response."""

    order_id: str | None
    location: str | None = field(repr=False)


class SchwabApiClient:
    """Thin typed façade over Schwab's Market Data and Trader REST APIs.

    The client deliberately has no built-in transport, retries, quota values,
    account geography assumptions, or permission assumptions.  It performs
    exactly one special replay: a 401 refreshes the token and retries once.
    A 429 is surfaced with parsed ``Retry-After`` data, and 5xx responses are
    never retried here.
    """

    def __init__(
        self,
        *,
        oauth: SchwabOAuthClient,
        transport: AsyncHttpTransport,
        endpoints: SchwabEndpoints | None = None,
        clock: Callable[[], datetime] | None = None,
        request_timeout: float | None = 30,
    ) -> None:
        # The default resolves to LIVE and therefore raises until the caller
        # constructs SchwabEndpoints.production(allow_live=True) explicitly.
        self.endpoints = endpoints or SchwabEndpoints.production()
        self._oauth = oauth
        self._transport = transport
        self._clock = clock or (lambda: datetime.now(UTC))
        self._request_timeout = request_timeout

    async def quotes(
        self,
        symbols: str | Sequence[str],
        *,
        fields: str | Sequence[str] | None = None,
        indicative: bool | None = None,
    ) -> object:
        symbol_value = _comma_values(symbols, name="symbols")
        params: dict[str, QueryValue] = {"symbols": symbol_value}
        if fields is not None:
            params["fields"] = _comma_values(fields, name="fields")
        if indicative is not None:
            params["indicative"] = indicative
        return await self._json_request(
            "GET", self.endpoints.market_data_base_url, "/quotes", params=params
        )

    async def price_history(self, symbol: str, **parameters: QueryValue) -> object:
        _require_non_empty(symbol, "symbol")
        params: dict[str, QueryValue] = {"symbol": symbol, **parameters}
        return await self._json_request(
            "GET", self.endpoints.market_data_base_url, "/pricehistory", params=params
        )

    async def option_chain(self, symbol: str, **parameters: QueryValue) -> object:
        _require_non_empty(symbol, "symbol")
        params: dict[str, QueryValue] = {"symbol": symbol, **parameters}
        return await self._json_request(
            "GET", self.endpoints.market_data_base_url, "/chains", params=params
        )

    async def market_hours(
        self,
        markets: str | Sequence[str],
        *,
        on_date: date | str | None = None,
    ) -> object:
        params: dict[str, QueryValue] = {
            "markets": _comma_values(markets, name="markets")
        }
        if on_date is not None:
            params["date"] = on_date
        return await self._json_request(
            "GET", self.endpoints.market_data_base_url, "/markets", params=params
        )

    async def account_numbers(self) -> tuple[AccountNumberHash, ...]:
        payload = await self._json_request(
            "GET", self.endpoints.trader_base_url, "/accounts/accountNumbers"
        )
        if not isinstance(payload, list):
            raise SchwabProtocolError("accountNumbers response must be a JSON array")
        result: list[AccountNumberHash] = []
        for item in payload:
            if not isinstance(item, dict):
                raise SchwabProtocolError("accountNumbers entry must be a JSON object")
            account_number = item.get("accountNumber")
            hash_value = item.get("hashValue")
            if (
                not isinstance(account_number, str)
                or not account_number
                or not isinstance(hash_value, str)
                or not hash_value
            ):
                raise SchwabProtocolError(
                    "accountNumbers entry omitted accountNumber or hashValue"
                )
            result.append(AccountNumberHash(account_number, hash_value))
        return tuple(result)

    async def accounts(self, *, include_positions: bool = False) -> object:
        params: dict[str, QueryValue] = {}
        if include_positions:
            params["fields"] = "positions"
        return await self._json_request(
            "GET", self.endpoints.trader_base_url, "/accounts", params=params
        )

    async def account(self, account_hash: str, *, include_positions: bool = False) -> object:
        _require_non_empty(account_hash, "account_hash")
        params: dict[str, QueryValue] = {}
        if include_positions:
            params["fields"] = "positions"
        return await self._json_request(
            "GET",
            self.endpoints.trader_base_url,
            f"/accounts/{quote(account_hash, safe='')}",
            params=params,
        )

    async def positions(self, account_hash: str) -> object:
        """Return the account representation including its positions field."""

        return await self.account(account_hash, include_positions=True)

    async def orders(
        self,
        account_hash: str,
        *,
        max_results: int | None = None,
        from_entered_time: datetime | str | None = None,
        to_entered_time: datetime | str | None = None,
        status: str | None = None,
        **parameters: QueryValue,
    ) -> object:
        _require_non_empty(account_hash, "account_hash")
        params: dict[str, QueryValue] = dict(parameters)
        if max_results is not None:
            params["maxResults"] = max_results
        if from_entered_time is not None:
            params["fromEnteredTime"] = from_entered_time
        if to_entered_time is not None:
            params["toEnteredTime"] = to_entered_time
        if status is not None:
            params["status"] = status
        return await self._json_request(
            "GET",
            self.endpoints.trader_base_url,
            f"/accounts/{quote(account_hash, safe='')}/orders",
            params=params,
        )

    async def order(self, account_hash: str, order_id: str | int) -> object:
        _require_non_empty(account_hash, "account_hash")
        _require_non_empty(str(order_id), "order_id")
        return await self._json_request(
            "GET",
            self.endpoints.trader_base_url,
            f"/accounts/{quote(account_hash, safe='')}/orders/{quote(str(order_id), safe='')}",
        )

    async def place_order(
        self, account_hash: str, order_payload: Mapping[str, object]
    ) -> PlacedOrder:
        _require_non_empty(account_hash, "account_hash")
        response = await self._request(
            "POST",
            self.endpoints.trader_base_url,
            f"/accounts/{quote(account_hash, safe='')}/orders",
            json_body=order_payload,
        )
        location = response.header("Location")
        order_id = _order_id_from_location(location)
        if order_id is None and response.body:
            payload = _response_json(response)
            if isinstance(payload, dict):
                candidate = payload.get("orderId")
                if isinstance(candidate, (str, int)):
                    order_id = str(candidate)
        return PlacedOrder(order_id=order_id, location=location)

    async def cancel_order(self, account_hash: str, order_id: str | int) -> None:
        _require_non_empty(account_hash, "account_hash")
        _require_non_empty(str(order_id), "order_id")
        await self._request(
            "DELETE",
            self.endpoints.trader_base_url,
            f"/accounts/{quote(account_hash, safe='')}/orders/{quote(str(order_id), safe='')}",
        )

    # Explicit get_* aliases make the public surface easy to discover without
    # choosing one naming convention for callers.
    get_quotes = quotes
    get_price_history = price_history
    get_option_chain = option_chain
    get_market_hours = market_hours
    get_account_numbers = account_numbers
    get_accounts = accounts
    get_account = account
    get_positions = positions
    get_orders = orders
    get_order = order

    async def _json_request(
        self,
        method: str,
        base_url: str,
        path: str,
        *,
        params: Mapping[str, QueryValue] | None = None,
    ) -> object:
        return _response_json(await self._request(method, base_url, path, params=params))

    async def _request(
        self,
        method: str,
        base_url: str,
        path: str,
        *,
        params: Mapping[str, QueryValue] | None = None,
        json_body: Mapping[str, object] | None = None,
    ) -> HttpResponse:
        url = _build_url(base_url, path, params)
        token = await self._oauth.token()
        for auth_attempt in range(2):
            headers = {
                "Accept": "application/json",
                "Authorization": f"{token.token_type} {token.access_token}",
            }
            body: bytes | None = None
            if json_body is not None:
                headers["Content-Type"] = "application/json"
                body = json.dumps(
                    json_body, separators=(",", ":"), ensure_ascii=False
                ).encode("utf-8")
            response = await self._transport.send(
                HttpRequest(
                    method=method,
                    url=url,
                    headers=headers,
                    body=body,
                    timeout=self._request_timeout,
                )
            )
            if response.status_code != 401:
                return self._validate_response(method, url, response)
            if auth_attempt == 0:
                # A concrete 401 indicates authorization failed; unlike an
                # ambiguous timeout/5xx, it is safe to refresh and replay once.
                token = await self._oauth.refresh(expected_access_token=token.access_token)
                continue
            raise SchwabAuthenticationError(401, method, url)
        raise AssertionError("unreachable")

    def _validate_response(
        self, method: str, url: str, response: HttpResponse
    ) -> HttpResponse:
        status = response.status_code
        if 200 <= status < 300:
            return response
        if status == 403:
            raise SchwabPermissionError(status, method, url)
        if status == 429:
            raw = response.header("Retry-After")
            retry_after, retry_at = self._parse_retry_after(raw)
            raise SchwabRateLimitError(
                method,
                url,
                retry_after=retry_after,
                retry_at=retry_at,
                raw_retry_after=raw,
            )
        if 500 <= status < 600:
            raise SchwabServerError(status, method, url)
        raise SchwabHttpError(status, method, url)

    def _parse_retry_after(
        self, raw: str | None
    ) -> tuple[float | None, datetime | None]:
        if raw is None:
            return None, None
        value = raw.strip()
        try:
            seconds = float(value)
        except ValueError:
            seconds = math.nan
        if math.isfinite(seconds) and seconds >= 0:
            return seconds, self._now() + _seconds(seconds)
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None, None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        retry_at = retry_at.astimezone(UTC)
        return max(0.0, (retry_at - self._now()).total_seconds()), retry_at

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("API clock must return a timezone-aware datetime")
        return now.astimezone(UTC)


def _is_absolute_http_url(url: str) -> bool:
    parsed = urlsplit(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _require_non_empty(value: str, name: str) -> None:
    if not value:
        raise ValueError(f"{name} must not be empty")


def _comma_values(values: str | Sequence[str], *, name: str) -> str:
    result = values if isinstance(values, str) else ",".join(values)
    if not result:
        raise ValueError(f"{name} must not be empty")
    return result


def _query_scalar(value: QueryScalar) -> str | int | float:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _build_url(
    base_url: str, path: str, params: Mapping[str, QueryValue] | None
) -> str:
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    if not params:
        return url
    pairs: list[tuple[str, str | int | float]] = []
    for name, value in params.items():
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool, date, datetime)):
            pairs.append((name, _query_scalar(value)))
        else:
            pairs.extend((name, _query_scalar(item)) for item in value)
    return f"{url}?{urlencode(pairs)}" if pairs else url


def _response_json(response: HttpResponse) -> object:
    try:
        return response.json()
    except (UnicodeDecodeError, ValueError) as error:
        raise SchwabProtocolError("Schwab API returned invalid JSON") from error


def _order_id_from_location(location: str | None) -> str | None:
    if not location:
        return None
    path = urlsplit(location).path.rstrip("/")
    return path.rsplit("/", 1)[-1] or None


def _seconds(value: float) -> timedelta:
    return timedelta(seconds=value)
