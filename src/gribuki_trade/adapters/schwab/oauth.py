"""OAuth 2.0 Authorization Code support for Charles Schwab."""

from __future__ import annotations

import asyncio
import base64
import hmac
import math
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit

from .errors import SchwabLiveAccessError, SchwabOAuthError
from .transport import AsyncHttpTransport, HttpRequest, HttpResponse


@dataclass(frozen=True, slots=True)
class OAuthToken:
    """An OAuth token whose access-token TTL comes from ``expires_in``."""

    access_token: str = field(repr=False)
    token_type: str
    issued_at: datetime
    expires_at: datetime
    refresh_token: str | None = field(default=None, repr=False)
    scope: str | None = None

    def is_expired(self, now: datetime, *, leeway_seconds: float = 0) -> bool:
        return now + timedelta(seconds=leeway_seconds) >= self.expires_at


class TokenStore(Protocol):
    """Async persistence boundary; implementations decide encryption/storage."""

    async def load(self) -> OAuthToken | None: ...

    async def save(self, token: OAuthToken) -> None: ...


class InMemoryTokenStore:
    """Process-local token store suitable for tests, never disk persistence."""

    def __init__(self, token: OAuthToken | None = None) -> None:
        self._token = token

    async def load(self) -> OAuthToken | None:
        return self._token

    async def save(self, token: OAuthToken) -> None:
        self._token = token


@dataclass(frozen=True, slots=True)
class AuthorizationRequest:
    url: str = field(repr=False)
    state: str = field(repr=False)
    redirect_uri: str

    def __repr__(self) -> str:
        return (
            f"AuthorizationRequest(url=<redacted>, state=<redacted>, "
            f"redirect_uri={self.redirect_uri!r})"
        )


class SchwabOAuthClient:
    """Authorization Code client with exact redirect URI and state checks."""

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        transport: AsyncHttpTransport,
        token_store: TokenStore,
        authorization_endpoint: str = "https://api.schwabapi.com/v1/oauth/authorize",
        token_endpoint: str = "https://api.schwabapi.com/v1/oauth/token",
        clock: Callable[[], datetime] | None = None,
        expiry_leeway_seconds: float = 30,
        request_timeout: float | None = 30,
        allow_live: bool = False,
    ) -> None:
        if not client_id or not client_secret:
            raise ValueError("client_id and client_secret must not be empty")
        self._validate_redirect_uri(redirect_uri)
        if expiry_leeway_seconds < 0:
            raise ValueError("expiry_leeway_seconds must be non-negative")
        if any(
            urlsplit(endpoint).hostname == "api.schwabapi.com"
            for endpoint in (authorization_endpoint, token_endpoint)
        ) and not allow_live:
            raise SchwabLiveAccessError(
                "Schwab production OAuth is disabled; pass allow_live=True explicitly"
            )
        self._client_id = client_id
        self._client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.authorization_endpoint = authorization_endpoint
        self.token_endpoint = token_endpoint
        self._transport = transport
        self._token_store = token_store
        self._clock = clock or (lambda: datetime.now(UTC))
        self._expiry_leeway_seconds = expiry_leeway_seconds
        self._request_timeout = request_timeout
        self._refresh_lock = asyncio.Lock()
        self._pending_states: set[str] = set()

    @staticmethod
    def _validate_redirect_uri(redirect_uri: str) -> None:
        parsed = urlsplit(redirect_uri)
        if not redirect_uri or parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("redirect_uri must be an absolute HTTP(S) URI")
        if parsed.fragment:
            raise ValueError("redirect_uri must not contain a fragment")

    def authorization_request(self, *, state: str | None = None) -> AuthorizationRequest:
        """Create an authorization URL and remember its one-time state value."""

        state = state or secrets.token_urlsafe(32)
        if not state or state in self._pending_states:
            raise ValueError("state must be non-empty and unique")
        self._pending_states.add(state)
        query = urlencode(
            {
                "client_id": self._client_id,
                "redirect_uri": self.redirect_uri,
                "response_type": "code",
                "state": state,
            }
        )
        separator = "&" if "?" in self.authorization_endpoint else "?"
        return AuthorizationRequest(
            url=f"{self.authorization_endpoint}{separator}{query}",
            state=state,
            redirect_uri=self.redirect_uri,
        )

    async def exchange_callback(
        self, callback_url: str, *, expected_state: str
    ) -> OAuthToken:
        """Validate an exact callback target and exchange its authorization code."""

        callback = urlsplit(callback_url)
        configured = urlsplit(self.redirect_uri)
        if (
            callback.scheme,
            callback.netloc,
            callback.path,
            callback.fragment,
        ) != (
            configured.scheme,
            configured.netloc,
            configured.path,
            configured.fragment,
        ):
            raise SchwabOAuthError("OAuth callback does not match the configured redirect URI")

        callback_pairs = parse_qsl(callback.query, keep_blank_values=True)
        configured_pairs = parse_qsl(configured.query, keep_blank_values=True)
        oauth_names = {"code", "state", "error", "error_description", "error_uri"}
        if [pair for pair in callback_pairs if pair[0] not in oauth_names] != configured_pairs:
            raise SchwabOAuthError("OAuth callback does not match the configured redirect URI")

        values = dict(callback_pairs)
        returned_state = values.get("state")
        if (
            expected_state not in self._pending_states
            or returned_state is None
            or not _constant_time_equal(returned_state, expected_state)
        ):
            raise SchwabOAuthError("OAuth state mismatch")
        self._pending_states.remove(expected_state)

        if "error" in values:
            raise SchwabOAuthError(f"OAuth authorization failed: {values['error']}")
        code = values.get("code")
        if not code:
            raise SchwabOAuthError("OAuth callback did not contain an authorization code")
        return await self.exchange_code(code, redirect_uri=self.redirect_uri)

    async def exchange_code(self, code: str, *, redirect_uri: str) -> OAuthToken:
        """Exchange a code using the byte-for-byte configured redirect URI."""

        if not _constant_time_equal(redirect_uri, self.redirect_uri):
            raise SchwabOAuthError("token exchange redirect_uri must match exactly")
        if not code:
            raise ValueError("authorization code must not be empty")
        return await self._request_token(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.redirect_uri,
            },
            previous=None,
        )

    async def token(self) -> OAuthToken:
        """Return a usable token, refreshing based on its server-provided TTL."""

        token = await self._token_store.load()
        if token is None:
            raise SchwabOAuthError("no OAuth token is available; authorization is required")
        if token.is_expired(self._now(), leeway_seconds=self._expiry_leeway_seconds):
            return await self.refresh(expected_access_token=token.access_token)
        return token

    async def refresh(self, *, expected_access_token: str | None = None) -> OAuthToken:
        """Refresh once, coalescing concurrent refreshes of the same token."""

        async with self._refresh_lock:
            current = await self._token_store.load()
            if current is None or current.refresh_token is None:
                raise SchwabOAuthError("no refresh token is available")
            if (
                expected_access_token is not None
                and not _constant_time_equal(current.access_token, expected_access_token)
            ):
                return current
            return await self._request_token(
                {
                    "grant_type": "refresh_token",
                    "refresh_token": current.refresh_token,
                },
                previous=current,
            )

    async def _request_token(
        self, form: Mapping[str, str], *, previous: OAuthToken | None
    ) -> OAuthToken:
        credentials = f"{self._client_id}:{self._client_secret}".encode()
        request = HttpRequest(
            method="POST",
            url=self.token_endpoint,
            headers={
                "Accept": "application/json",
                "Authorization": f"Basic {base64.b64encode(credentials).decode('ascii')}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            body=urlencode(form).encode("ascii"),
            timeout=self._request_timeout,
        )
        response = await self._transport.send(request)
        if not 200 <= response.status_code < 300:
            raise SchwabOAuthError(
                f"OAuth token endpoint returned HTTP {response.status_code}"
            )
        payload = self._json_object(response)
        access_token = payload.get("access_token")
        token_type = payload.get("token_type", "Bearer")
        if not isinstance(access_token, str) or not access_token:
            raise SchwabOAuthError("token response omitted access_token")
        if not isinstance(token_type, str) or not token_type:
            raise SchwabOAuthError("token response contained an invalid token_type")
        expires_in = self._expires_in(payload.get("expires_in"))
        issued_at = self._now()
        refresh_value = payload.get("refresh_token")
        if refresh_value is None and previous is not None:
            refresh_value = previous.refresh_token
        if refresh_value is not None and not isinstance(refresh_value, str):
            raise SchwabOAuthError("token response contained an invalid refresh_token")
        scope = payload.get("scope")
        if scope is not None and not isinstance(scope, str):
            raise SchwabOAuthError("token response contained an invalid scope")
        token = OAuthToken(
            access_token=access_token,
            token_type=token_type,
            issued_at=issued_at,
            expires_at=issued_at + timedelta(seconds=expires_in),
            refresh_token=refresh_value,
            scope=scope,
        )
        await self._token_store.save(token)
        return token

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("OAuth clock must return a timezone-aware datetime")
        return now

    @staticmethod
    def _expires_in(value: object) -> float:
        if isinstance(value, bool):
            raise SchwabOAuthError("token response omitted a valid expires_in")
        try:
            seconds = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError) as error:
            raise SchwabOAuthError("token response omitted a valid expires_in") from error
        if not math.isfinite(seconds) or seconds <= 0:
            raise SchwabOAuthError("token response omitted a valid expires_in")
        return seconds

    @staticmethod
    def _json_object(response: HttpResponse) -> dict[str, object]:
        try:
            payload = response.json()
        except (UnicodeDecodeError, ValueError) as error:
            raise SchwabOAuthError("token endpoint returned invalid JSON") from error
        if not isinstance(payload, dict):
            raise SchwabOAuthError("token endpoint returned a non-object JSON value")
        return payload


def _constant_time_equal(left: str, right: str) -> bool:
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))
