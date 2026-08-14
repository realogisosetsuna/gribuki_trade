"""把 Schwab OAuth 访问令牌与刷新令牌持久化到 OS keyring。"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime

from gribuki_trade.security import SecretProvider

from .errors import SchwabOAuthError
from .oauth import OAuthToken

SCHWAB_OAUTH_TOKEN_SECRET = "schwab.oauth.token"


class KeyringOAuthTokenStore:
    """通过 ``SecretProvider`` 持久化一份 OAuth 令牌文档。

    默认提供者应为 ``KeyringSecretProvider``。此实现接收协议类型，从而可在
    不接触本机凭据保险库的情况下进行测试。
    """

    def __init__(
        self,
        provider: SecretProvider,
        secret_name: str = SCHWAB_OAUTH_TOKEN_SECRET,
    ) -> None:
        if not secret_name.strip():
            raise ValueError("OAuth token secret name must not be blank")
        self._provider = provider
        self._secret_name = secret_name

    async def load(self) -> OAuthToken | None:
        encoded = await asyncio.to_thread(
            self._provider.get_secret,
            self._secret_name,
        )
        if encoded is None:
            return None
        try:
            payload = json.loads(encoded)
            if not isinstance(payload, dict):
                raise ValueError
            access_token = payload["access_token"]
            token_type = payload["token_type"]
            issued_at = datetime.fromisoformat(payload["issued_at"])
            expires_at = datetime.fromisoformat(payload["expires_at"])
            refresh_token = payload.get("refresh_token")
            scope = payload.get("scope")
            if not isinstance(access_token, str) or not access_token:
                raise ValueError
            if not isinstance(token_type, str) or not token_type:
                raise ValueError
            if refresh_token is not None and not isinstance(refresh_token, str):
                raise ValueError
            if scope is not None and not isinstance(scope, str):
                raise ValueError
            if issued_at.tzinfo is None or expires_at.tzinfo is None:
                raise ValueError
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            raise SchwabOAuthError("stored Schwab OAuth token is malformed") from None
        return OAuthToken(
            access_token=access_token,
            token_type=token_type,
            issued_at=issued_at,
            expires_at=expires_at,
            refresh_token=refresh_token,
            scope=scope,
        )

    async def save(self, token: OAuthToken) -> None:
        payload = json.dumps(
            {
                "access_token": token.access_token,
                "token_type": token.token_type,
                "issued_at": token.issued_at.isoformat(),
                "expires_at": token.expires_at.isoformat(),
                "refresh_token": token.refresh_token,
                "scope": token.scope,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        await asyncio.to_thread(
            self._provider.set_secret,
            self._secret_name,
            payload,
        )

    async def clear(self) -> bool:
        return await asyncio.to_thread(
            self._provider.delete_secret,
            self._secret_name,
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(secret_name={self._secret_name!r}, "
            "token=<redacted>)"
        )
