"""只读且不泄露表示层细节的 DeepSeek API 健康探测器。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from urllib.parse import urlsplit

import httpx

from gribuki_trade.security.config import SecretValue

from .deepseek_chat import DEFAULT_DEEPSEEK_BASE_URL, DEFAULT_DEEPSEEK_MODEL

_SAFE_MODEL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_MAX_MODEL_COUNT = 1_000


class DeepSeekHealthErrorCode(StrEnum):
    """不暴露供应商响应细节的稳定公开错误分类。"""

    AUTHENTICATION_FAILED = "AUTHENTICATION_FAILED"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    RATE_LIMITED = "RATE_LIMITED"
    NETWORK_ERROR = "NETWORK_ERROR"
    INVALID_RESPONSE = "INVALID_RESPONSE"
    PROVIDER_ERROR = "PROVIDER_ERROR"


@dataclass(frozen=True, slots=True)
class DeepSeekHealthResult:
    """允许穿过健康检查边界的唯一供应商状态投影。"""

    available_model_ids: tuple[str, ...]
    deepseek_v4_pro_available: bool
    error_code: DeepSeekHealthErrorCode | None = None

    @property
    def ok(self) -> bool:
        return self.error_code is None

    @property
    def deepseek_v4_flash_available(self) -> bool:
        return "deepseek-v4-flash" in self.available_model_ids

    @property
    def default_model_available(self) -> bool:
        return DEFAULT_DEEPSEEK_MODEL in self.available_model_ids


class DeepSeekHealthClient:
    """检查带鉴权且只读的 DeepSeek ``GET /models`` 端点。"""

    def __init__(
        self,
        api_key: SecretValue,
        *,
        base_url: str = DEFAULT_DEEPSEEK_BASE_URL,
        timeout_seconds: float = 10,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not api_key or not api_key.reveal().strip():
            raise ValueError("api_key must not be empty")
        if not 0 < timeout_seconds <= 60:
            raise ValueError("timeout_seconds must be between 0 and 60")
        self._api_key = api_key
        self._base_url = _validated_base_url(base_url)
        self._timeout = timeout_seconds
        self._client = client

    async def check(self) -> DeepSeekHealthResult:
        headers = {
            "Authorization": f"Bearer {self._api_key.reveal()}",
            "Accept": "application/json",
        }
        try:
            if self._client is not None:
                response = await self._client.get(
                    f"{self._base_url}/models",
                    headers=headers,
                    timeout=self._timeout,
                    follow_redirects=False,
                )
            else:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    response = await client.get(
                        f"{self._base_url}/models",
                        headers=headers,
                        follow_redirects=False,
                    )
        except (httpx.TimeoutException, httpx.NetworkError):
            return _failed(DeepSeekHealthErrorCode.NETWORK_ERROR)
        except httpx.HTTPError:
            return _failed(DeepSeekHealthErrorCode.NETWORK_ERROR)

        error_code = _http_error_code(response.status_code)
        if error_code is not None:
            return _failed(error_code)
        try:
            document = response.json()
            model_ids = _parse_model_ids(document)
        except (TypeError, ValueError):
            return _failed(DeepSeekHealthErrorCode.INVALID_RESPONSE)
        return DeepSeekHealthResult(
            available_model_ids=model_ids,
            deepseek_v4_pro_available="deepseek-v4-pro" in model_ids,
        )


def _validated_base_url(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("base_url must not be empty")
    resolved = value.strip().rstrip("/")
    parsed = urlsplit(resolved)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("base_url must be an HTTPS origin/path without credentials or query")
    return resolved


def _http_error_code(status_code: int) -> DeepSeekHealthErrorCode | None:
    if 200 <= status_code < 300:
        return None
    return {
        401: DeepSeekHealthErrorCode.AUTHENTICATION_FAILED,
        402: DeepSeekHealthErrorCode.INSUFFICIENT_BALANCE,
        429: DeepSeekHealthErrorCode.RATE_LIMITED,
    }.get(status_code, DeepSeekHealthErrorCode.PROVIDER_ERROR)


def _parse_model_ids(document: Any) -> tuple[str, ...]:
    if not isinstance(document, dict):
        raise TypeError("model list response must be an object")
    if document.get("object") != "list":
        raise ValueError("model list response has an invalid object type")
    data = document.get("data")
    if not isinstance(data, list) or len(data) > _MAX_MODEL_COUNT:
        raise TypeError("model list data must be a bounded array")
    identifiers: set[str] = set()
    for item in data:
        if not isinstance(item, dict):
            raise TypeError("model list item must be an object")
        identifier = item.get("id")
        if not isinstance(identifier, str) or _SAFE_MODEL_ID.fullmatch(identifier) is None:
            raise ValueError("model ID is invalid")
        identifiers.add(identifier)
    return tuple(sorted(identifiers))


def _failed(code: DeepSeekHealthErrorCode) -> DeepSeekHealthResult:
    return DeepSeekHealthResult(
        available_model_ids=(),
        deepseek_v4_pro_available=False,
        error_code=code,
    )
