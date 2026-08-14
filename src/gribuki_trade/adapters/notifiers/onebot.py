"""用于 NapCatQQ 的最小单向出站 OneBot v11 HTTP 适配器。

适配器提供文本发送、严格限定范围的本地产物发送，以及两个只读健康端点。
其中不包含反向 WebSocket 监听器、事件处理器、命令解析器、插件加载器或交易
回调。
"""

from __future__ import annotations

import ipaddress
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from gribuki_trade.ports.notifier import (
    DeliveryReceipt,
    NotificationDeliveryError,
    NotificationTargetKind,
    OutboundNotification,
)

NAPCAT_ACCESS_TOKEN_SECRET = "napcat.onebot.access_token"

_MEBIBYTE = 1024 * 1024
_IMAGE_MEDIA_TYPES = {
    ".gif": "image/gif",
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
}
_FILE_MEDIA_TYPES = {
    **_IMAGE_MEDIA_TYPES,
    ".csv": "text/csv",
    ".md": "text/markdown",
    ".pdf": "application/pdf",
    ".txt": "text/plain",
}
_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")


class OneBotError(NotificationDeliveryError):
    """已经净化的 OneBot 传输、HTTP 或协议错误。"""


class OneBotTargetNotAllowedError(OneBotError):
    """目标不在精确允许列表中时，于执行 I/O 前抛出。"""

    def __init__(self) -> None:
        super().__init__("target_not_allowed", retryable=False)


@dataclass(frozen=True, slots=True)
class OneBotFileUploadReceipt:
    """已经净化的 OneBot 私聊或群聊文件上传结果。"""

    channel: str
    provider_file_id: str | None = None
    uploaded_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class OneBotConfig:
    """本地 NapCat HTTP 端点的安全敏感配置。"""

    access_token: str = field(repr=False)
    base_url: str = "http://127.0.0.1:3000"
    private_target_ids: frozenset[str | int] = field(default_factory=frozenset)
    group_target_ids: frozenset[str | int] = field(default_factory=frozenset)
    timeout_seconds: float = 5.0
    max_message_chars: int = 8_000
    artifact_root: Path | None = field(default=None, repr=False)
    max_image_bytes: int = 10 * _MEBIBYTE
    max_file_bytes: int = 20 * _MEBIBYTE

    def __post_init__(self) -> None:
        _validate_loopback_url(self.base_url)
        if not self.access_token:
            raise ValueError("OneBot access_token must not be empty")
        if not 0 < self.timeout_seconds <= 60:
            raise ValueError("timeout_seconds must be in (0, 60]")
        if not 0 < self.max_message_chars <= 50_000:
            raise ValueError("max_message_chars must be in (0, 50000]")
        if not 0 < self.max_image_bytes <= 100 * _MEBIBYTE:
            raise ValueError("max_image_bytes must be in (0, 100 MiB]")
        if not 0 < self.max_file_bytes <= 100 * _MEBIBYTE:
            raise ValueError("max_file_bytes must be in (0, 100 MiB]")
        if self.artifact_root is not None:
            object.__setattr__(
                self,
                "artifact_root",
                _resolve_artifact_root(self.artifact_root),
            )
        object.__setattr__(
            self,
            "private_target_ids",
            frozenset(_normalize_qq_id(value) for value in self.private_target_ids),
        )
        object.__setattr__(
            self,
            "group_target_ids",
            frozenset(_normalize_qq_id(value) for value in self.group_target_ids),
        )


class OneBotNotifier:
    """通过回环 OneBot v11 端点发送出站 QQ 消息。"""

    channel = "onebot"

    def __init__(
        self,
        config: OneBotConfig,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config
        self._client = client or httpx.AsyncClient(
            follow_redirects=False,
            trust_env=False,
        )
        self._owns_client = client is None
        self._closed = False

    async def send(self, notification: OutboundNotification) -> DeliveryReceipt:
        if notification.channel != self.channel:
            raise OneBotError("channel_mismatch", retryable=False)
        if notification.target_kind is NotificationTargetKind.PRIVATE:
            return await self.send_private(notification.target_id, notification.text)
        if notification.target_kind is NotificationTargetKind.GROUP:
            return await self.send_group(notification.target_id, notification.text)
        raise OneBotError("unsupported_target_kind", retryable=False)

    async def send_private(self, target_id: str | int, text: str) -> DeliveryReceipt:
        normalized = _normalize_qq_id(target_id)
        if normalized not in self._config.private_target_ids:
            raise OneBotTargetNotAllowedError()
        return await self._send_message("send_private_msg", "user_id", normalized, text)

    async def send_group(self, target_id: str | int, text: str) -> DeliveryReceipt:
        normalized = _normalize_qq_id(target_id)
        if normalized not in self._config.group_target_ids:
            raise OneBotTargetNotAllowedError()
        return await self._send_message("send_group_msg", "group_id", normalized, text)

    async def send_private_image(
        self,
        target_id: str | int,
        artifact: str | os.PathLike[str],
    ) -> DeliveryReceipt:
        """向允许列表中的私聊目标发送一张获准的本地图像。"""

        normalized = self._require_allowed_target(
            target_id,
            self._config.private_target_ids,
        )
        resolved = self._resolve_artifact(artifact, image_only=True)
        return await self._send_image("send_private_msg", "user_id", normalized, resolved)

    async def send_group_image(
        self,
        target_id: str | int,
        artifact: str | os.PathLike[str],
    ) -> DeliveryReceipt:
        """向允许列表中的群聊目标发送一张获准的本地图像。"""

        normalized = self._require_allowed_target(
            target_id,
            self._config.group_target_ids,
        )
        resolved = self._resolve_artifact(artifact, image_only=True)
        return await self._send_image("send_group_msg", "group_id", normalized, resolved)

    async def upload_private_file(
        self,
        target_id: str | int,
        artifact: str | os.PathLike[str],
    ) -> OneBotFileUploadReceipt:
        """向私聊会话上传一个获准的本地产物。"""

        normalized = self._require_allowed_target(
            target_id,
            self._config.private_target_ids,
        )
        resolved = self._resolve_artifact(artifact, image_only=False)
        return await self._upload_file("upload_private_file", "user_id", normalized, resolved)

    async def upload_group_file(
        self,
        target_id: str | int,
        artifact: str | os.PathLike[str],
    ) -> OneBotFileUploadReceipt:
        """向允许列表中的群聊上传一个获准的本地产物。"""

        normalized = self._require_allowed_target(
            target_id,
            self._config.group_target_ids,
        )
        resolved = self._resolve_artifact(artifact, image_only=False)
        return await self._upload_file("upload_group_file", "group_id", normalized, resolved)

    async def get_status(self) -> Mapping[str, Any]:
        """返回 NapCat 的只读 OneBot 运行状态。"""

        return await self._post("get_status", {})

    async def get_version_info(self) -> Mapping[str, Any]:
        """返回 NapCat 的只读 OneBot 实现元数据。"""

        return await self._post("get_version_info", {})

    async def _send_message(
        self,
        action: str,
        id_field: str,
        target_id: str,
        text: str,
    ) -> DeliveryReceipt:
        if not text:
            raise OneBotError("empty_message", retryable=False)
        if len(text) > self._config.max_message_chars:
            raise OneBotError("message_too_long", retryable=False)

        # 使用 OneBot 文本段而非 CQ 码字符串，因此告警文本绝不可能向 NapCat
        # 偷渡图像、文件或提及操作。
        data = await self._post(
            action,
            {
                id_field: int(target_id),
                "message": [{"type": "text", "data": {"text": text}}],
            },
        )
        message_id = data.get("message_id")
        return DeliveryReceipt(
            channel=self.channel,
            provider_message_id=None if message_id is None else str(message_id),
        )

    async def _send_image(
        self,
        action: str,
        id_field: str,
        target_id: str,
        artifact: Path,
    ) -> DeliveryReceipt:
        # 这里刻意使用数组形式的 OneBot 段。公共 API 绝不接受 CQ 码、URL、
        # 文件 URI 或任意段数据。
        data = await self._post(
            action,
            {
                id_field: int(target_id),
                "message": [
                    {
                        "type": "image",
                        "data": {"file": os.fspath(artifact)},
                    }
                ],
            },
        )
        message_id = data.get("message_id")
        return DeliveryReceipt(
            channel=self.channel,
            provider_message_id=None if message_id is None else str(message_id),
        )

    async def _upload_file(
        self,
        action: str,
        id_field: str,
        target_id: str,
        artifact: Path,
    ) -> OneBotFileUploadReceipt:
        # NapCat v4.18.18 的私聊和群聊文件上传操作都需要目标编号、本地文件路径
        # 以及显示名称。
        data = await self._post(
            action,
            {
                id_field: target_id,
                "file": os.fspath(artifact),
                "name": artifact.name,
            },
        )
        file_id = data.get("file_id")
        return OneBotFileUploadReceipt(
            channel=self.channel,
            provider_file_id=None if file_id is None else str(file_id),
        )

    def _require_allowed_target(
        self,
        target_id: str | int,
        allowed: frozenset[str | int],
    ) -> str:
        normalized = _normalize_qq_id(target_id)
        if normalized not in allowed:
            raise OneBotTargetNotAllowedError()
        return normalized

    def _resolve_artifact(
        self,
        artifact: str | os.PathLike[str],
        *,
        image_only: bool,
    ) -> Path:
        root = self._config.artifact_root
        if root is None:
            raise OneBotError("artifact_root_not_configured", retryable=False)
        resolved = _resolve_local_artifact(root, artifact)
        maximum = (
            self._config.max_image_bytes if image_only else self._config.max_file_bytes
        )
        _validate_artifact_file(resolved, maximum_bytes=maximum)
        allowed_types = _IMAGE_MEDIA_TYPES if image_only else _FILE_MEDIA_TYPES
        _validate_artifact_media_type(resolved, allowed_types)
        return resolved

    async def _post(self, action: str, payload: Mapping[str, object]) -> Mapping[str, Any]:
        if self._closed:
            raise OneBotError("client_closed", retryable=False)
        url = f"{self._config.base_url.rstrip('/')}/{action}"
        try:
            response = await self._client.post(
                url,
                headers={"Authorization": f"Bearer {self._config.access_token}"},
                json=payload,
                timeout=self._config.timeout_seconds,
                follow_redirects=False,
            )
        except httpx.TimeoutException:
            raise OneBotError("transport_timeout", retryable=True) from None
        except (httpx.HTTPError, OSError):
            # 异常上下文中绝不保留已认证请求或 URL。
            raise OneBotError("transport_error", retryable=True) from None

        if response.status_code >= 400:
            raise _http_error(response.status_code)
        try:
            document = response.json()
        except ValueError:
            raise OneBotError("malformed_response", retryable=False) from None
        if not isinstance(document, dict):
            raise OneBotError("malformed_response", retryable=False)
        if document.get("status") != "ok" or document.get("retcode") != 0:
            raise OneBotError("remote_rejected", retryable=False)
        data = document.get("data")
        if data is None:
            return {}
        if not isinstance(data, dict):
            raise OneBotError("malformed_response", retryable=False)
        return data

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> OneBotNotifier:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()


def _resolve_artifact_root(value: Path) -> Path:
    text = os.fspath(value)
    if _looks_like_remote_or_unc(text):
        raise ValueError("artifact_root must be a local filesystem path")
    path = Path(text)
    if not path.is_absolute():
        raise ValueError("artifact_root must be absolute")
    try:
        if path.is_symlink():
            raise ValueError("artifact_root must not be a symbolic link")
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError):
        raise ValueError("artifact_root must be an existing directory") from None
    if _looks_like_remote_or_unc(os.fspath(resolved)):
        raise ValueError("artifact_root must be a local filesystem path")
    if not resolved.is_dir():
        raise ValueError("artifact_root must be an existing directory")
    return resolved


def _resolve_local_artifact(root: Path, value: str | os.PathLike[str]) -> Path:
    text = os.fspath(value)
    if not text or "\x00" in text or len(text) > 4096:
        raise OneBotError("artifact_path_not_allowed", retryable=False)
    if _looks_like_remote_or_unc(text):
        raise OneBotError("artifact_path_not_allowed", retryable=False)

    supplied = Path(text)
    candidate = supplied if supplied.is_absolute() else root / supplied
    # 在不跟随链接的情况下折叠点路径段，并在接触工作区外文件系统对象前拒绝
    # 任何越界路径。
    lexical = Path(os.path.abspath(candidate))
    try:
        relative = lexical.relative_to(root)
    except ValueError:
        raise OneBotError("artifact_path_not_allowed", retryable=False) from None

    current = root
    try:
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise OneBotError("artifact_path_not_allowed", retryable=False)
        resolved = lexical.resolve(strict=True)
    except OneBotError:
        raise
    except (OSError, RuntimeError):
        raise OneBotError("artifact_not_found", retryable=False) from None
    try:
        resolved.relative_to(root)
    except ValueError:
        raise OneBotError("artifact_path_not_allowed", retryable=False) from None
    if _looks_like_remote_or_unc(os.fspath(resolved)):
        raise OneBotError("artifact_path_not_allowed", retryable=False)
    return resolved


def _validate_artifact_file(path: Path, *, maximum_bytes: int) -> None:
    try:
        metadata = os.stat(path, follow_symlinks=False)
    except OSError:
        raise OneBotError("artifact_not_found", retryable=False) from None
    if not stat.S_ISREG(metadata.st_mode):
        raise OneBotError("artifact_not_regular_file", retryable=False)
    if metadata.st_size <= 0:
        raise OneBotError("artifact_empty", retryable=False)
    if metadata.st_size > maximum_bytes:
        raise OneBotError("artifact_too_large", retryable=False)


def _validate_artifact_media_type(
    path: Path,
    allowed_types: Mapping[str, str],
) -> None:
    expected = allowed_types.get(path.suffix.casefold())
    if expected is None:
        raise OneBotError("artifact_type_not_allowed", retryable=False)
    try:
        payload = path.read_bytes()
    except OSError:
        raise OneBotError("artifact_unreadable", retryable=False) from None

    if expected.startswith("text/"):
        try:
            payload.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise OneBotError("artifact_mime_mismatch", retryable=False) from None
        if b"\x00" in payload:
            raise OneBotError("artifact_mime_mismatch", retryable=False)
        return
    if _sniff_binary_media_type(payload) != expected:
        raise OneBotError("artifact_mime_mismatch", retryable=False)


def _sniff_binary_media_type(payload: bytes) -> str | None:
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if payload.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if payload.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(payload) >= 12 and payload.startswith(b"RIFF") and payload[8:12] == b"WEBP":
        return "image/webp"
    if payload.startswith(b"%PDF-"):
        return "application/pdf"
    return None


def _looks_like_remote_or_unc(value: str) -> bool:
    normalized = value.strip()
    return (
        normalized.startswith(("\\\\", "//"))
        or _URI_SCHEME.match(normalized) is not None
        or normalized.casefold().startswith(("data:", "base64:"))
    )


def _normalize_qq_id(value: str | int) -> str:
    text = str(value).strip()
    if not text.isascii() or not text.isdecimal() or int(text) <= 0:
        raise ValueError("OneBot target identifiers must be positive decimal integers")
    return str(int(text))


def _validate_loopback_url(value: str) -> None:
    try:
        parsed = urlsplit(value)
        # 强制验证格式错误或超出范围的端口。
        _ = parsed.port
    except ValueError:
        raise ValueError("OneBot base_url is invalid") from None
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("OneBot base_url must use http or https")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("OneBot base_url must not contain credentials")
    if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
        raise ValueError("OneBot base_url must not contain a path, query, or fragment")
    host = (parsed.hostname or "").casefold().rstrip(".")
    if host == "localhost":
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        raise ValueError("OneBot base_url must use a loopback host") from None
    if not address.is_loopback:
        raise ValueError("OneBot base_url must use a loopback host")


def _http_error(status_code: int) -> OneBotError:
    if status_code in {401, 403}:
        return OneBotError("authentication_rejected", retryable=False)
    if status_code == 404:
        return OneBotError("endpoint_not_found", retryable=False)
    if status_code in {408, 425, 429}:
        return OneBotError("temporarily_unavailable", retryable=True)
    if status_code >= 500:
        return OneBotError("server_error", retryable=True)
    return OneBotError("request_rejected", retryable=False)
