"""GUI 与生产命令共享的本机集成配置。

本文件只保存 LLM provider、模型 ID、本机 loopback 地址和 NapCat 目录等非秘密值。
API Key、OneBot access token 等秘密始终保存在 ``SecretProvider``，绝不写入此 JSON。
"""

from __future__ import annotations

import importlib
import ipaddress
import json
import os
import re
import tempfile
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import BinaryIO, cast
from urllib.parse import urlsplit

from gribuki_trade.adapters.llm.deepseek_chat import DEFAULT_DEEPSEEK_MODEL

INTEGRATION_SETTINGS_ENV = "GRIBUKI_TRADE_INTEGRATION_CONFIG"
DEFAULT_INTEGRATION_SETTINGS_PATH = Path("runtime/config/integrations.json")
DEFAULT_NAPCAT_RUNTIME = "vendor/NapCatQQ-shell-v4.18.18"
DEFAULT_ONEBOT_URL = "http://127.0.0.1:3000"
DEFAULT_NAPCAT_WEBUI_URL = "http://127.0.0.1:6099"
_SCHEMA_V1 = "gribuki-integration-settings@1"
_SCHEMA_V2 = "gribuki-integration-settings@2"
_SCHEMA = "gribuki-integration-settings@3"
_MODEL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_FIELDS_V1 = frozenset(
    {
        "schema",
        "deepseek_model",
        "napcat_runtime",
        "napcat_webui_url",
        "onebot_url",
    }
)
_FIELDS_V2 = frozenset(
    {
        "schema",
        "llm_provider",
        "deepseek_model",
        "openai_model",
        "napcat_runtime",
        "napcat_webui_url",
        "onebot_url",
    }
)
_FIELDS = _FIELDS_V2 | {"revision"}


class IntegrationSettingsError(RuntimeError):
    """共享配置缺失以外的读取、格式或写入错误。"""


@dataclass(frozen=True, slots=True)
class IntegrationRuntimeSettings:
    """可由 GUI 修改、可被生产命令读取的非秘密配置快照。"""

    llm_provider: str = "deepseek"
    deepseek_model: str = DEFAULT_DEEPSEEK_MODEL
    openai_model: str = "gpt-5.6"
    napcat_runtime: str = DEFAULT_NAPCAT_RUNTIME
    napcat_webui_url: str = DEFAULT_NAPCAT_WEBUI_URL
    onebot_url: str = DEFAULT_ONEBOT_URL
    revision: int = 0
    schema: str = _SCHEMA

    def __post_init__(self) -> None:
        if self.schema != _SCHEMA:
            raise ValueError("integration settings schema is unsupported")
        if (
            isinstance(self.revision, bool)
            or not isinstance(self.revision, int)
            or self.revision < 0
        ):
            raise ValueError("integration settings revision is invalid")
        provider = self.llm_provider.strip().casefold()
        if provider not in {"deepseek", "openai"}:
            raise ValueError("LLM provider is unsupported")
        object.__setattr__(self, "llm_provider", provider)
        object.__setattr__(self, "deepseek_model", validate_model_id(self.deepseek_model))
        object.__setattr__(self, "openai_model", validate_model_id(self.openai_model))
        runtime = self.napcat_runtime.strip()
        if not runtime or "\x00" in runtime:
            raise ValueError("NapCat runtime path is invalid")
        object.__setattr__(self, "napcat_runtime", runtime)
        object.__setattr__(
            self,
            "napcat_webui_url",
            validate_loopback_origin(self.napcat_webui_url, label="NapCat WebUI URL"),
        )
        object.__setattr__(
            self,
            "onebot_url",
            validate_loopback_origin(self.onebot_url, label="OneBot URL"),
        )

    def document(self) -> dict[str, object]:
        """返回稳定 JSON 文档；该文档不包含任何秘密。"""

        return asdict(self)


def integration_settings_path(explicit: Path | None = None) -> Path:
    """解析共享配置路径，显式参数优先于环境变量和仓库默认值。"""

    if explicit is not None:
        return explicit.resolve()
    configured = os.environ.get(INTEGRATION_SETTINGS_ENV)
    if configured is not None:
        if not configured.strip() or "\x00" in configured:
            raise IntegrationSettingsError(f"{INTEGRATION_SETTINGS_ENV} contains an invalid path")
        return Path(configured).resolve()
    return DEFAULT_INTEGRATION_SETTINGS_PATH.resolve()


class IntegrationSettingsStore:
    """对共享非秘密 JSON 做严格读取、版本校验和跨进程原子替换。"""

    def __init__(self, path: Path | None = None) -> None:
        self.path = integration_settings_path(path)

    def load(self) -> IntegrationRuntimeSettings:
        """文件不存在时返回安全默认值；损坏或链接文件一律拒绝。"""

        return self._load_unlocked()

    def _load_unlocked(self) -> IntegrationRuntimeSettings:
        """读取一个由原子替换保证完整的快照；写调用方必须已持有锁。"""

        if not self.path.exists():
            return IntegrationRuntimeSettings()
        if self.path.is_symlink() or not self.path.is_file():
            raise IntegrationSettingsError(
                "integration settings must be a regular non-symlink file"
            )
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise IntegrationSettingsError("integration settings are unreadable") from error
        if not isinstance(raw, dict):
            raise IntegrationSettingsError("integration settings fields are invalid")
        if any(not isinstance(key, str) for key in raw):
            raise IntegrationSettingsError("integration settings fields are invalid")
        checked = cast(dict[str, object], raw)
        schema = checked.get("schema")
        if schema == _SCHEMA_V1 and set(checked) == _FIELDS_V1:
            # 旧版只有 DeepSeek；读取时映射为新结构，下一次 GUI 保存会原子升级。
            try:
                return IntegrationRuntimeSettings(
                    llm_provider="deepseek",
                    deepseek_model=_legacy_text(checked, "deepseek_model"),
                    napcat_runtime=_legacy_text(checked, "napcat_runtime"),
                    napcat_webui_url=_legacy_text(checked, "napcat_webui_url"),
                    onebot_url=_legacy_text(checked, "onebot_url"),
                )
            except (TypeError, ValueError) as error:
                raise IntegrationSettingsError("integration settings values are invalid") from error
        if schema == _SCHEMA_V2 and set(checked) == _FIELDS_V2:
            try:
                return IntegrationRuntimeSettings(
                    llm_provider=_legacy_text(checked, "llm_provider"),
                    deepseek_model=_legacy_text(checked, "deepseek_model"),
                    openai_model=_legacy_text(checked, "openai_model"),
                    napcat_runtime=_legacy_text(checked, "napcat_runtime"),
                    napcat_webui_url=_legacy_text(checked, "napcat_webui_url"),
                    onebot_url=_legacy_text(checked, "onebot_url"),
                )
            except (TypeError, ValueError) as error:
                raise IntegrationSettingsError("integration settings values are invalid") from error
        if set(checked) != _FIELDS:
            raise IntegrationSettingsError("integration settings fields are invalid")
        try:
            return IntegrationRuntimeSettings(
                schema=_legacy_text(checked, "schema"),
                revision=_revision(checked.get("revision")),
                llm_provider=_legacy_text(checked, "llm_provider"),
                deepseek_model=_legacy_text(checked, "deepseek_model"),
                openai_model=_legacy_text(checked, "openai_model"),
                napcat_runtime=_legacy_text(checked, "napcat_runtime"),
                napcat_webui_url=_legacy_text(checked, "napcat_webui_url"),
                onebot_url=_legacy_text(checked, "onebot_url"),
            )
        except (TypeError, ValueError) as error:
            raise IntegrationSettingsError("integration settings values are invalid") from error

    def save(self, settings: IntegrationRuntimeSettings) -> IntegrationRuntimeSettings:
        """以乐观版本检查保存完整快照，拒绝覆盖其他进程的新配置。"""

        if not isinstance(settings, IntegrationRuntimeSettings):
            raise TypeError("settings must be an IntegrationRuntimeSettings snapshot")
        with self._mutation_lock():
            current = self._load_unlocked()
            if settings.revision != current.revision:
                raise IntegrationSettingsError(
                    "integration settings revision changed; reload before saving"
                )
            updated = replace(settings, revision=current.revision + 1)
            self._save_unlocked(updated)
            return updated

    def _save_unlocked(self, settings: IntegrationRuntimeSettings) -> None:
        """原子保存已递增版本的快照；调用方必须持有跨进程锁。"""

        parent = self.path.parent
        parent.mkdir(parents=True, exist_ok=True)
        if parent.is_symlink():
            raise IntegrationSettingsError("integration settings parent must not be a symlink")
        payload = (
            json.dumps(settings.document(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        ).encode("utf-8")
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                delete=False,
                dir=parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
            ) as handle:
                temporary = Path(handle.name)
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            temporary = None
        except OSError as error:
            raise IntegrationSettingsError("integration settings could not be saved") from error
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def set(self, name: str, value: str) -> IntegrationRuntimeSettings:
        """基于最新快照更新一个允许字段，并返回已落盘的新快照。"""

        return self.update({name: value})

    def update(self, values: Mapping[str, str]) -> IntegrationRuntimeSettings:
        """在一次锁和一次版本递增内更新多个允许字段。"""

        allowed = {
            "llm_provider",
            "deepseek_model",
            "openai_model",
            "napcat_runtime",
            "napcat_webui_url",
            "onebot_url",
        }
        if not values:
            raise ValueError("integration settings update must not be empty")
        if any(name not in allowed for name in values):
            raise ValueError("unknown integration setting")
        if any(not isinstance(value, str) for value in values.values()):
            raise TypeError("integration setting values must be text")
        with self._mutation_lock():
            current = self._load_unlocked()
            updated = replace(
                current,
                revision=current.revision + 1,
                **dict(values),
            )
            self._save_unlocked(updated)
            return updated

    @contextmanager
    def _mutation_lock(self, *, timeout_seconds: float = 15.0) -> Iterator[None]:
        """串行化不同 GUI/CLI 进程的读改写，避免最后写入者丢失其他字段。"""

        parent = self.path.parent
        parent.mkdir(parents=True, exist_ok=True)
        if parent.is_symlink():
            raise IntegrationSettingsError("integration settings parent must not be a symlink")
        lock_path = self.path.with_name(f".{self.path.name}.lock")
        if lock_path.is_symlink():
            raise IntegrationSettingsError("integration settings lock must not be a symlink")
        deadline = time.monotonic() + timeout_seconds
        try:
            with lock_path.open("a+b") as handle:
                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"0")
                    handle.flush()
                    os.fsync(handle.fileno())
                while True:
                    try:
                        _lock_file_byte(handle)
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise IntegrationSettingsError(
                                "integration settings lock is already held"
                            ) from None
                        time.sleep(0.02)
                    else:
                        break
                try:
                    yield
                finally:
                    _unlock_file_byte(handle)
        except OSError as error:
            raise IntegrationSettingsError("integration settings lock is unavailable") from error


def _legacy_text(document: dict[str, object], name: str) -> str:
    value = document.get(name)
    if not isinstance(value, str):
        raise ValueError(f"integration settings field {name!r} must be text")
    return value


def _revision(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("integration settings revision is invalid")
    return value


def _lock_file_byte(handle: BinaryIO) -> None:
    if os.name == "nt":
        msvcrt = importlib.import_module("msvcrt")
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    fcntl = importlib.import_module("fcntl")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file_byte(handle: BinaryIO) -> None:
    if os.name == "nt":
        msvcrt = importlib.import_module("msvcrt")
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    fcntl = importlib.import_module("fcntl")
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_integration_settings(path: Path | None = None) -> IntegrationRuntimeSettings:
    """便捷读取生产运行时共享配置。"""

    return IntegrationSettingsStore(path).load()


def validate_model_id(value: str) -> str:
    """校验可持久化的 provider 模型 ID。"""

    if not isinstance(value, str):
        raise TypeError("model ID must be text")
    checked = value.strip()
    if _MODEL_ID.fullmatch(checked) is None:
        raise ValueError("model ID is invalid")
    return checked


def validate_loopback_origin(value: str, *, label: str) -> str:
    """校验不带凭据、路径或查询参数的本机 HTTP(S) origin。"""

    if not isinstance(value, str):
        raise TypeError(f"{label} must be text")
    checked = value.strip().rstrip("/")
    try:
        parsed = urlsplit(checked)
        _ = parsed.port
    except ValueError:
        raise ValueError(f"{label} is invalid") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{label} must be a credential-free loopback origin")
    host = parsed.hostname.casefold().rstrip(".")
    if host != "localhost":
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            raise ValueError(f"{label} must use a loopback host") from None
        if not address.is_loopback:
            raise ValueError(f"{label} must use a loopback host")
    return checked
