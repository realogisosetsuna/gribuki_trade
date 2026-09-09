"""具有安全失败行为的可插拔秘密提供器。"""

from __future__ import annotations

import base64
import ctypes
import json
import os
from collections.abc import Mapping
from contextlib import suppress
from importlib import import_module
from pathlib import Path
from threading import RLock
from typing import Protocol, cast, runtime_checkable


class SecretProviderError(RuntimeError):
    """秘密操作失败，且不暴露后端错误细节。"""


class SecretProviderUnavailable(SecretProviderError):
    """当前机器上无法使用已配置的秘密后端。"""


@runtime_checkable
class SecretProvider(Protocol):
    """供可选认证集成使用的具名秘密存储。"""

    def get_secret(self, name: str) -> str | None:
        """返回秘密；名称不存在时返回 ``None``。"""

        ...

    def set_secret(self, name: str, value: str) -> None:
        """创建或替换具名秘密。"""

        ...

    def delete_secret(self, name: str) -> bool:
        """删除秘密，并报告其此前是否存在。"""

        ...


def _validate_name(name: str) -> str:
    if not isinstance(name, str):
        raise TypeError("secret name must be a string")
    if not name.strip():
        raise ValueError("secret name must not be empty")
    return name


def _validate_value(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("secret value must be a string")
    if not value:
        raise ValueError("secret value must not be empty")
    return value


class MemorySecretProvider:
    """仅供测试与离线演示使用的进程内提供器。"""

    def __init__(self, initial: Mapping[str, str] | None = None) -> None:
        self._lock = RLock()
        self._secrets: dict[str, str] = {}
        for name, value in (initial or {}).items():
            self.set_secret(name, value)

    def get_secret(self, name: str) -> str | None:
        checked_name = _validate_name(name)
        with self._lock:
            return self._secrets.get(checked_name)

    def set_secret(self, name: str, value: str) -> None:
        checked_name = _validate_name(name)
        checked_value = _validate_value(value)
        with self._lock:
            self._secrets[checked_name] = checked_value

    def delete_secret(self, name: str) -> bool:
        checked_name = _validate_name(name)
        with self._lock:
            return self._secrets.pop(checked_name, None) is not None

    def __repr__(self) -> str:
        with self._lock:
            count = len(self._secrets)
        return f"{type(self).__name__}(secret_count={count})"


class _KeyringBackend(Protocol):
    def get_password(self, service_name: str, username: str) -> str | None: ...

    def set_password(self, service_name: str, username: str, password: str) -> None: ...

    def delete_password(self, service_name: str, username: str) -> None: ...


class KeyringSecretProvider:
    """在操作系统 keyring 中保存具名值。

    ``keyring`` 仍是可选依赖，只在首次操作时导入，而不是在导入本模块或提供器时导入。
    后端错误会被有意替换为通用异常，因为某些第三方后端会在错误文本中包含调用参数。
    """

    def __init__(self, service_name: str = "gribuki-trade") -> None:
        if not isinstance(service_name, str):
            raise TypeError("service name must be a string")
        if not service_name.strip():
            raise ValueError("service name must not be empty")
        self._service_name = service_name
        self._lock = RLock()
        self._loaded_backend: _KeyringBackend | None = None

    @property
    def service_name(self) -> str:
        return self._service_name

    def get_secret(self, name: str) -> str | None:
        checked_name = _validate_name(name)
        try:
            backend = self._backend()
        except SecretProviderUnavailable:
            value = _file_secret_store(self._service_name).get(checked_name)
            if value is not None:
                return value
            raise
        try:
            value = backend.get_password(self._service_name, checked_name)
        except Exception:
            # 离开异常处理器后再抛出，避免把可能含凭据值的后端异常保留为上下文。
            pass
        else:
            if value is None or isinstance(value, str):
                if value is not None:
                    # 在已有 keyring 凭证被读取时补写 DPAPI 副本，避免升级后还需重新录入。
                    with suppress(OSError, SecretProviderError):
                        _file_secret_store(self._service_name).set(checked_name, value)
                    return value
                # keyring 可能在用户配置迁移后被重置或替换；DPAPI 回退可让同一
                # Windows 用户继续使用凭证。
                return _file_secret_store(self._service_name).get(checked_name)
        raise SecretProviderError("system keyring could not retrieve the requested secret")

    def set_secret(self, name: str, value: str) -> None:
        checked_name = _validate_name(name)
        checked_value = _validate_value(value)
        try:
            backend = self._backend()
        except SecretProviderUnavailable:
            try:
                _file_secret_store(self._service_name).set(checked_name, checked_value)
            except OSError:
                raise SecretProviderUnavailable(
                    "the system keyring and encrypted local secret store are unavailable"
                ) from None
            return
        try:
            backend.set_password(self._service_name, checked_name, checked_value)
        except Exception:
            pass
        else:
            # 同时保存用户绑定的加密副本，避免 keyring 后端变化或重启后再次录入凭证。
            with suppress(OSError):
                _file_secret_store(self._service_name).set(checked_name, checked_value)
            return
        raise SecretProviderError("system keyring could not store the requested secret")

    def delete_secret(self, name: str) -> bool:
        checked_name = _validate_name(name)
        try:
            backend = self._backend()
        except SecretProviderUnavailable:
            return _file_secret_store(self._service_name).delete(checked_name)
        try:
            backend.delete_password(self._service_name, checked_name)
        except Exception as error:
            if _is_missing_secret_error(backend, error):
                return False
        else:
            return _file_secret_store(self._service_name).delete(checked_name) or True
        # 不串联或保留可能包含后端数据的异常。
        raise SecretProviderError("system keyring could not delete the requested secret")

    def _backend(self) -> _KeyringBackend:
        with self._lock:
            if self._loaded_backend is not None:
                return self._loaded_backend
            try:
                backend = import_module("keyring")
            except Exception:
                pass
            else:
                self._loaded_backend = cast(_KeyringBackend, backend)
                return self._loaded_backend
        raise SecretProviderUnavailable(
            "the optional keyring package and a system keyring backend are required"
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(service_name={self._service_name!r})"


def _is_missing_secret_error(backend: _KeyringBackend, error: Exception) -> bool:
    errors = getattr(backend, "errors", None)
    missing_type = getattr(errors, "PasswordDeleteError", None)
    return isinstance(missing_type, type) and isinstance(error, missing_type)


class _EncryptedFileSecretStore:
    """基于 Windows DPAPI 的 keyring 回退，保证配置迁移和重启后的凭证可用。"""

    def __init__(self, service_name: str, path: Path) -> None:
        self._service_name = service_name
        self._path = path

    def _load(self) -> dict[str, str]:
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, TypeError):
            return {}
        return raw if isinstance(raw, dict) else {}

    def _save(self, values: dict[str, str]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(self._path.suffix + ".tmp")
        temporary.write_text(json.dumps(values, sort_keys=True), encoding="utf-8")
        with suppress(OSError):
            os.chmod(temporary, 0o600)
        os.replace(temporary, self._path)

    def get(self, name: str) -> str | None:
        encoded = self._load().get(name)
        if not isinstance(encoded, str):
            return None
        try:
            return _dpapi_unprotect(base64.b64decode(encoded), self._service_name, name)
        except (ValueError, OSError):
            return None

    def set(self, name: str, value: str) -> None:
        protected = _dpapi_protect(value, self._service_name, name)
        values = self._load()
        values[name] = base64.b64encode(protected).decode("ascii")
        self._save(values)

    def delete(self, name: str) -> bool:
        values = self._load()
        existed = name in values
        values.pop(name, None)
        if existed:
            self._save(values)
        return existed


def _file_secret_store(service_name: str) -> _EncryptedFileSecretStore:
    explicit = os.environ.get("GRIBUKI_TRADE_SECRET_FILE")
    if explicit:
        path = Path(explicit)
    elif os.name == "nt":
        local_app_data = os.environ.get("LOCALAPPDATA")
        path = (
            Path(local_app_data or Path.home() / "AppData" / "Local")
            / "gribuki-trade"
            / "secrets.json"
        )
    else:
        path = Path.home() / ".local" / "share" / "gribuki-trade" / "secrets.json"
    return _EncryptedFileSecretStore(service_name, path)


def _dpapi_entropy(service_name: str, name: str) -> bytes:
    return f"gribuki-trade:{service_name}:{name}".encode()


def _dpapi_protect(value: str, service_name: str, name: str) -> bytes:
    if os.name != "nt":
        raise OSError("Windows DPAPI is unavailable")
    return _dpapi_call(
        "CryptProtectData", value.encode("utf-8"), _dpapi_entropy(service_name, name)
    )


def _dpapi_unprotect(value: bytes, service_name: str, name: str) -> str:
    if os.name != "nt":
        raise OSError("Windows DPAPI is unavailable")
    return _dpapi_call(
        "CryptUnprotectData", value, _dpapi_entropy(service_name, name)
    ).decode("utf-8")


def _dpapi_call(function: str, payload: bytes, entropy: bytes) -> bytes:
    class _Blob(ctypes.Structure):
        _fields_ = [("cbData", ctypes.c_uint32), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]

    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    source = ctypes.create_string_buffer(payload)
    entropy_buffer = ctypes.create_string_buffer(entropy)
    input_blob = _Blob(len(payload), ctypes.cast(source, ctypes.POINTER(ctypes.c_ubyte)))
    entropy_blob = _Blob(len(entropy), ctypes.cast(entropy_buffer, ctypes.POINTER(ctypes.c_ubyte)))
    output_blob = _Blob()
    operation = getattr(crypt32, function)
    operation.argtypes = [
        ctypes.POINTER(_Blob),
        ctypes.c_void_p,
        ctypes.POINTER(_Blob),
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(_Blob),
    ]
    operation.restype = ctypes.c_int
    if not operation(
        ctypes.byref(input_blob),
        None,
        ctypes.byref(entropy_blob),
        None,
        None,
        0,
        ctypes.byref(output_blob),
    ):
        raise OSError(ctypes.get_last_error())
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        kernel32.LocalFree(output_blob.pbData)
