"""具有安全失败行为的可插拔秘密提供器。"""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
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
        backend = self._backend()
        try:
            value = backend.get_password(self._service_name, checked_name)
        except Exception:
            # 离开异常处理器后再抛出，避免把可能含凭据值的后端异常保留为上下文。
            pass
        else:
            if value is None or isinstance(value, str):
                return value
        raise SecretProviderError("system keyring could not retrieve the requested secret")

    def set_secret(self, name: str, value: str) -> None:
        checked_name = _validate_name(name)
        checked_value = _validate_value(value)
        backend = self._backend()
        try:
            backend.set_password(self._service_name, checked_name, checked_value)
        except Exception:
            pass
        else:
            return
        raise SecretProviderError("system keyring could not store the requested secret")

    def delete_secret(self, name: str) -> bool:
        checked_name = _validate_name(name)
        backend = self._backend()
        try:
            backend.delete_password(self._service_name, checked_name)
        except Exception as error:
            if _is_missing_secret_error(backend, error):
                return False
        else:
            return True
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
