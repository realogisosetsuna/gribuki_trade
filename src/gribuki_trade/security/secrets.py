"""Pluggable secret providers with safe failure behaviour."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from threading import RLock
from typing import Protocol, cast, runtime_checkable


class SecretProviderError(RuntimeError):
    """A secret operation failed without exposing backend error details."""


class SecretProviderUnavailable(SecretProviderError):
    """The configured secret backend is not available on this machine."""


@runtime_checkable
class SecretProvider(Protocol):
    """Named secret storage used by optional authenticated integrations."""

    def get_secret(self, name: str) -> str | None:
        """Return a secret, or ``None`` when the name does not exist."""

        ...

    def set_secret(self, name: str, value: str) -> None:
        """Create or replace a named secret."""

        ...

    def delete_secret(self, name: str) -> bool:
        """Delete a secret and report whether it existed."""

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
    """Process-local provider intended only for tests and offline demos."""

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
    """Store named values in the operating-system keyring.

    ``keyring`` remains an optional dependency and is imported on the first
    operation, not when this module or provider is imported.  Backend errors
    are replaced with deliberately generic exceptions because some third-party
    backends include call arguments in their error text.
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
            # Raise after leaving the handler so the backend exception is not
            # retained as context; it may contain a credential value.
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
        # Do not chain or retain an exception that may contain backend data.
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
