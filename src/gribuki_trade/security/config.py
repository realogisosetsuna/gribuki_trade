"""Representation-safe values and credential configuration."""

from __future__ import annotations

from dataclasses import dataclass, field

REDACTED = "<redacted>"


class SecretValue:
    """A string whose normal string representations never reveal its value.

    Callers must use :meth:`reveal` at the narrow integration boundary that
    actually needs the credential.  This is protection against accidental
    logging, not encryption of process memory.
    """

    __slots__ = ("__value",)

    def __init__(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("secret value must be a string")
        self.__value = value

    def reveal(self) -> str:
        """Return the wrapped value for an authenticated client call."""

        return self.__value

    def __repr__(self) -> str:
        return f"{type(self).__name__}({REDACTED})"

    def __str__(self) -> str:
        return REDACTED

    def __bool__(self) -> bool:
        return bool(self.__value)


def _as_secret_value(value: SecretValue | str | None) -> SecretValue | None:
    if value is None or isinstance(value, SecretValue):
        return value
    if not isinstance(value, str):
        raise TypeError("credential fields must be strings or SecretValue instances")
    return SecretValue(value)


@dataclass(frozen=True, slots=True, repr=False)
class CredentialConfig:
    """Resolved credentials with a deliberately non-sensitive ``repr``.

    Prefer keeping only secret names in long-lived application configuration
    and resolving them through a :class:`~gribuki_trade.security.SecretProvider`.
    This small object is for the short-lived boundary where a client library
    requires several resolved values together.
    """

    username: str | None = field(default=None, repr=False)
    password: SecretValue | str | None = field(default=None, repr=False)
    api_key: SecretValue | str | None = field(default=None, repr=False)
    api_secret: SecretValue | str | None = field(default=None, repr=False)
    passphrase: SecretValue | str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.username is not None and not isinstance(self.username, str):
            raise TypeError("username must be a string or None")
        for name in ("password", "api_key", "api_secret", "passphrase"):
            object.__setattr__(self, name, _as_secret_value(getattr(self, name)))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({REDACTED})"


# A concise alias for callers whose credentials are not broker-specific.
SecretConfig = CredentialConfig
