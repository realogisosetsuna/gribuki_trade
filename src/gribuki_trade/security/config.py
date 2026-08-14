"""不会在常规表示中泄密的值与凭据配置。"""

from __future__ import annotations

from dataclasses import dataclass, field

REDACTED = "<redacted>"


class SecretValue:
    """常规字符串表示永远不会泄露其内容的字符串。

    调用方必须只在确实需要凭据的狭窄集成边界使用 :meth:`reveal`。这一设计用于防止
    意外日志泄露，并不代表对进程内存进行了加密。
    """

    __slots__ = ("__value",)

    def __init__(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("secret value must be a string")
        self.__value = value

    def reveal(self) -> str:
        """返回包装值，供已认证客户端调用使用。"""

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
    """已解析的凭据，其 ``repr`` 被有意设计为不含敏感信息。

    长期应用配置应只保存秘密名称，并通过
    :class:`~gribuki_trade.security.SecretProvider` 解析。本小型对象仅用于客户端库需要
    同时取得多个已解析值的短生命周期边界。
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


# 供凭据不限定于券商场景的调用方使用的简洁别名。
SecretConfig = CredentialConfig
