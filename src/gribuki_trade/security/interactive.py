"""No-echo helpers for local credential management."""

from __future__ import annotations

import getpass
from collections.abc import Callable

from gribuki_trade.security.secrets import SecretProvider

GetpassFunction = Callable[[str], str]


def prompt_and_set_secret(
    provider: SecretProvider,
    name: str,
    *,
    prompt: str | None = None,
    getpass_fn: GetpassFunction | None = None,
) -> None:
    """Read a value without terminal echo and store it without printing it."""

    read_hidden = getpass_fn or getpass.getpass
    value = read_hidden(prompt or f"Enter secret for {name}: ")
    provider.set_secret(name, value)


class InteractiveSecretManager:
    """Small local management facade that never writes secret values.

    ``get_secret`` returns the value to trusted application code; this class
    itself performs no printing or logging.  Saving always uses a no-echo
    ``getpass`` reader.  Deletion and lookup report through return values only.
    """

    __slots__ = ("_getpass_fn", "_provider")

    def __init__(
        self,
        provider: SecretProvider,
        *,
        getpass_fn: GetpassFunction | None = None,
    ) -> None:
        self._provider = provider
        self._getpass_fn = getpass_fn

    def set_secret(self, name: str, *, prompt: str | None = None) -> None:
        prompt_and_set_secret(
            self._provider,
            name,
            prompt=prompt,
            getpass_fn=self._getpass_fn,
        )

    def get_secret(self, name: str) -> str | None:
        return self._provider.get_secret(name)

    def delete_secret(self, name: str) -> bool:
        return self._provider.delete_secret(name)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(provider={type(self._provider).__name__})"
