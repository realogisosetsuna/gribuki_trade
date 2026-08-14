"""用于本地凭据管理的无回显辅助组件。"""

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
    """在终端无回显地读取并保存值，全程不打印其内容。"""

    read_hidden = getpass_fn or getpass.getpass
    value = read_hidden(prompt or f"Enter secret for {name}: ")
    provider.set_secret(name, value)


class InteractiveSecretManager:
    """绝不写出秘密值的小型本地管理外观。

    ``get_secret`` 会把值返回给可信应用代码；本类自身不执行打印或日志记录。保存操作
    始终使用无回显的 ``getpass`` 读取器，删除与查询只通过返回值报告结果。
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
