"""本地券商集成所用的秘密处理基础组件。

本包有意不包含任何凭据，也不从环境变量读取秘密。生产凭据只按名称引用，并且仅在
确有需要时才从操作系统 keyring 解析。
"""

from gribuki_trade.security.config import CredentialConfig, SecretConfig, SecretValue
from gribuki_trade.security.interactive import InteractiveSecretManager, prompt_and_set_secret
from gribuki_trade.security.secrets import (
    KeyringSecretProvider,
    MemorySecretProvider,
    SecretProvider,
    SecretProviderError,
    SecretProviderUnavailable,
)

__all__ = [
    "CredentialConfig",
    "InteractiveSecretManager",
    "KeyringSecretProvider",
    "MemorySecretProvider",
    "SecretConfig",
    "SecretProvider",
    "SecretProviderError",
    "SecretProviderUnavailable",
    "SecretValue",
    "prompt_and_set_secret",
]
