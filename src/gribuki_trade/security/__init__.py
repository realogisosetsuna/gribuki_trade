"""Secret handling primitives for local broker integrations.

The package intentionally contains no credentials and does not read secrets
from environment variables.  Production credentials are referenced by name
and resolved from the operating-system keyring only when they are needed.
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
