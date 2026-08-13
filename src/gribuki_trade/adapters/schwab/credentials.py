"""Schwab application credentials resolved from the local secret provider."""

from __future__ import annotations

from dataclasses import dataclass, field

from gribuki_trade.security import SecretProvider

from .errors import SchwabOAuthError

SCHWAB_CLIENT_ID_SECRET = "schwab.client_id"
SCHWAB_CLIENT_SECRET_SECRET = "schwab.client_secret"


@dataclass(frozen=True, slots=True, repr=False)
class SchwabAppCredentials:
    client_id: str = field(repr=False)
    client_secret: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.client_id or not self.client_secret:
            raise ValueError("Schwab app credentials must not be blank")

    def __repr__(self) -> str:
        return "SchwabAppCredentials(client_id=<redacted>, client_secret=<redacted>)"


def load_schwab_app_credentials(provider: SecretProvider) -> SchwabAppCredentials:
    """Load the developer App Key/Secret after the app reaches Ready status."""

    client_id = provider.get_secret(SCHWAB_CLIENT_ID_SECRET)
    client_secret = provider.get_secret(SCHWAB_CLIENT_SECRET_SECRET)
    missing = [
        name
        for name, value in (
            (SCHWAB_CLIENT_ID_SECRET, client_id),
            (SCHWAB_CLIENT_SECRET_SECRET, client_secret),
        )
        if not value
    ]
    if missing:
        raise SchwabOAuthError(
            "missing Schwab developer secret(s): " + ", ".join(missing)
        )
    assert client_id is not None and client_secret is not None
    return SchwabAppCredentials(client_id=client_id, client_secret=client_secret)
