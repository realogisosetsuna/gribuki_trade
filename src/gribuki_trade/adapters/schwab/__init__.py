"""Charles Schwab OAuth, Market Data, and Trader API adapter skeleton."""

from .broker import (
    SCHWAB_ORDER_STATUS_EVENT,
    SchwabBroker,
    SchwabOrderSpec,
    SchwabOrderUpdate,
    build_limit_order_payload,
)
from .client import (
    AccountNumberHash,
    PlacedOrder,
    SchwabApiClient,
    SchwabEndpoints,
    SchwabEnvironment,
)
from .credentials import (
    SCHWAB_CLIENT_ID_SECRET,
    SCHWAB_CLIENT_SECRET_SECRET,
    SchwabAppCredentials,
    load_schwab_app_credentials,
)
from .errors import (
    SchwabAuthenticationError,
    SchwabError,
    SchwabHttpError,
    SchwabLiveAccessError,
    SchwabOAuthError,
    SchwabOrderValidationError,
    SchwabPermissionError,
    SchwabProtocolError,
    SchwabRateLimitError,
    SchwabServerError,
    SchwabTransportError,
)
from .oauth import (
    AuthorizationRequest,
    InMemoryTokenStore,
    OAuthToken,
    SchwabOAuthClient,
    TokenStore,
)
from .token_store import SCHWAB_OAUTH_TOKEN_SECRET, KeyringOAuthTokenStore
from .transport import (
    AsyncHttpTransport,
    HttpRequest,
    HttpResponse,
    HttpxAsyncHttpTransport,
)

__all__ = [
    "SCHWAB_ORDER_STATUS_EVENT",
    "SCHWAB_CLIENT_ID_SECRET",
    "SCHWAB_CLIENT_SECRET_SECRET",
    "SCHWAB_OAUTH_TOKEN_SECRET",
    "AccountNumberHash",
    "AsyncHttpTransport",
    "AuthorizationRequest",
    "HttpRequest",
    "HttpResponse",
    "HttpxAsyncHttpTransport",
    "InMemoryTokenStore",
    "KeyringOAuthTokenStore",
    "OAuthToken",
    "PlacedOrder",
    "SchwabApiClient",
    "SchwabAppCredentials",
    "SchwabAuthenticationError",
    "SchwabBroker",
    "SchwabEndpoints",
    "SchwabEnvironment",
    "SchwabError",
    "SchwabHttpError",
    "SchwabLiveAccessError",
    "SchwabOAuthClient",
    "SchwabOAuthError",
    "SchwabOrderSpec",
    "SchwabOrderUpdate",
    "SchwabOrderValidationError",
    "SchwabPermissionError",
    "SchwabProtocolError",
    "SchwabRateLimitError",
    "SchwabServerError",
    "SchwabTransportError",
    "TokenStore",
    "build_limit_order_payload",
    "load_schwab_app_credentials",
]
