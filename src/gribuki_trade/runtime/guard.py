"""Default-deny controls around operations on a real broker adapter."""

from __future__ import annotations

import getpass
import hmac
from collections.abc import AsyncIterator, Callable, Iterable
from enum import StrEnum
from threading import RLock

from gribuki_trade.domain.orders import OrderIntent
from gribuki_trade.ports.broker import BrokerAdapter, BrokerEvent
from gribuki_trade.runtime.mode import TradingMode

LIVE_CONFIRMATION_PHRASE = "ENABLE LIVE TRADING"


class BrokerOperation(StrEnum):
    """Known operations exposed by a real broker boundary."""

    CONNECT = "connect"
    DISCONNECT = "disconnect"
    QUERY = "query"
    SUBSCRIBE = "subscribe"
    SUBMIT_ORDER = "submit_order"
    CANCEL_ORDER = "cancel_order"
    REPLACE_ORDER = "replace_order"

    @property
    def changes_orders(self) -> bool:
        return self in {
            BrokerOperation.SUBMIT_ORDER,
            BrokerOperation.CANCEL_ORDER,
            BrokerOperation.REPLACE_ORDER,
        }


class TradingModeViolation(PermissionError):
    """A real broker operation was denied by the runtime mode boundary."""


class LiveTradingNotConfirmed(TradingModeViolation):
    """LIVE was selected but has not been explicitly unlocked locally."""


class AccountNotAllowed(TradingModeViolation):
    """The requested live account is not in the immutable allowlist."""


class ExchangeNotAllowed(TradingModeViolation):
    """The requested live exchange is not in the immutable allowlist."""


class OperationNotAllowed(TradingModeViolation):
    """The selected mode cannot perform the requested broker operation."""


def _normalized_entries(values: Iterable[str], *, kind: str, uppercase: bool) -> frozenset[str]:
    normalized: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            raise TypeError(f"{kind} allowlist entries must be strings")
        checked = value.strip()
        if not checked:
            raise ValueError(f"{kind} allowlist entries must not be empty")
        normalized.add(checked.upper() if uppercase else checked)
    return frozenset(normalized)


class LiveTradingGuard:
    """Authorize access to a real broker according to one runtime mode.

    The default instance is PAPER and denies every operation on a real broker.
    SHADOW permits only connection/read/subscription operations.  LIVE requires
    all three independent conditions: an exact local confirmation phrase, an
    allowlisted account, and an allowlisted exchange.  Confirmation lives only
    in this process and cannot be loaded from environment or configuration.
    """

    def __init__(
        self,
        mode: TradingMode | str = TradingMode.PAPER,
        *,
        allowed_accounts: Iterable[str] = (),
        allowed_exchanges: Iterable[str] = (),
        account_whitelist: Iterable[str] | None = None,
        exchange_whitelist: Iterable[str] | None = None,
    ) -> None:
        if account_whitelist is not None:
            if tuple(allowed_accounts):
                raise ValueError("provide allowed_accounts or account_whitelist, not both")
            allowed_accounts = account_whitelist
        if exchange_whitelist is not None:
            if tuple(allowed_exchanges):
                raise ValueError("provide allowed_exchanges or exchange_whitelist, not both")
            allowed_exchanges = exchange_whitelist

        self._mode = TradingMode(mode)
        self._allowed_accounts = _normalized_entries(
            allowed_accounts,
            kind="account",
            uppercase=False,
        )
        self._allowed_exchanges = _normalized_entries(
            allowed_exchanges,
            kind="exchange",
            uppercase=True,
        )
        self._live_confirmed = False
        self._lock = RLock()

    @property
    def mode(self) -> TradingMode:
        return self._mode

    @property
    def allowed_accounts(self) -> frozenset[str]:
        return self._allowed_accounts

    @property
    def allowed_exchanges(self) -> frozenset[str]:
        return self._allowed_exchanges

    # Whitelist spellings remain available for policy/configuration callers.
    @property
    def account_whitelist(self) -> frozenset[str]:
        return self._allowed_accounts

    @property
    def exchange_whitelist(self) -> frozenset[str]:
        return self._allowed_exchanges

    @property
    def live_confirmed(self) -> bool:
        with self._lock:
            return self._live_confirmed

    def confirm_live_trading(self, phrase: str) -> None:
        """Unlock this in-memory LIVE guard after an exact local phrase.

        Use :func:`prompt_for_live_confirmation` at an interactive local
        boundary.  A failed attempt always returns the guard to its locked
        state and the supplied phrase is never included in an exception.
        """

        with self._lock:
            self._live_confirmed = False
            if self._mode is not TradingMode.LIVE:
                raise LiveTradingNotConfirmed("live confirmation is valid only in LIVE mode")
            if not isinstance(phrase, str) or not hmac.compare_digest(
                phrase,
                LIVE_CONFIRMATION_PHRASE,
            ):
                raise LiveTradingNotConfirmed("the local live-trading confirmation failed")
            self._live_confirmed = True

    # Concise alias for integrations that already name the guard in context.
    confirm_live = confirm_live_trading

    def lock(self) -> None:
        """Immediately revoke the process-local LIVE confirmation."""

        with self._lock:
            self._live_confirmed = False

    def assert_broker_operation(
        self,
        exchange: str,
        account_id: str,
        operation: BrokerOperation | str,
    ) -> None:
        """Raise unless the requested real-broker operation is authorized."""

        checked_operation = _coerce_operation(operation)
        checked_exchange = _normalize_request(exchange, kind="exchange", uppercase=True)
        checked_account = _normalize_request(account_id, kind="account", uppercase=False)

        if self._mode is TradingMode.PAPER:
            raise OperationNotAllowed("PAPER mode cannot access a real broker")
        if checked_operation is BrokerOperation.DISCONNECT:
            # An already-real session must always be able to fail closed.
            return
        if self._mode is TradingMode.SHADOW:
            if checked_operation.changes_orders:
                raise OperationNotAllowed("SHADOW mode cannot change real orders")
            return

        with self._lock:
            confirmed = self._live_confirmed
        if not confirmed:
            raise LiveTradingNotConfirmed("LIVE mode is locked pending local confirmation")
        if checked_account not in self._allowed_accounts:
            raise AccountNotAllowed("the live account is not allowlisted")
        if checked_exchange not in self._allowed_exchanges:
            raise ExchangeNotAllowed("the live exchange is not allowlisted")

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(mode={self._mode.value!r}, "
            f"account_count={len(self._allowed_accounts)}, "
            f"exchange_count={len(self._allowed_exchanges)}, "
            f"live_confirmed={self.live_confirmed})"
        )


def _coerce_operation(operation: BrokerOperation | str) -> BrokerOperation:
    try:
        return BrokerOperation(operation)
    except (TypeError, ValueError):
        raise OperationNotAllowed("unknown real-broker operation; access denied") from None


def _normalize_request(value: str, *, kind: str, uppercase: bool) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OperationNotAllowed(f"a non-empty {kind} is required; access denied")
    checked = value.strip()
    return checked.upper() if uppercase else checked


GetpassFunction = Callable[[str], str]


def prompt_for_live_confirmation(
    guard: LiveTradingGuard,
    *,
    getpass_fn: GetpassFunction | None = None,
) -> bool:
    """Collect the LIVE phrase without echo on the machine running the guard."""

    read_hidden = getpass_fn or getpass.getpass
    phrase = read_hidden(f'Type "{LIVE_CONFIRMATION_PHRASE}" to unlock LIVE trading: ')
    try:
        guard.confirm_live_trading(phrase)
    except LiveTradingNotConfirmed:
        return False
    return True


class GuardedBrokerAdapter:
    """Broker facade that makes bypassing the mode guard difficult by default.

    In particular, SHADOW submissions and cancellations are rejected before
    the wrapped real adapter method can be invoked.
    """

    def __init__(
        self,
        broker: BrokerAdapter,
        guard: LiveTradingGuard,
        *,
        exchange: str,
        account_id: str,
    ) -> None:
        self._broker = broker
        self._guard = guard
        self._exchange = _normalize_request(exchange, kind="exchange", uppercase=True)
        self._account_id = _normalize_request(account_id, kind="account", uppercase=False)

    async def connect(self) -> None:
        self._guard.assert_broker_operation(
            self._exchange,
            self._account_id,
            BrokerOperation.CONNECT,
        )
        await self._broker.connect()

    async def disconnect(self) -> None:
        self._guard.assert_broker_operation(
            self._exchange,
            self._account_id,
            BrokerOperation.DISCONNECT,
        )
        await self._broker.disconnect()

    async def submit_order(self, order: OrderIntent) -> None:
        if order.account_id != self._account_id:
            raise AccountNotAllowed("the order account does not match the guarded broker")
        self._guard.assert_broker_operation(
            self._exchange,
            order.account_id,
            BrokerOperation.SUBMIT_ORDER,
        )
        await self._broker.submit_order(order)

    async def cancel_order(self, client_order_id: str) -> None:
        self._guard.assert_broker_operation(
            self._exchange,
            self._account_id,
            BrokerOperation.CANCEL_ORDER,
        )
        await self._broker.cancel_order(client_order_id)

    def events(self) -> AsyncIterator[BrokerEvent]:
        return self._broker.events()

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(mode={self._guard.mode.value!r}, "
            f"exchange={self._exchange!r}, account_id=<redacted>)"
        )
