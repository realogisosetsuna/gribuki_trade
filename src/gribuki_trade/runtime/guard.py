"""真实券商适配器操作外围的默认拒绝控制。"""

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
    """真实券商边界暴露的已知操作。"""

    CONNECT = "connect"
    DISCONNECT = "disconnect"
    QUERY = "query"
    SUBSCRIBE = "subscribe"
    SUBMIT_ORDER = "submit_order"
    CANCEL_ORDER = "cancel_order"
    REPLACE_ORDER = "replace_order"
    CHANGE_RISK = "change_risk"

    @property
    def changes_orders(self) -> bool:
        return self in {
            BrokerOperation.SUBMIT_ORDER,
            BrokerOperation.CANCEL_ORDER,
            BrokerOperation.REPLACE_ORDER,
            BrokerOperation.CHANGE_RISK,
        }


class TradingModeViolation(PermissionError):
    """运行时模式边界拒绝了一项真实券商操作。"""


class LiveTradingNotConfirmed(TradingModeViolation):
    """已选择 LIVE，但尚未在本地显式解锁。"""


class AccountNotAllowed(TradingModeViolation):
    """请求的实盘账户不在不可变允许列表中。"""


class ExchangeNotAllowed(TradingModeViolation):
    """请求的实盘交易所不在不可变允许列表中。"""


class OperationNotAllowed(TradingModeViolation):
    """所选模式不能执行请求的券商操作。"""


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
    """按照一种运行时模式授权访问真实券商。

    默认实例为 PAPER，会拒绝对真实券商的所有操作。SHADOW 仅允许连接、读取和
    订阅操作。LIVE 要求同时满足三个独立条件：精确的本地确认短语、允许列表中的
    账户，以及允许列表中的交易所。确认状态仅存在于本进程中，不能从环境或配置加载。
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

    # 为策略和配置调用方保留 whitelist 拼写的属性。
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
        """在本地短语精确匹配后，解锁此内存中的 LIVE 守卫。

        请在交互式本地边界调用 :func:`prompt_for_live_confirmation`。确认失败时，
        守卫始终恢复为锁定状态，提供的短语绝不会写入异常。
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

    # 为上下文中已明确指代该守卫的集成保留简短别名。
    confirm_live = confirm_live_trading

    def lock(self) -> None:
        """立即撤销进程本地的 LIVE 确认。"""

        with self._lock:
            self._live_confirmed = False

    def assert_broker_operation(
        self,
        exchange: str,
        account_id: str,
        operation: BrokerOperation | str,
    ) -> None:
        """若请求的真实券商操作未获授权，则抛出异常。"""

        checked_operation = _coerce_operation(operation)
        checked_exchange = _normalize_request(exchange, kind="exchange", uppercase=True)
        checked_account = _normalize_request(account_id, kind="account", uppercase=False)

        if self._mode is TradingMode.PAPER:
            raise OperationNotAllowed("PAPER mode cannot access a real broker")
        if checked_operation is BrokerOperation.DISCONNECT:
            # 已建立的真实会话必须始终能够以安全关闭方式断开。
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
    """在运行守卫的机器上无回显地收集 LIVE 短语。"""

    read_hidden = getpass_fn or getpass.getpass
    phrase = read_hidden(f'Type "{LIVE_CONFIRMATION_PHRASE}" to unlock LIVE trading: ')
    try:
        guard.confirm_live_trading(phrase)
    except LiveTradingNotConfirmed:
        return False
    return True


class GuardedBrokerAdapter:
    """默认难以绕过模式守卫的券商外观层。

    特别是，SHADOW 模式下的提交和撤单会在调用被包装的真实适配器方法前被拒绝。
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
