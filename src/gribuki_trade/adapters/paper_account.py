"""用于本地 Binance 现货模拟交易的纯 Decimal 余额预留。

模拟经纪商负责订单状态和成交，本模块则描述现货交易所的另一半：可用/锁定
资产余额以及以计价资产计收的费用。模块刻意采用同步实现，因为单个模拟引擎
拥有它并串行化变更；不可变快照可以安全地交给策略和图形界面。

买入手续费从计价资产中收取，因此买单会预留限价名义金额及可配置的手续费
缓冲。卖单预留基础资产数量，并从计价资产收入中扣除费用。这一保守约定具有
确定性，也不会假装模拟账户持有 BNB 手续费代币余额。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.domain.orders import OrderIntent, Side


class PaperLiquidityRole(StrEnum):
    """用于选择模拟佣金率的流动性角色。"""

    MAKER = "MAKER"
    TAKER = "TAKER"


class PaperReservationStatus(StrEnum):
    """一笔模拟账户资产预留的生命周期。"""

    ACTIVE = "ACTIVE"
    FILLED = "FILLED"
    CANCELED = "CANCELED"


class InsufficientPaperBalance(ValueError):
    """订单无法在不造成负余额的情况下预留足够的可用资产。"""


@dataclass(frozen=True, slots=True)
class SpotSymbolAssets:
    """一个 Binance 风格现货代码所对应的明确基础资产和计价资产。"""

    base_asset: str
    quote_asset: str

    def __post_init__(self) -> None:
        base = _asset(self.base_asset)
        quote = _asset(self.quote_asset)
        if base == quote:
            raise ValueError("base_asset and quote_asset must be distinct")
        object.__setattr__(self, "base_asset", base)
        object.__setattr__(self, "quote_asset", quote)


@dataclass(frozen=True, slots=True)
class PaperFeeSchedule:
    """挂单方/吃单方费率，以及限价买单预留的缓冲。

    ``buy_fee_buffer_rate`` 默认取较大的成交费率。明确指定的缓冲可以更大，
    但不能更小，否则有效成交可能需要比订单预留更多的计价资产。
    """

    maker_rate: Decimal = Decimal("0.001")
    taker_rate: Decimal = Decimal("0.001")
    buy_fee_buffer_rate: Decimal | None = None

    def __post_init__(self) -> None:
        maker = _rate(self.maker_rate, "maker_rate")
        taker = _rate(self.taker_rate, "taker_rate")
        buffer = (
            max(maker, taker)
            if self.buy_fee_buffer_rate is None
            else _rate(self.buy_fee_buffer_rate, "buy_fee_buffer_rate")
        )
        if buffer < max(maker, taker):
            raise ValueError("buy_fee_buffer_rate must cover maker and taker rates")
        object.__setattr__(self, "maker_rate", maker)
        object.__setattr__(self, "taker_rate", taker)
        object.__setattr__(self, "buy_fee_buffer_rate", buffer)

    def rate(self, role: PaperLiquidityRole | str) -> Decimal:
        resolved = PaperLiquidityRole(role)
        return self.maker_rate if resolved is PaperLiquidityRole.MAKER else self.taker_rate


@dataclass(frozen=True, slots=True)
class PaperAssetBalance:
    """一份不可变的可用/锁定余额投影。"""

    asset: str
    free: Decimal
    locked: Decimal

    @property
    def total(self) -> Decimal:
        return self.free + self.locked


@dataclass(frozen=True, slots=True)
class PaperOrderReservation:
    """订单最新的不可变预留状态。"""

    order: OrderIntent
    base_asset: str
    quote_asset: str
    locked_asset: str
    locked_amount: Decimal
    remaining_quantity: Decimal
    status: PaperReservationStatus


@dataclass(frozen=True, slots=True)
class PaperAccountFill:
    """一笔幂等模拟成交的账务结果。"""

    fill_id: str
    client_order_id: str
    side: Side
    liquidity_role: PaperLiquidityRole
    quantity: Decimal
    price: Decimal
    notional_quote: Decimal
    fee_asset: str
    fee_amount: Decimal
    released_amount: Decimal
    remaining_quantity: Decimal


@dataclass(frozen=True, slots=True)
class PaperAccountSnapshot:
    """供策略和界面读取的不可变时点余额。"""

    sequence: int
    balances: tuple[PaperAssetBalance, ...]

    def balance(self, asset: str) -> PaperAssetBalance:
        normalized = _asset(asset)
        return next(
            (value for value in self.balances if value.asset == normalized),
            PaperAssetBalance(normalized, Decimal("0"), Decimal("0")),
        )


@dataclass(slots=True)
class _MutableBalance:
    free: Decimal
    locked: Decimal = Decimal("0")


@dataclass(slots=True)
class _MutableReservation:
    order: OrderIntent
    base_asset: str
    quote_asset: str
    locked_asset: str
    locked_amount: Decimal
    remaining_quantity: Decimal
    status: PaperReservationStatus = PaperReservationStatus.ACTIVE

    def snapshot(self) -> PaperOrderReservation:
        return PaperOrderReservation(
            order=self.order,
            base_asset=self.base_asset,
            quote_asset=self.quote_asset,
            locked_asset=self.locked_asset,
            locked_amount=self.locked_amount,
            remaining_quantity=self.remaining_quantity,
            status=self.status,
        )


class PaperSpotAccount:
    """支持原子预留和成交记账的内存现货账户。

    系统刻意不猜测代码拆分方式。调用方必须提供交易所的基础/计价资产元数据，
    从而避免使用脆弱的后缀启发式规则拆分 ``ETHUSDT`` 等有歧义的代码。
    """

    def __init__(
        self,
        *,
        account_id: str,
        initial_balances: Mapping[str, Decimal | str | int],
        symbol_assets: Mapping[str, SpotSymbolAssets | tuple[str, str]],
        fees: PaperFeeSchedule | None = None,
    ) -> None:
        normalized_account = account_id.strip()
        if not normalized_account:
            raise ValueError("account_id must not be empty")
        self.account_id = normalized_account
        self.fees = fees or PaperFeeSchedule()

        balances: dict[str, _MutableBalance] = {}
        for raw_asset, raw_value in initial_balances.items():
            asset = _asset(raw_asset)
            if asset in balances:
                raise ValueError(f"duplicate normalized asset: {asset}")
            value = _amount(raw_value, f"initial_balances[{asset}]")
            balances[asset] = _MutableBalance(free=value)
        self._balances = balances

        symbols: dict[str, SpotSymbolAssets] = {}
        for raw_symbol, raw_assets in symbol_assets.items():
            symbol = _symbol(raw_symbol)
            if symbol in symbols:
                raise ValueError(f"duplicate normalized symbol: {symbol}")
            symbols[symbol] = (
                raw_assets
                if isinstance(raw_assets, SpotSymbolAssets)
                else SpotSymbolAssets(*raw_assets)
            )
        if not symbols:
            raise ValueError("symbol_assets must not be empty")
        self._symbols = symbols
        self._reservations: dict[str, _MutableReservation] = {}
        self._fills: dict[str, PaperAccountFill] = {}
        self._sequence = 0

    def balance(self, asset: str) -> PaperAssetBalance:
        """返回不可变余额；未知资产按零读取。"""

        normalized = _asset(asset)
        value = self._balances.get(normalized)
        if value is None:
            return PaperAssetBalance(normalized, Decimal("0"), Decimal("0"))
        return PaperAssetBalance(normalized, value.free, value.locked)

    def balances(self) -> tuple[PaperAssetBalance, ...]:
        """按确定的资产顺序返回所有已实体化余额。"""

        return tuple(self.balance(asset) for asset in sorted(self._balances))

    def snapshot(self) -> PaperAccountSnapshot:
        return PaperAccountSnapshot(sequence=self._sequence, balances=self.balances())

    def reservation(self, client_order_id: str) -> PaperOrderReservation | None:
        value = self._reservations.get(client_order_id)
        return None if value is None else value.snapshot()

    def fills(self) -> tuple[PaperAccountFill, ...]:
        return tuple(self._fills.values())

    def reserve_order(self, order: OrderIntent) -> PaperOrderReservation:
        """提交给经纪商前，将所需可用资产转为锁定状态。

        重复相同订单具有幂等性，即使订单已经终结也是如此。系统会拒绝把同一
        标识符用于不同内容。
        """

        existing = self._reservations.get(order.client_order_id)
        if existing is not None:
            if existing.order != order:
                raise ValueError(
                    f"client_order_id {order.client_order_id!r} is already reserved "
                    "for a different order"
                )
            return existing.snapshot()
        if order.account_id != self.account_id:
            raise ValueError(
                f"order account_id {order.account_id!r} does not match "
                f"paper account {self.account_id!r}"
            )

        assets = self._symbols.get(_symbol(order.symbol))
        if assets is None:
            raise ValueError(f"unknown paper Spot symbol: {order.symbol!r}")
        if order.side is Side.BUY:
            assert self.fees.buy_fee_buffer_rate is not None
            locked_asset = assets.quote_asset
            locked_amount = (
                order.quantity
                * order.limit_price
                * (Decimal("1") + self.fees.buy_fee_buffer_rate)
            )
        else:
            locked_asset = assets.base_asset
            locked_amount = order.quantity

        balance = self._mutable_balance(locked_asset)
        if balance.free < locked_amount:
            raise InsufficientPaperBalance(
                f"insufficient free {locked_asset}: need {locked_amount}, "
                f"have {balance.free}"
            )
        balance.free -= locked_amount
        balance.locked += locked_amount
        reservation = _MutableReservation(
            order=order,
            base_asset=assets.base_asset,
            quote_asset=assets.quote_asset,
            locked_asset=locked_asset,
            locked_amount=locked_amount,
            remaining_quantity=order.quantity,
        )
        self._reservations[order.client_order_id] = reservation
        self._sequence += 1
        self._assert_non_negative()
        return reservation.snapshot()

    def apply_fill(
        self,
        client_order_id: str,
        *,
        quantity: Decimal | str | int,
        price: Decimal | str | int,
        fill_id: str,
        liquidity_role: PaperLiquidityRole | str = PaperLiquidityRole.TAKER,
    ) -> PaperAccountFill:
        """原子消耗预留、收取手续费并记入成交收入。"""

        normalized_fill_id = fill_id.strip()
        if not normalized_fill_id:
            raise ValueError("fill_id must not be empty")
        normalized_quantity = _positive(quantity, "quantity")
        normalized_price = _positive(price, "price")
        role = PaperLiquidityRole(liquidity_role)

        duplicate = self._fills.get(normalized_fill_id)
        if duplicate is not None:
            if (
                duplicate.client_order_id != client_order_id
                or duplicate.quantity != normalized_quantity
                or duplicate.price != normalized_price
                or duplicate.liquidity_role is not role
            ):
                raise ValueError(f"fill_id {normalized_fill_id!r} has conflicting contents")
            return duplicate

        reservation = self._reservations.get(client_order_id)
        if reservation is None:
            raise KeyError(f"unknown client_order_id: {client_order_id!r}")
        if reservation.status is not PaperReservationStatus.ACTIVE:
            raise ValueError(
                f"order {client_order_id!r} cannot fill from {reservation.status.value}"
            )
        if normalized_quantity > reservation.remaining_quantity:
            raise ValueError(f"fill would overfill order {client_order_id!r}")
        if reservation.order.side is Side.BUY:
            if normalized_price > reservation.order.limit_price:
                raise ValueError("buy fill price must not exceed its limit price")
        elif normalized_price < reservation.order.limit_price:
            raise ValueError("sell fill price must not be below its limit price")

        notional = normalized_quantity * normalized_price
        fee = notional * self.fees.rate(role)
        released = Decimal("0")
        if reservation.order.side is Side.BUY:
            assert self.fees.buy_fee_buffer_rate is not None
            reserved_for_fill = (
                normalized_quantity
                * reservation.order.limit_price
                * (Decimal("1") + self.fees.buy_fee_buffer_rate)
            )
            actual_debit = notional + fee
            if actual_debit > reserved_for_fill:
                raise InsufficientPaperBalance(
                    "buy fill exceeds its reserved limit notional and fee buffer"
                )
            released = reserved_for_fill - actual_debit
            quote = self._mutable_balance(reservation.quote_asset)
            base = self._mutable_balance(reservation.base_asset)
            if quote.locked < reserved_for_fill:
                raise RuntimeError("paper quote reservation is internally inconsistent")
            quote.locked -= reserved_for_fill
            quote.free += released
            base.free += normalized_quantity
            consumed_locked = reserved_for_fill
        else:
            proceeds = notional - fee
            if proceeds < 0:
                raise ValueError("fee must not exceed sell proceeds")
            base = self._mutable_balance(reservation.base_asset)
            quote = self._mutable_balance(reservation.quote_asset)
            if base.locked < normalized_quantity:
                raise RuntimeError("paper base reservation is internally inconsistent")
            base.locked -= normalized_quantity
            quote.free += proceeds
            consumed_locked = normalized_quantity

        reservation.locked_amount -= consumed_locked
        reservation.remaining_quantity -= normalized_quantity
        if reservation.remaining_quantity == 0:
            # 所有预留运算均使用精确的 Decimal 操作。明确保留这一不变量，
            # 避免未来的量化器泄漏零碎余额。
            if reservation.locked_amount != 0:
                terminal_release = reservation.locked_amount
                locked = self._mutable_balance(reservation.locked_asset)
                if locked.locked < terminal_release:
                    raise RuntimeError("paper terminal reservation is inconsistent")
                locked.locked -= terminal_release
                locked.free += terminal_release
                released += terminal_release
                reservation.locked_amount = Decimal("0")
            reservation.status = PaperReservationStatus.FILLED

        receipt = PaperAccountFill(
            fill_id=normalized_fill_id,
            client_order_id=client_order_id,
            side=reservation.order.side,
            liquidity_role=role,
            quantity=normalized_quantity,
            price=normalized_price,
            notional_quote=notional,
            fee_asset=reservation.quote_asset,
            fee_amount=fee,
            released_amount=released,
            remaining_quantity=reservation.remaining_quantity,
        )
        self._fills[normalized_fill_id] = receipt
        self._sequence += 1
        self._assert_non_negative()
        return receipt

    def cancel_order(self, client_order_id: str) -> PaperOrderReservation:
        """释放所有未成交资产预留；重复取消是安全的。"""

        reservation = self._reservations.get(client_order_id)
        if reservation is None:
            raise KeyError(f"unknown client_order_id: {client_order_id!r}")
        if reservation.status is PaperReservationStatus.CANCELED:
            return reservation.snapshot()
        if reservation.status is PaperReservationStatus.FILLED:
            raise ValueError(f"filled order {client_order_id!r} cannot be canceled")

        balance = self._mutable_balance(reservation.locked_asset)
        if balance.locked < reservation.locked_amount:
            raise RuntimeError("paper cancellation reservation is internally inconsistent")
        balance.locked -= reservation.locked_amount
        balance.free += reservation.locked_amount
        reservation.locked_amount = Decimal("0")
        reservation.status = PaperReservationStatus.CANCELED
        self._sequence += 1
        self._assert_non_negative()
        return reservation.snapshot()

    def _mutable_balance(self, asset: str) -> _MutableBalance:
        return self._balances.setdefault(asset, _MutableBalance(free=Decimal("0")))

    def _assert_non_negative(self) -> None:
        if any(value.free < 0 or value.locked < 0 for value in self._balances.values()):
            raise RuntimeError("paper account invariant violated: negative balance")


def _asset(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("asset must not be empty")
    return normalized


def _symbol(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("symbol must not be empty")
    return normalized


def _amount(value: Decimal | str | int, name: str) -> Decimal:
    try:
        normalized = Decimal(str(value))
    except Exception as error:
        raise ValueError(f"{name} must be a decimal number") from error
    if not normalized.is_finite() or normalized < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return normalized


def _positive(value: Decimal | str | int, name: str) -> Decimal:
    normalized = _amount(value, name)
    if normalized <= 0:
        raise ValueError(f"{name} must be positive")
    return normalized


def _rate(value: Decimal | str | int, name: str) -> Decimal:
    normalized = _amount(value, name)
    if normalized >= 1:
        raise ValueError(f"{name} must be below one")
    return normalized
