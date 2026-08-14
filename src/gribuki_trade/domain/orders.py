"""与券商无关的订单领域类型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    LIMIT = "LIMIT"


class OrderStatus(StrEnum):
    CREATED = "CREATED"
    VALIDATED = "VALIDATED"
    LOCAL_REJECTED = "LOCAL_REJECTED"
    SUBMITTING = "SUBMITTING"
    ACCEPTED = "ACCEPTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELED = "CANCELED"
    BROKER_REJECTED = "BROKER_REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """应用交易所规则与风险校验前的不可变请求。"""

    client_order_id: str
    account_id: str
    strategy_id: str
    symbol: str
    side: Side
    quantity: Decimal
    limit_price: Decimal
    created_at: datetime
    order_type: OrderType = OrderType.LIMIT

    def __post_init__(self) -> None:
        if not self.client_order_id.strip():
            raise ValueError("client_order_id must not be empty")
        if not self.account_id.strip():
            raise ValueError("account_id must not be empty")
        if not self.strategy_id.strip():
            raise ValueError("strategy_id must not be empty")
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        try:
            quantity = Decimal(str(self.quantity))
            limit_price = Decimal(str(self.limit_price))
        except Exception as error:
            raise ValueError("quantity and limit_price must be decimal numbers") from error
        if not quantity.is_finite() or quantity <= 0:
            raise ValueError("quantity must be positive")
        if not limit_price.is_finite() or limit_price <= 0:
            raise ValueError("limit_price must be positive")
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "limit_price", limit_price)
