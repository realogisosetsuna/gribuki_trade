"""Binance 现货订单列表响应的纯协议解析。

订单列表响应只描述交易所返回的 OCO、OTO、OTOCO 或批量订单快照；本模块不持有
网关、凭据、网络传输或订单状态，因此可在离线测试和重启对账路径中独立复用。
"""

from __future__ import annotations

from collections.abc import Mapping

from .errors import BinanceProtocolError
from .models import BinanceOrderListSnapshot, BinanceOrderSnapshot
from .rules import BinanceValidationError
from .spot_parsing import parse_order_snapshot


def _parse_snapshot(payload: object, *, fallback_symbol: str) -> BinanceOrderSnapshot:
    """把列表成员转换为订单快照，并统一映射为协议错误。"""

    try:
        return parse_order_snapshot(payload, fallback_symbol=fallback_symbol)
    except (TypeError, ValueError, BinanceValidationError):
        raise BinanceProtocolError("Binance order response is malformed") from None


def parse_order_list_snapshot(
    payload: object, *, fallback_symbol: str
) -> BinanceOrderListSnapshot:
    """解析 OCO/OTO/OTOCO 的订单列表响应。"""

    if not isinstance(payload, Mapping):
        raise BinanceProtocolError("Binance order list response must be an object")
    reports = payload.get("orderReports", payload.get("orders", []))
    if not isinstance(reports, list):
        raise BinanceProtocolError("Binance order-list reports are malformed")
    orders = tuple(
        _parse_snapshot(item, fallback_symbol=fallback_symbol)
        for item in reports
        if isinstance(item, Mapping)
    )
    try:
        list_id = None if payload.get("orderListId") is None else int(payload["orderListId"])
        transaction = (
            None
            if payload.get("transactionTime") is None
            else int(payload["transactionTime"])
        )
    except (TypeError, ValueError):
        raise BinanceProtocolError("Binance order-list response is malformed") from None
    return BinanceOrderListSnapshot(
        order_list_id=list_id,
        contingency_type=(
            None
            if payload.get("contingencyType") is None
            else str(payload["contingencyType"])
        ),
        list_status_type=(
            None if payload.get("listStatusType") is None else str(payload["listStatusType"])
        ),
        list_order_status=(
            None if payload.get("listOrderStatus") is None else str(payload["listOrderStatus"])
        ),
        list_client_order_id=(
            None
            if payload.get("listClientOrderId") is None
            else str(payload["listClientOrderId"])
        ),
        symbol=str(payload.get("symbol", fallback_symbol)).upper(),
        orders=orders,
        transaction_time_ms=transaction,
    )


def parse_order_snapshots(
    payload: object, *, fallback_symbol: str
) -> tuple[BinanceOrderSnapshot, ...]:
    """解析返回订单快照数组（例如 ``DELETE /openOrders``）。"""

    if not isinstance(payload, list):
        raise BinanceProtocolError("Binance order-list response must be a list")
    try:
        return tuple(
            _parse_snapshot(item, fallback_symbol=fallback_symbol)
            for item in payload
        )
    except BinanceProtocolError:
        raise
    except (TypeError, ValueError, BinanceValidationError):
        raise BinanceProtocolError("Binance order-list response is malformed") from None


__all__ = ["parse_order_list_snapshot", "parse_order_snapshots"]
