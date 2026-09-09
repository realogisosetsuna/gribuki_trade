"""A 股 PAPER 日运行器的观察列表、候选、委托和成交文档边界。

这个模块只处理可重放的内存对象与普通 JSON 文档之间的转换。它不访问
日历、市场数据、账本或文件系统，因此运行器可以继续独占调度、事务和
副作用，而恢复路径也能复用同一套严格的编解码规则。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import (
    ASharePaperFill,
    PaperFillSource,
    PaperInstrumentType,
)
from gribuki_trade.features.ashare_screening import RankedAShareCandidate
from gribuki_trade.features.ashare_surveillance import (
    IntradayCandidate,
    IntradayCandidateClass,
)
from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.services.ashare.ashare_intraday_paper import IntradayPaperOrder
from gribuki_trade.services.ashare.ashare_paper_day_serialization import (
    _optional_positive_decimal,
)


@dataclass(frozen=True, slots=True)
class PaperDayWatchEntry:
    """冻结在 PAPER 日观察列表中的单个 A 股条目。"""

    symbol: str
    name: str
    board: AShareBoard
    source: str
    rank: int
    score: float


def strict_positive_int(value: object) -> int:
    """读取恢复文档中的正整数，拒绝布尔值和零。"""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("value must be a positive integer")
    return value


def board_from_symbol(symbol: str) -> AShareBoard:
    """根据带交易所后缀的 A 股代码解析上市板块。"""

    normalized = symbol.strip().upper()
    if normalized.endswith(".BJ"):
        return AShareBoard.BSE
    code = normalized[:6]
    if normalized.endswith(".SH"):
        return AShareBoard.STAR if code.startswith(("688", "689")) else AShareBoard.SSE_MAIN
    if normalized.endswith(".SZ"):
        return AShareBoard.CHINEXT if code.startswith(("300", "301")) else AShareBoard.SZSE_MAIN
    raise ValueError("unsupported A-share symbol")


def watch_entry_from_screen(item: RankedAShareCandidate) -> PaperDayWatchEntry:
    """把盘前筛选候选投影为 PAPER 观察列表条目。"""

    return PaperDayWatchEntry(
        symbol=item.symbol,
        name=item.name,
        board=item.board,
        source="PREOPEN_SCREEN",
        rank=item.rank or 999,
        score=item.composite_score or 0.0,
    )


def watch_entry_from_intraday(item: IntradayCandidate) -> PaperDayWatchEntry:
    """把盘中异动候选投影为观察列表条目。"""

    return PaperDayWatchEntry(
        symbol=item.symbol,
        name=item.name,
        board=board_from_symbol(item.symbol),
        source="INTRADAY_SCAN",
        rank=item.rank,
        score=item.anomaly_score,
    )


def watch_entry_document(item: PaperDayWatchEntry) -> dict[str, object]:
    """编码观察列表条目，保持 JSON 可序列化的标量字段。"""

    return {
        "board": item.board.value,
        "name": item.name,
        "rank": item.rank,
        "score": item.score,
        "source": item.source,
        "symbol": item.symbol,
    }


def watch_entry_from_document(
    value: object,
    default_source: str,
) -> PaperDayWatchEntry | None:
    """从恢复文档读取观察列表条目；坏条目安全地被跳过。"""

    if not isinstance(value, dict):
        return None
    symbol = value.get("symbol")
    name = value.get("name")
    rank = value.get("rank")
    score = value.get("score", value.get("composite_score"))
    board = value.get("board")
    source = value.get("source", default_source)
    if not isinstance(symbol, str) or not isinstance(name, str):
        return None
    if isinstance(rank, bool) or not isinstance(rank, int):
        rank = 999
    if not isinstance(score, (int, float)) or isinstance(score, bool):
        score = 0.0
    try:
        resolved_board = AShareBoard(board) if isinstance(board, str) else board_from_symbol(symbol)
    except ValueError:
        return None
    return PaperDayWatchEntry(
        symbol=symbol,
        name=name,
        board=resolved_board,
        source=source if isinstance(source, str) else default_source,
        rank=rank,
        score=float(score),
    )


def candidate_document(item: IntradayCandidate) -> dict[str, object]:
    """编码盘中候选，保留 Decimal 的十进制文本精度。"""

    return {
        "anomaly_score": item.anomaly_score,
        "candidate_class": item.candidate_class.value,
        "change_percent": str(item.change_percent),
        "factor_weight_coverage": item.factor_weight_coverage,
        "last_price": str(item.last_price),
        "name": item.name,
        "previous_close": None if item.previous_close is None else str(item.previous_close),
        "rank": item.rank,
        "reason_codes": list(item.reason_codes),
        "session_amount_cny": str(item.session_amount_cny),
        "symbol": item.symbol,
    }


def candidate_from_document(value: object) -> IntradayCandidate | None:
    """从候选恢复文档重建无因子明细的异动候选。"""

    if not isinstance(value, dict):
        return None
    try:
        return IntradayCandidate(
            symbol=str(value["symbol"]),
            name=str(value["name"]),
            rank=int(str(value["rank"])),
            candidate_class=IntradayCandidateClass(str(value["candidate_class"])),
            anomaly_score=float(str(value["anomaly_score"])),
            factor_weight_coverage=float(str(value["factor_weight_coverage"])),
            last_price=Decimal(str(value["last_price"])),
            change_percent=Decimal(str(value["change_percent"])),
            session_amount_cny=Decimal(str(value["session_amount_cny"])),
            factors=(),
            reason_codes=tuple(str(item) for item in value.get("reason_codes", [])),
            previous_close=_optional_positive_decimal(value.get("previous_close")),
        )
    except (KeyError, TypeError, ValueError):
        return None


def candidate_previous_close(item: IntradayCandidate | None) -> Decimal | None:
    """返回候选可信的前收盘价，并兼容旧日志的推导方式。"""

    if item is None:
        return None
    if item.previous_close is not None:
        value = item.previous_close
        return value if value.is_finite() and value > 0 else None
    # 旧日志只保存涨跌幅；仅在分母有效时做兼容性推导。
    denominator = Decimal("1") + item.change_percent / Decimal("100")
    if denominator <= 0:
        return None
    value = item.last_price / denominator
    return value if value.is_finite() and value > 0 else None


def order_document(order: IntradayPaperOrder) -> dict[str, object]:
    """编码待撮合 PAPER 委托。"""

    return {
        "account_id": order.account_id,
        "board": order.board.value,
        "created_at": order.created_at,
        "expires_at": order.expires_at,
        "instrument_type": order.instrument_type.value,
        "invalidation_price": order.invalidation_price,
        "limit_price": order.limit_price,
        "order_id": order.order_id,
        "previous_close": order.previous_close,
        "quantity": order.quantity,
        "session_date": order.session_date,
        "signal_bar_end": order.signal_bar_end,
        "signal_price": order.signal_price,
        "symbol": order.symbol,
    }


def order_from_document(value: Mapping[str, object]) -> IntradayPaperOrder:
    """从持久化文档严格恢复待撮合 PAPER 委托。"""

    return IntradayPaperOrder(
        order_id=str(value["order_id"]),
        account_id=str(value["account_id"]),
        symbol=str(value["symbol"]),
        board=AShareBoard(str(value["board"])),
        session_date=date.fromisoformat(str(value["session_date"])),
        signal_bar_end=datetime.fromisoformat(str(value["signal_bar_end"])),
        signal_price=Decimal(str(value["signal_price"])),
        invalidation_price=Decimal(str(value["invalidation_price"])),
        limit_price=Decimal(str(value["limit_price"])),
        quantity=int(str(value["quantity"])),
        created_at=datetime.fromisoformat(str(value["created_at"])),
        expires_at=datetime.fromisoformat(str(value["expires_at"])),
        previous_close=Decimal(str(value["previous_close"])),
        instrument_type=PaperInstrumentType(str(value["instrument_type"])),
    )


def fill_document(fill: ASharePaperFill) -> dict[str, object]:
    """编码 PAPER 成交，保留来源和外部订单关联字段。"""

    return {
        "account_id": fill.account_id,
        "executed_at": fill.executed_at,
        "external_order_id": fill.external_order_id,
        "fill_id": fill.fill_id,
        "instrument_type": fill.instrument_type.value,
        "note": fill.note,
        "price": fill.price,
        "quantity": fill.quantity,
        "side": fill.side.value,
        "source": fill.source.value,
        "symbol": fill.symbol,
        "trading_date": fill.trading_date,
    }


def fill_from_document(value: Mapping[str, object]) -> ASharePaperFill:
    """从成交文档严格恢复 A 股 PAPER 成交。"""

    return ASharePaperFill(
        account_id=str(value["account_id"]),
        fill_id=str(value["fill_id"]),
        symbol=str(value["symbol"]),
        side=Side(str(value["side"])),
        quantity=int(str(value["quantity"])),
        price=Decimal(str(value["price"])),
        instrument_type=PaperInstrumentType(str(value["instrument_type"])),
        trading_date=date.fromisoformat(str(value["trading_date"])),
        executed_at=datetime.fromisoformat(str(value["executed_at"])),
        source=PaperFillSource(str(value["source"])),
        external_order_id=(
            None if value.get("external_order_id") is None else str(value["external_order_id"])
        ),
        note=None if value.get("note") is None else str(value["note"]),
    )


__all__ = [
    "PaperDayWatchEntry",
    "board_from_symbol",
    "candidate_document",
    "candidate_from_document",
    "candidate_previous_close",
    "fill_document",
    "fill_from_document",
    "order_document",
    "order_from_document",
    "strict_positive_int",
    "watch_entry_document",
    "watch_entry_from_document",
    "watch_entry_from_intraday",
    "watch_entry_from_screen",
]
