"""AKShare 历史日线尾部拼接的纯策略实现。

本模块只校验两个已经解析的 ``DailyBar`` 序列，并在重叠窗口一致时拼接
较新来源的尾部。它不访问 provider、网络或持久化；适配器 facade 负责将
错误类型注入，从而保持既有 ``HistoricalDailyFallbackError`` 继承关系。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TypeAlias

from gribuki_trade.domain.market import DailyBar, PriceAdjustment

ErrorType: TypeAlias = type[Exception]


@dataclass(frozen=True, slots=True)
class HistoricalDailyTailStitchPolicy:
    """将延迟的长历史与新鲜尾部连接的明确选择加入策略。

    请求的 ``end`` 日期被视为 ``latest_completed_session``。只有主来源包含该
    精确日期时，才允许产生拼接结果。
    """

    minimum_overlap_sessions: int = 20

    def __post_init__(self) -> None:
        if self.minimum_overlap_sessions < 20:
            raise ValueError("minimum_overlap_sessions must be at least 20")


@dataclass(frozen=True, slots=True)
class HistoricalDailyTailStitchDiagnostics:
    """拼接结果的可审计证明与非阻塞单元诊断。"""

    base_source: str
    tail_source: str
    required_latest_session: date
    base_latest_session: date
    overlap_start_session: date
    overlap_end_session: date
    overlap_sessions_validated: int
    stitched_tail_sessions: int
    volume_mismatch_sessions: int
    amount_mismatch_sessions: int

    def __post_init__(self) -> None:
        if self.overlap_sessions_validated < 20:
            raise ValueError("overlap_sessions_validated must be at least 20")
        if self.stitched_tail_sessions < 1:
            raise ValueError("stitched_tail_sessions must be positive")
        if self.base_latest_session >= self.required_latest_session:
            raise ValueError("base source must lag required_latest_session")


def controlled_tail_stitch(
    *,
    base_bars: tuple[DailyBar, ...],
    base_source: str,
    tail_bars: tuple[DailyBar, ...],
    tail_source: str,
    required_latest_session: date,
    minimum_overlap_sessions: int,
    tail_error: ErrorType,
    overlap_error: ErrorType,
) -> tuple[tuple[DailyBar, ...], HistoricalDailyTailStitchDiagnostics]:
    """只把经过严格证明的新鲜尾部连接到较长的延迟历史。

    ``tail_error`` 和 ``overlap_error`` 由适配器 facade 注入，以便保留其
    对外公开的异常继承关系；该模块自身不依赖 provider 或 facade。
    """

    _validate_input(base_bars, source=base_source, error=tail_error)
    _validate_input(tail_bars, source=tail_source, error=tail_error)
    if base_bars[0].symbol != tail_bars[0].symbol:
        raise tail_error("tail stitch sources contain different symbols")
    if base_bars[-1].trade_date >= required_latest_session:
        raise tail_error(
            "tail stitch base is not delayed relative to required latest session"
        )
    tail_by_date = {bar.trade_date: bar for bar in tail_bars}
    if required_latest_session not in tail_by_date:
        raise tail_error(
            "tail source does not contain required latest_completed_session "
            f"{required_latest_session.isoformat()}"
        )

    overlap_base = tuple(
        bar
        for bar in base_bars
        if bar.trade_date <= base_bars[-1].trade_date and bar.is_trading
    )[-minimum_overlap_sessions:]
    if len(overlap_base) < minimum_overlap_sessions:
        raise tail_error(
            "insufficient base history for tail-stitch overlap validation: "
            f"required={minimum_overlap_sessions}, available={len(overlap_base)}"
        )
    overlap_start = overlap_base[0].trade_date
    overlap_end = overlap_base[-1].trade_date
    overlap_tail = tuple(
        bar
        for bar in tail_bars
        if overlap_start <= bar.trade_date <= overlap_end
    )
    base_dates = tuple(bar.trade_date for bar in overlap_base)
    tail_dates = tuple(bar.trade_date for bar in overlap_tail)
    if tail_dates != base_dates:
        missing_from_tail = sorted(set(base_dates).difference(tail_dates))
        extra_in_tail = sorted(set(tail_dates).difference(base_dates))
        raise overlap_error(
            "tail-stitch overlap dates differ: "
            f"missing_from_tail={[item.isoformat() for item in missing_from_tail]}, "
            f"extra_in_tail={[item.isoformat() for item in extra_in_tail]}"
        )

    volume_mismatches = 0
    amount_mismatches = 0
    for base_bar, tail_bar in zip(overlap_base, overlap_tail, strict=True):
        if not tail_bar.is_trading:
            raise overlap_error(
                "tail-stitch common date is not trading in tail source: "
                f"{tail_bar.trade_date.isoformat()}"
            )
        base_ohlc = (base_bar.open, base_bar.high, base_bar.low, base_bar.close)
        tail_ohlc = (tail_bar.open, tail_bar.high, tail_bar.low, tail_bar.close)
        if base_ohlc != tail_ohlc:
            field_names = ("open", "high", "low", "close")
            differences = [
                f"{field}:base={base_value},tail={tail_value}"
                for field, base_value, tail_value in zip(
                    field_names, base_ohlc, tail_ohlc, strict=True
                )
                if base_value != tail_value
            ]
            raise overlap_error(
                "tail-stitch OHLC mismatch on "
                f"{base_bar.trade_date.isoformat()}: {', '.join(differences)}"
            )
        volume_mismatches += base_bar.volume != tail_bar.volume
        amount_mismatches += base_bar.amount != tail_bar.amount

    base_latest = base_bars[-1].trade_date
    fresh_tail = tuple(
        bar
        for bar in tail_bars
        if base_latest < bar.trade_date <= required_latest_session
    )
    if not fresh_tail or fresh_tail[-1].trade_date != required_latest_session:
        raise tail_error("tail source cannot extend base through required latest session")
    stitched = (*base_bars, *fresh_tail)
    stitched_dates = tuple(bar.trade_date for bar in stitched)
    if stitched_dates != tuple(sorted(stitched_dates)):
        raise tail_error("stitched dates are not strictly ordered")
    if len(stitched_dates) != len(set(stitched_dates)):
        raise tail_error("stitched result contains duplicate dates")
    return stitched, HistoricalDailyTailStitchDiagnostics(
        base_source=base_source,
        tail_source=tail_source,
        required_latest_session=required_latest_session,
        base_latest_session=base_latest,
        overlap_start_session=overlap_start,
        overlap_end_session=overlap_end,
        overlap_sessions_validated=len(overlap_base),
        stitched_tail_sessions=len(fresh_tail),
        volume_mismatch_sessions=volume_mismatches,
        amount_mismatch_sessions=amount_mismatches,
    )


def _validate_input(
    bars: tuple[DailyBar, ...],
    *,
    source: str,
    error: ErrorType,
) -> None:
    if not bars:
        raise error(f"tail stitch source {source} returned no bars")
    dates = tuple(bar.trade_date for bar in bars)
    if dates != tuple(sorted(dates)) or len(dates) != len(set(dates)):
        raise error(f"tail stitch source {source} dates must be ordered and unique")
    symbol = bars[0].symbol
    if any(bar.symbol != symbol for bar in bars):
        raise error(f"tail stitch source {source} contains mixed symbols")
    if any(bar.adjustment is not PriceAdjustment.NONE for bar in bars):
        raise error(f"tail stitch source {source} contains adjusted prices")


# 保留具名边界，供历史适配器 facade 和独立测试复用。
validate_stitch_input = _validate_input
