"""A 股 PAPER 日运行器的纯支持函数。

这里集中放置市场 K 线、退出保护、时间规范化、哈希与事件 JSONL
编码。函数不持有运行器状态，也不访问网络；旧的
``services.ashare.ashare_paper_day`` 路径继续重新导出这些私有名称，
从而保持历史调用方和测试替身的兼容性。
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from gribuki_trade.domain.exit_plans import ExitBarrierKind, ExitPlan
from gribuki_trade.domain.paper_day import PaperDayEvent
from gribuki_trade.features.deep_exit_planning import (
    DeepExitTimeframe,
    aggregate_completed_bars,
)
from gribuki_trade.features.technical import TechnicalBar
from gribuki_trade.ports.market_data import IntradayBar

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _bar_document(bar: IntradayBar) -> dict[str, object]:
    return {
        "amount": bar.amount,
        "close": bar.close,
        "end_at": bar.end_at,
        "fetched_at": bar.meta.fetched_at,
        "high": bar.high,
        "interval": bar.interval.value,
        "is_closed": bar.is_closed,
        "low": bar.low,
        "open": bar.open,
        "provider": bar.meta.provider,
        "start_at": bar.start_at,
        "symbol": bar.symbol,
        "volume_lots": bar.volume_lots,
    }


def _bar_revision(bar: IntradayBar) -> str:
    # 有意排除采集时间：崩溃后重新获取同一不可变市场区间时，
    # 必须保留相同的撮合与成交身份。
    stable = {key: value for key, value in _bar_document(bar).items() if key != "fetched_at"}
    document = json.dumps(
        {key: str(value) for key, value in stable.items()},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(document.encode("utf-8")).hexdigest()


def _optional_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _aware_utc(datetime.fromisoformat(value), "stored datetime")
    except ValueError:
        return None


def _optional_positive_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        resolved = Decimal(str(value))
    except (ArithmeticError, ValueError):
        return None
    return resolved if resolved.is_finite() and resolved > 0 else None


def _decimal_display(value: Decimal | None) -> str:
    return "—" if value is None else f"{value:.3f}"


def _exit_protection_id(
    *,
    account_id: str,
    symbol: str,
    signal_bar_end: datetime,
    strategy_version: str,
) -> str:
    """把账户、标的和信号时点映射为稳定的保护流身份。"""

    document = "\0".join(
        (
            account_id.strip(),
            symbol.strip().upper(),
            _aware_utc(signal_bar_end, "signal_bar_end").isoformat(),
            strategy_version.strip(),
        )
    )
    return "paper-exit-" + hashlib.sha256(document.encode()).hexdigest()[:40]


def _calendar_time_exit(
    session_date: date,
    *,
    trading_sessions: tuple[date, ...],
    holding_sessions: int,
    market_close: time,
) -> datetime:
    """从启动时冻结的真实交易日历解析退出时间门。"""

    eligible = tuple(item for item in trading_sessions if item > session_date)
    if eligible:
        if len(eligible) < holding_sessions:
            raise ValueError("verified future trading sessions are insufficient")
        exit_date = eligible[holding_sessions - 1]
        return datetime.combine(exit_date, market_close, tzinfo=SHANGHAI).astimezone(UTC)

    # 仅保留给旧单元夹具和历史直接构造 Runner 的兼容路径。生产 CLI 必须注入
    # BaoStock 验证过的未来交易日，manifest 会明确标记此降级分支。
    remaining = holding_sessions
    cursor = session_date
    while remaining > 0:
        cursor += timedelta(days=1)
        if cursor.weekday() < 5:
            remaining -= 1
    return datetime.combine(cursor, market_close, tzinfo=SHANGHAI).astimezone(UTC)


def _exit_barriers_for_bar(
    plan: ExitPlan,
    bar: TechnicalBar,
) -> tuple[ExitBarrierKind, ...]:
    crossed: list[ExitBarrierKind] = []
    if bar.low <= plan.stop_price:
        crossed.append(ExitBarrierKind.STOP_LOSS)
    if bar.high >= plan.take_profit_price:
        crossed.append(ExitBarrierKind.TAKE_PROFIT)
    if _aware_utc(bar.end_time, "bar.end_time") >= plan.time_exit_at:
        crossed.append(ExitBarrierKind.TIME)
    return tuple(crossed)


def _technical_bar_document(bar: TechnicalBar) -> dict[str, object]:
    return {
        "available_at": bar.available_at,
        "close": bar.close,
        "complete": bar.complete,
        "end_time": bar.end_time,
        "high": bar.high,
        "low": bar.low,
        "open": bar.open,
        "volume": bar.volume,
    }


def _technical_bars_from_document(value: object) -> tuple[TechnicalBar, ...]:
    if not isinstance(value, list):
        return ()
    output: list[TechnicalBar] = []
    try:
        for item in value:
            if not isinstance(item, dict):
                return ()
            output.append(
                TechnicalBar(
                    end_time=datetime.fromisoformat(str(item["end_time"])),
                    available_at=datetime.fromisoformat(str(item["available_at"])),
                    open=Decimal(str(item["open"])),
                    high=Decimal(str(item["high"])),
                    low=Decimal(str(item["low"])),
                    close=Decimal(str(item["close"])),
                    volume=int(str(item["volume"])),
                    complete=bool(item["complete"]),
                )
            )
    except (ArithmeticError, KeyError, TypeError, ValueError):
        return ()
    return tuple(output)


def _paper_deep_timeframes(
    minute_bars: tuple[TechnicalBar, ...],
) -> tuple[DeepExitTimeframe, ...]:
    """构造 PAPER 主链冻结的 1/5/15 分钟多时间框架。"""

    return (
        DeepExitTimeframe(
            timeframe_id="1m",
            bars=aggregate_completed_bars(minute_bars, interval_minutes=1),
            weight=Decimal("0.20"),
            maximum_age=timedelta(minutes=5),
        ),
        DeepExitTimeframe(
            timeframe_id="5m",
            bars=aggregate_completed_bars(minute_bars, interval_minutes=5),
            weight=Decimal("0.30"),
            maximum_age=timedelta(minutes=15),
        ),
        DeepExitTimeframe(
            timeframe_id="15m",
            bars=aggregate_completed_bars(minute_bars, interval_minutes=15),
            weight=Decimal("0.50"),
            maximum_age=timedelta(minutes=30),
        ),
    )


def _exit_barrier_display(value: ExitBarrierKind) -> str:
    return {
        ExitBarrierKind.STOP_LOSS: "止损",
        ExitBarrierKind.TAKE_PROFIT: "止盈/趋势目标",
        ExitBarrierKind.TIME: "持有期限",
    }[value]


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _next_utc_minute(value: datetime) -> datetime:
    resolved = _aware_utc(value, "activation time")
    return resolved.replace(second=0, microsecond=0) + timedelta(minutes=1)


def _document_sha256(value: Mapping[str, object]) -> str:
    document = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(document.encode("utf-8")).hexdigest()


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def _append_and_sync_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())


def _event_jsonl(item: PaperDayEvent) -> str:
    return (
        json.dumps(
            {
                "correlation_id": item.correlation_id,
                "event_id": item.event_id,
                "event_type": item.event_type,
                "known_at": item.known_at.isoformat(),
                "occurred_at": item.occurred_at.isoformat(),
                "payload": item.payload,
                "phase": item.phase.value,
                "sequence": item.sequence,
                "severity": item.severity.value,
                "symbol": item.symbol,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        + "\n"
    )
