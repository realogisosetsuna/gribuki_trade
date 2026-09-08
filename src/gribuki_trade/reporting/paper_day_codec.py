"""PAPER-day sidecar 的纯 JSON 解码工具。

本模块只负责把状态 JSON 和追加事件日志解码为报告投影使用的基础值。
它不打开数据库、不访问网络，也不改变事件顺序或业务状态。异常沿用
``paper_day_summary`` 中的稳定 ``PaperDaySidecarError`` 类型，保持旧调用方
的错误处理契约不变。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import cast

from .paper_day_summary import PaperDaySidecarError, PaperDaySidecarEvent


def read_json_object(path: Path, code: str) -> dict[str, object]:
    """读取一个 UTF-8 JSON 对象，并映射为稳定的 sidecar 错误。"""

    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise PaperDaySidecarError(code, f"cannot read {path.name}") from error
    if not isinstance(parsed, dict):
        raise PaperDaySidecarError(code, f"{path.name} must contain a JSON object")
    return cast(dict[str, object], parsed)


def read_events(path: Path) -> tuple[tuple[PaperDaySidecarEvent, ...], tuple[str, ...]]:
    """逐行解码事件 sidecar，容忍最后一个并发追加的半行。"""

    try:
        raw = path.read_bytes()
    except OSError as error:
        raise PaperDaySidecarError(
            "EVENT_LOG_NOT_AVAILABLE", "cannot read session.log.jsonl"
        ) from error
    warnings: list[str] = []
    events: list[PaperDaySidecarEvent] = []
    lines = raw.splitlines(keepends=True)
    for index, raw_line in enumerate(lines):
        if not raw_line.strip():
            continue
        try:
            parsed = json.loads(raw_line.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as error:
            is_trailing_partial = index == len(lines) - 1 and not raw_line.endswith(
                (b"\n", b"\r")
            )
            if is_trailing_partial:
                warnings.append("忽略了 session.log.jsonl 末尾一个尚未完成的并发追加片段。")
                continue
            raise PaperDaySidecarError(
                "EVENT_LOG_INVALID", f"invalid event JSON at line {index + 1}"
            ) from error
        if not isinstance(parsed, dict):
            raise PaperDaySidecarError(
                "EVENT_LOG_INVALID", f"event line {index + 1} is not an object"
            )
        events.append(parse_event(cast(dict[str, object], parsed), index + 1))
    return tuple(events), tuple(warnings)


def parse_event(value: dict[str, object], line: int) -> PaperDaySidecarEvent:
    """解码单条事件并校验其身份、时间和 payload。"""

    sequence = optional_int(value.get("sequence"))
    if sequence is None or sequence < 1:
        raise PaperDaySidecarError("EVENT_LOG_INVALID", f"line {line} has invalid sequence")
    payload = optional_object(value.get("payload"))
    if payload is None:
        raise PaperDaySidecarError("EVENT_LOG_INVALID", f"line {line} has invalid payload")
    return PaperDaySidecarEvent(
        sequence=sequence,
        event_id=required_string(value, "event_id", "EVENT_LOG_INVALID"),
        event_type=required_string(value, "event_type", "EVENT_LOG_INVALID"),
        known_at=required_datetime(value, "known_at", line),
        occurred_at=required_datetime(value, "occurred_at", line),
        payload=payload,
        phase=required_string(value, "phase", "EVENT_LOG_INVALID"),
        severity=required_string(value, "severity", "EVENT_LOG_INVALID"),
        symbol=optional_string(value.get("symbol")),
        correlation_id=optional_string(value.get("correlation_id")),
    )


def required_string(value: Mapping[str, object], key: str, code: str) -> str:
    """读取非空字符串字段。"""

    result = optional_string(value.get(key))
    if result is None:
        raise PaperDaySidecarError(code, f"missing or invalid {key}")
    return result


def required_date(value: Mapping[str, object], key: str, code: str) -> date:
    """读取 ISO 日期字段。"""

    raw = required_string(value, key, code)
    try:
        return date.fromisoformat(raw)
    except ValueError as error:
        raise PaperDaySidecarError(code, f"invalid {key}") from error


def required_datetime(value: Mapping[str, object], key: str, line: int) -> datetime:
    """读取带时区的 ISO 时间，并统一转换为 UTC。"""

    raw = optional_string(value.get(key))
    if raw is None:
        raise PaperDaySidecarError("EVENT_LOG_INVALID", f"line {line} has invalid {key}")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as error:
        raise PaperDaySidecarError(
            "EVENT_LOG_INVALID", f"line {line} has invalid {key}"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PaperDaySidecarError("EVENT_LOG_INVALID", f"line {line} has naive {key}")
    return parsed.astimezone(UTC)


def optional_string(value: object) -> str | None:
    """把非空字符串保留下来，其余值视为缺失。"""

    return value if isinstance(value, str) and value.strip() else None


def optional_int(value: object) -> int | None:
    """读取不带布尔子类语义的整数。"""

    return value if isinstance(value, int) and not isinstance(value, bool) else None


def optional_bool(value: object) -> bool | None:
    """读取布尔字段。"""

    return value if isinstance(value, bool) else None


def optional_decimal(value: object) -> Decimal | None:
    """把有限数值转换为 Decimal，拒绝布尔值、对象和非有限值。"""

    if isinstance(value, bool) or value is None:
        return None
    if not isinstance(value, (str, int, float, Decimal)):
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        return None
    return result if result.is_finite() else None


def optional_object(value: object) -> dict[str, object] | None:
    """读取 JSON 对象并保留其键值。"""

    if not isinstance(value, dict):
        return None
    return cast(dict[str, object], value)


def list_length(value: object) -> int | None:
    """返回 JSON 数组长度；非数组值视为未知。"""

    return len(value) if isinstance(value, list) else None


__all__ = [
    "list_length",
    "optional_bool",
    "optional_decimal",
    "optional_int",
    "optional_object",
    "optional_string",
    "parse_event",
    "read_events",
    "read_json_object",
    "required_date",
    "required_datetime",
    "required_string",
]
