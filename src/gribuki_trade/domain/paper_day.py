"""一次 A 股完整交易时段 PAPER 运行的不可变审计值。

日内运行器有意表示为事件流。``occurred_at`` 描述事实发生的时间，
``known_at`` 则是进程首次可能使用该事实的时点。时点重放始终采用后者，
因此无法用后来才观察到的信息反向影响过去。
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, StrEnum
from typing import TypeAlias, cast

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/+\-]{0,127}$")
_SYMBOL = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class PaperDayPhase(StrEnum):
    """附加到每条 PAPER 日内审计事件的运行阶段。"""

    BOOTSTRAP = "BOOTSTRAP"
    PREOPEN = "PREOPEN"
    OPEN_AUCTION = "OPEN_AUCTION"
    MORNING = "MORNING"
    LUNCH = "LUNCH"
    AFTERNOON = "AFTERNOON"
    CLOSING = "CLOSING"
    POST_CLOSE = "POST_CLOSE"
    TERMINAL = "TERMINAL"


class PaperDaySeverity(StrEnum):
    """供人工和机器路由使用的严重级别；它不是交易评分。"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    NOTICE = "NOTICE"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True, slots=True)
class PaperDayRunManifest:
    """一个交易日的不可变身份与脱敏配置。"""

    run_id: str
    session_date: date
    account_id: str
    config_json: str
    config_sha256: str
    created_at: datetime
    target_hash: str
    initial_cash: Decimal
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.session_date, date) or isinstance(
            self.session_date, datetime
        ):
            raise TypeError("session_date must be a date")
        account_id = _safe_identifier(self.account_id, "account_id")
        config_json = _canonical_object_json(self.config_json, "config_json")
        config_sha256 = _digest(config_json)
        if self.config_sha256 != config_sha256:
            raise ValueError("config_sha256 does not match config_json")
        if not _SHA256.fullmatch(self.target_hash):
            raise ValueError("target_hash must be a lowercase SHA-256 digest")
        created_at = aware_utc(self.created_at, "created_at")
        initial_cash = _positive_decimal(self.initial_cash, "initial_cash")
        if isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ValueError("schema_version must be 1")
        expected_run_id = paper_day_run_id(
            session_date=self.session_date,
            account_id=account_id,
            config_sha256=config_sha256,
        )
        if self.run_id != expected_run_id:
            raise ValueError("run_id does not match the PAPER-day identity")
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "config_json", config_json)
        object.__setattr__(self, "config_sha256", config_sha256)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "initial_cash", initial_cash)

    @classmethod
    def create(
        cls,
        *,
        session_date: date,
        account_id: str,
        config: Mapping[str, object],
        created_at: datetime,
        target_hash: str,
        initial_cash: Decimal,
    ) -> PaperDayRunManifest:
        """冻结配置，并确保每次重启都派生出相同的运行身份。"""

        config_json = paper_day_canonical_json(config)
        config_sha256 = _digest(config_json)
        return cls(
            run_id=paper_day_run_id(
                session_date=session_date,
                account_id=account_id,
                config_sha256=config_sha256,
            ),
            session_date=session_date,
            account_id=account_id,
            config_json=config_json,
            config_sha256=config_sha256,
            created_at=created_at,
            target_hash=target_hash,
            initial_cash=initial_cash,
        )

    @property
    def config(self) -> dict[str, JSONValue]:
        """返回分离的配置文档；调用方无法修改清单。"""

        return _object_from_json(self.config_json, "stored config")


@dataclass(frozen=True, slots=True, init=False)
class NewPaperDayEvent:
    """尚未生成序列与哈希链元数据前，已经校验的事件命令。"""

    run_id: str
    event_key: str
    event_type: str
    phase: PaperDayPhase
    severity: PaperDaySeverity
    occurred_at: datetime
    known_at: datetime
    notification_required: bool
    payload_json: str
    symbol: str | None
    correlation_id: str | None

    def __init__(
        self,
        *,
        run_id: str,
        event_key: str,
        event_type: str,
        phase: PaperDayPhase,
        severity: PaperDaySeverity,
        occurred_at: datetime,
        known_at: datetime,
        notification_required: bool,
        payload: Mapping[str, object] | None = None,
        payload_json: str | None = None,
        symbol: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        if (payload is None) == (payload_json is None):
            raise ValueError("provide exactly one of payload or payload_json")
        frozen_payload = (
            paper_day_canonical_json(payload)
            if payload is not None
            else _canonical_object_json(cast(str, payload_json), "payload_json")
        )
        _set_event_core(
            self,
            run_id=run_id,
            event_key=event_key,
            event_type=event_type,
            phase=phase,
            severity=severity,
            occurred_at=occurred_at,
            known_at=known_at,
            notification_required=notification_required,
            payload_json=frozen_payload,
            symbol=symbol,
            correlation_id=correlation_id,
        )

    @property
    def event_id(self) -> str:
        return paper_day_event_id(run_id=self.run_id, event_key=self.event_key)

    @property
    def payload(self) -> dict[str, JSONValue]:
        return _object_from_json(self.payload_json, "stored payload")


@dataclass(frozen=True, slots=True)
class PaperDayEvent:
    """一条带有确定性身份和防篡改链接的持久事件。"""

    sequence: int
    event_id: str
    run_id: str
    event_key: str
    event_type: str
    phase: PaperDayPhase
    severity: PaperDaySeverity
    occurred_at: datetime
    known_at: datetime
    notification_required: bool
    payload_json: str
    payload_sha256: str
    previous_hash: str | None
    event_hash: str
    symbol: str | None = None
    correlation_id: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int):
            raise TypeError("sequence must be an integer")
        if self.sequence < 1:
            raise ValueError("sequence must be positive")
        _set_event_core(
            self,
            run_id=self.run_id,
            event_key=self.event_key,
            event_type=self.event_type,
            phase=self.phase,
            severity=self.severity,
            occurred_at=self.occurred_at,
            known_at=self.known_at,
            notification_required=self.notification_required,
            payload_json=self.payload_json,
            symbol=self.symbol,
            correlation_id=self.correlation_id,
        )
        expected_id = paper_day_event_id(run_id=self.run_id, event_key=self.event_key)
        if self.event_id != expected_id:
            raise ValueError("event_id does not match the PAPER-day event identity")
        if self.payload_sha256 != _digest(self.payload_json):
            raise ValueError("payload_sha256 does not match payload_json")
        if self.previous_hash is not None and not _SHA256.fullmatch(self.previous_hash):
            raise ValueError("previous_hash must be a lowercase SHA-256 digest")
        if not _SHA256.fullmatch(self.event_hash):
            raise ValueError("event_hash must be a lowercase SHA-256 digest")

    @property
    def payload(self) -> dict[str, JSONValue]:
        return _object_from_json(self.payload_json, "stored payload")


@dataclass(frozen=True, slots=True)
class PaperDayReplay:
    """依据 known-at 时间戳重建的时点不可变视图。"""

    manifest: PaperDayRunManifest
    as_of: datetime
    events: tuple[PaperDayEvent, ...]

    def __post_init__(self) -> None:
        as_of = aware_utc(self.as_of, "as_of")
        if any(event.run_id != self.manifest.run_id for event in self.events):
            raise ValueError("replay events must belong to the manifest run")
        if any(event.known_at > as_of for event in self.events):
            raise ValueError("replay cannot include events learned after as_of")
        sequences = tuple(event.sequence for event in self.events)
        if sequences != tuple(sorted(sequences)) or len(sequences) != len(set(sequences)):
            raise ValueError("replay events must have unique ascending sequences")
        object.__setattr__(self, "as_of", as_of)

    @property
    def latest_phase(self) -> PaperDayPhase | None:
        return None if not self.events else self.events[-1].phase

    @property
    def notification_events(self) -> tuple[PaperDayEvent, ...]:
        return tuple(event for event in self.events if event.notification_required)


def paper_day_run_id(
    *,
    session_date: date,
    account_id: str,
    config_sha256: str,
) -> str:
    """派生恢复时使用的稳定交易时段、账户与配置身份。"""

    if not isinstance(session_date, date) or isinstance(session_date, datetime):
        raise TypeError("session_date must be a date")
    account_id = _safe_identifier(account_id, "account_id")
    if not _SHA256.fullmatch(config_sha256):
        raise ValueError("config_sha256 must be a lowercase SHA-256 digest")
    material = (
        f"paper-day-run@1\0{session_date.isoformat()}\0{account_id}\0{config_sha256}"
    )
    return "paper-day-" + _digest(material)[:40]


def paper_day_event_id(*, run_id: str, event_key: str) -> str:
    """在不纳入可变载荷内容的情况下派生事件身份。"""

    run_id = _safe_identifier(run_id, "run_id")
    event_key = _safe_identifier(event_key, "event_key")
    return "pde-" + _digest(f"paper-day-event@1\0{run_id}\0{event_key}")[:40]


def paper_day_target_hash(*, channel: str, target_kind: str, target_id: str) -> str:
    """为通知路由生成指纹，使清单永不保留其实际地址。"""

    channel = _safe_identifier(channel, "channel")
    target_kind = _safe_identifier(target_kind, "target_kind")
    target_id = target_id.strip()
    if not target_id:
        raise ValueError("target_id must not be empty")
    return _digest(f"paper-day-target@1\0{channel}\0{target_kind}\0{target_id}")


def paper_day_canonical_json(document: Mapping[str, object]) -> str:
    """确定性编码受支持的审计数据，并拒绝非有限值。"""

    if not isinstance(document, Mapping):
        raise TypeError("PAPER-day document must be a mapping")
    normalized = _normalize_json(document)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def paper_day_document_sha256(document: Mapping[str, object]) -> str:
    return _digest(paper_day_canonical_json(document))


def aware_utc(value: datetime, field_name: str = "timestamp") -> datetime:
    """把带时区的时间戳规范化为 UTC。"""

    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _set_event_core(
    target: NewPaperDayEvent | PaperDayEvent,
    *,
    run_id: str,
    event_key: str,
    event_type: str,
    phase: PaperDayPhase,
    severity: PaperDaySeverity,
    occurred_at: datetime,
    known_at: datetime,
    notification_required: bool,
    payload_json: str,
    symbol: str | None,
    correlation_id: str | None,
) -> None:
    run_id = _safe_identifier(run_id, "run_id")
    event_key = _safe_identifier(event_key, "event_key")
    event_type = event_type.strip()
    if not event_type or len(event_type) > 100:
        raise ValueError("event_type must be a non-empty string of at most 100 characters")
    try:
        phase = PaperDayPhase(phase)
        severity = PaperDaySeverity(severity)
    except ValueError as error:
        raise ValueError("unsupported PAPER-day phase or severity") from error
    occurred_at = aware_utc(occurred_at, "occurred_at")
    known_at = aware_utc(known_at, "known_at")
    if occurred_at > known_at:
        raise ValueError("occurred_at must not follow known_at")
    if not isinstance(notification_required, bool):
        raise TypeError("notification_required must be bool")
    payload_json = _canonical_object_json(payload_json, "payload_json")
    if symbol is not None:
        symbol = symbol.strip().upper()
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError("symbol must use the canonical 000001.SZ form")
    if correlation_id is not None:
        correlation_id = _safe_identifier(correlation_id, "correlation_id")
    object.__setattr__(target, "run_id", run_id)
    object.__setattr__(target, "event_key", event_key)
    object.__setattr__(target, "event_type", event_type)
    object.__setattr__(target, "phase", phase)
    object.__setattr__(target, "severity", severity)
    object.__setattr__(target, "occurred_at", occurred_at)
    object.__setattr__(target, "known_at", known_at)
    object.__setattr__(target, "notification_required", notification_required)
    object.__setattr__(target, "payload_json", payload_json)
    object.__setattr__(target, "symbol", symbol)
    object.__setattr__(target, "correlation_id", correlation_id)


def _normalize_json(value: object) -> JSONValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("PAPER-day documents must not contain non-finite floats")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("PAPER-day documents must not contain non-finite decimals")
        return format(value, "f")
    if isinstance(value, datetime):
        return aware_utc(value, "document datetime").isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return _normalize_json(value.value)
    if isinstance(value, Mapping):
        normalized: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError("PAPER-day document keys must be non-empty strings")
            normalized[key] = _normalize_json(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_json(item) for item in value]
    raise ValueError("PAPER-day document contains an unsupported value type")


def _canonical_object_json(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, ValueError) as error:
        raise ValueError(f"{field_name} must contain valid JSON") from error
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must contain a JSON object")
    canonical = paper_day_canonical_json(cast(dict[str, object], parsed))
    if canonical != value:
        raise ValueError(f"{field_name} must use canonical JSON encoding")
    return canonical


def _object_from_json(value: str, field_name: str) -> dict[str, JSONValue]:
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, ValueError) as error:  # pragma: no cover - constructor guards
        raise ValueError(f"{field_name} is invalid") from error
    if not isinstance(parsed, dict):  # pragma: no cover - constructor guards
        raise ValueError(f"{field_name} is invalid")
    return cast(dict[str, JSONValue], parsed)


def _positive_decimal(value: object, field_name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be a Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{field_name} must be positive and finite")
    return value


def _safe_identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not _IDENTIFIER.fullmatch(normalized):
        raise ValueError(f"{field_name} must be a safe non-empty identifier")
    return normalized


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
