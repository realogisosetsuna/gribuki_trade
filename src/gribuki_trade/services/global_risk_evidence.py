"""Convert official global-risk observations into bounded research evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from gribuki_trade.analysis.schemas import EvidenceItem
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.ports.global_risk import VIXDailyHistory


@dataclass(frozen=True, slots=True)
class GlobalRiskEvidenceBundle:
    items: tuple[EvidenceItem, ...]
    references: tuple[EvidenceReference, ...]
    report_lines: tuple[str, ...]

    def __post_init__(self) -> None:
        item_ids = tuple(item.evidence_id for item in self.items)
        if item_ids != tuple(item.evidence_id for item in self.references):
            raise ValueError("global-risk evidence items and references must align")


def build_vix_evidence(
    history: VIXDailyHistory,
    *,
    as_of: datetime,
) -> GlobalRiskEvidenceBundle:
    """Build one official Cboe VIX EOD observation with a hard PIT cutoff."""

    _require_aware(as_of)
    latest = history.bars[-1]
    if history.as_of > as_of or history.meta.fetched_at > as_of:
        raise ValueError("VIX history was fetched after as_of")
    if latest.available_at > as_of:
        raise ValueError("VIX session was unavailable at as_of")
    payload = {
        "schema": "gribuki.global_risk.vix_eod",
        "schema_version": 1,
        "session_date": latest.session_date.isoformat(),
        "open": _decimal(latest.open),
        "high": _decimal(latest.high),
        "low": _decimal(latest.low),
        "close": _decimal(latest.close),
        "available_at": latest.available_at.astimezone(UTC).isoformat(timespec="seconds"),
        "fetched_at": history.meta.fetched_at.astimezone(UTC).isoformat(timespec="seconds"),
        "stale": history.meta.stale,
        "skipped_invalid_ohlc_rows": history.meta.skipped_invalid_ohlc_rows,
        "document_sha256": history.meta.content_sha256,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    content_hash = hashlib.sha256(encoded).hexdigest()
    evidence_id = hashlib.sha256(
        f"gribuki.global_risk.vix_eod.v1\0{content_hash}".encode()
    ).hexdigest()
    item = EvidenceItem(
        evidence_id=evidence_id,
        publisher=history.meta.source_id,
        source_tier=1,
        published_at=latest.available_at,
        first_seen_at=history.meta.fetched_at,
        title=f"Cboe官方VIX日线｜{latest.session_date.isoformat()}",
        excerpt=(
            f"Cboe官方VIX EOD：交易日={latest.session_date.isoformat()}；"
            f"开={_three(latest.open)}；高={_three(latest.high)}；"
            f"低={_three(latest.low)}；收={_three(latest.close)}；"
            "这是波动率指数观测，不是可直接交易价格；"
            f"官方历史文件中隔离了{history.meta.skipped_invalid_ohlc_rows}条"
            "OHLC包络异常旧记录，未修改原始值。"
        ),
        canonical_url=history.meta.source_url,
        content_hash=content_hash,
    )
    reference = EvidenceReference(
        evidence_id=item.evidence_id,
        title=item.title,
        canonical_url=item.canonical_url,
        published_at=item.published_at,
        first_seen_at=item.first_seen_at,
        source_tier=item.source_tier,
    )
    status = "陈旧" if history.meta.stale else "正常"
    return GlobalRiskEvidenceBundle(
        items=(item,),
        references=(reference,),
        report_lines=(
            f"Cboe官方VIX日线（{latest.session_date.isoformat()}）："
            f"开={_three(latest.open)}，高={_three(latest.high)}，"
            f"低={_three(latest.low)}，收={_three(latest.close)}；"
            f"美国市场完成时点={latest.available_at.isoformat(timespec='seconds')}；"
            f"状态={status}；历史异常行隔离="
            f"{history.meta.skipped_invalid_ohlc_rows}条（不修改原值）。"
            "A股收盘报告仅使用此前已完成的美国交易日。",
        ),
    )


def _decimal(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if normalized.is_zero() else format(normalized, "f")


def _three(value: Decimal) -> str:
    return f"{value:.3f}"


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
