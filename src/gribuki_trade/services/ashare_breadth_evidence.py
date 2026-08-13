"""Pure conversion of an A-share breadth snapshot into auditable evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from gribuki_trade.analysis.schemas import EvidenceItem
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.ports.ashare_breadth import AShareBreadthSnapshot, AShareExchange

_SCHEMA_VERSION = 1
_SOURCE_TIER = 2
_EXCHANGE_LABEL = {
    AShareExchange.SHANGHAI: "上海",
    AShareExchange.SHENZHEN: "深圳",
    AShareExchange.BEIJING: "北京",
}


@dataclass(frozen=True, slots=True)
class AShareBreadthEvidenceBundle:
    items: tuple[EvidenceItem, ...]
    references: tuple[EvidenceReference, ...]
    report_lines: tuple[str, ...]

    def __post_init__(self) -> None:
        item_ids = tuple(item.evidence_id for item in self.items)
        reference_ids = tuple(item.evidence_id for item in self.references)
        if item_ids != reference_ids:
            raise ValueError("breadth evidence items and references must align")
        if len(item_ids) != len(set(item_ids)):
            raise ValueError("breadth evidence IDs must be unique")


def build_ashare_breadth_evidence(
    snapshot: AShareBreadthSnapshot | None,
    as_of: datetime,
) -> AShareBreadthEvidenceBundle:
    """Build deterministic breadth evidence with a strict first-seen cutoff."""

    _require_aware(as_of, "as_of")
    if snapshot is None:
        return AShareBreadthEvidenceBundle(
            items=(),
            references=(),
            report_lines=(
                "A股市场宽度：缺失；未用零值、指数涨跌或局部样本替代全市场统计。",
            ),
        )
    if snapshot.meta.available_at.astimezone(UTC) > as_of.astimezone(UTC):
        raise ValueError("breadth first-seen time must not be after as_of")

    item = _evidence_item(snapshot)
    return AShareBreadthEvidenceBundle(
        items=(item,),
        references=(_reference(item),),
        report_lines=_report_lines(snapshot, item.title),
    )


def _evidence_item(snapshot: AShareBreadthSnapshot) -> EvidenceItem:
    payload = {
        "session_date": snapshot.session_date.isoformat(),
        "received_count": snapshot.received_count,
        "eligible_count": snapshot.eligible_count,
        "minimum_eligible_count": snapshot.minimum_eligible_count,
        "expected_count": snapshot.expected_count,
        "coverage_percent": _canonical_optional(snapshot.coverage_percent),
        "duplicate_count": snapshot.duplicate_count,
        "excluded_non_equity_count": snapshot.excluded_non_equity_count,
        "non_trading_count": snapshot.non_trading_count,
        "included_exchanges": [
            {
                "exchange": item.exchange.value,
                "eligible_count": item.eligible_count,
            }
            for item in snapshot.included_exchanges
        ],
        "advancing_count": snapshot.advancing_count,
        "declining_count": snapshot.declining_count,
        "flat_count": snapshot.flat_count,
        "advance_decline_ratio": _canonical_optional(snapshot.advance_decline_ratio),
        "advancing_amount_share_percent": _canonical_optional(
            snapshot.advancing_amount_share_percent
        ),
        "equal_weight_mean_change_percent": _canonical(
            snapshot.equal_weight_mean_change_percent
        ),
        "median_change_percent": _canonical(snapshot.median_change_percent),
        "total_amount_cny": _canonical(snapshot.total_amount_cny),
        "limit_up_count": snapshot.limit_up_count,
        "limit_down_count": snapshot.limit_down_count,
        "meta": {
            "source_id": snapshot.meta.source_id,
            "source_url": snapshot.meta.source_url,
            "observed_at": snapshot.meta.observed_at.astimezone(UTC).isoformat(
                timespec="seconds"
            ),
            "available_at": snapshot.meta.available_at.astimezone(UTC).isoformat(
                timespec="seconds"
            ),
            "fetched_at": snapshot.meta.fetched_at.astimezone(UTC).isoformat(
                timespec="seconds"
            ),
            "stale": snapshot.meta.stale,
            "degraded": snapshot.meta.degraded,
            "fallback_used": snapshot.meta.fallback_used,
            "warnings": sorted(set(snapshot.meta.warnings)),
        },
    }
    envelope = {
        "schema": "gribuki.ashare_breadth.close",
        "schema_version": _SCHEMA_VERSION,
        **payload,
    }
    content_hash = _hash(envelope)
    evidence_id = hashlib.sha256(
        f"gribuki.ashare_breadth.close.v{_SCHEMA_VERSION}\0{content_hash}".encode()
    ).hexdigest()
    return EvidenceItem(
        evidence_id=evidence_id,
        publisher=snapshot.meta.source_id,
        source_tier=_SOURCE_TIER,
        published_at=snapshot.meta.observed_at.astimezone(UTC),
        first_seen_at=snapshot.meta.available_at.astimezone(UTC),
        title=f"A股全市场宽度｜沪深京收盘快照 {snapshot.session_date.isoformat()}",
        excerpt=(
            "二手公开网页收盘快照的结构化宽度统计；"
            f"结构化内容={json.dumps(payload, ensure_ascii=False, sort_keys=True)}；"
            "不代表交易所全量行情、因果关系或交易指令。"
        ),
        canonical_url=snapshot.meta.source_url,
        content_hash=content_hash,
    )


def _report_lines(
    snapshot: AShareBreadthSnapshot,
    reference: str,
) -> tuple[str, ...]:
    exchange_counts = "、".join(
        f"{_EXCHANGE_LABEL[item.exchange]}={item.eligible_count}家"
        for item in snapshot.included_exchanges
    )
    coverage = (
        "独立预期上市数量缺失，因此不报告精确覆盖率；"
        f"仅通过有效样本不少于{snapshot.minimum_eligible_count}家的门禁"
        if snapshot.expected_count is None
        else (
            f"独立预期数量={snapshot.expected_count}家，"
            f"覆盖率={_optional(snapshot.coverage_percent, '%')}"
        )
    )
    status = "陈旧" if snapshot.meta.stale else "正常"
    if snapshot.meta.degraded:
        status += "、降级"
    return (
        "A股市场宽度：二手公开网页研究快照，仅描述横截面，不用于因果归因或直接下单。",
        f"样本（{snapshot.session_date.isoformat()}）：收到={snapshot.received_count}行，"
        f"有效A股={snapshot.eligible_count}家（{exchange_counts}），重复="
        f"{snapshot.duplicate_count}行，非A股排除={snapshot.excluded_non_equity_count}行，"
        f"停牌/无完整行情={snapshot.non_trading_count}家；{coverage}。",
        f"涨跌分布：上涨={snapshot.advancing_count}家，下跌={snapshot.declining_count}家，"
        f"平盘={snapshot.flat_count}家，涨跌家数比="
        f"{_optional(snapshot.advance_decline_ratio)}。",
        f"横截面收益：等权平均涨跌幅={_three(snapshot.equal_weight_mean_change_percent)}%，"
        f"中位涨跌幅={_three(snapshot.median_change_percent)}%；上涨股票成交额占比="
        f"{_optional(snapshot.advancing_amount_share_percent, '%')}，"
        f"有效样本成交额合计={_three(snapshot.total_amount_cny / Decimal('100000000'))}亿元。",
        "涨停/跌停家数：缺失；在板块、ST、上市年龄与特殊交易状态限价规则完成审计前，"
        "不以固定百分比近似。",
        f"来源：{snapshot.meta.source_id}；证据引用：{reference}；链接见文末证据索引；"
        f"抓取={snapshot.meta.fetched_at.isoformat(timespec='seconds')}；"
        f"首次可用={snapshot.meta.available_at.isoformat(timespec='seconds')}；"
        f"回退={'是' if snapshot.meta.fallback_used else '否'}；状态={status}；"
        "PIT约束=仅纳入首次可用时间不晚于报告as_of的观测。",
    )


def _reference(item: EvidenceItem) -> EvidenceReference:
    return EvidenceReference(
        evidence_id=item.evidence_id,
        title=item.title,
        canonical_url=item.canonical_url,
        published_at=item.published_at,
        first_seen_at=item.first_seen_at,
        source_tier=item.source_tier,
    )


def _hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical(value: Decimal) -> str:
    return "0" if value.is_zero() else format(value.normalize(), "f")


def _canonical_optional(value: Decimal | None) -> str | None:
    return None if value is None else _canonical(value)


def _three(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP), "f")


def _optional(value: Decimal | None, suffix: str = "") -> str:
    return "缺失" if value is None else f"{_three(value)}{suffix}"


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
