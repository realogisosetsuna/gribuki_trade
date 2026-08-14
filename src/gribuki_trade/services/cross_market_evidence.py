"""将跨市场观测纯转换为可审计证据。

本模块不执行 I/O，也不作方向或因果推断。它把每个报价转换为内容寻址证据项，加入一个
明确的覆盖率证据项（包括已请求但缺失的序列），并准备数值保留三位小数的紧凑中文报告行。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any
from zoneinfo import ZoneInfo

from gribuki_trade.analysis.schemas import EvidenceItem
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.ports.cross_market import (
    CrossMarketMissingItem,
    CrossMarketQuote,
    CrossMarketSegment,
    CrossMarketSnapshot,
)

_SCHEMA_VERSION = 1
_SOURCE_TIER = 2
_CANONICAL_URL = "https://quote.eastmoney.com/center/gridlist.html#global_qtzs"
_SEGMENT_LABELS = {
    CrossMarketSegment.A_SHARE: "A股",
    CrossMarketSegment.HONG_KONG: "港股",
    CrossMarketSegment.ASIA_PACIFIC: "亚太市场",
    CrossMarketSegment.UNITED_STATES: "美股",
    CrossMarketSegment.DOLLAR_COMMODITY_RISK: "美元、商品与风险指标",
}
_SEGMENT_ORDER = {segment: index for index, segment in enumerate(CrossMarketSegment)}


@dataclass(frozen=True, slots=True)
class CrossMarketEvidenceBundle:
    """严格由单个快照派生的证据与展示视图。"""

    items: tuple[EvidenceItem, ...]
    references: tuple[EvidenceReference, ...]
    report_lines: tuple[str, ...]
    quote_evidence_ids: tuple[str, ...]
    coverage_evidence_id: str

    def __post_init__(self) -> None:
        item_ids = tuple(item.evidence_id for item in self.items)
        reference_ids = tuple(reference.evidence_id for reference in self.references)
        if item_ids != reference_ids:
            raise ValueError("evidence items and references must be aligned")
        expected_ids = (*self.quote_evidence_ids, self.coverage_evidence_id)
        if item_ids != expected_ids:
            raise ValueError("bundle evidence ID partitions are inconsistent")
        if len(item_ids) != len(set(item_ids)):
            raise ValueError("bundle evidence IDs must be unique")


def build_cross_market_evidence(
    snapshot: CrossMarketSnapshot,
) -> CrossMarketEvidenceBundle:
    """构建确定性报价证据、覆盖率证据与报告行。

    输入快照中报价及缺失项的顺序不具语义。转换会先按板块/标的稳定排序，因此等价观测
    会产生相同标识与输出顺序。
    """

    quotes = tuple(sorted(snapshot.quotes, key=_quote_sort_key))
    missing = tuple(sorted(snapshot.missing, key=_missing_sort_key))
    quote_items = tuple(_quote_evidence_item(quote) for quote in quotes)
    coverage_item = _coverage_evidence_item(snapshot, quotes, missing)
    items = (*quote_items, coverage_item)
    return CrossMarketEvidenceBundle(
        items=items,
        references=tuple(_reference(item) for item in items),
        report_lines=format_cross_market_report_lines(snapshot),
        quote_evidence_ids=tuple(item.evidence_id for item in quote_items),
        coverage_evidence_id=coverage_item.evidence_id,
    )


def format_cross_market_report_lines(
    snapshot: CrossMarketSnapshot,
) -> tuple[str, ...]:
    """返回数值保留三位小数的确定性中文观测行。"""

    quotes = tuple(sorted(snapshot.quotes, key=_quote_sort_key))
    missing = tuple(sorted(snapshot.missing, key=_missing_sort_key))
    lines = [
        "跨市场快照：只陈列同一采集批次的市场观测，不解释市场之间的相互关系。",
        f"采集时间：{_timestamp(snapshot.fetched_at)}；来源：{snapshot.provider}；"
        f"整体状态：{'降级' if snapshot.degraded else '正常'}。",
    ]
    for segment in CrossMarketSegment:
        segment_quotes = tuple(quote for quote in quotes if quote.segment is segment)
        segment_missing = tuple(item for item in missing if item.segment is segment)
        if not segment_quotes and not segment_missing:
            continue
        lines.append(f"{_SEGMENT_LABELS[segment]}：")
        lines.extend(f"- {_quote_report_text(quote)}" for quote in segment_quotes)
        lines.extend(f"- {_missing_report_text(item)}" for item in segment_missing)
    if not missing:
        lines.append("缺失覆盖：无；本次配置的观察项均有返回记录。")
    else:
        lines.append(
            f"缺失覆盖：{len(missing)} 项；缺失仅表示本次来源未返回，未填入替代值。"
        )
    return tuple(lines)


def _quote_evidence_item(quote: CrossMarketQuote) -> EvidenceItem:
    payload = _quote_payload(quote)
    content_hash = _content_hash(payload)
    evidence_id = _evidence_id("quote", content_hash)
    published_at = min(
        quote.local_quote_time.astimezone(UTC),
        quote.fetched_at.astimezone(UTC),
    )
    return EvidenceItem(
        evidence_id=evidence_id,
        publisher=quote.provider,
        source_tier=_SOURCE_TIER,
        published_at=published_at,
        first_seen_at=quote.fetched_at.astimezone(UTC),
        title=f"跨市场快照｜{_SEGMENT_LABELS[quote.segment]}｜{quote.display_name}",
        excerpt=_quote_evidence_text(quote),
        canonical_url=_CANONICAL_URL,
        content_hash=content_hash,
    )


def _coverage_evidence_item(
    snapshot: CrossMarketSnapshot,
    quotes: tuple[CrossMarketQuote, ...],
    missing: tuple[CrossMarketMissingItem, ...],
) -> EvidenceItem:
    payload = {
        "degraded": snapshot.degraded,
        "fetched_at": _canonical_timestamp(snapshot.fetched_at),
        "missing": [_missing_payload(item) for item in missing],
        "provider": snapshot.provider.strip(),
        "quote_observations": [_quote_payload(quote) for quote in quotes],
        "schema": "gribuki.cross_market.coverage",
        "schema_version": _SCHEMA_VERSION,
        "warnings": sorted(set(snapshot.warnings)),
    }
    content_hash = _content_hash(payload)
    evidence_id = _evidence_id("coverage", content_hash)
    if missing:
        missing_text = "；".join(_missing_evidence_text(item) for item in missing)
    else:
        missing_text = "无；本次配置的观察项均有返回记录"
    excerpt = (
        f"采集时间={_timestamp(snapshot.fetched_at)}；来源={snapshot.provider}；"
        f"已返回={len(quotes)}项；缺失={len(missing)}项；整体降级="
        f"{_yes_no(snapshot.degraded)}；缺失明细={missing_text}。"
        "缺失仅表示本次来源未返回，未填入替代值。"
    )
    fetched_at = snapshot.fetched_at.astimezone(UTC)
    return EvidenceItem(
        evidence_id=evidence_id,
        publisher=snapshot.provider,
        source_tier=_SOURCE_TIER,
        published_at=fetched_at,
        first_seen_at=fetched_at,
        title="跨市场快照｜数据覆盖记录",
        excerpt=excerpt,
        canonical_url=_CANONICAL_URL,
        content_hash=content_hash,
    )


def _quote_payload(quote: CrossMarketQuote) -> dict[str, Any]:
    local_time = quote.local_quote_time.astimezone(ZoneInfo(quote.local_timezone))
    return {
        "amplitude_percent": _canonical_optional_decimal(quote.amplitude_percent),
        "change_amount": _canonical_optional_decimal(quote.change_amount),
        "change_percent": _canonical_decimal(quote.change_percent),
        "degraded": quote.degraded,
        "display_name": quote.display_name.strip(),
        "fetched_at": _canonical_timestamp(quote.fetched_at),
        "high": _canonical_optional_decimal(quote.high),
        "instrument_id": quote.instrument_id.strip(),
        "last": _canonical_decimal(quote.last),
        "local_quote_time": local_time.isoformat(timespec="seconds"),
        "local_timezone": quote.local_timezone,
        "low": _canonical_optional_decimal(quote.low),
        "open": _canonical_optional_decimal(quote.open),
        "previous_close": _canonical_optional_decimal(quote.previous_close),
        "provider": quote.provider.strip(),
        "provider_code": quote.provider_code.strip(),
        "provider_name": quote.provider_name.strip(),
        "schema": "gribuki.cross_market.quote",
        "schema_version": _SCHEMA_VERSION,
        "segment": quote.segment.value,
        "stale": quote.stale,
        "warnings": sorted(set(quote.warnings)),
    }


def _missing_payload(item: CrossMarketMissingItem) -> dict[str, Any]:
    return {
        "display_name": item.display_name.strip(),
        "expected_codes": sorted(set(alias.strip() for alias in item.expected_codes)),
        "expected_names": sorted(set(alias.strip() for alias in item.expected_names)),
        "instrument_id": item.instrument_id.strip(),
        "reason": " ".join(item.reason.split()),
        "segment": item.segment.value,
    }


def _quote_evidence_text(quote: CrossMarketQuote) -> str:
    return (
        f"观察项={quote.display_name}；内部标识={quote.instrument_id}；"
        f"来源代码={quote.provider_code}；来源名称={quote.provider_name}；"
        f"最新值={_three(quote.last)}；涨跌幅={_signed_three(quote.change_percent)}%；"
        f"涨跌额={_three_optional(quote.change_amount)}；开盘={_three_optional(quote.open)}；"
        f"最高={_three_optional(quote.high)}；最低={_three_optional(quote.low)}；"
        f"昨收={_three_optional(quote.previous_close)}；"
        f"振幅={_three_optional(quote.amplitude_percent, suffix='%')}；"
        f"行情时间={_local_quote_timestamp(quote)}；采集时间={_timestamp(quote.fetched_at)}；"
        f"来源={quote.provider}；陈旧={_yes_no(quote.stale)}；"
        f"降级={_yes_no(quote.degraded)}。"
    )


def _quote_report_text(quote: CrossMarketQuote) -> str:
    return (
        f"{quote.display_name}：{_three(quote.last)}"
        f"（{_signed_three(quote.change_percent)}%）；"
        f"行情时间 {_local_quote_timestamp(quote)}；"
        f"采集时间 {_timestamp(quote.fetched_at)}；来源 {quote.provider}；"
        f"状态 {_quote_status(quote)}。"
    )


def _missing_evidence_text(item: CrossMarketMissingItem) -> str:
    codes = "、".join(sorted(set(item.expected_codes))) or "未配置"
    names = "、".join(sorted(set(item.expected_names))) or "未配置"
    return (
        f"{item.display_name}（内部标识={item.instrument_id}，预期代码={codes}，"
        f"预期名称={names}，记录={item.reason}）"
    )


def _missing_report_text(item: CrossMarketMissingItem) -> str:
    codes = "、".join(sorted(set(item.expected_codes))) or "未配置"
    names = "、".join(sorted(set(item.expected_names))) or "未配置"
    return (
        f"{item.display_name}：本次来源未返回；预期代码 {codes}；"
        f"预期名称 {names}；未填入替代值。"
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


def _content_hash(payload: dict[str, Any]) -> str:
    content = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _evidence_id(kind: str, content_hash: str) -> str:
    material = f"gribuki.cross_market.{kind}.v{_SCHEMA_VERSION}\0{content_hash}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _quote_sort_key(quote: CrossMarketQuote) -> tuple[int, str]:
    return _SEGMENT_ORDER[quote.segment], quote.instrument_id


def _missing_sort_key(item: CrossMarketMissingItem) -> tuple[int, str]:
    return _SEGMENT_ORDER[item.segment], item.instrument_id


def _quote_status(quote: CrossMarketQuote) -> str:
    if quote.stale and quote.degraded:
        return "陈旧、降级"
    if quote.stale:
        return "陈旧"
    if quote.degraded:
        return "降级"
    return "正常"


def _local_quote_timestamp(quote: CrossMarketQuote) -> str:
    local_time = quote.local_quote_time.astimezone(ZoneInfo(quote.local_timezone))
    return f"{local_time.isoformat(timespec='seconds')} [{quote.local_timezone}]"


def _timestamp(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")


def _canonical_decimal(value: Decimal) -> str:
    if value.is_zero():
        return "0"
    return format(value.normalize(), "f")


def _canonical_optional_decimal(value: Decimal | None) -> str | None:
    return None if value is None else _canonical_decimal(value)


def _three(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP), "f")


def _signed_three(value: Decimal) -> str:
    rendered = _three(value)
    return rendered if value < 0 else f"+{rendered}"


def _three_optional(value: Decimal | None, *, suffix: str = "") -> str:
    return "暂无" if value is None else f"{_three(value)}{suffix}"


def _yes_no(value: bool) -> str:
    return "是" if value else "否"
