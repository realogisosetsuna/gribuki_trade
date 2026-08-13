"""Convert official SSE ETF-share and option-risk observations into evidence.

The converter is deliberately descriptive.  An ETF total-share observation is
not NAV, assets under management, or a subscription/redemption flow.  Likewise,
the option-risk snapshot is not averaged into a volatility signal: ATM IV,
skew, and term structure require an explicit contract-selection methodology.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from gribuki_trade.analysis.schemas import EvidenceItem
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.ports.ashare_derivatives import (
    SSEETFShareObservation,
    SSEOfficialSourceMeta,
    SSEOptionRiskSnapshot,
)

_SCHEMA_VERSION = 1
_SOURCE_TIER = 1


@dataclass(frozen=True, slots=True)
class AShareDerivativesEvidenceBundle:
    """Aligned evidence, references, and human-readable report lines."""

    items: tuple[EvidenceItem, ...]
    references: tuple[EvidenceReference, ...]
    report_lines: tuple[str, ...]

    def __post_init__(self) -> None:
        item_ids = tuple(item.evidence_id for item in self.items)
        reference_ids = tuple(item.evidence_id for item in self.references)
        if item_ids != reference_ids:
            raise ValueError("A-share derivative evidence items and references must align")
        if len(item_ids) != len(set(item_ids)):
            raise ValueError("A-share derivative evidence IDs must be unique")


def build_ashare_derivatives_evidence(
    etf_shares: SSEETFShareObservation | None,
    option_risk: SSEOptionRiskSnapshot | None,
    *,
    as_of: datetime,
) -> AShareDerivativesEvidenceBundle:
    """Build deterministic, point-in-time official SSE evidence.

    ``None`` means unavailable and remains unavailable.  The function performs
    no network I/O, interpolation, imputation, or directional scoring.
    """

    _require_aware(as_of, "as_of")
    cutoff = as_of.astimezone(UTC)
    _validate_pit(etf_shares, option_risk, cutoff)

    items: list[EvidenceItem] = []
    lines: list[str] = []

    if etf_shares is None:
        lines.append(
            "上交所ETF官方份额：缺失；未使用NAV、AUM、成交额或估算值替代。"
        )
    else:
        item = _etf_share_item(etf_shares)
        items.append(item)
        lines.extend(_etf_share_lines(etf_shares, item.title))

    if option_risk is None:
        lines.append(
            "上交所期权风险参数：缺失；未构造合约统计、隐含波动率或替代指标。"
        )
    else:
        item = _option_risk_item(option_risk)
        items.append(item)
        lines.extend(_option_risk_lines(option_risk, item.title))

    evidence = tuple(items)
    return AShareDerivativesEvidenceBundle(
        items=evidence,
        references=tuple(_reference(item) for item in evidence),
        report_lines=tuple(lines),
    )


def _validate_pit(
    etf_shares: SSEETFShareObservation | None,
    option_risk: SSEOptionRiskSnapshot | None,
    cutoff: datetime,
) -> None:
    observations: tuple[
        tuple[str, SSEETFShareObservation | SSEOptionRiskSnapshot], ...
    ] = tuple(
        (label, value)
        for label, value in (
            ("ETF share", etf_shares),
            ("option risk", option_risk),
        )
        if value is not None
    )
    for label, value in observations:
        if value.meta.fetched_at.astimezone(UTC) > cutoff:
            raise ValueError(f"{label} fetched_at must not be after as_of")


def _etf_share_item(value: SSEETFShareObservation) -> EvidenceItem:
    payload: dict[str, Any] = {
        "symbol": value.symbol.strip().upper(),
        "name": value.name.strip(),
        "expanded_name": None if value.expanded_name is None else value.expanded_name.strip(),
        "etf_type": None if value.etf_type is None else value.etf_type.strip(),
        "total_shares_ten_thousands": _canonical(value.total_shares_ten_thousands),
        "total_shares": _canonical(value.total_shares),
        "raw_total_shares": value.raw_total_shares,
        "meta": _meta_payload(value.meta),
    }
    title = (
        f"上交所官方ETF总份额｜{value.expanded_name or value.name}｜"
        f"{value.symbol}｜{value.meta.observed_date.isoformat()}"
    )
    excerpt = (
        f"请求日={value.meta.requested_date.isoformat()}；"
        f"实际统计日={value.meta.observed_date.isoformat()}；"
        f"官方总份额={_three_grouped(value.total_shares)}份；"
        f"原始口径={_three_grouped(value.total_shares_ten_thousands)}万份；"
        f"日期状态={_date_status(value.meta)}。该值不是NAV、AUM或申购赎回净流量。"
    )
    return _item("etf-shares", title, excerpt, payload, value.meta)


def _option_risk_item(value: SSEOptionRiskSnapshot) -> EvidenceItem:
    calls, puts, positive_iv, zero_iv = _option_counts(value)
    contracts = [
        {
            "security_id": contract.security_id,
            "contract_id": contract.contract_id,
            "contract_symbol": contract.contract_symbol,
            "contract_type": contract.contract_type,
            "delta": _canonical(contract.delta),
            "theta": _canonical(contract.theta),
            "gamma": _canonical(contract.gamma),
            "vega": _canonical(contract.vega),
            "rho": _canonical(contract.rho),
            "implied_volatility": _canonical(contract.implied_volatility),
            "raw_fields": sorted(contract.raw_fields),
        }
        for contract in sorted(
            value.contracts,
            key=lambda contract: (contract.security_id, contract.contract_id),
        )
    ]
    payload: dict[str, Any] = {
        "underlying_symbol": value.underlying_symbol.strip().upper(),
        "underlying_name": value.underlying_name.strip(),
        "contract_count": len(value.contracts),
        "call_count": calls,
        "put_count": puts,
        "positive_iv_count": positive_iv,
        "zero_iv_count": zero_iv,
        "contracts": contracts,
        "meta": _meta_payload(value.meta),
    }
    title = (
        f"上交所官方期权收盘风险参数｜{value.underlying_name}｜"
        f"{value.underlying_symbol}｜{value.meta.observed_date.isoformat()}"
    )
    excerpt = (
        f"请求日={value.meta.requested_date.isoformat()}；"
        f"实际交易日={value.meta.observed_date.isoformat()}；"
        f"总合约={len(value.contracts)}，认购={calls}，认沽={puts}，"
        f"正隐含波动率={positive_iv}，官方零隐含波动率={zero_iv}。"
        "官方零值原样保留；当前不派生ATM IV、skew或term structure，"
        "也不以全部合约简单平均代替这些指标。"
    )
    return _item("option-risk", title, excerpt, payload, value.meta)


def _etf_share_lines(
    value: SSEETFShareObservation,
    reference_title: str,
) -> tuple[str, ...]:
    expanded = "" if value.expanded_name is None else f"，全称={value.expanded_name}"
    etf_type = "缺失" if value.etf_type is None else value.etf_type
    return (
        f"上交所ETF官方份额：{value.name}（{value.symbol}{expanded}），"
        f"类型={etf_type}；请求日={value.meta.requested_date.isoformat()}，"
        f"实际统计日={value.meta.observed_date.isoformat()}，"
        f"日期状态={_date_status(value.meta)}。",
        f"官方总份额={_three_grouped(value.total_shares)}份；"
        f"原始口径={_three_grouped(value.total_shares_ten_thousands)}万份。"
        "这是结算后总份额水平，不是NAV、AUM，也不是当日申购赎回净流量；"
        "单日份额水平不能推导资金流向。",
        _source_line(value.meta, reference_title),
    )


def _option_risk_lines(
    value: SSEOptionRiskSnapshot,
    reference_title: str,
) -> tuple[str, ...]:
    calls, puts, positive_iv, zero_iv = _option_counts(value)
    return (
        f"上交所期权官方收盘风险参数：{value.underlying_name}"
        f"（{value.underlying_symbol}）；请求日={value.meta.requested_date.isoformat()}，"
        f"实际交易日={value.meta.observed_date.isoformat()}，"
        f"日期状态={_date_status(value.meta)}。",
        f"合约覆盖：总合约={len(value.contracts)}，认购={calls}，认沽={puts}；"
        f"隐含波动率字段：正值={positive_iv}，官方零值={zero_iv}。"
        "零值保留官方原值，未删除、改写或填充。",
        "指标边界：当前不派生ATM IV、波动率偏度（skew）或期限结构"
        "（term structure），也不将全部合约简单平均冒充上述指标；"
        "后续计算须先明确到期日、行权价、流动性和ATM合约选择规则。",
        _source_line(value.meta, reference_title),
    )


def _option_counts(value: SSEOptionRiskSnapshot) -> tuple[int, int, int, int]:
    calls = sum(contract.contract_type == "认购" for contract in value.contracts)
    puts = sum(contract.contract_type == "认沽" for contract in value.contracts)
    positive_iv = sum(contract.implied_volatility > 0 for contract in value.contracts)
    zero_iv = sum(contract.implied_volatility == 0 for contract in value.contracts)
    return calls, puts, positive_iv, zero_iv


def _item(
    kind: str,
    title: str,
    excerpt: str,
    payload: dict[str, Any],
    meta: SSEOfficialSourceMeta,
) -> EvidenceItem:
    envelope = {
        "schema": f"gribuki.ashare_derivatives.{kind}",
        "schema_version": _SCHEMA_VERSION,
        "title": title,
        "excerpt": excerpt,
        **payload,
    }
    content_hash = _hash(envelope)
    evidence_id = hashlib.sha256(
        f"gribuki.ashare_derivatives.{kind}.v{_SCHEMA_VERSION}\0{content_hash}".encode()
    ).hexdigest()
    return EvidenceItem(
        evidence_id=evidence_id,
        publisher=meta.source_id,
        source_tier=_SOURCE_TIER,
        published_at=meta.available_at.astimezone(UTC),
        first_seen_at=meta.fetched_at.astimezone(UTC),
        title=title,
        excerpt=excerpt,
        canonical_url=meta.source_url,
        content_hash=content_hash,
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


def _meta_payload(meta: SSEOfficialSourceMeta) -> dict[str, Any]:
    return {
        "source_id": meta.source_id,
        "source_url": meta.source_url,
        "requested_date": meta.requested_date.isoformat(),
        "observed_date": meta.observed_date.isoformat(),
        "available_at": meta.available_at.astimezone(UTC).isoformat(timespec="seconds"),
        "fetched_at": meta.fetched_at.astimezone(UTC).isoformat(timespec="seconds"),
        "exact_date_match": meta.exact_date_match,
        "latest_available_fallback": meta.latest_available_fallback,
        "source_content_sha256": meta.content_sha256,
        "warnings": sorted(set(meta.warnings)),
    }


def _date_status(meta: SSEOfficialSourceMeta) -> str:
    return "精确日期匹配" if meta.exact_date_match else "最新可用日期回退"


def _source_line(meta: SSEOfficialSourceMeta, reference_title: str) -> str:
    return (
        f"来源：{meta.source_id}；证据标题：{reference_title}；链接见文末证据索引；"
        f"首次抓取={meta.fetched_at.isoformat(timespec='seconds')}。"
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


def _three_grouped(value: Decimal) -> str:
    rounded = value.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    return f"{rounded:,.3f}"


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
