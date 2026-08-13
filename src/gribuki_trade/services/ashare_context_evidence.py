"""Pure point-in-time conversion of A-share context into research evidence.

The converter performs no I/O and makes no directional inference.  Missing
observations remain missing, vendor order-size labels remain vendor labels,
and an IF basis is calculated only against a same-session CSI 300 spot quote.
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
from gribuki_trade.ports.ashare_context import (
    AShareContextMeta,
    ETFContextSnapshot,
    GovernmentBondYieldCurve,
    IFContractDailyObservation,
    IFDailyContextSnapshot,
    LiquidityContextSnapshot,
    RepoFixingFamily,
    RepoFixingObservation,
)
from gribuki_trade.ports.cross_market import CrossMarketQuote, CrossMarketSnapshot

_SCHEMA_VERSION = 1
_SOURCE_TIER = 2
_CSI_300_URL = "https://quote.eastmoney.com/zs000300.html"


@dataclass(frozen=True, slots=True)
class AShareContextEvidenceBundle:
    """Evidence and human-readable lines derived from one PIT input set."""

    items: tuple[EvidenceItem, ...]
    references: tuple[EvidenceReference, ...]
    report_lines: tuple[str, ...]

    def __post_init__(self) -> None:
        item_ids = tuple(item.evidence_id for item in self.items)
        reference_ids = tuple(item.evidence_id for item in self.references)
        if item_ids != reference_ids:
            raise ValueError("A-share context evidence items and references must align")
        if len(item_ids) != len(set(item_ids)):
            raise ValueError("A-share context evidence IDs must be unique")


def build_ashare_context_evidence(
    etf_context: ETFContextSnapshot | None,
    liquidity: LiquidityContextSnapshot | None,
    if_context: IFDailyContextSnapshot | None,
    cross_market_snapshot: CrossMarketSnapshot | None,
    as_of: datetime,
    *,
    etf_expected: bool = True,
) -> AShareContextEvidenceBundle:
    """Return deterministic evidence after enforcing a hard first-seen cutoff."""

    _require_aware(as_of, "as_of")
    cutoff = as_of.astimezone(UTC)
    _validate_pit(etf_context, liquidity, if_context, cross_market_snapshot, cutoff)

    items: list[EvidenceItem] = []
    lines: list[str] = ["A股补充上下文：以下数值仅为可审计观测，不代表因果或交易指令。"]

    if etf_context is None:
        if etf_expected:
            lines.append("ETF上下文：缺失；未用零值或其他证券替代。")
        else:
            lines.append("ETF上下文：非ETF标的，不适用ETF快照。")
    else:
        item = _etf_item(etf_context)
        items.append(item)
        lines.extend(_etf_lines(etf_context, item.title))

    if liquidity is None:
        lines.append("流动性上下文：缺失；FR、FDR与中债国债收益率曲线均未填值。")
    else:
        for fixing in sorted(liquidity.repo_fixings, key=lambda value: value.family.value):
            item = _fixing_item(fixing)
            items.append(item)
            lines.extend(_fixing_lines(fixing, item.title))
        present_families = {item.family for item in liquidity.repo_fixings}
        for family in RepoFixingFamily:
            if family not in present_families:
                lines.append(f"{family.value}定盘利率：缺失；绝不改称或替代为DR007。")
        if liquidity.government_curve is None:
            lines.append("中债国债收益率曲线：缺失；1年、10年、30年及期限利差均不可计算。")
        else:
            item = _curve_item(liquidity.government_curve)
            items.append(item)
            lines.extend(_curve_lines(liquidity.government_curve, item.title))
        for missing in sorted(liquidity.missing, key=lambda value: value.context_id):
            lines.append(
                f"缺失来源：{missing.display_name}；状态={missing.failure_code.value}；"
                f"预期来源={missing.expected_source}；原因={missing.reason}；未填替代值。"
            )

    spot = _same_session_csi_300(if_context, cross_market_snapshot)
    if if_context is None:
        lines.append("IF期货上下文：缺失；收盘基差及基差率不可计算。")
    else:
        for contract in sorted(if_context.contracts, key=lambda value: value.symbol):
            item = _if_item(contract, spot)
            items.append(item)
            lines.extend(_if_lines(contract, spot, item.title))

    evidence = tuple(items)
    return AShareContextEvidenceBundle(
        items=evidence,
        references=tuple(_reference(item) for item in evidence),
        report_lines=tuple(lines),
    )


def _validate_pit(
    etf: ETFContextSnapshot | None,
    liquidity: LiquidityContextSnapshot | None,
    if_context: IFDailyContextSnapshot | None,
    cross_market: CrossMarketSnapshot | None,
    cutoff: datetime,
) -> None:
    metas: list[AShareContextMeta] = []
    if etf is not None:
        metas.append(etf.meta)
    if liquidity is not None:
        if liquidity.fetched_at.astimezone(UTC) > cutoff:
            raise ValueError("liquidity first-seen time must not be after as_of")
        metas.extend(item.meta for item in liquidity.repo_fixings)
        if liquidity.government_curve is not None:
            metas.append(liquidity.government_curve.meta)
    if if_context is not None:
        if if_context.fetched_at.astimezone(UTC) > cutoff:
            raise ValueError("IF context first-seen time must not be after as_of")
        metas.extend(item.meta for item in if_context.contracts)
    for meta in metas:
        if meta.available_at.astimezone(UTC) > cutoff:
            raise ValueError("context first-seen time must not be after as_of")
    if cross_market is not None:
        if cross_market.fetched_at.astimezone(UTC) > cutoff:
            raise ValueError("cross-market first-seen time must not be after as_of")
        if any(quote.fetched_at.astimezone(UTC) > cutoff for quote in cross_market.quotes):
            raise ValueError("cross-market quote first-seen time must not be after as_of")


def _etf_item(value: ETFContextSnapshot) -> EvidenceItem:
    payload = {
        "amount_cny": _canonical_optional(value.amount_cny),
        "ask1": _canonical_optional(value.ask1),
        "bid1": _canonical_optional(value.bid1),
        "data_date": value.data_date.isoformat(),
        "discount_rate_percent": _canonical_optional(value.discount_rate_percent),
        "iopv": _canonical_optional(value.iopv),
        "last": _canonical(value.last),
        "main_net_inflow_cny": _canonical_optional(value.main_net_inflow_cny),
        "main_net_inflow_percent": _canonical_optional(value.main_net_inflow_percent),
        "name": value.name.strip(),
        "shares_outstanding": _canonical_optional(value.shares_outstanding),
        "symbol": value.symbol.strip().upper(),
        "turnover_percent": _canonical_optional(value.turnover_percent),
        "meta": _meta_payload(value.meta),
    }
    return _item("etf", f"A股ETF上下文｜{value.name}", payload, value.meta)


def _fixing_item(value: RepoFixingObservation) -> EvidenceItem:
    prefix = value.family.value
    payload = {
        "family": prefix,
        "session_date": value.session_date.isoformat(),
        "001_percent": _canonical(value.overnight_percent),
        "007_percent": _canonical(value.seven_day_percent),
        "014_percent": _canonical(value.fourteen_day_percent),
        "meta": _meta_payload(value.meta),
    }
    return _item("repo-fixing", f"中国货币网{prefix}定盘利率", payload, value.meta)


def _curve_item(value: GovernmentBondYieldCurve) -> EvidenceItem:
    payload = {
        "curve_name": value.curve_name.strip(),
        "session_date": value.session_date.isoformat(),
        "points": [
            [_canonical(point.tenor_years), _canonical(point.yield_percent)]
            for point in value.points
        ],
        "meta": _meta_payload(value.meta),
    }
    return _item("government-curve", f"中债曲线｜{value.curve_name}", payload, value.meta)


def _if_item(
    contract: IFContractDailyObservation,
    spot: CrossMarketQuote | None,
) -> EvidenceItem:
    meta: AShareContextMeta = contract.meta
    basis: Decimal | None = None
    basis_percent: Decimal | None = None
    if spot is not None:
        basis = contract.close - spot.last
        basis_percent = basis / spot.last * Decimal("100")
    payload = {
        "symbol": contract.symbol,
        "session_date": contract.session_date.isoformat(),
        "open": _canonical(contract.open),
        "high": _canonical(contract.high),
        "low": _canonical(contract.low),
        "close": _canonical(contract.close),
        "settle": _canonical(contract.settle),
        "previous_settle": _canonical(contract.previous_settle),
        "volume": contract.volume,
        "open_interest": contract.open_interest,
        "turnover_reported": _canonical(contract.turnover_reported),
        "csi_300_spot": None if spot is None else _canonical(spot.last),
        "close_basis": _canonical_optional(basis),
        "close_basis_percent": _canonical_optional(basis_percent),
        "spot_first_seen": None
        if spot is None
        else spot.fetched_at.astimezone(UTC).isoformat(timespec="seconds"),
        "meta": _meta_payload(meta),
    }
    return _item("if-contract", f"中金所IF日行情｜{contract.symbol}", payload, meta)


def _item(
    kind: str,
    title: str,
    payload: dict[str, Any],
    meta: AShareContextMeta,
) -> EvidenceItem:
    envelope = {
        "schema": f"gribuki.ashare_context.{kind}",
        "schema_version": _SCHEMA_VERSION,
        **payload,
    }
    content_hash = _hash(envelope)
    evidence_id = hashlib.sha256(
        f"gribuki.ashare_context.{kind}.v{_SCHEMA_VERSION}\0{content_hash}".encode()
    ).hexdigest()
    return EvidenceItem(
        evidence_id=evidence_id,
        publisher=meta.source_id,
        source_tier=_SOURCE_TIER,
        published_at=meta.observed_at.astimezone(UTC),
        first_seen_at=meta.available_at.astimezone(UTC),
        title=title,
        excerpt=_excerpt(kind, payload),
        canonical_url=meta.source_url,
        content_hash=content_hash,
    )


def _excerpt(kind: str, payload: dict[str, Any]) -> str:
    return (
        f"类型={kind}；结构化内容={json.dumps(payload, ensure_ascii=False, sort_keys=True)}；"
        "数值为观测值，缺失项未填入替代值。"
    )


def _etf_lines(value: ETFContextSnapshot, reference: str) -> tuple[str, ...]:
    return (
        f"ETF快照：{value.name}（{value.symbol}），日期={value.data_date.isoformat()}；"
        f"最新价={_three(value.last)}，IOPV={_optional(value.iopv)}，"
        f"折溢价率={_optional(value.discount_rate_percent, '%')}，"
        f"换手率={_optional(value.turnover_percent, '%')}。",
        f"ETF规模与交易：份额={_optional(value.shares_outstanding, '份')}，"
        f"成交额={_optional(value.amount_cny, '元')}，买一={_optional(value.bid1)}，"
        f"卖一={_optional(value.ask1)}。",
        f"供应商订单规模分类“主力”：净流入={_optional(value.main_net_inflow_cny, '元')}，"
        f"净占比={_optional(value.main_net_inflow_percent, '%')}；该标签不是参与者身份。",
        _source_line(value.meta, reference),
    )


def _fixing_lines(value: RepoFixingObservation, reference: str) -> tuple[str, ...]:
    prefix = value.family.value
    return (
        f"{prefix}定盘利率（{value.session_date.isoformat()}）："
        f"{prefix}001={_three(value.overnight_percent)}%，"
        f"{prefix}007={_three(value.seven_day_percent)}%，"
        f"{prefix}014={_three(value.fourteen_day_percent)}%；"
        "这是定盘利率，绝不是DR007。",
        _source_line(value.meta, reference),
    )


def _curve_lines(value: GovernmentBondYieldCurve, reference: str) -> tuple[str, ...]:
    points = {point.tenor_years: point.yield_percent for point in value.points}
    one = points.get(Decimal("1"))
    ten = points.get(Decimal("10"))
    thirty = points.get(Decimal("30"))
    spread_10_1 = None if one is None or ten is None else ten - one
    spread_30_10 = None if ten is None or thirty is None else thirty - ten
    return (
        f"中债国债收益率曲线（{value.session_date.isoformat()}）："
        f"1年={_optional(one, '%')}，10年={_optional(ten, '%')}，"
        f"30年={_optional(thirty, '%')}；10年-1年={_optional(spread_10_1, '个百分点')}，"
        f"30年-10年={_optional(spread_30_10, '个百分点')}。",
        _source_line(value.meta, reference),
    )


def _if_lines(
    contract: IFContractDailyObservation,
    spot: CrossMarketQuote | None,
    reference: str,
) -> tuple[str, ...]:
    base = (
        f"{contract.symbol}（{contract.session_date.isoformat()}）："
        f"开={_three(contract.open)}，高={_three(contract.high)}，低={_three(contract.low)}，"
        f"收={_three(contract.close)}，结算={_three(contract.settle)}，"
        f"前结算={_three(contract.previous_settle)}，成交量={contract.volume}，"
        f"持仓量={contract.open_interest}，供应商原始成交额={_three(contract.turnover_reported)}。"
    )
    if spot is None:
        basis = "IF收盘基差：不可计算；缺少同一交易日的CSI_300现货观测，未年化、未填值。"
    else:
        difference = contract.close - spot.last
        percent = difference / spot.last * Decimal("100")
        basis = (
            f"IF收盘基差：{contract.symbol}收盘-{spot.display_name}现货="
            f"{_three(difference)}点，基差率={_three(percent)}%；仅为同日收盘差，未年化。"
        )
    source = _source_line(contract.meta, reference)
    if spot is not None:
        source += f"；现货来源={spot.provider}（{_CSI_300_URL}）"
    return base, basis, source


def _same_session_csi_300(
    if_context: IFDailyContextSnapshot | None,
    cross_market: CrossMarketSnapshot | None,
) -> CrossMarketQuote | None:
    if if_context is None or cross_market is None:
        return None
    matches = [quote for quote in cross_market.quotes if quote.instrument_id == "CSI_300"]
    if len(matches) != 1:
        return None
    quote = matches[0]
    try:
        local_date = quote.local_quote_time.astimezone(ZoneInfo(quote.local_timezone)).date()
    except Exception:  # pragma: no cover - port normally validates time zone
        return None
    return quote if local_date == if_context.session_date else None


def _source_line(meta: AShareContextMeta, reference: str) -> str:
    return (
        f"来源：{meta.source_id}；证据引用：{reference}；链接见文末证据索引；"
        f"首次可用={meta.available_at.isoformat(timespec='seconds')}；"
        f"状态={'陈旧' if meta.stale else '正常'}{'、降级' if meta.degraded else ''}。"
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


def _meta_payload(meta: AShareContextMeta) -> dict[str, Any]:
    return {
        "source_id": meta.source_id.strip(),
        "source_url": meta.source_url.strip(),
        "observed_at": meta.observed_at.astimezone(UTC).isoformat(timespec="seconds"),
        "available_at": meta.available_at.astimezone(UTC).isoformat(timespec="seconds"),
        "fetched_at": meta.fetched_at.astimezone(UTC).isoformat(timespec="seconds"),
        "stale": meta.stale,
        "degraded": meta.degraded,
        "warnings": sorted(set(meta.warnings)),
    }


def _hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
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


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
