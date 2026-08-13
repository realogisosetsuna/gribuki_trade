"""Pure PIT alignment and evidence for descriptive cross-market relations.

The converter deliberately owns no network or storage operation.  It turns
completed A-share daily bars and independently collected index histories into
the strictly aligned inputs expected by :mod:`gribuki_trade.features`.

An external close-to-close return is consumed exactly once: at the first
A-share 15:05 Asia/Shanghai decision point at or after the return became
available.  When several external sessions accrue during an A-share holiday,
their returns are compounded into that first decision point.  Missing external
sessions are never forward-filled, so an overseas holiday cannot repeat a
stale return across several A-share sessions.

The resulting correlations and regressions are descriptive associations.  In
particular, the lag-1 view is *not* labelled a forecast: it describes the
historical relation between the factor return already known at the preceding
A-share decision point and the current A-share return.
"""

from __future__ import annotations

import hashlib
import json
from bisect import bisect_left
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import ROUND_HALF_UP, Decimal
from typing import Any
from zoneinfo import ZoneInfo

from gribuki_trade.analysis.schemas import EvidenceItem
from gribuki_trade.domain.market import DailyBar
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.features import (
    AlignedFactorReturn,
    CrossMarketFactorRelation,
    CrossMarketFactorSeries,
    CrossMarketLagRelation,
    CrossMarketRelationsReport,
    CrossMarketRiskDirection,
    TargetCloseObservation,
    build_cross_market_relations,
)
from gribuki_trade.ports.cross_market_history import (
    CrossMarketHistoryMissingSeries,
    CrossMarketHistoryObservation,
    CrossMarketHistorySeries,
    CrossMarketHistorySnapshot,
)

_SCHEMA_VERSION = 1
_SOURCE_TIER = 2
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_AKSHARE_HOME = "https://akshare.akfamily.xyz/"
_AKSHARE_INDEX_DOCS = "https://akshare.akfamily.xyz/data/index/index.html"


@dataclass(frozen=True, slots=True)
class CrossMarketRelationEvidenceBundle:
    """One relation report plus its content-addressed evidence views."""

    report: CrossMarketRelationsReport
    items: tuple[EvidenceItem, ...]
    references: tuple[EvidenceReference, ...]
    report_lines: tuple[str, ...]

    def __post_init__(self) -> None:
        item_ids = tuple(item.evidence_id for item in self.items)
        reference_ids = tuple(reference.evidence_id for reference in self.references)
        if item_ids != reference_ids:
            raise ValueError("evidence items and references must be aligned")
        if len(item_ids) != len(set(item_ids)):
            raise ValueError("relation evidence IDs must be unique")


@dataclass(frozen=True, slots=True)
class _AlignedSeries:
    source_series: CrossMarketHistorySeries
    visible_observations: tuple[CrossMarketHistoryObservation, ...]
    factor: CrossMarketFactorSeries


def build_cross_market_relation_evidence(
    target_bars: Sequence[DailyBar],
    history: CrossMarketHistorySnapshot,
    as_of: datetime,
) -> CrossMarketRelationEvidenceBundle:
    """Align histories, calculate relations, and emit deterministic evidence.

    ``as_of`` is a hard point-in-time boundary.  A history snapshot fetched
    after that boundary is rejected rather than back-dated, and target or
    factor observations unavailable by the boundary are excluded.
    """

    _require_aware(as_of, "as_of")
    as_of_utc = as_of.astimezone(UTC)
    if history.fetched_at.astimezone(UTC) > as_of_utc:
        raise ValueError("history.fetched_at must not be after as_of")

    target_symbol, target = _target_observations(target_bars, as_of_utc)
    aligned = tuple(
        _align_series(series, target, as_of_utc)
        for series in sorted(history.series, key=lambda item: item.market_id)
    )
    report = build_cross_market_relations(
        target_symbol,
        target,
        tuple(item.factor for item in aligned),
    )

    relation_by_id = {item.factor_id: item for item in report.factors}
    target_hash = _target_content_hash(target_symbol, target)
    factor_items = tuple(
        _factor_evidence_item(
            aligned=item,
            relation=relation_by_id[item.factor.factor_id],
            target_hash=target_hash,
            report=report,
            as_of=as_of_utc,
        )
        for item in aligned
    )
    missing_items = tuple(
        _missing_evidence_item(item, history=history, as_of=as_of_utc)
        for item in sorted(history.missing, key=lambda item: item.market_id)
    )
    items = (*factor_items, *missing_items)
    return CrossMarketRelationEvidenceBundle(
        report=report,
        items=items,
        references=tuple(_reference(item) for item in items),
        report_lines=_format_report_lines(
            report,
            aligned=aligned,
            missing=tuple(sorted(history.missing, key=lambda item: item.market_id)),
        ),
    )


def _target_observations(
    bars: Sequence[DailyBar],
    as_of: datetime,
) -> tuple[str, tuple[TargetCloseObservation, ...]]:
    symbols = {bar.symbol.strip().upper() for bar in bars if bar.symbol.strip()}
    if len(symbols) != 1:
        raise ValueError("target_bars must contain exactly one non-blank symbol")
    symbol = next(iter(symbols))
    observations: list[TargetCloseObservation] = []
    seen_dates: set[object] = set()
    for bar in sorted(bars, key=lambda item: item.trade_date):
        if not bar.is_trading or bar.close is None:
            continue
        if bar.trade_date in seen_dates:
            raise ValueError("target_bars cannot contain duplicate trading dates")
        seen_dates.add(bar.trade_date)
        decision_at = datetime.combine(bar.trade_date, time(15, 5), tzinfo=_SHANGHAI)
        if decision_at.astimezone(UTC) > as_of:
            continue
        observations.append(
            TargetCloseObservation(
                trade_date=bar.trade_date,
                close=bar.close,
                decision_at=decision_at,
            )
        )
    if not observations:
        raise ValueError("target_bars contain no completed trading close at as_of")
    return symbol, tuple(observations)


def _align_series(
    series: CrossMarketHistorySeries,
    target: tuple[TargetCloseObservation, ...],
    as_of: datetime,
) -> _AlignedSeries:
    visible = tuple(
        observation
        for observation in series.observations
        if observation.available_at.astimezone(UTC) <= as_of
    )
    decision_times = [point.decision_at.astimezone(UTC) for point in target]
    compounded: dict[int, Decimal] = {}
    latest_available: dict[int, datetime] = {}
    for previous, current in zip(visible, visible[1:], strict=False):
        available_at = current.available_at.astimezone(UTC)
        target_index = bisect_left(decision_times, available_at)
        if target_index >= len(target):
            continue
        simple_return = current.close / previous.close - Decimal("1")
        existing = compounded.get(target_index)
        compounded[target_index] = (
            simple_return
            if existing is None
            else (Decimal("1") + existing) * (Decimal("1") + simple_return)
            - Decimal("1")
        )
        latest_available[target_index] = max(
            latest_available.get(target_index, available_at),
            available_at,
        )
    observations = tuple(
        AlignedFactorReturn(
            target_trade_date=target[index].trade_date,
            value=value,
            available_at=latest_available[index],
        )
        for index, value in sorted(compounded.items())
    )
    return _AlignedSeries(
        source_series=series,
        visible_observations=visible,
        factor=CrossMarketFactorSeries(
            factor_id=series.market_id,
            risk_direction=CrossMarketRiskDirection.POSITIVE_IS_RISK_ON,
            observations=observations,
        ),
    )


def _factor_evidence_item(
    *,
    aligned: _AlignedSeries,
    relation: CrossMarketFactorRelation,
    target_hash: str,
    report: CrossMarketRelationsReport,
    as_of: datetime,
) -> EvidenceItem:
    line = _factor_report_line(aligned.source_series.display_name, relation)
    payload = {
        "aligned_returns": [
            {
                "available_at": _timestamp(point.available_at),
                "target_trade_date": point.target_trade_date.isoformat(),
                "value": _canonical_numeric(point.value),
            }
            for point in aligned.factor.observations
        ],
        "as_of": _timestamp(as_of),
        "display_name": aligned.source_series.display_name.strip(),
        "factor_history_hash": _factor_history_hash(aligned),
        "market_id": aligned.source_series.market_id.strip(),
        "methodology_version": report.methodology_version,
        "relation": _relation_payload(relation),
        "risk_direction": CrossMarketRiskDirection.POSITIVE_IS_RISK_ON.value,
        "schema": "gribuki.cross_market_relation.factor",
        "schema_version": _SCHEMA_VERSION,
        "source": aligned.source_series.source.strip(),
        "target_history_hash": target_hash,
        "target_symbol": report.target_symbol,
    }
    content_hash = _content_hash(payload)
    return EvidenceItem(
        evidence_id=_evidence_id("factor", content_hash),
        publisher=_publisher(aligned.source_series.source),
        source_tier=_SOURCE_TIER,
        published_at=as_of,
        first_seen_at=as_of,
        title=f"跨市场历史关系｜{aligned.source_series.display_name}",
        excerpt=(
            f"{line} 相关不代表因果、预测能力或可执行交易优势；"
            "lag1仅描述前一A股决策时点已可得因子收益与当前A股收益的历史关系。"
        ),
        canonical_url=_canonical_source_url(aligned.source_series.source),
        content_hash=content_hash,
    )


def _missing_evidence_item(
    missing: CrossMarketHistoryMissingSeries,
    *,
    history: CrossMarketHistorySnapshot,
    as_of: datetime,
) -> EvidenceItem:
    payload = {
        "analysis_as_of": _timestamp(as_of),
        "expected_source": missing.expected_source.strip(),
        "failure_code": missing.failure_code.value,
        "history_as_of": _timestamp(history.as_of),
        "history_fetched_at": _timestamp(history.fetched_at),
        "market_id": missing.market_id.strip(),
        "reason": " ".join(missing.reason.split()),
        "schema": "gribuki.cross_market_relation.missing_coverage",
        "schema_version": _SCHEMA_VERSION,
    }
    content_hash = _content_hash(payload)
    return EvidenceItem(
        evidence_id=_evidence_id("missing", content_hash),
        publisher=_publisher(missing.expected_source),
        source_tier=_SOURCE_TIER,
        published_at=as_of,
        first_seen_at=as_of,
        title=f"跨市场历史关系｜数据缺口｜{missing.display_name}",
        excerpt=(
            f"{missing.display_name}（{missing.market_id}）历史缺失；"
            f"失败={missing.failure_code.value}；原因={missing.reason}；"
            "未使用代理序列、前值填充或重复收益。"
        ),
        canonical_url=_canonical_source_url(missing.expected_source),
        content_hash=content_hash,
    )


def _format_report_lines(
    report: CrossMarketRelationsReport,
    *,
    aligned: tuple[_AlignedSeries, ...],
    missing: tuple[CrossMarketHistoryMissingSeries, ...],
) -> tuple[str, ...]:
    names = {
        item.source_series.market_id: item.source_series.display_name for item in aligned
    }
    lines = [
        "跨市场历史关系：相关不代表因果、预测能力或可执行交易优势。",
        "阅读说明：\u201c同期关系\u201d比较同一A股决策时点已可得的两组收益；"
        "\u201c前一决策时点关系\u201d比较前一A股决策时点已知的外部市场收益与当前A股收益。"
        "两者均只作历史描述，不称为预测。",
        "指标说明：线性相关与指数加权相关均在 -1 至 1 之间；"
        "指数加权结果更重视近期样本；120日敏感度\u03b2表示外部市场每变动1%时"
        "本标的历史平均同向变动幅度，t值仅用于描述统计显著性；"
        "方向稳定度越接近1，三个观察窗口的正负方向越一致。",
        f"样本：{report.target_symbol}有效收盘={report.target_close_samples}；"
        f"最少共同收益样本={report.minimum_common_samples}；"
        f"方法={report.methodology_version}。",
    ]
    lines.extend(
        _factor_report_line(names[item.factor_id], item) for item in report.factors
    )
    lines.extend(
        f"- {item.display_name}：历史缺失；失败={item.failure_code.value}；"
        f"来源={item.expected_source}；未使用代理、填充或重复收益。"
        for item in missing
    )
    if not report.factors and not missing:
        lines.append("- 本次未配置跨市场历史因子。")
    return tuple(lines)


def _factor_report_line(
    display_name: str,
    relation: CrossMarketFactorRelation,
) -> str:
    return (
        f"- {display_name}："
        f"同期关系〔{_lag_report(relation.lag_0)}〕；"
        f"前一决策时点关系〔{_lag_report(relation.lag_1)}〕。"
    )


def _lag_report(lag: CrossMarketLagRelation) -> str:
    status = (
        "可用"
        if lag.failure_reason is None
        else _RELATION_FAILURE_ZH.get(
            lag.failure_reason.value,
            f"不可用（{lag.failure_reason.value}）",
        )
    )
    return (
        f"数据覆盖率 {_three(lag.coverage)}（{lag.common_samples}/"
        f"{lag.eligible_target_samples}个收益样本）；"
        f"近20/60/120日线性相关 "
        f"{_three_optional(lag.correlation_20)}/"
        f"{_three_optional(lag.correlation_60)}/"
        f"{_three_optional(lag.correlation_120)}；"
        f"指数加权相关（20/60日半衰期）"
        f"{_three_optional(lag.ewma_correlation_half_life_20)}/"
        f"{_three_optional(lag.ewma_correlation_half_life_60)}；"
        f"120日敏感度\u03b2/t值 {_three_optional(lag.beta_120)}/"
        f"{_three_optional(lag.beta_t_stat_120)}；"
        f"方向稳定度 {_three_optional(lag.correlation_sign_stability)}；"
        f"状态 {status}"
    )


_RELATION_FAILURE_ZH = {
    "INSUFFICIENT_COMMON_SAMPLES": "共同样本不足",
    "ZERO_FACTOR_VARIANCE": "外部市场收益无波动",
    "ZERO_TARGET_VARIANCE": "本标的收益无波动",
    "UNDEFINED_CORRELATION": "相关系数无法计算",
    "BETA_STANDARD_ERROR_UNAVAILABLE": "敏感度显著性无法计算",
}


def _target_content_hash(
    symbol: str,
    target: tuple[TargetCloseObservation, ...],
) -> str:
    return _content_hash(
        {
            "symbol": symbol,
            "target": [
                {
                    "close": _canonical_numeric(point.close),
                    "decision_at": _timestamp(point.decision_at),
                    "trade_date": point.trade_date.isoformat(),
                }
                for point in target
            ],
        }
    )


def _factor_history_hash(aligned: _AlignedSeries) -> str:
    return _content_hash(
        {
            "market_id": aligned.source_series.market_id,
            "observations": [
                {
                    "available_at": _timestamp(point.available_at),
                    "close": _canonical_numeric(point.close),
                    "session_date": point.session_date.isoformat(),
                    "source": point.source,
                }
                for point in aligned.visible_observations
            ],
        }
    )


def _relation_payload(relation: CrossMarketFactorRelation) -> dict[str, Any]:
    return {
        "factor_id": relation.factor_id,
        "lag_0": _lag_payload(relation.lag_0),
        "lag_1": _lag_payload(relation.lag_1),
        "risk_direction": relation.risk_direction.value,
    }


def _lag_payload(lag: CrossMarketLagRelation) -> dict[str, Any]:
    return {
        "alpha_120": _canonical_optional_float(lag.alpha_120),
        "beta_120": _canonical_optional_float(lag.beta_120),
        "beta_t_stat_120": _canonical_optional_float(lag.beta_t_stat_120),
        "common_samples": lag.common_samples,
        "correlation_20": _canonical_optional_float(lag.correlation_20),
        "correlation_60": _canonical_optional_float(lag.correlation_60),
        "correlation_120": _canonical_optional_float(lag.correlation_120),
        "correlation_sign_stability": _canonical_optional_float(
            lag.correlation_sign_stability
        ),
        "coverage": _canonical_float(lag.coverage),
        "eligible_target_samples": lag.eligible_target_samples,
        "ewma_20": _canonical_optional_float(lag.ewma_correlation_half_life_20),
        "ewma_60": _canonical_optional_float(lag.ewma_correlation_half_life_60),
        "failure_reason": (
            None if lag.failure_reason is None else lag.failure_reason.value
        ),
        "first_common_date": (
            None if lag.first_common_date is None else lag.first_common_date.isoformat()
        ),
        "lag": lag.lag,
        "last_common_date": (
            None if lag.last_common_date is None else lag.last_common_date.isoformat()
        ),
        "risk_alignment": lag.risk_alignment.value,
        "sign_regime": lag.sign_regime.value,
    }


def _reference(item: EvidenceItem) -> EvidenceReference:
    return EvidenceReference(
        evidence_id=item.evidence_id,
        title=item.title,
        canonical_url=item.canonical_url,
        published_at=item.published_at,
        first_seen_at=item.first_seen_at,
        source_tier=item.source_tier,
    )


def _canonical_source_url(source: str) -> str:
    normalized = source.casefold()
    if any(
        method in normalized
        for method in (
            "stock_zh_index_daily",
            "stock_hk_index_daily_sina",
            "index_us_stock_sina",
            "index_global_hist_sina",
        )
    ):
        return _AKSHARE_INDEX_DOCS
    return _AKSHARE_HOME


def _publisher(source: str) -> str:
    return "AKShare/Sina" if "akshare" in source.casefold() else source.strip()


def _content_hash(payload: dict[str, Any]) -> str:
    content = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _evidence_id(kind: str, content_hash: str) -> str:
    material = f"gribuki.cross_market_relation.{kind}.v{_SCHEMA_VERSION}\0{content_hash}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")


def _canonical_numeric(value: object) -> str:
    if isinstance(value, Decimal):
        if value.is_zero():
            return "0"
        return format(value.normalize(), "f")
    if isinstance(value, float):
        return _canonical_float(value)
    return str(value)


def _canonical_float(value: float) -> str:
    return format(value, ".17g")


def _canonical_optional_float(value: float | None) -> str | None:
    return None if value is None else _canonical_float(value)


def _three(value: float) -> str:
    rounded = Decimal(str(value)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    if rounded.is_zero():
        rounded = abs(rounded)
    return format(rounded, "f")


def _three_optional(value: float | None) -> str:
    return "暂无" if value is None else _three(value)


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
