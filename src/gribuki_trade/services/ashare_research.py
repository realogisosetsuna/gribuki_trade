"""One-shot A-share research orchestration.

This service deliberately stops at a research recommendation.  It has no
broker, account, order, or execution dependency.  A caller may explicitly ask
it to enqueue a text notification, but notifications remain one-way and cannot
turn a recommendation into an order.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Protocol

from gribuki_trade.analysis.schemas import MacroAnalysis
from gribuki_trade.domain.recommendations import (
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
    ResearchRecommendation,
)
from gribuki_trade.features.technical import (
    TechnicalBar,
    TechnicalSignal,
    TechnicalSignalConfig,
    build_technical_signal,
)
from gribuki_trade.policy.recommendation_gate import (
    RecommendationGateConfig,
    build_recommendation,
)
from gribuki_trade.ports.market_data import (
    AsyncIntradayMarketData,
    FreshnessStatus,
    IntradayBar,
    MarketDataUnavailableError,
    MinuteInterval,
    SourceSemantics,
)
from gribuki_trade.ports.notifier import NotificationTargetKind, OutboundNotification


class NotificationOutbox(Protocol):
    """Structural subset implemented by ``SQLiteOutbox``."""

    def enqueue(self, notification: OutboundNotification) -> object: ...


@dataclass(frozen=True, slots=True)
class AShareResearchRequest:
    """Inputs for one point-in-time technical research evaluation."""

    symbol: str
    start: datetime
    end: datetime
    horizon: RecommendationHorizon = RecommendationHorizon.SHORT_1_TO_5_DAYS
    interval: MinuteInterval = MinuteInterval.ONE_MINUTE
    decision_time: datetime | None = None
    is_currently_held: bool = False
    evidence: tuple[EvidenceReference, ...] = ()
    macro: MacroAnalysis | None = None

    def __post_init__(self) -> None:
        _canonical_ashare_symbol(self.symbol)
        _require_aware(self.start, "start")
        _require_aware(self.end, "end")
        if self.start >= self.end:
            raise ValueError("start must be before end")
        if self.decision_time is not None:
            _require_aware(self.decision_time, "decision_time")
            if self.end > self.decision_time:
                raise ValueError("end must not be after decision_time")
        identifiers = [item.evidence_id for item in self.evidence]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("evidence IDs must be unique")

    @property
    def canonical_symbol(self) -> str:
        return _canonical_ashare_symbol(self.symbol)


@dataclass(frozen=True, slots=True)
class ResearchNotificationTarget:
    """Explicit destination and publication policy for a research alert."""

    target_id: str
    target_kind: NotificationTargetKind = NotificationTargetKind.PRIVATE
    channel: str = "onebot"
    decisions: frozenset[RecommendationDecision] = frozenset(
        {
            RecommendationDecision.ENTER_CANDIDATE,
            RecommendationDecision.WATCH,
            RecommendationDecision.REDUCE,
        }
    )
    max_characters: int = 3500

    def __post_init__(self) -> None:
        if not self.target_id.strip():
            raise ValueError("target_id must not be empty")
        if not self.channel.strip():
            raise ValueError("channel must not be empty")
        if not self.decisions:
            raise ValueError("at least one notification decision is required")
        if self.max_characters < 200:
            raise ValueError("max_characters must be at least 200")


@dataclass(frozen=True, slots=True)
class AShareResearchRun:
    """Result of one evaluation and its optional outbox side effect."""

    recommendation: ResearchRecommendation
    notification: OutboundNotification | None = None
    notification_enqueued: bool = False
    failure_code: str | None = None


@dataclass(frozen=True, slots=True)
class AShareMarketDataCollection:
    """Sanitized result of the market-data phase for point-in-time evaluation."""

    bars: tuple[IntradayBar, ...]
    failure_code: str | None = None

    def __post_init__(self) -> None:
        if self.failure_code is not None and self.bars:
            raise ValueError("failed market-data collection cannot contain bars")


class AShareResearchService:
    """Fetch completed bars, build a signal, and apply the publication gate."""

    def __init__(
        self,
        market_data: AsyncIntradayMarketData,
        *,
        technical_config: TechnicalSignalConfig | None = None,
        gate_config: RecommendationGateConfig | None = None,
        outbox: NotificationOutbox | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._market_data = market_data
        self._technical_config = technical_config or TechnicalSignalConfig()
        self._gate_config = gate_config or RecommendationGateConfig()
        self._outbox = outbox
        self._clock = clock

    async def run_once(
        self,
        request: AShareResearchRequest,
        *,
        notification_target: ResearchNotificationTarget | None = None,
    ) -> AShareResearchRun:
        """Evaluate once; enqueue only when a target was explicitly supplied."""

        collection = await self.collect_market_data(request)
        return self.evaluate_collection(
            request,
            collection,
            notification_target=notification_target,
        )

    async def collect_market_data(
        self, request: AShareResearchRequest
    ) -> AShareMarketDataCollection:
        """Collect bars without freezing the recommendation time beforehand."""

        try:
            bars = tuple(
                await self._market_data.fetch_intraday_bars_async(
                    request.canonical_symbol,
                    request.start,
                    request.end,
                    interval=request.interval,
                    completed_only=True,
                )
            )
        except MarketDataUnavailableError:
            # Provider messages can contain request details. Expose only this
            # stable code and keep unrelated programming errors visible.
            return AShareMarketDataCollection(
                bars=(),
                failure_code="MARKET_DATA_FETCH_FAILED",
            )
        return AShareMarketDataCollection(bars=bars)

    def evaluate_collection(
        self,
        request: AShareResearchRequest,
        collection: AShareMarketDataCollection,
        *,
        notification_target: ResearchNotificationTarget | None = None,
    ) -> AShareResearchRun:
        """Evaluate an immutable collection at an explicit or post-fetch time."""

        decision_time = request.decision_time or self._clock()
        _require_aware(decision_time, "decision_time")
        self._validate_evidence(request.evidence, decision_time)

        technical = (
            _abstain_signal(
                request.canonical_symbol,
                request.horizon,
                decision_time,
                self._technical_config.strategy_version,
                collection.failure_code,
                latest_end=None,
            )
            if collection.failure_code is not None
            else self._technical_from_market_bars(
                request,
                collection.bars,
                decision_time=decision_time,
            )
        )
        recommendation = build_recommendation(
            technical,
            request.evidence,
            macro=request.macro,
            config=self._gate_config,
        )

        if (
            notification_target is None
            or recommendation.decision not in notification_target.decisions
        ):
            return AShareResearchRun(
                recommendation=recommendation,
                failure_code=collection.failure_code,
            )
        if self._outbox is None:
            raise RuntimeError(
                "notification_target requires an explicitly configured outbox"
            )

        notification = format_recommendation_notification(
            recommendation,
            notification_target,
        )
        self._outbox.enqueue(notification)
        return AShareResearchRun(
            recommendation=recommendation,
            notification=notification,
            notification_enqueued=True,
            failure_code=collection.failure_code,
        )

    def _technical_from_market_bars(
        self,
        request: AShareResearchRequest,
        bars: tuple[IntradayBar, ...],
        *,
        decision_time: datetime,
    ) -> TechnicalSignal:
        invalid_reason = _market_bar_invariant_failure(
            request.canonical_symbol,
            request.interval,
            request.start,
            request.end,
            bars,
        )
        if invalid_reason is not None:
            return _abstain_signal(
                request.canonical_symbol,
                request.horizon,
                decision_time,
                self._technical_config.strategy_version,
                invalid_reason,
                latest_end=bars[-1].end_at if bars else None,
            )

        try:
            technical_bars = tuple(
                TechnicalBar(
                    end_time=bar.end_at,
                    # Collection time is the earliest time this process can
                    # prove the provider record was visible.
                    available_at=bar.meta.fetched_at,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume_lots,
                    complete=bar.is_closed,
                )
                for bar in bars
            )
            signal = build_technical_signal(
                request.canonical_symbol,
                technical_bars,
                decision_time=decision_time,
                horizon=request.horizon,
                is_currently_held=request.is_currently_held,
                config=self._technical_config,
            )
        except ValueError:
            # A provider chronology/schema anomaly must never become an entry
            # signal.  Request validation has already happened at the boundary,
            # so this catch is limited to provider-derived bars.
            return _abstain_signal(
                request.canonical_symbol,
                request.horizon,
                decision_time,
                self._technical_config.strategy_version,
                "INVALID_MARKET_DATA",
                latest_end=bars[-1].end_at if bars else None,
            )

        if signal.decision is RecommendationDecision.ABSTAIN or not bars:
            return signal
        latest = bars[-1]
        if latest.meta.degraded:
            return _force_abstain(signal, "DEGRADED_MARKET_DATA")
        if latest.meta.freshness is FreshnessStatus.STALE:
            return _force_abstain(signal, "UPSTREAM_MARKET_DATA_STALE")
        if latest.meta.freshness is FreshnessStatus.UNKNOWN:
            return _force_abstain(signal, "UPSTREAM_FRESHNESS_UNKNOWN")
        return signal

    @staticmethod
    def _validate_evidence(
        evidence: tuple[EvidenceReference, ...], decision_time: datetime
    ) -> None:
        for item in evidence:
            _require_aware(item.published_at, "evidence published_at")
            _require_aware(item.first_seen_at, "evidence first_seen_at")
            if item.published_at > decision_time or item.first_seen_at > decision_time:
                raise ValueError("evidence was not available at decision_time")
            if item.first_seen_at < item.published_at:
                raise ValueError("evidence first_seen_at must not precede published_at")


def format_recommendation_notification(
    recommendation: ResearchRecommendation,
    target: ResearchNotificationTarget,
) -> OutboundNotification:
    """Render a bounded, text-only, non-executable research notification."""

    labels = {
        RecommendationDecision.ENTER_CANDIDATE: "进入候选",
        RecommendationDecision.WATCH: "观察",
        RecommendationDecision.REDUCE: "考虑降低暴露",
        RecommendationDecision.ABSTAIN: "暂不判断",
    }
    lines = [
        "【A股研究建议｜不会自动下单】",
        f"标的：{_single_line(recommendation.symbol)}",
        f"结论：{labels[recommendation.decision]}",
        f"周期：{recommendation.horizon.value}",
        f"决策时点：{recommendation.as_of.isoformat(timespec='seconds')}",
        f"有效至：{recommendation.expires_at.isoformat(timespec='seconds')}",
        f"技术分：{recommendation.technical_score}",
        f"参考价：{_format_decimal(recommendation.reference_price)}",
        f"失效参考：{_format_decimal(recommendation.invalidation_price)}",
        f"原因：{', '.join(recommendation.reason_codes)}",
    ]
    if recommendation.uncertainties:
        lines.append(f"不确定性：{', '.join(recommendation.uncertainties)}")
    if recommendation.evidence:
        lines.append("证据：")
        for item in recommendation.evidence[:3]:
            lines.append(
                f"- {_single_line(item.title)} | {_single_line(item.canonical_url)}"
            )
    lines.append("仅供个人研究与人工复核，不构成可执行交易指令。")
    text = "\n".join(lines)
    if len(text) > target.max_characters:
        text = text[: target.max_characters - 1] + "…"

    destination_hash = sha256(
        f"{target.channel}|{target.target_kind.value}|{target.target_id}".encode()
    ).hexdigest()[:16]
    return OutboundNotification(
        idempotency_key=(
            f"research:{recommendation.recommendation_id}:"
            f"{target.channel}:{destination_hash}"
        ),
        channel=target.channel,
        target_kind=target.target_kind,
        target_id=target.target_id,
        text=text,
        created_at=recommendation.as_of,
        expires_at=recommendation.expires_at,
    )


def _market_bar_invariant_failure(
    requested_symbol: str,
    requested_interval: MinuteInterval,
    requested_start: datetime,
    requested_end: datetime,
    bars: tuple[IntradayBar, ...],
) -> str | None:
    if any(bar.symbol.upper() != requested_symbol for bar in bars):
        return "MARKET_DATA_SYMBOL_MISMATCH"
    if any(bar.interval is not requested_interval for bar in bars):
        return "MARKET_DATA_INTERVAL_MISMATCH"
    if any(not bar.is_closed for bar in bars):
        return "INCOMPLETE_MARKET_BAR"
    if any(
        bar.start_at < requested_start
        or bar.end_at > requested_end
        or bar.start_at >= bar.end_at
        for bar in bars
    ):
        return "MARKET_DATA_OUTSIDE_REQUEST_WINDOW"
    if any(
        bar.meta.semantics is not SourceSemantics.AGGREGATED_MINUTE_BAR for bar in bars
    ):
        return "UNSUPPORTED_MARKET_DATA_SEMANTICS"
    return None


def _force_abstain(signal: TechnicalSignal, reason: str) -> TechnicalSignal:
    return replace(
        signal,
        decision=RecommendationDecision.ABSTAIN,
        score=Decimal("0"),
        reference_price=None,
        invalidation_price=None,
        reason_codes=(reason,),
        metrics=(),
    )


def _abstain_signal(
    symbol: str,
    horizon: RecommendationHorizon,
    decision_time: datetime,
    strategy_version: str,
    reason: str,
    *,
    latest_end: datetime | None,
) -> TechnicalSignal:
    age = timedelta(0) if latest_end is None else max(decision_time - latest_end, timedelta(0))
    return TechnicalSignal(
        symbol=symbol.strip().upper(),
        as_of=decision_time,
        horizon=horizon,
        decision=RecommendationDecision.ABSTAIN,
        score=Decimal("0"),
        reference_price=None,
        invalidation_price=None,
        reason_codes=(reason,),
        data_age=age,
        strategy_version=strategy_version,
        metrics=(),
    )


def _format_decimal(value: Decimal | None) -> str:
    return "—" if value is None else format(value, "f")


def _single_line(value: str) -> str:
    return " ".join(value.split())


def _canonical_ashare_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        exchange = "SH" if value.startswith(("5", "6", "9")) else "SZ"
        return f"{value}.{exchange}"
    if len(value) == 9 and value[6] == ".":
        code, exchange = value.split(".", maxsplit=1)
        if code.isdigit() and exchange in {"SH", "SZ"}:
            return value
    raise ValueError("symbol must look like 600000.SH or 000001.SZ")


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
