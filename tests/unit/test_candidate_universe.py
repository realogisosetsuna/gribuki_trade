from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.domain.candidates import (
    CandidatePriority,
    CandidateSource,
    CandidateStatus,
)
from gribuki_trade.features.ashare_screening import (
    AShareFactorRanking,
    HardFilterResult,
    RankedAShareCandidate,
    ScreeningCandidateDataStatus,
)
from gribuki_trade.features.ashare_surveillance import (
    AShareIntradayRanking,
    IntradayCandidate,
    IntradayCandidateClass,
)
from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.services.ashare_screening import (
    AShareScreeningRun,
    AShareScreeningRunStatus,
)
from gribuki_trade.services.ashare_surveillance import (
    AShareSurveillanceRun,
    AShareSurveillanceRunStatus,
)
from gribuki_trade.services.candidate_universe import (
    CandidateDiscovery,
    CandidateLifecycleError,
    CandidateUniversePolicy,
    CandidateUniverseService,
)
from gribuki_trade.storage.candidate_store import SQLiteCandidateStore

NOW = datetime(2026, 8, 14, 1, 0, tzinfo=UTC)


def discovery(
    *,
    source: CandidateSource = CandidateSource.INTRADAY_ANOMALY,
    run_id: str = "scan-1",
    observed_at: datetime | None = NOW,
    expires_at: datetime | None = None,
) -> CandidateDiscovery:
    return CandidateDiscovery(
        symbol="000001",
        source=source,
        source_run_id=run_id,
        discovered_at=NOW - timedelta(minutes=1),
        observed_at=observed_at,
        expires_at=expires_at,
        reason_codes=("PRICE_VOLUME_ANOMALY",),
        evidence_ids=("market-evidence-1",),
    )


def test_source_policy_applies_ttl_priority_and_idempotent_upsert(tmp_path) -> None:
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        service = CandidateUniverseService(store, clock=lambda: NOW)
        first = service.upsert(discovery(observed_at=None))
        replay = service.upsert(discovery(observed_at=None))

    assert first.appended is True
    assert replay.appended is False
    assert first.candidate.symbol == "000001.SZ"
    assert first.candidate.priority is CandidatePriority.HIGH
    assert first.candidate.expires_at == NOW + timedelta(hours=8)


def test_manual_candidate_has_no_implicit_expiry(tmp_path) -> None:
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        service = CandidateUniverseService(store, clock=lambda: NOW)
        result = service.upsert(
            discovery(source=CandidateSource.MANUAL, run_id="manual-1")
        )
        much_later = service.get("000001", as_of=NOW + timedelta(days=3650))

    assert result.candidate.expires_at is None
    assert much_later is not None and much_later.status is CandidateStatus.ACTIVE


def test_cooling_automatically_returns_to_active_before_ttl(tmp_path) -> None:
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        service = CandidateUniverseService(store, clock=lambda: NOW)
        service.upsert(
            discovery(source=CandidateSource.MANUAL, run_id="manual-1")
        )
        cooled = service.cool(
            "000001.SZ",
            at=NOW + timedelta(hours=1),
            until=NOW + timedelta(hours=3),
            reason_code="DUPLICATE_ALERT_SUPPRESSION",
        )
        during = service.get("000001", as_of=NOW + timedelta(hours=2))
        after = service.get("000001", as_of=NOW + timedelta(hours=3))

    assert cooled.candidate.status is CandidateStatus.COOLING
    assert during is not None and during.status is CandidateStatus.COOLING
    assert after is not None and after.status is CandidateStatus.ACTIVE


def test_explicit_removal_survives_new_discovery_until_explicit_activation(
    tmp_path,
) -> None:
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        service = CandidateUniverseService(store, clock=lambda: NOW)
        service.upsert(
            discovery(source=CandidateSource.MANUAL, run_id="manual-1")
        )
        removed = service.remove(
            "000001",
            at=NOW + timedelta(hours=1),
            reason_code="USER_REMOVED",
        )
        service.upsert(
            CandidateDiscovery(
                symbol="000001.SZ",
                source=CandidateSource.STRATEGY,
                source_run_id="strategy-2",
                discovered_at=NOW + timedelta(hours=2),
                observed_at=NOW + timedelta(hours=2),
                reason_codes=("STRATEGY_MATCH",),
            )
        )
        still_removed = service.get("000001", as_of=NOW + timedelta(hours=2))
        activated = service.activate(
            "000001",
            at=NOW + timedelta(hours=3),
            reason_code="USER_RESTORED",
        )

    assert removed.candidate.status is CandidateStatus.REMOVED
    assert still_removed is not None
    assert still_removed.status is CandidateStatus.REMOVED
    assert len(still_removed.provenance) == 2
    assert activated.candidate.status is CandidateStatus.ACTIVE


def test_tracking_query_excludes_expired_and_removed_but_can_include_cooling(
    tmp_path,
) -> None:
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        service = CandidateUniverseService(store, clock=lambda: NOW)
        service.upsert(
            discovery(
                source=CandidateSource.MANUAL,
                run_id="manual-1",
            )
        )
        service.cool(
            "000001",
            at=NOW + timedelta(minutes=1),
            until=NOW + timedelta(hours=1),
            reason_code="WAIT_FOR_CONFIRMATION",
        )
        with_cooling = service.tracking_candidates(
            as_of=NOW + timedelta(minutes=2),
            include_cooling=True,
        )
        active_only = service.tracking_candidates(
            as_of=NOW + timedelta(minutes=2),
        )

    assert len(with_cooling) == 1
    assert with_cooling[0].status is CandidateStatus.COOLING
    assert with_cooling[0].is_trackable is False
    assert active_only == ()


def test_intraday_surveillance_ingestion_retains_rank_class_and_coverage(tmp_path) -> None:
    candidates = tuple(
        IntradayCandidate(
            symbol=f"{index:06d}.SZ",
            name=f"测试{index}",
            rank=index,
            candidate_class=(
                IntradayCandidateClass.MOMENTUM_EXPANSION
                if index == 1
                else IntradayCandidateClass.ACTIVE_STRENGTH
            ),
            anomaly_score=0.8 - index / 100,
            factor_weight_coverage=0.8,
            last_price=Decimal("10"),
            change_percent=Decimal("3"),
            session_amount_cny=Decimal("100000000"),
            factors=(),
            reason_codes=("PRICE_STRENGTH",),
        )
        for index in (1, 6)
    )
    run = AShareSurveillanceRun(
        session_date=NOW.date(),
        requested_at=NOW,
        decision_at=NOW + timedelta(seconds=5),
        status=AShareSurveillanceRunStatus.COMPLETE,
        strategy_version="ashare-intraday-anomaly@test",
        source_id="eastmoney",
        source_revision="revision-1",
        universe_count=5_000,
        ranking=AShareIntradayRanking(
            candidates=candidates,
            excluded=(),
            globally_unavailable_factors=(),
            eligible_count=4_000,
        ),
        warnings=(),
    )
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        service = CandidateUniverseService(store)
        first = service.ingest_intraday_surveillance(run)
        replay = service.ingest_intraday_surveillance(run)

    assert [item.appended for item in first] == [True, True]
    assert [item.appended for item in replay] == [False, False]
    assert first[0].candidate.priority is CandidatePriority.URGENT
    assert first[1].candidate.priority is CandidatePriority.HIGH
    provenance = first[0].candidate.provenance[0]
    assert provenance.source is CandidateSource.INTRADAY_ANOMALY
    assert provenance.source_run_id.startswith(f"intraday-{NOW.date().isoformat()}-")
    assert "INTRADAY_CLASS_MOMENTUM_EXPANSION" in provenance.reason_codes
    assert "INTRADAY_RANK_0001" in provenance.reason_codes
    assert "FACTOR_WEIGHT_COVERAGE_0.800" in provenance.reason_codes


def test_close_screen_ingestion_is_stable_and_retains_ranking_lineage(tmp_path) -> None:
    candidate = RankedAShareCandidate(
        symbol="600000.SH",
        name="浦发银行",
        board=AShareBoard.SSE_MAIN,
        industry="银行",
        rank=1,
        composite_score=0.72,
        factor_weight_coverage=1.0,
        data_status=ScreeningCandidateDataStatus.COMPLETE,
        degradation_reasons=(),
        factor_contributions=(),
    )
    ranking = AShareFactorRanking(
        ranked_candidates=(candidate,),
        insufficient_candidates=(),
        factor_eligibility_exclusions=(),
        globally_unavailable_factors=(),
    )
    run = AShareScreeningRun(
        as_of=NOW.date(),
        decision_at=NOW,
        status=AShareScreeningRunStatus.COMPLETE,
        strategy_version="ashare-cross-section@test",
        universe_source_id="eastmoney",
        universe_source_revision="universe-1",
        factor_source_id="akshare-daily",
        factor_source_revision="factors-1",
        feature_version="features-1",
        universe_count=5_000,
        hard_filter_eligible_count=1_000,
        factor_requested_count=300,
        eligible_count=280,
        ranked_count=280,
        hard_filter=HardFilterResult(eligible=(), excluded=()),
        factor_budget_deferred=(),
        factor_ranking=ranking,
        top_candidates=(candidate,),
        warnings=(),
    )
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        service = CandidateUniverseService(store)
        first = service.ingest_close_screening(run)
        replay = service.ingest_close_screening(run)

    assert first[0].appended is True
    assert replay[0].appended is False
    assert first[0].candidate.priority is CandidatePriority.HIGH
    provenance = first[0].candidate.provenance[0]
    assert provenance.source is CandidateSource.CLOSE_SCREEN
    assert provenance.source_run_id.startswith(f"close-{NOW.date().isoformat()}-")
    assert "CLOSE_SCREEN_RANK_0001" in provenance.reason_codes


def test_invalid_lifecycle_transition_and_policy_fail_closed(tmp_path) -> None:
    with pytest.raises(ValueError, match="TTL values must be positive"):
        CandidateUniversePolicy(close_screen_ttl=timedelta(0))

    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        service = CandidateUniverseService(store, clock=lambda: NOW)
        with pytest.raises(CandidateLifecycleError, match="does not exist"):
            service.remove("000001", reason_code="NO_PARENT")
        service.upsert(
            discovery(
                expires_at=NOW + timedelta(minutes=1),
            )
        )
        with pytest.raises(CandidateLifecycleError, match="expired state"):
            service.cool(
                "000001",
                at=NOW + timedelta(minutes=2),
                reason_code="TOO_LATE",
            )
