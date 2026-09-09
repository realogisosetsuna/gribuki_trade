from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.backtest.recommendation_outcomes import OutcomeStatus
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.domain.recommendations import (
    ConfidenceBand,
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
    ResearchRecommendation,
)
from gribuki_trade.services.research.recommendation_evaluation import (
    RecommendationEvaluationService,
)
from gribuki_trade.storage.research.research_store import SQLiteResearchStore

AS_OF = datetime(2026, 8, 3, 7, 0, tzinfo=UTC)
EVALUATED_AT = datetime(2026, 8, 12, 1, 0, tzinfo=UTC)


def recommendation(
    identifier: str = "rec-1",
    *,
    decision: RecommendationDecision = RecommendationDecision.ENTER_CANDIDATE,
    symbol: str = "600000.SH",
    as_of: datetime = AS_OF,
) -> ResearchRecommendation:
    return ResearchRecommendation(
        recommendation_id=identifier,
        symbol=symbol,
        as_of=as_of,
        expires_at=as_of + timedelta(days=1),
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=decision,
        confidence=ConfidenceBand.UNCALIBRATED,
        technical_score=Decimal("0.6"),
        macro_score=None,
        reference_price=Decimal("10"),
        invalidation_price=Decimal("9.5"),
        reason_codes=("TEST",),
        uncertainties=(),
        evidence=(
            EvidenceReference(
                evidence_id=f"evidence-{identifier}",
                title="Retained evidence",
                canonical_url=f"https://example.test/{identifier}",
                published_at=as_of - timedelta(minutes=2),
                first_seen_at=as_of - timedelta(minutes=1),
                source_tier=2,
            ),
        ),
        strategy_version="test@1",
    )


def bar(
    day: int,
    *,
    symbol: str = "600000.SH",
    adjustment: PriceAdjustment = PriceAdjustment.NONE,
) -> DailyBar:
    return DailyBar(
        symbol=symbol,
        trade_date=date(2026, 8, day),
        open=Decimal("10"),
        high=Decimal("10.5"),
        low=Decimal("9.8"),
        close=Decimal("10.2"),
        previous_close=Decimal("10"),
        volume=1_000_000,
        amount=Decimal("10000000"),
        turnover_percent=Decimal("1"),
        is_trading=True,
        is_st=False,
        adjustment=adjustment,
    )


def complete_bars() -> tuple[DailyBar, ...]:
    return tuple(bar(day) for day in (4, 5, 6, 7, 10))


class FakeHistoricalData:
    def __init__(self, responses: dict[str, tuple[DailyBar, ...] | Exception]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, date, date, PriceAdjustment]] = []

    async def fetch_daily_bars_async(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        self.calls.append((symbol, start, end, adjustment))
        response = self.responses[symbol]
        if isinstance(response, Exception):
            raise response
        return response


def test_evaluates_from_next_session_with_unadjusted_bounded_request(tmp_path) -> None:
    provider = FakeHistoricalData({"600000.SH": complete_bars()})
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(recommendation())
        service = RecommendationEvaluationService(store, provider)

        result = asyncio.run(service.run_once(evaluated_at=EVALUATED_AT))

        assert result.selected == 1
        assert (result.evaluated, result.pending, result.unevaluable, result.errors) == (
            1,
            0,
            0,
            0,
        )
        assert result.observations_written == 1
        assert result.outcomes[0].entry_date == date(2026, 8, 4)
        assert result.outcomes[0].exit_date == date(2026, 8, 10)
        assert provider.calls == [
            (
                "600000.SH",
                date(2026, 8, 4),
                date(2026, 8, 11),
                PriceAdjustment.NONE,
            )
        ]
        assert len(store.latest_outcomes("rec-1")) == 1


def test_terminal_result_is_reused_without_refetch_or_duplicate(tmp_path) -> None:
    provider = FakeHistoricalData({"600000.SH": complete_bars()})
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(recommendation())
        service = RecommendationEvaluationService(store, provider)
        first = asyncio.run(service.run_once(evaluated_at=EVALUATED_AT))
        second = asyncio.run(
            service.run_once(evaluated_at=EVALUATED_AT + timedelta(days=1))
        )

        assert first.observations_written == 1
        assert second.evaluated == 1
        assert second.observations_written == 0
        assert second.observations_reused == 1
        assert len(provider.calls) == 1
        assert len(store.latest_outcomes("rec-1")) == 1


def test_pending_is_retried_and_only_state_changes_are_appended(tmp_path) -> None:
    provider = FakeHistoricalData({"600000.SH": complete_bars()[:2]})
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(recommendation())
        service = RecommendationEvaluationService(store, provider)

        first = asyncio.run(service.run_once(evaluated_at=EVALUATED_AT))
        unchanged = asyncio.run(
            service.run_once(evaluated_at=EVALUATED_AT + timedelta(days=1))
        )
        provider.responses["600000.SH"] = complete_bars()
        completed = asyncio.run(
            service.run_once(evaluated_at=EVALUATED_AT + timedelta(days=2))
        )

        assert first.pending == 1
        assert first.observations_written == 1
        assert unchanged.pending == 1
        assert unchanged.observations_written == 0
        assert unchanged.observations_reused == 1
        assert completed.evaluated == 1
        assert completed.observations_written == 1
        assert tuple(item.status for item in store.latest_outcomes("rec-1")) == (
            OutcomeStatus.COMPLETE,
            OutcomeStatus.PENDING,
        )


def test_non_directional_result_is_unevaluable_without_provider_call(tmp_path) -> None:
    item = recommendation(decision=RecommendationDecision.WATCH)
    provider = FakeHistoricalData({})
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(item)
        service = RecommendationEvaluationService(store, provider)

        result = asyncio.run(service.run_once(evaluated_at=EVALUATED_AT))

        assert result.unevaluable == 1
        assert result.outcomes[0].reason_code == "NON_DIRECTIONAL_DECISION"
        assert provider.calls == []


@pytest.mark.parametrize(
    "unsafe_bars",
    [
        (replace(bar(4), adjustment=PriceAdjustment.FORWARD),),
        (replace(bar(4), trade_date=date(2026, 8, 12)),),
    ],
)
def test_adjusted_or_out_of_window_history_fails_closed(tmp_path, unsafe_bars) -> None:
    provider = FakeHistoricalData({"600000.SH": unsafe_bars})
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(recommendation())
        result = asyncio.run(
            RecommendationEvaluationService(store, provider).run_once(
                evaluated_at=EVALUATED_AT
            )
        )

        assert result.errors == 1
        assert result.outcomes == ()
        assert result.failures[0].error_type == "HistoricalOutcomeDataError"
        assert store.latest_outcomes("rec-1") == ()


def test_one_provider_failure_does_not_block_other_recommendations(tmp_path) -> None:
    broken = recommendation("rec-broken", symbol="000001.SZ")
    healthy = recommendation("rec-healthy")
    provider = FakeHistoricalData(
        {
            "000001.SZ": RuntimeError("sensitive upstream detail"),
            "600000.SH": complete_bars(),
        }
    )
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(broken)
        store.append_recommendation(healthy)
        result = asyncio.run(
            RecommendationEvaluationService(store, provider).run_once(
                recommendations=(broken, healthy),
                evaluated_at=EVALUATED_AT,
            )
        )

        assert result.selected == 2
        assert result.evaluated == 1
        assert result.errors == 1
        assert result.failures[0].recommendation_id == "rec-broken"
        assert result.failures[0].phase == "fetch"
        assert result.failures[0].error_type == "RuntimeError"
        assert store.latest_outcomes("rec-broken") == ()
        assert len(store.latest_outcomes("rec-healthy")) == 1


def test_unretained_explicit_item_is_isolated_as_persistence_error(tmp_path) -> None:
    retained = recommendation("rec-retained")
    missing = recommendation("rec-missing")
    provider = FakeHistoricalData({"600000.SH": complete_bars()})
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(retained)
        result = asyncio.run(
            RecommendationEvaluationService(store, provider).run_once(
                recommendations=(missing, retained),
                evaluated_at=EVALUATED_AT,
            )
        )

        assert result.selected == 2
        assert result.evaluated == 1
        assert result.errors == 1
        assert result.failures[0].recommendation_id == "rec-missing"
        assert result.failures[0].phase == "persist"
        assert result.outcomes[0].recommendation_id == "rec-retained"


def test_store_selection_supports_exact_symbol_and_as_of_filters(tmp_path) -> None:
    later = AS_OF + timedelta(minutes=1)
    first = recommendation("rec-first")
    second = recommendation("rec-second", as_of=later)
    provider = FakeHistoricalData({"600000.SH": complete_bars()})
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(first)
        store.append_recommendation(second)
        service = RecommendationEvaluationService(store, provider)

        result = asyncio.run(
            service.run_once(
                symbol="600000.sh",
                as_of=later,
                evaluated_at=EVALUATED_AT,
            )
        )

        assert result.selected == 1
        assert result.outcomes[0].recommendation_id == "rec-second"


def test_rejects_current_day_as_closed_data_and_ambiguous_selection(tmp_path) -> None:
    provider = FakeHistoricalData({})
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(recommendation())
        service = RecommendationEvaluationService(store, provider)

        with pytest.raises(ValueError, match="must precede"):
            asyncio.run(
                service.run_once(
                    evaluated_at=EVALUATED_AT,
                    data_through=EVALUATED_AT.date(),
                )
            )
        with pytest.raises(ValueError, match="cannot be combined"):
            asyncio.run(
                service.run_once(
                    (recommendation(),),
                    symbol="600000.SH",
                    evaluated_at=EVALUATED_AT,
                )
            )
