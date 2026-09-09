import math
from collections.abc import Sequence
from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase
from zoneinfo import ZoneInfo

from gribuki_trade.features.ashare_screening import (
    AShareScreeningConfig,
    FactorObservationStatus,
    HardFilterReason,
    ScreeningCandidateDataStatus,
    ScreeningFactorDirection,
    ScreeningFactorSpec,
    hard_filter_ashare_universe,
    rank_ashare_factor_cross_section,
)
from gribuki_trade.ports.ashare_screening import (
    AShareBoard,
    AShareFactorRecord,
    AShareFactorSnapshot,
    AShareFactorValue,
    AShareUniverseRecord,
    AShareUniverseSnapshot,
    ScreeningFactorId,
    ScreeningHistoryPolicy,
    ScreeningSourceQuality,
)
from gribuki_trade.services.ashare_screening import (
    AShareScreeningPointInTimeError,
    AShareScreeningRunStatus,
    AShareScreeningService,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
AS_OF = date(2026, 8, 13)
DECISION_AT = datetime(2026, 8, 13, 16, 0, tzinfo=SHANGHAI)


def universe_record(
    symbol: str,
    *,
    name: str | None = None,
    board: AShareBoard = AShareBoard.SSE_MAIN,
    industry: str | None = "industrials",
    listing_days: int | None = 1000,
    is_tradable: bool | None = True,
    is_st: bool | None = False,
    is_suspended: bool | None = False,
    last_price: Decimal | None = Decimal("10"),
    average_amount: Decimal | None = Decimal("100000000"),
    market_cap: Decimal | None = Decimal("10000000000"),
) -> AShareUniverseRecord:
    return AShareUniverseRecord(
        symbol=symbol,
        name=name or symbol,
        board=board,
        industry=industry,
        listing_days=listing_days,
        is_tradable=is_tradable,
        is_st=is_st,
        is_suspended=is_suspended,
        last_price=last_price,
        session_amount_cny=average_amount,
        market_cap_cny=market_cap,
    )


def factor_record(
    symbol: str,
    values: dict[ScreeningFactorId, float | None],
) -> AShareFactorRecord:
    resolved_values = dict(values)
    resolved_values.setdefault(ScreeningFactorId.AVERAGE_AMOUNT_20_CNY, 100_000_000.0)
    return AShareFactorRecord(
        symbol=symbol,
        values=tuple(
            AShareFactorValue(factor_id=factor_id, value=value)
            for factor_id, value in resolved_values.items()
        ),
    )


def two_factor_config(**overrides: object) -> AShareScreeningConfig:
    values: dict[str, object] = {
        "factor_specs": (
            ScreeningFactorSpec(
                ScreeningFactorId.MOMENTUM_20,
                0.60,
                ScreeningFactorDirection.HIGHER_IS_BETTER,
            ),
            ScreeningFactorSpec(
                ScreeningFactorId.ANNUALIZED_VOLATILITY_60,
                0.40,
                ScreeningFactorDirection.LOWER_IS_BETTER,
            ),
        ),
        "min_cross_section_observations": 3,
        "min_factor_weight_coverage": 1.0,
        "top_n": 2,
        "lower_winsor_quantile": 0.0,
        "upper_winsor_quantile": 1.0,
    }
    values.update(overrides)
    return AShareScreeningConfig(**values)  # type: ignore[arg-type]


class AShareScreeningPortTests(TestCase):
    def test_snapshot_requires_timezone_aware_availability(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            AShareUniverseSnapshot(
                as_of=AS_OF,
                available_at=datetime(2026, 8, 13, 15, 30),
                observed_at=datetime(2026, 8, 13, 15, 31),
                source_id="test",
                source_revision="1",
                records=(),
            )

    def test_symbol_exchange_must_match_board(self) -> None:
        with self.assertRaisesRegex(ValueError, "inconsistent"):
            universe_record("000001.SZ", board=AShareBoard.SSE_MAIN)


class AShareHardFilterTests(TestCase):
    def test_fail_closed_filter_retains_all_exclusion_reasons(self) -> None:
        valid = universe_record("600001.SH")
        unknown = universe_record(
            "600002.SH",
            listing_days=None,
            is_tradable=None,
            is_st=None,
            is_suspended=None,
            last_price=None,
            average_amount=None,
            market_cap=None,
        )
        blocked = universe_record(
            "600003.SH",
            listing_days=10,
            is_tradable=False,
            is_st=True,
            is_suspended=True,
            last_price=Decimal("0.50"),
            average_amount=Decimal("10"),
            market_cap=Decimal("100"),
        )

        result = hard_filter_ashare_universe((blocked, valid, unknown))

        self.assertEqual(tuple(item.symbol for item in result.eligible), ("600001.SH",))
        exclusions = {item.symbol: set(item.reasons) for item in result.excluded}
        self.assertEqual(
            exclusions["600002.SH"],
            {
                HardFilterReason.UNKNOWN_TRADABILITY,
                HardFilterReason.UNKNOWN_ST_STATUS,
                HardFilterReason.UNKNOWN_SUSPENSION_STATUS,
                HardFilterReason.MISSING_LISTING_AGE,
                HardFilterReason.MISSING_PRICE,
                HardFilterReason.MISSING_SESSION_AMOUNT,
                HardFilterReason.MISSING_MARKET_CAP,
            },
        )
        self.assertTrue(
            {
                HardFilterReason.NOT_TRADABLE,
                HardFilterReason.ST_SECURITY,
                HardFilterReason.SUSPENDED,
                HardFilterReason.INSUFFICIENT_LISTING_AGE,
                HardFilterReason.PRICE_BELOW_MINIMUM,
                HardFilterReason.LOW_SESSION_AMOUNT,
                HardFilterReason.LOW_MARKET_CAP,
            }.issubset(exclusions["600003.SH"])
        )

    def test_duplicate_universe_symbols_are_rejected(self) -> None:
        duplicate = universe_record("600001.SH")
        with self.assertRaisesRegex(ValueError, "unique"):
            hard_filter_ashare_universe((duplicate, duplicate))


class AShareFactorRankingTests(TestCase):
    def test_directional_percentiles_and_contributions_are_auditable(self) -> None:
        eligible = tuple(universe_record(f"60000{index}.SH") for index in range(1, 4))
        factors = (
            factor_record(
                "600001.SH",
                {
                    ScreeningFactorId.MOMENTUM_20: 0.30,
                    ScreeningFactorId.ANNUALIZED_VOLATILITY_60: 0.10,
                },
            ),
            factor_record(
                "600002.SH",
                {
                    ScreeningFactorId.MOMENTUM_20: 0.20,
                    ScreeningFactorId.ANNUALIZED_VOLATILITY_60: 0.20,
                },
            ),
            factor_record(
                "600003.SH",
                {
                    ScreeningFactorId.MOMENTUM_20: 0.10,
                    ScreeningFactorId.ANNUALIZED_VOLATILITY_60: 0.30,
                },
            ),
        )

        ranking = rank_ashare_factor_cross_section(
            eligible,
            factors,
            config=two_factor_config(),
        )

        self.assertEqual(
            tuple(item.symbol for item in ranking.ranked_candidates),
            ("600001.SH", "600002.SH", "600003.SH"),
        )
        self.assertEqual(
            tuple(item.composite_score for item in ranking.ranked_candidates),
            (1.0, 0.0, -1.0),
        )
        best = ranking.ranked_candidates[0]
        self.assertEqual(best.data_status, ScreeningCandidateDataStatus.COMPLETE)
        self.assertEqual(
            tuple(item.contribution for item in best.factor_contributions),
            (0.60, 0.40),
        )

    def test_winsorization_keeps_raw_and_clipped_values_separate(self) -> None:
        spec = ScreeningFactorSpec(
            ScreeningFactorId.MOMENTUM_20,
            1.0,
            ScreeningFactorDirection.HIGHER_IS_BETTER,
        )
        config = replace(
            AShareScreeningConfig(),
            factor_specs=(spec,),
            min_cross_section_observations=5,
            min_factor_weight_coverage=1.0,
            lower_winsor_quantile=0.20,
            upper_winsor_quantile=0.80,
        )
        symbols = tuple(f"6000{index:02d}.SH" for index in range(1, 6))
        eligible = tuple(universe_record(symbol) for symbol in symbols)
        raw_values = (0.0, 1.0, 2.0, 3.0, 100.0)
        factors = tuple(
            factor_record(symbol, {ScreeningFactorId.MOMENTUM_20: value})
            for symbol, value in zip(symbols, raw_values, strict=True)
        )

        ranking = rank_ashare_factor_cross_section(eligible, factors, config=config)

        top = ranking.ranked_candidates[0].factor_contributions[0]
        self.assertEqual(top.raw_value, 100.0)
        self.assertLess(top.winsorized_value or math.inf, 100.0)
        self.assertEqual(top.status, FactorObservationStatus.AVAILABLE)

    def test_missing_factor_is_not_neutral_filled(self) -> None:
        config = two_factor_config(min_factor_weight_coverage=0.60)
        eligible = tuple(universe_record(f"60000{index}.SH") for index in range(1, 4))
        factors = (
            factor_record(
                "600001.SH",
                {ScreeningFactorId.MOMENTUM_20: 0.30},
            ),
            factor_record(
                "600002.SH",
                {
                    ScreeningFactorId.MOMENTUM_20: 0.20,
                    ScreeningFactorId.ANNUALIZED_VOLATILITY_60: 0.20,
                },
            ),
            factor_record(
                "600003.SH",
                {
                    ScreeningFactorId.MOMENTUM_20: 0.10,
                    ScreeningFactorId.ANNUALIZED_VOLATILITY_60: 0.30,
                },
            ),
        )

        ranking = rank_ashare_factor_cross_section(eligible, factors, config=config)

        candidate = next(
            item for item in ranking.ranked_candidates if item.symbol == "600001.SH"
        )
        volatility = next(
            item
            for item in candidate.factor_contributions
            if item.factor_id is ScreeningFactorId.ANNUALIZED_VOLATILITY_60
        )
        self.assertEqual(candidate.data_status, ScreeningCandidateDataStatus.DEGRADED)
        self.assertAlmostEqual(candidate.factor_weight_coverage, 0.60)
        self.assertIsNone(volatility.percentile_rank)
        self.assertIsNone(volatility.contribution)
        self.assertIn(
            "INSUFFICIENT_CROSS_SECTION:ANNUALIZED_VOLATILITY_60",
            candidate.degradation_reasons,
        )

        strict = rank_ashare_factor_cross_section(
            eligible,
            factors,
            config=two_factor_config(min_factor_weight_coverage=0.80),
        )
        strict_candidate = next(
            item
            for item in strict.insufficient_candidates
            if item.symbol == "600001.SH"
        )
        self.assertIsNone(strict_candidate.composite_score)
        self.assertEqual(
            strict_candidate.data_status,
            ScreeningCandidateDataStatus.INSUFFICIENT,
        )

    def test_ties_are_deterministic_and_input_order_independent(self) -> None:
        spec = ScreeningFactorSpec(
            ScreeningFactorId.MOMENTUM_20,
            1.0,
            ScreeningFactorDirection.HIGHER_IS_BETTER,
        )
        config = replace(
            AShareScreeningConfig(),
            factor_specs=(spec,),
            min_cross_section_observations=3,
            min_factor_weight_coverage=1.0,
        )
        records = tuple(universe_record(f"60000{index}.SH") for index in (3, 1, 2))
        factors = tuple(
            factor_record(item.symbol, {ScreeningFactorId.MOMENTUM_20: 0.10})
            for item in reversed(records)
        )

        ranking = rank_ashare_factor_cross_section(records, factors, config=config)

        self.assertEqual(
            tuple(item.symbol for item in ranking.ranked_candidates),
            ("600001.SH", "600002.SH", "600003.SH"),
        )
        self.assertTrue(
            all(item.composite_score == 0.0 for item in ranking.ranked_candidates)
        )


class FakeScreeningData:
    def __init__(
        self,
        universe: AShareUniverseSnapshot,
        factors: AShareFactorSnapshot,
    ) -> None:
        self.universe = universe
        self.factors = factors
        self.factor_requests: list[tuple[str, ...]] = []

    async def fetch_universe_snapshot(
        self,
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareUniverseSnapshot:
        return self.universe

    async def fetch_factor_snapshot(
        self,
        symbols: Sequence[str],
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareFactorSnapshot:
        self.factor_requests.append(tuple(symbols))
        return self.factors


def universe_snapshot(
    records: tuple[AShareUniverseRecord, ...],
    *,
    available_at: datetime = DECISION_AT - timedelta(minutes=15),
    quality: ScreeningSourceQuality = ScreeningSourceQuality.COMPLETE,
) -> AShareUniverseSnapshot:
    observed_at = max(available_at, DECISION_AT + timedelta(seconds=1))
    return AShareUniverseSnapshot(
        as_of=AS_OF,
        available_at=available_at,
        observed_at=observed_at,
        source_id="full-market-test",
        source_revision="u1",
        records=records,
        quality=quality,
    )


def factor_snapshot(
    records: tuple[AShareFactorRecord, ...],
    *,
    available_at: datetime = DECISION_AT - timedelta(minutes=5),
    history_policy: ScreeningHistoryPolicy = (
        ScreeningHistoryPolicy.UNADJUSTED_WITH_CORPORATE_ACTION_GUARD
    ),
) -> AShareFactorSnapshot:
    observed_at = max(available_at, DECISION_AT + timedelta(seconds=2))
    return AShareFactorSnapshot(
        as_of=AS_OF,
        available_at=available_at,
        observed_at=observed_at,
        source_id="factor-test",
        source_revision="f1",
        feature_version="test@1",
        history_policy=history_policy,
        records=records,
    )


class AShareScreeningServiceTests(IsolatedAsyncioTestCase):
    async def test_three_layers_fetch_factors_only_for_hard_filter_survivors(self) -> None:
        eligible = tuple(universe_record(f"60000{index}.SH") for index in range(1, 4))
        rejected = universe_record("600004.SH", is_st=True)
        factors = tuple(
            factor_record(
                item.symbol,
                {
                    ScreeningFactorId.MOMENTUM_20: float(4 - index),
                    ScreeningFactorId.ANNUALIZED_VOLATILITY_60: float(index),
                },
            )
            for index, item in enumerate(eligible, start=1)
        )
        source = FakeScreeningData(
            universe_snapshot(eligible + (rejected,)),
            factor_snapshot(factors),
        )
        service = AShareScreeningService(source, config=two_factor_config())

        result = await service.run(as_of=AS_OF, decision_at=DECISION_AT)

        self.assertEqual(result.status, AShareScreeningRunStatus.COMPLETE)
        self.assertEqual(result.universe_count, 4)
        self.assertEqual(result.eligible_count, 3)
        self.assertEqual(result.ranked_count, 3)
        self.assertEqual(
            source.factor_requests,
            [("600001.SH", "600002.SH", "600003.SH")],
        )
        self.assertEqual(len(result.top_candidates), 2)
        self.assertEqual(result.top_candidates[0].symbol, "600001.SH")
        self.assertNotIn("600004.SH", {item.symbol for item in result.top_candidates})

    async def test_factor_budget_is_explicit_and_uses_session_amount_order(self) -> None:
        records = (
            universe_record("600001.SH", average_amount=Decimal("30000000")),
            universe_record("600002.SH", average_amount=Decimal("90000000")),
            universe_record("600003.SH", average_amount=Decimal("60000000")),
            universe_record("600004.SH", average_amount=Decimal("60000000")),
        )
        requested_symbols = ("600002.SH", "600003.SH", "600004.SH")
        factors = tuple(
            factor_record(
                symbol,
                {
                    ScreeningFactorId.MOMENTUM_20: float(index),
                    ScreeningFactorId.ANNUALIZED_VOLATILITY_60: float(4 - index),
                },
            )
            for index, symbol in enumerate(requested_symbols, start=1)
        )
        source = FakeScreeningData(
            universe_snapshot(records),
            factor_snapshot(factors),
        )
        config = replace(two_factor_config(), max_factor_candidates=3)

        result = await AShareScreeningService(source, config=config).run(
            as_of=AS_OF,
            decision_at=DECISION_AT,
        )

        self.assertEqual(result.hard_filter_eligible_count, 4)
        self.assertEqual(result.factor_requested_count, 3)
        self.assertEqual(source.factor_requests, [requested_symbols])
        self.assertEqual(
            tuple(item.symbol for item in result.factor_budget_deferred),
            ("600001.SH",),
        )
        self.assertIn("FACTOR_BUDGET_DEFERRED", result.warnings)

    async def test_low_historical_average_amount_is_a_second_layer_exclusion(self) -> None:
        records = tuple(universe_record(f"60000{index}.SH") for index in range(1, 4))
        factors = tuple(
            factor_record(
                record.symbol,
                {
                    ScreeningFactorId.AVERAGE_AMOUNT_20_CNY: (
                        1.0 if index == 3 else 100_000_000.0
                    ),
                    ScreeningFactorId.MOMENTUM_20: float(index),
                    ScreeningFactorId.ANNUALIZED_VOLATILITY_60: float(index),
                },
            )
            for index, record in enumerate(records, start=1)
        )
        source = FakeScreeningData(
            universe_snapshot(records),
            factor_snapshot(factors),
        )

        result = await AShareScreeningService(
            source,
            config=two_factor_config(min_cross_section_observations=2),
        ).run(as_of=AS_OF, decision_at=DECISION_AT)

        self.assertEqual(result.hard_filter_eligible_count, 3)
        self.assertEqual(result.factor_requested_count, 3)
        self.assertEqual(result.eligible_count, 2)
        self.assertEqual(
            tuple(
                item.symbol
                for item in result.factor_ranking.factor_eligibility_exclusions
            ),
            ("600003.SH",),
        )

    async def test_no_survivor_skips_expensive_factor_fetch(self) -> None:
        source = FakeScreeningData(
            universe_snapshot((universe_record("600001.SH", is_st=True),)),
            factor_snapshot(()),
        )
        service = AShareScreeningService(source, config=two_factor_config())

        result = await service.run(as_of=AS_OF, decision_at=DECISION_AT)

        self.assertEqual(result.status, AShareScreeningRunStatus.NO_ELIGIBLE_UNIVERSE)
        self.assertEqual(source.factor_requests, [])
        self.assertEqual(result.top_candidates, ())

    async def test_rejects_future_batches_and_currently_adjusted_history(self) -> None:
        record = universe_record("600001.SH")
        future_source = FakeScreeningData(
            universe_snapshot(
                (record,),
                available_at=DECISION_AT + timedelta(seconds=1),
            ),
            factor_snapshot(()),
        )
        with self.assertRaises(AShareScreeningPointInTimeError) as future:
            await AShareScreeningService(future_source).run(
                as_of=AS_OF,
                decision_at=DECISION_AT,
            )
        self.assertEqual(future.exception.code, "UNIVERSE_AVAILABLE_IN_FUTURE")

        unsafe_source = FakeScreeningData(
            universe_snapshot((record,)),
            factor_snapshot(
                (),
                history_policy=ScreeningHistoryPolicy.CURRENTLY_ADJUSTED,
            ),
        )
        with self.assertRaises(AShareScreeningPointInTimeError) as unsafe:
            await AShareScreeningService(unsafe_source).run(
                as_of=AS_OF,
                decision_at=DECISION_AT,
            )
        self.assertEqual(
            unsafe.exception.code,
            "UNSAFE_CURRENTLY_ADJUSTED_HISTORY",
        )

    async def test_degraded_source_propagates_to_candidate_and_run(self) -> None:
        records = tuple(universe_record(f"60000{index}.SH") for index in range(1, 4))
        factors = tuple(
            factor_record(
                record.symbol,
                {
                    ScreeningFactorId.MOMENTUM_20: float(index),
                    ScreeningFactorId.ANNUALIZED_VOLATILITY_60: float(4 - index),
                },
            )
            for index, record in enumerate(records, start=1)
        )
        source = FakeScreeningData(
            universe_snapshot(records, quality=ScreeningSourceQuality.DEGRADED),
            factor_snapshot(factors),
        )

        result = await AShareScreeningService(
            source,
            config=two_factor_config(),
        ).run(as_of=AS_OF, decision_at=DECISION_AT)

        self.assertEqual(result.status, AShareScreeningRunStatus.DEGRADED)
        self.assertIn("UNIVERSE_SOURCE_DEGRADED", result.warnings)
        self.assertTrue(
            all(
                candidate.data_status is ScreeningCandidateDataStatus.DEGRADED
                for candidate in result.top_candidates
            )
        )
