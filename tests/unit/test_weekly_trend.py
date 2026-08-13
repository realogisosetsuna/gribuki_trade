import math
from dataclasses import replace
from datetime import date
from unittest import TestCase

from gribuki_trade.strategy.weekly_trend import (
    SymbolDailySnapshot,
    WeeklyTrendConfig,
    build_weekly_trend_decision,
)

DECISION_DATE = date(2026, 8, 7)


def trend_prices(*, daily_growth: float = 0.001, length: int = 130) -> tuple[float, ...]:
    return tuple(10.0 * (1.0 + daily_growth) ** index for index in range(length))


def noisy_trend_prices(
    *, daily_growth: float = 0.0015, noise: float = 0.015, length: int = 130
) -> tuple[float, ...]:
    return tuple(
        10.0
        * (1.0 + daily_growth) ** index
        * (1.0 + noise * math.sin(index * math.pi / 2.0))
        for index in range(length)
    )


def snapshot(
    symbol: str,
    *,
    closes: tuple[float, ...] | None = None,
    turnover: float = 100_000_000.0,
    listing_days: int = 300,
    is_tradable: bool = True,
    is_st: bool = False,
    as_of: date = DECISION_DATE,
) -> SymbolDailySnapshot:
    return SymbolDailySnapshot(
        symbol=symbol,
        as_of=as_of,
        closes=trend_prices() if closes is None else closes,
        average_turnover_20_cny=turnover,
        listing_days=listing_days,
        is_tradable=is_tradable,
        is_st=is_st,
    )


class WeeklyTrendConfigTests(TestCase):
    def test_rejects_invalid_parameter_relationships(self) -> None:
        invalid_configs = (
            {"momentum_lookback_days": 5, "momentum_skip_days": 5},
            {"momentum_skip_days": -1},
            {"fast_ma_days": 60, "slow_ma_days": 60},
            {"volatility_days": 1},
            {"retention_rank": 9, "max_positions": 10},
            {"max_weight_per_symbol": 0.0},
            {"min_cash_weight": 1.0},
            {"min_average_turnover_20_cny": -1.0},
        )

        for overrides in invalid_configs:
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                WeeklyTrendConfig(**overrides)

    def test_snapshot_rejects_invalid_prices(self) -> None:
        with self.assertRaisesRegex(ValueError, "finite positive"):
            snapshot("BAD", closes=(10.0, float("nan")))


class WeeklyTrendDecisionTests(TestCase):
    def test_rejects_snapshot_from_another_date(self) -> None:
        stale = snapshot("STALE", as_of=date(2026, 8, 6))

        with self.assertRaisesRegex(ValueError, "decision_date"):
            build_weekly_trend_decision(DECISION_DATE, [stale])

    def test_filters_insufficient_history_liquidity_age_status_and_trend(self) -> None:
        inputs = [
            snapshot("VALID"),
            snapshot("SHORT", closes=trend_prices(length=120)),
            snapshot("ILLIQUID", turnover=49_999_999.0),
            snapshot("NEW", listing_days=249),
            snapshot("ST", is_st=True),
            snapshot("HALTED", is_tradable=False),
            snapshot("DOWNTREND", closes=trend_prices(daily_growth=-0.001)),
        ]

        decision = build_weekly_trend_decision(DECISION_DATE, inputs)

        self.assertEqual([item.symbol for item in decision.ranked_candidates], ["VALID"])
        self.assertEqual([target.symbol for target in decision.targets], ["VALID"])

    def test_listing_age_and_liquidity_thresholds_are_inclusive(self) -> None:
        boundary = snapshot("BOUNDARY", turnover=50_000_000.0, listing_days=250)

        decision = build_weekly_trend_decision(DECISION_DATE, [boundary])

        self.assertEqual([target.symbol for target in decision.targets], ["BOUNDARY"])

    def test_momentum_uses_day_120_to_day_5_and_ignores_last_five_closes(self) -> None:
        base = trend_prices()
        changed_recent = base[:-5] + tuple(price * 1.01 for price in base[-5:])

        base_decision = build_weekly_trend_decision(
            DECISION_DATE, [snapshot("BASE", closes=base)]
        )
        changed_decision = build_weekly_trend_decision(
            DECISION_DATE, [snapshot("CHANGED", closes=changed_recent)]
        )

        self.assertAlmostEqual(
            base_decision.ranked_candidates[0].momentum,
            changed_decision.ranked_candidates[0].momentum,
        )
        expected = base[-6] / base[-121] - 1.0
        self.assertAlmostEqual(base_decision.ranked_candidates[0].momentum, expected)

    def test_risk_adjusted_ranking_and_inverse_volatility_weighting(self) -> None:
        low_volatility = snapshot("LOW", closes=trend_prices(daily_growth=0.0015))
        high_volatility = snapshot("HIGH", closes=noisy_trend_prices())
        config = replace(
            WeeklyTrendConfig(),
            max_positions=2,
            retention_rank=2,
            max_weight_per_symbol=0.80,
        )

        decision = build_weekly_trend_decision(
            DECISION_DATE,
            [high_volatility, low_volatility],
            config=config,
        )
        metrics = {item.symbol: item for item in decision.ranked_candidates}
        weights = {target.symbol: target.weight for target in decision.targets}

        self.assertLess(
            metrics["LOW"].annualized_volatility,
            metrics["HIGH"].annualized_volatility,
        )
        self.assertEqual(decision.ranked_candidates[0].symbol, "LOW")
        self.assertGreater(weights["LOW"], weights["HIGH"])
        self.assertLessEqual(weights["LOW"], 0.80)

    def test_top_ten_with_rank_twenty_holding_buffer(self) -> None:
        inputs = [
            snapshot(f"S{index:02d}", closes=trend_prices(daily_growth=0.0005 + index / 100_000))
            for index in range(25)
        ]
        initial = build_weekly_trend_decision(DECISION_DATE, inputs)
        rank_to_symbol = {item.rank: item.symbol for item in initial.ranked_candidates}
        buffered_holding = rank_to_symbol[15]

        buffered = build_weekly_trend_decision(
            DECISION_DATE,
            inputs,
            current_holdings={buffered_holding},
        )
        selected_ranks = {target.rank for target in buffered.targets}

        self.assertEqual(len(buffered.targets), 10)
        self.assertEqual(selected_ranks, {*range(1, 10), 15})
        self.assertTrue(
            next(target for target in buffered.targets if target.rank == 15).was_held
        )

        dropped = build_weekly_trend_decision(
            DECISION_DATE,
            inputs,
            current_holdings={rank_to_symbol[21]},
        )
        self.assertEqual({target.rank for target in dropped.targets}, set(range(1, 11)))

    def test_weights_respect_cash_floor_cap_and_risk_scaling(self) -> None:
        inputs = [
            snapshot(f"S{index:02d}", closes=noisy_trend_prices(noise=0.002 + index / 1000))
            for index in range(10)
        ]

        decision = build_weekly_trend_decision(DECISION_DATE, inputs)

        self.assertEqual(len(decision.targets), 10)
        self.assertGreaterEqual(decision.cash_weight, 0.05 - 1e-12)
        self.assertAlmostEqual(
            sum(target.weight for target in decision.targets) + decision.cash_weight,
            1.0,
        )
        self.assertTrue(all(0 < target.weight <= 0.12 + 1e-12 for target in decision.targets))

    def test_fewer_symbols_leave_extra_cash_when_position_caps_bind(self) -> None:
        config = replace(WeeklyTrendConfig(), max_positions=3, retention_rank=3)

        decision = build_weekly_trend_decision(
            DECISION_DATE,
            [snapshot("A"), snapshot("B"), snapshot("C")],
            config=config,
        )

        self.assertAlmostEqual(sum(target.weight for target in decision.targets), 0.36)
        self.assertAlmostEqual(decision.cash_weight, 0.64)

    def test_duplicate_symbols_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "unique symbols"):
            build_weekly_trend_decision(
                DECISION_DATE,
                [snapshot("DUP"), snapshot("DUP")],
            )
