from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.backtest.costs import InstrumentType
from gribuki_trade.strategy_lab import (
    AShareDailyEvaluatorConfig,
    AShareDailyStrategyEvaluator,
    AShareDailyStrategyObservation,
    AShareEvaluationAction,
    AShareExecutionPolicy,
    CompletedAShareDailyBar,
    CostScenario,
    DataManifest,
    PITStrategyScore,
    StrategyManifest,
    StrategyWeights,
    WalkForwardConfig,
    WeightConstraints,
    ashare_evaluation_source_revisions,
    build_walk_forward_plan,
    canonical_ashare_evaluation_content,
    run_walk_forward_experiment,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _observation(
    index: int,
    score: str,
    *,
    macro: str = "0",
    open_: str = "10",
    close: str = "10",
    volume: int = 1_000_000,
    suspended: bool = False,
    with_price_limits: bool = True,
    buy_limit: str | None = None,
    sell_limit: str | None = None,
    bar_revision: str = "daily-r1",
    feature_revision: str = "momentum-r1",
    macro_revision: str = "macro-r1",
) -> AShareDailyStrategyObservation:
    signal_date = date(2026, 1, 1) + timedelta(days=index * 2)
    session_date = signal_date + timedelta(days=1)
    signal_at = datetime.combine(signal_date, time(15, 10), tzinfo=_SHANGHAI)
    completed_at = datetime.combine(session_date, time(15, 5), tzinfo=_SHANGHAI)
    open_price = Decimal(open_)
    close_price = Decimal(close)
    return AShareDailyStrategyObservation(
        observation_id=f"obs-{index:03d}",
        symbol="600000.SH",
        instrument_type=InstrumentType.STOCK,
        signal_as_of=signal_at,
        technical_scores=(
            PITStrategyScore(
                family_id="momentum",
                value=Decimal(score),
                known_at=signal_at,
                source_id="features",
                source_revision=feature_revision,
            ),
        ),
        macro_score=PITStrategyScore(
            family_id="macro",
            value=Decimal(macro),
            known_at=signal_at,
            source_id="deepseek",
            source_revision=macro_revision,
        ),
        execution_bar=CompletedAShareDailyBar(
            session_date=session_date,
            open=open_price,
            high=max(open_price, close_price),
            low=min(open_price, close_price),
            close=close_price,
            volume_shares=volume,
            suspended=suspended,
            lower_price_limit=Decimal("8") if with_price_limits else None,
            upper_price_limit=Decimal("12") if with_price_limits else None,
            completed_at=completed_at,
            source_id="baostock",
            source_revision=bar_revision,
        ),
        buy_limit_price=Decimal(buy_limit) if buy_limit is not None else None,
        sell_limit_price=Decimal(sell_limit) if sell_limit is not None else None,
    )


def _evaluator(
    observations: tuple[AShareDailyStrategyObservation, ...],
    *,
    config: AShareDailyEvaluatorConfig | None = None,
    frozen_at: datetime | None = None,
) -> AShareDailyStrategyEvaluator:
    resolved_config = config or AShareDailyEvaluatorConfig()
    manifest = DataManifest.freeze(
        dataset_id="pudong-bank-pit",
        schema_version="ashare-daily-evaluation@1",
        observation_count=len(observations),
        observed_start=observations[0].signal_date,
        observed_end=observations[-1].signal_date,
        frozen_at=frozen_at
        or observations[-1].execution_bar.completed_at + timedelta(minutes=1),
        feature_ids=("momentum", "macro"),
        label_id=resolved_config.label_id,
        canonical_content=canonical_ashare_evaluation_content(observations),
        source_revisions=ashare_evaluation_source_revisions(observations),
    )
    strategy = StrategyManifest(
        strategy_version=resolved_config.strategy_version,
        code_revision="evaluator-code-r1",
        parameters=resolved_config.manifest_parameters,
        factor_expressions=(("momentum", "return(close,20)"),),
    )
    return AShareDailyStrategyEvaluator(
        observations,
        data_manifest=manifest,
        strategy_manifest=strategy,
        config=resolved_config,
    )


def _weights(technical: str = "0.8", macro: str = "0.2") -> StrategyWeights:
    return StrategyWeights(
        technical=(("momentum", Decimal(technical)),),
        macro=Decimal(macro),
    )


def _costs(
    *, commission: str = "0", slippage: str = "0", tax: str = "0"
) -> CostScenario:
    return CostScenario(
        "costs",
        commission_bps=Decimal(commission),
        slippage_bps=Decimal(slippage),
        tax_bps=Decimal(tax),
    )


def test_deterministic_next_open_buy_t_plus_one_sell_and_metrics() -> None:
    observations = (
        _observation(0, "1", open_="10", close="11"),
        _observation(1, "-1", open_="11", close="10"),
    )
    evaluator = _evaluator(observations)

    first = evaluator.evaluate_with_trace(_weights(), (0, 1), _costs())
    second = evaluator.evaluate_with_trace(_weights(), (0, 1), _costs())

    assert first == second
    assert [event.action for event in first.events] == [
        AShareEvaluationAction.BUY,
        AShareEvaluationAction.SELL,
    ]
    assert first.events[0].quantity % 100 == 0
    assert first.metrics.trade_count == 2
    assert first.metrics.hit_rate == Decimal("1")
    assert first.metrics.net_return > 0
    assert len(first.data_manifest_sha256) == 64
    assert len(first.strategy_manifest_sha256) == 64


def test_cost_and_slippage_scenario_reduce_realized_performance() -> None:
    observations = (
        _observation(0, "1", open_="10", close="10.5"),
        _observation(1, "-1", open_="10.5", close="10.5"),
    )
    evaluator = _evaluator(observations)

    free = evaluator.evaluate(_weights(), (0, 1), _costs())
    stressed = evaluator.evaluate(
        _weights(),
        (0, 1),
        _costs(commission="10", slippage="20", tax="5"),
    )

    assert stressed.net_return < free.net_return
    assert stressed.hit_rate == Decimal("1")


@pytest.mark.parametrize(
    ("observation", "expected"),
    [
        (_observation(0, "1", suspended=True), AShareEvaluationAction.NO_FILL_SUSPENDED),
        (
            _observation(0, "1", with_price_limits=False),
            AShareEvaluationAction.NO_FILL_PRICE_LIMIT_UNKNOWN,
        ),
        (
            _observation(0, "1", open_="12", close="12"),
            AShareEvaluationAction.NO_FILL_LIMIT_LOCKED,
        ),
        (_observation(0, "1", volume=9_900), AShareEvaluationAction.NO_FILL_VOLUME),
    ],
)
def test_execution_gates_fail_closed_without_fabricated_fills(
    observation: AShareDailyStrategyObservation,
    expected: AShareEvaluationAction,
) -> None:
    evaluator = _evaluator((observation,))
    result = evaluator.evaluate_with_trace(_weights(), (0,), _costs())

    assert result.events[0].action is expected
    assert result.metrics.trade_count == 0
    assert result.metrics.net_return == 0


def test_conservative_limit_policy_only_matches_at_an_acceptable_open() -> None:
    config = AShareDailyEvaluatorConfig(
        execution_policy=AShareExecutionPolicy.CONSERVATIVE_OPEN_LIMIT
    )
    rejected = _evaluator(
        (_observation(0, "1", open_="10", close="10", buy_limit="9.9"),),
        config=config,
    ).evaluate_with_trace(_weights(), (0,), _costs())
    accepted = _evaluator(
        (_observation(0, "1", open_="10", close="10", buy_limit="10"),),
        config=config,
    ).evaluate_with_trace(_weights(), (0,), _costs())

    assert rejected.events[0].action is AShareEvaluationAction.NO_FILL_ORDER_LIMIT
    assert accepted.events[0].action is AShareEvaluationAction.BUY


def test_future_known_score_is_rejected_at_observation_boundary() -> None:
    signal_at = datetime(2026, 1, 1, 15, 10, tzinfo=_SHANGHAI)
    bar = _observation(0, "1").execution_bar
    with pytest.raises(ValueError, match="known_at"):
        AShareDailyStrategyObservation(
            observation_id="future-score",
            symbol="600000.SH",
            instrument_type=InstrumentType.STOCK,
            signal_as_of=signal_at,
            technical_scores=(
                PITStrategyScore(
                    "momentum",
                    Decimal("1"),
                    signal_at + timedelta(minutes=1),
                    "features",
                    "r1",
                ),
            ),
            macro_score=PITStrategyScore(
                "macro", Decimal("0"), signal_at, "deepseek", "r1"
            ),
            execution_bar=bar,
        )


def test_non_contiguous_slice_is_rejected_instead_of_skipping_position_marks() -> None:
    evaluator = _evaluator(
        (_observation(0, "1"), _observation(1, "1"), _observation(2, "-1"))
    )

    with pytest.raises(ValueError, match="contiguous"):
        evaluator.evaluate(_weights(), (0, 2), _costs())


def test_manifest_content_and_freeze_time_mismatches_fail_closed() -> None:
    observations = (_observation(0, "1"),)
    valid = _evaluator(observations)
    assert valid.evaluate(_weights(), (0,), _costs()).trade_count == 1

    with pytest.raises(ValueError, match="frozen before"):
        _evaluator(
            observations,
            frozen_at=observations[0].execution_bar.completed_at - timedelta(minutes=1),
        )

    config = AShareDailyEvaluatorConfig()
    bad_manifest = DataManifest(
        dataset_id="bad-content",
        content_sha256="0" * 64,
        schema_version="ashare-daily-evaluation@1",
        observation_count=1,
        observed_start=observations[0].signal_date,
        observed_end=observations[0].signal_date,
        frozen_at=observations[0].execution_bar.completed_at + timedelta(minutes=1),
        feature_ids=("macro", "momentum"),
        label_id=config.label_id,
        source_revisions=ashare_evaluation_source_revisions(observations),
    )
    strategy = StrategyManifest(
        strategy_version=config.strategy_version,
        code_revision="r1",
        parameters=config.manifest_parameters,
        factor_expressions=(("momentum", "return(close,20)"),),
    )
    with pytest.raises(ValueError, match="content hash"):
        AShareDailyStrategyEvaluator(
            observations,
            data_manifest=bad_manifest,
            strategy_manifest=strategy,
            config=config,
        )


def test_manifest_accepts_per_observation_provider_revisions_without_key_collision() -> None:
    observations = (
        _observation(
            0,
            "1",
            bar_revision="daily-2026-01-02",
            feature_revision="feature-2026-01-01",
            macro_revision="macro-2026-01-01",
        ),
        _observation(
            1,
            "-1",
            bar_revision="daily-2026-01-04",
            feature_revision="feature-2026-01-03",
            macro_revision="macro-2026-01-03",
        ),
    )
    evaluator = _evaluator(observations)

    assert len(evaluator.data_manifest.source_revisions) == 6
    assert evaluator.evaluate(_weights(), (0, 1), _costs()).trade_count == 2


def test_etf_requires_explicit_zero_tax_cost_scenario() -> None:
    stock_observation = _observation(0, "1")
    etf_observation = AShareDailyStrategyObservation(
        observation_id=stock_observation.observation_id,
        symbol="510300.SH",
        instrument_type=InstrumentType.ETF,
        signal_as_of=stock_observation.signal_as_of,
        technical_scores=stock_observation.technical_scores,
        macro_score=stock_observation.macro_score,
        execution_bar=stock_observation.execution_bar,
    )
    evaluator = _evaluator((etf_observation,))

    assert evaluator.evaluate(_weights(), (0,), _costs(tax="0")).trade_count == 1
    with pytest.raises(ValueError, match="ETF.*tax_bps"):
        evaluator.evaluate(_weights(), (0,), _costs(tax="5"))


def test_real_evaluator_composes_with_walk_forward_baseline_and_holdout() -> None:
    observations = tuple(
        _observation(
            index,
            "1" if index % 3 != 2 else "-1",
            macro="-0.2" if index % 2 else "0.2",
            open_=str(10 + index / 10),
            close=str(10.1 + index / 10),
        )
        for index in range(14)
    )
    evaluator = _evaluator(observations)
    dates = tuple(item.signal_date for item in observations)
    plan = build_walk_forward_plan(
        dates,
        WalkForwardConfig(
            initial_train_size=3,
            validation_size=2,
            test_size=2,
            step_size=2,
            purge_size=1,
            embargo_size=1,
            label_horizon_sessions=1,
            minimum_folds=2,
        ),
    )
    candidate = _weights("0.8", "0.2")
    baseline = _weights("0.6", "0.4")

    experiment = run_walk_forward_experiment(
        data_manifest=evaluator.data_manifest,
        strategy_manifest=evaluator.strategy_manifest,
        plan=plan,
        constraints=WeightConstraints(
            technical_family_ids=("momentum",),
            max_technical_family_weight=Decimal("0.8"),
            max_macro_weight=Decimal("0.4"),
        ),
        candidates=(baseline, candidate),
        baseline=baseline,
        cost_scenarios=(_costs(),),
        evaluator=evaluator,
        created_at=datetime(2026, 3, 1, tzinfo=UTC),
    )

    assert experiment.selected_trial_id
    assert experiment.baseline_trial_id
    assert len(experiment.holdout.selected) == 1
    assert len(experiment.holdout.baseline) == 1
