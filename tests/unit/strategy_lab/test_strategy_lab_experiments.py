from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.strategy_lab.experiment_serialization import (
    experiment_document,
)
from gribuki_trade.strategy_lab.experiment_serialization import (
    experiment_to_json as serialize_experiment_to_json,
)
from gribuki_trade.strategy_lab.experiments import (
    CostScenario,
    DataManifest,
    ExperimentObjective,
    PerformanceMetrics,
    StrategyExperiment,
    StrategyManifest,
    StrategyWeights,
    WalkForwardConfig,
    WalkForwardPlan,
    WeightConstraints,
    WeightGridConfig,
    build_walk_forward_plan,
    experiment_to_json,
    generate_simplex_weight_grid,
    run_walk_forward_experiment,
    validate_weights,
)


def _dates(count: int = 60) -> tuple[date, ...]:
    start = date(2026, 1, 1)
    return tuple(start + timedelta(days=index) for index in range(count))


def _plan() -> WalkForwardPlan:
    return build_walk_forward_plan(
        _dates(),
        WalkForwardConfig(
            initial_train_size=10,
            validation_size=5,
            test_size=5,
            step_size=5,
            purge_size=2,
            embargo_size=2,
            label_horizon_sessions=2,
            minimum_folds=2,
        ),
    )


def _constraints() -> WeightConstraints:
    return WeightConstraints(
        technical_family_ids=("momentum", "trend"),
        max_technical_family_weight=Decimal("0.70"),
        max_macro_weight=Decimal("0.40"),
    )


def _weights(momentum: str, trend: str, macro: str = "0.20") -> StrategyWeights:
    return StrategyWeights(
        technical=(
            ("momentum", Decimal(momentum)),
            ("trend", Decimal(trend)),
        ),
        macro=Decimal(macro),
    )


def test_walk_forward_plan_separates_purge_embargo_and_final_holdout() -> None:
    plan = _plan()
    assert hasattr(plan, "folds")
    assert len(plan.folds) >= 2
    assert plan.test_indices == tuple(range(55, 60))
    assert plan.final_embargo_indices == (53, 54)
    for fold in plan.folds:
        assert max(fold.train_indices) < min(fold.purge_indices)
        assert max(fold.purge_indices) < min(fold.validation_indices)
        assert not set(fold.validation_indices) & set(plan.test_indices)


def test_data_manifest_freeze_hashes_exact_canonical_content() -> None:
    content = b"immutable point-in-time dataset"
    manifest = DataManifest.freeze(
        dataset_id="dataset",
        schema_version="bars@1",
        observation_count=2,
        observed_start=date(2026, 1, 1),
        observed_end=date(2026, 1, 2),
        frozen_at=datetime(2026, 1, 3, tzinfo=UTC),
        feature_ids=("volume", "close"),
        label_id="forward-return",
        canonical_content=content,
    )

    assert manifest.content_sha256 == hashlib.sha256(content).hexdigest()
    assert manifest.feature_ids == ("close", "volume")


def test_walk_forward_requires_gaps_covering_label_horizon() -> None:
    with pytest.raises(ValueError, match="purge_size"):
        WalkForwardConfig(
            initial_train_size=10,
            validation_size=5,
            test_size=5,
            step_size=5,
            purge_size=1,
            embargo_size=2,
            label_horizon_sessions=2,
        )


def test_simplex_grid_is_deterministic_bounded_and_complete() -> None:
    constraints = _constraints()
    config = WeightGridConfig(step=Decimal("0.20"), max_candidates=100)

    first = generate_simplex_weight_grid(constraints, config)
    second = generate_simplex_weight_grid(constraints, config)

    assert first == second
    assert first
    for candidate in first:
        validate_weights(candidate, constraints)
        assert candidate.macro <= Decimal("0.40")
        assert all(value <= Decimal("0.70") for _, value in candidate.technical)


def test_simplex_grid_never_silently_truncates_trials() -> None:
    with pytest.raises(ValueError, match="max_candidates"):
        generate_simplex_weight_grid(
            _constraints(),
            WeightGridConfig(step=Decimal("0.10"), max_candidates=1),
        )


class _LeakageSpyEvaluator:
    def __init__(self, test_indices: tuple[int, ...]) -> None:
        self.test_indices = test_indices
        self.calls: list[tuple[str, tuple[int, ...], str]] = []

    def evaluate(
        self,
        weights: StrategyWeights,
        observation_indices: tuple[int, ...],
        cost_scenario: CostScenario,
    ) -> PerformanceMetrics:
        self.calls.append((weights.fingerprint, observation_indices, cost_scenario.scenario_id))
        momentum = dict(weights.technical)["momentum"]
        # 最终留出集刻意偏向相反候选项；若它泄漏进模型选择，下面的测试就会
        # 选中错误候选项。
        base = Decimal("1") - momentum if observation_indices == self.test_indices else momentum
        cost_penalty = (
            cost_scenario.commission_bps
            + cost_scenario.slippage_bps
            + cost_scenario.tax_bps
        ) / Decimal("10000")
        score = base - cost_penalty
        return PerformanceMetrics(
            net_return=score,
            annualized_return=score,
            annualized_volatility=Decimal("0.20"),
            sharpe=score / Decimal("0.20"),
            max_drawdown=Decimal("0.10"),
            turnover=Decimal("1"),
            trade_count=10,
            hit_rate=Decimal("0.60"),
            family_contributions=(
                ("momentum", momentum),
                ("trend", dict(weights.technical)["trend"]),
                ("macro", weights.macro),
            ),
        )


def _experiment() -> tuple[
    StrategyExperiment,
    _LeakageSpyEvaluator,
    StrategyWeights,
    StrategyWeights,
]:
    plan = _plan()
    dates = _dates()
    high_momentum = _weights("0.60", "0.20")
    baseline = _weights("0.40", "0.40")
    evaluator = _LeakageSpyEvaluator(plan.test_indices)
    experiment = run_walk_forward_experiment(
        data_manifest=DataManifest(
            dataset_id="ashare-daily-pit",
            content_sha256="a" * 64,
            schema_version="daily-bars@1",
            observation_count=len(dates),
            observed_start=dates[0],
            observed_end=dates[-1],
            frozen_at=datetime(2026, 3, 2, tzinfo=UTC),
            feature_ids=("close", "volume"),
            label_id="forward-return-5d",
            source_revisions=(("baostock", "0.9.3"),),
        ),
        strategy_manifest=StrategyManifest(
            strategy_version="close-signal@2",
            code_revision="abc123",
            parameters=(("horizon", "5"),),
            factor_expressions=(("momentum", "return(close,20)"),),
        ),
        plan=plan,
        constraints=_constraints(),
        candidates=(baseline, high_momentum),
        baseline=baseline,
        cost_scenarios=(
            CostScenario(
                "base",
                commission_bps=Decimal("2"),
                slippage_bps=Decimal("3"),
                tax_bps=Decimal("5"),
            ),
            CostScenario(
                "stress",
                commission_bps=Decimal("4"),
                slippage_bps=Decimal("10"),
                tax_bps=Decimal("5"),
            ),
        ),
        evaluator=evaluator,
        objective=ExperimentObjective.NET_RETURN,
        created_at=datetime(2026, 3, 3, tzinfo=UTC),
    )
    return experiment, evaluator, high_momentum, baseline


def test_selection_uses_validation_only_then_touches_holdout_once_per_locked_strategy() -> None:
    experiment, evaluator, high_momentum, baseline = _experiment()
    selected = next(
        trial for trial in experiment.trials if trial.trial_id == experiment.selected_trial_id
    )

    assert selected.weights == high_momentum
    assert experiment.holdout.selected_objective < experiment.holdout.baseline_objective
    holdout_calls = [
        call
        for call in evaluator.calls
        if call[1] == experiment.walk_forward_plan.test_indices
    ]
    # 入选候选项与预注册基线各自运行两种成本情景。
    assert len(holdout_calls) == 4
    assert {call[0] for call in holdout_calls} == {
        high_momentum.fingerprint,
        baseline.fingerprint,
    }
    first_holdout_call = evaluator.calls.index(holdout_calls[0])
    assert all(
        indices != experiment.walk_forward_plan.test_indices
        for _, indices, _ in evaluator.calls[:first_holdout_call]
    )


def test_experiment_freezes_manifests_metrics_costs_and_overfit_warnings() -> None:
    experiment, _, _, _ = _experiment()
    document = experiment_to_json(experiment)

    assert experiment.trial_count == 2
    assert "MULTIPLE_HYPOTHESIS_SELECTION_BIAS" in experiment.warnings
    assert "RESEARCH_ONLY_NOT_APPROVED_FOR_ONLINE_AUTO_TUNING" in experiment.warnings
    assert '"data_manifest_sha256"' in document
    assert '"strategy_manifest_sha256"' in document
    assert '"validation_family_contributions"' in document
    assert '"stress"' in document
    assert '"baseline_trial_id"' in document


def test_experiment_serialization_facade_preserves_research_only_boundary() -> None:
    experiment, _, _, _ = _experiment()

    document = experiment_document(experiment)
    assert document["research_only"] is True
    assert serialize_experiment_to_json(experiment) == experiment_to_json(experiment)
    # 拆分前完整试验的归档摘要，防止同一实验 ID 的持久化 JSON 悄然漂移。
    assert hashlib.sha256(experiment_to_json(experiment).encode("utf-8")).hexdigest() == (
        "5268aea21bc7ff2dabf7d79d0b9275a3369f9db6fdb9dbbab6550bb556e6218e"
    )


def test_invalid_weight_caps_and_manifest_mismatch_fail_closed() -> None:
    with pytest.raises(ValueError, match="sum exactly"):
        StrategyWeights(
            technical=(("momentum", Decimal("0.5")),),
            macro=Decimal("0.2"),
        )
    with pytest.raises(ValueError, match="0.40"):
        WeightConstraints(
            technical_family_ids=("momentum", "trend"),
            max_technical_family_weight=Decimal("0.6"),
            max_macro_weight=Decimal("0.5"),
        )
