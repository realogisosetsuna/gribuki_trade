"""抗信息泄漏且可解释的策略实验基础组件。

本模块有意保持券商无关并仅供研究使用。它冻结实验所用数据与策略标识，
构建按时间顺序滚动前进的开发折，并在仅凭验证指标选出候选之前始终禁止
访问最终留出集。
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal

from gribuki_trade.strategy_lab import experiment_serialization as _serialization
from gribuki_trade.strategy_lab.experiment_models import (
    CostScenario,
    DataManifest,
    ExperimentObjective,
    FoldEvaluation,
    HoldoutEvaluation,
    ScenarioEvaluation,
    StrategyEvaluator,
    StrategyExperiment,
    StrategyManifest,
    StrategyWeights,
    TrialResult,
    WalkForwardConfig,
    WalkForwardFold,
    WalkForwardPlan,
    WeightConstraints,
    WeightGridConfig,
)
from gribuki_trade.strategy_lab.experiment_models import PerformanceMetrics as _PerformanceMetrics

# 历史导入路径继续可用；归档格式只由独立编码器实现。
_experiment_document = _serialization.experiment_document
experiment_to_json = _serialization.experiment_to_json
_scenario_evaluations_document = _serialization.scenario_evaluations_document
_optional_decimal = _serialization.optional_decimal
_sha256_document = _serialization.sha256_document
PerformanceMetrics = _PerformanceMetrics

def build_walk_forward_plan(
    timestamps: Sequence[date],
    config: WalkForwardConfig,
) -> WalkForwardPlan:
    """构建按时间扩展的折，以及一个从未触碰的最终留出集。

    清除区将每个训练窗口与其验证窗口隔开。禁运区标记每个验证窗口之后
    紧邻的观测，并在最终测试前再预留一个完整间隔。测试索引绝不会出现
    在开发折中。
    """

    resolved = tuple(timestamps)
    if not resolved:
        raise ValueError("timestamps must not be empty")
    if tuple(sorted(resolved)) != resolved or len(set(resolved)) != len(resolved):
        raise ValueError("timestamps must be strictly increasing and unique")

    test_start = len(resolved) - config.test_size
    final_embargo_start = test_start - config.embargo_size
    if final_embargo_start <= 0:
        raise ValueError("dataset is too short for embargo and final test")

    folds: list[WalkForwardFold] = []
    train_end = config.initial_train_size
    while True:
        purge_start = train_end
        validation_start = purge_start + config.purge_size
        validation_end = validation_start + config.validation_size
        if validation_end > final_embargo_start:
            break
        embargo_end = min(validation_end + config.embargo_size, final_embargo_start)
        folds.append(
            WalkForwardFold(
                fold_id=f"wf-{len(folds) + 1:03d}",
                train_indices=tuple(range(0, train_end)),
                purge_indices=tuple(range(purge_start, validation_start)),
                validation_indices=tuple(range(validation_start, validation_end)),
                embargo_indices=tuple(range(validation_end, embargo_end)),
            )
        )
        train_end += config.step_size

    if len(folds) < config.minimum_folds:
        raise ValueError("dataset is too short for the requested minimum fold count")
    test_indices = tuple(range(test_start, len(resolved)))
    if any(index in test_indices for fold in folds for index in fold.validation_indices):
        raise AssertionError("internal error: development and holdout overlap")
    return WalkForwardPlan(
        timestamps=resolved,
        config=config,
        folds=tuple(folds),
        final_embargo_indices=tuple(range(final_embargo_start, test_start)),
        test_indices=test_indices,
    )


def generate_simplex_weight_grid(
    constraints: WeightConstraints,
    config: WeightGridConfig | None = None,
) -> tuple[StrategyWeights, ...]:
    """枚举确定且有界的单纯形网格。

    枚举绝不静默截断：超过 ``max_candidates`` 的空间会被拒绝，从而保证
    记录的试验数量与实际测试数量一致。
    """

    resolved = config or WeightGridConfig()
    units = int(Decimal("1") / resolved.step)
    technical_cap = int(
        (constraints.max_technical_family_weight / resolved.step).to_integral_value(
            rounding="ROUND_FLOOR"
        )
    )
    macro_min = int(
        (constraints.min_macro_weight / resolved.step).to_integral_value(
            rounding="ROUND_CEILING"
        )
    )
    macro_max = int(
        (constraints.max_macro_weight / resolved.step).to_integral_value(
            rounding="ROUND_FLOOR"
        )
    )
    output: list[StrategyWeights] = []

    def append_compositions(prefix: tuple[int, ...], remaining: int) -> None:
        family_index = len(prefix)
        if family_index == len(constraints.technical_family_ids) - 1:
            if 0 <= remaining <= technical_cap:
                vector = (*prefix, remaining)
                technical = tuple(
                    (family, Decimal(value) * resolved.step)
                    for family, value in zip(
                        constraints.technical_family_ids,
                        vector,
                        strict=True,
                    )
                )
                macro = Decimal(units - sum(vector)) * resolved.step
                output.append(StrategyWeights(technical=technical, macro=macro))
                if len(output) > resolved.max_candidates:
                    raise ValueError("simplex grid exceeds max_candidates")
            return
        for value in range(min(technical_cap, remaining) + 1):
            append_compositions((*prefix, value), remaining - value)

    for macro_units in range(macro_min, macro_max + 1):
        append_compositions((), units - macro_units)
    if not output:
        raise ValueError("weight constraints produce an empty simplex grid")
    return tuple(output)


def validate_weights(
    weights: StrategyWeights,
    constraints: WeightConstraints,
) -> None:
    if tuple(name for name, _ in weights.technical) != constraints.technical_family_ids:
        raise ValueError("weight vector does not match constrained technical families")
    if not constraints.min_macro_weight <= weights.macro <= constraints.max_macro_weight:
        raise ValueError("macro weight is outside configured bounds")
    if any(
        value > constraints.max_technical_family_weight
        for _, value in weights.technical
    ):
        raise ValueError("technical family weight exceeds configured cap")


def run_walk_forward_experiment(
    *,
    data_manifest: DataManifest,
    strategy_manifest: StrategyManifest,
    plan: WalkForwardPlan,
    constraints: WeightConstraints,
    candidates: Sequence[StrategyWeights],
    baseline: StrategyWeights,
    cost_scenarios: Sequence[CostScenario],
    evaluator: StrategyEvaluator,
    objective: ExperimentObjective = ExperimentObjective.NET_RETURN,
    created_at: datetime,
    experiment_version: str = "walk-forward-weight-search@1",
    max_trials: int = 10_000,
) -> StrategyExperiment:
    """只在验证集上选择，再于测试集上评估一次锁定的选择。

    训练指标仅保留用于诊断，绝不参与选择键。各成本场景的验证分数先跨折
    求平均，并以最差成本场景作为选择目标。只有选定试验标识固定后才能
    访问留出集。
    """

    resolved_created_at = _utc_time(created_at, "created_at")
    _require_identifier(experiment_version, "experiment_version")
    if max_trials < 1:
        raise ValueError("max_trials must be positive")
    if data_manifest.observation_count != len(plan.timestamps):
        raise ValueError("data manifest observation count does not match plan")
    if data_manifest.observed_start != plan.timestamps[0]:
        raise ValueError("data manifest start does not match plan")
    if data_manifest.observed_end != plan.timestamps[-1]:
        raise ValueError("data manifest end does not match plan")

    scenarios = tuple(cost_scenarios)
    if not scenarios or len({item.scenario_id for item in scenarios}) != len(scenarios):
        raise ValueError("cost scenarios must be non-empty and unique")
    ordered_scenarios = tuple(sorted(scenarios, key=lambda item: item.scenario_id))
    validate_weights(baseline, constraints)

    unique: dict[str, StrategyWeights] = {}
    for candidate in (*tuple(candidates), baseline):
        validate_weights(candidate, constraints)
        unique[candidate.fingerprint] = candidate
    ordered_candidates = tuple(unique[key] for key in sorted(unique))
    if not ordered_candidates or len(ordered_candidates) > max_trials:
        raise ValueError("candidate trial count is outside configured bounds")

    trials: list[TrialResult] = []
    for candidate in ordered_candidates:
        fold_results: list[FoldEvaluation] = []
        for fold in plan.folds:
            train = tuple(
                ScenarioEvaluation(
                    scenario_id=scenario.scenario_id,
                    metrics=evaluator.evaluate(
                        candidate,
                        fold.train_indices,
                        scenario,
                    ),
                )
                for scenario in ordered_scenarios
            )
            validation = tuple(
                ScenarioEvaluation(
                    scenario_id=scenario.scenario_id,
                    metrics=evaluator.evaluate(
                        candidate,
                        fold.validation_indices,
                        scenario,
                    ),
                )
                for scenario in ordered_scenarios
            )
            fold_results.append(
                FoldEvaluation(
                    fold_id=fold.fold_id,
                    train=train,
                    validation=validation,
                )
            )
        trial_id = f"trial-{candidate.fingerprint[:24]}"
        trials.append(
            TrialResult(
                trial_id=trial_id,
                weights=candidate,
                folds=tuple(fold_results),
                validation_objective=_development_objective(
                    tuple(fold_results),
                    ordered_scenarios,
                    objective,
                ),
                validation_family_contributions=_validation_contributions(
                    tuple(fold_results)
                ),
            )
        )

    ordered_trials = tuple(sorted(trials, key=lambda item: item.trial_id))
    selected = sorted(
        ordered_trials,
        key=lambda item: (-item.validation_objective, item.trial_id),
    )[0]
    baseline_trial = next(
        item for item in ordered_trials if item.weights.fingerprint == baseline.fingerprint
    )

    # 这是唯一一次将留出集索引传给评估器；此时选择已经不可逆地收敛为
    # 一个试验标识。
    selected_test = _evaluate_holdout(
        evaluator,
        selected.weights,
        plan.test_indices,
        ordered_scenarios,
    )
    baseline_test = (
        selected_test
        if baseline_trial.trial_id == selected.trial_id
        else _evaluate_holdout(
            evaluator,
            baseline,
            plan.test_indices,
            ordered_scenarios,
        )
    )
    holdout = HoldoutEvaluation(
        selected=selected_test,
        selected_objective=_holdout_objective(selected_test, objective),
        baseline=baseline_test,
        baseline_objective=_holdout_objective(baseline_test, objective),
    )
    warnings = _experiment_warnings(ordered_trials, plan)
    identity = {
        "created_at": resolved_created_at.isoformat(),
        "data_manifest": data_manifest.manifest_sha256,
        "strategy_manifest": strategy_manifest.manifest_sha256,
        "experiment_version": experiment_version,
        "objective": objective.value,
        "selected_trial_id": selected.trial_id,
        "trial_ids": [item.trial_id for item in ordered_trials],
    }
    experiment_id = f"strategy-exp-{_sha256_document(identity)[:24]}"
    return StrategyExperiment(
        experiment_id=experiment_id,
        created_at=resolved_created_at,
        experiment_version=experiment_version,
        data_manifest=data_manifest,
        strategy_manifest=strategy_manifest,
        walk_forward_plan=plan,
        constraints=constraints,
        objective=objective,
        cost_scenarios=ordered_scenarios,
        trials=ordered_trials,
        selected_trial_id=selected.trial_id,
        baseline_trial_id=baseline_trial.trial_id,
        holdout=holdout,
        warnings=warnings,
    )


def _evaluate_holdout(
    evaluator: StrategyEvaluator,
    weights: StrategyWeights,
    indices: tuple[int, ...],
    scenarios: tuple[CostScenario, ...],
) -> tuple[ScenarioEvaluation, ...]:
    return tuple(
        ScenarioEvaluation(
            scenario_id=scenario.scenario_id,
            metrics=evaluator.evaluate(weights, indices, scenario),
        )
        for scenario in scenarios
    )


def _development_objective(
    folds: tuple[FoldEvaluation, ...],
    scenarios: tuple[CostScenario, ...],
    objective: ExperimentObjective,
) -> Decimal:
    scenario_means = []
    for scenario in scenarios:
        values = tuple(
            next(
                item.metrics.objective_value(objective)
                for item in fold.validation
                if item.scenario_id == scenario.scenario_id
            )
            for fold in folds
        )
        scenario_means.append(sum(values, Decimal("0")) / Decimal(len(values)))
    return min(scenario_means)


def _holdout_objective(
    results: tuple[ScenarioEvaluation, ...],
    objective: ExperimentObjective,
) -> Decimal:
    return min(item.metrics.objective_value(objective) for item in results)


def _validation_contributions(
    folds: tuple[FoldEvaluation, ...],
) -> tuple[tuple[str, Decimal], ...]:
    grouped: dict[str, list[Decimal]] = {}
    for fold in folds:
        for scenario in fold.validation:
            for family_id, contribution in scenario.metrics.family_contributions:
                grouped.setdefault(family_id, []).append(contribution)
    return tuple(
        (family_id, sum(values, Decimal("0")) / Decimal(len(values)))
        for family_id, values in sorted(grouped.items())
    )


def _experiment_warnings(
    trials: tuple[TrialResult, ...],
    plan: WalkForwardPlan,
) -> tuple[str, ...]:
    warnings = ["RESEARCH_ONLY_NOT_APPROVED_FOR_ONLINE_AUTO_TUNING"]
    if len(trials) > 1:
        warnings.append("MULTIPLE_HYPOTHESIS_SELECTION_BIAS")
    validation_observations = len(
        {
            index
            for fold in plan.folds
            for index in fold.validation_indices
        }
    )
    if len(trials) > validation_observations:
        warnings.append("TRIAL_COUNT_EXCEEDS_UNIQUE_VALIDATION_OBSERVATIONS")
    if len(trials) >= 100:
        warnings.append("LARGE_SEARCH_SPACE_OVERFITTING_RISK")
    return tuple(warnings)


def _utc_time(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _require_finite(value: Decimal, name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")


def _require_sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$")


def _require_identifier(value: str, name: str) -> None:
    if not isinstance(value, str) or _SAFE_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a safe non-empty identifier")


def _normalized_unique_pairs_or_ids(values: Sequence[str], name: str) -> tuple[str, ...]:
    normalized = tuple(sorted(values))
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{name} must contain unique values")
    for value in normalized:
        _require_identifier(value, name)
    return normalized


def _normalized_pairs(
    values: Sequence[tuple[str, str]],
    name: str,
) -> tuple[tuple[str, str], ...]:
    normalized = tuple(sorted(values))
    if len({key for key, _ in normalized}) != len(normalized):
        raise ValueError(f"{name} must contain unique keys")
    for key, value in normalized:
        _require_identifier(key, f"{name} key")
        if not isinstance(value, str) or not value or len(value) > 2_000:
            raise ValueError(f"{name} values must be bounded non-empty strings")
    return normalized


__all__ = [
    "CostScenario",
    "DataManifest",
    "ExperimentObjective",
    "FoldEvaluation",
    "HoldoutEvaluation",
    "PerformanceMetrics",
    "ScenarioEvaluation",
    "StrategyEvaluator",
    "StrategyExperiment",
    "StrategyManifest",
    "StrategyWeights",
    "TrialResult",
    "WalkForwardConfig",
    "WalkForwardFold",
    "WalkForwardPlan",
    "WeightConstraints",
    "WeightGridConfig",
    "build_walk_forward_plan",
    "generate_simplex_weight_grid",
    "validate_weights",
    "run_walk_forward_experiment",
    "experiment_to_json",
]
