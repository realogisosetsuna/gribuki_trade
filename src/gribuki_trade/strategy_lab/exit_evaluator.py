"""退出策略的确定性离线评估、滚动验证与试验登记。

本模块只重放已经冻结的历史样本，不连接行情、券商或生产参数。评估按 A 股
T+1、停牌和一字跌停不可卖等约束执行；日线同时触及止损和止盈时固定采用更
保守的止损优先语义。候选策略只能依据滚动验证集排序，最终留出集在候选编号
确定后才会读取，登记结果也不会自动晋升为生产配置。
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from gribuki_trade.strategy_lab.exit_models import (
    ExitEvaluationBar,
    ExitPolicyBarrier,
    ExitPolicyCostModel,
    ExitPolicyDataset,
    ExitPolicyEpisode,
    ExitPolicyEvaluation,
    ExitPolicyEvaluatorProtocol,
    ExitPolicyFoldResult,
    ExitPolicyInvalidPlanError,
    ExitPolicyMetrics,
    ExitPolicyObjective,
    ExitPolicyOutcome,
    ExitPolicyOutcomeStatus,
    ExitPolicyTrial,
    ExitPolicyTrialRegistry,
    ExitPolicyTrialStatus,
)
from gribuki_trade.strategy_lab.exit_policies import (
    ExitPolicyParameters,
    ExitPolicySearchSpace,
    generate_exit_policy_candidates,
)
from gribuki_trade.strategy_lab.exit_serialization import (
    registry_to_json as _registry_to_json,
)
from gribuki_trade.strategy_lab.exit_serialization import (
    sha256_document as _sha256_document,
)
from gribuki_trade.strategy_lab.exit_walk_forward import (
    ExitPolicyWalkForwardFold,
    ExitPolicyWalkForwardPlan,
    build_exit_policy_walk_forward_plan,
)


class ExitPolicyEvaluator:
    """按冻结日线确定性重放每一个退出候选。"""

    def __init__(self, dataset: ExitPolicyDataset) -> None:
        self._dataset = dataset

    @property
    def dataset(self) -> ExitPolicyDataset:
        return self._dataset

    def evaluate(
        self,
        parameters: ExitPolicyParameters,
        observation_indices: tuple[int, ...],
        costs: ExitPolicyCostModel,
        *,
        label_horizon_sessions: int,
    ) -> ExitPolicyEvaluation:
        indices = _validated_indices(observation_indices, len(self._dataset.episodes))
        if (
            isinstance(label_horizon_sessions, bool)
            or not isinstance(label_horizon_sessions, int)
            or label_horizon_sessions < parameters.maximum_holding_sessions
        ):
            raise ValueError("label horizon must cover maximum_holding_sessions")
        if any(
            len(self._dataset.episodes[index].future_bars) < label_horizon_sessions
            for index in indices
        ):
            raise ValueError("episode future-bar coverage is shorter than label horizon")
        outcomes = tuple(
            _evaluate_episode(
                self._dataset.episodes[index],
                parameters,
                costs,
                label_horizon_sessions,
            )
            for index in indices
        )
        return ExitPolicyEvaluation(
            parameters=parameters,
            observation_indices=indices,
            metrics=_metrics(outcomes),
            outcomes=outcomes,
        )


_MARKET_TZ = ZoneInfo("Asia/Shanghai")
_BPS = Decimal("10000")
_ONE = Decimal("1")
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def run_exit_policy_walk_forward_experiment(
    *,
    evaluator: ExitPolicyEvaluatorProtocol,
    search_space: ExitPolicySearchSpace,
    baseline: ExitPolicyParameters,
    walk_forward: ExitPolicyWalkForwardPlan,
    costs: ExitPolicyCostModel,
    objective: ExitPolicyObjective,
    created_at: datetime,
    experiment_version: str = "exit-policy-walk-forward@1",
) -> ExitPolicyTrialRegistry:
    """登记全部候选，只按验证折选参，然后评估选中项和基线留出集。"""

    experiment_version = _identifier(experiment_version, "experiment_version")
    objective = ExitPolicyObjective(objective)
    created_at = _aware_utc(created_at, "created_at")
    if created_at < evaluator.dataset.frozen_at:
        raise ValueError("created_at must not precede dataset.frozen_at")
    if evaluator.dataset.content_sha256 != walk_forward.dataset_sha256:
        raise ValueError("walk-forward plan belongs to a different frozen dataset")
    if evaluator.dataset.entry_sessions != walk_forward.session_plan.timestamps:
        raise ValueError("walk-forward session lineage does not match the evaluator")
    expected_plan = build_exit_policy_walk_forward_plan(
        evaluator.dataset,
        walk_forward.session_plan.config,
        minimum_validation_episodes=walk_forward.minimum_validation_episodes,
        minimum_holdout_episodes=walk_forward.minimum_holdout_episodes,
    )
    if walk_forward != expected_plan:
        raise ValueError("walk-forward plan does not match its deterministic rebuild")
    candidates = generate_exit_policy_candidates(search_space)
    candidate_by_fingerprint = {candidate.fingerprint: candidate for candidate in candidates}
    if baseline.fingerprint not in candidate_by_fingerprint:
        raise ValueError("baseline must be a registered search-space candidate")
    if max(candidate.maximum_holding_sessions for candidate in candidates) > (
        walk_forward.session_plan.config.label_horizon_sessions
    ):
        raise ValueError("walk-forward label horizon must cover every candidate")

    trials: list[ExitPolicyTrial] = []
    for candidate in candidates:
        trial_id = f"exit-{candidate.fingerprint[:24]}"
        try:
            fold_results = tuple(
                ExitPolicyFoldResult(
                    fold_id=fold.fold_id,
                    evaluation=evaluator.evaluate(
                        candidate,
                        fold.validation_episode_indices,
                        costs,
                        label_horizon_sessions=(
                            walk_forward.session_plan.config.label_horizon_sessions
                        ),
                    ),
                )
                for fold in walk_forward.folds
            )
            validation = _combine_evaluations(candidate, fold_results)
            trials.append(
                ExitPolicyTrial(
                    trial_id=trial_id,
                    parameters=candidate,
                    status=ExitPolicyTrialStatus.EVALUATED,
                    fold_results=fold_results,
                    validation_metrics=validation.metrics,
                    validation_score=_objective_score(validation.metrics, objective),
                    rejection_code=None,
                )
            )
        except ExitPolicyInvalidPlanError:
            trials.append(
                ExitPolicyTrial(
                    trial_id=trial_id,
                    parameters=candidate,
                    status=ExitPolicyTrialStatus.REJECTED_INVALID_PLAN,
                    fold_results=(),
                    validation_metrics=None,
                    validation_score=None,
                    rejection_code="INVALID_NONPOSITIVE_STOP",
                )
            )

    evaluated = tuple(trial for trial in trials if trial.status is ExitPolicyTrialStatus.EVALUATED)
    if not evaluated:
        raise ValueError("no valid exit-policy candidate remains after evaluation")
    selected = min(
        evaluated,
        key=lambda trial: (-_required_score(trial), trial.trial_id),
    )
    baseline_trial = next(
        trial for trial in trials if trial.parameters.fingerprint == baseline.fingerprint
    )
    if baseline_trial.status is not ExitPolicyTrialStatus.EVALUATED:
        raise ValueError("baseline candidate is invalid for the frozen dataset")

    horizon = walk_forward.session_plan.config.label_horizon_sessions
    selected_holdout = evaluator.evaluate(
        selected.parameters,
        walk_forward.holdout_episode_indices,
        costs,
        label_horizon_sessions=horizon,
    )
    baseline_holdout = (
        selected_holdout
        if selected.trial_id == baseline_trial.trial_id
        else evaluator.evaluate(
            baseline_trial.parameters,
            walk_forward.holdout_episode_indices,
            costs,
            label_horizon_sessions=horizon,
        )
    )
    registry_seed = {
        "cost": costs.fingerprint,
        "dataset": evaluator.dataset.content_sha256,
        "experiment": experiment_version,
        "objective": objective.value,
        "plan": walk_forward.plan_sha256,
        "search": search_space.manifest_sha256,
    }
    return ExitPolicyTrialRegistry(
        registry_id=f"exit-registry-{_sha256_document(registry_seed)[:24]}",
        experiment_version=experiment_version,
        dataset_sha256=evaluator.dataset.content_sha256,
        search_space_sha256=search_space.manifest_sha256,
        walk_forward_sha256=walk_forward.plan_sha256,
        cost_model_sha256=costs.fingerprint,
        objective=objective,
        trials=tuple(trials),
        selected_trial_id=selected.trial_id,
        baseline_trial_id=baseline_trial.trial_id,
        selected_holdout=selected_holdout,
        baseline_holdout=baseline_holdout,
        created_at=created_at,
    )


def exit_policy_registry_to_json(registry: ExitPolicyTrialRegistry) -> str:
    """以稳定字段顺序输出完整试验登记，便于文件归档和摘要复核。"""

    return _registry_to_json(registry)


def _evaluate_episode(
    episode: ExitPolicyEpisode,
    parameters: ExitPolicyParameters,
    costs: ExitPolicyCostModel,
    label_horizon_sessions: int,
) -> ExitPolicyOutcome:
    """兼容历史私有 helper；实现位于纯模拟模块。"""

    from gribuki_trade.strategy_lab.exit_simulation import evaluate_episode

    return evaluate_episode(episode, parameters, costs, label_horizon_sessions)


def _filled_outcome(
    episode: ExitPolicyEpisode,
    parameters: ExitPolicyParameters,
    costs: ExitPolicyCostModel,
    *,
    initial_stop: Decimal,
    final_stop: Decimal,
    take_profit: Decimal,
    barrier: ExitPolicyBarrier,
    barrier_date: date,
    raw_price: Decimal,
    bar: ExitEvaluationBar,
    holding_sessions: int,
    both_touched: bool,
    blocked_sessions: int,
    warnings: Sequence[str],
) -> ExitPolicyOutcome:
    """兼容历史私有 helper；实现位于纯模拟模块。"""

    from gribuki_trade.strategy_lab.exit_simulation import filled_outcome

    return filled_outcome(
        episode,
        parameters,
        costs,
        initial_stop=initial_stop,
        final_stop=final_stop,
        take_profit=take_profit,
        barrier=barrier,
        barrier_date=barrier_date,
        raw_price=raw_price,
        bar=bar,
        holding_sessions=holding_sessions,
        both_touched=both_touched,
        blocked_sessions=blocked_sessions,
        warnings=warnings,
    )


def _net_sell_price(
    raw_price: Decimal,
    bar: ExitEvaluationBar,
    costs: ExitPolicyCostModel,
) -> Decimal:
    """兼容历史私有 helper；实现位于纯模拟模块。"""

    from gribuki_trade.strategy_lab.exit_simulation import net_sell_price

    return net_sell_price(raw_price, bar, costs)


def _net_trade_result(
    episode: ExitPolicyEpisode,
    net_sell_price: Decimal,
    costs: ExitPolicyCostModel,
) -> tuple[Decimal, Decimal]:
    """兼容历史私有 helper；实现位于纯模拟模块。"""

    from gribuki_trade.strategy_lab.exit_simulation import net_trade_result

    return net_trade_result(episode, net_sell_price, costs)


def _metrics(outcomes: tuple[ExitPolicyOutcome, ...]) -> ExitPolicyMetrics:
    """兼容历史私有 helper；实现位于纯模拟模块。"""

    from gribuki_trade.strategy_lab.exit_simulation import metrics

    return metrics(outcomes)


def _combine_evaluations(
    parameters: ExitPolicyParameters,
    fold_results: tuple[ExitPolicyFoldResult, ...],
) -> ExitPolicyEvaluation:
    """兼容历史私有 helper；实现位于纯模拟模块。"""

    from gribuki_trade.strategy_lab.exit_simulation import combine_evaluations

    return combine_evaluations(parameters, fold_results)


def _objective_score(
    metrics: ExitPolicyMetrics,
    objective: ExitPolicyObjective,
) -> Decimal:
    """兼容历史私有 helper；实现位于纯模拟模块。"""

    from gribuki_trade.strategy_lab.exit_simulation import objective_score

    return objective_score(metrics, objective)


def _required_score(trial: ExitPolicyTrial) -> Decimal:
    if trial.validation_score is None:
        raise AssertionError("evaluated trial is missing validation_score")
    return trial.validation_score


def _validated_indices(indices: tuple[int, ...], size: int) -> tuple[int, ...]:
    if not indices:
        raise ValueError("observation_indices must not be empty")
    if indices != tuple(sorted(indices)) or len(set(indices)) != len(indices):
        raise ValueError("observation_indices must be strictly increasing and unique")
    if indices[0] < 0 or indices[-1] >= size:
        raise IndexError("observation index is outside the frozen dataset")
    return indices


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if _SAFE_IDENTIFIER.fullmatch(normalized) is None:
        raise ValueError(f"{name} must be a safe non-empty identifier")
    return normalized


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _finite_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _positive_decimal(value: object, name: str) -> Decimal:
    value = _finite_decimal(value, name)
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _nonnegative_decimal(value: object, name: str) -> Decimal:
    value = _finite_decimal(value, name)
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _aware_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "ExitEvaluationBar",
    "ExitPolicyBarrier",
    "ExitPolicyCostModel",
    "ExitPolicyDataset",
    "ExitPolicyEpisode",
    "ExitPolicyEvaluation",
    "ExitPolicyEvaluator",
    "ExitPolicyEvaluatorProtocol",
    "ExitPolicyFoldResult",
    "ExitPolicyInvalidPlanError",
    "ExitPolicyMetrics",
    "ExitPolicyObjective",
    "ExitPolicyOutcome",
    "ExitPolicyOutcomeStatus",
    "ExitPolicyTrial",
    "ExitPolicyTrialRegistry",
    "ExitPolicyTrialStatus",
    "ExitPolicyWalkForwardFold",
    "ExitPolicyWalkForwardPlan",
    "build_exit_policy_walk_forward_plan",
    "exit_policy_registry_to_json",
    "run_exit_policy_walk_forward_experiment",
]
