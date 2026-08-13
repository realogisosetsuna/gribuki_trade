"""Leakage-resistant, explainable strategy experiment primitives.

This module is deliberately broker independent and research only.  It freezes the
data and strategy identities used by an experiment, builds chronological
walk-forward development folds, and keeps the final holdout inaccessible until a
candidate has been selected using validation metrics alone.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol


class ExperimentObjective(StrEnum):
    NET_RETURN = "NET_RETURN"
    SHARPE = "SHARPE"
    CALMAR = "CALMAR"


@dataclass(frozen=True, slots=True)
class DataManifest:
    """Immutable identity for the exact point-in-time dataset under study."""

    dataset_id: str
    content_sha256: str
    schema_version: str
    observation_count: int
    observed_start: date
    observed_end: date
    frozen_at: datetime
    feature_ids: tuple[str, ...]
    label_id: str
    source_revisions: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.dataset_id, "dataset_id")
        _require_sha256(self.content_sha256, "content_sha256")
        _require_identifier(self.schema_version, "schema_version")
        _require_identifier(self.label_id, "label_id")
        if self.observation_count < 1:
            raise ValueError("observation_count must be positive")
        if self.observed_end < self.observed_start:
            raise ValueError("observed_end must not precede observed_start")
        frozen_at = _utc_time(self.frozen_at, "frozen_at")
        if frozen_at.date() < self.observed_end:
            raise ValueError("frozen_at must not precede observed_end")
        feature_ids = _normalized_unique_pairs_or_ids(self.feature_ids, "feature_ids")
        if not feature_ids:
            raise ValueError("feature_ids must not be empty")
        revisions = _normalized_pairs(self.source_revisions, "source_revisions")
        object.__setattr__(self, "frozen_at", frozen_at)
        object.__setattr__(self, "feature_ids", feature_ids)
        object.__setattr__(self, "source_revisions", revisions)

    @property
    def manifest_sha256(self) -> str:
        return _sha256_document(_data_manifest_document(self))

    @classmethod
    def freeze(
        cls,
        *,
        dataset_id: str,
        schema_version: str,
        observation_count: int,
        observed_start: date,
        observed_end: date,
        frozen_at: datetime,
        feature_ids: Sequence[str],
        label_id: str,
        canonical_content: bytes,
        source_revisions: Sequence[tuple[str, str]] = (),
    ) -> DataManifest:
        """Create a manifest whose dataset digest is computed, not hand-entered."""

        return cls(
            dataset_id=dataset_id,
            content_sha256=hashlib.sha256(canonical_content).hexdigest(),
            schema_version=schema_version,
            observation_count=observation_count,
            observed_start=observed_start,
            observed_end=observed_end,
            frozen_at=frozen_at,
            feature_ids=tuple(feature_ids),
            label_id=label_id,
            source_revisions=tuple(source_revisions),
        )


@dataclass(frozen=True, slots=True)
class StrategyManifest:
    """Frozen code/config identity, excluding weights varied by the experiment."""

    strategy_version: str
    code_revision: str
    parameters: tuple[tuple[str, str], ...]
    factor_expressions: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.strategy_version, "strategy_version")
        _require_identifier(self.code_revision, "code_revision")
        object.__setattr__(
            self,
            "parameters",
            _normalized_pairs(self.parameters, "parameters"),
        )
        object.__setattr__(
            self,
            "factor_expressions",
            _normalized_pairs(self.factor_expressions, "factor_expressions"),
        )

    @property
    def manifest_sha256(self) -> str:
        return _sha256_document(_strategy_manifest_document(self))


@dataclass(frozen=True, slots=True)
class WalkForwardConfig:
    initial_train_size: int
    validation_size: int
    test_size: int
    step_size: int
    purge_size: int
    embargo_size: int
    label_horizon_sessions: int
    minimum_folds: int = 2

    def __post_init__(self) -> None:
        for name in (
            "initial_train_size",
            "validation_size",
            "test_size",
            "step_size",
            "purge_size",
            "embargo_size",
            "label_horizon_sessions",
            "minimum_folds",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.step_size < self.validation_size:
            raise ValueError("step_size must be at least validation_size")
        if self.purge_size < self.label_horizon_sessions:
            raise ValueError("purge_size must cover the label horizon")
        if self.embargo_size < self.label_horizon_sessions:
            raise ValueError("embargo_size must cover the label horizon")


@dataclass(frozen=True, slots=True)
class WalkForwardFold:
    fold_id: str
    train_indices: tuple[int, ...]
    purge_indices: tuple[int, ...]
    validation_indices: tuple[int, ...]
    embargo_indices: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class WalkForwardPlan:
    timestamps: tuple[date, ...]
    config: WalkForwardConfig
    folds: tuple[WalkForwardFold, ...]
    final_embargo_indices: tuple[int, ...]
    test_indices: tuple[int, ...]


def build_walk_forward_plan(
    timestamps: Sequence[date],
    config: WalkForwardConfig,
) -> WalkForwardPlan:
    """Build expanding chronological folds and one untouched final holdout.

    Purge separates every training window from its validation window.  Embargo
    marks the observations immediately after each validation window and reserves
    another complete gap before the final test.  The test indices never appear in
    a development fold.
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


@dataclass(frozen=True, slots=True)
class WeightConstraints:
    technical_family_ids: tuple[str, ...]
    max_technical_family_weight: Decimal = Decimal("0.45")
    min_macro_weight: Decimal = Decimal("0")
    max_macro_weight: Decimal = Decimal("0.40")

    def __post_init__(self) -> None:
        families = _normalized_unique_pairs_or_ids(
            self.technical_family_ids,
            "technical_family_ids",
        )
        if not families:
            raise ValueError("technical_family_ids must not be empty")
        for name in (
            "max_technical_family_weight",
            "min_macro_weight",
            "max_macro_weight",
        ):
            _require_finite(getattr(self, name), name)
        if not Decimal("0") < self.max_technical_family_weight <= Decimal("1"):
            raise ValueError("max_technical_family_weight must be in (0, 1]")
        if not Decimal("0") <= self.min_macro_weight <= self.max_macro_weight:
            raise ValueError("macro weight bounds are invalid")
        if self.max_macro_weight > Decimal("0.40"):
            raise ValueError("max_macro_weight must not exceed 0.40")
        if (
            Decimal(len(families)) * self.max_technical_family_weight
            + self.max_macro_weight
            < Decimal("1")
        ):
            raise ValueError("weight caps make the simplex infeasible")
        object.__setattr__(self, "technical_family_ids", families)


@dataclass(frozen=True, slots=True)
class StrategyWeights:
    technical: tuple[tuple[str, Decimal], ...]
    macro: Decimal

    def __post_init__(self) -> None:
        technical = tuple(sorted(self.technical))
        if not technical or len({name for name, _ in technical}) != len(technical):
            raise ValueError("technical weights must be non-empty and unique")
        for name, weight in technical:
            _require_identifier(name, "technical family ID")
            _require_finite(weight, f"weight for {name}")
            if weight < 0:
                raise ValueError("technical weights must be non-negative")
        _require_finite(self.macro, "macro weight")
        if self.macro < 0:
            raise ValueError("macro weight must be non-negative")
        total = sum((weight for _, weight in technical), self.macro)
        if total != Decimal("1"):
            raise ValueError("technical and macro weights must sum exactly to one")
        object.__setattr__(self, "technical", technical)

    @property
    def fingerprint(self) -> str:
        return _sha256_document(_weights_document(self))


@dataclass(frozen=True, slots=True)
class WeightGridConfig:
    step: Decimal = Decimal("0.10")
    max_candidates: int = 10_000

    def __post_init__(self) -> None:
        _require_finite(self.step, "step")
        if not Decimal("0") < self.step <= Decimal("1"):
            raise ValueError("step must be in (0, 1]")
        reciprocal = Decimal("1") / self.step
        if reciprocal != reciprocal.to_integral_value():
            raise ValueError("step must divide one exactly")
        if self.max_candidates < 1:
            raise ValueError("max_candidates must be positive")


def generate_simplex_weight_grid(
    constraints: WeightConstraints,
    config: WeightGridConfig | None = None,
) -> tuple[StrategyWeights, ...]:
    """Enumerate a deterministic, bounded simplex grid.

    Enumeration never truncates silently: a space exceeding ``max_candidates``
    is rejected so the recorded trial count cannot differ from the tested count.
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


@dataclass(frozen=True, slots=True)
class CostScenario:
    scenario_id: str
    commission_bps: Decimal
    slippage_bps: Decimal
    tax_bps: Decimal
    minimum_commission_cny: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        _require_identifier(self.scenario_id, "scenario_id")
        for name in (
            "commission_bps",
            "slippage_bps",
            "tax_bps",
            "minimum_commission_cny",
        ):
            value = getattr(self, name)
            _require_finite(value, name)
            if value < 0:
                raise ValueError(f"{name} must be non-negative")


@dataclass(frozen=True, slots=True)
class PerformanceMetrics:
    net_return: Decimal
    annualized_return: Decimal | None
    annualized_volatility: Decimal | None
    sharpe: Decimal | None
    max_drawdown: Decimal
    turnover: Decimal
    trade_count: int
    hit_rate: Decimal | None
    family_contributions: tuple[tuple[str, Decimal], ...] = ()

    def __post_init__(self) -> None:
        for name in ("net_return", "max_drawdown", "turnover"):
            _require_finite(getattr(self, name), name)
        for name in ("annualized_return", "annualized_volatility", "sharpe", "hit_rate"):
            value = getattr(self, name)
            if value is not None:
                _require_finite(value, name)
        if self.max_drawdown < 0:
            raise ValueError("max_drawdown must be a non-negative magnitude")
        if self.turnover < 0:
            raise ValueError("turnover must be non-negative")
        if self.trade_count < 0:
            raise ValueError("trade_count must be non-negative")
        if self.hit_rate is not None and not Decimal("0") <= self.hit_rate <= Decimal("1"):
            raise ValueError("hit_rate must be in [0, 1]")
        contributions = tuple(sorted(self.family_contributions))
        if len({name for name, _ in contributions}) != len(contributions):
            raise ValueError("family contributions must have unique IDs")
        for name, value in contributions:
            _require_identifier(name, "family contribution ID")
            _require_finite(value, f"contribution for {name}")
        object.__setattr__(self, "family_contributions", contributions)

    def objective_value(self, objective: ExperimentObjective) -> Decimal:
        if objective is ExperimentObjective.NET_RETURN:
            return self.net_return
        if objective is ExperimentObjective.SHARPE:
            if self.sharpe is None:
                raise ValueError("SHARPE objective requires a sharpe metric")
            return self.sharpe
        if self.annualized_return is None:
            raise ValueError("CALMAR objective requires annualized_return")
        if self.max_drawdown == 0:
            raise ValueError("CALMAR objective requires non-zero max_drawdown")
        return self.annualized_return / self.max_drawdown


class StrategyEvaluator(Protocol):
    """Pure evaluator supplied by a backtest engine; no broker calls are allowed."""

    def evaluate(
        self,
        weights: StrategyWeights,
        observation_indices: tuple[int, ...],
        cost_scenario: CostScenario,
    ) -> PerformanceMetrics: ...


@dataclass(frozen=True, slots=True)
class ScenarioEvaluation:
    scenario_id: str
    metrics: PerformanceMetrics


@dataclass(frozen=True, slots=True)
class FoldEvaluation:
    fold_id: str
    train: tuple[ScenarioEvaluation, ...]
    validation: tuple[ScenarioEvaluation, ...]


@dataclass(frozen=True, slots=True)
class TrialResult:
    trial_id: str
    weights: StrategyWeights
    folds: tuple[FoldEvaluation, ...]
    validation_objective: Decimal
    validation_family_contributions: tuple[tuple[str, Decimal], ...]


@dataclass(frozen=True, slots=True)
class HoldoutEvaluation:
    selected: tuple[ScenarioEvaluation, ...]
    selected_objective: Decimal
    baseline: tuple[ScenarioEvaluation, ...]
    baseline_objective: Decimal


@dataclass(frozen=True, slots=True)
class StrategyExperiment:
    experiment_id: str
    created_at: datetime
    experiment_version: str
    data_manifest: DataManifest
    strategy_manifest: StrategyManifest
    walk_forward_plan: WalkForwardPlan
    constraints: WeightConstraints
    objective: ExperimentObjective
    cost_scenarios: tuple[CostScenario, ...]
    trials: tuple[TrialResult, ...]
    selected_trial_id: str
    baseline_trial_id: str
    holdout: HoldoutEvaluation
    warnings: tuple[str, ...]
    research_only: bool = True

    @property
    def trial_count(self) -> int:
        return len(self.trials)


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
    """Select on validation only, then evaluate the locked choice once on test.

    Training metrics are retained for diagnostics but never participate in the
    selection key.  Validation scores are averaged across folds per cost scenario;
    the worst cost scenario is the objective used for selection.  Holdout access
    occurs only after the selected trial ID is fixed.
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

    # This is the only point at which the holdout indices are passed to the
    # evaluator.  Selection has already been irrevocably reduced to a trial ID.
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


def experiment_to_json(experiment: StrategyExperiment) -> str:
    """Return a canonical, lossless-enough audit document for append-only storage."""

    return json.dumps(
        _experiment_document(experiment),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
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


def _data_manifest_document(manifest: DataManifest) -> dict[str, object]:
    return {
        "dataset_id": manifest.dataset_id,
        "content_sha256": manifest.content_sha256,
        "schema_version": manifest.schema_version,
        "observation_count": manifest.observation_count,
        "observed_start": manifest.observed_start.isoformat(),
        "observed_end": manifest.observed_end.isoformat(),
        "frozen_at": manifest.frozen_at.isoformat(),
        "feature_ids": list(manifest.feature_ids),
        "label_id": manifest.label_id,
        "source_revisions": [list(item) for item in manifest.source_revisions],
    }


def _strategy_manifest_document(manifest: StrategyManifest) -> dict[str, object]:
    return {
        "strategy_version": manifest.strategy_version,
        "code_revision": manifest.code_revision,
        "parameters": [list(item) for item in manifest.parameters],
        "factor_expressions": [list(item) for item in manifest.factor_expressions],
    }


def _weights_document(weights: StrategyWeights) -> dict[str, object]:
    return {
        "technical": [[name, str(value)] for name, value in weights.technical],
        "macro": str(weights.macro),
    }


def _metrics_document(metrics: PerformanceMetrics) -> dict[str, object]:
    return {
        "net_return": str(metrics.net_return),
        "annualized_return": _optional_decimal(metrics.annualized_return),
        "annualized_volatility": _optional_decimal(metrics.annualized_volatility),
        "sharpe": _optional_decimal(metrics.sharpe),
        "max_drawdown": str(metrics.max_drawdown),
        "turnover": str(metrics.turnover),
        "trade_count": metrics.trade_count,
        "hit_rate": _optional_decimal(metrics.hit_rate),
        "family_contributions": [
            [name, str(value)] for name, value in metrics.family_contributions
        ],
    }


def _scenario_evaluations_document(
    evaluations: tuple[ScenarioEvaluation, ...],
) -> list[dict[str, object]]:
    return [
        {"scenario_id": item.scenario_id, "metrics": _metrics_document(item.metrics)}
        for item in evaluations
    ]


def _experiment_document(experiment: StrategyExperiment) -> dict[str, object]:
    plan = experiment.walk_forward_plan
    return {
        "experiment_id": experiment.experiment_id,
        "created_at": experiment.created_at.isoformat(),
        "experiment_version": experiment.experiment_version,
        "research_only": experiment.research_only,
        "data_manifest": _data_manifest_document(experiment.data_manifest),
        "data_manifest_sha256": experiment.data_manifest.manifest_sha256,
        "strategy_manifest": _strategy_manifest_document(experiment.strategy_manifest),
        "strategy_manifest_sha256": experiment.strategy_manifest.manifest_sha256,
        "walk_forward": {
            "config": {
                name: getattr(plan.config, name)
                for name in (
                    "initial_train_size",
                    "validation_size",
                    "test_size",
                    "step_size",
                    "purge_size",
                    "embargo_size",
                    "label_horizon_sessions",
                    "minimum_folds",
                )
            },
            "folds": [
                {
                    "fold_id": fold.fold_id,
                    "train_indices": list(fold.train_indices),
                    "purge_indices": list(fold.purge_indices),
                    "validation_indices": list(fold.validation_indices),
                    "embargo_indices": list(fold.embargo_indices),
                }
                for fold in plan.folds
            ],
            "final_embargo_indices": list(plan.final_embargo_indices),
            "test_indices": list(plan.test_indices),
        },
        "constraints": {
            "technical_family_ids": list(experiment.constraints.technical_family_ids),
            "max_technical_family_weight": str(
                experiment.constraints.max_technical_family_weight
            ),
            "min_macro_weight": str(experiment.constraints.min_macro_weight),
            "max_macro_weight": str(experiment.constraints.max_macro_weight),
        },
        "objective": experiment.objective.value,
        "cost_scenarios": [
            {
                "scenario_id": item.scenario_id,
                "commission_bps": str(item.commission_bps),
                "slippage_bps": str(item.slippage_bps),
                "tax_bps": str(item.tax_bps),
                "minimum_commission_cny": str(item.minimum_commission_cny),
            }
            for item in experiment.cost_scenarios
        ],
        "trial_count": experiment.trial_count,
        "trials": [
            {
                "trial_id": trial.trial_id,
                "weights": _weights_document(trial.weights),
                "validation_objective": str(trial.validation_objective),
                "validation_family_contributions": [
                    [name, str(value)]
                    for name, value in trial.validation_family_contributions
                ],
                "folds": [
                    {
                        "fold_id": fold.fold_id,
                        "train": _scenario_evaluations_document(fold.train),
                        "validation": _scenario_evaluations_document(fold.validation),
                    }
                    for fold in trial.folds
                ],
            }
            for trial in experiment.trials
        ],
        "selected_trial_id": experiment.selected_trial_id,
        "baseline_trial_id": experiment.baseline_trial_id,
        "holdout": {
            "selected": _scenario_evaluations_document(experiment.holdout.selected),
            "selected_objective": str(experiment.holdout.selected_objective),
            "baseline": _scenario_evaluations_document(experiment.holdout.baseline),
            "baseline_objective": str(experiment.holdout.baseline_objective),
        },
        "warnings": list(experiment.warnings),
    }


def _optional_decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _sha256_document(document: object) -> str:
    encoded = json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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
