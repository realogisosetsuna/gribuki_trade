"""策略实验的不可变清单、配置、指标和结果模型。"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from gribuki_trade.strategy_lab import experiment_serialization as _serialization

_data_manifest_document = _serialization.data_manifest_document
_metrics_document = _serialization.metrics_document
_sha256_document = _serialization.sha256_document
_strategy_manifest_document = _serialization.strategy_manifest_document
_weights_document = _serialization.weights_document


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


class ExperimentObjective(StrEnum):
    NET_RETURN = "NET_RETURN"
    SHARPE = "SHARPE"
    CALMAR = "CALMAR"


@dataclass(frozen=True, slots=True)
class DataManifest:
    """被研究的精确时点数据集之不可变标识。"""

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
        """创建数据集摘要由程序计算而非手工填写的清单。"""

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
    """冻结的代码与配置标识，不含实验中变化的权重。"""

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
    """由回测引擎提供的纯评估器；禁止调用券商接口。"""

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


