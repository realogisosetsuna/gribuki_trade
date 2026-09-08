"""策略实验产物的确定性 JSON 编码与摘要。

实验执行器只负责构造候选、折叠和留出集结果；本模块负责把这些不可变值
转换为审计文档。模型类型仅在类型检查时导入，不在运行时反向依赖执行器；
本模块不引入文件、数据库、网络或任何晋升权限，并保留原有归档格式。
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gribuki_trade.strategy_lab.experiments import (
        DataManifest,
        PerformanceMetrics,
        ScenarioEvaluation,
        StrategyExperiment,
        StrategyManifest,
        StrategyWeights,
    )


def data_manifest_document(manifest: DataManifest) -> dict[str, object]:
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


def strategy_manifest_document(manifest: StrategyManifest) -> dict[str, object]:
    return {
        "strategy_version": manifest.strategy_version,
        "code_revision": manifest.code_revision,
        "parameters": [list(item) for item in manifest.parameters],
        "factor_expressions": [list(item) for item in manifest.factor_expressions],
    }


def weights_document(weights: StrategyWeights) -> dict[str, object]:
    return {
        "technical": [[name, str(value)] for name, value in weights.technical],
        "macro": str(weights.macro),
    }


def metrics_document(metrics: PerformanceMetrics) -> dict[str, object]:
    return {
        "net_return": str(metrics.net_return),
        "annualized_return": optional_decimal(metrics.annualized_return),
        "annualized_volatility": optional_decimal(metrics.annualized_volatility),
        "sharpe": optional_decimal(metrics.sharpe),
        "max_drawdown": str(metrics.max_drawdown),
        "turnover": str(metrics.turnover),
        "trade_count": metrics.trade_count,
        "hit_rate": optional_decimal(metrics.hit_rate),
        "family_contributions": [
            [name, str(value)] for name, value in metrics.family_contributions
        ],
    }


def scenario_evaluations_document(
    evaluations: tuple[ScenarioEvaluation, ...],
) -> list[dict[str, object]]:
    return [
        {"scenario_id": item.scenario_id, "metrics": metrics_document(item.metrics)}
        for item in evaluations
    ]


def experiment_document(experiment: StrategyExperiment) -> dict[str, object]:
    """编码完整实验，显式保留 research-only 审计标记。"""

    plan = experiment.walk_forward_plan
    return {
        "experiment_id": experiment.experiment_id,
        "created_at": experiment.created_at.isoformat(),
        "experiment_version": experiment.experiment_version,
        "research_only": experiment.research_only,
        "data_manifest": data_manifest_document(experiment.data_manifest),
        "data_manifest_sha256": experiment.data_manifest.manifest_sha256,
        "strategy_manifest": strategy_manifest_document(experiment.strategy_manifest),
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
                "weights": weights_document(trial.weights),
                "validation_objective": str(trial.validation_objective),
                "validation_family_contributions": [
                    [name, str(value)]
                    for name, value in trial.validation_family_contributions
                ],
                "folds": [
                    {
                        "fold_id": fold.fold_id,
                        "train": scenario_evaluations_document(fold.train),
                        "validation": scenario_evaluations_document(fold.validation),
                    }
                    for fold in trial.folds
                ],
            }
            for trial in experiment.trials
        ],
        "selected_trial_id": experiment.selected_trial_id,
        "baseline_trial_id": experiment.baseline_trial_id,
        "holdout": {
            "selected": scenario_evaluations_document(experiment.holdout.selected),
            "selected_objective": str(experiment.holdout.selected_objective),
            "baseline": scenario_evaluations_document(experiment.holdout.baseline),
            "baseline_objective": str(experiment.holdout.baseline_objective),
        },
        "warnings": list(experiment.warnings),
    }


def experiment_to_json(experiment: StrategyExperiment) -> str:
    """返回适合仅追加存储且信息足够无损的规范审计文档。"""

    return json.dumps(
        experiment_document(experiment),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_document(document: object) -> str:
    encoded = json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def optional_decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
