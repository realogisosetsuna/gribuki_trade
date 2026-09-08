"""退出策略研究产物的稳定文档编解码器。

评估器负责校验和历史重放，本模块只负责把评估值确定性地转换成 JSON 文档并
计算内容摘要。输入采用结构化属性访问，避免反向依赖 ``exit_evaluator``；历史
门面会以原有私有名称导入这些函数，从而保持旧调用路径兼容。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def dataset_document(dataset: Any) -> dict[str, object]:
    return {
        "dataset_id": dataset.dataset_id,
        "episodes": [episode_document(item) for item in dataset.episodes],
        "frozen_at": dataset.frozen_at.isoformat(),
        "schema_version": dataset.schema_version,
    }


def episode_document(episode: Any) -> dict[str, object]:
    return {
        "atr_at_entry": str(episode.atr_at_entry),
        "entry_at": episode.entry_at.isoformat(),
        "entry_price": str(episode.entry_price),
        "entry_session_date": episode.entry_session_date.isoformat(),
        "episode_id": episode.episode_id,
        "features_known_at": episode.features_known_at.isoformat(),
        "future_bars": [bar_document(bar) for bar in episode.future_bars],
        "quantity": episode.quantity,
        "source_revisions": [list(item) for item in episode.source_revisions],
        "structure_low_at_entry": str(episode.structure_low_at_entry),
        "symbol": episode.symbol,
    }


def bar_document(bar: Any) -> dict[str, object]:
    return {
        "close": str(bar.close),
        "completed_at": bar.completed_at.isoformat(),
        "high": str(bar.high),
        "low": str(bar.low),
        "lower_price_limit": str(bar.lower_price_limit),
        "open": str(bar.open),
        "session_date": bar.session_date.isoformat(),
        "source_id": bar.source_id,
        "source_revision": bar.source_revision,
        "suspended": bar.suspended,
        "upper_price_limit": str(bar.upper_price_limit),
        "volume_shares": bar.volume_shares,
    }


def cost_document(costs: Any) -> dict[str, object]:
    return {
        "commission_bps": str(costs.commission_bps),
        "minimum_commission_cny": str(costs.minimum_commission_cny),
        "sell_slippage_bps": str(costs.sell_slippage_bps),
        "tax_bps": str(costs.tax_bps),
        "transfer_fee_bps": str(costs.transfer_fee_bps),
    }


def parameters_document(parameters: Any) -> dict[str, object]:
    return {
        "atr_stop_multiple": str(parameters.atr_stop_multiple),
        "maximum_holding_sessions": parameters.maximum_holding_sessions,
        "policy_version": parameters.policy_version,
        "reward_to_risk": str(parameters.reward_to_risk),
        "structure_buffer_atr": str(parameters.structure_buffer_atr),
        "trailing_atr_multiple": (
            None
            if parameters.trailing_atr_multiple is None
            else str(parameters.trailing_atr_multiple)
        ),
    }


def walk_forward_document(plan: Any) -> dict[str, object]:
    config = plan.session_plan.config
    return {
        "dataset_sha256": plan.dataset_sha256,
        "config": {
            "embargo_size": config.embargo_size,
            "initial_train_size": config.initial_train_size,
            "label_horizon_sessions": config.label_horizon_sessions,
            "minimum_folds": config.minimum_folds,
            "purge_size": config.purge_size,
            "step_size": config.step_size,
            "test_size": config.test_size,
            "validation_size": config.validation_size,
        },
        "folds": [
            {
                "embargo_sessions": [value.isoformat() for value in fold.embargo_sessions],
                "fold_id": fold.fold_id,
                "purge_sessions": [value.isoformat() for value in fold.purge_sessions],
                "train_sessions": [value.isoformat() for value in fold.train_sessions],
                "validation_episode_indices": list(fold.validation_episode_indices),
                "validation_sessions": [
                    value.isoformat() for value in fold.validation_sessions
                ],
            }
            for fold in plan.folds
        ],
        "holdout_episode_indices": list(plan.holdout_episode_indices),
        "holdout_sessions": [value.isoformat() for value in plan.holdout_sessions],
        "minimum_holdout_episodes": plan.minimum_holdout_episodes,
        "minimum_validation_episodes": plan.minimum_validation_episodes,
    }


def metrics_document(metrics: Any) -> dict[str, object]:
    return {
        "average_return": str(metrics.average_return),
        "blocked_session_count": metrics.blocked_session_count,
        "completed_count": metrics.completed_count,
        "episode_count": metrics.episode_count,
        "gross_loss_cny": str(metrics.gross_loss_cny),
        "gross_profit_cny": str(metrics.gross_profit_cny),
        "hit_rate": str(metrics.hit_rate),
        "losing_count": metrics.losing_count,
        "marked_open_count": metrics.marked_open_count,
        "maximum_drawdown": str(metrics.maximum_drawdown),
        "net_return": str(metrics.net_return),
        "profit_factor": (
            None if metrics.profit_factor is None else str(metrics.profit_factor)
        ),
        "winning_count": metrics.winning_count,
    }


def outcome_document(outcome: Any) -> dict[str, object]:
    return {
        "barrier": outcome.barrier.value,
        "barrier_observed_on": outcome.barrier_observed_on.isoformat(),
        "blocked_sessions": outcome.blocked_sessions,
        "both_price_barriers_touched": outcome.both_price_barriers_touched,
        "episode_id": outcome.episode_id,
        "entry_session_date": outcome.entry_session_date.isoformat(),
        "execution_on": (
            None if outcome.execution_on is None else outcome.execution_on.isoformat()
        ),
        "final_stop_price": str(outcome.final_stop_price),
        "holding_sessions": outcome.holding_sessions,
        "initial_stop_price": str(outcome.initial_stop_price),
        "net_execution_price": str(outcome.net_execution_price),
        "net_pnl_cny": str(outcome.net_pnl_cny),
        "net_return": str(outcome.net_return),
        "parameter_fingerprint": outcome.parameter_fingerprint,
        "raw_execution_price": (
            None
            if outcome.raw_execution_price is None
            else str(outcome.raw_execution_price)
        ),
        "status": outcome.status.value,
        "take_profit_price": str(outcome.take_profit_price),
        "warning_codes": list(outcome.warning_codes),
    }


def evaluation_document(evaluation: Any) -> dict[str, object]:
    return {
        "metrics": metrics_document(evaluation.metrics),
        "observation_indices": list(evaluation.observation_indices),
        "outcomes": [outcome_document(item) for item in evaluation.outcomes],
        "parameter_fingerprint": evaluation.parameters.fingerprint,
    }


def registry_document(registry: Any) -> dict[str, object]:
    return {
        "baseline_holdout": evaluation_document(registry.baseline_holdout),
        "baseline_trial_id": registry.baseline_trial_id,
        "cost_model_sha256": registry.cost_model_sha256,
        "created_at": registry.created_at.isoformat(),
        "dataset_sha256": registry.dataset_sha256,
        "experiment_version": registry.experiment_version,
        "objective": registry.objective.value,
        "promotion_authorized": registry.promotion_authorized,
        "registry_id": registry.registry_id,
        "research_only": registry.research_only,
        "search_space_sha256": registry.search_space_sha256,
        "selected_holdout": evaluation_document(registry.selected_holdout),
        "selected_trial_id": registry.selected_trial_id,
        "trials": [
            {
                "folds": [
                    {
                        "evaluation": evaluation_document(fold.evaluation),
                        "fold_id": fold.fold_id,
                    }
                    for fold in trial.fold_results
                ],
                "parameter_fingerprint": trial.parameters.fingerprint,
                "parameters": parameters_document(trial.parameters),
                "rejection_code": trial.rejection_code,
                "status": trial.status.value,
                "trial_id": trial.trial_id,
                "validation_metrics": (
                    None
                    if trial.validation_metrics is None
                    else metrics_document(trial.validation_metrics)
                ),
                "validation_score": (
                    None if trial.validation_score is None else str(trial.validation_score)
                ),
            }
            for trial in registry.trials
        ],
        "walk_forward_sha256": registry.walk_forward_sha256,
    }


def sha256_document(document: object) -> str:
    encoded = json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def registry_to_json(registry: Any) -> str:
    return json.dumps(
        registry_document(registry),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


__all__ = [
    "bar_document",
    "cost_document",
    "dataset_document",
    "episode_document",
    "evaluation_document",
    "metrics_document",
    "outcome_document",
    "parameters_document",
    "registry_document",
    "registry_to_json",
    "sha256_document",
    "walk_forward_document",
]
