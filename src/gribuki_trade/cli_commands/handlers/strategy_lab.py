"""策略实验与因子发现 CLI 处理器。

这里的命令只产生 research_only 结果，不拥有交易执行权限；依赖通过 CLI facade
延迟解析，保持历史调用和测试替换点。
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from gribuki_trade.strategy_lab.discovery import FactorCandidateInventory


class _LazyCliFacade:
    """延迟解析 CLI facade，避免独立导入时的循环依赖。"""

    def __getattr__(self, name: str) -> Any:
        from gribuki_trade import cli

        return getattr(cli, name)


_cli: Any = _LazyCliFacade()

__all__ = [
    "_factor_candidate_inventory_json",
    "_strategy_exit_evaluate",
    "_strategy_factor_discover",
]


def _strategy_factor_discover(max_trials: int) -> dict[str, object]:
    """在不写入数据或配置的情况下扩展受控因子语法。"""

    from gribuki_trade.strategy_lab import (
        FactorSearchBudget,
        FactorSearchBudgetExceeded,
        default_factor_template_grammar,
        generate_factor_candidates,
    )

    grammar = default_factor_template_grammar()
    try:
        inventory = generate_factor_candidates(
            grammar,
            FactorSearchBudget(max_trials=max_trials),
        )
    except FactorSearchBudgetExceeded as exc:
        return {
            "ok": False,
            "error_code": "FACTOR_SEARCH_BUDGET_EXCEEDED",
            "grammar_version": grammar.grammar_version,
            "grammar_sha256": grammar.grammar_sha256,
            "max_trials": exc.max_trials,
            "required_trials": exc.required_trials,
            "research_only": True,
            "market_data_accessed": False,
            "llm_accessed": False,
            "holdout_accessed": False,
            "online_configuration_changed": False,
        }
    return _factor_candidate_inventory_json(inventory)


def _strategy_exit_evaluate(
    *,
    dataset: str,
    specification: str,
    output: str,
    created_at: datetime | None,
    overwrite: bool,
    confirmation: str | None,
) -> dict[str, object]:
    """运行冻结退出策略实验并返回不含逐笔大对象的可读摘要。"""

    if confirmation != "RESEARCH_ONLY":
        return {
            "error_code": "EXIT_POLICY_RESEARCH_CONFIRMATION_REQUIRED",
            "execution_authority": False,
            "ok": False,
            "promotion_authorized": False,
            "research_only": True,
        }
    from gribuki_trade.strategy_lab import run_frozen_exit_policy_experiment

    try:
        artifact = run_frozen_exit_policy_experiment(
            _cli.Path(dataset),
            _cli.Path(specification),
            _cli.Path(output),
            created_at=created_at,
            overwrite=overwrite,
        )
    except FileExistsError:
        return {
            "error_code": "EXIT_POLICY_ARTIFACT_ALREADY_EXISTS",
            "execution_authority": False,
            "ok": False,
            "promotion_authorized": False,
            "research_only": True,
        }
    except (OSError, TypeError, ValueError, ArithmeticError):
        return {
            "error_code": "EXIT_POLICY_EXPERIMENT_INPUT_INVALID",
            "execution_authority": False,
            "ok": False,
            "promotion_authorized": False,
            "research_only": True,
        }
    registry = artifact.registry
    selected = registry.selected_holdout.metrics
    baseline = registry.baseline_holdout.metrics
    return {
        "artifact_sha256": artifact.artifact_sha256,
        "baseline_holdout": {
            "maximum_drawdown": str(baseline.maximum_drawdown),
            "net_return": str(baseline.net_return),
        },
        "dataset_file_sha256": artifact.dataset_file_sha256,
        "execution_authority": False,
        "ok": True,
        "output": str(artifact.destination),
        "promotion_authorized": False,
        "registry_sha256": registry.registry_sha256,
        "research_only": True,
        "selected_holdout": {
            "maximum_drawdown": str(selected.maximum_drawdown),
            "net_return": str(selected.net_return),
        },
        "selected_trial_id": registry.selected_trial_id,
        "specification_file_sha256": artifact.specification_file_sha256,
        "trial_count": len(registry.trials),
    }


def _factor_candidate_inventory_json(
    inventory: FactorCandidateInventory,
) -> dict[str, object]:
    return {
        "ok": True,
        "inventory_id": inventory.inventory_id,
        "grammar_version": inventory.grammar_version,
        "grammar_sha256": inventory.grammar_sha256,
        "max_trials": inventory.max_trials,
        "trial_count": inventory.trial_count,
        "unique_hypothesis_count": inventory.unique_hypothesis_count,
        "rejected_count": inventory.rejected_count,
        "duplicate_count": inventory.duplicate_count,
        "candidates": [
            {
                "candidate_id": item.candidate_id,
                "grammar_version": item.grammar_version,
                "grammar_sha256": item.grammar_sha256,
                "template_id": item.template_id,
                "template_version": item.template_version,
                "economic_family": item.economic_family.value,
                "parameters": [{"name": name, "value": value} for name, value in item.parameters],
                "expression": item.expression,
                "canonical_expression": item.canonical_expression,
                "expression_sha256": item.expression_sha256,
                "required_warmup": item.required_warmup,
                "ast_node_count": item.ast_node_count,
            }
            for item in inventory.candidates
        ],
        "rejected": [
            {
                "attempt_id": item.attempt_id,
                "template_id": item.template_id,
                "template_version": item.template_version,
                "economic_family": item.economic_family.value,
                "parameters": [{"name": name, "value": value} for name, value in item.parameters],
                "expression": item.expression,
                "reason_codes": [reason.value for reason in item.reason_codes],
                "canonical_expression": item.canonical_expression,
                "duplicate_of_candidate_id": item.duplicate_of_candidate_id,
            }
            for item in inventory.rejected
        ],
        "warnings": list(inventory.warnings),
        "research_only": inventory.research_only,
        "market_data_accessed": False,
        "llm_accessed": False,
        "holdout_accessed": False,
        "online_configuration_changed": False,
    }
