"""确定性、有预算约束且仅供研究的因子候选生成。

生成器将带版本的模板语法展开为安全因子 DSL 可接受的表达式。它不会评估
结果、查看留出集、调用大语言模型、变更策略配置或发布候选。其唯一输出
是可审计的候选清单及全部拒绝原因。
"""

from __future__ import annotations

import itertools
import math
import re
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from statistics import fmean

from gribuki_trade.strategy_lab.discovery_models import (
    CandidateRejectionReason,
    FactorCandidate,
    FactorCandidateInventory,
    FactorEconomicFamily,
    FactorRedundancyResult,
    FactorSearchBudget,
    FactorSearchBudgetExceeded,
    FactorTemplate,
    FactorTemplateGrammar,
    RedundancyFilterConfig,
    RedundancyRejection,
    RedundancyRejectionReason,
    RejectedFactorCandidate,
    TemplateParameter,
    TemplateParameterKind,
    _sha256_document,
)
from gribuki_trade.strategy_lab.factors import (
    FactorExpressionError,
    compile_factor_expression,
)

__all__ = [
    "CandidateRejectionReason",
    "FactorCandidate",
    "FactorCandidateInventory",
    "FactorEconomicFamily",
    "FactorRedundancyResult",
    "FactorSearchBudget",
    "FactorSearchBudgetExceeded",
    "FactorTemplate",
    "FactorTemplateGrammar",
    "RedundancyFilterConfig",
    "RedundancyRejection",
    "RedundancyRejectionReason",
    "RejectedFactorCandidate",
    "TemplateParameter",
    "TemplateParameterKind",
    "default_factor_template_grammar",
    "filter_redundant_candidates",
    "generate_factor_candidates",
]


def default_factor_template_grammar() -> FactorTemplateGrammar:
    """返回用于有界技术探索的不可变第一版语法。"""

    window = TemplateParameter(
        "window",
        TemplateParameterKind.WINDOW,
        (5, 10, 20, 60, 120),
    )
    short = TemplateParameter(
        "short_window",
        TemplateParameterKind.WINDOW,
        (5, 10, 20),
    )
    long = TemplateParameter(
        "long_window",
        TemplateParameterKind.WINDOW,
        (20, 60, 120),
    )
    return FactorTemplateGrammar(
        grammar_version="technical-factor-grammar@1",
        window_whitelist=(1, 5, 10, 20, 60, 120),
        column_whitelist=("amount", "close", "high", "low", "open", "volume"),
        templates=(
            FactorTemplate(
                "close-momentum",
                FactorEconomicFamily.MOMENTUM,
                "return(close, {window})",
                (window,),
            ),
            FactorTemplate(
                "moving-average-gap",
                FactorEconomicFamily.TREND,
                "ma(close, {short_window}) / ma(close, {long_window}) - 1",
                (short, long),
                (("short_window", "long_window"),),
            ),
            FactorTemplate(
                "negative-price-zscore",
                FactorEconomicFamily.MEAN_REVERSION,
                "-zscore(close, {window})",
                (window,),
            ),
            FactorTemplate(
                "return-volatility",
                FactorEconomicFamily.VOLATILITY,
                "-vol(return(close, 1), {window})",
                (window,),
            ),
            FactorTemplate(
                "turnover-zscore",
                FactorEconomicFamily.LIQUIDITY,
                "zscore(amount, {window})",
                (window,),
            ),
            FactorTemplate(
                "volume-confirmed-momentum",
                FactorEconomicFamily.VOLUME_CONFIRMATION,
                "return(close, {window}) * zscore(volume, {window})",
                (window,),
            ),
        ),
    )


def generate_factor_candidates(
    grammar: FactorTemplateGrammar,
    budget: FactorSearchBudget | None = None,
) -> FactorCandidateInventory:
    """展开全部合法参数组合；否则在生成部分结果前失败。"""

    resolved_budget = budget or FactorSearchBudget()
    bounded_attempts: list[
        tuple[FactorTemplate, tuple[tuple[str, str], ...], str]
    ] = []
    for attempt in _enumerate_attempts(grammar):
        bounded_attempts.append(attempt)
        if len(bounded_attempts) > resolved_budget.max_trials:
            # 在首次确认超限时立即停止，因此恶意构造的巨大笛卡尔积最多
            # 只能消耗 max_trials + 1 次展开。
            raise FactorSearchBudgetExceeded(
                required_trials=len(bounded_attempts),
                max_trials=resolved_budget.max_trials,
            )
    attempts = tuple(bounded_attempts)
    required_trials = len(attempts)

    candidates: list[FactorCandidate] = []
    rejected: list[RejectedFactorCandidate] = []
    canonical_to_candidate: dict[str, str] = {}
    for template, parameters, expression in attempts:
        attempt_id = _attempt_id(grammar, template, parameters, expression)
        try:
            compiled = compile_factor_expression(expression, grammar.dsl_config)
        except FactorExpressionError as exc:
            rejected.append(
                RejectedFactorCandidate(
                    attempt_id=attempt_id,
                    template_id=template.template_id,
                    template_version=template.template_version,
                    economic_family=template.economic_family,
                    parameters=parameters,
                    expression=expression,
                    reason_codes=(_dsl_rejection_reason(exc),),
                )
            )
            continue
        if set(compiled.window_lengths) - set(grammar.window_whitelist):
            rejected.append(
                RejectedFactorCandidate(
                    attempt_id=attempt_id,
                    template_id=template.template_id,
                    template_version=template.template_version,
                    economic_family=template.economic_family,
                    parameters=parameters,
                    expression=expression,
                    canonical_expression=compiled.canonical_expression,
                    reason_codes=(CandidateRejectionReason.WINDOW_OUTSIDE_WHITELIST,),
                )
            )
            continue
        existing_id = canonical_to_candidate.get(compiled.canonical_expression)
        if existing_id is not None:
            rejected.append(
                RejectedFactorCandidate(
                    attempt_id=attempt_id,
                    template_id=template.template_id,
                    template_version=template.template_version,
                    economic_family=template.economic_family,
                    parameters=parameters,
                    expression=expression,
                    canonical_expression=compiled.canonical_expression,
                    duplicate_of_candidate_id=existing_id,
                    reason_codes=(
                        CandidateRejectionReason.DUPLICATE_CANONICAL_EXPRESSION,
                    ),
                )
            )
            continue
        candidate_id = _candidate_id(
            grammar,
            template.economic_family,
            compiled.canonical_expression,
        )
        canonical_to_candidate[compiled.canonical_expression] = candidate_id
        candidates.append(
            FactorCandidate(
                candidate_id=candidate_id,
                grammar_version=grammar.grammar_version,
                grammar_sha256=grammar.grammar_sha256,
                template_id=template.template_id,
                template_version=template.template_version,
                economic_family=template.economic_family,
                parameters=parameters,
                expression=expression,
                canonical_expression=compiled.canonical_expression,
                expression_sha256=compiled.expression_sha256,
                required_warmup=compiled.required_warmup,
                ast_node_count=compiled.ast_node_count,
            )
        )

    candidate_items = tuple(candidates)
    rejected_items = tuple(rejected)
    warnings = (
        ("MULTIPLE_HYPOTHESIS_TESTING_REQUIRED",)
        if len(candidate_items) > 1
        else ()
    )
    inventory_identity = {
        "grammar_sha256": grammar.grammar_sha256,
        "max_trials": resolved_budget.max_trials,
        "attempt_ids": [
            _attempt_id(grammar, template, parameters, expression)
            for template, parameters, expression in attempts
        ],
        "candidate_ids": [item.candidate_id for item in candidate_items],
    }
    return FactorCandidateInventory(
        inventory_id=f"factor-inventory-{_sha256_document(inventory_identity)[:24]}",
        grammar_version=grammar.grammar_version,
        grammar_sha256=grammar.grammar_sha256,
        max_trials=resolved_budget.max_trials,
        trial_count=required_trials,
        unique_hypothesis_count=len(candidate_items),
        rejected_count=len(rejected_items),
        duplicate_count=sum(
            CandidateRejectionReason.DUPLICATE_CANONICAL_EXPRESSION
            in item.reason_codes
            for item in rejected_items
        ),
        candidates=candidate_items,
        rejected=rejected_items,
        warnings=warnings,
    )


def filter_redundant_candidates(
    candidates: Sequence[FactorCandidate],
    observations: Mapping[str, Sequence[float | int | None]],
    config: RedundancyFilterConfig | None = None,
) -> FactorRedundancyResult:
    """从调用方提供的样本中保守移除冗余候选。

    此函数是纯函数且顺序稳定。调用方只能提供开发集观测；本模块没有任何
    访问留出集的机制。
    """

    resolved = config or RedundancyFilterConfig()
    items = tuple(candidates)
    candidate_ids = tuple(item.candidate_id for item in items)
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("candidates must contain unique candidate IDs")
    observed_lengths = (
        len(observations[candidate_id])
        for candidate_id in candidate_ids
        if candidate_id in observations
    )
    length_counts = Counter(observed_lengths)
    expected_length = (
        max(length_counts, key=lambda length: (length_counts[length], length))
        if length_counts
        else None
    )

    retained: list[FactorCandidate] = []
    rejected: list[RedundancyRejection] = []
    normalized: dict[str, tuple[float | None, ...]] = {}
    for candidate in items:
        raw = observations.get(candidate.candidate_id)
        if raw is None:
            rejected.append(
                RedundancyRejection(
                    candidate.candidate_id,
                    (RedundancyRejectionReason.MISSING_SERIES,),
                )
            )
            continue
        if expected_length is None or len(raw) != expected_length:
            rejected.append(
                RedundancyRejection(
                    candidate.candidate_id,
                    (RedundancyRejectionReason.SERIES_LENGTH_MISMATCH,),
                )
            )
            continue
        try:
            series = tuple(
                None if value is None else _finite_float(value)
                for value in raw
            )
        except ValueError:
            rejected.append(
                RedundancyRejection(
                    candidate.candidate_id,
                    (RedundancyRejectionReason.NONFINITE_OBSERVATION,),
                )
            )
            continue
        numeric = tuple(value for value in series if value is not None)
        if len(numeric) < resolved.minimum_overlap:
            rejected.append(
                RedundancyRejection(
                    candidate.candidate_id,
                    (RedundancyRejectionReason.INSUFFICIENT_OBSERVATIONS,),
                    overlap=len(numeric),
                )
            )
            continue
        if _population_variance(numeric) == 0:
            rejected.append(
                RedundancyRejection(
                    candidate.candidate_id,
                    (RedundancyRejectionReason.ZERO_VARIANCE,),
                    overlap=len(numeric),
                )
            )
            continue

        rejection: RedundancyRejection | None = None
        for existing in retained:
            if (
                resolved.within_family_only
                and existing.economic_family is not candidate.economic_family
            ):
                continue
            existing_series = normalized[existing.candidate_id]
            pairs = tuple(
                (left, right)
                for left, right in zip(series, existing_series, strict=True)
                if left is not None and right is not None
            )
            if len(pairs) < resolved.minimum_overlap:
                rejection = RedundancyRejection(
                    candidate.candidate_id,
                    (RedundancyRejectionReason.INSUFFICIENT_PAIRWISE_OVERLAP,),
                    compared_with_candidate_id=existing.candidate_id,
                    overlap=len(pairs),
                )
                break
            correlation = _pearson(pairs)
            if correlation is None:
                rejection = RedundancyRejection(
                    candidate.candidate_id,
                    (RedundancyRejectionReason.ZERO_VARIANCE,),
                    compared_with_candidate_id=existing.candidate_id,
                    overlap=len(pairs),
                )
                break
            if abs(correlation) >= resolved.maximum_absolute_correlation:
                rejection = RedundancyRejection(
                    candidate.candidate_id,
                    (RedundancyRejectionReason.REDUNDANT_CORRELATION,),
                    compared_with_candidate_id=existing.candidate_id,
                    correlation=correlation,
                    overlap=len(pairs),
                )
                break
        if rejection is not None:
            rejected.append(rejection)
            continue
        normalized[candidate.candidate_id] = series
        retained.append(candidate)

    return FactorRedundancyResult(
        retained=tuple(retained),
        rejected=tuple(rejected),
        input_candidate_count=len(items),
        retained_count=len(retained),
        rejected_count=len(rejected),
    )


def _enumerate_attempts(
    grammar: FactorTemplateGrammar,
) -> Iterator[
    tuple[FactorTemplate, tuple[tuple[str, str], ...], str]
]:
    for template in grammar.templates:
        domains: list[tuple[str | int, ...]] = []
        for parameter in template.parameters:
            default_values: tuple[str | int, ...]
            if parameter.kind is TemplateParameterKind.WINDOW:
                default_values = grammar.window_whitelist
            else:
                default_values = grammar.column_whitelist
            domains.append(parameter.values or default_values)
        combinations = itertools.product(*domains) if domains else ((),)
        for combination in combinations:
            raw_parameters = {
                parameter.parameter_id: value
                for parameter, value in zip(
                    template.parameters,
                    combination,
                    strict=True,
                )
            }
            if any(
                int(raw_parameters[short_id]) >= int(raw_parameters[long_id])
                for short_id, long_id in template.ordered_window_pairs
            ):
                continue
            parameters = tuple(
                sorted((name, str(value)) for name, value in raw_parameters.items())
            )
            expression = template.expression_pattern
            for name, value in parameters:
                expression = expression.replace("{" + name + "}", value)
            yield template, parameters, expression


def _attempt_id(
    grammar: FactorTemplateGrammar,
    template: FactorTemplate,
    parameters: tuple[tuple[str, str], ...],
    expression: str,
) -> str:
    return "factor-attempt-" + _sha256_document(
        {
            "grammar_sha256": grammar.grammar_sha256,
            "template_id": template.template_id,
            "template_version": template.template_version,
            "parameters": [list(item) for item in parameters],
            "expression": expression,
        }
    )[:24]


def _candidate_id(
    grammar: FactorTemplateGrammar,
    family: FactorEconomicFamily,
    canonical_expression: str,
) -> str:
    return "factor-candidate-" + _sha256_document(
        {
            "grammar_sha256": grammar.grammar_sha256,
            "economic_family": family.value,
            "canonical_expression": canonical_expression,
        }
    )[:24]



def _finite_float(value: float | int) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean observations are invalid")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("observations must be finite or missing")
    return result


def _dsl_rejection_reason(
    error: FactorExpressionError,
) -> CandidateRejectionReason:
    message = str(error)
    if "AST node limit" in message:
        return CandidateRejectionReason.AST_NODE_LIMIT_EXCEEDED
    if "AST depth limit" in message:
        return CandidateRejectionReason.AST_DEPTH_LIMIT_EXCEEDED
    if "warm-up limit" in message:
        return CandidateRejectionReason.WARMUP_LIMIT_EXCEEDED
    return CandidateRejectionReason.EXPRESSION_REJECTED_BY_DSL


def _population_variance(values: Sequence[float]) -> float:
    average = fmean(values)
    return fmean((value - average) ** 2 for value in values)


def _pearson(pairs: Sequence[tuple[float, float]]) -> float | None:
    left_values = tuple(item[0] for item in pairs)
    right_values = tuple(item[1] for item in pairs)
    left_average = fmean(left_values)
    right_average = fmean(right_values)
    left_variance = fmean((value - left_average) ** 2 for value in left_values)
    right_variance = fmean((value - right_average) ** 2 for value in right_values)
    if left_variance == 0 or right_variance == 0:
        return None
    covariance = fmean(
        (left - left_average) * (right - right_average)
        for left, right in pairs
    )
    result = covariance / math.sqrt(left_variance * right_variance)
    return max(-1.0, min(1.0, result))


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$")
