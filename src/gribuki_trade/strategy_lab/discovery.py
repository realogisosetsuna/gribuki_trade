"""确定性、有预算约束且仅供研究的因子候选生成。

生成器将带版本的模板语法展开为安全因子 DSL 可接受的表达式。它不会评估
结果、查看留出集、调用大语言模型、变更策略配置或发布候选。其唯一输出
是可审计的候选清单及全部拒绝原因。
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import re
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from statistics import fmean
from typing import ClassVar

from gribuki_trade.strategy_lab.factors import (
    FactorDSLConfig,
    FactorExpressionError,
    compile_factor_expression,
)


class FactorEconomicFamily(StrEnum):
    """经济学意图，并不声称因子具有预测能力。"""

    LIQUIDITY = "LIQUIDITY"
    MEAN_REVERSION = "MEAN_REVERSION"
    MOMENTUM = "MOMENTUM"
    TREND = "TREND"
    VOLATILITY = "VOLATILITY"
    VOLUME_CONFIRMATION = "VOLUME_CONFIRMATION"


class TemplateParameterKind(StrEnum):
    COLUMN = "COLUMN"
    WINDOW = "WINDOW"


@dataclass(frozen=True, slots=True)
class TemplateParameter:
    parameter_id: str
    kind: TemplateParameterKind
    """Empty values mean use the corresponding grammar-wide whitelist."""

    values: tuple[str | int, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.parameter_id, "parameter_id")
        if not isinstance(self.kind, TemplateParameterKind):
            raise ValueError("kind must be a TemplateParameterKind")
        if len(self.values) != len(set(self.values)):
            raise ValueError("template parameter values must be unique")
        if self.kind is TemplateParameterKind.COLUMN:
            if any(
                not isinstance(value, str)
                or re.fullmatch(r"[a-z][a-z0-9_]*", value) is None
                for value in self.values
            ):
                raise ValueError("column parameter values must be safe identifiers")
        elif any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1
            for value in self.values
        ):
            raise ValueError("window parameter values must be positive integers")


@dataclass(frozen=True, slots=True)
class FactorTemplate:
    template_id: str
    economic_family: FactorEconomicFamily
    expression_pattern: str
    parameters: tuple[TemplateParameter, ...] = ()
    ordered_window_pairs: tuple[tuple[str, str], ...] = ()
    template_version: str = "1"

    def __post_init__(self) -> None:
        _require_identifier(self.template_id, "template_id")
        _require_identifier(self.template_version, "template_version")
        if not isinstance(self.economic_family, FactorEconomicFamily):
            raise ValueError("economic_family must be a FactorEconomicFamily")
        if not self.expression_pattern.strip() or len(self.expression_pattern) > 1_000:
            raise ValueError("expression_pattern must be bounded and non-empty")
        parameter_ids = tuple(item.parameter_id for item in self.parameters)
        if len(parameter_ids) != len(set(parameter_ids)):
            raise ValueError("template parameter IDs must be unique")
        placeholders = tuple(
            re.findall(r"\{([a-z][a-z0-9_]*)\}", self.expression_pattern)
        )
        residue = re.sub(
            r"\{[a-z][a-z0-9_]*\}",
            "",
            self.expression_pattern,
        )
        if "{" in residue or "}" in residue:
            raise ValueError("expression_pattern contains an invalid placeholder")
        if set(placeholders) != set(parameter_ids):
            raise ValueError("template placeholders must exactly match parameter IDs")
        parameter_map = {item.parameter_id: item for item in self.parameters}
        for short_id, long_id in self.ordered_window_pairs:
            if short_id == long_id:
                raise ValueError("ordered window pair IDs must be distinct")
            if short_id not in parameter_map or long_id not in parameter_map:
                raise ValueError("ordered window pair references an unknown parameter")
            if (
                parameter_map[short_id].kind is not TemplateParameterKind.WINDOW
                or parameter_map[long_id].kind is not TemplateParameterKind.WINDOW
            ):
                raise ValueError("ordered pairs may reference only window parameters")


@dataclass(frozen=True, slots=True)
class FactorTemplateGrammar:
    grammar_version: str
    window_whitelist: tuple[int, ...]
    column_whitelist: tuple[str, ...]
    templates: tuple[FactorTemplate, ...]
    max_ast_nodes: int = 64
    max_ast_depth: int = 12
    max_required_warmup: int = 504

    def __post_init__(self) -> None:
        _require_identifier(self.grammar_version, "grammar_version")
        windows = tuple(sorted(self.window_whitelist))
        if (
            not windows
            or len(windows) != len(set(windows))
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value < 1
                for value in windows
            )
        ):
            raise ValueError("window_whitelist must be unique positive integers")
        columns = tuple(sorted(self.column_whitelist))
        if (
            not columns
            or len(columns) != len(set(columns))
            or any(re.fullmatch(r"[a-z][a-z0-9_]*", item) is None for item in columns)
        ):
            raise ValueError("column_whitelist must contain unique safe identifiers")
        if not self.templates:
            raise ValueError("templates must not be empty")
        template_ids = tuple(item.template_id for item in self.templates)
        if len(template_ids) != len(set(template_ids)):
            raise ValueError("template IDs must be unique")
        for name in ("max_ast_nodes", "max_ast_depth", "max_required_warmup"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for template in self.templates:
            for parameter in template.parameters:
                if parameter.kind is TemplateParameterKind.WINDOW:
                    values = parameter.values or windows
                    if any(value not in windows for value in values):
                        raise ValueError(
                            "template window values must belong to window_whitelist"
                        )
                else:
                    values = parameter.values or columns
                    if any(value not in columns for value in values):
                        raise ValueError(
                            "template column values must belong to column_whitelist"
                        )
        object.__setattr__(self, "window_whitelist", windows)
        object.__setattr__(self, "column_whitelist", columns)
        object.__setattr__(
            self,
            "templates",
            tuple(sorted(self.templates, key=lambda item: item.template_id)),
        )

    @property
    def grammar_sha256(self) -> str:
        return _sha256_document(_grammar_document(self))

    @property
    def dsl_config(self) -> FactorDSLConfig:
        return FactorDSLConfig(
            allowed_columns=self.column_whitelist,
            max_ast_nodes=self.max_ast_nodes,
            max_depth=self.max_ast_depth,
            max_window=max(self.window_whitelist),
            max_required_warmup=self.max_required_warmup,
        )


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


@dataclass(frozen=True, slots=True)
class FactorSearchBudget:
    max_trials: int = 100

    ABSOLUTE_MAX_TRIALS: ClassVar[int] = 100_000

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_trials, bool)
            or not isinstance(self.max_trials, int)
            or self.max_trials < 1
        ):
            raise ValueError("max_trials must be a positive integer")
        if self.max_trials > self.ABSOLUTE_MAX_TRIALS:
            raise ValueError(
                f"max_trials must not exceed {self.ABSOLUTE_MAX_TRIALS}"
            )


class FactorSearchBudgetExceeded(ValueError):
    def __init__(self, *, required_trials: int, max_trials: int) -> None:
        self.required_trials = required_trials
        self.max_trials = max_trials
        super().__init__(
            "factor search requires at least "
            f"{required_trials} trials; budget permits {max_trials}"
        )


class CandidateRejectionReason(StrEnum):
    AST_DEPTH_LIMIT_EXCEEDED = "AST_DEPTH_LIMIT_EXCEEDED"
    AST_NODE_LIMIT_EXCEEDED = "AST_NODE_LIMIT_EXCEEDED"
    DUPLICATE_CANONICAL_EXPRESSION = "DUPLICATE_CANONICAL_EXPRESSION"
    EXPRESSION_REJECTED_BY_DSL = "EXPRESSION_REJECTED_BY_DSL"
    WARMUP_LIMIT_EXCEEDED = "WARMUP_LIMIT_EXCEEDED"
    WINDOW_OUTSIDE_WHITELIST = "WINDOW_OUTSIDE_WHITELIST"


@dataclass(frozen=True, slots=True)
class FactorCandidate:
    candidate_id: str
    grammar_version: str
    grammar_sha256: str
    template_id: str
    template_version: str
    economic_family: FactorEconomicFamily
    parameters: tuple[tuple[str, str], ...]
    expression: str
    canonical_expression: str
    expression_sha256: str
    required_warmup: int
    ast_node_count: int


@dataclass(frozen=True, slots=True)
class RejectedFactorCandidate:
    attempt_id: str
    template_id: str
    template_version: str
    economic_family: FactorEconomicFamily
    parameters: tuple[tuple[str, str], ...]
    expression: str
    reason_codes: tuple[CandidateRejectionReason, ...]
    canonical_expression: str | None = None
    duplicate_of_candidate_id: str | None = None


@dataclass(frozen=True, slots=True)
class FactorCandidateInventory:
    inventory_id: str
    grammar_version: str
    grammar_sha256: str
    max_trials: int
    trial_count: int
    unique_hypothesis_count: int
    rejected_count: int
    duplicate_count: int
    candidates: tuple[FactorCandidate, ...]
    rejected: tuple[RejectedFactorCandidate, ...]
    warnings: tuple[str, ...]
    research_only: bool = True


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


class RedundancyRejectionReason(StrEnum):
    INSUFFICIENT_OBSERVATIONS = "INSUFFICIENT_OBSERVATIONS"
    INSUFFICIENT_PAIRWISE_OVERLAP = "INSUFFICIENT_PAIRWISE_OVERLAP"
    MISSING_SERIES = "MISSING_SERIES"
    NONFINITE_OBSERVATION = "NONFINITE_OBSERVATION"
    REDUNDANT_CORRELATION = "REDUNDANT_CORRELATION"
    SERIES_LENGTH_MISMATCH = "SERIES_LENGTH_MISMATCH"
    ZERO_VARIANCE = "ZERO_VARIANCE"


@dataclass(frozen=True, slots=True)
class RedundancyFilterConfig:
    maximum_absolute_correlation: float = 0.95
    minimum_overlap: int = 20
    within_family_only: bool = False

    def __post_init__(self) -> None:
        if (
            not math.isfinite(self.maximum_absolute_correlation)
            or not 0 <= self.maximum_absolute_correlation < 1
        ):
            raise ValueError("maximum_absolute_correlation must be in [0, 1)")
        if (
            isinstance(self.minimum_overlap, bool)
            or not isinstance(self.minimum_overlap, int)
            or self.minimum_overlap < 2
        ):
            raise ValueError("minimum_overlap must be at least two")


@dataclass(frozen=True, slots=True)
class RedundancyRejection:
    candidate_id: str
    reason_codes: tuple[RedundancyRejectionReason, ...]
    compared_with_candidate_id: str | None = None
    correlation: float | None = None
    overlap: int | None = None


@dataclass(frozen=True, slots=True)
class FactorRedundancyResult:
    retained: tuple[FactorCandidate, ...]
    rejected: tuple[RedundancyRejection, ...]
    input_candidate_count: int
    retained_count: int
    rejected_count: int


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


def _grammar_document(grammar: FactorTemplateGrammar) -> dict[str, object]:
    return {
        "grammar_version": grammar.grammar_version,
        "window_whitelist": list(grammar.window_whitelist),
        "column_whitelist": list(grammar.column_whitelist),
        "max_ast_nodes": grammar.max_ast_nodes,
        "max_ast_depth": grammar.max_ast_depth,
        "max_required_warmup": grammar.max_required_warmup,
        "templates": [
            {
                "template_id": template.template_id,
                "template_version": template.template_version,
                "economic_family": template.economic_family.value,
                "expression_pattern": template.expression_pattern,
                "parameters": [
                    {
                        "parameter_id": parameter.parameter_id,
                        "kind": parameter.kind.value,
                        "values": list(parameter.values),
                    }
                    for parameter in template.parameters
                ],
                "ordered_window_pairs": [
                    list(pair) for pair in template.ordered_window_pairs
                ],
            }
            for template in grammar.templates
        ],
    }


def _sha256_document(document: object) -> str:
    encoded = json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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


def _require_identifier(value: str, name: str) -> None:
    if not isinstance(value, str) or _SAFE_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a safe non-empty identifier")
