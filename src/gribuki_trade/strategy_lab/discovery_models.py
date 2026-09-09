"""因子发现的模板、候选清单与冗余过滤结果模型。"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

from gribuki_trade.strategy_lab.factors import FactorDSLConfig

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$")


def _require_identifier(value: str, name: str) -> None:
    if not isinstance(value, str) or _SAFE_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a safe non-empty identifier")


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

