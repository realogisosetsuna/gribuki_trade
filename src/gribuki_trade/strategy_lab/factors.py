"""用于探索性技术因子的有界时点 DSL。

表达式采用常见算术及极精简的函数集合。系统只解析并解释表达式，绝不求值
任意 Python 代码。所有运算符只查看尾部历史，缺失观测会继续传播，并在
评估前计算所需预热长度。

支持的函数：

``lag(series, n)``
    恰好 ``n`` 个观测之前的值。
``return(series, n)``
    最近 ``n`` 个观测的简单收益率。由于 ``return`` 是 Python 关键字，
    ``ret`` 可作为其别名。
``ma(series, n)``
    尾部窗口算术平均值。
``vol(series, n)``
    所给序列尾部窗口的总体标准差。收益波动率须显式写为
    ``vol(return(close, 1), n)``。
``zscore(series, n)``
    尾部窗口标准分数；常数窗口结果为零。
"""

from __future__ import annotations

import ast
import hashlib
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from statistics import fmean, pstdev
from typing import TypeAlias


class FactorExpressionError(ValueError):
    """表达式违反安全因子语言约定。"""


@dataclass(frozen=True, slots=True)
class FactorDSLConfig:
    allowed_columns: tuple[str, ...] = (
        "amount",
        "close",
        "high",
        "low",
        "open",
        "volume",
    )
    max_ast_nodes: int = 64
    max_depth: int = 12
    max_window: int = 252
    max_required_warmup: int = 504
    max_absolute_constant: float = 1_000_000.0

    def __post_init__(self) -> None:
        columns = tuple(sorted(self.allowed_columns))
        if not columns or len(columns) != len(set(columns)):
            raise ValueError("allowed_columns must be non-empty and unique")
        if any(re.fullmatch(r"[a-z][a-z0-9_]*", item) is None for item in columns):
            raise ValueError("allowed columns must use safe lowercase identifiers")
        for name in (
            "max_ast_nodes",
            "max_depth",
            "max_window",
            "max_required_warmup",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if (
            not math.isfinite(self.max_absolute_constant)
            or self.max_absolute_constant <= 0
        ):
            raise ValueError("max_absolute_constant must be finite and positive")
        object.__setattr__(self, "allowed_columns", columns)


@dataclass(frozen=True, slots=True)
class CompiledFactorExpression:
    source: str
    canonical_expression: str
    expression_sha256: str
    required_warmup: int
    required_observations: int
    ast_node_count: int
    referenced_columns: tuple[str, ...]
    window_lengths: tuple[int, ...]
    _tree: ast.Expression

    def __repr__(self) -> str:
        return (
            "CompiledFactorExpression("
            f"source={self.source!r}, required_warmup={self.required_warmup}, "
            f"ast_node_count={self.ast_node_count})"
        )


@dataclass(frozen=True, slots=True)
class FactorEvaluation:
    values: tuple[float | None, ...]
    required_warmup: int
    first_valid_index: int | None


@dataclass(frozen=True, slots=True)
class _Analysis:
    warmup: int
    is_series: bool
    columns: frozenset[str]
    windows: frozenset[int]


def compile_factor_expression(
    expression: str,
    config: FactorDSLConfig | None = None,
) -> CompiledFactorExpression:
    """在不编译可执行 Python 的情况下解析并校验表达式。"""

    resolved = config or FactorDSLConfig()
    if not isinstance(expression, str) or not expression.strip():
        raise FactorExpressionError("expression must be a non-empty string")
    if len(expression) > 1_000:
        raise FactorExpressionError("expression exceeds the length limit")
    # ``return`` 无法被解析为 Python 调用，因此解析前只规范化这一精确的
    # 函数词元；不会执行任何由用户控制的标识符。
    normalized = re.sub(r"\breturn\s*(?=\()", "ret", expression.strip())
    try:
        tree = ast.parse(normalized, mode="eval")
    except SyntaxError as exc:
        raise FactorExpressionError("invalid factor expression syntax") from exc
    node_count = sum(1 for _ in ast.walk(tree))
    if node_count > resolved.max_ast_nodes:
        raise FactorExpressionError("expression exceeds the AST node limit")
    analysis = _analyze(tree.body, resolved, depth=1)
    if not analysis.is_series:
        raise FactorExpressionError("factor expression must produce a series")
    if analysis.warmup > resolved.max_required_warmup:
        raise FactorExpressionError("expression exceeds the warm-up limit")
    canonical = ast.unparse(tree.body)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return CompiledFactorExpression(
        source=expression.strip(),
        canonical_expression=canonical,
        expression_sha256=digest,
        required_warmup=analysis.warmup,
        required_observations=analysis.warmup + 1,
        ast_node_count=node_count,
        referenced_columns=tuple(sorted(analysis.columns)),
        window_lengths=tuple(sorted(analysis.windows)),
        _tree=tree,
    )


def validate_pit_warmup(
    compiled: CompiledFactorExpression,
    available_observations: int,
) -> None:
    if available_observations < compiled.required_observations:
        raise FactorExpressionError(
            "insufficient point-in-time history: "
            f"requires {compiled.required_observations}, got {available_observations}"
        )


def evaluate_factor_expression(
    compiled: CompiledFactorExpression,
    columns: Mapping[str, Sequence[float | int | None]],
) -> FactorEvaluation:
    """仅使用尾部历史观测解释已校验的表达式。"""

    missing = set(compiled.referenced_columns) - set(columns)
    if missing:
        raise FactorExpressionError(
            f"missing required columns: {', '.join(sorted(missing))}"
        )
    lengths = {len(columns[name]) for name in compiled.referenced_columns}
    if len(lengths) != 1:
        raise FactorExpressionError("referenced columns must have equal lengths")
    length = next(iter(lengths))
    validate_pit_warmup(compiled, length)
    normalized_columns: dict[str, _Series] = {}
    for name in compiled.referenced_columns:
        values: list[float | None] = []
        for value in columns[name]:
            if value is None:
                values.append(None)
                continue
            normalized = float(value)
            if not math.isfinite(normalized):
                raise FactorExpressionError("input columns must be finite or missing")
            values.append(normalized)
        normalized_columns[name] = values

    try:
        output = _interpret(compiled._tree.body, normalized_columns, length)
    except ArithmeticError as exc:
        raise FactorExpressionError("factor arithmetic produced a non-finite value") from exc
    if isinstance(output, float):
        raise AssertionError("validated series expression produced a scalar")
    if any(value is not None and not math.isfinite(value) for value in output):
        raise FactorExpressionError("factor output must be finite or missing")
    for index in range(compiled.required_warmup):
        if output[index] is not None:
            raise AssertionError("internal error: factor violated declared warm-up")
    first_valid = next(
        (index for index, value in enumerate(output) if value is not None),
        None,
    )
    return FactorEvaluation(
        values=tuple(output),
        required_warmup=compiled.required_warmup,
        first_valid_index=first_valid,
    )


def _analyze(
    node: ast.AST,
    config: FactorDSLConfig,
    *,
    depth: int,
) -> _Analysis:
    if depth > config.max_depth:
        raise FactorExpressionError("expression exceeds the AST depth limit")
    if isinstance(node, ast.Name):
        if node.id not in config.allowed_columns:
            raise FactorExpressionError(f"name is not allowed: {node.id}")
        return _Analysis(0, True, frozenset((node.id,)), frozenset())
    if isinstance(node, ast.Constant):
        value = node.value
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise FactorExpressionError("only numeric constants are allowed")
        numeric = float(value)
        if not math.isfinite(numeric) or abs(numeric) > config.max_absolute_constant:
            raise FactorExpressionError("numeric constant is outside safe bounds")
        return _Analysis(0, False, frozenset(), frozenset())
    if isinstance(node, ast.UnaryOp):
        if not isinstance(node.op, (ast.UAdd, ast.USub)):
            raise FactorExpressionError("unary operator is not allowed")
        return _analyze(node.operand, config, depth=depth + 1)
    if isinstance(node, ast.BinOp):
        if not isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div)):
            raise FactorExpressionError("binary operator is not allowed")
        left = _analyze(node.left, config, depth=depth + 1)
        right = _analyze(node.right, config, depth=depth + 1)
        return _Analysis(
            warmup=max(left.warmup, right.warmup),
            is_series=left.is_series or right.is_series,
            columns=left.columns | right.columns,
            windows=left.windows | right.windows,
        )
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name) or node.func.id not in {
            "lag",
            "ret",
            "ma",
            "vol",
            "zscore",
        }:
            raise FactorExpressionError("function is not allowed")
        if node.keywords or len(node.args) != 2:
            raise FactorExpressionError("factor functions require two positional arguments")
        source = _analyze(node.args[0], config, depth=depth + 1)
        if not source.is_series:
            raise FactorExpressionError("factor function input must be a series")
        window = _literal_window(node.args[1], config)
        additional_warmup = (
            window if node.func.id == "lag" or node.func.id == "ret" else window - 1
        )
        return _Analysis(
            warmup=source.warmup + additional_warmup,
            is_series=True,
            columns=source.columns,
            windows=source.windows | frozenset((window,)),
        )
    raise FactorExpressionError(f"syntax node is not allowed: {type(node).__name__}")


def _literal_window(node: ast.AST, config: FactorDSLConfig) -> int:
    if (
        not isinstance(node, ast.Constant)
        or isinstance(node.value, bool)
        or not isinstance(node.value, int)
    ):
        raise FactorExpressionError("window must be a positive integer literal")
    window = node.value
    if not 1 <= window <= config.max_window:
        raise FactorExpressionError("window is outside configured bounds")
    return window


_Series: TypeAlias = list[float | None]
_Value: TypeAlias = float | _Series


def _interpret(
    node: ast.AST,
    columns: Mapping[str, _Series],
    length: int,
) -> _Value:
    if isinstance(node, ast.Name):
        return list(columns[node.id])
    if isinstance(node, ast.Constant):
        if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
            raise AssertionError("validated constant was not numeric")
        return float(node.value)
    if isinstance(node, ast.UnaryOp):
        operand = _interpret(node.operand, columns, length)
        multiplier = -1.0 if isinstance(node.op, ast.USub) else 1.0
        return _map_value(operand, lambda value: multiplier * value)
    if isinstance(node, ast.BinOp):
        left = _interpret(node.left, columns, length)
        right = _interpret(node.right, columns, length)
        return _binary_value(left, right, node.op, length)
    if isinstance(node, ast.Call):
        source = _interpret(node.args[0], columns, length)
        if isinstance(source, float):
            raise AssertionError("validated factor function received a scalar")
        window_node = node.args[1]
        if not isinstance(window_node, ast.Constant) or not isinstance(
            window_node.value, int
        ):
            raise AssertionError("validated window was not a literal")
        window = window_node.value
        if not isinstance(node.func, ast.Name):
            raise AssertionError("validated call target was not a name")
        if node.func.id == "lag":
            return [None] * window + source[:-window]
        if node.func.id == "ret":
            lagged = [None] * window + source[:-window]
            return [
                None
                if current is None or previous is None or previous == 0
                else current / previous - 1.0
                for current, previous in zip(source, lagged, strict=True)
            ]
        return _rolling(source, window, node.func.id)
    raise AssertionError("validated expression contained an unknown node")


def _map_value(value: _Value, operation: Callable[[float], float]) -> _Value:
    if isinstance(value, float):
        return float(operation(value))
    return [None if item is None else float(operation(item)) for item in value]


def _binary_value(
    left: _Value,
    right: _Value,
    operator: ast.operator,
    length: int,
) -> _Value:
    if isinstance(left, float) and isinstance(right, float):
        result = _apply_binary(left, right, operator)
        return float("nan") if result is None else result
    left_values = [left] * length if isinstance(left, float) else left
    right_values = [right] * length if isinstance(right, float) else right
    return [
        None
        if left_item is None or right_item is None
        else _apply_binary(left_item, right_item, operator)
        for left_item, right_item in zip(left_values, right_values, strict=True)
    ]


def _apply_binary(left: float, right: float, operator: ast.operator) -> float | None:
    if isinstance(operator, ast.Add):
        return left + right
    if isinstance(operator, ast.Sub):
        return left - right
    if isinstance(operator, ast.Mult):
        result = left * right
        if not math.isfinite(result):
            raise ArithmeticError("non-finite multiplication")
        return result
    if right == 0:
        raise ArithmeticError("division by zero")
    result = left / right
    if not math.isfinite(result):
        raise ArithmeticError("non-finite division")
    return result


def _rolling(source: _Series, window: int, function_id: str) -> _Series:
    output: _Series = [None] * (window - 1)
    for end in range(window, len(source) + 1):
        values = source[end - window : end]
        if any(value is None for value in values):
            output.append(None)
            continue
        numeric = [value for value in values if value is not None]
        if function_id == "ma":
            output.append(fmean(numeric))
        elif function_id == "vol":
            output.append(pstdev(numeric))
        elif function_id == "zscore":
            deviation = pstdev(numeric)
            output.append(0.0 if deviation == 0 else (numeric[-1] - fmean(numeric)) / deviation)
        else:
            raise AssertionError("validated rolling function was unknown")
    return output
