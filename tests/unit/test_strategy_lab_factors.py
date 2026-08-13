from __future__ import annotations

import math

import pytest

from gribuki_trade.strategy_lab.factors import (
    FactorDSLConfig,
    FactorExpressionError,
    compile_factor_expression,
    evaluate_factor_expression,
    validate_pit_warmup,
)


def test_composable_factor_has_declared_pit_warmup_and_trailing_values() -> None:
    compiled = compile_factor_expression(
        "zscore(return(close, 1), 3) + ma(volume, 2) * 0.0"
    )
    close = [10.0, 10.5, 10.2, 10.8, 11.0, 10.9]
    volume = [100.0, 110.0, 90.0, 130.0, 120.0, 115.0]
    result = evaluate_factor_expression(
        compiled,
        {"close": close, "volume": volume},
    )

    assert compiled.required_warmup == 3
    assert compiled.required_observations == 4
    assert compiled.window_lengths == (1, 2, 3)
    assert result.values[:3] == (None, None, None)
    assert result.first_valid_index == 3
    assert all(value is None or math.isfinite(value) for value in result.values)

    changed = evaluate_factor_expression(
        compiled,
        {"close": [*close[:-1], 99.0], "volume": volume},
    )
    assert changed.values[:-1] == result.values[:-1]


def test_nested_warmup_is_additive_and_insufficient_history_is_rejected() -> None:
    compiled = compile_factor_expression("lag(ma(close, 5), 2)")
    assert compiled.required_warmup == 6

    with pytest.raises(FactorExpressionError, match="requires 7"):
        validate_pit_warmup(compiled, 6)
    with pytest.raises(FactorExpressionError, match="insufficient"):
        evaluate_factor_expression(compiled, {"close": [1, 2, 3, 4, 5, 6]})


@pytest.mark.parametrize(
    "expression",
    (
        "__import__('os').system('whoami')",
        "close.__class__",
        "close[0]",
        "lambda: close",
        "close ** 2",
        "ma(close, volume)",
        "ma(close, 253)",
        "unknown(close, 2)",
        "[value for value in close]",
    ),
)
def test_arbitrary_python_and_unbounded_constructs_are_rejected(expression: str) -> None:
    with pytest.raises(FactorExpressionError):
        compile_factor_expression(expression)


def test_complexity_depth_and_constants_are_bounded() -> None:
    with pytest.raises(FactorExpressionError, match="node limit"):
        compile_factor_expression(
            " + ".join(["close"] * 20),
            FactorDSLConfig(max_ast_nodes=10),
        )
    with pytest.raises(FactorExpressionError, match="safe bounds"):
        compile_factor_expression("close * 1000001")


def test_missing_and_zero_denominators_are_not_filled_with_neutral_values() -> None:
    compiled = compile_factor_expression("return(close, 1)")
    result = evaluate_factor_expression(
        compiled,
        {"close": [1.0, 0.0, 2.0, None, 3.0]},
    )
    assert result.values == (None, -1.0, None, None, None)


def test_inputs_and_outputs_must_remain_finite() -> None:
    compiled = compile_factor_expression("close / (1 - 1)")
    with pytest.raises(FactorExpressionError, match="non-finite"):
        evaluate_factor_expression(compiled, {"close": [1.0]})

    with pytest.raises(FactorExpressionError, match="finite"):
        evaluate_factor_expression(
            compile_factor_expression("close"),
            {"close": [float("nan")]},
        )
