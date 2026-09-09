"""通用策略实验的纯编码契约，不调用评价器或持久化存储。"""

from decimal import Decimal

import pytest

from gribuki_trade.strategy_lab.experiment_serialization import (
    metrics_document,
    sha256_document,
)
from gribuki_trade.strategy_lab.experiments import PerformanceMetrics


def test_metrics_document_preserves_decimal_precision_and_missing_values() -> None:
    metrics = PerformanceMetrics(
        net_return=Decimal("0.123456789123456789"),
        annualized_return=None,
        annualized_volatility=None,
        sharpe=None,
        max_drawdown=Decimal("0.01"),
        turnover=Decimal("1.0000"),
        trade_count=3,
        hit_rate=None,
        family_contributions=(("trend", Decimal("0.00200")),),
    )

    document = metrics_document(metrics)

    assert document["net_return"] == "0.123456789123456789"
    assert document["turnover"] == "1.0000"
    assert document["family_contributions"] == [["trend", "0.00200"]]
    assert document["annualized_return"] is None
    assert document["sharpe"] is None


def test_document_hash_is_independent_of_mapping_order() -> None:
    assert sha256_document({"版本": "研究", "trial_ids": ["a", "b"]}) == (
        sha256_document({"trial_ids": ["a", "b"], "版本": "研究"})
    )


def test_document_hash_rejects_nonfinite_json_numbers() -> None:
    with pytest.raises(ValueError, match="Out of range"):
        sha256_document({"score": float("nan")})
