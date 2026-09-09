from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace

from gribuki_trade.strategy_lab.exit_serialization import (
    bar_document,
    registry_to_json,
    sha256_document,
)


def test_sha256_document_is_canonical_for_mapping_order() -> None:
    first = sha256_document({"b": 2, "a": 1})
    second = sha256_document({"a": 1, "b": 2})

    assert first == second
    assert len(first) == 64


def test_bar_document_and_registry_json_are_stable_wire_values() -> None:
    bar = SimpleNamespace(
        close=Decimal("10.10"),
        completed_at=datetime(2026, 8, 14, 7, tzinfo=UTC),
        high=Decimal("10.50"),
        low=Decimal("9.50"),
        lower_price_limit=Decimal("8.00"),
        open=Decimal("10.00"),
        session_date=date(2026, 8, 14),
        source_id="daily-bars",
        source_revision="r-1",
        suspended=False,
        upper_price_limit=Decimal("12.00"),
        volume_shares=100,
    )
    document = bar_document(bar)
    assert document["close"] == "10.10"
    assert document["session_date"] == "2026-08-14"

    registry = SimpleNamespace(
        baseline_holdout=SimpleNamespace(
            metrics=SimpleNamespace(
                average_return=Decimal("0"),
                blocked_session_count=0,
                completed_count=0,
                episode_count=1,
                gross_loss_cny=Decimal("0"),
                gross_profit_cny=Decimal("0"),
                hit_rate=Decimal("0"),
                losing_count=0,
                marked_open_count=1,
                maximum_drawdown=Decimal("0"),
                net_return=Decimal("0"),
                profit_factor=None,
                winning_count=0,
            ),
            observation_indices=(0,),
            outcomes=(),
            parameters=SimpleNamespace(fingerprint="a" * 64),
        ),
        baseline_trial_id="baseline",
        cost_model_sha256="b" * 64,
        created_at=datetime(2026, 8, 15, tzinfo=UTC),
        dataset_sha256="c" * 64,
        experiment_version="experiment@1",
        objective=SimpleNamespace(value="NET_RETURN"),
        promotion_authorized=False,
        registry_id="registry-1",
        research_only=True,
        search_space_sha256="d" * 64,
        selected_holdout=SimpleNamespace(
            metrics=SimpleNamespace(
                average_return=Decimal("0"),
                blocked_session_count=0,
                completed_count=0,
                episode_count=1,
                gross_loss_cny=Decimal("0"),
                gross_profit_cny=Decimal("0"),
                hit_rate=Decimal("0"),
                losing_count=0,
                marked_open_count=1,
                maximum_drawdown=Decimal("0"),
                net_return=Decimal("0"),
                profit_factor=None,
                winning_count=0,
            ),
            observation_indices=(0,),
            outcomes=(),
            parameters=SimpleNamespace(fingerprint="a" * 64),
        ),
        selected_trial_id="selected",
        trials=(),
        walk_forward_sha256="e" * 64,
    )
    encoded = registry_to_json(registry)
    assert '"registry_id":"registry-1"' in encoded
    assert encoded == registry_to_json(registry)
