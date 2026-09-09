"""实盘保护批次的 T+1 与 FIFO 数量策略测试。"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from gribuki_trade.storage.live_records.live_record_protection_policy import (
    SellLot,
    SellLotAllocation,
    SellLotAllocationError,
    allocate_sell_lots,
    sellable_quantity_for_t1,
)

_ACQUIRED = datetime(2026, 8, 14, 7, 30, tzinfo=UTC)


def test_t1_policy_blocks_same_shanghai_trade_date() -> None:
    assert (
        sellable_quantity_for_t1(
            _ACQUIRED,
            100,
            as_of=datetime(2026, 8, 14, 15, 0, tzinfo=UTC),
        )
        == 0
    )
    assert (
        sellable_quantity_for_t1(
            _ACQUIRED,
            100,
            as_of=datetime(2026, 8, 15, 1, 0, tzinfo=UTC),
        )
        == 100
    )


def test_fifo_policy_allocates_across_lots_and_marks_closed_lots() -> None:
    allocations = allocate_sell_lots(
        120,
        (
            SellLot("protection-a", "buy-a", 50),
            SellLot("protection-b", "buy-b", 100),
        ),
    )

    assert allocations == (
        # 第一批次完全耗尽，第二批次保留 30 股。
        SellLotAllocation("protection-a", "buy-a", 50, True),
        SellLotAllocation("protection-b", "buy-b", 70, False),
    )


def test_fifo_policy_fails_closed_when_lots_cannot_cover_sell() -> None:
    with pytest.raises(SellLotAllocationError, match="exceeds available"):
        allocate_sell_lots(101, (SellLot("protection-a", "buy-a", 100),))


@pytest.mark.parametrize("quantity", [0, -1, True])
def test_fifo_policy_rejects_non_positive_sell_quantity(quantity: object) -> None:
    with pytest.raises(SellLotAllocationError, match="positive integer"):
        allocate_sell_lots(quantity, ())  # type: ignore[arg-type]


def test_t1_policy_rejects_negative_remaining_quantity() -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        sellable_quantity_for_t1(_ACQUIRED, -1, as_of=_ACQUIRED)
