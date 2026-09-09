from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from gribuki_trade.features.ashare_screening import AShareScreeningConfig
from gribuki_trade.ports.ashare_screening import (
    AShareBoard,
    AShareFactorRecord,
    AShareFactorSnapshot,
    AShareFactorValue,
    AShareUniverseRecord,
    AShareUniverseSnapshot,
    ScreeningFactorId,
    ScreeningHistoryPolicy,
    ScreeningSourceQuality,
)
from gribuki_trade.services.ashare.research.ashare_preopen_screening import (
    ASharePreopenScreeningError,
    ASharePreopenScreeningService,
)

SESSION = date(2026, 8, 13)
REQUESTED = datetime(2026, 8, 14, 0, 0, tzinfo=UTC)
UNIVERSE_AT = datetime(2026, 8, 14, 0, 1, tzinfo=UTC)
FACTOR_AT = datetime(2026, 8, 14, 0, 2, tzinfo=UTC)


class _Source:
    def __init__(self, *, quality: ScreeningSourceQuality) -> None:
        self.quality = quality
        self.factor_known_at: datetime | None = None

    async def fetch_universe_snapshot(
        self, *, as_of: date, known_at: datetime
    ) -> AShareUniverseSnapshot:
        assert as_of == SESSION
        assert known_at == REQUESTED
        return AShareUniverseSnapshot(
            as_of=SESSION,
            available_at=UNIVERSE_AT,
            observed_at=UNIVERSE_AT,
            source_id="preopen-universe",
            source_revision="u1",
            records=tuple(_universe(index) for index in range(3)),
            quality=self.quality,
            warnings=("PREOPEN",),
        )

    async def fetch_factor_snapshot(
        self,
        symbols: tuple[str, ...],
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareFactorSnapshot:
        self.factor_known_at = known_at
        return AShareFactorSnapshot(
            as_of=as_of,
            available_at=FACTOR_AT,
            observed_at=FACTOR_AT,
            source_id="preopen-factors",
            source_revision="f1",
            feature_version="fixture@1",
            history_policy=(
                ScreeningHistoryPolicy.UNADJUSTED_WITH_CORPORATE_ACTION_GUARD
            ),
            records=tuple(_factors(symbol, index) for index, symbol in enumerate(symbols)),
            quality=self.quality,
            warnings=("PREOPEN",),
        )


class _Clock:
    def __init__(self) -> None:
        self.values = iter((UNIVERSE_AT, FACTOR_AT))

    def __call__(self) -> datetime:
        return next(self.values)


def test_preopen_screen_uses_post_fetch_visibility_boundaries() -> None:
    source = _Source(quality=ScreeningSourceQuality.DEGRADED)
    service = ASharePreopenScreeningService(
        source,
        config=_config(),
        clock=_Clock(),
    )

    result = asyncio.run(service.run(as_of=SESSION, requested_at=REQUESTED))

    assert source.factor_known_at == UNIVERSE_AT
    assert result.decision_at == FACTOR_AT
    assert len(result.top_candidates) == 2
    assert {item.symbol for item in result.top_candidates}.issubset(
        {"600000.SH", "600001.SH", "600002.SH"}
    )
    assert "CURRENT_SESSION_CORROBORATION_REQUIRED_BEFORE_PAPER_ENTRY" in result.warnings


def test_preopen_screen_requires_explicit_degraded_source_semantics() -> None:
    service = ASharePreopenScreeningService(
        _Source(quality=ScreeningSourceQuality.COMPLETE),
        config=_config(),
        clock=_Clock(),
    )

    with pytest.raises(ASharePreopenScreeningError) as caught:
        asyncio.run(service.run(as_of=SESSION, requested_at=REQUESTED))

    assert caught.value.code == "UNIVERSE_NOT_MARKED_DEGRADED"


def _config() -> AShareScreeningConfig:
    return AShareScreeningConfig(
        min_listing_days=0,
        min_session_amount_cny=Decimal("0"),
        min_average_amount_20_cny=Decimal("0"),
        min_market_cap_cny=Decimal("0"),
        allowed_boards=(AShareBoard.SSE_MAIN, AShareBoard.SZSE_MAIN),
        min_cross_section_observations=2,
        max_factor_candidates=3,
        top_n=2,
    )


def _universe(index: int) -> AShareUniverseRecord:
    return AShareUniverseRecord(
        symbol=f"60000{index}.SH",
        name=f"fixture-{index}",
        board=AShareBoard.SSE_MAIN,
        industry="fixture",
        listing_days=1000,
        is_tradable=True,
        is_st=False,
        is_suspended=False,
        last_price=Decimal("10"),
        session_amount_cny=Decimal(100_000_000 + index),
        market_cap_cny=Decimal("10000000000"),
    )


def _factors(symbol: str, index: int) -> AShareFactorRecord:
    values = tuple(
        AShareFactorValue(
            factor_id=factor_id,
            value=(
                100_000_000.0
                if factor_id is ScreeningFactorId.AVERAGE_AMOUNT_20_CNY
                else float(index)
            ),
        )
        for factor_id in ScreeningFactorId
    )
    return AShareFactorRecord(symbol=symbol, values=values)
