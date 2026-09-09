from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from gribuki_trade.adapters.ashare_surveillance import (
    TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID,
    TENCENT_SURVEILLANCE_SOURCE_ID,
    AKShareAShareSurveillanceAdapter,
)
from gribuki_trade.features.ashare_surveillance import (
    AShareIntradaySurveillanceConfig,
    IntradayCandidateClass,
    IntradayExclusionReason,
    rank_intraday_anomalies,
)
from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.ports.ashare_surveillance import (
    AShareIntradayUniverseRecord,
    AShareIntradayUniverseSnapshot,
    SurveillanceSourceQuality,
)
from gribuki_trade.services.ashare_surveillance import (
    AShareIntradaySurveillanceService,
    AShareMarketSessionError,
    AShareSurveillanceRunStatus,
)

SESSION = date(2026, 8, 14)
REQUESTED = datetime(2026, 8, 14, 2, 0, tzinfo=UTC)  # 上海时间 10:00


class _Frame:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def to_dict(self, *, orient: str) -> list[dict[str, Any]]:
        assert orient == "records"
        return self._rows


def _record(index: int, **updates: Any) -> AShareIntradayUniverseRecord:
    last = Decimal("10") + Decimal(index) / Decimal("10")
    values: dict[str, Any] = {
        "symbol": f"{index:06d}.SZ",
        "name": f"样本{index}",
        "board": AShareBoard.SZSE_MAIN,
        "is_st": False,
        "is_suspended": False,
        "last_price": last,
        "previous_close": Decimal("10"),
        "open_price": Decimal("10"),
        "high_price": last + Decimal("0.05"),
        "low_price": Decimal("9.5"),
        "change_percent": (last / Decimal("10") - 1) * Decimal("100"),
        "session_amount_cny": Decimal(index) * Decimal("10000000"),
        "turnover_rate_percent": Decimal(index) / Decimal("10"),
        "volume_ratio": Decimal("1") + Decimal(index) / Decimal("20"),
    }
    values.update(updates)
    return AShareIntradayUniverseRecord(**values)


def test_intraday_ranker_surfaces_strong_complete_candidate() -> None:
    ranking = rank_intraday_anomalies(
        tuple(_record(index) for index in range(1, 41)),
        config=AShareIntradaySurveillanceConfig(
            min_cross_section_observations=20,
            top_n=5,
        ),
    )

    assert len(ranking.candidates) == 5
    assert ranking.candidates[0].symbol == "000040.SZ"
    assert ranking.candidates[0].previous_close == Decimal("10")
    assert ranking.candidates[0].candidate_class is IntradayCandidateClass.MOMENTUM_EXPANSION
    assert ranking.candidates[0].factor_weight_coverage == pytest.approx(1.0)
    assert not ranking.globally_unavailable_factors


def test_intraday_ranker_does_not_neutral_fill_unknown_status() -> None:
    records = tuple(
        _record(1, is_suspended=None) if index == 1 else _record(index)
        for index in range(1, 41)
    )
    ranking = rank_intraday_anomalies(
        records,
        config=AShareIntradaySurveillanceConfig(min_cross_section_observations=20),
    )

    excluded = next(item for item in ranking.excluded if item.symbol == "000001.SZ")
    assert IntradayExclusionReason.UNKNOWN_SUSPENSION_STATUS in excluded.reasons


class _TencentFallbackClient:
    def stock_zh_a_spot_em(self) -> object:
        raise RuntimeError("primary unavailable")

    def stock_zh_a_spot_tx(self) -> _Frame:
        return _Frame(
            [
                {
                    "code": f"sz{index:06d}",
                    "name": f"样本{index}",
                    "zxj": 10 + index / 10,
                    "zd": index / 10,
                    "zdf": index,
                    "turnover": index * 1000,
                    "hsl": index / 10,
                    "state": "正常",
                }
                for index in range(1, 41)
            ]
        )


def test_adapter_uses_independent_tencent_fallback_with_explicit_degradation() -> None:
    fetched = datetime(2026, 8, 14, 2, 0, 5, tzinfo=UTC)
    adapter = AKShareAShareSurveillanceAdapter(
        _TencentFallbackClient(), minimum_universe_count=20, now=lambda: fetched
    )

    snapshot = asyncio.run(
        adapter.fetch_intraday_universe(
            session_date=SESSION,
            known_at=REQUESTED,
        )
    )

    assert snapshot.source_id == TENCENT_SURVEILLANCE_SOURCE_ID
    assert snapshot.quality is SurveillanceSourceQuality.DEGRADED
    assert len(snapshot.records) == 40
    assert snapshot.records[0].session_amount_cny == Decimal("10000000")
    assert snapshot.records[0].open_price is None


def _tencent_quote_rows(provider_codes: tuple[str, ...]) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    for provider_code in provider_codes:
        index = int(provider_code[2:])
        last = Decimal("10") + Decimal(index) / Decimal("10")
        rows.append(
            {
                "provider_code": provider_code,
                "name": f"样本{index}",
                "last": last,
                "previous_close": Decimal("10"),
                "open": Decimal("10"),
                "provider_timestamp": "20260814100000",
                "change_percent": Decimal(index),
                "high": last + Decimal("0.05"),
                "low": Decimal("9.5"),
                "amount_cny": Decimal(index) * Decimal("10000000"),
                "turnover_rate": Decimal(index) / Decimal("10"),
                "volume_ratio": Decimal("1") + Decimal(index) / Decimal("20"),
            }
        )
    return tuple(rows)


def test_adapter_strictly_enriches_tencent_board_with_bulk_quotes() -> None:
    fetched = datetime(2026, 8, 14, 2, 0, 5, tzinfo=UTC)
    requested_codes: tuple[str, ...] | None = None

    def quote_fetcher(
        provider_codes: tuple[str, ...],
    ) -> tuple[dict[str, Any], ...]:
        nonlocal requested_codes
        requested_codes = provider_codes
        return _tencent_quote_rows(provider_codes)

    adapter = AKShareAShareSurveillanceAdapter(
        _TencentFallbackClient(),
        minimum_universe_count=20,
        now=lambda: fetched,
        tencent_quote_fetcher=quote_fetcher,
    )

    snapshot = asyncio.run(
        adapter.fetch_intraday_universe(
            session_date=SESSION,
            known_at=REQUESTED,
        )
    )

    assert requested_codes is not None
    assert requested_codes[0] == "sz000001"
    assert snapshot.source_id == TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID
    assert snapshot.quality is SurveillanceSourceQuality.COMPLETE
    assert len(snapshot.records) == 40
    assert snapshot.records[0].open_price == Decimal("10")
    assert snapshot.records[0].high_price == Decimal("10.15")
    assert snapshot.records[0].volume_ratio == Decimal("1.05")
    assert any("strict board/quote inner join" in item for item in snapshot.warnings)


def test_adapter_does_not_label_incomplete_tencent_enrichment_complete() -> None:
    fetched = datetime(2026, 8, 14, 2, 0, 5, tzinfo=UTC)

    def incomplete_quote_fetcher(
        provider_codes: tuple[str, ...],
    ) -> tuple[dict[str, Any], ...]:
        return _tencent_quote_rows(provider_codes[:10])

    adapter = AKShareAShareSurveillanceAdapter(
        _TencentFallbackClient(),
        minimum_universe_count=20,
        now=lambda: fetched,
        tencent_quote_fetcher=incomplete_quote_fetcher,
    )

    snapshot = asyncio.run(
        adapter.fetch_intraday_universe(
            session_date=SESSION,
            known_at=REQUESTED,
        )
    )

    assert snapshot.source_id == TENCENT_SURVEILLANCE_SOURCE_ID
    assert snapshot.quality is SurveillanceSourceQuality.DEGRADED
    assert any(
        item == "PRIOR_SOURCE_FAILED:tencent_bulk_quote:INCOMPLETE_ENRICHED_UNIVERSE"
        for item in snapshot.warnings
    )


class _Source:
    def __init__(self, snapshot: AShareIntradayUniverseSnapshot) -> None:
        self.snapshot = snapshot

    async def fetch_intraday_universe(
        self, *, session_date: date, known_at: datetime
    ) -> AShareIntradayUniverseSnapshot:
        assert session_date == SESSION
        assert known_at == REQUESTED
        return self.snapshot


def test_service_returns_degraded_research_candidates_not_trade_actions() -> None:
    available = REQUESTED + timedelta(seconds=5)
    snapshot = AShareIntradayUniverseSnapshot(
        session_date=SESSION,
        available_at=available,
        observed_at=available,
        source_id="fixture",
        source_revision="revision",
        records=tuple(_record(index) for index in range(1, 41)),
        quality=SurveillanceSourceQuality.DEGRADED,
    )
    service = AShareIntradaySurveillanceService(
        _Source(snapshot),
        config=AShareIntradaySurveillanceConfig(
            min_cross_section_observations=20, top_n=3
        ),
        minimum_universe_count=20,
        clock=lambda: available + timedelta(seconds=1),
    )

    run = asyncio.run(service.run_once(session_date=SESSION, requested_at=REQUESTED))

    assert run.status is AShareSurveillanceRunStatus.DEGRADED
    assert len(run.ranking.candidates) == 3
    assert any("candidate discovery only" in warning for warning in run.warnings)


def test_service_rejects_stale_snapshot() -> None:
    snapshot = AShareIntradayUniverseSnapshot(
        session_date=SESSION,
        available_at=REQUESTED,
        observed_at=REQUESTED,
        source_id="fixture",
        source_revision="revision",
        records=tuple(_record(index) for index in range(1, 41)),
    )
    service = AShareIntradaySurveillanceService(
        _Source(snapshot),
        config=AShareIntradaySurveillanceConfig(min_cross_section_observations=20),
        minimum_universe_count=20,
        clock=lambda: REQUESTED + timedelta(minutes=4),
    )

    with pytest.raises(AShareMarketSessionError, match="STALE_SNAPSHOT"):
        asyncio.run(service.run_once(session_date=SESSION, requested_at=REQUESTED))
