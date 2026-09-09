from __future__ import annotations

import re
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.features import (
    CrossMarketRelationFailureReason,
    CrossMarketRiskDirection,
)
from gribuki_trade.ports.cross_market_history import (
    CrossMarketHistoryFailureCode,
    CrossMarketHistoryMissingSeries,
    CrossMarketHistoryObservation,
    CrossMarketHistorySeries,
    CrossMarketHistorySnapshot,
)
from gribuki_trade.services.research.cross_market_relation_evidence import (
    build_cross_market_relation_evidence,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
NEW_YORK = ZoneInfo("America/New_York")
HONG_KONG = ZoneInfo("Asia/Hong_Kong")


def _business_dates(start: date, count: int) -> tuple[date, ...]:
    output: list[date] = []
    current = start
    while len(output) < count:
        if current.weekday() < 5:
            output.append(current)
        current += timedelta(days=1)
    return tuple(output)


def _returns(count: int) -> tuple[Decimal, ...]:
    state = 716_293_355
    output: list[Decimal] = []
    for _ in range(count):
        state = (1_103_515_245 * state + 12_345) % (2**31)
        output.append((Decimal(state) / Decimal(2**31) - Decimal("0.5")) / 40)
    return tuple(output)


def _closes(first: Decimal, returns: tuple[Decimal, ...]) -> tuple[Decimal, ...]:
    output = [first]
    for value in returns:
        output.append(output[-1] * (Decimal("1") + value))
    return tuple(output)


def _target_bars(
    dates: tuple[date, ...],
    *,
    closes: tuple[Decimal, ...] | None = None,
    symbol: str = "510300.SH",
) -> tuple[DailyBar, ...]:
    values = closes or _closes(Decimal("4"), _returns(len(dates) - 1))
    assert len(values) == len(dates)
    return tuple(
        DailyBar(
            symbol=symbol,
            trade_date=session_date,
            open=value,
            high=value,
            low=value,
            close=value,
            previous_close=None if index == 0 else values[index - 1],
            volume=1_000_000,
            amount=Decimal("10000000"),
            turnover_percent=Decimal("0.5"),
            is_trading=True,
            is_st=False,
            adjustment=PriceAdjustment.NONE,
        )
        for index, (session_date, value) in enumerate(
            zip(dates, values, strict=True)
        )
    )


def _series(
    market_id: str,
    display_name: str,
    dates: tuple[date, ...],
    *,
    closes: tuple[Decimal, ...] | None = None,
    local_zone: ZoneInfo = SHANGHAI,
    close_time: time = time(15),
    source: str = "AKShare/Sina stock_zh_index_daily(sh000300)",
) -> CrossMarketHistorySeries:
    values = closes or _closes(Decimal("4000"), _returns(len(dates) - 1))
    return CrossMarketHistorySeries(
        market_id=market_id,
        display_name=display_name,
        observations=tuple(
            CrossMarketHistoryObservation(
                market_id=market_id,
                session_date=session_date,
                close=value,
                available_at=datetime.combine(
                    session_date,
                    close_time,
                    tzinfo=local_zone,
                ),
                source=source,
                local_timezone=local_zone.key,
            )
            for session_date, value in zip(dates, values, strict=True)
        ),
    )


def _snapshot(
    *,
    as_of: datetime,
    series: tuple[CrossMarketHistorySeries, ...] = (),
    missing: tuple[CrossMarketHistoryMissingSeries, ...] = (),
    fetched_at: datetime | None = None,
) -> CrossMarketHistorySnapshot:
    return CrossMarketHistorySnapshot(
        as_of=as_of,
        fetched_at=fetched_at or as_of - timedelta(minutes=1),
        minimum_observations=130,
        series=series,
        missing=missing,
        degraded=bool(missing),
        warnings=(),
    )


def _late_as_of(dates: tuple[date, ...]) -> datetime:
    return datetime.combine(dates[-1] + timedelta(days=2), time(23), tzinfo=SHANGHAI)


def test_basic_conversion_emits_one_factor_item_and_aligned_reference() -> None:
    dates = _business_dates(date(2025, 1, 2), 150)
    bars = _target_bars(dates)
    factor = _series("CSI_300", "沪深300指数", dates[-130:])
    as_of = _late_as_of(dates)

    bundle = build_cross_market_relation_evidence(
        bars,
        _snapshot(as_of=as_of, series=(factor,)),
        as_of,
    )

    assert bundle.report.target_symbol == "510300.SH"
    assert len(bundle.report.factors) == 1
    assert len(bundle.items) == len(bundle.references) == 1
    assert bundle.items[0].evidence_id == bundle.references[0].evidence_id
    assert bundle.items[0].title.endswith("沪深300指数")
    assert bundle.report.factors[0].risk_direction is (
        CrossMarketRiskDirection.POSITIVE_IS_RISK_ON
    )


def test_us_close_return_is_assigned_to_next_ashare_decision() -> None:
    dates = _business_dates(date(2025, 1, 2), 155)
    factor_dates = dates[-130:]
    factor = _series(
        "S_AND_P_500",
        "标普500指数",
        factor_dates,
        local_zone=NEW_YORK,
        close_time=time(16),
        source="AKShare/Sina index_us_stock_sina(.INX)",
    )
    as_of = _late_as_of(dates)

    relation = build_cross_market_relation_evidence(
        _target_bars(dates),
        _snapshot(as_of=as_of, series=(factor,)),
        as_of,
    ).report.factors[0].lag_0

    expected_first = dates[dates.index(factor_dates[1]) + 1]
    assert relation.first_common_date == expected_first
    assert relation.common_samples == 128


def test_hong_kong_close_auction_return_is_assigned_to_next_decision() -> None:
    dates = _business_dates(date(2025, 1, 2), 155)
    factor_dates = dates[-130:]
    factor = _series(
        "HANG_SENG",
        "恒生指数",
        factor_dates,
        local_zone=HONG_KONG,
        close_time=time(16, 10),
        source="AKShare/Sina stock_hk_index_daily_sina(HSI)",
    )
    as_of = _late_as_of(dates)

    relation = build_cross_market_relation_evidence(
        _target_bars(dates),
        _snapshot(as_of=as_of, series=(factor,)),
        as_of,
    ).report.factors[0].lag_0

    assert relation.first_common_date == dates[dates.index(factor_dates[1]) + 1]
    assert relation.common_samples == 128


def test_mainland_1500_return_is_available_for_same_day_decision() -> None:
    dates = _business_dates(date(2025, 1, 2), 150)
    factor_dates = dates[-130:]
    factor = _series("CSI_300", "沪深300指数", factor_dates)
    as_of = _late_as_of(dates)

    relation = build_cross_market_relation_evidence(
        _target_bars(dates),
        _snapshot(as_of=as_of, series=(factor,)),
        as_of,
    ).report.factors[0].lag_0

    assert relation.first_common_date == factor_dates[1]
    assert relation.common_samples == 129


def test_external_holidays_do_not_reuse_the_last_return() -> None:
    dates = _business_dates(date(2024, 1, 2), 260)
    sparse_dates = dates[::2]
    assert len(sparse_dates) == 130
    factor = _series("CSI_300", "沪深300指数", sparse_dates)
    as_of = _late_as_of(dates)

    relation = build_cross_market_relation_evidence(
        _target_bars(dates),
        _snapshot(as_of=as_of, series=(factor,)),
        as_of,
    ).report.factors[0].lag_0

    assert relation.common_samples == 129
    assert relation.coverage == pytest.approx(129 / 259)


def test_returns_accrued_during_ashare_holidays_are_compounded_once() -> None:
    external_dates = _business_dates(date(2023, 1, 2), 650)
    external_returns = _returns(len(external_dates) - 1)
    external_closes = _closes(Decimal("1000"), external_returns)
    target_indexes = tuple(range(4, len(external_dates), 5))
    target_dates = tuple(external_dates[index] for index in target_indexes)
    target_closes = tuple(external_closes[index] for index in target_indexes)
    factor = _series(
        "CSI_300",
        "沪深300指数",
        external_dates,
        closes=external_closes,
    )
    as_of = _late_as_of(target_dates)

    relation = build_cross_market_relation_evidence(
        _target_bars(target_dates, closes=target_closes),
        _snapshot(as_of=as_of, series=(factor,)),
        as_of,
    ).report.factors[0].lag_0

    assert relation.common_samples == 129
    assert relation.correlation_120 is not None
    assert relation.correlation_120 > 0.999999


def test_as_of_filters_future_target_and_factor_observations() -> None:
    dates = _business_dates(date(2025, 1, 2), 150)
    cutoff_index = 139
    cutoff = datetime.combine(dates[cutoff_index], time(15, 5), tzinfo=SHANGHAI)
    history_as_of = _late_as_of(dates)
    factor = _series("CSI_300", "沪深300指数", dates[-130:])
    snapshot = _snapshot(
        as_of=history_as_of,
        series=(factor,),
        fetched_at=cutoff - timedelta(minutes=1),
    )

    bundle = build_cross_market_relation_evidence(_target_bars(dates), snapshot, cutoff)

    assert bundle.report.as_of_trade_date == dates[cutoff_index]
    assert bundle.report.target_close_samples == cutoff_index + 1
    assert bundle.report.factors[0].lag_0.last_common_date == dates[cutoff_index]
    assert all(item.published_at <= cutoff for item in bundle.items)
    assert all(item.first_seen_at <= cutoff for item in bundle.items)


def test_snapshot_fetched_after_as_of_is_rejected_instead_of_backdated() -> None:
    dates = _business_dates(date(2025, 1, 2), 150)
    as_of = _late_as_of(dates)
    snapshot = _snapshot(
        as_of=as_of,
        series=(_series("CSI_300", "沪深300指数", dates[-130:]),),
        fetched_at=as_of + timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="fetched_at"):
        build_cross_market_relation_evidence(_target_bars(dates), snapshot, as_of)


def test_less_than_120_common_returns_reports_stable_failure() -> None:
    target_dates = _business_dates(date(2025, 1, 2), 100)
    factor_dates = _business_dates(date(2024, 6, 3), 130)
    as_of = _late_as_of(target_dates)
    # 确保每个因子观察值在所选分析边界上都有效。
    factor_dates = tuple(item for item in factor_dates if item <= target_dates[-1])
    if len(factor_dates) < 130:
        factor_dates = _business_dates(date(2024, 1, 2), 130)
    factor = _series("CSI_300", "沪深300指数", factor_dates)

    relation = build_cross_market_relation_evidence(
        _target_bars(target_dates),
        _snapshot(as_of=as_of, series=(factor,)),
        as_of,
    ).report.factors[0].lag_0

    assert relation.common_samples < 120
    assert relation.failure_reason is (
        CrossMarketRelationFailureReason.INSUFFICIENT_COMMON_SAMPLES
    )
    assert relation.correlation_120 is None


def test_missing_history_creates_explicit_coverage_evidence() -> None:
    dates = _business_dates(date(2025, 1, 2), 130)
    as_of = _late_as_of(dates)
    missing = CrossMarketHistoryMissingSeries(
        market_id="S_AND_P_500",
        display_name="标普500指数",
        expected_source="AKShare/Sina index_us_stock_sina(.INX)",
        failure_code=CrossMarketHistoryFailureCode.TIMEOUT,
        reason="frozen timeout",
    )

    bundle = build_cross_market_relation_evidence(
        _target_bars(dates),
        _snapshot(as_of=as_of, missing=(missing,)),
        as_of,
    )

    assert bundle.report.factors == ()
    assert len(bundle.items) == 1
    assert "数据缺口" in bundle.items[0].title
    assert "TIMEOUT" in bundle.items[0].excerpt
    assert "未使用代理序列" in bundle.items[0].excerpt
    assert any("历史缺失" in line for line in bundle.report_lines)


def test_content_addressing_is_stable_across_input_order() -> None:
    dates = _business_dates(date(2025, 1, 2), 150)
    as_of = _late_as_of(dates)
    csi = _series("CSI_300", "沪深300指数", dates[-130:])
    us = _series(
        "S_AND_P_500",
        "标普500指数",
        dates[-130:],
        local_zone=NEW_YORK,
        close_time=time(16),
        source="AKShare/Sina index_us_stock_sina(.INX)",
    )
    bars = _target_bars(dates)

    first = build_cross_market_relation_evidence(
        bars,
        _snapshot(as_of=as_of, series=(us, csi)),
        as_of,
    )
    second = build_cross_market_relation_evidence(
        tuple(reversed(bars)),
        _snapshot(as_of=as_of, series=(csi, us)),
        as_of,
    )

    assert tuple(item.evidence_id for item in first.items) == tuple(
        item.evidence_id for item in second.items
    )
    assert first.report_lines == second.report_lines


def test_report_renders_all_relation_numbers_to_exactly_three_decimals() -> None:
    dates = _business_dates(date(2025, 1, 2), 150)
    as_of = _late_as_of(dates)
    factor = _series("CSI_300", "沪深300指数", dates[-130:])

    lines = build_cross_market_relation_evidence(
        _target_bars(dates),
        _snapshot(as_of=as_of, series=(factor,)),
        as_of,
    ).report_lines
    relation_line = next(line for line in lines if line.startswith("- 沪深300指数"))

    assert re.search(r"数据覆盖率 \d+\.\d{3}", relation_line)
    assert re.search(r"近20/60/120日线性相关 -?\d+\.\d{3}", relation_line)
    assert re.search(r"120日敏感度β/t值 -?\d+\.\d{3}", relation_line)
    assert not re.search(r"-?\d+\.\d{4,}", relation_line)


def test_report_uses_chinese_names_and_non_predictive_lag_one_language() -> None:
    dates = _business_dates(date(2025, 1, 2), 150)
    as_of = _late_as_of(dates)
    us = _series(
        "S_AND_P_500",
        "标普500指数",
        dates[-130:],
        local_zone=NEW_YORK,
        close_time=time(16),
        source="AKShare/Sina index_us_stock_sina(.INX)",
    )

    lines = build_cross_market_relation_evidence(
        _target_bars(dates),
        _snapshot(as_of=as_of, series=(us,)),
        as_of,
    ).report_lines
    text = "\n".join(lines)

    assert "标普500指数" in text
    assert "相关不代表因果" in text
    assert "前一A股决策时点已知" in text
    assert "前一决策时点关系" in text
    assert "lag0" not in text
    assert "lag1" not in text
    assert "EWMA" not in text
    assert "不称为预测" in text


def test_suspended_and_missing_close_target_rows_are_excluded() -> None:
    dates = _business_dates(date(2025, 1, 2), 152)
    bars = list(_target_bars(dates))
    bars[10] = replace(bars[10], is_trading=False)
    bars[20] = replace(bars[20], close=None)
    factor = _series("CSI_300", "沪深300指数", dates[-130:])
    as_of = _late_as_of(dates)

    report = build_cross_market_relation_evidence(
        bars,
        _snapshot(as_of=as_of, series=(factor,)),
        as_of,
    ).report

    assert report.target_close_samples == 150


def test_canonical_urls_map_to_official_akshare_documentation() -> None:
    dates = _business_dates(date(2025, 1, 2), 155)
    as_of = _late_as_of(dates)
    csi = _series("CSI_300", "沪深300指数", dates[-130:])
    us = _series(
        "S_AND_P_500",
        "标普500指数",
        dates[-130:],
        local_zone=NEW_YORK,
        close_time=time(16),
        source="AKShare/Sina index_us_stock_sina(.INX)",
    )

    bundle = build_cross_market_relation_evidence(
        _target_bars(dates),
        _snapshot(as_of=as_of, series=(us, csi)),
        as_of,
    )
    urls = {item.title: item.canonical_url for item in bundle.items}

    assert urls["跨市场历史关系｜沪深300指数"].endswith("/data/index/index.html")
    assert urls["跨市场历史关系｜标普500指数"].endswith("/data/index/index.html")
