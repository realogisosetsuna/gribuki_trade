from datetime import UTC, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from gribuki_trade.ports.cross_market import (
    CrossMarketMissingItem,
    CrossMarketQuote,
    CrossMarketSegment,
    CrossMarketSnapshot,
)
from gribuki_trade.services.research.cross_market_evidence import (
    build_cross_market_evidence,
    format_cross_market_report_lines,
)

FETCHED_AT = datetime(2026, 8, 13, 8, 1, 2, tzinfo=UTC)


def _quote(
    instrument_id: str = "CSI_300",
    *,
    display_name: str = "沪深300指数（沪深大盘宽基）",
    segment: CrossMarketSegment = CrossMarketSegment.A_SHARE,
    last: str = "4729.1235",
    change_percent: str = "-0.4567",
    local_quote_time: datetime | None = None,
    local_timezone: str = "Asia/Shanghai",
    stale: bool = False,
    degraded: bool = False,
) -> CrossMarketQuote:
    return CrossMarketQuote(
        instrument_id=instrument_id,
        display_name=display_name,
        segment=segment,
        provider_code="000300",
        provider_name="沪深300",
        last=Decimal(last),
        change_percent=Decimal(change_percent),
        change_amount=Decimal("-21.6996"),
        open=Decimal("4750.1"),
        high=Decimal("4770.2222"),
        low=Decimal("4700.3333"),
        previous_close=Decimal("4750.8231"),
        amplitude_percent=Decimal("1.4734"),
        local_quote_time=local_quote_time
        or datetime(2026, 8, 13, 15, 0, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
        local_timezone=local_timezone,
        fetched_at=FETCHED_AT,
        provider="AKShare/Eastmoney index_global_spot_em",
        stale=stale,
        degraded=degraded,
        warnings=("single provider observation",),
    )


def _missing() -> CrossMarketMissingItem:
    return CrossMarketMissingItem(
        instrument_id="CBOE_VIX",
        display_name="CBOE波动率指数（美股隐含波动率）",
        segment=CrossMarketSegment.DOLLAR_COMMODITY_RISK,
        expected_codes=("VIX",),
        expected_names=("CBOE波动率指数", "VIX恐慌指数"),
        reason="configured exact aliases were absent from provider snapshot",
    )


def _snapshot(
    *,
    quotes: tuple[CrossMarketQuote, ...] | None = None,
    missing: tuple[CrossMarketMissingItem, ...] = (),
    degraded: bool = False,
) -> CrossMarketSnapshot:
    return CrossMarketSnapshot(
        fetched_at=FETCHED_AT,
        provider="AKShare/Eastmoney index_global_spot_em",
        quotes=quotes if quotes is not None else (_quote(),),
        missing=missing,
        degraded=degraded,
        warnings=("single provider table", "observational snapshot"),
    )


def test_conversion_is_deterministic_and_references_align() -> None:
    snapshot = _snapshot(missing=(_missing(),), degraded=True)

    first = build_cross_market_evidence(snapshot)
    second = build_cross_market_evidence(snapshot)

    assert first == second
    assert tuple(item.evidence_id for item in first.items) == tuple(
        reference.evidence_id for reference in first.references
    )
    assert len(first.items) == 2
    assert first.items[-1].evidence_id == first.coverage_evidence_id
    assert len(first.quote_evidence_ids) == 1
    assert all(len(item.evidence_id) == 64 for item in first.items)
    assert all(len(item.content_hash) == 64 for item in first.items)


def test_semantically_reordered_snapshot_has_stable_output() -> None:
    us_quote = _quote(
        "S_AND_P_500",
        display_name="标普500指数（美国大盘）",
        segment=CrossMarketSegment.UNITED_STATES,
    )
    a_quote = _quote()
    first = _snapshot(quotes=(us_quote, a_quote), missing=(_missing(),), degraded=True)
    second = _snapshot(quotes=(a_quote, us_quote), missing=(_missing(),), degraded=True)

    first_bundle = build_cross_market_evidence(first)
    second_bundle = build_cross_market_evidence(second)

    assert first_bundle == second_bundle
    assert "A股" in first_bundle.items[0].title
    assert "美股" in first_bundle.items[1].title


def test_quote_evidence_retains_times_provider_and_quality_flags() -> None:
    quote = _quote(stale=True, degraded=True)

    item = build_cross_market_evidence(
        _snapshot(quotes=(quote,), degraded=True)
    ).items[0]

    assert "行情时间=2026-08-13T15:00:01+08:00 [Asia/Shanghai]" in item.excerpt
    assert "采集时间=2026-08-13T08:01:02+00:00" in item.excerpt
    assert "来源=AKShare/Eastmoney index_global_spot_em" in item.excerpt
    assert "陈旧=是" in item.excerpt
    assert "降级=是" in item.excerpt
    assert item.published_at == datetime(2026, 8, 13, 7, 0, 1, tzinfo=UTC)
    assert item.first_seen_at == FETCHED_AT


def test_quote_revision_changes_content_hash_and_evidence_id() -> None:
    first = build_cross_market_evidence(_snapshot(quotes=(_quote(last="4729.1"),)))
    second = build_cross_market_evidence(_snapshot(quotes=(_quote(last="4729.2"),)))

    assert first.items[0].content_hash != second.items[0].content_hash
    assert first.items[0].evidence_id != second.items[0].evidence_id
    assert first.coverage_evidence_id != second.coverage_evidence_id


def test_decimal_scale_does_not_change_content_identity() -> None:
    first = build_cross_market_evidence(_snapshot(quotes=(_quote(last="4729.100"),)))
    second = build_cross_market_evidence(_snapshot(quotes=(_quote(last="4729.1"),)))

    assert first.items[0].content_hash == second.items[0].content_hash
    assert first.items[0].evidence_id == second.items[0].evidence_id


def test_missing_coverage_is_explicit_in_evidence_and_report() -> None:
    bundle = build_cross_market_evidence(
        _snapshot(missing=(_missing(),), degraded=True)
    )
    coverage = bundle.items[-1]
    report = "\n".join(bundle.report_lines)

    assert "缺失=1项" in coverage.excerpt
    assert "CBOE波动率指数" in coverage.excerpt
    assert "预期代码=VIX" in coverage.excerpt
    assert "未填入替代值" in coverage.excerpt
    assert "CBOE波动率指数" in report
    assert "本次来源未返回" in report
    assert "缺失覆盖：1 项" in report


def test_complete_coverage_is_also_explicit() -> None:
    bundle = build_cross_market_evidence(_snapshot())

    assert "缺失=0项" in bundle.items[-1].excerpt
    assert "缺失明细=无" in bundle.items[-1].excerpt
    assert bundle.report_lines[-1] == "缺失覆盖：无；本次配置的观察项均有返回记录。"


def test_report_uses_three_decimals_and_observational_language() -> None:
    lines = format_cross_market_report_lines(_snapshot())
    report = "\n".join(lines)

    assert "4729.124（-0.457%）" in report
    assert "同一采集批次的市场观测" in report
    assert "不解释市场之间的相互关系" in report
    for prohibited in ("导致", "驱动", "传导", "利好", "利空", "所以"):
        assert prohibited not in report


def test_positive_change_has_explicit_plus_sign() -> None:
    report = "\n".join(
        format_cross_market_report_lines(
            _snapshot(quotes=(_quote(change_percent="0.1235"),))
        )
    )

    assert "（+0.124%）" in report


def test_slightly_future_provider_time_is_preserved_but_chronology_is_valid() -> None:
    future_local_time = datetime(
        2026,
        8,
        13,
        16,
        3,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    quote = _quote(
        local_quote_time=future_local_time,
        degraded=True,
    )

    item = build_cross_market_evidence(
        _snapshot(quotes=(quote,), degraded=True)
    ).items[0]

    assert "2026-08-13T16:03:00+08:00 [Asia/Shanghai]" in item.excerpt
    assert item.published_at == FETCHED_AT
    assert item.first_seen_at == FETCHED_AT


def test_missing_only_snapshot_still_produces_coverage_evidence() -> None:
    bundle = build_cross_market_evidence(
        _snapshot(quotes=(), missing=(_missing(),), degraded=True)
    )

    assert bundle.quote_evidence_ids == ()
    assert len(bundle.items) == 1
    assert bundle.items[0].evidence_id == bundle.coverage_evidence_id
    assert "已返回=0项" in bundle.items[0].excerpt
