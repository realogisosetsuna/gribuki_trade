from datetime import UTC, date, datetime
from decimal import Decimal

from gribuki_trade.ports.official_rates import (
    FXQuoteConvention,
    InterestRateUnit,
    OfficialRateSourceMeta,
    SafeCentralParityHistory,
    ShiborDailyObservation,
    ShiborHistory,
    ShiborRate,
    ShiborTenor,
    USDCNYCentralParityObservation,
)
from gribuki_trade.services.official_rates_evidence import (
    build_official_rates_evidence,
)


def test_official_rate_evidence_keeps_units_and_human_readable_semantics() -> None:
    as_of = datetime(2026, 8, 13, 5, tzinfo=UTC)
    available = datetime(2026, 8, 13, 1, 15, tzinfo=UTC)
    fetched = datetime(2026, 8, 13, 4, 59, tzinfo=UTC)
    safe_meta = OfficialRateSourceMeta(
        source_id="SAFE_USD_CNY_CENTRAL_PARITY",
        source_url="https://www.safe.gov.cn/AppStructured/hlw/RMBQuery.do",
        available_at=available,
        fetched_at=fetched,
        stale=False,
        content_sha256="a" * 64,
    )
    safe = SafeCentralParityHistory(
        as_of=as_of,
        start_date=date(2026, 8, 13),
        end_date=date(2026, 8, 13),
        observations=(
            USDCNYCentralParityObservation(
                session_date=date(2026, 8, 13),
                base_currency="USD",
                quote_currency="CNY",
                quote_convention=(
                    FXQuoteConvention.QUOTE_CURRENCY_PER_BASE_CURRENCY
                ),
                cny_per_usd=Decimal("6.7888"),
                source_base_amount_usd=Decimal("100"),
                source_quote_amount_cny=Decimal("678.88"),
                observed_at=available,
                available_at=available,
            ),
        ),
        meta=safe_meta,
    )
    shibor_available = datetime(2026, 8, 13, 3, tzinfo=UTC)
    shibor_meta = OfficialRateSourceMeta(
        source_id="OFFICIAL_SHIBOR",
        source_url="https://www.shibor.net.cn/ags/ms/cm-u-bk-shibor/ShiborHis",
        available_at=shibor_available,
        fetched_at=fetched,
        stale=False,
        content_sha256="b" * 64,
    )
    shibor = ShiborHistory(
        as_of=as_of,
        start_date=date(2026, 8, 13),
        end_date=date(2026, 8, 13),
        observations=(
            ShiborDailyObservation(
                session_date=date(2026, 8, 13),
                rates=tuple(
                    ShiborRate(tenor, Decimal("1.5") + Decimal(index) / 100)
                    for index, tenor in enumerate(ShiborTenor)
                ),
                unit=InterestRateUnit.PERCENT_PER_ANNUM,
                day_count="ACT/360",
                settlement="T+0",
                observed_at=shibor_available,
                available_at=shibor_available,
            ),
        ),
        meta=shibor_meta,
    )

    bundle = build_official_rates_evidence(safe, shibor, as_of=as_of)

    assert len(bundle.items) == 2
    assert "1美元=6.7888元人民币" in bundle.items[0].excerpt
    assert "不是在岸即期收盘价" in bundle.items[0].excerpt
    assert "O/N=1.500%" in bundle.items[1].excerpt
    assert "不是回购利率" in bundle.items[1].excerpt
    assert "ACT/360" in bundle.report_lines[1]
