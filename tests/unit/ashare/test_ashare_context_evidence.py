from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.ports.ashare_context import (
    AShareContextMeta,
    ETFContextSnapshot,
    GovernmentBondYieldCurve,
    GovernmentBondYieldPoint,
    IFContractDailyObservation,
    IFDailyContextSnapshot,
    LiquidityContextSnapshot,
    RepoFixingFamily,
    RepoFixingObservation,
)
from gribuki_trade.ports.cross_market import (
    CrossMarketQuote,
    CrossMarketSegment,
    CrossMarketSnapshot,
)
from gribuki_trade.services.ashare.evidence.ashare_context_evidence import (
    build_ashare_context_evidence,
)

SEEN = datetime(2026, 8, 13, 8, 30, tzinfo=UTC)
AS_OF = datetime(2026, 8, 13, 9, 0, tzinfo=UTC)


def _meta(source: str, url: str = "https://example.test/source") -> AShareContextMeta:
    return AShareContextMeta(
        source_id=source,
        source_url=url,
        observed_at=SEEN,
        available_at=SEEN,
        fetched_at=SEEN,
        stale=False,
        degraded=False,
    )


def _etf(*, iopv: Decimal | None = Decimal("4.7266")) -> ETFContextSnapshot:
    return ETFContextSnapshot(
        symbol="510300.SH",
        name="沪深300ETF华泰柏瑞",
        data_date=date(2026, 8, 13),
        last=Decimal("4.729"),
        iopv=iopv,
        discount_rate_percent=Decimal("-0.05"),
        turnover_percent=Decimal("3.69"),
        shares_outstanding=Decimal("24824287744"),
        amount_cny=Decimal("4364799044"),
        main_net_inflow_cny=Decimal("1194412192"),
        main_net_inflow_percent=Decimal("27.36"),
        bid1=Decimal("4.729"),
        ask1=Decimal("4.730"),
        meta=_meta("AKShare/Eastmoney ETF", "https://quote.eastmoney.com/fund.html"),
    )


def _fixing(family: RepoFixingFamily) -> RepoFixingObservation:
    return RepoFixingObservation(
        family=family,
        session_date=date(2026, 8, 13),
        overnight_percent=Decimal("1.36"),
        seven_day_percent=Decimal("1.40"),
        fourteen_day_percent=Decimal("1.39"),
        meta=_meta(f"ChinaMoney/{family.value}"),
    )


def _curve() -> GovernmentBondYieldCurve:
    return GovernmentBondYieldCurve(
        curve_name="中债国债收益率曲线",
        session_date=date(2026, 8, 13),
        points=(
            GovernmentBondYieldPoint(Decimal("1"), Decimal("1.2079")),
            GovernmentBondYieldPoint(Decimal("10"), Decimal("1.7032")),
            GovernmentBondYieldPoint(Decimal("30"), Decimal("2.1598")),
        ),
        meta=_meta("AKShare/ChinaBond", "https://yield.chinabond.com.cn/"),
    )


def _liquidity(*, reverse: bool = False) -> LiquidityContextSnapshot:
    fixings = (_fixing(RepoFixingFamily.FR), _fixing(RepoFixingFamily.FDR))
    return LiquidityContextSnapshot(
        fetched_at=SEEN,
        cutoff_date=date(2026, 8, 13),
        repo_fixings=tuple(reversed(fixings)) if reverse else fixings,
        government_curve=_curve(),
        missing=(),
        degraded=False,
    )


def _contract() -> IFContractDailyObservation:
    return IFContractDailyObservation(
        symbol="IF2608",
        session_date=date(2026, 8, 13),
        open=Decimal("4688"),
        high=Decimal("4712.4"),
        low=Decimal("4620"),
        close=Decimal("4650"),
        settle=Decimal("4667.6"),
        previous_settle=Decimal("4674.2"),
        volume=19320,
        open_interest=34433,
        turnover_reported=Decimal("2717708.742"),
        meta=_meta("AKShare/CFFEX", "https://www.cffex.com.cn/"),
    )


def _if_context() -> IFDailyContextSnapshot:
    return IFDailyContextSnapshot(
        session_date=date(2026, 8, 13),
        fetched_at=SEEN,
        contracts=(_contract(),),
        degraded=False,
    )


def _cross_market(*, session: date = date(2026, 8, 13)) -> CrossMarketSnapshot:
    local_time = datetime.combine(
        session,
        datetime.min.time().replace(hour=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    quote = CrossMarketQuote(
        instrument_id="CSI_300",
        display_name="沪深300指数",
        segment=CrossMarketSegment.A_SHARE,
        provider_code="000300",
        provider_name="沪深300",
        last=Decimal("4700"),
        change_percent=Decimal("-0.5"),
        change_amount=None,
        open=None,
        high=None,
        low=None,
        previous_close=None,
        amplitude_percent=None,
        local_quote_time=local_time,
        local_timezone="Asia/Shanghai",
        fetched_at=SEEN,
        provider="AKShare/Eastmoney",
        stale=False,
        degraded=False,
    )
    return CrossMarketSnapshot(
        fetched_at=SEEN,
        provider="AKShare/Eastmoney",
        quotes=(quote,),
        missing=(),
        degraded=False,
    )


def test_full_conversion_is_stable_aligned_and_human_readable() -> None:
    first = build_ashare_context_evidence(
        _etf(), _liquidity(), _if_context(), _cross_market(), AS_OF
    )
    second = build_ashare_context_evidence(
        _etf(), _liquidity(reverse=True), _if_context(), _cross_market(), AS_OF
    )

    assert first == second
    assert tuple(item.evidence_id for item in first.items) == tuple(
        item.evidence_id for item in first.references
    )
    assert all(len(item.evidence_id) == 64 for item in first.items)
    text = "\n".join(first.report_lines)
    assert "最新价=4.729，IOPV=4.727" in text
    assert "换手率=3.690%" in text
    assert "买一=4.729，卖一=4.730" in text
    assert "供应商订单规模分类“主力”" in text
    assert "该标签不是参与者身份" in text
    assert "FR007=1.400%" in text
    assert "FDR007=1.400%" in text
    assert "这是定盘利率，绝不是DR007" in text
    assert "1年=1.208%" in text
    assert "10年-1年=0.495个百分点" in text
    assert "30年-10年=0.457个百分点" in text
    assert "基差率=-1.064%" in text
    assert "未年化" in text
    assert "来源：" in text and "链接见文末证据索引" in text and "证据引用：" in text


def test_if_basis_requires_same_session_csi_300_spot() -> None:
    bundle = build_ashare_context_evidence(
        None,
        None,
        _if_context(),
        _cross_market(session=date(2026, 8, 12)),
        AS_OF,
    )

    text = "\n".join(bundle.report_lines)
    assert "IF收盘基差：不可计算" in text
    assert "未年化、未填值" in text
    assert "-1.064" not in text


def test_optional_and_source_gaps_remain_explicitly_missing() -> None:
    bundle = build_ashare_context_evidence(_etf(iopv=None), None, None, None, AS_OF)
    text = "\n".join(bundle.report_lines)

    assert "IOPV=缺失" in text
    assert "FR、FDR与中债国债收益率曲线均未填值" in text
    assert "收盘基差及基差率不可计算" in text


def test_first_seen_after_as_of_is_rejected() -> None:
    with pytest.raises(ValueError, match="first-seen"):
        build_ashare_context_evidence(_etf(), None, None, None, SEEN.replace(hour=8, minute=29))


def test_non_etf_instrument_marks_snapshot_not_applicable() -> None:
    bundle = build_ashare_context_evidence(
        None,
        None,
        None,
        None,
        AS_OF,
        etf_expected=False,
    )

    text = "\n".join(bundle.report_lines)
    assert "非ETF标的，不适用ETF快照" in text
    assert "ETF上下文：缺失" not in text
