from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.ports.ashare_derivatives import (
    SSEETFShareObservation,
    SSEOfficialSourceMeta,
    SSEOptionRiskContract,
    SSEOptionRiskSnapshot,
)
from gribuki_trade.services.ashare_derivatives_evidence import (
    build_ashare_derivatives_evidence,
)

FETCHED_AT = datetime(2026, 8, 14, 0, 0, tzinfo=UTC)
AS_OF = FETCHED_AT + timedelta(minutes=1)


def _meta(
    source_id: str,
    *,
    requested: date = date(2026, 8, 14),
    observed: date = date(2026, 8, 13),
    fetched_at: datetime = FETCHED_AT,
) -> SSEOfficialSourceMeta:
    exact = requested == observed
    return SSEOfficialSourceMeta(
        source_id=source_id,
        source_url=f"https://query.sse.com.cn/{source_id}",
        requested_date=requested,
        observed_date=observed,
        available_at=fetched_at,
        fetched_at=fetched_at,
        exact_date_match=exact,
        latest_available_fallback=not exact,
        content_sha256=("a" if source_id == "SSE_ETF_SHARES" else "b") * 64,
        warnings=("frozen official fixture",),
    )


def _etf_shares(*, fetched_at: datetime = FETCHED_AT) -> SSEETFShareObservation:
    return SSEETFShareObservation(
        symbol="510300",
        name="300ETF",
        expanded_name="沪深300ETF华泰柏瑞",
        etf_type="跨市",
        total_shares_ten_thousands=Decimal("2416818.77"),
        total_shares=Decimal("24168187700.00"),
        raw_total_shares="2416818.77",
        meta=_meta("SSE_ETF_SHARES", fetched_at=fetched_at),
    )


def _contract(
    *,
    security_id: str,
    contract_id: str,
    contract_symbol: str,
    contract_type: str,
    delta: str,
    implied_volatility: str,
) -> SSEOptionRiskContract:
    return SSEOptionRiskContract(
        security_id=security_id,
        contract_id=contract_id,
        contract_symbol=contract_symbol,
        contract_type=contract_type,
        delta=Decimal(delta),
        theta=Decimal("-0.747"),
        gamma=Decimal("3.095"),
        vega=Decimal("0.342"),
        rho=Decimal("0.101") if contract_type == "认购" else Decimal("-0.064"),
        implied_volatility=Decimal(implied_volatility),
        raw_fields=(
            ("SECURITY_ID", security_id),
            ("CONTRACT_ID", contract_id),
            ("IMPLC_VOLATLTY", implied_volatility),
        ),
    )


def _option_risk(
    *,
    reverse: bool = False,
    fetched_at: datetime = FETCHED_AT,
) -> SSEOptionRiskSnapshot:
    contracts = (
        _contract(
            security_id="10011870",
            contract_id="510300C2608M04700",
            contract_symbol="300ETF购8月4700",
            contract_type="认购",
            delta="0.613",
            implied_volatility="0.139",
        ),
        _contract(
            security_id="10011890",
            contract_id="510300P2608M04700",
            contract_symbol="300ETF沽8月4700",
            contract_type="认沽",
            delta="-0.387",
            implied_volatility="0.000",
        ),
    )
    return SSEOptionRiskSnapshot(
        underlying_symbol="510300",
        underlying_name="300ETF",
        contracts=tuple(reversed(contracts)) if reverse else contracts,
        meta=_meta("SSE_OPTION_RISK", fetched_at=fetched_at),
    )


def test_conversion_is_stable_aligned_and_content_addressed() -> None:
    first = build_ashare_derivatives_evidence(
        _etf_shares(),
        _option_risk(),
        as_of=AS_OF,
    )
    reordered = build_ashare_derivatives_evidence(
        _etf_shares(),
        _option_risk(reverse=True),
        as_of=AS_OF,
    )

    assert first == reordered
    assert tuple(item.evidence_id for item in first.items) == tuple(
        reference.evidence_id for reference in first.references
    )
    assert len(first.items) == 2
    assert all(len(item.evidence_id) == 64 for item in first.items)
    assert all(len(item.content_hash) == 64 for item in first.items)
    assert all(item.source_tier == 1 for item in first.items)


def test_etf_share_report_preserves_dates_units_and_semantic_boundaries() -> None:
    bundle = build_ashare_derivatives_evidence(_etf_shares(), None, as_of=AS_OF)
    text = "\n".join(bundle.report_lines)

    assert "请求日=2026-08-14" in text
    assert "实际统计日=2026-08-13" in text
    assert "日期状态=最新可用日期回退" in text
    assert "官方总份额=24,168,187,700.000份" in text
    assert "原始口径=2,416,818.770万份" in text
    assert "不是NAV、AUM" in text
    assert "不是当日申购赎回净流量" in text
    assert "单日份额水平不能推导资金流向" in text
    assert "raw_total_shares" not in text
    assert "上交所期权风险参数：缺失" in text


def test_option_report_counts_and_retains_official_zero_without_fake_average() -> None:
    bundle = build_ashare_derivatives_evidence(None, _option_risk(), as_of=AS_OF)
    text = "\n".join(bundle.report_lines)
    excerpt = bundle.items[0].excerpt

    assert "总合约=2，认购=1，认沽=1" in text
    assert "正值=1，官方零值=1" in text
    assert "零值保留官方原值，未删除、改写或填充" in text
    assert "不派生ATM IV" in text
    assert "波动率偏度（skew）" in text
    assert "期限结构（term structure）" in text
    assert "不将全部合约简单平均" in text
    assert "正隐含波动率=1，官方零隐含波动率=1" in excerpt
    assert "上交所ETF官方份额：缺失" in text


def test_exact_date_status_is_not_masqueraded_as_fallback() -> None:
    value = _etf_shares()
    exact_meta = _meta(
        "SSE_ETF_SHARES",
        requested=date(2026, 8, 13),
        observed=date(2026, 8, 13),
    )
    exact = SSEETFShareObservation(
        symbol=value.symbol,
        name=value.name,
        expanded_name=value.expanded_name,
        etf_type=value.etf_type,
        total_shares_ten_thousands=value.total_shares_ten_thousands,
        total_shares=value.total_shares,
        raw_total_shares=value.raw_total_shares,
        meta=exact_meta,
    )

    bundle = build_ashare_derivatives_evidence(exact, None, as_of=AS_OF)

    assert "日期状态=精确日期匹配" in "\n".join(bundle.report_lines)


@pytest.mark.parametrize("kind", ["etf", "option"])
def test_fetched_after_as_of_is_rejected(kind: str) -> None:
    late = AS_OF + timedelta(seconds=1)
    etf = _etf_shares(fetched_at=late) if kind == "etf" else None
    option = _option_risk(fetched_at=late) if kind == "option" else None

    with pytest.raises(ValueError, match="fetched_at must not be after as_of"):
        build_ashare_derivatives_evidence(etf, option, as_of=AS_OF)


def test_missing_inputs_remain_missing_and_naive_cutoff_fails_closed() -> None:
    bundle = build_ashare_derivatives_evidence(None, None, as_of=AS_OF)

    assert bundle.items == ()
    assert bundle.references == ()
    assert bundle.report_lines == (
        "上交所ETF官方份额：缺失；未使用NAV、AUM、成交额或估算值替代。",
        "上交所期权风险参数：缺失；未构造合约统计、隐含波动率或替代指标。",
    )
    with pytest.raises(ValueError, match="as_of must be timezone-aware"):
        build_ashare_derivatives_evidence(None, None, as_of=datetime(2026, 8, 14))
