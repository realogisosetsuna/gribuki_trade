import asyncio
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from gribuki_trade.adapters.ashare_context import (
    AKShareETFContextAdapter,
    AKShareIFContextAdapter,
    AKShareLiquidityContextAdapter,
)
from gribuki_trade.ports.ashare_context import (
    AShareContextFailureCode,
    AShareContextNoDataError,
    AShareContextPayloadError,
    AShareContextTimeoutError,
    RepoFixingFamily,
)

NOW = datetime(2026, 8, 13, 8, 30, tzinfo=UTC)
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _etf_row(**updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "代码": "510300",
        "名称": "沪深300ETF华泰柏瑞",
        "最新价": "4.729",
        "IOPV实时估值": "4.7266",
        "基金折价率": "-0.05",
        "换手率": "3.69",
        "最新份额": "24824287744",
        "成交额": "4364799044",
        "主力净流入-净额": "1194412192",
        "主力净流入-净占比": "27.36",
        "买一": "4.729",
        "卖一": "4.730",
        "数据日期": "2026-08-13",
        "更新时间": datetime(2026, 8, 13, 16, 11, 38, tzinfo=SHANGHAI),
    }
    row.update(updates)
    return row


def _repo_frame(prefix: str, value: str = "1.400") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2026, 8, 12),
                f"{prefix}001": "1.350",
                f"{prefix}007": "1.390",
                f"{prefix}014": "1.410",
            },
            {
                "date": date(2026, 8, 13),
                f"{prefix}001": "1.360",
                f"{prefix}007": value,
                f"{prefix}014": "1.390",
            },
        ]
    )


def _curve_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "曲线名称": "中债商业银行普通债收益率曲线(AAA)",
                "日期": date(2026, 8, 13),
                "3月": "1.3914",
                "6月": "1.4425",
                "1年": "1.4850",
                "3年": "1.5517",
                "5年": "1.6106",
                "7年": "1.7899",
                "10年": "1.9602",
                "30年": "2.3325",
            },
            {
                "曲线名称": "中债国债收益率曲线",
                "日期": date(2026, 8, 13),
                "3月": "1.1915",
                "6月": "1.1975",
                "1年": "1.2079",
                "3年": "1.2628",
                "5年": "1.4009",
                "7年": "1.5304",
                "10年": "1.7032",
                "30年": "2.1598",
            },
        ]
    )


def _if_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "IF2609",
                "date": "20260813",
                "open": "4657.8",
                "high": "4681.0",
                "low": "4621.4",
                "close": "4624.0",
                "volume": "59433",
                "open_interest": "149010",
                "turnover": "8305297.848",
                "settle": "4637.0",
                "pre_settle": "4643.0",
                "variety": "IF",
            },
            {
                "symbol": "IH2609",
                "date": "20260813",
                "open": "3200",
                "high": "3210",
                "low": "3190",
                "close": "3205",
                "volume": "1",
                "open_interest": "2",
                "turnover": "3",
                "settle": "3204",
                "pre_settle": "3200",
                "variety": "IH",
            },
            {
                "symbol": "IF2608",
                "date": "20260813",
                "open": "4688.0",
                "high": "4712.4",
                "low": "4652.0",
                "close": "4655.0",
                "volume": "19320",
                "open_interest": "34433",
                "turnover": "2717708.742",
                "settle": "4667.6",
                "pre_settle": "4674.2",
                "variety": "IF",
            },
        ]
    )


def test_etf_context_preserves_units_provenance_and_first_seen_time() -> None:
    calls: list[int] = []

    def endpoint() -> pd.DataFrame:
        calls.append(1)
        return pd.DataFrame([_etf_row(), _etf_row(代码="159915", 名称="创业板ETF")])

    adapter = AKShareETFContextAdapter(
        SimpleNamespace(fund_etf_spot_em=endpoint),
        now=lambda: NOW,
    )

    result = adapter.fetch_etf_context("510300.SH")

    assert calls == [1]
    assert result.symbol == "510300.SH"
    assert result.last == Decimal("4.729")
    assert result.iopv == Decimal("4.7266")
    assert result.discount_rate_percent == Decimal("-0.05")
    assert result.turnover_percent == Decimal("3.69")
    assert result.shares_outstanding == Decimal("24824287744")
    assert result.main_net_inflow_cny == Decimal("1194412192")
    assert result.meta.observed_at.isoformat() == "2026-08-13T16:11:38+08:00"
    assert result.meta.available_at == NOW
    assert result.meta.fetched_at == NOW
    assert result.meta.stale is False
    assert result.meta.degraded is False
    assert "participant identity" in " ".join(result.meta.warnings)


def test_etf_context_missing_optional_metrics_is_explicitly_degraded() -> None:
    row = _etf_row(**{"IOPV实时估值": "--", "最新份额": None})
    client = SimpleNamespace(fund_etf_spot_em=lambda: pd.DataFrame([row]))

    result = AKShareETFContextAdapter(client, now=lambda: NOW).fetch_etf_context("510300")

    assert result.iopv is None
    assert result.shares_outstanding is None
    assert result.meta.degraded is True
    assert "missing optional ETF fields" in " ".join(result.meta.warnings)


def test_etf_context_rejects_duplicate_exact_code() -> None:
    client = SimpleNamespace(fund_etf_spot_em=lambda: pd.DataFrame([_etf_row(), _etf_row()]))

    with pytest.raises(AShareContextPayloadError, match="duplicate"):
        AKShareETFContextAdapter(client, now=lambda: NOW).fetch_etf_context("510300.SH")


def test_etf_context_rejects_future_provider_timestamp() -> None:
    client = SimpleNamespace(
        fund_etf_spot_em=lambda: pd.DataFrame(
            [_etf_row(更新时间=datetime(2026, 8, 13, 17, 0, tzinfo=SHANGHAI))]
        )
    )

    with pytest.raises(AShareContextPayloadError, match="after fetched_at"):
        AKShareETFContextAdapter(client, now=lambda: NOW).fetch_etf_context("510300.SH")


def test_etf_async_timeout_is_typed() -> None:
    def slow() -> pd.DataFrame:
        time.sleep(0.2)
        return pd.DataFrame([_etf_row()])

    adapter = AKShareETFContextAdapter(
        SimpleNamespace(fund_etf_spot_em=slow),
        timeout_seconds=0.02,
        now=lambda: NOW,
    )

    with pytest.raises(AShareContextTimeoutError):
        asyncio.run(adapter.fetch_etf_context_async("510300.SH"))


def test_liquidity_context_collects_fr_fdr_and_exact_government_curve() -> None:
    symbols: list[str] = []

    def repo_rate_query(*, symbol: str) -> pd.DataFrame:
        symbols.append(symbol)
        return _repo_frame("FR", "1.425") if symbol == "回购定盘利率" else _repo_frame("FDR")

    client = SimpleNamespace(
        repo_rate_query=repo_rate_query,
        bond_china_yield=lambda **_: _curve_frame(),
    )
    result = AKShareLiquidityContextAdapter(client, now=lambda: NOW).fetch_liquidity_context(
        start_date=date(2026, 8, 10),
        end_date=date(2026, 8, 13),
    )

    assert symbols == ["回购定盘利率", "银银间回购定盘利率"]
    assert result.degraded is False
    assert result.missing == ()
    by_family = {item.family: item for item in result.repo_fixings}
    assert by_family[RepoFixingFamily.FR].seven_day_percent == Decimal("1.425")
    assert by_family[RepoFixingFamily.FDR].seven_day_percent == Decimal("1.400")
    assert by_family[RepoFixingFamily.FR].meta.observed_at == NOW
    assert "not DR007" in " ".join(by_family[RepoFixingFamily.FR].meta.warnings)
    assert result.government_curve is not None
    curve = result.government_curve
    assert curve.curve_name == "中债国债收益率曲线"
    assert curve.session_date == date(2026, 8, 13)
    assert {point.tenor_years: point.yield_percent for point in curve.points}[
        Decimal("10")
    ] == Decimal("1.7032")


def test_liquidity_source_failure_isolated_with_stable_reason_code() -> None:
    def repo_rate_query(*, symbol: str) -> pd.DataFrame:
        if symbol == "银银间回购定盘利率":
            raise RuntimeError("upstream disconnected")
        return _repo_frame("FR")

    client = SimpleNamespace(
        repo_rate_query=repo_rate_query,
        bond_china_yield=lambda **_: _curve_frame(),
    )
    result = AKShareLiquidityContextAdapter(client, now=lambda: NOW).fetch_liquidity_context(
        start_date=date(2026, 8, 10),
        end_date=date(2026, 8, 13),
    )

    assert result.degraded is True
    assert {item.family for item in result.repo_fixings} == {RepoFixingFamily.FR}
    assert result.government_curve is not None
    assert len(result.missing) == 1
    assert result.missing[0].context_id == "CHINAMONEY_FDR_FIXING"
    assert result.missing[0].failure_code is AShareContextFailureCode.UPSTREAM_ERROR


def test_liquidity_invalid_curve_does_not_discard_valid_fixings() -> None:
    bad_curve = _curve_frame().drop(columns=["10年"])
    client = SimpleNamespace(
        repo_rate_query=lambda *, symbol: (
            _repo_frame("FR") if symbol == "回购定盘利率" else _repo_frame("FDR")
        ),
        bond_china_yield=lambda **_: bad_curve,
    )
    result = AKShareLiquidityContextAdapter(client, now=lambda: NOW).fetch_liquidity_context(
        start_date=date(2026, 8, 10),
        end_date=date(2026, 8, 13),
    )

    assert len(result.repo_fixings) == 2
    assert result.government_curve is None
    assert result.missing[0].failure_code is AShareContextFailureCode.INVALID_PAYLOAD


def test_liquidity_each_source_has_an_independent_timeout() -> None:
    def slow_repo(*, symbol: str) -> pd.DataFrame:
        if symbol == "回购定盘利率":
            time.sleep(0.1)
        return _repo_frame("FR" if symbol == "回购定盘利率" else "FDR")

    client = SimpleNamespace(
        repo_rate_query=slow_repo,
        bond_china_yield=lambda **_: _curve_frame(),
    )
    result = AKShareLiquidityContextAdapter(
        client,
        timeout_seconds=0.02,
        now=lambda: NOW,
    ).fetch_liquidity_context(
        start_date=date(2026, 8, 10),
        end_date=date(2026, 8, 13),
    )

    assert {item.family for item in result.repo_fixings} == {RepoFixingFamily.FDR}
    timeout_gap = next(item for item in result.missing if item.context_id == "CHINAMONEY_FR_FIXING")
    assert timeout_gap.failure_code is AShareContextFailureCode.TIMEOUT
    assert result.government_curve is not None


def test_if_context_returns_only_ordered_raw_if_contracts() -> None:
    called: list[str] = []

    def endpoint(*, date: str) -> pd.DataFrame:
        called.append(date)
        return _if_frame()

    adapter = AKShareIFContextAdapter(
        SimpleNamespace(futures_hist_daily_cffex=endpoint),
        now=lambda: NOW,
    )
    result = adapter.fetch_if_daily_context(date(2026, 8, 13))

    assert called == ["20260813"]
    assert [item.symbol for item in result.contracts] == ["IF2608", "IF2609"]
    assert result.contracts[1].close == Decimal("4624.0")
    assert result.contracts[1].turnover_reported == Decimal("8305297.848")
    assert result.contracts[1].meta.available_at == NOW
    assert result.degraded is False
    assert "aligned CSI 300 spot" in " ".join(result.warnings)


def test_if_context_does_not_substitute_other_index_futures() -> None:
    only_ih = _if_frame().loc[lambda frame: frame["variety"] == "IH"]
    client = SimpleNamespace(futures_hist_daily_cffex=lambda **_: only_ih)

    with pytest.raises(AShareContextNoDataError, match="no IF contracts"):
        AKShareIFContextAdapter(client, now=lambda: NOW).fetch_if_daily_context(date(2026, 8, 13))


def test_if_context_rejects_inconsistent_ohlc() -> None:
    frame = _if_frame().loc[lambda value: value["variety"] == "IF"].copy()
    frame.loc[0, "high"] = "4600"
    client = SimpleNamespace(futures_hist_daily_cffex=lambda **_: frame)

    with pytest.raises(ValueError, match="high is inconsistent"):
        AKShareIFContextAdapter(client, now=lambda: NOW).fetch_if_daily_context(date(2026, 8, 13))
