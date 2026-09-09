from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.ports.ashare_breadth import (
    AShareBreadthMeta,
    AShareBreadthSnapshot,
    AShareExchange,
    AShareExchangeCount,
)
from gribuki_trade.services.ashare.evidence.ashare_breadth_evidence import (
    build_ashare_breadth_evidence,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
SEEN = datetime(2026, 8, 13, 16, 30, tzinfo=SHANGHAI)
AS_OF = datetime(2026, 8, 13, 9, 0, tzinfo=UTC)


def _snapshot(*, degraded: bool = False) -> AShareBreadthSnapshot:
    return AShareBreadthSnapshot(
        session_date=date(2026, 8, 13),
        received_count=5005,
        eligible_count=5000,
        minimum_eligible_count=4500,
        expected_count=None,
        coverage_percent=None,
        duplicate_count=1,
        excluded_non_equity_count=2,
        non_trading_count=2,
        included_exchanges=(
            AShareExchangeCount(AShareExchange.SHANGHAI, 2200),
            AShareExchangeCount(AShareExchange.SHENZHEN, 2500),
            AShareExchangeCount(AShareExchange.BEIJING, 300),
        ),
        advancing_count=3000,
        declining_count=1800,
        flat_count=200,
        advance_decline_ratio=Decimal("1.666666666666"),
        advancing_amount_share_percent=Decimal("63.45678"),
        equal_weight_mean_change_percent=Decimal("0.23888"),
        median_change_percent=Decimal("0.11001"),
        total_amount_cny=Decimal("2500123456789"),
        limit_up_count=None,
        limit_down_count=None,
        meta=AShareBreadthMeta(
            source_id="AKShare/Eastmoney stock_zh_a_spot_em",
            source_url="https://quote.eastmoney.com/center/gridlist.html#hs_a_board",
            observed_at=SEEN,
            available_at=SEEN,
            fetched_at=SEEN,
            stale=False,
            degraded=degraded,
            fallback_used=degraded,
            warnings=("secondary public-web snapshot",),
        ),
    )


def test_evidence_is_content_addressed_human_readable_and_three_decimal() -> None:
    first = build_ashare_breadth_evidence(_snapshot(), AS_OF)
    second = build_ashare_breadth_evidence(_snapshot(), AS_OF)

    assert first == second
    assert len(first.items) == 1
    assert first.items[0].evidence_id == first.references[0].evidence_id
    assert len(first.items[0].evidence_id) == 64
    assert first.items[0].publisher.startswith("AKShare/Eastmoney")
    text = "\n".join(first.report_lines)
    assert "二手公开网页研究快照" in text
    assert "上海=2200家、深圳=2500家、北京=300家" in text
    assert "不报告精确覆盖率" in text
    assert "涨跌家数比=1.667" in text
    assert "等权平均涨跌幅=0.239%" in text
    assert "中位涨跌幅=0.110%" in text
    assert "上涨股票成交额占比=63.457%" in text
    assert "25001.235亿元" in text
    assert "涨停/跌停家数：缺失" in text
    assert "证据引用：A股全市场宽度｜沪深京收盘快照" in text
    assert "回退=否；状态=正常；PIT约束=" in text
    assert first.items[0].evidence_id not in text


def test_missing_snapshot_is_explicit_and_creates_no_evidence() -> None:
    bundle = build_ashare_breadth_evidence(None, AS_OF)

    assert bundle.items == ()
    assert bundle.references == ()
    assert "未用零值" in bundle.report_lines[0]


def test_first_seen_after_as_of_is_rejected() -> None:
    with pytest.raises(ValueError, match="first-seen"):
        build_ashare_breadth_evidence(
            _snapshot(),
            datetime(2026, 8, 13, 8, 29, tzinfo=UTC),
        )


def test_fallback_status_is_reported_as_degraded() -> None:
    text = "\n".join(build_ashare_breadth_evidence(_snapshot(degraded=True), AS_OF).report_lines)
    assert "回退=是；状态=正常、降级" in text
