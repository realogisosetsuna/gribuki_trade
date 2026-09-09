"""验证 A 股 PAPER 日账户摘要和日报投影的纯函数边界。"""

from datetime import UTC, date, datetime
from decimal import Decimal

from gribuki_trade.domain.paper_trading import PaperAccountSnapshot
from gribuki_trade.services.ashare import ashare_paper_day_reports as reports


def _snapshot() -> PaperAccountSnapshot:
    now = datetime(2026, 8, 14, 8, 0, tzinfo=UTC)
    return PaperAccountSnapshot(
        account_id="report-account",
        session_date=date(2026, 8, 14),
        cash=Decimal("100000"),
        positions=(),
        opened_at=now,
        updated_at=now,
        last_sequence=1,
    )


def test_account_summary_is_deterministic_and_text_reuses_document() -> None:
    document = reports.account_summary_document(_snapshot(), {})

    assert document == {
        "cash": Decimal("100000"),
        "estimated_equity": Decimal("100000"),
        "estimated_market_value": Decimal("0"),
        "positions": [],
    }
    assert reports.account_summary_text(
        document,
        label="收盘",
        pending_order_count=2,
    ) == (
        "【A股模拟盘｜收盘摘要】\n"
        "现金：100000.00 元\n"
        "估算持仓市值：0.00 元\n"
        "估算权益：100000.00 元\n"
        "持仓数量：0；待撮合：2。"
    )


def test_render_report_is_side_effect_free_and_validates_daily_contract() -> None:
    rendered = reports.render_report(
        events=(),
        snapshot=_snapshot(),
        fills=(),
        session_date=date(2026, 8, 14),
        run_id="run-1",
        strategy_version="paper-v1",
        partial_session=False,
        initial_cash=Decimal("100000"),
        last_prices={},
        exit_plans={},
        required=0,
        sent=0,
        gaps=0,
    )

    assert rendered.startswith("# A股模拟盘全天运行报告\n")
    assert "- 交易日：2026-08-14" in rendered
    assert "- 成交笔数：0" in rendered
    assert rendered.endswith("\n")
