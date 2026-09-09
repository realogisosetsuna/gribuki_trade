"""六类用户报告的稳定骨架黄金测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from gribuki_trade.reporting.contracts import (
    REPORT_CONTRACTS,
    ReportKind,
    render_stable_markdown_report,
    render_stable_text_report,
)

FIXTURES = Path(__file__).parents[2] / "fixtures" / "report_contracts"


@pytest.mark.parametrize(
    ("kind", "title", "filename"),
    (
        (ReportKind.EXECUTION_RECEIPT, "契约黄金样例｜成交", "execution_receipt.md"),
        (ReportKind.POSITION_REVIEW, "契约黄金样例｜持仓", "position_review.md"),
        (ReportKind.DAILY_REVIEW, "契约黄金样例｜日报", "daily_review.md"),
        (ReportKind.INSTRUMENT_RESEARCH, "契约黄金样例｜深研", "instrument_research.md"),
        (ReportKind.SYSTEM_HEALTH, "契约黄金样例｜健康", "system_health.md"),
    ),
)
def test_markdown_contract_matches_reviewed_golden(
    kind: ReportKind,
    title: str,
    filename: str,
) -> None:
    contract = REPORT_CONTRACTS[kind]
    rendered = render_stable_markdown_report(
        kind,
        title=title,
        sections={name: f"{name}内容" for name in contract.required_sections},
    )

    assert rendered == (FIXTURES / filename).read_text(encoding="utf-8")


def test_intraday_text_contract_matches_reviewed_golden() -> None:
    contract = REPORT_CONTRACTS[ReportKind.INTRADAY_ALERT]
    rendered = render_stable_text_report(
        ReportKind.INTRADAY_ALERT,
        title="契约黄金样例｜盘中",
        sections={name: f"{name}内容" for name in contract.required_sections},
    )

    assert rendered == (FIXTURES / "intraday_alert.txt").read_text(
        encoding="utf-8"
    ).rstrip("\n")
