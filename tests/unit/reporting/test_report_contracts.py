"""稳定报告分类与中文可读原因的测试。"""

from __future__ import annotations

from gribuki_trade.reporting.contracts import (
    REPORT_CONTRACTS,
    ReportDelivery,
    ReportKind,
    humanize_codes,
    humanize_internal_code,
    render_stable_markdown_report,
    render_stable_text_report,
    stable_markdown_template,
    validate_markdown_report_contract,
    validate_text_report_contract,
)


def test_every_report_kind_has_one_stable_contract() -> None:
    assert set(REPORT_CONTRACTS) == set(ReportKind)
    assert len({item.chinese_name for item in REPORT_CONTRACTS.values()}) == len(ReportKind)
    assert all(item.required_sections for item in REPORT_CONTRACTS.values())


def test_long_reports_prefer_markdown_artifacts() -> None:
    assert REPORT_CONTRACTS[ReportKind.INTRADAY_ALERT].delivery is ReportDelivery.SHORT_TEXT
    for kind in (
        ReportKind.POSITION_REVIEW,
        ReportKind.DAILY_REVIEW,
        ReportKind.INSTRUMENT_RESEARCH,
    ):
        assert REPORT_CONTRACTS[kind].delivery in {
            ReportDelivery.MARKDOWN_FILE,
            ReportDelivery.SHORT_TEXT_OR_MARKDOWN,
            ReportDelivery.SHORT_TEXT_AND_MARKDOWN,
        }


def test_delivery_contract_does_not_promise_two_artifacts_when_one_is_valid() -> None:
    """成交、标的研究和健康报告可按场景选一种格式，日报才要求双交付。"""

    for kind in (
        ReportKind.EXECUTION_RECEIPT,
        ReportKind.INSTRUMENT_RESEARCH,
        ReportKind.SYSTEM_HEALTH,
    ):
        assert (
            REPORT_CONTRACTS[kind].delivery
            is ReportDelivery.SHORT_TEXT_OR_MARKDOWN
        )
    assert (
        REPORT_CONTRACTS[ReportKind.DAILY_REVIEW].delivery
        is ReportDelivery.SHORT_TEXT_AND_MARKDOWN
    )


def test_unknown_internal_code_keeps_precise_audit_locator() -> None:
    unknown = "SOME_NEW_INTERNAL_FAILURE_CODE"
    rendered = humanize_internal_code(unknown)
    assert unknown in rendered
    assert "审计码" in rendered
    assert humanize_internal_code("GROSS_LIMIT_REACHED") == "组合总敞口已达到策略上限"
    assert humanize_codes(("GROSS_LIMIT_REACHED", "GROSS_LIMIT_REACHED")) == (
        "组合总敞口已达到策略上限"
    )


def test_markdown_template_preserves_required_order_and_rejects_omission() -> None:
    contract = REPORT_CONTRACTS[ReportKind.DAILY_REVIEW]
    rendered = render_stable_markdown_report(
        ReportKind.DAILY_REVIEW,
        title="交易日复盘",
        sections={name: f"{name}内容" for name in reversed(contract.required_sections)},
    )
    offsets = [rendered.index(f"## {name}") for name in contract.required_sections]
    assert offsets == sorted(offsets)
    assert rendered.startswith("# 交易日复盘\n")

    try:
        render_stable_markdown_report(
            ReportKind.DAILY_REVIEW,
            title="缺节报告",
            sections={contract.required_sections[0]: "只有一节"},
        )
    except ValueError as error:
        assert "missing required report sections" in str(error)
    else:  # pragma: no cover - fail-closed contract
        raise AssertionError("missing sections must be rejected")


def test_template_is_markdown_first_but_intraday_alert_remains_short_text() -> None:
    template = stable_markdown_template(ReportKind.POSITION_REVIEW, title="持仓复核")
    assert template.count("## ") == len(
        REPORT_CONTRACTS[ReportKind.POSITION_REVIEW].required_sections
    )
    try:
        stable_markdown_template(ReportKind.INTRADAY_ALERT, title="盘中提醒")
    except ValueError as error:
        assert "short-text" in str(error)
    else:  # pragma: no cover - delivery contract
        raise AssertionError("intraday alerts must not become Markdown files")


def test_text_contract_is_ordered_validatable_and_markdown_only_kind_is_rejected() -> None:
    contract = REPORT_CONTRACTS[ReportKind.INTRADAY_ALERT]
    rendered = render_stable_text_report(
        ReportKind.INTRADAY_ALERT,
        title="A股模拟盘｜信号提醒",
        sections={name: f"{name}内容" for name in reversed(contract.required_sections)},
    )

    validate_text_report_contract(ReportKind.INTRADAY_ALERT, rendered)
    offsets = [rendered.index(f"〔{name}〕") for name in contract.required_sections]
    assert offsets == sorted(offsets)
    assert "报告类型：盘中交易告警" in rendered

    try:
        render_stable_text_report(
            ReportKind.POSITION_REVIEW,
            title="持仓复核",
            sections={
                name: "内容"
                for name in REPORT_CONTRACTS[ReportKind.POSITION_REVIEW].required_sections
            },
        )
    except ValueError as error:
        assert "Markdown-only" in str(error)
    else:  # pragma: no cover - 交付契约必须失败关闭
        raise AssertionError("Markdown-only report must reject text rendering")


def test_custom_markdown_validator_rejects_wrong_order_and_empty_section() -> None:
    correct = "\n".join(
        (
            "# 持仓复核",
            "",
            "## 当前结论",
            "有持仓",
            "## 保护计划",
            "止损有效",
            "## 证据与反证",
            "证据已冻结",
            "## 下一复核条件",
            "下一交易日",
        )
    )
    validate_markdown_report_contract(ReportKind.POSITION_REVIEW, correct)

    wrong = correct.replace("## 当前结论", "## 临时").replace(
        "## 保护计划", "## 当前结论"
    ).replace("## 临时", "## 保护计划")
    try:
        validate_markdown_report_contract(ReportKind.POSITION_REVIEW, wrong)
    except ValueError as error:
        assert "out of contract order" in str(error)
    else:  # pragma: no cover - 错序必须失败关闭
        raise AssertionError("wrong-order report must be rejected")
