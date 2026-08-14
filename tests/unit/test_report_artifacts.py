from __future__ import annotations

import os
from pathlib import Path

import pytest

from gribuki_trade.reporting import export_report_artifacts, report_text_to_markdown
from gribuki_trade.reporting.contracts import (
    ReportKind,
    render_stable_text_report,
    validate_markdown_report_contract,
)


def test_report_text_to_markdown_promotes_title_and_sections() -> None:
    source = "【A股｜收盘研究分析】\n\n一、标的档案\n代码：510300.SH\n- 指标 `MA20`"

    rendered = report_text_to_markdown(source)

    assert rendered.startswith("# A股｜收盘研究分析\n")
    assert "## 一、标的档案" in rendered
    assert "- 指标 `MA20`" in rendered


def test_export_report_markdown_is_atomic_and_refuses_overwrite(tmp_path: Path) -> None:
    bundle = export_report_artifacts(
        "【A股｜收盘研究分析】\n一、结论\n观察",
        tmp_path,
        "510300-20260813",
        render_images=False,
    )

    assert bundle.markdown_path.read_text(encoding="utf-8").startswith("# A股")
    assert bundle.image_paths == ()
    with pytest.raises(FileExistsError):
        export_report_artifacts(
            "另一份报告",
            tmp_path,
            "510300-20260813",
            render_images=False,
        )


def test_contract_text_becomes_contract_valid_markdown() -> None:
    source = render_stable_text_report(
        ReportKind.INSTRUMENT_RESEARCH,
        title="A股标的深研",
        sections={
            "结论": "继续观察",
            "技术结构": "趋势仍在",
            "基本面与宏观": "证据完整",
            "对抗观点": "反方提示估值风险",
            "失效条件": "跌破结构位",
        },
    )

    rendered = report_text_to_markdown(source)

    validate_markdown_report_contract(ReportKind.INSTRUMENT_RESEARCH, rendered)
    assert rendered.startswith("# A股标的深研\n")
    assert "> 报告类型：标的深度研究" in rendered


def test_unknown_declared_report_type_fails_closed() -> None:
    with pytest.raises(ValueError, match="unknown user-readable report type"):
        report_text_to_markdown(
            "【未知报告】\n报告类型：未注册报告\n\n〔结论〕\n内容"
        )


def test_export_report_rejects_unsafe_stem(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        export_report_artifacts("报告", tmp_path, "../outside", render_images=False)


@pytest.mark.skipif(os.name != "nt", reason="PNG font/layout smoke uses Windows Qt")
def test_export_report_renders_readable_png_pages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    report = "\n".join(
        (
            "【A股｜收盘研究分析】",
            "一、指标解释",
            *(f"- 第 {index} 项：`beta = cov(r_x, r_y) / var(r_x)`" for index in range(80)),
            "二、证据索引",
            "- [证据1] Cboe VIX 官方日线",
            "来源：https://www.cboe.com/",
        )
    )

    bundle = export_report_artifacts(
        report,
        tmp_path,
        "render-smoke",
        page_height=900,
    )

    assert len(bundle.image_paths) > 1
    assert all(path.stat().st_size > 1_000 for path in bundle.image_paths)
