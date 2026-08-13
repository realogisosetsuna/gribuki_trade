from __future__ import annotations

import os
from pathlib import Path

import pytest

from gribuki_trade.reporting import export_report_artifacts, report_text_to_markdown


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
