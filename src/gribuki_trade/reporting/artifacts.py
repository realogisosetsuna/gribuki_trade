"""将研究报告导出为可搜索的 Markdown 与易读的 PNG 页面。

QQ 客户端不提供可靠的 Markdown 或 LaTeX 渲染约定。因此 Markdown 文件
作为可复制的归档形式，PNG 页面作为便携的视觉形式。本模块绝不会通过
网络发送任何一种产物。
"""

from __future__ import annotations

import html
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from gribuki_trade.reporting.contracts import (
    REPORT_CONTRACTS,
    ReportKind,
    validate_markdown_report_contract,
)

if TYPE_CHECKING:
    from PySide6.QtGui import QTextDocument

_SECTION = re.compile(r"^[一二三四五六七八九十百]+、\S")
_CONTRACT_SECTION = re.compile(r"^〔(?P<name>[^〔〕\r\n]+)〕$")
_SAFE_STEM = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,119}$")


@dataclass(frozen=True, slots=True)
class ReportArtifactBundle:
    """一份 Markdown 报告及零张或多张已渲染 PNG 页面的路径。"""

    markdown_path: Path
    image_paths: tuple[Path, ...]


def report_text_to_markdown(report_text: str) -> str:
    """把 QQ 文本转换为 Markdown；带契约标签的报告会在返回前强校验。"""

    normalized = report_text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        raise ValueError("report_text must not be empty")
    output: list[str] = []
    declared_kind: ReportKind | None = None
    names = {contract.chinese_name: kind for kind, contract in REPORT_CONTRACTS.items()}
    for index, raw_line in enumerate(normalized.splitlines()):
        line = raw_line.rstrip()
        if index == 0 and line.startswith("【") and line.endswith("】"):
            output.append(f"# {line[1:-1]}")
        elif line.startswith("报告类型："):
            chinese_name = line.removeprefix("报告类型：").strip()
            declared_kind = names.get(chinese_name)
            if declared_kind is None:
                raise ValueError("unknown user-readable report type")
            output.append(f"> {line}")
        elif match := _CONTRACT_SECTION.fullmatch(line):
            output.append(f"## {match.group('name')}")
        elif _SECTION.match(line):
            output.append(f"## {line}")
        else:
            output.append(line)
    rendered = "\n".join(output).rstrip() + "\n"
    if declared_kind is not None:
        validate_markdown_report_contract(declared_kind, rendered)
    return rendered


def export_report_artifacts(
    report_text: str,
    output_dir: Path,
    stem: str,
    *,
    render_images: bool = True,
    overwrite: bool = False,
    page_width: int = 1240,
    page_height: int = 1754,
) -> ReportArtifactBundle:
    """原子导出 Markdown，以及可选的分页 PNG 图像。"""

    if not _SAFE_STEM.fullmatch(stem):
        raise ValueError("stem must contain only safe ASCII filename characters")
    if page_width < 640 or page_height < 800:
        raise ValueError("rendered pages are too small for a readable report")
    root = output_dir.resolve()
    if root.exists() and root.is_symlink():
        raise ValueError("output_dir must not be a symbolic link")
    root.mkdir(parents=True, exist_ok=True)
    markdown = report_text_to_markdown(report_text)
    markdown_path = root / f"{stem}.md"
    _atomic_write_text(markdown_path, markdown, overwrite=overwrite)
    images: tuple[Path, ...] = ()
    try:
        if render_images:
            images = _render_markdown_pages(
                markdown,
                root,
                stem,
                page_width=page_width,
                page_height=page_height,
                overwrite=overwrite,
            )
    except Exception:
        if not overwrite:
            markdown_path.unlink(missing_ok=True)
        raise
    return ReportArtifactBundle(markdown_path=markdown_path, image_paths=images)


def _render_markdown_pages(
    markdown: str,
    output_dir: Path,
    stem: str,
    *,
    page_width: int,
    page_height: int,
    overwrite: bool,
) -> tuple[Path, ...]:
    from PySide6.QtCore import QRectF
    from PySide6.QtGui import (
        QColor,
        QGuiApplication,
        QImage,
        QPainter,
    )

    application = QGuiApplication.instance()
    owns_application = application is None
    if application is None:
        application = QGuiApplication([])
    margin = 64
    content_width = page_width - 2 * margin
    content_height = page_height - 2 * margin - 42
    fragments = _markdown_fragments(markdown)
    pages: list[list[str]] = []
    current: list[str] = []
    for fragment in fragments:
        candidate = [*current, fragment]
        if current and _document_height(candidate, content_width) > content_height:
            pages.append(current)
            current = [fragment]
        else:
            current = candidate
    if current:
        pages.append(current)
    if not pages:
        raise ValueError("report did not contain renderable content")

    destinations = tuple(
        output_dir / f"{stem}-page-{index:02d}.png"
        for index in range(1, len(pages) + 1)
    )
    if not overwrite:
        collisions = [path for path in destinations if path.exists()]
        if collisions:
            raise FileExistsError("one or more report image pages already exist")

    written: list[Path] = []
    try:
        for index, (fragments_on_page, destination) in enumerate(
            zip(pages, destinations, strict=True),
            start=1,
        ):
            document = _text_document(fragments_on_page, content_width)
            image = QImage(page_width, page_height, QImage.Format.Format_RGB32)
            image.fill(QColor("#f5f7fb"))
            painter = QPainter(image)
            try:
                painter.translate(margin, margin)
                document.drawContents(
                    painter,
                    QRectF(0, 0, content_width, content_height),
                )
                painter.resetTransform()
                painter.setPen(QColor("#64748b"))
                painter.drawText(
                    margin,
                    page_height - 28,
                    f"Gribuki Trade · {index}/{len(pages)}",
                )
            finally:
                painter.end()
            temporary = destination.with_name(
                f".{destination.name}.{uuid.uuid4().hex}.tmp.png"
            )
            if not image.save(str(temporary)):
                raise RuntimeError("Qt failed to encode a report PNG")
            if not overwrite and destination.exists():
                temporary.unlink(missing_ok=True)
                raise FileExistsError("report image page already exists")
            os.replace(temporary, destination)
            written.append(destination)
    except Exception:
        for path in written:
            path.unlink(missing_ok=True)
        raise
    finally:
        if owns_application:
            application.quit()
    return destinations


def _document_height(fragments: list[str], width: int) -> float:
    return float(_text_document(fragments, width).size().height())


def _text_document(fragments: list[str], width: int) -> QTextDocument:
    from PySide6.QtGui import QTextDocument

    document = QTextDocument()
    document.setDefaultStyleSheet(
        "body{font-family:'Microsoft YaHei UI','PingFang SC',sans-serif;"
        "font-size:17px;line-height:1.55;color:#172033;}"
        "h1{font-size:30px;color:#0f3d6e;margin:0 0 18px 0;}"
        "h2{font-size:23px;color:#165d96;margin:18px 0 9px 0;"
        "border-bottom:1px solid #cbd5e1;padding-bottom:5px;}"
        "p{margin:5px 0;}ul{margin:4px 0 7px 22px;}"
        "li{margin:3px 0;}code{font-family:Consolas,monospace;"
        "background:#e8eef6;color:#0f3d6e;padding:1px 3px;}"
        ".evidence{color:#475569;font-size:15px;}"
    )
    document.setHtml("<html><body>" + "".join(fragments) + "</body></html>")
    document.setTextWidth(width)
    return document


def _markdown_fragments(markdown: str) -> list[str]:
    fragments: list[str] = []
    in_list = False
    for raw_line in markdown.splitlines():
        line = raw_line.strip()
        if not line:
            if in_list:
                fragments.append("</ul>")
                in_list = False
            fragments.append("<p>&nbsp;</p>")
            continue
        if line.startswith("# "):
            if in_list:
                fragments.append("</ul>")
                in_list = False
            fragments.append(f"<h1>{_inline(line[2:])}</h1>")
        elif line.startswith("## "):
            if in_list:
                fragments.append("</ul>")
                in_list = False
            fragments.append(f"<h2>{_inline(line[3:])}</h2>")
        elif line.startswith("- "):
            if not in_list:
                fragments.append("<ul>")
                in_list = True
            css_class = " class='evidence'" if "证据" in line else ""
            fragments.append(f"<li{css_class}>{_inline(line[2:])}</li>")
        else:
            if in_list:
                fragments.append("</ul>")
                in_list = False
            css_class = " class='evidence'" if line.startswith("来源：") else ""
            fragments.append(f"<p{css_class}>{_inline(line)}</p>")
    if in_list:
        fragments.append("</ul>")
    return fragments


def _inline(value: str) -> str:
    escaped = html.escape(value, quote=False)
    # 反引号足以为公式与标识符提供确定的样式；数学含义仍保留在源文本中，
    # 而不是交由渲染器解释。
    pieces = escaped.split("`")
    return "".join(
        f"<code>{piece}</code>" if index % 2 else piece
        for index, piece in enumerate(pieces)
    )


def _atomic_write_text(path: Path, text: str, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"report artifact already exists: {path.name}")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        if path.exists() and not overwrite:
            raise FileExistsError(f"report artifact already exists: {path.name}")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
