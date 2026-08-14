from __future__ import annotations

import ast
import io
import re
import tokenize
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SOURCE_ROOT = _PROJECT_ROOT / "src" / "gribuki_trade"
_TEST_ROOT = _PROJECT_ROOT / "tests"
_SCRIPT_ROOT = _PROJECT_ROOT / "scripts"
_CONFIG_ROOT = _PROJECT_ROOT / "config"
_WORKFLOW_ROOT = _PROJECT_ROOT / ".github"
_CHINESE = re.compile(r"[\u3400-\u9fff]")
_ENGLISH_WORD = re.compile(r"[A-Za-z]{2,}")
_MACHINE_DIRECTIVE = re.compile(
    r"^(?:noqa|type:|pyright:|mypy:|pragma:|fmt:|isort:|pylint:|nosec|"
    r"coverage:|ruff:|coding[:=]|requires\b|shellcheck\b|!/)",
    re.IGNORECASE,
)
_MACHINE_ONLY = re.compile(r"[A-Z0-9_./:@+\-=<>|` *(),\[\]{}\\]+")


def _pure_english_explanations(path: Path) -> tuple[str, ...]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    findings: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(
            node,
            (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            continue
        value = ast.get_docstring(node, clean=False)
        if value is None or _CHINESE.search(value) or not _ENGLISH_WORD.search(value):
            continue
        line = getattr(node, "lineno", 1)
        findings.append(
            f"{path.relative_to(_PROJECT_ROOT)}:{line}:docstring:{value.splitlines()[0]}"
        )

    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type != tokenize.COMMENT:
            continue
        value = token.string[1:].strip()
        if (
            not value
            or _MACHINE_DIRECTIVE.match(value)
            or _CHINESE.search(value)
            or not _ENGLISH_WORD.search(value)
            or _MACHINE_ONLY.fullmatch(value)
        ):
            continue
        findings.append(
            f"{path.relative_to(_PROJECT_ROOT)}:{token.start[0]}:comment:{value}"
        )
    return tuple(findings)


def _checked_python_sources() -> tuple[Path, ...]:
    paths = {
        *_SOURCE_ROOT.rglob("*.py"),
        *_TEST_ROOT.rglob("*.py"),
        *_SCRIPT_ROOT.glob("*.py"),
        _PROJECT_ROOT / "conftest.py",
    }
    return tuple(sorted(path for path in paths if path.is_file()))


def _hash_comment(value: str) -> str | None:
    """提取 TOML、PowerShell、YAML 或 shell 行中不在引号内的井号注释。"""

    quote: str | None = None
    escaped = False
    for index, character in enumerate(value):
        if escaped:
            escaped = False
            continue
        if quote == '"' and character == "\\":
            escaped = True
            continue
        if character in {"'", '"'}:
            quote = None if quote == character else character if quote is None else quote
            continue
        if character == "#" and quote is None:
            return value[index + 1 :].strip()
    return None


def _pure_english_hash_comments(path: Path) -> tuple[str, ...]:
    findings: list[str] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        value = _hash_comment(line)
        if (
            value is None
            or not value
            or _MACHINE_DIRECTIVE.match(value)
            or _CHINESE.search(value)
            or not _ENGLISH_WORD.search(value)
            or _MACHINE_ONLY.fullmatch(value)
        ):
            continue
        findings.append(
            f"{path.relative_to(_PROJECT_ROOT)}:{line_number}:comment:{value}"
        )
    return tuple(findings)


def _checked_hash_comment_sources() -> tuple[Path, ...]:
    paths = {
        *_PROJECT_ROOT.glob("*.toml"),
        *_PROJECT_ROOT.glob("*.ps1"),
        *_PROJECT_ROOT.glob("*.sh"),
        *_CONFIG_ROOT.rglob("*.toml"),
        *_CONFIG_ROOT.rglob("*.yaml"),
        *_CONFIG_ROOT.rglob("*.yml"),
        *_SCRIPT_ROOT.rglob("*.ps1"),
        *_SCRIPT_ROOT.rglob("*.sh"),
        *_WORKFLOW_ROOT.rglob("*.yaml"),
        *_WORKFLOW_ROOT.rglob("*.yml"),
    }
    return tuple(sorted(path for path in paths if path.is_file()))


def test_source_explanatory_comments_and_docstrings_are_chinese() -> None:
    findings = tuple(
        finding
        for path in _checked_python_sources()
        for finding in _pure_english_explanations(path)
    )
    assert not findings, "仍有纯英文说明性注释或文档字符串：\n" + "\n".join(findings)


def test_non_python_explanatory_comments_are_chinese() -> None:
    findings = tuple(
        finding
        for path in _checked_hash_comment_sources()
        for finding in _pure_english_hash_comments(path)
    )
    assert not findings, "仍有纯英文非 Python 说明性注释：\n" + "\n".join(findings)
