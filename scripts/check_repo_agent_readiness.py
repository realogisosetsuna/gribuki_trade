from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = (
    ROOT / "AGENTS.md",
    ROOT / "ARCHITECTURE.md",
    ROOT / "docs" / "architecture" / "README.md",
    ROOT / "docs" / "exec-plans" / "README.md",
)
LINK_PATTERN = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
FORBIDDEN_IMPORTS = {
    "domain": {"adapters", "services", "storage", "gui", "cli"},
    "ports": {"adapters", "services", "storage", "gui", "cli"},
    "features": {"adapters", "services", "gui", "cli"},
    "strategy_lab": {"adapters", "services", "gui", "runtime", "cli"},
}


def _markdown_files() -> tuple[Path, ...]:
    roots = (ROOT / "AGENTS.md", ROOT / "ARCHITECTURE.md")
    architecture = tuple((ROOT / "docs" / "architecture").glob("*.md"))
    plans = tuple((ROOT / "docs" / "exec-plans").rglob("*.md"))
    local_maps = tuple((ROOT / "src").rglob("AGENTS.md"))
    return tuple(sorted((*roots, *architecture, *plans, *local_maps)))


def _check_required_files(errors: list[str]) -> None:
    for path in REQUIRED_FILES:
        if not path.is_file():
            errors.append(f"missing required file: {path.relative_to(ROOT)}")


def _check_markdown_links(errors: list[str]) -> None:
    for path in _markdown_files():
        text = path.read_text(encoding="utf-8")
        for raw_target in LINK_PATTERN.findall(text):
            target = raw_target.strip().split("#", 1)[0]
            if not target or target.startswith(("http://", "https://", "mailto:")):
                continue
            resolved = (path.parent / target).resolve()
            try:
                resolved.relative_to(ROOT)
            except ValueError:
                errors.append(f"link escapes repository: {path.relative_to(ROOT)} -> {target}")
                continue
            if not resolved.exists():
                errors.append(f"broken local link: {path.relative_to(ROOT)} -> {target}")


def _check_architecture_evidence(errors: list[str]) -> None:
    architecture_root = ROOT / "docs" / "architecture"
    for path in architecture_root.glob("*.md"):
        if path.name == "README.md":
            continue
        text = path.read_text(encoding="utf-8")
        if "src/gribuki_trade/" not in text:
            errors.append(f"architecture page lacks source evidence: {path.relative_to(ROOT)}")
        if "tests/unit/" not in text:
            errors.append(f"architecture page lacks test evidence: {path.relative_to(ROOT)}")


def _check_active_plans(errors: list[str]) -> None:
    active_root = ROOT / "docs" / "exec-plans" / "active"
    if not active_root.is_dir():
        errors.append("missing docs/exec-plans/active")
        return
    for path in active_root.glob("*.md"):
        text = path.read_text(encoding="utf-8")
        if not re.search(r"^Status:\s*active(?:\s*\(.*\))?\s*$", text, re.MULTILINE):
            errors.append(f"active plan has no active status: {path.relative_to(ROOT)}")
        if "## Objective" not in text:
            errors.append(f"active plan has no objective: {path.relative_to(ROOT)}")


def _check_local_maps(errors: list[str]) -> None:
    for path in (ROOT / "src").rglob("AGENTS.md"):
        text = path.read_text(encoding="utf-8")
        if "ARCHITECTURE.md" not in text:
            errors.append(f"local map lacks architecture route: {path.relative_to(ROOT)}")
        if "tests" not in text:
            errors.append(f"local map lacks test route: {path.relative_to(ROOT)}")


def _imported_packages(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    packages: set[str] = set()
    for node in ast.walk(tree):
        names: tuple[str, ...]
        if isinstance(node, ast.Import):
            names = tuple(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names = (node.module,)
        else:
            continue
        for name in names:
            prefix = "gribuki_trade."
            if name.startswith(prefix):
                remainder = name[len(prefix) :]
                packages.add(remainder.split(".", 1)[0])
    return packages


def _check_dependency_direction(errors: list[str]) -> None:
    source_root = ROOT / "src" / "gribuki_trade"
    for package, forbidden in FORBIDDEN_IMPORTS.items():
        package_root = source_root / package
        for path in package_root.rglob("*.py"):
            for imported in sorted(_imported_packages(path) & forbidden):
                errors.append(
                    f"forbidden dependency: {path.relative_to(ROOT)} -> gribuki_trade.{imported}"
                )


def check_repository() -> tuple[str, ...]:
    errors: list[str] = []
    _check_required_files(errors)
    _check_markdown_links(errors)
    _check_architecture_evidence(errors)
    _check_active_plans(errors)
    _check_local_maps(errors)
    _check_dependency_direction(errors)
    return tuple(errors)


def main() -> int:
    errors = check_repository()
    if errors:
        print("仓库 agent-readiness 检查失败：")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("仓库 agent-readiness 检查通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
