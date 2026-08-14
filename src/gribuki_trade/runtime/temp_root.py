"""为本地工具集中、可审计地解析临时工作根目录。

此模块专用于 pytest 基础目录和诊断副本等可丢弃工作区。原子替换文件必须与目标文件
保持相邻，以便 ``os.replace`` 继续具备同一文件系统内的安全保证。
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

GRIBUKI_TRADE_TMP_DIR = "GRIBUKI_TRADE_TMP_DIR"
DEFAULT_TEMP_ROOT = Path("runtime") / "tmp"
_SCOPE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class TempRootSource(StrEnum):
    """已解析临时工作根目录所采用的优先级来源。"""

    EXPLICIT = "EXPLICIT"
    ENVIRONMENT = "ENVIRONMENT"
    DEFAULT = "DEFAULT"


@dataclass(frozen=True, slots=True)
class ResolvedTempRoot:
    """已解析的非根目录及其非敏感配置来源。"""

    path: Path
    source: TempRootSource
    created: bool

    def audit_document(self) -> dict[str, object]:
        return {
            "created": self.created,
            "environment_variable": GRIBUKI_TRADE_TMP_DIR,
            "path": str(self.path),
            "source": self.source.value,
        }


class TempRootResolver:
    """按显式参数、环境变量、默认值的优先级解析临时存储。"""

    def __init__(
        self,
        *,
        workspace_root: str | os.PathLike[str] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        base = Path.cwd() if workspace_root is None else Path(workspace_root)
        self._workspace_root = base.expanduser().resolve()
        self._environ = os.environ if environ is None else environ

    @property
    def workspace_root(self) -> Path:
        return self._workspace_root

    def resolve(
        self,
        explicit: str | os.PathLike[str] | None = None,
        *,
        create: bool = False,
    ) -> ResolvedTempRoot:
        """在不更改进程全局临时状态的前提下解析安全的临时工作根目录。"""

        raw_explicit = _optional_path_text(explicit, "explicit temp directory")
        raw_environment = _optional_path_text(
            self._environ.get(GRIBUKI_TRADE_TMP_DIR),
            GRIBUKI_TRADE_TMP_DIR,
        )
        if raw_explicit is not None:
            raw = raw_explicit
            source = TempRootSource.EXPLICIT
        elif raw_environment is not None:
            raw = raw_environment
            source = TempRootSource.ENVIRONMENT
        else:
            raw = str(DEFAULT_TEMP_ROOT)
            source = TempRootSource.DEFAULT

        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = self._workspace_root / candidate
        path = candidate.resolve()
        _reject_broad_root(path, self._workspace_root)
        if path.exists() and not path.is_dir():
            raise NotADirectoryError(f"temporary root is not a directory: {path}")
        existed = path.is_dir()
        if create:
            path.mkdir(parents=True, exist_ok=True)
            if not path.is_dir():  # pragma: no cover - defensive filesystem race
                raise NotADirectoryError(f"temporary root was not created: {path}")
        return ResolvedTempRoot(path=path, source=source, created=create and not existed)

    def scoped(
        self,
        scope: str,
        explicit: str | os.PathLike[str] | None = None,
        *,
        create: bool = False,
    ) -> ResolvedTempRoot:
        """在选定临时根目录下解析一个已验证的子命名空间。"""

        if not isinstance(scope, str) or _SCOPE_NAME.fullmatch(scope) is None:
            raise ValueError(
                "temporary scope must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}"
            )
        root = self.resolve(explicit, create=create)
        path = (root.path / scope).resolve()
        if path.parent != root.path:
            raise ValueError("temporary scope escaped the resolved root")
        existed = path.is_dir()
        if path.exists() and not path.is_dir():
            raise NotADirectoryError(f"temporary scope is not a directory: {path}")
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return ResolvedTempRoot(
            path=path,
            source=root.source,
            created=create and not existed,
        )


def _optional_path_text(
    value: str | os.PathLike[str] | None,
    name: str,
) -> str | None:
    if value is None:
        return None
    text = os.fspath(value).strip()
    if not text:
        raise ValueError(f"{name} must not be blank")
    if "\x00" in text:
        raise ValueError(f"{name} must not contain NUL")
    return text


def _reject_broad_root(path: Path, workspace_root: Path) -> None:
    anchor = Path(path.anchor).resolve()
    if path == anchor:
        raise ValueError("temporary root must not be a filesystem root")
    if path == workspace_root:
        raise ValueError("temporary root must not be the workspace root")


__all__ = [
    "DEFAULT_TEMP_ROOT",
    "GRIBUKI_TRADE_TMP_DIR",
    "ResolvedTempRoot",
    "TempRootResolver",
    "TempRootSource",
]
