from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from gribuki_trade.runtime.temp_root import (
    GRIBUKI_TRADE_TMP_DIR,
    TempRootResolver,
    TempRootSource,
)


def test_default_root_is_runtime_tmp_without_creating_it(tmp_path: Path) -> None:
    resolver = TempRootResolver(workspace_root=tmp_path, environ={})

    resolved = resolver.resolve()

    assert resolved.path == (tmp_path / "runtime" / "tmp").resolve()
    assert resolved.source is TempRootSource.DEFAULT
    assert resolved.created is False
    assert not resolved.path.exists()


def test_pytest_stdlib_temporary_directory_is_process_isolated() -> None:
    with tempfile.TemporaryDirectory() as directory:
        resolved = Path(directory).resolve()
        assert resolved.parent.name == "stdlib"
        assert resolved.parent.parent.name.startswith("run-")
        assert resolved.parent.parent.parent.name == "pytest"


def test_explicit_path_precedes_environment_and_relative_paths_use_workspace(
    tmp_path: Path,
) -> None:
    resolver = TempRootResolver(
        workspace_root=tmp_path,
        environ={GRIBUKI_TRADE_TMP_DIR: "environment-temp"},
    )

    environment = resolver.resolve(create=True)
    explicit = resolver.resolve("operator-temp", create=True)

    assert environment.path == (tmp_path / "environment-temp").resolve()
    assert environment.source is TempRootSource.ENVIRONMENT
    assert environment.created is True
    assert explicit.path == (tmp_path / "operator-temp").resolve()
    assert explicit.source is TempRootSource.EXPLICIT
    assert explicit.created is True


def test_scoped_root_is_validated_and_auditable(tmp_path: Path) -> None:
    resolved = TempRootResolver(workspace_root=tmp_path, environ={}).scoped(
        "pytest",
        create=True,
    )

    assert resolved.path == (tmp_path / "runtime" / "tmp" / "pytest").resolve()
    assert resolved.path.is_dir()
    assert resolved.audit_document() == {
        "created": True,
        "environment_variable": GRIBUKI_TRADE_TMP_DIR,
        "path": str(resolved.path),
        "source": "DEFAULT",
    }


@pytest.mark.parametrize("scope", ("", "../escape", "two/levels", "has space"))
def test_scoped_root_rejects_unsafe_names(tmp_path: Path, scope: str) -> None:
    resolver = TempRootResolver(workspace_root=tmp_path, environ={})

    with pytest.raises(ValueError, match="temporary scope"):
        resolver.scoped(scope)


def test_root_rejects_blank_broad_and_file_targets(tmp_path: Path) -> None:
    resolver = TempRootResolver(workspace_root=tmp_path, environ={})
    file_path = tmp_path / "not-a-directory"
    file_path.write_text("fixture", encoding="utf-8")

    with pytest.raises(ValueError, match="must not be blank"):
        resolver.resolve(" ")
    with pytest.raises(ValueError, match="workspace root"):
        resolver.resolve(tmp_path)
    with pytest.raises(NotADirectoryError):
        resolver.resolve(file_path)
