"""仓库级 pytest 临时目录策略。

本文件刻意放在仓库根目录。pytest 会在校验命令行参数前先加载根级
conftest，因此自定义 ``--temp-dir`` 选项也能在直接执行
``python -m pytest`` 且未显式传入 ``tests`` 路径时生效。
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from gribuki_trade.runtime.temp_root import TempRootResolver


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("gribuki-trade")
    group.addoption(
        "--temp-dir",
        dest="gribuki_trade_temp_dir",
        metavar="PATH",
        help=(
            "scratch root for this test process; otherwise "
            "GRIBUKI_TRADE_TMP_DIR and runtime/tmp are used"
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    """在不破坏并发运行的前提下，把 pytest 产物集中到统一临时根。"""

    if config.getoption("basetemp") is not None:
        return
    explicit = config.getoption("gribuki_trade_temp_dir")
    workspace = Path(str(config.rootpath)).resolve()
    pytest_root = TempRootResolver(workspace_root=workspace).scoped(
        "pytest",
        explicit,
        create=True,
    )
    run_root = pytest_root.path / f"run-{os.getpid()}"
    standard_library_root = run_root / "stdlib"
    standard_library_root.mkdir(parents=True, exist_ok=True)
    config.option.basetemp = str(run_root / "basetemp")
    # 将 unittest 风格的 TemporaryDirectory 调用者和子进程一并引到同一棵
    # 进程隔离树下。pytest 只拥有旁侧的 basetemp，因此其启动清理不会误伤这里。
    tempfile.tempdir = str(standard_library_root)
    for variable in ("TMPDIR", "TEMP", "TMP"):
        os.environ[variable] = str(standard_library_root)
