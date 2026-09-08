"""NapCat 进程生命周期边界。

该模块只负责解析受支持的本机启动命令，以及管理当前 GUI 实例亲自启动的
``QProcess``。它不读取凭据、不访问网络，也不渲染进程输出；集成面板通过
``NapCatProcessControl`` 协议使用它。
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from PySide6.QtCore import QObject, QProcess, QTimer


@dataclass(frozen=True, slots=True)
class NapCatLaunchCommand:
    """针对唯一一个受支持本机 NapCat 运行时的已校验命令。"""

    program: str
    arguments: tuple[str, ...]
    working_directory: str


@dataclass(frozen=True, slots=True)
class ProcessSnapshot:
    """本 GUI 实例创建进程的非敏感状态。"""

    owned: bool
    running: bool
    starting: bool
    detail: str


class NapCatProcessControl(Protocol):
    """集成面板与离屏测试替身共用的生命周期边界。"""

    def set_listener(self, listener: Callable[[ProcessSnapshot], None]) -> None: ...

    def snapshot(self) -> ProcessSnapshot: ...

    def start(self, command: NapCatLaunchCommand) -> bool: ...

    def stop_owned(self) -> bool: ...


class QtNapCatProcessController(QObject):
    """仅管理由本 GUI 实例显式启动的进程树。"""

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._process = QProcess(self)
        self._process.setProcessChannelMode(QProcess.ProcessChannelMode.MergedChannels)
        self._listener: Callable[[ProcessSnapshot], None] | None = None
        self._owned = False
        self._starting = False
        self._stopper: QProcess | None = None
        self._process.started.connect(self._on_started)
        self._process.finished.connect(self._on_finished)
        self._process.errorOccurred.connect(self._on_error)
        self._process.readyReadStandardOutput.connect(self._discard_output)

    def set_listener(self, listener: Callable[[ProcessSnapshot], None]) -> None:
        self._listener = listener
        listener(self.snapshot())

    def snapshot(self) -> ProcessSnapshot:
        running = self._process.state() == QProcess.ProcessState.Running
        return ProcessSnapshot(
            owned=self._owned,
            running=running,
            starting=self._starting,
            detail=self._detail(running),
        )

    def start(self, command: NapCatLaunchCommand) -> bool:
        if self._owned or self._process.state() != QProcess.ProcessState.NotRunning:
            return False
        self._owned = True
        self._starting = True
        self._process.setWorkingDirectory(command.working_directory)
        self._process.setProgram(command.program)
        self._process.setArguments(list(command.arguments))
        self._process.start()
        self._publish()
        return True

    def stop_owned(self) -> bool:
        if not self._owned:
            return False
        if self._process.state() == QProcess.ProcessState.NotRunning:
            self._owned = False
            self._starting = False
            self._publish()
            return True
        process_id = int(self._process.processId())
        if os.name == "nt" and process_id > 0:
            # 精确的子进程 PID 来自当前 QProcess。/T 也会关闭其 QQ/NapCat
            # 子进程，但不会影响无关的 NapCat 实例。
            stopper = QProcess(self)
            stopper.finished.connect(self._clear_stopper)
            self._stopper = stopper
            stopper.start("taskkill.exe", ["/PID", str(process_id), "/T", "/F"])
        else:
            self._process.terminate()
            QTimer.singleShot(5_000, self._kill_if_still_owned)
        self._publish("正在停止本窗口启动的进程…")
        return True

    def _on_started(self) -> None:
        self._starting = False
        self._publish("本窗口启动的 NapCat 进程正在运行。")

    def _on_finished(self, _exit_code: int, _exit_status: QProcess.ExitStatus) -> None:
        self._owned = False
        self._starting = False
        self._publish("本窗口启动的 NapCat 进程已退出。")

    def _on_error(self, _error: QProcess.ProcessError) -> None:
        if self._process.state() == QProcess.ProcessState.NotRunning:
            self._owned = False
            self._starting = False
        self._publish("NapCat 进程操作失败；详细信息已隐藏。")

    def _clear_stopper(self, _exit_code: int, _exit_status: QProcess.ExitStatus) -> None:
        if self._stopper is not None:
            self._stopper.deleteLater()
            self._stopper = None

    def _discard_output(self) -> None:
        # 运行输出可能包含账户元数据或二维码、登录详情。这里持续排空以限制
        # 内存占用，并且绝不把内容复制到 GUI。
        self._process.readAllStandardOutput()

    def _kill_if_still_owned(self) -> None:
        if self._owned and self._process.state() != QProcess.ProcessState.NotRunning:
            self._process.kill()

    def _detail(self, running: bool) -> str:
        if self._starting:
            return "正在启动本窗口管理的 NapCat 进程…"
        if running and self._owned:
            return "本窗口启动的 NapCat 进程正在运行。"
        return "本窗口当前未持有 NapCat 进程。"

    def _publish(self, detail: str | None = None) -> None:
        if self._listener is None:
            return
        current = self.snapshot()
        if detail is not None:
            current = ProcessSnapshot(
                owned=current.owned,
                running=current.running,
                starting=current.starting,
                detail=detail,
            )
        self._listener(current)


def resolve_napcat_launch(runtime_dir: str) -> NapCatLaunchCommand:
    """解析受支持的 NapCat 启动器，但不执行它。"""

    if os.name != "nt":
        raise ValueError("NapCat 本地启动目前仅支持 Windows。")
    if not isinstance(runtime_dir, str) or not runtime_dir.strip():
        raise ValueError("NapCat 运行目录不能为空。")
    candidate = Path(runtime_dir.strip())
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    if candidate.is_symlink():
        raise ValueError("NapCat 运行目录不能是符号链接。")
    try:
        runtime = candidate.resolve(strict=True)
    except OSError:
        raise ValueError("NapCat 运行目录不存在。") from None
    if not runtime.is_dir():
        raise ValueError("NapCat 运行目录必须是目录。")
    launchers = (runtime / "launcher-user.bat", runtime / "napcat.bat")
    available = [path for path in launchers if path.is_file() and not path.is_symlink()]
    if len(available) != 1:
        raise ValueError("NapCat 目录必须包含一个受支持且非链接的启动脚本。")
    command_processor = os.environ.get("COMSPEC", "cmd.exe")
    return NapCatLaunchCommand(
        program=command_processor,
        arguments=("/d", "/c", os.fspath(available[0])),
        working_directory=os.fspath(runtime),
    )
