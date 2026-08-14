"""本机集成的显式、非交易 GUI 控制面。

所有凭据库和网络操作均交给后台线程。NapCat 通过异步 ``QProcess`` 启动；
进程控制器明确拒绝停止并非由本窗口启动的进程。
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypeVar, cast
from urllib.parse import urlsplit

import httpx
from PySide6.QtCore import (
    QObject,
    QProcess,
    QRunnable,
    QThreadPool,
    QTimer,
    QUrl,
    Signal,
)
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from gribuki_trade.adapters.llm import (
    OPENAI_API_KEY_SECRET,
    DeepSeekHealthClient,
    DeepSeekHealthErrorCode,
)
from gribuki_trade.adapters.llm.deepseek_chat import (
    DEEPSEEK_API_KEY_SECRET,
    DEFAULT_DEEPSEEK_MODEL,
)
from gribuki_trade.adapters.notifiers.onebot import (
    NAPCAT_ACCESS_TOKEN_SECRET,
    OneBotConfig,
    OneBotError,
    OneBotNotifier,
)
from gribuki_trade.napcat_setup import (
    NAPCAT_WEBUI_TOKEN_SECRET,
    NapCatSetupResult,
    configure_portable_napcat_runtime,
)
from gribuki_trade.runtime.integration_settings import (
    DEFAULT_NAPCAT_RUNTIME,
    DEFAULT_NAPCAT_WEBUI_URL,
    DEFAULT_ONEBOT_URL,
    IntegrationSettingsError,
    IntegrationSettingsStore,
)
from gribuki_trade.runtime.integration_settings import (
    validate_loopback_origin as _validate_runtime_loopback_origin,
)
from gribuki_trade.runtime.integration_settings import (
    validate_model_id as _validate_runtime_model_id,
)
from gribuki_trade.security import KeyringSecretProvider, SecretProvider
from gribuki_trade.security.config import SecretValue

_IMPLEMENTATION_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._:/+-]{0,79}\Z")


@dataclass(frozen=True, slots=True)
class NapCatHealth:
    """OneBot 只读探测返回的脱敏状态。"""

    configured: bool
    reachable: bool
    logged_in: bool | None
    implementation: str | None
    detail: str


@dataclass(frozen=True, slots=True)
class DeepSeekHealth:
    """脱敏后的 LLM 凭据与模型可用状态。"""

    configured: bool
    reachable: bool
    selected_model: str
    selected_model_available: bool
    available_models: tuple[str, ...]
    detail: str


class IntegrationGateway(Protocol):
    """阻塞式集成操作；调用方必须放到 GUI 线程之外。"""

    def check_napcat(self, base_url: str) -> NapCatHealth: ...

    def check_llm(self, provider: str, selected_model: str) -> DeepSeekHealth: ...

    def configure_napcat_runtime(
        self,
        runtime_dir: str,
        onebot_url: str,
        webui_url: str,
        access_token: str,
        *,
        force: bool,
    ) -> NapCatSetupResult: ...

    def get_napcat_webui_token(self) -> str: ...

    def save_llm_key(self, provider: str, api_key: str) -> None: ...


class IntegrationPreferences(Protocol):
    """GUI 与生产命令共享的非秘密本机配置。"""

    def get(self, name: str, default: str) -> str: ...

    def set(self, name: str, value: str) -> None: ...

    def set_many(self, values: Mapping[str, str]) -> None: ...


_T = TypeVar("_T")


class BackgroundExecutor(Protocol):
    """在不占用 Qt GUI 线程的情况下提交一次操作。"""

    def submit(
        self,
        operation: Callable[[], _T],
        on_success: Callable[[_T], None],
        on_failure: Callable[[str], None],
    ) -> None: ...


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


class QtIntegrationPreferences:
    """把路径、loopback origin 和模型 ID 写入共享运行时 JSON。"""

    def __init__(self, store: IntegrationSettingsStore | None = None) -> None:
        self._store = store or IntegrationSettingsStore()

    def get(self, name: str, default: str) -> str:
        try:
            settings = self._store.load()
        except IntegrationSettingsError:
            return default
        value = getattr(settings, name, default)
        return value if isinstance(value, str) else default

    def set(self, name: str, value: str) -> None:
        try:
            self._store.set(name, value)
        except (IntegrationSettingsError, TypeError, ValueError):
            raise RuntimeError("共享集成配置未能保存。") from None

    def set_many(self, values: Mapping[str, str]) -> None:
        try:
            self._store.update(values)
        except (IntegrationSettingsError, TypeError, ValueError):
            raise RuntimeError("共享集成配置未能保存。") from None


class DefaultIntegrationGateway:
    """由操作系统凭据库和只读健康客户端支撑的生产网关。"""

    def __init__(self, secret_provider: SecretProvider | None = None) -> None:
        self._secrets = secret_provider or KeyringSecretProvider()

    def check_napcat(self, base_url: str) -> NapCatHealth:
        checked_url = validate_loopback_origin(base_url, label="OneBot 地址")
        try:
            token = self._secrets.get_secret(NAPCAT_ACCESS_TOKEN_SECRET)
        except Exception:
            return NapCatHealth(False, False, None, None, "无法读取系统凭据库。")
        if not token:
            return NapCatHealth(False, False, None, None, "尚未在系统凭据库配置 OneBot 令牌。")

        async def probe() -> NapCatHealth:
            config = OneBotConfig(access_token=token, base_url=checked_url)
            try:
                async with OneBotNotifier(config) as notifier:
                    status = await notifier.get_status()
                    implementation: str | None = None
                    try:
                        version = await notifier.get_version_info()
                    except OneBotError:
                        version = {}
                    candidate = version.get("app_name")
                    if (
                        isinstance(candidate, str)
                        and _IMPLEMENTATION_ID.fullmatch(candidate.strip()) is not None
                    ):
                        implementation = candidate.strip()
            except OneBotError as error:
                return NapCatHealth(
                    True,
                    False,
                    None,
                    None,
                    _onebot_failure_text(error.code),
                )
            online = status.get("online")
            logged_in = online if isinstance(online, bool) else None
            if logged_in is True:
                detail = "OneBot 可达，QQ 已登录。"
            elif logged_in is False:
                detail = "OneBot 可达，QQ 尚未登录。"
            else:
                detail = "OneBot 可达，但未返回明确的登录状态。"
            return NapCatHealth(True, True, logged_in, implementation, detail)

        try:
            return asyncio.run(probe())
        except Exception:
            return NapCatHealth(True, False, None, None, "NapCat 状态检查失败；详细信息已隐藏。")

    def check_llm(self, provider: str, selected_model: str) -> DeepSeekHealth:
        checked_provider = _validate_llm_provider(provider)
        if checked_provider == "deepseek":
            return self._check_deepseek(selected_model)
        return self._check_openai(selected_model)

    def _check_deepseek(self, selected_model: str) -> DeepSeekHealth:
        checked_model = validate_model_id(selected_model)
        try:
            api_key = self._secrets.get_secret(DEEPSEEK_API_KEY_SECRET)
        except Exception:
            return DeepSeekHealth(
                False,
                False,
                checked_model,
                False,
                (),
                "无法读取系统凭据库。",
            )
        if not api_key:
            return DeepSeekHealth(
                False,
                False,
                checked_model,
                False,
                (),
                "尚未在系统凭据库配置 DeepSeek API Key。",
            )
        try:
            result = asyncio.run(DeepSeekHealthClient(SecretValue(api_key)).check())
        except Exception:
            return DeepSeekHealth(
                True,
                False,
                checked_model,
                False,
                (),
                "DeepSeek 健康检查失败；详细信息已隐藏。",
            )
        if result.ok:
            available = result.available_model_ids
            selected_available = checked_model in available
            detail = (
                "API 可用，所选模型可用。"
                if selected_available
                else "API 可用，但所选模型不在服务端模型清单中。"
            )
            return DeepSeekHealth(
                True,
                True,
                checked_model,
                selected_available,
                available,
                detail,
            )
        return DeepSeekHealth(
            True,
            False,
            checked_model,
            False,
            (),
            _deepseek_failure_text(result.error_code),
        )

    def _check_openai(self, selected_model: str) -> DeepSeekHealth:
        """通过官方模型列表接口核对凭据与所选模型，不发起语义生成。"""

        checked_model = validate_model_id(selected_model)
        try:
            api_key = self._secrets.get_secret(OPENAI_API_KEY_SECRET)
        except Exception:
            return DeepSeekHealth(False, False, checked_model, False, (), "无法读取系统凭据库。")
        if not api_key:
            return DeepSeekHealth(
                False,
                False,
                checked_model,
                False,
                (),
                "尚未在系统凭据库配置 OpenAI API Key。",
            )
        try:
            response = httpx.get(
                "https://api.openai.com/v1/models",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=15.0,
            )
            response.raise_for_status()
            payload = response.json()
            raw_models = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(raw_models, list):
                raise ValueError("model response is invalid")
            available = tuple(
                sorted(
                    {
                        str(item["id"])
                        for item in raw_models
                        if isinstance(item, dict)
                        and isinstance(item.get("id"), str)
                        and _validate_optional_model_id(str(item["id"])) is not None
                    }
                )
            )
        except Exception:
            return DeepSeekHealth(
                True,
                False,
                checked_model,
                False,
                (),
                "OpenAI 健康检查失败；详细信息已隐藏。",
            )
        selected_available = checked_model in available
        return DeepSeekHealth(
            True,
            True,
            checked_model,
            selected_available,
            available,
            (
                "API 可用，所选模型可用。"
                if selected_available
                else "API 可用，但所选模型不在服务端模型清单中。"
            ),
        )

    def configure_napcat_runtime(
        self,
        runtime_dir: str,
        onebot_url: str,
        webui_url: str,
        access_token: str,
        *,
        force: bool,
    ) -> NapCatSetupResult:
        """把 GUI 所选端口和令牌事务性同步到 runtime 与系统凭据库。"""

        checked_token = _validate_access_token(access_token)
        onebot_port = _napcat_http_port(onebot_url, label="OneBot 地址")
        webui_port = _napcat_http_port(webui_url, label="WebUI 地址")
        if onebot_port == webui_port:
            raise ValueError("OneBot 与 WebUI 端口必须不同。")
        candidate = Path(runtime_dir.strip())
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        try:
            existing_webui_token = self._secrets.get_secret(NAPCAT_WEBUI_TOKEN_SECRET)
            return configure_portable_napcat_runtime(
                candidate.absolute(),
                self._secrets,
                onebot_port=onebot_port,
                webui_port=webui_port,
                onebot_token=checked_token,
                webui_token=existing_webui_token,
                force=force,
            )
        except (FileExistsError, TypeError, ValueError):
            raise
        except Exception:
            raise RuntimeError("NapCat 本地配置未能安全保存；原配置已尽力恢复。") from None

    def get_napcat_webui_token(self) -> str:
        """读取仅供本机 WebUI 登录使用的令牌；调用方不得渲染或记录它。"""

        try:
            token = self._secrets.get_secret(NAPCAT_WEBUI_TOKEN_SECRET)
        except Exception:
            raise RuntimeError("系统凭据库未能读取 WebUI 令牌。") from None
        if token is None:
            raise RuntimeError("尚未配置 WebUI 令牌；请先安全配置 NapCat。")
        return _validate_access_token(token)

    def save_llm_key(self, provider: str, api_key: str) -> None:
        checked_provider = _validate_llm_provider(provider)
        checked_key = _validate_api_key(api_key)
        try:
            secret_name = (
                DEEPSEEK_API_KEY_SECRET if checked_provider == "deepseek" else OPENAI_API_KEY_SECRET
            )
            self._secrets.set_secret(secret_name, checked_key)
        except Exception:
            raise RuntimeError("系统凭据库未能保存 API Key。") from None


class _JobSignals(QObject):
    succeeded = Signal(object)
    failed = Signal(str)
    finished = Signal()


class _BackgroundJob(QRunnable):
    def __init__(self, operation: Callable[[], object]) -> None:
        super().__init__()
        self.signals = _JobSignals()
        self._operation = operation

    def run(self) -> None:
        try:
            result = self._operation()
        except Exception:
            self.signals.failed.emit("后台操作失败；详细信息已隐藏。")
        else:
            self.signals.succeeded.emit(result)
        finally:
            self.signals.finished.emit()


class QtBackgroundExecutor(QObject):
    """在任务完成前持续持有任务的小型 ``QThreadPool`` 适配器。"""

    def __init__(self, parent: QObject | None = None, pool: QThreadPool | None = None) -> None:
        super().__init__(parent)
        self._pool = pool or QThreadPool.globalInstance()
        self._jobs: set[_BackgroundJob] = set()

    def submit(
        self,
        operation: Callable[[], _T],
        on_success: Callable[[_T], None],
        on_failure: Callable[[str], None],
    ) -> None:
        job = _BackgroundJob(cast(Callable[[], object], operation))

        def deliver(value: object) -> None:
            on_success(cast(_T, value))

        def forget() -> None:
            self._jobs.discard(job)

        job.signals.succeeded.connect(deliver)
        job.signals.failed.connect(on_failure)
        job.signals.finished.connect(forget)
        self._jobs.add(job)
        self._pool.start(job)


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


@dataclass(slots=True)
class IntegrationDependencies:
    """生产 GUI 与离屏测试使用的可注入边界。"""

    gateway: IntegrationGateway
    preferences: IntegrationPreferences
    executor: BackgroundExecutor
    process: NapCatProcessControl
    open_browser: Callable[[str], bool]


class IntegrationsPanel(QWidget):
    """管理 NapCat 登录可见性和 DeepSeek 配置，绝不执行交易。"""

    napcat_summary_changed = Signal(str)

    def __init__(
        self,
        dependencies: IntegrationDependencies | None = None,
        *,
        auto_refresh: bool = True,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        if dependencies is None:
            dependencies = IntegrationDependencies(
                gateway=DefaultIntegrationGateway(),
                preferences=QtIntegrationPreferences(),
                executor=QtBackgroundExecutor(self),
                process=QtNapCatProcessController(self),
                open_browser=_open_browser,
            )
        self._dependencies = dependencies
        self._napcat_pending = False
        self._napcat_save_pending = False
        self._deepseek_pending = False
        self._save_pending = False
        self._build_ui()
        self._dependencies.process.set_listener(self._show_process_state)

        self._refresh_timer = QTimer(self)
        self._refresh_timer.setInterval(30_000)
        self._refresh_timer.timeout.connect(self.refresh_all)
        if auto_refresh:
            self._refresh_timer.start()
            QTimer.singleShot(0, self.refresh_all)

    def shutdown(self) -> None:
        """停止周期探测；进程生命周期仍由用户显式操作。"""

        self._refresh_timer.stop()

    def refresh_all(self) -> None:
        self._refresh_napcat()
        self._refresh_llm()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 15, 18, 18)
        layout.setSpacing(14)

        boundary = QLabel(
            "集成管理仅负责本机登录、状态监看和模型凭据配置；不连接券商，不提交实盘或模拟盘订单。"
        )
        boundary.setWordWrap(True)
        boundary.setStyleSheet(
            "background:#153d35; color:#7de0bb; border:1px solid #28725f;"
            "border-radius:7px; padding:10px;"
        )
        layout.addWidget(boundary)

        napcat = QGroupBox("NapCat / OneBot 本地登录与监看")
        napcat_layout = QVBoxLayout(napcat)
        napcat_form = QFormLayout()
        self._runtime_path = QLineEdit(
            self._dependencies.preferences.get("napcat_runtime", _default_runtime_path())
        )
        self._runtime_path.setObjectName("napcatRuntimePath")
        runtime_row = QHBoxLayout()
        runtime_row.addWidget(self._runtime_path, 1)
        browse = QPushButton("选择目录")
        browse.setObjectName("napcatBrowseButton")
        browse.clicked.connect(self._browse_runtime)
        runtime_row.addWidget(browse)
        napcat_form.addRow("本地运行目录", runtime_row)

        self._onebot_url = QLineEdit(
            self._dependencies.preferences.get("onebot_url", DEFAULT_ONEBOT_URL)
        )
        self._onebot_url.setObjectName("onebotBaseUrl")
        napcat_form.addRow("OneBot 地址", self._onebot_url)
        self._onebot_token = QLineEdit()
        self._onebot_token.setObjectName("onebotAccessToken")
        self._onebot_token.setEchoMode(QLineEdit.EchoMode.Password)
        self._onebot_token.setPlaceholderText("留空不会覆盖系统凭据库中的现有令牌")
        napcat_form.addRow("OneBot 令牌", self._onebot_token)
        self._webui_url = QLineEdit(
            self._dependencies.preferences.get("napcat_webui_url", DEFAULT_NAPCAT_WEBUI_URL)
        )
        self._webui_url.setObjectName("napcatWebUiUrl")
        napcat_form.addRow("登录 WebUI", self._webui_url)
        self._napcat_force_config = QCheckBox("明确覆盖已有 NapCat 本机配置")
        self._napcat_force_config.setObjectName("napcatForceConfig")
        self._napcat_force_config.setToolTip(
            "默认拒绝覆盖已有 onebot11.json/webui.json；仅在确认同步端口和令牌时勾选。"
        )
        napcat_form.addRow("配置保护", self._napcat_force_config)
        napcat_layout.addLayout(napcat_form)

        self._napcat_status = QLabel("等待后台状态检查…")
        self._napcat_status.setObjectName("napcatIntegrationStatus")
        self._napcat_status.setWordWrap(True)
        self._napcat_status.setStyleSheet("color:#e8b967")
        napcat_layout.addWidget(self._napcat_status)
        self._napcat_process_status = QLabel()
        self._napcat_process_status.setObjectName("napcatProcessStatus")
        self._napcat_process_status.setWordWrap(True)
        napcat_layout.addWidget(self._napcat_process_status)

        napcat_actions = QHBoxLayout()
        self._napcat_refresh = QPushButton("刷新状态")
        self._napcat_refresh.setObjectName("napcatRefreshButton")
        self._napcat_refresh.clicked.connect(self._refresh_napcat)
        napcat_actions.addWidget(self._napcat_refresh)
        self._save_napcat_token = QPushButton("安全配置 NapCat 与令牌")
        self._save_napcat_token.setObjectName("napcatSaveTokenButton")
        self._save_napcat_token.clicked.connect(self._save_onebot_token)
        napcat_actions.addWidget(self._save_napcat_token)
        self._napcat_start = QPushButton("启动本地 NapCat")
        self._napcat_start.setObjectName("napcatStartButton")
        self._napcat_start.clicked.connect(self._start_napcat)
        napcat_actions.addWidget(self._napcat_start)
        self._open_webui = QPushButton("打开 WebUI 登录")
        self._open_webui.setObjectName("napcatOpenWebUiButton")
        self._open_webui.clicked.connect(self._open_napcat_webui)
        napcat_actions.addWidget(self._open_webui)
        self._copy_webui_token = QPushButton("复制 WebUI 令牌")
        self._copy_webui_token.setObjectName("napcatCopyWebUiTokenButton")
        self._copy_webui_token.clicked.connect(self._copy_napcat_webui_token)
        napcat_actions.addWidget(self._copy_webui_token)
        self._napcat_stop = QPushButton("停止本窗口启动的进程")
        self._napcat_stop.setObjectName("napcatStopButton")
        self._napcat_stop.clicked.connect(self._stop_napcat)
        napcat_actions.addWidget(self._napcat_stop)
        napcat_actions.addStretch()
        napcat_layout.addLayout(napcat_actions)
        layout.addWidget(napcat)

        llm = QGroupBox("LLM provider、模型与凭据")
        llm_layout = QVBoxLayout(llm)
        llm_form = QFormLayout()
        self._llm_provider = QComboBox()
        self._llm_provider.setObjectName("llmProvider")
        self._llm_provider.addItem("DeepSeek", "deepseek")
        self._llm_provider.addItem("OpenAI", "openai")
        selected_provider = _validate_llm_provider(
            self._dependencies.preferences.get("llm_provider", "deepseek")
        )
        self._llm_provider.setCurrentIndex(0 if selected_provider == "deepseek" else 1)
        llm_form.addRow("默认 provider", self._llm_provider)
        self._api_key = QLineEdit()
        self._api_key.setObjectName("deepseekApiKey")
        self._api_key.setEchoMode(QLineEdit.EchoMode.Password)
        self._api_key.setPlaceholderText("留空不会覆盖系统凭据库中的现有 Key")
        self._api_key_label = QLabel("API Key")
        llm_form.addRow(self._api_key_label, self._api_key)
        self._model = QComboBox()
        self._model.setObjectName("deepseekModel")
        self._model.setEditable(True)
        self._model.currentTextChanged.connect(self._store_model_if_valid)
        llm_form.addRow("默认模型", self._model)
        self._load_provider_model(selected_provider)
        llm_layout.addLayout(llm_form)

        self._deepseek_status = QLabel("等待后台状态检查…")
        self._deepseek_status.setObjectName("deepseekIntegrationStatus")
        self._deepseek_status.setWordWrap(True)
        self._deepseek_status.setStyleSheet("color:#e8b967")
        llm_layout.addWidget(self._deepseek_status)
        llm_actions = QHBoxLayout()
        self._save_key = QPushButton("保存 Key 到系统凭据库")
        self._save_key.setObjectName("deepseekSaveKeyButton")
        self._save_key.clicked.connect(self._save_llm_key)
        llm_actions.addWidget(self._save_key)
        self._deepseek_refresh = QPushButton("后台健康检查")
        self._deepseek_refresh.setObjectName("deepseekRefreshButton")
        self._deepseek_refresh.clicked.connect(self._refresh_llm)
        llm_actions.addWidget(self._deepseek_refresh)
        llm_actions.addStretch()
        llm_layout.addLayout(llm_actions)
        layout.addWidget(llm)
        self._llm_provider.currentIndexChanged.connect(self._provider_changed)
        layout.addStretch()

    def _browse_runtime(self) -> None:
        selected = QFileDialog.getExistingDirectory(
            self,
            "选择 NapCat 本地运行目录",
            self._runtime_path.text().strip(),
        )
        if selected:
            self._runtime_path.setText(selected)
            self._dependencies.preferences.set("napcat_runtime", selected)

    def _refresh_napcat(self) -> None:
        if self._napcat_pending:
            return
        try:
            base_url = validate_loopback_origin(self._onebot_url.text(), label="OneBot 地址")
        except ValueError as error:
            self._show_napcat_failure(str(error))
            return
        self._dependencies.preferences.set("onebot_url", base_url)
        self._napcat_pending = True
        self._napcat_refresh.setEnabled(False)
        self._napcat_status.setText("正在后台检查 OneBot 与 QQ 登录状态…")
        self._dependencies.executor.submit(
            lambda: self._dependencies.gateway.check_napcat(base_url),
            self._show_napcat_health,
            self._show_napcat_failure,
        )

    def _show_napcat_health(self, health: NapCatHealth) -> None:
        self._napcat_pending = False
        self._napcat_refresh.setEnabled(True)
        prefix = "已配置" if health.configured else "未配置"
        implementation = f" · {health.implementation}" if health.implementation else ""
        rendered = f"{prefix} · {health.detail}{implementation}"
        self._napcat_status.setText(rendered)
        color = "#64e0ae" if health.reachable and health.logged_in is True else "#e8b967"
        self._napcat_status.setStyleSheet(f"color:{color}")
        self.napcat_summary_changed.emit(rendered)

    def _show_napcat_failure(self, message: str) -> None:
        self._napcat_pending = False
        self._napcat_refresh.setEnabled(True)
        safe = _safe_ui_message(message, "NapCat 状态检查失败。")
        self._napcat_status.setText(safe)
        self._napcat_status.setStyleSheet("color:#ef8d8d")
        self.napcat_summary_changed.emit(safe)

    def _save_onebot_token(self) -> None:
        """在后台同步 runtime 配置与两枚本机秘密，不允许只改客户端 token。"""

        if self._napcat_save_pending:
            return
        try:
            token = _validate_access_token(self._onebot_token.text())
            base_url = validate_loopback_origin(self._onebot_url.text(), label="OneBot 地址")
            webui_url = validate_loopback_origin(self._webui_url.text(), label="WebUI 地址")
            _napcat_http_port(base_url, label="OneBot 地址")
            _napcat_http_port(webui_url, label="WebUI 地址")
            runtime = self._runtime_path.text().strip()
            if not runtime or "\x00" in runtime:
                raise ValueError("NapCat 运行目录不能为空。")
            force = self._napcat_force_config.isChecked()
        except (RuntimeError, TypeError, ValueError) as error:
            self._show_napcat_failure(str(error))
            return
        self._napcat_save_pending = True
        self._save_napcat_token.setEnabled(False)
        self._napcat_status.setText("正在后台同步 NapCat 配置与系统凭据库…")

        def saved(result: NapCatSetupResult) -> None:
            self._napcat_save_pending = False
            self._save_napcat_token.setEnabled(True)
            self._onebot_token.clear()
            self._napcat_force_config.setChecked(False)
            try:
                self._dependencies.preferences.set_many(
                    {
                        "napcat_runtime": os.fspath(result.runtime_dir),
                        "napcat_webui_url": webui_url,
                        "onebot_url": base_url,
                    }
                )
            except RuntimeError as error:
                self._show_napcat_failure(str(error))
                return
            self._runtime_path.setText(os.fspath(result.runtime_dir))
            self._napcat_status.setText(
                "NapCat 本机配置、OneBot/WebUI 令牌与共享运行时地址已同步；输入框已清空。"
            )
            self._napcat_status.setStyleSheet("color:#64e0ae")
            self._refresh_napcat()

        def failed(message: str) -> None:
            self._napcat_save_pending = False
            self._save_napcat_token.setEnabled(True)
            self._show_napcat_failure(message)

        self._dependencies.executor.submit(
            lambda: self._dependencies.gateway.configure_napcat_runtime(
                runtime,
                base_url,
                webui_url,
                token,
                force=force,
            ),
            saved,
            failed,
        )

    def _copy_napcat_webui_token(self) -> None:
        """把 WebUI token 仅写入系统剪贴板，不在标签、日志或共享 JSON 中渲染。"""

        self._copy_webui_token.setEnabled(False)
        self._napcat_process_status.setText("正在后台读取本机 WebUI 令牌…")

        def copied(token: str) -> None:
            self._copy_webui_token.setEnabled(True)
            QApplication.clipboard().setText(token)
            self._napcat_process_status.setText(
                "WebUI 令牌已复制到系统剪贴板；请仅粘贴到本机 NapCat 登录页。"
            )

        def failed(message: str) -> None:
            self._copy_webui_token.setEnabled(True)
            self._napcat_process_status.setText(
                _safe_ui_message(message, "系统凭据库未能读取 WebUI 令牌。")
            )

        self._dependencies.executor.submit(
            self._dependencies.gateway.get_napcat_webui_token,
            copied,
            failed,
        )

    def _start_napcat(self) -> None:
        if self._dependencies.process.snapshot().owned:
            self._show_process_state(self._dependencies.process.snapshot())
            return
        runtime = self._runtime_path.text().strip()
        self._dependencies.preferences.set("napcat_runtime", runtime)
        self._napcat_start.setEnabled(False)
        self._napcat_process_status.setText("正在后台校验本地 NapCat 运行目录…")

        def started(command: NapCatLaunchCommand) -> None:
            self._napcat_start.setEnabled(True)
            if not self._dependencies.process.start(command):
                self._napcat_process_status.setText("已有本窗口管理的进程正在启动或运行。")

        def failed(message: str) -> None:
            self._napcat_start.setEnabled(True)
            self._napcat_process_status.setText(_safe_ui_message(message, "NapCat 启动准备失败。"))

        self._dependencies.executor.submit(
            lambda: resolve_napcat_launch(runtime),
            started,
            failed,
        )

    def _stop_napcat(self) -> None:
        if not self._dependencies.process.stop_owned():
            self._napcat_process_status.setText(
                "拒绝停止：本窗口未启动 NapCat；外部进程不会被操作。"
            )

    def _show_process_state(self, snapshot: ProcessSnapshot) -> None:
        self._napcat_process_status.setText(snapshot.detail)
        self._napcat_stop.setEnabled(snapshot.owned)
        self._napcat_start.setEnabled(not snapshot.owned)
        if snapshot.running:
            QTimer.singleShot(1_500, self._refresh_napcat)

    def _open_napcat_webui(self) -> None:
        try:
            url = validate_loopback_origin(self._webui_url.text(), label="WebUI 地址")
        except ValueError as error:
            self._napcat_process_status.setText(str(error))
            return
        self._dependencies.preferences.set("napcat_webui_url", url)
        if not self._dependencies.open_browser(url):
            self._napcat_process_status.setText("系统未能打开本机 WebUI。")
            return
        self._napcat_process_status.setText("已请求系统浏览器打开本机 WebUI；请在页面内登录 QQ。")

    def _store_model_if_valid(self, value: str) -> None:
        try:
            model = validate_model_id(value)
        except ValueError:
            return
        self._dependencies.preferences.set(self._model_setting_name(), model)

    def _provider(self) -> str:
        return _validate_llm_provider(str(self._llm_provider.currentData()))

    def _model_setting_name(self) -> str:
        return f"{self._provider()}_model"

    def _provider_changed(self, _index: int) -> None:
        provider = self._provider()
        self._dependencies.preferences.set("llm_provider", provider)
        self._load_provider_model(provider)
        self._api_key.clear()
        self._deepseek_status.setText(f"已切换到 {provider}；请保存对应凭据或执行后台健康检查。")
        self._deepseek_status.setStyleSheet("color:#e8b967")

    def _load_provider_model(self, provider: str) -> None:
        """切换 provider 时恢复各自模型，不让一个 provider 覆盖另一个。"""

        checked = _validate_llm_provider(provider)
        defaults = {
            "deepseek": (DEFAULT_DEEPSEEK_MODEL, ("deepseek-v4-flash", "deepseek-v4-pro")),
            "openai": ("gpt-5.6", ("gpt-5.6", "gpt-5.6-terra", "gpt-5.6-luna")),
        }
        default, choices = defaults[checked]
        selected = self._dependencies.preferences.get(f"{checked}_model", default)
        self._model.blockSignals(True)
        self._model.clear()
        self._model.addItems(list(choices))
        if self._model.findText(selected) < 0:
            self._model.addItem(selected)
        self._model.setCurrentText(selected)
        self._model.blockSignals(False)
        self._api_key_label.setText(f"{checked} API Key")

    def _save_llm_key(self) -> None:
        if self._save_pending:
            return
        value = self._api_key.text()
        try:
            checked = _validate_api_key(value)
            model = validate_model_id(self._model.currentText())
            provider = self._provider()
        except ValueError as error:
            self._deepseek_status.setText(str(error))
            self._deepseek_status.setStyleSheet("color:#ef8d8d")
            return
        self._dependencies.preferences.set("llm_provider", provider)
        self._dependencies.preferences.set(self._model_setting_name(), model)
        self._save_pending = True
        self._save_key.setEnabled(False)
        self._deepseek_status.setText("正在后台写入系统凭据库…")

        def saved(_unused: None) -> None:
            self._save_pending = False
            self._save_key.setEnabled(True)
            self._api_key.clear()
            self._deepseek_status.setText("API Key 已保存到系统凭据库；输入框已清空。")
            self._deepseek_status.setStyleSheet("color:#64e0ae")
            self._refresh_llm()

        def failed(message: str) -> None:
            self._save_pending = False
            self._save_key.setEnabled(True)
            self._deepseek_status.setText(_safe_ui_message(message, "系统凭据库未能保存 API Key。"))
            self._deepseek_status.setStyleSheet("color:#ef8d8d")

        self._dependencies.executor.submit(
            lambda: self._dependencies.gateway.save_llm_key(provider, checked),
            saved,
            failed,
        )

    def _refresh_llm(self) -> None:
        if self._deepseek_pending:
            return
        try:
            model = validate_model_id(self._model.currentText())
            provider = self._provider()
        except ValueError as error:
            self._deepseek_status.setText(str(error))
            self._deepseek_status.setStyleSheet("color:#ef8d8d")
            return
        self._dependencies.preferences.set("llm_provider", provider)
        self._dependencies.preferences.set(self._model_setting_name(), model)
        self._deepseek_pending = True
        self._deepseek_refresh.setEnabled(False)
        self._deepseek_status.setText("正在后台检查凭据与模型可用性…")
        self._dependencies.executor.submit(
            lambda: self._dependencies.gateway.check_llm(provider, model),
            self._show_deepseek_health,
            self._show_deepseek_failure,
        )

    def _show_deepseek_health(self, health: DeepSeekHealth) -> None:
        self._deepseek_pending = False
        self._deepseek_refresh.setEnabled(True)
        prefix = "Key 已配置" if health.configured else "Key 未配置"
        models = ""
        if health.available_models:
            models = " · 可用模型：" + "、".join(health.available_models[:8])
        self._deepseek_status.setText(f"{prefix} · {health.detail}{models}")
        color = "#64e0ae" if health.reachable and health.selected_model_available else "#e8b967"
        self._deepseek_status.setStyleSheet(f"color:{color}")

    def _show_deepseek_failure(self, message: str) -> None:
        self._deepseek_pending = False
        self._deepseek_refresh.setEnabled(True)
        self._deepseek_status.setText(_safe_ui_message(message, "DeepSeek 健康检查失败。"))
        self._deepseek_status.setStyleSheet("color:#ef8d8d")


def validate_model_id(value: str) -> str:
    """返回适合本机共享配置的有界 provider 模型 ID。"""

    try:
        return _validate_runtime_model_id(value)
    except TypeError:
        raise TypeError("模型 ID 必须是文本。") from None
    except ValueError:
        raise ValueError("模型 ID 格式无效。") from None


def _validate_optional_model_id(value: str) -> str | None:
    try:
        return validate_model_id(value)
    except (TypeError, ValueError):
        return None


def _validate_llm_provider(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("LLM provider 必须是文本。")
    checked = value.strip().casefold()
    if checked not in {"deepseek", "openai"}:
        raise ValueError("LLM provider 只支持 DeepSeek 或 OpenAI。")
    return checked


def validate_loopback_origin(value: str, *, label: str) -> str:
    """校验不带凭据的 HTTP(S) loopback origin。"""

    try:
        return _validate_runtime_loopback_origin(value, label=label)
    except TypeError:
        raise TypeError(f"{label}必须是文本。") from None
    except ValueError:
        raise ValueError(f"{label}必须是不含凭据、路径或查询参数的本机地址。") from None


def _napcat_http_port(value: str, *, label: str) -> int:
    checked = validate_loopback_origin(value, label=label)
    parsed = urlsplit(checked)
    if parsed.scheme != "http":
        raise ValueError(f"{label}必须使用本机 HTTP。")
    return parsed.port or 80


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


def _validate_api_key(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("API Key 必须是文本。")
    if value != value.strip() or any(character.isspace() for character in value):
        raise ValueError("API Key 不能包含首尾或内部空白。")
    if not 8 <= len(value) <= 512:
        raise ValueError("API Key 长度无效。")
    return value


def _validate_access_token(value: str) -> str:
    """校验 OneBot token；不假设具体前缀，也不把值写入错误文本。"""

    if not isinstance(value, str):
        raise TypeError("OneBot 令牌必须是文本。")
    if value != value.strip() or any(character.isspace() for character in value):
        raise ValueError("OneBot 令牌不能包含首尾或内部空白。")
    if not 8 <= len(value) <= 512:
        raise ValueError("OneBot 令牌长度无效。")
    return value


def _onebot_failure_text(code: str) -> str:
    if code in {"transport_timeout", "transport_error"}:
        return "OneBot 当前不可达；请确认 NapCat 已启动且端口正确。"
    if code == "authentication_rejected":
        return "OneBot 拒绝认证；请重新配置本地令牌。"
    return "OneBot 返回异常；详细信息已隐藏。"


def _deepseek_failure_text(code: DeepSeekHealthErrorCode | None) -> str:
    if code is None:
        return "DeepSeek 健康检查失败。"
    return {
        DeepSeekHealthErrorCode.AUTHENTICATION_FAILED: "API Key 认证失败。",
        DeepSeekHealthErrorCode.INSUFFICIENT_BALANCE: "API 账户余额不足。",
        DeepSeekHealthErrorCode.RATE_LIMITED: "API 当前限流，请稍后重试。",
        DeepSeekHealthErrorCode.NETWORK_ERROR: "DeepSeek 当前不可达。",
        DeepSeekHealthErrorCode.INVALID_RESPONSE: "DeepSeek 返回了无法验证的响应。",
        DeepSeekHealthErrorCode.PROVIDER_ERROR: "DeepSeek 服务端返回异常。",
    }.get(code, "DeepSeek 健康检查失败。")


def _safe_ui_message(message: str, fallback: str) -> str:
    allowed = {
        "后台操作失败；详细信息已隐藏。",
        "系统凭据库未能保存 API Key。",
        "系统凭据库未能保存 OneBot 令牌。",
        "共享集成配置未能保存。",
        "NapCat 启动准备失败。",
        "NapCat 运行目录不存在。",
        "NapCat 运行目录必须是目录。",
        "NapCat 运行目录不能是符号链接。",
        "NapCat 目录必须包含一个受支持且非链接的启动脚本。",
        "NapCat 本地启动目前仅支持 Windows。",
    }
    return message if message in allowed else fallback


def _default_runtime_path() -> str:
    return os.fspath((Path.cwd() / DEFAULT_NAPCAT_RUNTIME).resolve())


def _open_browser(url: str) -> bool:
    return QDesktopServices.openUrl(QUrl(url))
