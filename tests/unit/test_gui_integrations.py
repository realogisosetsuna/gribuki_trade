"""NapCat 与 DeepSeek 图形界面显式集成控件的离屏测试。"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TypeVar

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import httpx
import pytest
from PySide6.QtWidgets import QCheckBox, QComboBox, QLabel, QLineEdit, QPushButton

from gribuki_trade.adapters.llm import OPENAI_API_KEY_SECRET
from gribuki_trade.adapters.notifiers.onebot import NAPCAT_ACCESS_TOKEN_SECRET
from gribuki_trade.gui import create_application
from gribuki_trade.gui import integrations as integrations_module
from gribuki_trade.gui.integrations import (
    DeepSeekHealth,
    DefaultIntegrationGateway,
    IntegrationDependencies,
    IntegrationsPanel,
    NapCatHealth,
    NapCatLaunchCommand,
    ProcessSnapshot,
    QtBackgroundExecutor,
    QtNapCatProcessController,
    resolve_napcat_launch,
    validate_loopback_origin,
)
from gribuki_trade.napcat_setup import (
    NAPCAT_WEBUI_TOKEN_SECRET,
    NapCatSetupResult,
)
from gribuki_trade.security import MemorySecretProvider

_T = TypeVar("_T")


class _InlineExecutor:
    def submit(
        self,
        operation: Callable[[], _T],
        on_success: Callable[[_T], None],
        on_failure: Callable[[str], None],
    ) -> None:
        try:
            result = operation()
        except Exception:
            on_failure("后台操作失败；详细信息已隐藏。")
        else:
            on_success(result)


class _Preferences:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get(self, name: str, default: str) -> str:
        return self.values.get(name, default)

    def set(self, name: str, value: str) -> None:
        self.values[name] = value

    def set_many(self, values: Mapping[str, str]) -> None:
        self.values.update(values)


class _Gateway:
    def __init__(self) -> None:
        self.napcat_urls: list[str] = []
        self.models: list[tuple[str, str]] = []
        self.saved_tokens: list[str] = []
        self.saved_keys: list[str] = []
        self.napcat_configurations: list[tuple[str, str, str, str, bool]] = []
        self.webui_token = "webui-test-token-that-must-not-be-rendered"

    def check_napcat(self, base_url: str) -> NapCatHealth:
        self.napcat_urls.append(base_url)
        return NapCatHealth(True, True, True, "NapCat.OneBot", "OneBot 可达，QQ 已登录。")

    def check_llm(self, provider: str, selected_model: str) -> DeepSeekHealth:
        self.models.append((provider, selected_model))
        return DeepSeekHealth(
            True,
            True,
            selected_model,
            True,
            (selected_model,),
            "API 可用，所选模型可用。",
        )

    def save_llm_key(self, provider: str, api_key: str) -> None:
        self.saved_keys.append(f"{provider}:{api_key}")

    def configure_napcat_runtime(
        self,
        runtime_dir: str,
        onebot_url: str,
        webui_url: str,
        access_token: str,
        *,
        force: bool,
    ) -> NapCatSetupResult:
        self.saved_tokens.append(access_token)
        self.napcat_configurations.append((runtime_dir, onebot_url, webui_url, access_token, force))
        runtime = Path(runtime_dir).absolute()
        return NapCatSetupResult(
            runtime_dir=runtime,
            onebot_config_path=runtime / "config" / "onebot11.json",
            webui_config_path=runtime / "config" / "webui.json",
            onebot_port=3000,
            webui_port=6099,
        )

    def get_napcat_webui_token(self) -> str:
        return self.webui_token


class _Process:
    def __init__(self) -> None:
        self.listener: Callable[[ProcessSnapshot], None] | None = None
        self.owned = False
        self.commands: list[NapCatLaunchCommand] = []
        self.stop_calls = 0

    def set_listener(self, listener: Callable[[ProcessSnapshot], None]) -> None:
        self.listener = listener
        listener(self.snapshot())

    def snapshot(self) -> ProcessSnapshot:
        return ProcessSnapshot(
            self.owned,
            self.owned,
            False,
            (
                "本窗口启动的 NapCat 进程正在运行。"
                if self.owned
                else "本窗口当前未持有 NapCat 进程。"
            ),
        )

    def start(self, command: NapCatLaunchCommand) -> bool:
        if self.owned:
            return False
        self.owned = True
        self.commands.append(command)
        assert self.listener is not None
        self.listener(self.snapshot())
        return True

    def stop_owned(self) -> bool:
        self.stop_calls += 1
        if not self.owned:
            return False
        self.owned = False
        assert self.listener is not None
        self.listener(self.snapshot())
        return True


def _panel() -> tuple[IntegrationsPanel, _Gateway, _Preferences, _Process, list[str]]:
    gateway = _Gateway()
    preferences = _Preferences()
    process = _Process()
    opened: list[str] = []
    dependencies = IntegrationDependencies(
        gateway=gateway,
        preferences=preferences,
        executor=_InlineExecutor(),
        process=process,
        open_browser=lambda url: not opened.append(url),
    )
    panel = IntegrationsPanel(dependencies, auto_refresh=False)
    return panel, gateway, preferences, process, opened


def test_health_controls_use_injected_background_boundaries() -> None:
    app = create_application(["gribuki-gui-integrations-test"])
    panel, gateway, preferences, _process, _opened = _panel()

    panel.refresh_all()
    app.processEvents()

    napcat = panel.findChild(QLabel, "napcatIntegrationStatus")
    deepseek = panel.findChild(QLabel, "deepseekIntegrationStatus")
    assert napcat is not None
    assert napcat.text() == "已配置 · OneBot 可达，QQ 已登录。 · NapCat.OneBot"
    assert deepseek is not None
    assert "Key 已配置" in deepseek.text()
    assert "所选模型可用" in deepseek.text()
    assert gateway.napcat_urls == ["http://127.0.0.1:3000"]
    assert gateway.models == [("deepseek", "deepseek-v4-flash")]
    assert preferences.values["onebot_url"] == "http://127.0.0.1:3000"

    panel.close()
    panel.deleteLater()
    app.processEvents()


def test_default_gateway_reports_missing_keyring_entries_without_network() -> None:
    gateway = DefaultIntegrationGateway(MemorySecretProvider())

    napcat = gateway.check_napcat("http://127.0.0.1:3000")
    deepseek = gateway.check_llm("deepseek", "deepseek-v4-flash")
    openai = gateway.check_llm("openai", "gpt-5.6")

    assert napcat.configured is False
    assert napcat.reachable is False
    assert "尚未" in napcat.detail
    assert deepseek.configured is False
    assert deepseek.reachable is False
    assert "尚未" in deepseek.detail
    assert openai.configured is False
    assert "尚未" in openai.detail


def test_openai_health_uses_model_list_without_semantic_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secrets = MemorySecretProvider()
    secrets.set_secret(OPENAI_API_KEY_SECRET, "openai-health-test-key")
    requested: list[str] = []

    def fake_get(url: str, **kwargs: object) -> httpx.Response:
        requested.append(url)
        headers = kwargs.get("headers")
        assert isinstance(headers, dict)
        assert headers["Authorization"].startswith("Bearer ")
        return httpx.Response(
            200,
            json={"data": [{"id": "gpt-5.6"}, {"id": "gpt-5.6-terra"}]},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(integrations_module.httpx, "get", fake_get)

    health = DefaultIntegrationGateway(secrets).check_llm("openai", "gpt-5.6")

    assert requested == ["https://api.openai.com/v1/models"]
    assert health.reachable is True
    assert health.selected_model_available is True
    assert health.available_models == ("gpt-5.6", "gpt-5.6-terra")


def test_real_process_controller_refuses_to_stop_an_unowned_process() -> None:
    app = create_application(["gribuki-gui-process-ownership-test"])
    controller = QtNapCatProcessController()

    assert controller.snapshot().owned is False
    assert controller.stop_owned() is False

    controller.deleteLater()
    app.processEvents()


def test_qt_executor_runs_blocking_work_outside_the_gui_thread() -> None:
    app = create_application(["gribuki-gui-worker-test"])
    executor = QtBackgroundExecutor()
    gui_thread = threading.get_ident()
    worker_threads: list[int] = []
    failures: list[str] = []

    executor.submit(threading.get_ident, worker_threads.append, failures.append)
    deadline = time.monotonic() + 2
    while not worker_threads and not failures and time.monotonic() < deadline:
        app.processEvents()
        time.sleep(0.005)

    assert failures == []
    assert len(worker_threads) == 1
    assert worker_threads[0] != gui_thread

    executor.deleteLater()
    app.processEvents()


def test_key_is_saved_off_thread_boundary_and_never_rendered() -> None:
    app = create_application(["gribuki-gui-key-test"])
    panel, gateway, _preferences, _process, _opened = _panel()
    key = "sk-gui-test-value-that-must-not-be-rendered"
    editor = panel.findChild(QLineEdit, "deepseekApiKey")
    button = panel.findChild(QPushButton, "deepseekSaveKeyButton")
    status = panel.findChild(QLabel, "deepseekIntegrationStatus")
    assert editor is not None
    assert button is not None
    assert status is not None

    editor.setText(key)
    button.click()
    app.processEvents()

    assert gateway.saved_keys == [f"deepseek:{key}"]
    assert editor.text() == ""
    assert key not in status.text()
    assert "Key 已配置" in status.text()

    panel.close()
    panel.deleteLater()
    app.processEvents()


def test_provider_switch_persists_openai_model_and_uses_openai_key() -> None:
    app = create_application(["gribuki-gui-provider-test"])
    panel, gateway, preferences, _process, _opened = _panel()
    provider = panel.findChild(QComboBox, "llmProvider")
    model = panel.findChild(QComboBox, "deepseekModel")
    editor = panel.findChild(QLineEdit, "deepseekApiKey")
    save = panel.findChild(QPushButton, "deepseekSaveKeyButton")
    refresh = panel.findChild(QPushButton, "deepseekRefreshButton")
    assert provider is not None
    assert model is not None
    assert editor is not None
    assert save is not None
    assert refresh is not None

    provider.setCurrentIndex(provider.findData("openai"))
    model.setCurrentText("gpt-5.6-terra")
    editor.setText("openai-test-key-that-is-not-rendered")
    save.click()
    refresh.click()
    app.processEvents()

    assert preferences.values["llm_provider"] == "openai"
    assert preferences.values["openai_model"] == "gpt-5.6-terra"
    assert gateway.saved_keys == ["openai:openai-test-key-that-is-not-rendered"]
    assert gateway.models[-1] == ("openai", "gpt-5.6-terra")
    assert editor.text() == ""

    panel.close()
    panel.deleteLater()
    app.processEvents()


def test_onebot_token_is_saved_and_cleared_without_rendering() -> None:
    app = create_application(["gribuki-gui-onebot-token-test"])
    panel, gateway, preferences, _process, _opened = _panel()
    token = "onebot-test-token-that-must-not-be-rendered"
    editor = panel.findChild(QLineEdit, "onebotAccessToken")
    button = panel.findChild(QPushButton, "napcatSaveTokenButton")
    status = panel.findChild(QLabel, "napcatIntegrationStatus")
    force = panel.findChild(QCheckBox, "napcatForceConfig")
    assert editor is not None
    assert button is not None
    assert status is not None
    assert force is not None

    force.setChecked(True)
    editor.setText(token)
    button.click()
    app.processEvents()

    assert gateway.saved_tokens == [token]
    assert gateway.napcat_configurations[0][1:] == (
        "http://127.0.0.1:3000",
        "http://127.0.0.1:6099",
        token,
        True,
    )
    assert editor.text() == ""
    assert force.isChecked() is False
    assert token not in status.text()
    assert preferences.values["onebot_url"] == "http://127.0.0.1:3000"
    assert preferences.values["napcat_webui_url"] == "http://127.0.0.1:6099"
    assert Path(preferences.values["napcat_runtime"]).is_absolute()
    assert "OneBot 可达" in status.text()

    panel.close()
    panel.deleteLater()
    app.processEvents()


def test_default_gateway_configures_new_runtime_and_both_tokens_without_network(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "NapCat"
    config = runtime / "napcat" / "config"
    config.mkdir(parents=True)
    (runtime / "napcat.bat").write_text("@exit /b 0\n", encoding="utf-8")
    secrets = MemorySecretProvider()
    gateway = DefaultIntegrationGateway(secrets)
    onebot_token = "onebot_SAFE-token_0123456789_ABCDEFGHIJKLMNO"

    result = gateway.configure_napcat_runtime(
        os.fspath(runtime),
        "http://127.0.0.1:3100",
        "http://127.0.0.1:6199",
        onebot_token,
        force=False,
    )

    onebot = json.loads(result.onebot_config_path.read_text(encoding="utf-8"))
    webui = json.loads(result.webui_config_path.read_text(encoding="utf-8"))
    assert onebot["network"]["httpServers"][0]["port"] == 3100
    assert onebot["network"]["httpServers"][0]["token"] == onebot_token
    assert webui["port"] == 6199
    assert webui["token"] == secrets.get_secret(NAPCAT_WEBUI_TOKEN_SECRET)
    assert secrets.get_secret(NAPCAT_ACCESS_TOKEN_SECRET) == onebot_token
    assert gateway.get_napcat_webui_token() == webui["token"]
    assert onebot_token not in repr(result)
    assert webui["token"] not in repr(result)


def test_gui_copies_webui_token_without_rendering_it() -> None:
    app = create_application(["gribuki-gui-webui-token-test"])
    panel, gateway, _preferences, _process, _opened = _panel()
    button = panel.findChild(QPushButton, "napcatCopyWebUiTokenButton")
    status = panel.findChild(QLabel, "napcatProcessStatus")
    assert button is not None
    assert status is not None

    button.click()
    app.processEvents()

    assert app.clipboard().text() == gateway.webui_token
    assert gateway.webui_token not in status.text()
    assert "已复制" in status.text()
    app.clipboard().clear()
    panel.close()
    panel.deleteLater()
    app.processEvents()


def test_process_buttons_only_stop_the_injected_owned_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_application(["gribuki-gui-process-test"])
    panel, _gateway, _preferences, process, _opened = _panel()
    command = NapCatLaunchCommand("cmd.exe", ("/d", "/c", "launcher.bat"), "C:\\NapCat")
    monkeypatch.setattr(integrations_module, "resolve_napcat_launch", lambda _path: command)
    start = panel.findChild(QPushButton, "napcatStartButton")
    stop = panel.findChild(QPushButton, "napcatStopButton")
    status = panel.findChild(QLabel, "napcatProcessStatus")
    assert start is not None
    assert stop is not None
    assert status is not None

    stop.click()
    assert process.stop_calls == 0  # 图形界面未持有进程时该控件禁用
    start.click()
    app.processEvents()
    assert process.commands == [command]
    assert stop.isEnabled() is True

    stop.click()
    app.processEvents()
    assert process.stop_calls == 1
    assert process.owned is False
    assert stop.isEnabled() is False

    panel.close()
    panel.deleteLater()
    app.processEvents()


def test_webui_only_opens_a_loopback_origin() -> None:
    app = create_application(["gribuki-gui-webui-test"])
    panel, _gateway, _preferences, _process, opened = _panel()
    editor = panel.findChild(QLineEdit, "napcatWebUiUrl")
    button = panel.findChild(QPushButton, "napcatOpenWebUiButton")
    assert editor is not None
    assert button is not None

    button.click()
    assert opened == ["http://127.0.0.1:6099"]
    editor.setText("https://example.com/login?token=unsafe")
    button.click()
    assert opened == ["http://127.0.0.1:6099"]

    panel.close()
    panel.deleteLater()
    app.processEvents()


@pytest.mark.parametrize(
    "value",
    [
        "https://example.com",
        "http://127.0.0.1:6099/path",
        "http://user:password@127.0.0.1:6099",
        "http://127.0.0.1:6099?token=unsafe",
    ],
)
def test_loopback_origin_validation_rejects_remote_or_credential_urls(value: str) -> None:
    with pytest.raises(ValueError):
        validate_loopback_origin(value, label="测试地址")


@pytest.mark.skipif(os.name != "nt", reason="NapCat desktop launcher is Windows-only")
def test_launch_resolution_accepts_exact_supported_runtime(tmp_path: Path) -> None:
    runtime = tmp_path / "NapCat Shell"
    runtime.mkdir()
    launcher = runtime / "launcher-user.bat"
    launcher.write_text("@exit /b 0\n", encoding="utf-8")

    command = resolve_napcat_launch(os.fspath(runtime))

    assert Path(command.working_directory) == runtime.resolve()
    assert command.arguments[:2] == ("/d", "/c")
    assert Path(command.arguments[2]) == launcher.resolve()
