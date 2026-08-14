"""GUI 与生产命令共享集成配置的冻结行为。"""

from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import pytest

from gribuki_trade.runtime.integration_settings import (
    DEFAULT_ONEBOT_URL,
    IntegrationRuntimeSettings,
    IntegrationSettingsError,
    IntegrationSettingsStore,
)


def _set_shared_setting(
    path: str,
    name: str,
    value: str,
    start: multiprocessing.synchronize.Event,
) -> None:
    start.wait(timeout=5)
    IntegrationSettingsStore(Path(path)).set(name, value)


def test_missing_file_uses_defaults_and_saved_values_round_trip(tmp_path) -> None:
    store = IntegrationSettingsStore(tmp_path / "integrations.json")

    assert store.load().onebot_url == DEFAULT_ONEBOT_URL

    store.set("deepseek_model", "deepseek-test-model")
    store.set("openai_model", "gpt-test-model")
    store.set("llm_provider", "openai")
    stored = store.set("onebot_url", "http://localhost:3456")

    assert stored.deepseek_model == "deepseek-test-model"
    assert stored.openai_model == "gpt-test-model"
    assert stored.llm_provider == "openai"
    assert stored.onebot_url == "http://localhost:3456"
    assert stored.revision == 4
    assert store.load() == stored


def test_shared_config_rejects_unknown_fields_and_remote_origins(tmp_path) -> None:
    path = tmp_path / "integrations.json"
    document = IntegrationRuntimeSettings().document()
    document["unexpected"] = "value"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(IntegrationSettingsError, match="fields"):
        IntegrationSettingsStore(path).load()

    with pytest.raises(ValueError, match="loopback"):
        IntegrationRuntimeSettings(onebot_url="https://example.com")


def test_shared_config_contains_no_secret_fields(tmp_path) -> None:
    store = IntegrationSettingsStore(tmp_path / "integrations.json")
    store.save(IntegrationRuntimeSettings())

    document = json.loads(store.path.read_text(encoding="utf-8"))

    assert set(document) == {
        "deepseek_model",
        "llm_provider",
        "napcat_runtime",
        "napcat_webui_url",
        "onebot_url",
        "openai_model",
        "revision",
        "schema",
    }
    assert all("token" not in key and "key" not in key for key in document)
    assert document["revision"] == 1


def test_v1_deepseek_settings_are_read_without_losing_values(tmp_path) -> None:
    path = tmp_path / "integrations.json"
    path.write_text(
        json.dumps(
            {
                "schema": "gribuki-integration-settings@1",
                "deepseek_model": "deepseek-legacy",
                "napcat_runtime": "vendor/legacy",
                "napcat_webui_url": "http://127.0.0.1:6099",
                "onebot_url": "http://127.0.0.1:3000",
            }
        ),
        encoding="utf-8",
    )

    migrated = IntegrationSettingsStore(path).load()

    assert migrated.llm_provider == "deepseek"
    assert migrated.deepseek_model == "deepseek-legacy"
    assert migrated.openai_model == "gpt-5.6"
    assert migrated.napcat_runtime == "vendor/legacy"


def test_v2_settings_upgrade_on_first_serialized_mutation(tmp_path) -> None:
    path = tmp_path / "integrations.json"
    path.write_text(
        json.dumps(
            {
                "schema": "gribuki-integration-settings@2",
                "llm_provider": "deepseek",
                "deepseek_model": "deepseek-legacy",
                "openai_model": "gpt-legacy",
                "napcat_runtime": "vendor/legacy",
                "napcat_webui_url": "http://127.0.0.1:6099",
                "onebot_url": "http://127.0.0.1:3000",
            }
        ),
        encoding="utf-8",
    )

    upgraded = IntegrationSettingsStore(path).set("llm_provider", "openai")

    assert upgraded.revision == 1
    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["schema"] == "gribuki-integration-settings@3"
    assert document["revision"] == 1


def test_stale_snapshot_cannot_overwrite_newer_revision(tmp_path) -> None:
    store = IntegrationSettingsStore(tmp_path / "integrations.json")
    stale = store.load()
    store.set("llm_provider", "openai")

    with pytest.raises(IntegrationSettingsError, match="revision changed"):
        store.save(stale)


def test_cross_process_updates_are_serialized_without_lost_fields(tmp_path) -> None:
    path = tmp_path / "integrations.json"
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    processes = (
        context.Process(
            target=_set_shared_setting,
            args=(str(path), "deepseek_model", "deepseek-process-model", start),
        ),
        context.Process(
            target=_set_shared_setting,
            args=(str(path), "openai_model", "gpt-process-model", start),
        ),
    )
    for process in processes:
        process.start()
    start.set()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    stored = IntegrationSettingsStore(path).load()
    assert stored.deepseek_model == "deepseek-process-model"
    assert stored.openai_model == "gpt-process-model"
    assert stored.revision == 2
