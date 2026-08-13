from __future__ import annotations

import json
import os
from dataclasses import fields
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from gribuki_trade.adapters.notifiers.onebot import NAPCAT_ACCESS_TOKEN_SECRET
from gribuki_trade.napcat_setup import (
    MIN_TOKEN_LENGTH,
    NAPCAT_WEBUI_TOKEN_SECRET,
    NapCatSetupError,
    NapCatSetupResult,
    configure_portable_napcat_runtime,
)
from gribuki_trade.security import MemorySecretProvider

ONEBOT_TOKEN = "onebot_SAFE-token_0123456789_ABCDEFGHIJKLMN"
WEBUI_TOKEN = "webui_SAFE-token_0123456789_ABCDEFGHIJKLMNO"
OTHER_ONEBOT_TOKEN = "other_ONEBOT-token_0123456789_ABCDEFGHIJKLM"
OTHER_WEBUI_TOKEN = "other_WEBUI-token_0123456789_ABCDEFGHIJKLMN"


class NapCatSetupTests(TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.runtime = Path(self.temporary.name).resolve() / "NapCat.Portable"
        self.runtime.mkdir()
        (self.runtime / "napcat.bat").write_text("@echo off\n", encoding="utf-8")
        (self.runtime / "napcat" / "config").mkdir(parents=True)
        self.config_dir = self.runtime / "napcat" / "config"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_generates_locked_down_configs_and_stores_distinct_tokens(self) -> None:
        provider = MemorySecretProvider()

        result = configure_portable_napcat_runtime(self.runtime, provider)

        onebot_token = provider.get_secret(NAPCAT_ACCESS_TOKEN_SECRET)
        webui_token = provider.get_secret(NAPCAT_WEBUI_TOKEN_SECRET)
        self.assertIsNotNone(onebot_token)
        self.assertIsNotNone(webui_token)
        assert onebot_token is not None
        assert webui_token is not None
        self.assertGreaterEqual(len(onebot_token), MIN_TOKEN_LENGTH)
        self.assertGreaterEqual(len(webui_token), MIN_TOKEN_LENGTH)
        self.assertNotEqual(onebot_token, webui_token)

        onebot = self._read_json("onebot11.json")
        server = onebot["network"]["httpServers"][0]
        self.assertEqual(server["name"], "gribuki-local")
        self.assertTrue(server["enable"])
        self.assertEqual(server["host"], "127.0.0.1")
        self.assertEqual(server["port"], 3000)
        self.assertFalse(server["enableCors"])
        self.assertFalse(server["enableWebsocket"])
        self.assertEqual(server["messagePostFormat"], "array")
        self.assertFalse(server["debug"])
        self.assertEqual(server["token"], onebot_token)
        for disabled_group in (
            "httpSseServers",
            "httpClients",
            "websocketServers",
            "websocketClients",
            "plugins",
        ):
            self.assertEqual(onebot["network"][disabled_group], [])
        self.assertFalse(onebot["enableLocalFile2Url"])
        self.assertFalse(onebot["parseMultMsg"])

        webui = self._read_json("webui.json")
        self.assertEqual(webui["host"], "127.0.0.1")
        self.assertEqual(webui["port"], 6099)
        self.assertEqual(webui["token"], webui_token)
        self.assertEqual(webui["accessControlMode"], "whitelist")
        self.assertEqual(webui["ipWhitelist"], ["127.0.0.1", "::1"])
        self.assertEqual(webui["ipBlacklist"], [])
        self.assertFalse(webui["enableXForwardedFor"])

        self.assertEqual(
            {field.name for field in fields(NapCatSetupResult)},
            {
                "runtime_dir",
                "onebot_config_path",
                "webui_config_path",
                "onebot_port",
                "webui_port",
            },
        )
        rendered = repr(result)
        self.assertNotIn(onebot_token, rendered)
        self.assertNotIn(webui_token, rendered)
        self.assertEqual(result.runtime_dir, self.runtime)
        self.assertEqual(result.onebot_config_path, self.config_dir / "onebot11.json")
        self.assertEqual(result.webui_config_path, self.config_dir / "webui.json")
        self.assertEqual(result.onebot_port, 3000)
        self.assertEqual(result.webui_port, 6099)

    def test_accepts_the_official_standalone_shell_layout(self) -> None:
        shell = Path(self.temporary.name).resolve() / "NapCat.Shell"
        (shell / "config").mkdir(parents=True)
        for name in (
            "launcher-user.bat",
            "NapCatWinBootMain.exe",
            "NapCatWinBootHook.dll",
            "napcat.mjs",
        ):
            (shell / name).write_bytes(b"official-shell-placeholder")
        provider = MemorySecretProvider()

        result = configure_portable_napcat_runtime(shell, provider)

        self.assertEqual(result.runtime_dir, shell)
        self.assertEqual(result.onebot_config_path, shell / "config" / "onebot11.json")
        self.assertEqual(result.webui_config_path, shell / "config" / "webui.json")
        self.assertEqual(list(self.config_dir.glob(".*.tmp")), [])

    def test_accepts_supplied_tokens_and_custom_ports(self) -> None:
        provider = MemorySecretProvider()

        result = configure_portable_napcat_runtime(
            self.runtime,
            provider,
            onebot_port=3100,
            webui_port=6199,
            onebot_token=ONEBOT_TOKEN,
            webui_token=WEBUI_TOKEN,
        )

        self.assertEqual(provider.get_secret(NAPCAT_ACCESS_TOKEN_SECRET), ONEBOT_TOKEN)
        self.assertEqual(provider.get_secret(NAPCAT_WEBUI_TOKEN_SECRET), WEBUI_TOKEN)
        onebot_server = self._read_json("onebot11.json")["network"]["httpServers"][0]
        self.assertEqual(onebot_server["port"], 3100)
        self.assertEqual(self._read_json("webui.json")["port"], 6199)
        self.assertEqual(result.onebot_port, 3100)
        self.assertEqual(result.webui_port, 6199)

    def test_refuses_overwrite_by_default_and_force_replaces_both(self) -> None:
        provider = MemorySecretProvider()
        configure_portable_napcat_runtime(
            self.runtime,
            provider,
            onebot_token=ONEBOT_TOKEN,
            webui_token=WEBUI_TOKEN,
        )
        old_onebot = (self.config_dir / "onebot11.json").read_bytes()
        old_webui = (self.config_dir / "webui.json").read_bytes()

        with self.assertRaises(FileExistsError):
            configure_portable_napcat_runtime(
                self.runtime,
                provider,
                onebot_token=OTHER_ONEBOT_TOKEN,
                webui_token=OTHER_WEBUI_TOKEN,
            )

        self.assertEqual((self.config_dir / "onebot11.json").read_bytes(), old_onebot)
        self.assertEqual((self.config_dir / "webui.json").read_bytes(), old_webui)
        self.assertEqual(provider.get_secret(NAPCAT_ACCESS_TOKEN_SECRET), ONEBOT_TOKEN)
        self.assertEqual(provider.get_secret(NAPCAT_WEBUI_TOKEN_SECRET), WEBUI_TOKEN)

        configure_portable_napcat_runtime(
            self.runtime,
            provider,
            onebot_token=OTHER_ONEBOT_TOKEN,
            webui_token=OTHER_WEBUI_TOKEN,
            force=True,
        )

        self.assertEqual(provider.get_secret(NAPCAT_ACCESS_TOKEN_SECRET), OTHER_ONEBOT_TOKEN)
        self.assertEqual(provider.get_secret(NAPCAT_WEBUI_TOKEN_SECRET), OTHER_WEBUI_TOKEN)
        self.assertEqual(
            self._read_json("onebot11.json")["network"]["httpServers"][0]["token"],
            OTHER_ONEBOT_TOKEN,
        )
        self.assertEqual(self._read_json("webui.json")["token"], OTHER_WEBUI_TOKEN)

    def test_rejects_invalid_ports_before_writing(self) -> None:
        provider = MemorySecretProvider()
        invalid_cases: tuple[tuple[object, object, type[Exception]], ...] = (
            (True, 6099, TypeError),
            (0, 6099, ValueError),
            (65536, 6099, ValueError),
            (3000, "6099", TypeError),
            (3000, 3000, ValueError),
        )

        for onebot_port, webui_port, expected in invalid_cases:
            with self.subTest(
                onebot_port=onebot_port,
                webui_port=webui_port,
            ), self.assertRaises(expected):
                configure_portable_napcat_runtime(
                    self.runtime,
                    provider,
                    onebot_port=onebot_port,  # type: ignore[arg-type]
                    webui_port=webui_port,  # type: ignore[arg-type]
                )

        self.assertEqual(list(self.config_dir.iterdir()), [])

    def test_rejects_weak_invalid_or_reused_tokens(self) -> None:
        provider = MemorySecretProvider()
        invalid_onebot_tokens = (
            "short",
            "a" * MIN_TOKEN_LENGTH,
            "token with spaces_0123456789_ABCDEFGHIJKLMN",
            "unicode_token_令牌_0123456789_ABCDEFGHIJKLMNO",
        )
        for token in invalid_onebot_tokens:
            with self.subTest(token_kind="invalid"), self.assertRaises(ValueError):
                configure_portable_napcat_runtime(
                    self.runtime,
                    provider,
                    onebot_token=token,
                    webui_token=WEBUI_TOKEN,
                )

        with self.assertRaisesRegex(ValueError, "different"):
            configure_portable_napcat_runtime(
                self.runtime,
                provider,
                onebot_token=ONEBOT_TOKEN,
                webui_token=ONEBOT_TOKEN,
            )
        self.assertEqual(list(self.config_dir.iterdir()), [])

    def test_requires_absolute_preexisting_portable_runtime_shape(self) -> None:
        provider = MemorySecretProvider()
        with self.assertRaisesRegex(ValueError, "absolute"):
            configure_portable_napcat_runtime(Path("relative-runtime"), provider)

        (self.runtime / "napcat.bat").unlink()
        with self.assertRaisesRegex(ValueError, "supported NapCat runtime layout"):
            configure_portable_napcat_runtime(self.runtime, provider)

        (self.runtime / "napcat.bat").write_text("@echo off\n", encoding="utf-8")
        self.config_dir.rmdir()
        with self.assertRaisesRegex(ValueError, "supported NapCat runtime layout"):
            configure_portable_napcat_runtime(self.runtime, provider)

    def test_rejects_symlinked_config_destination_when_supported(self) -> None:
        outside = Path(self.temporary.name) / "outside.json"
        outside.write_text("outside", encoding="utf-8")
        destination = self.config_dir / "onebot11.json"
        try:
            os.symlink(outside, destination)
        except OSError:
            self.skipTest("symbolic links are unavailable for this test user")

        with self.assertRaisesRegex(ValueError, "symbolic"):
            configure_portable_napcat_runtime(
                self.runtime,
                MemorySecretProvider(),
                force=True,
            )
        self.assertEqual(outside.read_text(encoding="utf-8"), "outside")

    def test_provider_failure_restores_files_and_both_previous_secrets(self) -> None:
        onebot_path = self.config_dir / "onebot11.json"
        webui_path = self.config_dir / "webui.json"
        onebot_path.write_bytes(b"old-onebot\n")
        webui_path.write_bytes(b"old-webui\n")
        provider = _FailingSecondWriteProvider(
            {
                NAPCAT_ACCESS_TOKEN_SECRET: ONEBOT_TOKEN,
                NAPCAT_WEBUI_TOKEN_SECRET: WEBUI_TOKEN,
            }
        )

        try:
            configure_portable_napcat_runtime(
                self.runtime,
                provider,
                onebot_token=OTHER_ONEBOT_TOKEN,
                webui_token=OTHER_WEBUI_TOKEN,
                force=True,
            )
        except NapCatSetupError as error:
            rendered = repr(error) + str(error) + repr(error.__context__)
        else:
            self.fail("expected a sanitized setup failure")

        self.assertNotIn(OTHER_ONEBOT_TOKEN, rendered)
        self.assertNotIn(OTHER_WEBUI_TOKEN, rendered)
        self.assertEqual(onebot_path.read_bytes(), b"old-onebot\n")
        self.assertEqual(webui_path.read_bytes(), b"old-webui\n")
        self.assertEqual(provider.get_secret(NAPCAT_ACCESS_TOKEN_SECRET), ONEBOT_TOKEN)
        self.assertEqual(provider.get_secret(NAPCAT_WEBUI_TOKEN_SECRET), WEBUI_TOKEN)
        self.assertEqual(list(self.config_dir.glob(".*.tmp")), [])

    def _read_json(self, name: str) -> dict[str, object]:
        document = json.loads((self.config_dir / name).read_text(encoding="utf-8"))
        assert isinstance(document, dict)
        return document


class _FailingSecondWriteProvider(MemorySecretProvider):
    def __init__(self, initial: dict[str, str]) -> None:
        self._fail_next_webui_write = False
        super().__init__(initial)
        self._fail_next_webui_write = True

    def set_secret(self, name: str, value: str) -> None:
        super().set_secret(name, value)
        if name == NAPCAT_WEBUI_TOKEN_SECRET and self._fail_next_webui_write:
            self._fail_next_webui_write = False
            raise RuntimeError(f"unsafe backend error containing {value}")
