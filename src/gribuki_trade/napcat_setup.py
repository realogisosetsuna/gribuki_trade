"""Safe, non-executing configuration for a portable NapCatQQ runtime."""

from __future__ import annotations

import json
import os
import secrets
import stat
import string
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from gribuki_trade.adapters.notifiers.onebot import NAPCAT_ACCESS_TOKEN_SECRET
from gribuki_trade.security import SecretProvider

NAPCAT_WEBUI_TOKEN_SECRET = "napcat.webui.token"

DEFAULT_ONEBOT_PORT = 3000
DEFAULT_WEBUI_PORT = 6099
MIN_TOKEN_LENGTH = 32
MAX_TOKEN_LENGTH = 256

_TOKEN_CHARACTERS = frozenset(string.ascii_letters + string.digits + "-._~")
_ONEBOT_CONFIG_NAME = "onebot11.json"
_WEBUI_CONFIG_NAME = "webui.json"


class NapCatSetupError(RuntimeError):
    """Portable NapCat configuration could not be completed safely."""


@dataclass(frozen=True, slots=True)
class NapCatSetupResult:
    """Non-secret paths and ports produced by a successful setup."""

    runtime_dir: Path
    onebot_config_path: Path
    webui_config_path: Path
    onebot_port: int
    webui_port: int


def configure_portable_napcat_runtime(
    runtime_dir: str | os.PathLike[str],
    secret_provider: SecretProvider,
    *,
    onebot_port: int = DEFAULT_ONEBOT_PORT,
    webui_port: int = DEFAULT_WEBUI_PORT,
    onebot_token: str | None = None,
    webui_token: str | None = None,
    force: bool = False,
) -> NapCatSetupResult:
    """Write locked-down NapCat configs without starting NapCat or QQ.

    ``runtime_dir`` must be an absolute, already extracted portable runtime
    containing either the official standalone Shell files and ``config`` or
    the developer Node bundle's ``napcat.bat`` and ``napcat/config``. Secrets
    are written both to the configuration files required by NapCat and to
    ``secret_provider``. They are deliberately absent from the returned value.
    """

    if not isinstance(force, bool):
        raise TypeError("force must be a boolean")
    checked_onebot_port = _validate_port(onebot_port, "onebot_port")
    checked_webui_port = _validate_port(webui_port, "webui_port")
    if checked_onebot_port == checked_webui_port:
        raise ValueError("OneBot and WebUI ports must be different")

    checked_runtime, config_dir = _validate_runtime(runtime_dir)
    onebot_path = _validate_destination(config_dir, _ONEBOT_CONFIG_NAME)
    webui_path = _validate_destination(config_dir, _WEBUI_CONFIG_NAME)
    destinations = (onebot_path, webui_path)

    existing = tuple(path for path in destinations if path.exists())
    if existing and not force:
        raise FileExistsError("NapCat configuration already exists; use force to replace it")

    checked_onebot_token = _new_token() if onebot_token is None else _validate_token(
        onebot_token,
        "onebot_token",
    )
    checked_webui_token = _new_token() if webui_token is None else _validate_token(
        webui_token,
        "webui_token",
    )
    if secrets.compare_digest(checked_onebot_token, checked_webui_token):
        raise ValueError("OneBot and WebUI tokens must be different")

    documents = {
        onebot_path: _json_bytes(
            _onebot_config(checked_onebot_port, checked_onebot_token)
        ),
        webui_path: _json_bytes(_webui_config(checked_webui_port, checked_webui_token)),
    }
    previous_files = {
        path: path.read_bytes() if path.exists() else None for path in destinations
    }

    previous_secrets: dict[str, str | None] = {}
    secret_read_failed = False
    try:
        previous_secrets[NAPCAT_ACCESS_TOKEN_SECRET] = secret_provider.get_secret(
            NAPCAT_ACCESS_TOKEN_SECRET
        )
        previous_secrets[NAPCAT_WEBUI_TOKEN_SECRET] = secret_provider.get_secret(
            NAPCAT_WEBUI_TOKEN_SECRET
        )
    except Exception:
        secret_read_failed = True
    if secret_read_failed:
        raise NapCatSetupError("unable to read the existing NapCat secrets")

    operation_failed = False
    rollback_failed = False
    try:
        for path, content in documents.items():
            _atomic_write(path, content)
        secret_provider.set_secret(NAPCAT_ACCESS_TOKEN_SECRET, checked_onebot_token)
        secret_provider.set_secret(NAPCAT_WEBUI_TOKEN_SECRET, checked_webui_token)
    except Exception:
        operation_failed = True
        rollback_failed = not _restore_files(previous_files)
        if not _restore_secrets(secret_provider, previous_secrets):
            rollback_failed = True

    if operation_failed:
        if rollback_failed:
            raise NapCatSetupError("NapCat setup failed and rollback could not be completed")
        raise NapCatSetupError("NapCat setup failed; prior state was restored")

    return NapCatSetupResult(
        runtime_dir=checked_runtime,
        onebot_config_path=onebot_path,
        webui_config_path=webui_path,
        onebot_port=checked_onebot_port,
        webui_port=checked_webui_port,
    )


def _validate_runtime(runtime_dir: str | os.PathLike[str]) -> tuple[Path, Path]:
    try:
        raw_path = os.fspath(runtime_dir)
    except TypeError:
        raise TypeError("runtime_dir must be a string or path-like object") from None
    if not isinstance(raw_path, str):
        raise TypeError("runtime_dir must resolve to a text path")
    if not raw_path.strip():
        raise ValueError("runtime_dir must not be empty")

    candidate = Path(raw_path)
    if not candidate.is_absolute():
        raise ValueError("runtime_dir must be an absolute path")
    if candidate.is_symlink():
        raise ValueError("runtime_dir must not be a symbolic link")
    try:
        runtime = candidate.resolve(strict=True)
    except OSError:
        raise ValueError("runtime_dir must exist") from None
    if not runtime.is_dir():
        raise ValueError("runtime_dir must be a directory")

    node_launcher = runtime / "napcat.bat"
    node_dir = runtime / "napcat"
    node_config = node_dir / "config"
    shell_files = (
        runtime / "launcher-user.bat",
        runtime / "NapCatWinBootMain.exe",
        runtime / "NapCatWinBootHook.dll",
        runtime / "napcat.mjs",
    )
    shell_config = runtime / "config"
    node_layout = (
        node_launcher.is_file()
        and not node_launcher.is_symlink()
        and node_dir.is_dir()
        and not node_dir.is_symlink()
        and node_config.is_dir()
        and not node_config.is_symlink()
    )
    shell_layout = (
        all(path.is_file() and not path.is_symlink() for path in shell_files)
        and shell_config.is_dir()
        and not shell_config.is_symlink()
    )
    if node_layout == shell_layout:
        raise ValueError(
            "runtime_dir must contain exactly one supported NapCat runtime layout"
        )
    config_dir = node_config if node_layout else shell_config

    resolved_config = config_dir.resolve(strict=True)
    if not resolved_config.is_relative_to(runtime):
        raise ValueError("NapCat config directory must remain inside runtime_dir")
    return runtime, resolved_config


def _require_regular_file(path: Path, message: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise ValueError(message)


def _require_directory(path: Path, message: str) -> None:
    if path.is_symlink() or not path.is_dir():
        raise ValueError(message)


def _validate_destination(config_dir: Path, name: str) -> Path:
    destination = config_dir / name
    if destination.is_symlink():
        raise ValueError("NapCat configuration files must not be symbolic links")
    resolved = destination.resolve(strict=False)
    if resolved.parent != config_dir or not resolved.is_relative_to(config_dir):
        raise ValueError("NapCat configuration path escaped the config directory")
    if destination.exists() and not destination.is_file():
        raise ValueError("NapCat configuration destination must be a regular file")
    return resolved


def _validate_port(value: int, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if not 1 <= value <= 65535:
        raise ValueError(f"{name} must be in the range 1..65535")
    return value


def _validate_token(value: str, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not MIN_TOKEN_LENGTH <= len(value) <= MAX_TOKEN_LENGTH:
        raise ValueError(
            f"{name} must contain between {MIN_TOKEN_LENGTH} and {MAX_TOKEN_LENGTH} characters"
        )
    if any(character not in _TOKEN_CHARACTERS for character in value):
        raise ValueError(f"{name} must use URL-safe ASCII characters without whitespace")
    if len(set(value)) < 8:
        raise ValueError(f"{name} does not have enough character diversity")
    return value


def _new_token() -> str:
    while True:
        candidate = secrets.token_urlsafe(32)
        try:
            return _validate_token(candidate, "generated_token")
        except ValueError:
            continue


def _onebot_config(port: int, token: str) -> dict[str, Any]:
    return {
        "network": {
            "httpServers": [
                {
                    "name": "gribuki-local",
                    "enable": True,
                    "port": port,
                    "host": "127.0.0.1",
                    "enableCors": False,
                    "enableWebsocket": False,
                    "messagePostFormat": "array",
                    "token": token,
                    "debug": False,
                }
            ],
            "httpSseServers": [],
            "httpClients": [],
            "websocketServers": [],
            "websocketClients": [],
            "plugins": [],
        },
        "musicSignUrl": "",
        "enableLocalFile2Url": False,
        "parseMultMsg": False,
        "imageDownloadProxy": "",
    }


def _webui_config(port: int, token: str) -> dict[str, Any]:
    return {
        "host": "127.0.0.1",
        "port": port,
        "token": token,
        "loginRate": 3,
        "autoLoginAccount": "",
        "disableWebUI": False,
        "accessControlMode": "whitelist",
        "ipWhitelist": ["127.0.0.1", "::1"],
        "ipBlacklist": [],
        "enableXForwardedFor": False,
        "enable2FA": False,
        "totpSecret": "",
    }


def _json_bytes(document: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(document, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")


def _atomic_write(path: Path, content: bytes) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            delete=False,
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        ) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        assert temporary is not None
        os.chmod(temporary, stat.S_IRUSR | stat.S_IWUSR)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _restore_files(previous: Mapping[Path, bytes | None]) -> bool:
    restored = True
    for path, content in previous.items():
        try:
            if content is None:
                path.unlink(missing_ok=True)
            else:
                _atomic_write(path, content)
        except Exception:
            restored = False
    return restored


def _restore_secrets(
    provider: SecretProvider,
    previous: Mapping[str, str | None],
) -> bool:
    restored = True
    for name, value in previous.items():
        try:
            if value is None:
                provider.delete_secret(name)
            else:
                provider.set_secret(name, value)
        except Exception:
            restored = False
    return restored
