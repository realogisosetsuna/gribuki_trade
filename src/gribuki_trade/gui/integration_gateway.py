"""GUI 集成网关及其健康状态协议。"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import httpx

from gribuki_trade.adapters.llm import OPENAI_API_KEY_SECRET, DeepSeekHealthClient
from gribuki_trade.adapters.llm.deepseek_chat import (
    DEEPSEEK_API_KEY_SECRET,
)
from gribuki_trade.adapters.notifiers.onebot import (
    NAPCAT_ACCESS_TOKEN_SECRET,
    OneBotConfig,
    OneBotError,
    OneBotNotifier,
)
from gribuki_trade.gui.integration_validation import (
    _deepseek_failure_text,
    _napcat_http_port,
    _onebot_failure_text,
    _validate_access_token,
    _validate_api_key,
    _validate_llm_provider,
    _validate_optional_model_id,
    validate_loopback_origin,
    validate_model_id,
)
from gribuki_trade.napcat_setup import (
    NAPCAT_WEBUI_TOKEN_SECRET,
    NapCatSetupResult,
    configure_portable_napcat_runtime,
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

__all__ = [
    "DefaultIntegrationGateway",
    "DeepSeekHealth",
    "IntegrationGateway",
    "NapCatHealth",
]
