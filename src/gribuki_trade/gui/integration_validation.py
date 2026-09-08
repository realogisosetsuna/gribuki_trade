"""GUI 集成配置的无副作用校验和错误文案。"""

from __future__ import annotations

from urllib.parse import urlsplit

from gribuki_trade.adapters.llm import DeepSeekHealthErrorCode
from gribuki_trade.runtime.integration_settings import (
    validate_loopback_origin as _validate_runtime_loopback_origin,
)
from gribuki_trade.runtime.integration_settings import (
    validate_model_id as _validate_runtime_model_id,
)


def validate_model_id(value: str) -> str:
    """返回适合本机共享配置的有界模型 ID。"""

    try:
        return _validate_runtime_model_id(value)
    except TypeError:
        raise TypeError("模型 ID 必须是文本。") from None
    except ValueError:
        raise ValueError("模型 ID 格式无效。") from None


def validate_optional_model_id(value: str) -> str | None:
    try:
        return validate_model_id(value)
    except (TypeError, ValueError):
        return None


def validate_llm_provider(value: str) -> str:
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


def napcat_http_port(value: str, *, label: str) -> int:
    checked = validate_loopback_origin(value, label=label)
    parsed = urlsplit(checked)
    if parsed.scheme != "http":
        raise ValueError(f"{label}必须使用本机 HTTP。")
    return parsed.port or 80


def validate_api_key(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("API Key 必须是文本。")
    if value != value.strip() or any(character.isspace() for character in value):
        raise ValueError("API Key 不能包含首尾或内部空白。")
    if not 8 <= len(value) <= 512:
        raise ValueError("API Key 长度无效。")
    return value


def validate_access_token(value: str) -> str:
    """校验 OneBot token，不把令牌值写入错误文本。"""

    if not isinstance(value, str):
        raise TypeError("OneBot 令牌必须是文本。")
    if value != value.strip() or any(character.isspace() for character in value):
        raise ValueError("OneBot 令牌不能包含首尾或内部空白。")
    if not 8 <= len(value) <= 512:
        raise ValueError("OneBot 令牌长度无效。")
    return value


def onebot_failure_text(code: str) -> str:
    if code in {"transport_timeout", "transport_error"}:
        return "OneBot 当前不可达；请确认 NapCat 已启动且端口正确。"
    if code == "authentication_rejected":
        return "OneBot 拒绝认证；请重新配置本地令牌。"
    return "OneBot 返回异常；详细信息已隐藏。"


def deepseek_failure_text(code: DeepSeekHealthErrorCode | None) -> str:
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


def safe_ui_message(message: str, fallback: str) -> str:
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


_validate_optional_model_id = validate_optional_model_id
_validate_llm_provider = validate_llm_provider
_napcat_http_port = napcat_http_port
_validate_api_key = validate_api_key
_validate_access_token = validate_access_token
_onebot_failure_text = onebot_failure_text
_deepseek_failure_text = deepseek_failure_text
_safe_ui_message = safe_ui_message
