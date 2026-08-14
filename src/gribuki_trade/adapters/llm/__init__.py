"""可选的大语言模型适配器。"""

from .deepseek_chat import (
    DEEPSEEK_ADAPTER_VERSION,
    DEEPSEEK_API_KEY_SECRET,
    DEEPSEEK_INTRADAY_PROFILE,
    DEEPSEEK_PREOPEN_PROFILE,
    DEEPSEEK_PROMPT_SCHEMA_SHA256,
    DEEPSEEK_PROMPT_VERSION,
    DEFAULT_DEEPSEEK_BASE_URL,
    DEFAULT_DEEPSEEK_MODEL,
    DeepSeekChatMacroAnalyzer,
    DeepSeekMacroAnalyzerError,
    DeepSeekMacroAnalyzerProfile,
)
from .deepseek_health import (
    DeepSeekHealthClient,
    DeepSeekHealthErrorCode,
    DeepSeekHealthResult,
)
from .openai_responses import (
    OPENAI_API_KEY_SECRET,
    MacroAnalyzerError,
    OpenAIResponsesMacroAnalyzer,
)

__all__ = [
    "DEEPSEEK_API_KEY_SECRET",
    "DEEPSEEK_ADAPTER_VERSION",
    "DEEPSEEK_INTRADAY_PROFILE",
    "DEEPSEEK_PREOPEN_PROFILE",
    "DEEPSEEK_PROMPT_SCHEMA_SHA256",
    "DEEPSEEK_PROMPT_VERSION",
    "DEFAULT_DEEPSEEK_BASE_URL",
    "DEFAULT_DEEPSEEK_MODEL",
    "DeepSeekChatMacroAnalyzer",
    "DeepSeekMacroAnalyzerProfile",
    "DeepSeekHealthClient",
    "DeepSeekHealthErrorCode",
    "DeepSeekHealthResult",
    "DeepSeekMacroAnalyzerError",
    "MacroAnalyzerError",
    "OPENAI_API_KEY_SECRET",
    "OpenAIResponsesMacroAnalyzer",
]
