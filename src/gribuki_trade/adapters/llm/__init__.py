"""Optional language-model adapters."""

from .deepseek_chat import (
    DEEPSEEK_API_KEY_SECRET,
    DEFAULT_DEEPSEEK_BASE_URL,
    DEFAULT_DEEPSEEK_MODEL,
    DeepSeekChatMacroAnalyzer,
    DeepSeekMacroAnalyzerError,
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
    "DEFAULT_DEEPSEEK_BASE_URL",
    "DEFAULT_DEEPSEEK_MODEL",
    "DeepSeekChatMacroAnalyzer",
    "DeepSeekHealthClient",
    "DeepSeekHealthErrorCode",
    "DeepSeekHealthResult",
    "DeepSeekMacroAnalyzerError",
    "MacroAnalyzerError",
    "OPENAI_API_KEY_SECRET",
    "OpenAIResponsesMacroAnalyzer",
]
