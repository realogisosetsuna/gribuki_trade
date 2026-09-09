"""CLI 运行时配置、终端和本地凭据辅助。

这些函数不负责构造命令树，也不参与业务服务编排。它们只处理进程启动时
共享的运行时默认值、终端编码、SQLite 诊断和本地 secret 读取，因此可以在
不加载完整命令实现的情况下单独审阅和测试。

函数通过 ``gribuki_trade.cli`` facade 解析依赖。这样保留了历史入口的
monkeypatch 行为：嵌入式调用方仍可以替换 ``cli.load_integration_settings``、
``cli.KeyringSecretProvider`` 或 ``cli.sqlite_runtime_status``。
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any, cast

from gribuki_trade.security import InteractiveSecretManager


def _facade() -> Any:
    """延迟加载 CLI facade，允许本模块独立导入并避免循环依赖。"""

    from gribuki_trade import cli

    return cli


def apply_integration_runtime_defaults(args: argparse.Namespace) -> None:
    """把共享 GUI 配置应用到未由命令行显式覆盖的集成参数。"""

    command = args.command or "gui"
    action = getattr(args, "action", None)
    paper_day_run = command == "ashare-paper-day" and action == "run"
    post_close_run = command == "ashare-post-close" and action == "run"
    live_cycle = command == "live-sync" and action == "cycle"
    live_ingest = command == "live-sync" and action == "ingest"
    live_onebot = live_cycle or live_ingest
    base_url_commands = {
        "napcat-status",
        "napcat-dispatch",
        "napcat-send-test",
        "napcat-send-artifact",
    }
    model_commands = {
        "ashare-post-close",
        "ashare-research-once",
        "ashare-close-research-once",
        "ashare-close-research-batch",
        "ashare-research-watch",
    }
    needs_settings = (
        (
            (command in base_url_commands or paper_day_run or post_close_run or live_onebot)
            and getattr(args, "base_url", None) is None
        )
        or (command == "napcat-configure" and getattr(args, "runtime_dir", None) is None)
        or (
            command in model_commands
            and (command != "ashare-post-close" or post_close_run)
            and (
                getattr(args, "macro_provider", None) is None
                or getattr(args, "model", None) is None
            )
        )
        or (
            paper_day_run
            and (
                getattr(args, "intraday_llm_provider", None) is None
                or getattr(args, "intraday_llm_model", None) is None
            )
        )
        or (
            live_cycle
            and (
                getattr(args, "llm_provider", None) is None
                or getattr(args, "llm_model", None) is None
            )
        )
    )
    if not needs_settings:
        return
    runtime_cli = _facade()
    settings = runtime_cli.load_integration_settings()
    if (
        command in base_url_commands or paper_day_run or post_close_run or live_onebot
    ) and getattr(args, "base_url", None) is None:
        args.base_url = settings.onebot_url
    if command == "napcat-configure" and getattr(args, "runtime_dir", None) is None:
        args.runtime_dir = settings.napcat_runtime
    if command in model_commands and (command != "ashare-post-close" or post_close_run):
        if getattr(args, "macro_provider", None) is None:
            args.macro_provider = settings.llm_provider
        if getattr(args, "macro", False) is True and getattr(args, "model", None) is None:
            args.model = (
                settings.deepseek_model
                if args.macro_provider == "deepseek"
                else settings.openai_model
            )
    if paper_day_run:
        if args.intraday_llm_provider is None:
            args.intraday_llm_provider = settings.llm_provider
        if args.intraday_llm_model is None:
            args.intraday_llm_model = (
                settings.deepseek_model
                if args.intraday_llm_provider == "deepseek"
                else settings.openai_model
            )
    if live_cycle:
        if args.llm_provider is None:
            args.llm_provider = settings.llm_provider
        if args.llm_model is None:
            args.llm_model = (
                settings.deepseek_model
                if args.llm_provider == "deepseek"
                else settings.openai_model
            )


def configure_terminal_encoding() -> None:
    """在 Windows 旧版控制台代码页下保持中文命令行输出可读。"""

    if os.name != "nt":
        return
    for stream in (sys.stdout, sys.stderr):
        if not hasattr(stream, "reconfigure"):
            continue
        stream.reconfigure(encoding="utf-8", errors="replace")


def set_secret(name: str) -> None:
    """交互式写入一个已登记的本地 secret。"""

    runtime_cli = _facade()
    InteractiveSecretManager(runtime_cli.KeyringSecretProvider()).set_secret(name)


def secret_status() -> dict[str, bool]:
    """返回已登记 secret 的存在状态，不返回 secret 内容。"""

    runtime_cli = _facade()
    provider = runtime_cli.KeyringSecretProvider()
    return {
        name: provider.get_secret(name) is not None
        for name in runtime_cli.KNOWN_SECRET_STATUS_NAMES
    }


def sqlite_runtime_status() -> dict[str, object]:
    """把 SQLite 运行时诊断转换为 CLI JSON 文档。"""

    status = _facade().sqlite_runtime_status()
    return {
        "error_code": status.error_code,
        "fixed_releases": list(status.fixed_releases),
        "guidance_url": status.guidance_url,
        "ok": status.shared_wal_safe,
        "shared_wal_safe": status.shared_wal_safe,
        "single_connection_local_allowed": True,
        "sqlite_version": status.version,
    }


def temp_root(action: str, explicit: str | None) -> dict[str, object]:
    """查询或创建由统一解析器管理的 CLI 临时目录。"""

    from gribuki_trade.runtime.temp_root import TempRootResolver

    if action not in {"status", "prepare"}:
        raise ValueError("unsupported temp-root action")
    resolved = TempRootResolver().resolve(explicit, create=action == "prepare")
    return {
        "action": action,
        **resolved.audit_document(),
        "exists": resolved.path.is_dir(),
        "ok": True,
    }


def required_local_secret(name: str) -> str:
    """读取必需 secret，缺失时以可操作错误终止当前命令。"""

    value = _facade().KeyringSecretProvider().get_secret(name)
    if value is None:
        raise RuntimeError(f"required local secret {name!r} is not configured; use secret-set")
    return cast(str, value)


def optional_local_secret(name: str) -> str | None:
    """读取可选集成密钥，同时不把对应供应商变为必需依赖。"""

    runtime_cli = _facade()
    try:
        return cast(str | None, runtime_cli.KeyringSecretProvider().get_secret(name))
    except runtime_cli.SecretProviderError:
        return None


# ``cli.py`` 的历史私有名称由 facade 重新导出；模块内也保留私有别名，便于
# 旧的内部调用方在逐步迁移期间直接引用本模块。
_apply_integration_runtime_defaults = apply_integration_runtime_defaults
_configure_terminal_encoding = configure_terminal_encoding
_set_secret = set_secret
_secret_status = secret_status
_sqlite_runtime_status = sqlite_runtime_status
_temp_root = temp_root
_required_local_secret = required_local_secret
_optional_local_secret = optional_local_secret


__all__ = [
    "apply_integration_runtime_defaults",
    "configure_terminal_encoding",
    "optional_local_secret",
    "required_local_secret",
    "secret_status",
    "set_secret",
    "sqlite_runtime_status",
    "temp_root",
]
