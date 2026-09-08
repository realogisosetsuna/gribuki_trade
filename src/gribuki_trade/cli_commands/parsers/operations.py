from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    KNOWN_SECRET_NAMES,
)


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 operations 命令族。"""

    secret_set = commands.add_parser(
        "secret-set",
        help="store a known integration secret using a no-echo local prompt",
    )
    secret_set.add_argument("name", choices=KNOWN_SECRET_NAMES)
    commands.add_parser(
        "secret-status",
        help="show which known local secrets are configured without revealing values",
    )
    commands.add_parser(
        "deepseek-configure",
        help="store the DeepSeek API key using a no-echo local prompt",
    )
    commands.add_parser(
        "deepseek-status",
        help="perform an authenticated read-only DeepSeek model availability check",
    )
    commands.add_parser(
        "sqlite-runtime-status",
        help="report whether the bundled SQLite is safe for shared WAL deployment",
    )
    temp_root = commands.add_parser(
        "temp-root",
        help="resolve or prepare the centralized disposable-work root",
    )
    temp_root.add_argument("action", choices=("status", "prepare"))
    temp_root.add_argument(
        "--temp-dir",
        help=(
            "explicit scratch root; otherwise GRIBUKI_TRADE_TMP_DIR and then runtime/tmp are used"
        ),
    )
