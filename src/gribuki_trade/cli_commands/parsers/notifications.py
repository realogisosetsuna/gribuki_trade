from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    ReportKind,
    _non_negative_float,
    _positive_integer,
)


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 notifications 命令族。"""

    napcat_configure = commands.add_parser(
        "napcat-configure",
        help="lock down an extracted portable NapCat runtime and store random tokens",
    )
    napcat_configure.add_argument(
        "--runtime-dir",
        default=None,
        help="NapCat 目录；省略时读取 GUI 共享配置",
    )
    napcat_configure.add_argument("--onebot-port", type=_positive_integer, default=3000)
    napcat_configure.add_argument("--webui-port", type=_positive_integer, default=6099)
    napcat_configure.add_argument("--force", action="store_true")
    napcat_status = commands.add_parser(
        "napcat-status",
        help="check a loopback NapCat/OneBot endpoint without sending a message",
    )
    napcat_status.add_argument("--base-url", default=None)
    napcat_dispatch = commands.add_parser(
        "napcat-dispatch",
        help="dispatch a finite number of outbound-only NapCat outbox polling cycles",
    )
    napcat_dispatch.add_argument("--base-url", default=None)
    napcat_dispatch.add_argument(
        "--target-kind",
        choices=("private", "group"),
        required=True,
    )
    napcat_dispatch.add_argument("--target-id", required=True)
    napcat_dispatch.add_argument(
        "--outbox-path",
        default="runtime/notifications/outbox.sqlite3",
    )
    napcat_dispatch.add_argument(
        "--cycles",
        type=_positive_integer,
        default=1,
        help="finite number of polling cycles (default: 1)",
    )
    napcat_dispatch.add_argument(
        "--poll-interval",
        type=_non_negative_float,
        default=1.0,
        help="seconds between finite polling cycles (default: 1)",
    )
    napcat_test = commands.add_parser(
        "napcat-send-test",
        help="send one fixed, non-trading test message through local NapCat",
    )
    napcat_test.add_argument("--base-url", default=None)
    napcat_test.add_argument("--target-kind", choices=("private", "group"), required=True)
    napcat_test.add_argument("--target-id", required=True)
    napcat_test.add_argument("--confirm", choices=("SEND_TEST",), required=True)
    napcat_artifact = commands.add_parser(
        "napcat-send-artifact",
        help="validate and durably upload one allowlisted Markdown report",
    )
    napcat_artifact.add_argument("--base-url", default=None)
    napcat_artifact.add_argument("--target-kind", choices=("private", "group"), required=True)
    napcat_artifact.add_argument("--target-id", required=True)
    napcat_artifact.add_argument("--artifact-kind", choices=("file",), required=True)
    napcat_artifact.add_argument(
        "--report-kind",
        choices=tuple(kind.value for kind in ReportKind),
        required=True,
        help="stable report contract the Markdown artifact must satisfy",
    )
    napcat_artifact.add_argument(
        "--artifact-root",
        default="runtime/reports",
        help="trusted local report root; the artifact must remain inside it",
    )
    napcat_artifact.add_argument(
        "--artifact",
        required=True,
        help="path relative to --artifact-root, or an absolute path inside it",
    )
    napcat_artifact.add_argument(
        "--receipt-db",
        default="runtime/notifications/report-artifacts.sqlite3",
        help="durable delivery claim and provider receipt database",
    )
    napcat_artifact.add_argument("--confirm", choices=("SEND_ARTIFACT",), required=True)
