from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    Decimal,
    _iso_date,
    _macro_weight_decimal,
    _non_negative_float,
    _positive_integer,
)


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 post_close 命令族。"""

    post_close = commands.add_parser(
        "ashare-post-close",
        help="run or inspect one idempotent same-day A-share post-close review",
    )
    post_close.add_argument(
        "action",
        choices=("run", "status", "report"),
    )
    post_close.add_argument("--runtime-dir", default="runtime/paper/day")
    post_close.add_argument("--session-date", type=_iso_date)
    post_close.add_argument("--account", default="ashare-paper-day")
    post_close.add_argument("--target-kind", choices=("private", "group"))
    post_close.add_argument("--target-id")
    post_close.add_argument(
        "--base-url",
        default=None,
        help="OneBot 地址；省略时读取 GUI 共享配置",
    )
    post_close.add_argument(
        "--candidate-db",
        default="runtime/research/candidates.sqlite3",
    )
    post_close.add_argument("--history-days", type=_positive_integer, default=540)
    post_close.add_argument("--news-runtime-dir", default="runtime/news")
    post_close.add_argument(
        "--research-db",
        default="runtime/research/research.sqlite3",
    )
    post_close.add_argument(
        "--market-evidence-dir",
        default="runtime/research/market_evidence",
    )
    post_close.add_argument(
        "--news-feed",
        action="append",
        choices=(
            "global_eastmoney",
            "global_cailianpress",
            "global_sina",
            "global_10jqka",
        ),
        dest="news_feeds",
    )
    post_close.add_argument(
        "--refresh-news",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    post_close.add_argument(
        "--search-discovery",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    post_close.add_argument("--searxng-url")
    post_close.add_argument(
        "--macro",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="enable configured evidence-bound dual-track close research (default: enabled)",
    )
    post_close.add_argument(
        "--macro-provider",
        choices=("deepseek", "openai"),
        default=None,
    )
    post_close.add_argument("--model")
    post_close.add_argument(
        "--macro-weight",
        type=_macro_weight_decimal,
        default=Decimal("0.25"),
    )
    post_close.add_argument(
        "--dispatch-cycles",
        type=_positive_integer,
        default=3,
    )
    post_close.add_argument(
        "--dispatch-poll-interval",
        type=_non_negative_float,
        default=2.0,
    )
    post_close.add_argument(
        "--recover-analysis",
        action="store_true",
        help="explicitly authorize re-analysis after an ambiguous interrupted phase",
    )
    post_close.add_argument(
        "--recover-delivery",
        action="store_true",
        help="explicitly authorize a possibly duplicate delivery after ambiguity",
    )
    post_close.add_argument(
        "--confirm",
        choices=("POST_CLOSE",),
        help="run requires POST_CLOSE",
    )
