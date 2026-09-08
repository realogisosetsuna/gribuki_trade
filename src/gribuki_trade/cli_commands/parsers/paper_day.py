from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    Decimal,
    _iso_date,
    _positive_decimal,
    _positive_integer,
    _positive_integer_or_unlimited,
)


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 paper_day 命令族。"""

    paper_day = commands.add_parser(
        "ashare-paper-day",
        help="run or inspect one realistic broker-free A-share PAPER trading day",
    )
    paper_day.add_argument("action", choices=("run", "status", "report", "summary"))
    paper_day.add_argument(
        "--runtime-dir",
        default="runtime/paper/day",
        help="session parent directory; each trading date receives an isolated child",
    )
    paper_day.add_argument(
        "--session-date",
        type=_iso_date,
        help="Shanghai trading date; defaults to today's local date",
    )
    paper_day.add_argument("--account", default="ashare-paper-day")
    paper_day.add_argument(
        "--initial-cash",
        type=_positive_decimal,
        default=Decimal("200000"),
    )
    paper_day.add_argument("--target-kind", choices=("private", "group"))
    paper_day.add_argument("--target-id")
    paper_day.add_argument(
        "--base-url",
        default=None,
        help="OneBot 地址；省略时读取 GUI 共享配置",
    )
    paper_day.add_argument(
        "--confirm",
        choices=("PAPER_DAY",),
        help="run requires exactly PAPER_DAY; no real broker is ever contacted",
    )
    paper_day.add_argument(
        "--recover-after-abort",
        action="store_true",
        help=(
            "explicitly authorize the same append-only run to continue after "
            "a terminal DAY_ABORTED event"
        ),
    )
    paper_day.add_argument(
        "--maximum-positions",
        type=_positive_integer,
        default=None,
        help=(
            "optional positive count-based position circuit breaker; omitted "
            "means no hard position-count limit (capital/risk limits still apply)"
        ),
    )
    paper_day.add_argument(
        "--confirm-risk-policy-change",
        choices=("PAPER_RISK_POLICY_CHANGE",),
        help=(
            "strong one-time confirmation required to migrate an already "
            "journalled intraday risk policy"
        ),
    )
    paper_day.add_argument(
        "--report-artifact-recovery-action",
        choices=(
            "MARK_SENT_AFTER_PROVIDER_VERIFICATION",
            "RESEND_AFTER_PROVIDER_NON_RECEIPT_VERIFICATION",
        ),
        help=(
            "only for an AMBIGUOUS DAILY_REVIEW attachment after the operator "
            "has checked the provider-side result"
        ),
    )
    paper_day.add_argument(
        "--report-artifact-provider-id",
        help="provider file ID required only when recovery marks a verified receipt as sent",
    )
    paper_day.add_argument(
        "--confirm-report-artifact-recovery",
        choices=("PAPER_REPORT_ARTIFACT_RECOVERY",),
        help="strong confirmation required for either ambiguous attachment recovery action",
    )
    paper_day.add_argument(
        "--intraday-llm",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "require background dual-track LLM evidence review for new PAPER buys; "
            "--no-intraday-llm is an audited monitor/sell-only mode that blocks "
            "every new buy"
        ),
    )
    paper_day.add_argument(
        "--intraday-llm-review-top-n",
        type=_positive_integer,
        default=6,
        help="maximum ranked surveillance candidates reviewed per refresh",
    )
    paper_day.add_argument(
        "--intraday-llm-review-ttl-minutes",
        type=_positive_integer,
        default=20,
        help="positive lifetime of one journal-accepted candidate review",
    )
    paper_day.add_argument(
        "--intraday-llm-max-calls",
        type=_positive_integer_or_unlimited,
        default=None,
        metavar="POSITIVE_INT|unlimited",
        help="optional per-session provider-call cap (default: unlimited)",
    )
    paper_day.add_argument(
        "--intraday-llm-events-db",
        default="runtime/news/events.sqlite3",
        help="existing SQLite event database frozen read-only at startup",
    )
    paper_day.add_argument(
        "--intraday-llm-provider",
        choices=("deepseek", "openai"),
        default=None,
        help="LLM provider；省略时读取 GUI 共享配置",
    )
    paper_day.add_argument(
        "--intraday-llm-model",
        default=None,
        help="provider 模型；省略时读取 GUI 共享配置",
    )
