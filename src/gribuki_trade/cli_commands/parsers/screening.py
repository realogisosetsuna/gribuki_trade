from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    Decimal,
    _non_negative_decimal,
    _non_negative_integer,
    _positive_integer,
)


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 screening 命令族。"""

    market_screen = commands.add_parser(
        "ashare-market-screen-once",
        help=("run one post-close, non-trading three-layer full-market A-share screen"),
    )
    market_screen.add_argument(
        "--top-n",
        type=_positive_integer,
        default=30,
        help="maximum ranked candidates returned for deep research (default: 30)",
    )
    market_screen.add_argument(
        "--factor-budget",
        type=_positive_integer,
        default=300,
        help="maximum hard-filter survivors enriched with daily history (default: 300)",
    )
    market_screen.add_argument(
        "--min-listing-days",
        type=_non_negative_integer,
        default=250,
        help="minimum known listing age in calendar days (default: 250)",
    )
    market_screen.add_argument(
        "--min-session-amount-cny",
        type=_non_negative_decimal,
        default=Decimal("20000000"),
        help="minimum current-session accumulated amount in CNY (default: 20000000)",
    )
    market_screen.add_argument(
        "--min-average-amount-20-cny",
        type=_non_negative_decimal,
        default=Decimal("50000000"),
        help="minimum historical 20-session average amount in CNY (default: 50000000)",
    )
    market_screen.add_argument(
        "--min-market-cap-cny",
        type=_non_negative_decimal,
        default=Decimal("2000000000"),
        help="minimum provider total market capitalization in CNY (default: 2000000000)",
    )
    market_screen.add_argument(
        "--output",
        help="optional JSON destination written atomically; stdout is always retained",
    )
    market_screen.add_argument(
        "--candidate-db",
        default="runtime/research/candidates.sqlite3",
        help="append Top-N discoveries to this candidate event store",
    )
    market_screen.add_argument(
        "--run-db",
        default="runtime/research/runs.sqlite3",
        help="append the canonical screening output and lineage to this run registry",
    )
    market_screen.add_argument(
        "--no-store-run",
        action="store_true",
        help="return output without appending the screening run registry",
    )
    market_screen.add_argument(
        "--no-store-candidates",
        action="store_true",
        help="return screening output without mutating the candidate event store",
    )
    intraday_scan = commands.add_parser(
        "ashare-intraday-scan-once",
        help="run one current-session full-market anomaly scan and update candidates",
    )
    intraday_scan.add_argument("--top-n", type=_positive_integer, default=30)
    intraday_scan.add_argument(
        "--min-session-amount-cny",
        type=_non_negative_decimal,
        default=Decimal("2000000"),
    )
    intraday_scan.add_argument("--candidate-db", default="runtime/research/candidates.sqlite3")
    intraday_scan.add_argument("--no-store-candidates", action="store_true")
    intraday_scan.add_argument("--run-db", default="runtime/research/runs.sqlite3")
    intraday_scan.add_argument("--no-store-run", action="store_true")
    intraday_scan.add_argument(
        "--output",
        help="optional JSON destination written atomically; stdout is always retained",
    )
