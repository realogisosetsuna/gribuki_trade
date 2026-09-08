from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    _iso_date,
    _iso_datetime,
    _non_negative_decimal,
    _positive_decimal,
    _positive_integer,
)


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 research 命令族。"""

    research_runs = commands.add_parser(
        "ashare-research-runs",
        help="read immutable A-share operational research run outputs and lineage",
    )
    research_runs.add_argument("action", choices=("list", "get"))
    research_runs.add_argument("--run-db", default="runtime/research/runs.sqlite3")
    research_runs.add_argument("--run-id")
    research_runs.add_argument("--run-type")
    research_runs.add_argument("--limit", type=_positive_integer, default=100)
    candidates = commands.add_parser(
        "ashare-candidates",
        help="inspect or mutate the unified research-only candidate universe",
    )
    candidates.add_argument("action", choices=("list", "add", "cool", "activate", "remove"))
    candidates.add_argument("--symbol")
    candidates.add_argument("--reason", default="MANUAL_RESEARCH_SELECTION")
    candidates.add_argument("--candidate-db", default="runtime/research/candidates.sqlite3")
    candidates.add_argument("--cooling-minutes", type=_positive_integer, default=240)
    candidates.add_argument("--limit", type=_positive_integer, default=500)
    review = commands.add_parser(
        "ashare-review",
        help="open and resolve broker-independent recommendation research reviews",
    )
    review.add_argument("action", choices=("open", "list", "get", "confirm", "reject", "cancel"))
    review.add_argument("--review-db", default="runtime/research/reviews.sqlite3")
    review.add_argument("--research-db", default="runtime/research/research.sqlite3")
    review.add_argument("--candidate-db", default="runtime/research/candidates.sqlite3")
    review.add_argument("--recommendation-id")
    review.add_argument("--case-id")
    review.add_argument(
        "--reason",
        action="append",
        help="repeatable bounded audit reason code (never an execution instruction)",
    )
    review.add_argument("--limit", type=_positive_integer, default=100)
    review.add_argument(
        "--confirm",
        choices=("RESEARCH_ONLY",),
        help="required for confirm; confirms research only and never authorizes execution",
    )
    paper = commands.add_parser(
        "ashare-paper",
        help="manage the broker-free, append-only A-share PAPER account ledger",
    )
    paper.add_argument("action", choices=("open", "snapshot", "rollover", "fill", "fills"))
    paper.add_argument("--ledger-db", default="runtime/paper/ashare-paper.sqlite3")
    paper.add_argument("--account", default="personal-paper")
    paper.add_argument("--initial-cash", type=_non_negative_decimal)
    paper.add_argument("--session-date", type=_iso_date)
    paper.add_argument("--symbol")
    paper.add_argument("--side", choices=("BUY", "SELL"))
    paper.add_argument("--quantity", type=_positive_integer)
    paper.add_argument("--price", type=_positive_decimal)
    paper.add_argument("--instrument", choices=("STOCK", "ETF"))
    paper.add_argument("--source", choices=("MANUAL", "SIMULATED"), default="MANUAL")
    paper.add_argument("--fill-id")
    paper.add_argument("--executed-at", type=_iso_datetime)
    paper.add_argument("--external-order-id")
    paper.add_argument("--note")
    paper.add_argument("--actual-commission", type=_non_negative_decimal)
    paper.add_argument("--actual-transfer-fee", type=_non_negative_decimal)
    paper.add_argument("--actual-stamp-tax", type=_non_negative_decimal)
