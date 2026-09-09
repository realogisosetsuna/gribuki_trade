from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    DEFAULT_ASHARE_WATCHLIST,
    Decimal,
    _iso_date,
    _macro_weight_decimal,
    _positive_float,
    _positive_integer,
)

from .ashare_common import GLOBAL_NEWS_FEEDS, add_optional_notify_target_arguments


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 close_research 命令族。"""

    research = commands.add_parser(
        "ashare-research-once",
        help="build and retain one evidence-gated A-share research recommendation",
    )
    research.add_argument("--symbol", default="600000.SH")
    research.add_argument("--interval", choices=("1m", "5m"), default="5m")
    research.add_argument("--lookback-minutes", type=_positive_integer, default=480)
    research.add_argument("--events-db", default="runtime/news/events.sqlite3")
    research.add_argument("--research-db", default="runtime/research/research.sqlite3")
    research.add_argument("--outbox-db", default="runtime/research/outbox.sqlite3")
    research.add_argument(
        "--macro",
        action="store_true",
        help="invoke the configured macro analyzer; otherwise technical-only",
    )
    research.add_argument(
        "--macro-provider",
        choices=("deepseek", "openai"),
        default=None,
    )
    research.add_argument(
        "--model",
        help="provider model ID; defaults to deepseek-v4-flash or gpt-5.6",
    )
    add_optional_notify_target_arguments(research)
    close_research = commands.add_parser(
        "ashare-close-research-once",
        help="deeply analyze completed A-share daily data and news for the next session",
    )
    close_research.add_argument("--symbol", default="510300.SH")
    close_research.add_argument(
        "--history-days",
        type=_positive_integer,
        default=450,
        help="calendar-day lookback for unadjusted daily bars (default: 450)",
    )
    close_research.add_argument(
        "--session-date",
        type=_iso_date,
        help="explicit latest completed trading session (still calendar-validated)",
    )
    close_research.add_argument(
        "--next-session",
        type=_iso_date,
        help="explicit target trading session (still calendar-validated)",
    )
    close_research.add_argument("--news-runtime-dir", default="runtime/news")
    close_research.add_argument(
        "--research-db",
        default="runtime/research/research.sqlite3",
    )
    close_research.add_argument(
        "--market-evidence-dir",
        default="runtime/research/market_evidence",
    )
    close_research.add_argument(
        "--outbox-db",
        default="runtime/notifications/outbox.sqlite3",
    )
    close_research.add_argument(
        "--report-dir",
        default="runtime/reports",
        help="write a UTF-8 Markdown report and paginated PNG pages here",
    )
    close_research.add_argument(
        "--news-feed",
        action="append",
        choices=GLOBAL_NEWS_FEEDS,
        dest="news_feeds",
        help="repeat to override the default four global AKShare feeds",
    )
    close_research.add_argument(
        "--refresh-news",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="refresh multi-source news before analysis (default: enabled)",
    )
    close_research.add_argument(
        "--search-discovery",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "add configured Tavily/SearXNG discovery queries; single-provider "
            "hints are retained but cannot enter the macro score"
        ),
    )
    close_research.add_argument(
        "--searxng-url",
        help="optional trusted public HTTPS SearXNG base URL with JSON enabled",
    )
    close_research.add_argument(
        "--macro",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="run configured evidence-bound dual-track macro analysis (default: enabled)",
    )
    close_research.add_argument(
        "--macro-provider",
        choices=("deepseek", "openai"),
        default=None,
    )
    close_research.add_argument("--model")
    close_research.add_argument(
        "--macro-weight",
        type=_macro_weight_decimal,
        default=Decimal("0.25"),
        help=(
            "bounded macro contribution to the uncalibrated combined score (0..0.40; default: 0.25)"
        ),
    )
    close_research.add_argument(
        "--held",
        action="store_true",
        help="evaluate reduce triggers for an already-held position",
    )
    add_optional_notify_target_arguments(close_research)
    close_batch = commands.add_parser(
        "ashare-close-research-batch",
        help=(
            "sequentially run the evidence-gated close analysis for explicit or "
            "ACTIVE candidate symbols"
        ),
    )
    close_batch.add_argument("--symbol", action="append", dest="symbols")
    close_batch.add_argument(
        "--candidate-db",
        help="also include ACTIVE symbols from this candidate event store",
    )
    close_batch.add_argument(
        "--limit",
        type=_positive_integer,
        default=10,
        help="maximum symbols analyzed in this bounded run (default: 10)",
    )
    close_batch.add_argument("--history-days", type=_positive_integer, default=450)
    close_batch.add_argument("--session-date", type=_iso_date)
    close_batch.add_argument("--next-session", type=_iso_date)
    close_batch.add_argument("--news-runtime-dir", default="runtime/news")
    close_batch.add_argument("--research-db", default="runtime/research/research.sqlite3")
    close_batch.add_argument("--market-evidence-dir", default="runtime/research/market_evidence")
    close_batch.add_argument("--outbox-db", default="runtime/notifications/outbox.sqlite3")
    close_batch.add_argument("--report-dir", default="runtime/reports")
    close_batch.add_argument(
        "--news-feed",
        action="append",
        choices=GLOBAL_NEWS_FEEDS,
        dest="news_feeds",
    )
    close_batch.add_argument("--refresh-news", action=argparse.BooleanOptionalAction, default=True)
    close_batch.add_argument(
        "--search-discovery", action=argparse.BooleanOptionalAction, default=True
    )
    close_batch.add_argument("--searxng-url")
    close_batch.add_argument("--macro", action=argparse.BooleanOptionalAction, default=True)
    close_batch.add_argument("--macro-provider", choices=("deepseek", "openai"), default=None)
    close_batch.add_argument("--model")
    close_batch.add_argument(
        "--macro-weight",
        type=_macro_weight_decimal,
        default=Decimal("0.25"),
    )
    close_batch.add_argument(
        "--held-symbol",
        action="append",
        dest="held_symbols",
        help="repeat for symbols that should evaluate position-reduction rules",
    )
    add_optional_notify_target_arguments(close_batch)
    close_batch.add_argument(
        "--output",
        help="optional JSON destination written atomically; stdout is always retained",
    )
    research_watch = commands.add_parser(
        "ashare-research-watch",
        help="run bounded multi-symbol A-share research polling cycles",
    )
    research_watch.add_argument("--symbol", action="append", dest="symbols")
    research_watch.add_argument("--watchlist", default=DEFAULT_ASHARE_WATCHLIST)
    research_watch.add_argument(
        "--watchlist-all",
        action="store_true",
        help="use all configured symbols instead of the balanced default subset",
    )
    research_watch.add_argument("--interval", choices=("1m", "5m"), default="5m")
    research_watch.add_argument(
        "--lookback-minutes",
        type=_positive_integer,
        default=480,
    )
    research_watch.add_argument(
        "--interval-seconds",
        type=_positive_float,
        default=60.0,
    )
    research_watch.add_argument(
        "--cycles",
        type=_positive_integer,
        default=3,
        help="bounded polling cycles; a future application runtime owns continuity",
    )
    research_watch.add_argument("--events-db", default="runtime/news/events.sqlite3")
    research_watch.add_argument(
        "--research-db",
        default="runtime/research/research.sqlite3",
    )
    research_watch.add_argument(
        "--outbox-db",
        default="runtime/research/outbox.sqlite3",
    )
    research_watch.add_argument(
        "--macro",
        action="store_true",
        help="invoke the configured macro analyzer for each symbol",
    )
    research_watch.add_argument(
        "--macro-provider",
        choices=("deepseek", "openai"),
        default=None,
    )
    research_watch.add_argument(
        "--model",
        help="provider model ID; defaults to deepseek-v4-flash or gpt-5.6",
    )
    add_optional_notify_target_arguments(research_watch)
    research_watch.add_argument(
        "--candidate-db",
        help="merge ACTIVE symbols from the unified candidate event store",
    )
    research_watch.add_argument(
        "--candidates-only",
        action="store_true",
        help="monitor only ACTIVE candidate-store symbols (requires --candidate-db)",
    )
