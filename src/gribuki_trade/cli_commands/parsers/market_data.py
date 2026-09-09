from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    DEFAULT_ASHARE_WATCHLIST,
    _non_negative_integer,
    _positive_float,
    _positive_integer,
)

from .ashare_common import ALL_NEWS_FEEDS, GLOBAL_NEWS_FEEDS


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 market_data 命令族。"""

    snapshot = commands.add_parser(
        "ashare-snapshot",
        help="fetch one research-grade AKShare public-web quote snapshot",
    )
    snapshot.add_argument("--symbol", default="600000.SH")
    bars = commands.add_parser(
        "ashare-bars",
        help="fetch completed AKShare 1m/5m provider-aggregated bars",
    )
    bars.add_argument("--symbol", default="600000.SH")
    bars.add_argument("--interval", choices=("1m", "5m"), default="5m")
    bars.add_argument("--lookback-minutes", type=_positive_integer, default=480)
    daily = commands.add_parser(
        "ashare-daily",
        help="fetch anonymous BaoStock daily research data",
    )
    daily.add_argument("--symbol", default="600000.SH")
    daily.add_argument("--days", type=_positive_integer, default=60)
    news = commands.add_parser(
        "ashare-news",
        help="collect one AKShare public-news lead batch and archive its raw table",
    )
    news.add_argument(
        "--feed",
        choices=ALL_NEWS_FEEDS,
        default="global_sina",
    )
    news.add_argument("--symbol")
    news.add_argument("--limit", type=_positive_integer, default=10)
    news.add_argument("--archive-dir", default="runtime/news/raw")
    news_watch = commands.add_parser(
        "ashare-news-watch",
        help="continuously archive configured AKShare news leads until interrupted",
    )
    news_watch.add_argument(
        "--feed",
        action="append",
        choices=GLOBAL_NEWS_FEEDS,
        dest="feeds",
    )
    news_watch.add_argument("--symbol", action="append", dest="symbols")
    news_watch.add_argument(
        "--interval-seconds",
        type=_positive_float,
        default=60.0,
    )
    news_watch.add_argument(
        "--cycles",
        type=_non_negative_integer,
        default=0,
        help="0 means run until interrupted",
    )
    news_watch.add_argument("--runtime-dir", default="runtime/news")
    source_health = commands.add_parser(
        "ashare-source-health",
        help="summarize retained A-share source health observations",
    )
    source_health.add_argument("--source-id")
    source_health.add_argument("--operation", default="news_collect")
    source_health.add_argument("--days", type=_positive_integer, default=20)
    source_health.add_argument("--runtime-dir", default="runtime/news")
    disclosures = commands.add_parser(
        "ashare-disclosures",
        help="collect and retain one symbol's CNINFO disclosure index through AKShare",
    )
    disclosures.add_argument("--symbol", default="600000.SH")
    disclosures.add_argument("--lookback-days", type=_positive_integer, default=14)
    disclosures.add_argument("--category", default="")
    disclosures.add_argument("--runtime-dir", default="runtime/news")
    watchlist = commands.add_parser(
        "ashare-watchlist",
        help="validate and display the configured A-share research coverage pool",
    )
    watchlist.add_argument("--config", default=DEFAULT_ASHARE_WATCHLIST)
