"""Operational command-line entry points for local integration checks."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
from collections.abc import Awaitable, Sequence
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from datetime import time as datetime_time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import uuid4
from zoneinfo import ZoneInfo

from gribuki_trade.adapters.binance import (
    BINANCE_COIN_FUTURES_DEMO_API_KEY_SECRET,
    BINANCE_COIN_FUTURES_DEMO_SECRET_KEY_SECRET,
    BINANCE_LIVE_API_KEY_SECRET,
    BINANCE_LIVE_SECRET_KEY_SECRET,
    BINANCE_TESTNET_API_KEY_SECRET,
    BINANCE_TESTNET_SECRET_KEY_SECRET,
    BINANCE_USDS_FUTURES_DEMO_API_KEY_SECRET,
    BINANCE_USDS_FUTURES_DEMO_SECRET_KEY_SECRET,
    BinanceAccount,
    BinanceEnvironment,
    BinanceExecutionReport,
    BinanceSpotGateway,
    BinanceSpotUserDataStream,
    BinanceTransportError,
    binance_futures_demo_secret_names,
    load_binance_credentials,
    load_binance_futures_demo_credentials,
)
from gribuki_trade.adapters.llm import (
    DEEPSEEK_API_KEY_SECRET,
    DEFAULT_DEEPSEEK_MODEL,
    OPENAI_API_KEY_SECRET,
)
from gribuki_trade.adapters.notifiers import NAPCAT_ACCESS_TOKEN_SECRET
from gribuki_trade.adapters.schwab import (
    SCHWAB_CLIENT_ID_SECRET,
    SCHWAB_CLIENT_SECRET_SECRET,
    SCHWAB_OAUTH_TOKEN_SECRET,
)
from gribuki_trade.domain.events import (
    DISCOVERY_CONFIRMED_EVENT_TYPE,
    DISCOVERY_HINT_EVENT_TYPE,
    NormalizedEvent,
)
from gribuki_trade.domain.instruments import ResearchInstrumentProfile
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.features import CloseInstrumentType
from gribuki_trade.napcat_setup import NAPCAT_WEBUI_TOKEN_SECRET
from gribuki_trade.ports.news import NewsSource
from gribuki_trade.security import KeyringSecretProvider, SecretProviderError
from gribuki_trade.services.binance_execution import (
    BinanceSpotTestnetExecutionService,
    BinanceStartupReconciliation,
)
from gribuki_trade.sqlite_runtime import sqlite_runtime_status
from gribuki_trade.trading import SQLiteOrderManagementStore

if TYPE_CHECKING:
    from gribuki_trade.ports.ashare_screening import AsyncAShareScreeningData
    from gribuki_trade.ports.ashare_surveillance import AsyncAShareIntradayUniverseData
    from gribuki_trade.storage.research_runs import StoredResearchRun
    from gribuki_trade.strategy_lab.discovery import FactorCandidateInventory

DEFAULT_TESTNET_ACCOUNT = "binance-testnet"
DEFAULT_ASHARE_WATCHLIST = "config/ashare_research_watchlist.toml"
TAVILY_API_KEY_SECRET = "search.tavily.api_key"
SEARXNG_BEARER_TOKEN_SECRET = "search.searxng.bearer_token"
KNOWN_SECRET_NAMES = (
    BINANCE_TESTNET_API_KEY_SECRET,
    BINANCE_TESTNET_SECRET_KEY_SECRET,
    BINANCE_LIVE_API_KEY_SECRET,
    BINANCE_LIVE_SECRET_KEY_SECRET,
    BINANCE_USDS_FUTURES_DEMO_API_KEY_SECRET,
    BINANCE_USDS_FUTURES_DEMO_SECRET_KEY_SECRET,
    BINANCE_COIN_FUTURES_DEMO_API_KEY_SECRET,
    BINANCE_COIN_FUTURES_DEMO_SECRET_KEY_SECRET,
    SCHWAB_CLIENT_ID_SECRET,
    SCHWAB_CLIENT_SECRET_SECRET,
    DEEPSEEK_API_KEY_SECRET,
    OPENAI_API_KEY_SECRET,
    NAPCAT_ACCESS_TOKEN_SECRET,
    TAVILY_API_KEY_SECRET,
    SEARXNG_BEARER_TOKEN_SECRET,
)
KNOWN_SECRET_STATUS_NAMES = (
    *KNOWN_SECRET_NAMES,
    SCHWAB_OAUTH_TOKEN_SECRET,
    NAPCAT_WEBUI_TOKEN_SECRET,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gribuki-trade")
    commands = parser.add_subparsers(dest="command")

    commands.add_parser("gui", help="start the PAPER desktop GUI")

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

    status = commands.add_parser(
        "binance-testnet-status",
        help="perform authenticated read-only Spot Testnet checks",
    )
    status.add_argument("--symbol", default="BTCUSDT")

    order_test = commands.add_parser(
        "binance-testnet-order-test",
        help="validate a virtual order without entering the matching engine",
    )
    _add_order_arguments(order_test)

    cycle = commands.add_parser(
        "binance-testnet-cycle",
        help="place, query, and cancel one virtual Spot Testnet order",
    )
    _add_order_arguments(cycle)
    cycle.add_argument(
        "--confirm",
        required=True,
        choices=("TESTNET",),
        help="must be exactly TESTNET; this command never targets LIVE",
    )

    oms_cycle = commands.add_parser(
        "binance-testnet-oms-cycle",
        help="run one durable, reconciled Spot Testnet submit/cancel cycle",
    )
    _add_order_arguments(oms_cycle)
    oms_cycle.add_argument(
        "--database",
        default="runtime/binance/testnet-oms.sqlite3",
        help="durable SQLite OMS database",
    )
    oms_cycle.add_argument(
        "--confirm",
        required=True,
        choices=("TESTNET",),
        help="must be exactly TESTNET; this command never targets LIVE",
    )

    oms_fill = commands.add_parser(
        "binance-testnet-oms-fill",
        help="place one marketable BUY through the durable Spot Testnet OMS",
    )
    _add_order_arguments(oms_fill)
    oms_fill.add_argument(
        "--database",
        default="runtime/binance/testnet-oms-fill.sqlite3",
        help="durable SQLite OMS database",
    )
    oms_fill.add_argument(
        "--confirm",
        required=True,
        choices=("TESTNET_FILL",),
        help="must be exactly TESTNET_FILL; this command never targets LIVE",
    )

    history = commands.add_parser(
        "binance-history-sync",
        help="archive completed public Binance Spot klines for deterministic replay",
    )
    history.add_argument("--symbol", default="BTCUSDT")
    history.add_argument(
        "--interval",
        choices=("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d"),
        default="5m",
    )
    history.add_argument("--days", type=_positive_integer, default=30)
    history.add_argument(
        "--environment",
        choices=("LIVE", "TESTNET"),
        default="LIVE",
        help="LIVE uses public production market data only and never sends orders",
    )
    history.add_argument("--database", default="runtime/binance/market.sqlite3")

    backtest = commands.add_parser(
        "binance-backtest",
        help="run the deterministic moving-average baseline on archived Spot klines",
    )
    backtest.add_argument("--symbol", default="BTCUSDT")
    backtest.add_argument(
        "--interval",
        choices=("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d"),
        default="5m",
    )
    backtest.add_argument("--environment", choices=("LIVE", "TESTNET"), default="LIVE")
    backtest.add_argument("--database", default="runtime/binance/market.sqlite3")
    backtest.add_argument("--base-asset", default="BTC")
    backtest.add_argument("--quote-asset", default="USDT")
    backtest.add_argument(
        "--initial-quote",
        type=_positive_decimal,
        default=Decimal("10000"),
    )
    backtest.add_argument("--fast-window", type=_positive_integer, default=20)
    backtest.add_argument("--slow-window", type=_positive_integer, default=50)
    backtest.add_argument(
        "--target-position",
        type=_unit_fraction_decimal,
        default=Decimal("0.60"),
    )
    backtest.add_argument(
        "--rebalance-band",
        type=_unit_fraction_decimal,
        default=Decimal("0.02"),
        help="maximum target-weight drift before rebalancing (default: 0.02)",
    )
    backtest.add_argument(
        "--maker-fee",
        type=_non_negative_decimal,
        default=Decimal("0.001"),
    )
    backtest.add_argument(
        "--taker-fee",
        type=_non_negative_decimal,
        default=Decimal("0.001"),
    )
    backtest.add_argument(
        "--slippage",
        type=_non_negative_decimal,
        default=Decimal("0.0005"),
    )

    shadow = commands.add_parser(
        "binance-shadow-run",
        help="run a credential-free Spot strategy against public data and local PAPER execution",
    )
    shadow.add_argument("--symbol", choices=("BTCUSDT", "ETHUSDT"), default="BTCUSDT")
    shadow.add_argument(
        "--interval",
        choices=("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d"),
        default="1m",
    )
    shadow.add_argument(
        "--environment",
        choices=("LIVE", "TESTNET"),
        default="LIVE",
        help="selects public market data only; remote order submission is always disabled",
    )
    shadow.add_argument("--database", default="runtime/binance/shadow-oms.sqlite3")
    shadow.add_argument(
        "--closed-bars",
        type=_non_negative_integer,
        default=3,
        help="stop after this many closed bars; zero runs until interrupted",
    )
    shadow.add_argument(
        "--initial-quote",
        type=_positive_decimal,
        default=Decimal("10000"),
    )
    shadow.add_argument("--fast-window", type=_positive_integer, default=20)
    shadow.add_argument("--slow-window", type=_positive_integer, default=50)
    shadow.add_argument(
        "--target-position",
        type=_unit_fraction_decimal,
        default=Decimal("0.60"),
    )
    shadow.add_argument(
        "--rebalance-band",
        type=_unit_fraction_decimal,
        default=Decimal("0.02"),
    )
    shadow.add_argument(
        "--maximum-order-notional",
        type=_positive_decimal,
        default=Decimal("100"),
    )

    futures_status = commands.add_parser(
        "binance-futures-demo-status",
        help="check public USD-M or COIN-M Binance Demo Trading endpoints",
    )
    futures_status.add_argument(
        "--product",
        choices=("USDS_FUTURES", "COIN_FUTURES"),
        default="USDS_FUTURES",
    )
    futures_status.add_argument("--symbol")
    futures_status.add_argument(
        "--validate-order-test",
        action="store_true",
        help="call the official Demo order/test endpoint; it never creates an order",
    )
    futures_status.add_argument(
        "--confirm",
        choices=("FUTURES_DEMO_TEST",),
        help="required with --validate-order-test",
    )
    futures_status.add_argument(
        "--quantity",
        type=_positive_decimal,
        help=(
            "USD-M base-asset quantity or COIN-M integer contract count; "
            "defaults to 0.001 BTC or 1 contract"
        ),
    )
    futures_status.add_argument("--side", choices=("BUY", "SELL"), default="BUY")

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
        choices=(
            "individual_eastmoney",
            "global_eastmoney",
            "global_cailianpress",
            "global_sina",
            "global_10jqka",
        ),
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
        choices=(
            "global_eastmoney",
            "global_cailianpress",
            "global_sina",
            "global_10jqka",
        ),
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

    market_screen = commands.add_parser(
        "ashare-market-screen-once",
        help=(
            "run one post-close, non-trading three-layer full-market A-share screen"
        ),
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
    intraday_scan.add_argument(
        "--candidate-db", default="runtime/research/candidates.sqlite3"
    )
    intraday_scan.add_argument("--no-store-candidates", action="store_true")
    intraday_scan.add_argument(
        "--run-db", default="runtime/research/runs.sqlite3"
    )
    intraday_scan.add_argument("--no-store-run", action="store_true")
    intraday_scan.add_argument(
        "--output",
        help="optional JSON destination written atomically; stdout is always retained",
    )

    factor_discover = commands.add_parser(
        "strategy-factor-discover",
        help="expand the bounded research-only factor grammar without market data",
    )
    factor_discover.add_argument(
        "--max-trials",
        type=_positive_integer,
        default=100,
        help="hard expansion budget; the command fails instead of truncating",
    )
    factor_discover.add_argument(
        "--output",
        help="optional JSON destination written atomically; stdout is always retained",
    )

    research_runs = commands.add_parser(
        "ashare-research-runs",
        help="read immutable A-share operational research run outputs and lineage",
    )
    research_runs.add_argument("action", choices=("list", "get"))
    research_runs.add_argument(
        "--run-db", default="runtime/research/runs.sqlite3"
    )
    research_runs.add_argument("--run-id")
    research_runs.add_argument("--run-type")
    research_runs.add_argument("--limit", type=_positive_integer, default=100)

    candidates = commands.add_parser(
        "ashare-candidates",
        help="inspect or mutate the unified research-only candidate universe",
    )
    candidates.add_argument(
        "action", choices=("list", "add", "cool", "activate", "remove")
    )
    candidates.add_argument("--symbol")
    candidates.add_argument("--reason", default="MANUAL_RESEARCH_SELECTION")
    candidates.add_argument(
        "--candidate-db", default="runtime/research/candidates.sqlite3"
    )
    candidates.add_argument(
        "--cooling-minutes", type=_positive_integer, default=240
    )
    candidates.add_argument("--limit", type=_positive_integer, default=500)

    review = commands.add_parser(
        "ashare-review",
        help="open and resolve broker-independent recommendation research reviews",
    )
    review.add_argument(
        "action", choices=("open", "list", "get", "confirm", "reject", "cancel")
    )
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
        default="deepseek",
    )
    research.add_argument(
        "--model",
        help="provider model ID; defaults to deepseek-v4-flash or gpt-5.6",
    )
    research.add_argument("--notify-target-kind", choices=("private", "group"))
    research.add_argument("--notify-target-id")

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
        choices=(
            "global_eastmoney",
            "global_cailianpress",
            "global_sina",
            "global_10jqka",
        ),
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
        help="run evidence-bound DeepSeek macro analysis (default: enabled)",
    )
    close_research.add_argument(
        "--macro-provider",
        choices=("deepseek", "openai"),
        default="deepseek",
    )
    close_research.add_argument("--model")
    close_research.add_argument(
        "--macro-weight",
        type=_macro_weight_decimal,
        default=Decimal("0.25"),
        help=(
            "bounded macro contribution to the uncalibrated combined score "
            "(0..0.40; default: 0.25)"
        ),
    )
    close_research.add_argument(
        "--held",
        action="store_true",
        help="evaluate reduce triggers for an already-held position",
    )
    close_research.add_argument("--notify-target-kind", choices=("private", "group"))
    close_research.add_argument("--notify-target-id")

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
    close_batch.add_argument(
        "--research-db", default="runtime/research/research.sqlite3"
    )
    close_batch.add_argument(
        "--market-evidence-dir", default="runtime/research/market_evidence"
    )
    close_batch.add_argument(
        "--outbox-db", default="runtime/notifications/outbox.sqlite3"
    )
    close_batch.add_argument("--report-dir", default="runtime/reports")
    close_batch.add_argument(
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
    close_batch.add_argument(
        "--refresh-news", action=argparse.BooleanOptionalAction, default=True
    )
    close_batch.add_argument(
        "--search-discovery", action=argparse.BooleanOptionalAction, default=True
    )
    close_batch.add_argument("--searxng-url")
    close_batch.add_argument(
        "--macro", action=argparse.BooleanOptionalAction, default=True
    )
    close_batch.add_argument(
        "--macro-provider", choices=("deepseek", "openai"), default="deepseek"
    )
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
    close_batch.add_argument("--notify-target-kind", choices=("private", "group"))
    close_batch.add_argument("--notify-target-id")
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
        help="bounded polling cycles; use an OS supervisor for continuous operation",
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
        default="deepseek",
    )
    research_watch.add_argument(
        "--model",
        help="provider model ID; defaults to deepseek-v4-flash or gpt-5.6",
    )
    research_watch.add_argument("--notify-target-kind", choices=("private", "group"))
    research_watch.add_argument("--notify-target-id")
    research_watch.add_argument(
        "--candidate-db",
        help="merge ACTIVE symbols from the unified candidate event store",
    )
    research_watch.add_argument(
        "--candidates-only",
        action="store_true",
        help="monitor only ACTIVE candidate-store symbols (requires --candidate-db)",
    )

    napcat_configure = commands.add_parser(
        "napcat-configure",
        help="lock down an extracted portable NapCat runtime and store random tokens",
    )
    napcat_configure.add_argument(
        "--runtime-dir",
        default="vendor/NapCatQQ-shell-v4.18.18",
    )
    napcat_configure.add_argument("--onebot-port", type=_positive_integer, default=3000)
    napcat_configure.add_argument("--webui-port", type=_positive_integer, default=6099)
    napcat_configure.add_argument("--force", action="store_true")

    napcat_status = commands.add_parser(
        "napcat-status",
        help="check a loopback NapCat/OneBot endpoint without sending a message",
    )
    napcat_status.add_argument("--base-url", default="http://127.0.0.1:3000")

    napcat_dispatch = commands.add_parser(
        "napcat-dispatch",
        help="dispatch a finite number of outbound-only NapCat outbox polling cycles",
    )
    napcat_dispatch.add_argument("--base-url", default="http://127.0.0.1:3000")
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
    napcat_test.add_argument("--base-url", default="http://127.0.0.1:3000")
    napcat_test.add_argument("--target-kind", choices=("private", "group"), required=True)
    napcat_test.add_argument("--target-id", required=True)
    napcat_test.add_argument("--confirm", choices=("SEND_TEST",), required=True)

    napcat_artifact = commands.add_parser(
        "napcat-send-artifact",
        help="send one allowlisted local report image or upload one report file",
    )
    napcat_artifact.add_argument("--base-url", default="http://127.0.0.1:3000")
    napcat_artifact.add_argument(
        "--target-kind", choices=("private", "group"), required=True
    )
    napcat_artifact.add_argument("--target-id", required=True)
    napcat_artifact.add_argument(
        "--artifact-kind", choices=("image", "file"), required=True
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
        "--confirm", choices=("SEND_ARTIFACT",), required=True
    )
    return parser


def _add_order_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument(
        "--notional",
        type=_positive_decimal,
        default=Decimal("20"),
        help="target virtual quote notional (default: 20)",
    )


def _positive_decimal(value: str) -> Decimal:
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise argparse.ArgumentTypeError("notional must be a decimal number") from None
    if not number.is_finite() or number <= 0:
        raise argparse.ArgumentTypeError("notional must be positive and finite")
    return number


def _non_negative_decimal(value: str) -> Decimal:
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise argparse.ArgumentTypeError("value must be a decimal number") from None
    if not number.is_finite() or number < 0:
        raise argparse.ArgumentTypeError("value must be finite and non-negative")
    return number


def _unit_fraction_decimal(value: str) -> Decimal:
    number = _non_negative_decimal(value)
    if number > 1:
        raise argparse.ArgumentTypeError("value must be between zero and one")
    return number


def _macro_weight_decimal(value: str) -> Decimal:
    number = _unit_fraction_decimal(value)
    if number > Decimal("0.40"):
        raise argparse.ArgumentTypeError(
            "macro weight must be between zero and 0.40 so technical evidence remains dominant"
        )
    return number


def _positive_integer(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("value must be a positive integer") from None
    if number <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return number


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from None


def _iso_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "datetime must be ISO-8601 and include a timezone"
        ) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("datetime must include a timezone offset")
    return parsed


def _non_negative_integer(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("value must be a non-negative integer") from None
    if number < 0:
        raise argparse.ArgumentTypeError("value must be a non-negative integer")
    return number


def _positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("value must be positive") from None
    if not 0 < number < float("inf"):
        raise argparse.ArgumentTypeError("value must be positive and finite")
    return number


def _non_negative_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("value must be non-negative") from None
    if not 0 <= number < float("inf"):
        raise argparse.ArgumentTypeError("value must be non-negative and finite")
    return number


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "gui"
    if command == "gui":
        from gribuki_trade.gui import run

        return run([])
    if command == "secret-set":
        _set_secret(args.name)
        result = {"configured": True, "name": args.name}
    elif command == "secret-status":
        result = _secret_status()
    elif command == "deepseek-configure":
        _set_secret(DEEPSEEK_API_KEY_SECRET)
        result = {"configured": True, "name": DEEPSEEK_API_KEY_SECRET}
    elif command == "deepseek-status":
        result = asyncio.run(_deepseek_status())
    elif command == "sqlite-runtime-status":
        result = _sqlite_runtime_status()
    elif command == "binance-testnet-status":
        result = asyncio.run(_binance_testnet_status(args.symbol))
    elif command == "binance-testnet-order-test":
        result = asyncio.run(_binance_testnet_order_test(args.symbol, args.notional))
    elif command == "binance-testnet-cycle":
        result = asyncio.run(_binance_testnet_cycle(args.symbol, args.notional))
    elif command == "binance-testnet-oms-cycle":
        result = asyncio.run(
            _binance_testnet_oms_cycle(args.symbol, args.notional, args.database)
        )
    elif command == "binance-testnet-oms-fill":
        result = asyncio.run(
            _binance_testnet_oms_fill(args.symbol, args.notional, args.database)
        )
    elif command == "binance-history-sync":
        result = asyncio.run(
            _binance_history_sync(
                args.symbol,
                args.interval,
                args.days,
                args.environment,
                args.database,
            )
        )
    elif command == "binance-backtest":
        result = _binance_backtest(
            args.symbol,
            args.interval,
            args.environment,
            args.database,
            args.base_asset,
            args.quote_asset,
            args.initial_quote,
            args.fast_window,
            args.slow_window,
            args.target_position,
            args.rebalance_band,
            args.maker_fee,
            args.taker_fee,
            args.slippage,
        )
    elif command == "binance-shadow-run":
        result = asyncio.run(
            _binance_shadow_run(
                args.symbol,
                args.interval,
                args.environment,
                args.database,
                args.closed_bars,
                args.initial_quote,
                args.fast_window,
                args.slow_window,
                args.target_position,
                args.rebalance_band,
                args.maximum_order_notional,
            )
        )
    elif command == "binance-futures-demo-status":
        result = asyncio.run(
            _binance_futures_demo_status(
                args.product,
                args.symbol,
                args.validate_order_test,
                args.confirm,
                args.quantity,
                args.side,
            )
        )
    elif command == "ashare-snapshot":
        result = _ashare_snapshot(args.symbol)
    elif command == "ashare-bars":
        result = _ashare_bars(args.symbol, args.interval, args.lookback_minutes)
    elif command == "ashare-daily":
        result = _ashare_daily(args.symbol, args.days)
    elif command == "ashare-news":
        result = asyncio.run(
            _ashare_news(args.feed, args.symbol, args.limit, args.archive_dir)
        )
    elif command == "ashare-news-watch":
        result = asyncio.run(
            _ashare_news_watch(
                args.feeds,
                args.symbols,
                args.interval_seconds,
                args.cycles,
                args.runtime_dir,
            )
        )
    elif command == "ashare-source-health":
        result = _ashare_source_health(
            args.source_id,
            args.operation,
            args.days,
            args.runtime_dir,
        )
    elif command == "ashare-disclosures":
        result = asyncio.run(
            _ashare_disclosures(
                args.symbol,
                args.lookback_days,
                args.category,
                args.runtime_dir,
            )
        )
    elif command == "ashare-watchlist":
        result = _ashare_watchlist(args.config)
    elif command == "ashare-market-screen-once":
        result = asyncio.run(
            _ashare_market_screen_once(
                args.top_n,
                args.factor_budget,
                args.min_listing_days,
                args.min_session_amount_cny,
                args.min_average_amount_20_cny,
                args.min_market_cap_cny,
                None if args.no_store_candidates else args.candidate_db,
                None if args.no_store_run else args.run_db,
            )
        )
        if args.output is not None:
            output_path = Path(args.output).resolve()
            result["output"] = str(output_path)
            _atomic_write_cli_json(output_path, result)
    elif command == "ashare-intraday-scan-once":
        result = asyncio.run(
            _ashare_intraday_scan_once(
                args.top_n,
                args.min_session_amount_cny,
                None if args.no_store_candidates else args.candidate_db,
                None if args.no_store_run else args.run_db,
            )
        )
        if args.output is not None:
            output_path = Path(args.output).resolve()
            result["output"] = str(output_path)
            _atomic_write_cli_json(output_path, result)
    elif command == "strategy-factor-discover":
        result = _strategy_factor_discover(args.max_trials)
        if args.output is not None:
            output_path = Path(args.output).resolve()
            result["output"] = str(output_path)
            _atomic_write_cli_json(output_path, result)
    elif command == "ashare-research-runs":
        result = _ashare_research_runs(
            args.action,
            args.run_db,
            args.run_id,
            args.run_type,
            args.limit,
        )
    elif command == "ashare-candidates":
        result = _ashare_candidates(
            args.action,
            args.symbol,
            args.reason,
            args.candidate_db,
            args.cooling_minutes,
            args.limit,
        )
    elif command == "ashare-review":
        result = _ashare_review(
            args.action,
            args.review_db,
            args.research_db,
            args.candidate_db,
            args.recommendation_id,
            args.case_id,
            args.reason,
            args.limit,
            args.confirm,
        )
    elif command == "ashare-paper":
        result = _ashare_paper(
            args.action,
            args.ledger_db,
            args.account,
            args.initial_cash,
            args.session_date,
            args.symbol,
            args.side,
            args.quantity,
            args.price,
            args.instrument,
            args.source,
            args.fill_id,
            args.executed_at,
            args.external_order_id,
            args.note,
            args.actual_commission,
            args.actual_transfer_fee,
            args.actual_stamp_tax,
        )
    elif command == "ashare-research-once":
        result = asyncio.run(
            _ashare_research_once(
                args.symbol,
                args.interval,
                args.lookback_minutes,
                args.events_db,
                args.research_db,
                args.outbox_db,
                args.macro,
                args.macro_provider,
                args.model,
                args.notify_target_kind,
                args.notify_target_id,
            )
        )
    elif command == "ashare-close-research-once":
        result = asyncio.run(
            _ashare_close_research_once(
                args.symbol,
                args.history_days,
                args.session_date,
                args.next_session,
                args.news_runtime_dir,
                args.research_db,
                args.market_evidence_dir,
                args.outbox_db,
                args.news_feeds,
                args.refresh_news,
                args.search_discovery,
                args.searxng_url,
                args.macro,
                args.macro_provider,
                args.model,
                args.macro_weight,
                args.held,
                args.notify_target_kind,
                args.notify_target_id,
                args.report_dir,
            )
        )
    elif command == "ashare-close-research-batch":
        result = asyncio.run(
            _ashare_close_research_batch(
                args.symbols,
                args.candidate_db,
                args.limit,
                args.history_days,
                args.session_date,
                args.next_session,
                args.news_runtime_dir,
                args.research_db,
                args.market_evidence_dir,
                args.outbox_db,
                args.news_feeds,
                args.refresh_news,
                args.search_discovery,
                args.searxng_url,
                args.macro,
                args.macro_provider,
                args.model,
                args.macro_weight,
                args.held_symbols,
                args.notify_target_kind,
                args.notify_target_id,
                args.report_dir,
            )
        )
        if args.output is not None:
            output_path = Path(args.output).resolve()
            result["output"] = str(output_path)
            _atomic_write_cli_json(output_path, result)
    elif command == "ashare-research-watch":
        result = asyncio.run(
            _ashare_research_watch(
                args.symbols,
                args.watchlist,
                args.watchlist_all,
                args.interval,
                args.lookback_minutes,
                args.interval_seconds,
                args.cycles,
                args.events_db,
                args.research_db,
                args.outbox_db,
                args.macro,
                args.macro_provider,
                args.model,
                args.notify_target_kind,
                args.notify_target_id,
                args.candidate_db,
                args.candidates_only,
            )
        )
    elif command == "napcat-configure":
        result = _napcat_configure(
            args.runtime_dir,
            args.onebot_port,
            args.webui_port,
            args.force,
        )
    elif command == "napcat-status":
        result = asyncio.run(_napcat_status(args.base_url))
    elif command == "napcat-dispatch":
        result = asyncio.run(
            _napcat_dispatch(
                args.base_url,
                args.target_kind,
                args.target_id,
                args.outbox_path,
                args.cycles,
                args.poll_interval,
            )
        )
    elif command == "napcat-send-test":
        result = asyncio.run(
            _napcat_send_test(args.base_url, args.target_kind, args.target_id)
        )
    elif command == "napcat-send-artifact":
        result = asyncio.run(
            _napcat_send_artifact(
                args.base_url,
                args.target_kind,
                args.target_id,
                args.artifact_kind,
                args.artifact_root,
                args.artifact,
            )
        )
    else:  # pragma: no cover - argparse owns the accepted command set
        parser.error(f"unknown command: {command}")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


def _set_secret(name: str) -> None:
    from gribuki_trade.security import InteractiveSecretManager

    InteractiveSecretManager(KeyringSecretProvider()).set_secret(name)


def _secret_status() -> dict[str, bool]:
    provider = KeyringSecretProvider()
    return {
        name: provider.get_secret(name) is not None
        for name in KNOWN_SECRET_STATUS_NAMES
    }


def _sqlite_runtime_status() -> dict[str, object]:
    status = sqlite_runtime_status()
    return {
        "error_code": status.error_code,
        "fixed_releases": list(status.fixed_releases),
        "guidance_url": status.guidance_url,
        "ok": status.shared_wal_safe,
        "shared_wal_safe": status.shared_wal_safe,
        "single_connection_local_allowed": True,
        "sqlite_version": status.version,
    }


def _required_local_secret(name: str) -> str:
    value = KeyringSecretProvider().get_secret(name)
    if value is None:
        raise RuntimeError(
            f"required local secret {name!r} is not configured; use secret-set"
        )
    return value


def _optional_local_secret(name: str) -> str | None:
    """Read an optional integration secret without making that provider mandatory."""

    try:
        return KeyringSecretProvider().get_secret(name)
    except SecretProviderError:
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _three_decimal_text(value: Decimal | None) -> str | None:
    """Stable presentation precision for human-facing research documents."""

    return None if value is None else format(value.quantize(Decimal("0.001")), "f")


def _atomic_write_cli_json(path: Path, payload: object) -> None:
    """Durably replace one optional CLI JSON artifact on the same filesystem."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            delete=False,
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        ) as stream:
            temporary = Path(stream.name)
            json.dump(
                payload,
                stream,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _strategy_factor_discover(max_trials: int) -> dict[str, object]:
    """Expand the controlled factor grammar without data or configuration writes."""

    from gribuki_trade.strategy_lab import (
        FactorSearchBudget,
        FactorSearchBudgetExceeded,
        default_factor_template_grammar,
        generate_factor_candidates,
    )

    grammar = default_factor_template_grammar()
    try:
        inventory = generate_factor_candidates(
            grammar,
            FactorSearchBudget(max_trials=max_trials),
        )
    except FactorSearchBudgetExceeded as exc:
        return {
            "ok": False,
            "error_code": "FACTOR_SEARCH_BUDGET_EXCEEDED",
            "grammar_version": grammar.grammar_version,
            "grammar_sha256": grammar.grammar_sha256,
            "max_trials": exc.max_trials,
            "required_trials": exc.required_trials,
            "research_only": True,
            "market_data_accessed": False,
            "llm_accessed": False,
            "holdout_accessed": False,
            "online_configuration_changed": False,
        }
    return _factor_candidate_inventory_json(inventory)


def _factor_candidate_inventory_json(
    inventory: FactorCandidateInventory,
) -> dict[str, object]:
    return {
        "ok": True,
        "inventory_id": inventory.inventory_id,
        "grammar_version": inventory.grammar_version,
        "grammar_sha256": inventory.grammar_sha256,
        "max_trials": inventory.max_trials,
        "trial_count": inventory.trial_count,
        "unique_hypothesis_count": inventory.unique_hypothesis_count,
        "rejected_count": inventory.rejected_count,
        "duplicate_count": inventory.duplicate_count,
        "candidates": [
            {
                "candidate_id": item.candidate_id,
                "grammar_version": item.grammar_version,
                "grammar_sha256": item.grammar_sha256,
                "template_id": item.template_id,
                "template_version": item.template_version,
                "economic_family": item.economic_family.value,
                "parameters": [
                    {"name": name, "value": value}
                    for name, value in item.parameters
                ],
                "expression": item.expression,
                "canonical_expression": item.canonical_expression,
                "expression_sha256": item.expression_sha256,
                "required_warmup": item.required_warmup,
                "ast_node_count": item.ast_node_count,
            }
            for item in inventory.candidates
        ],
        "rejected": [
            {
                "attempt_id": item.attempt_id,
                "template_id": item.template_id,
                "template_version": item.template_version,
                "economic_family": item.economic_family.value,
                "parameters": [
                    {"name": name, "value": value}
                    for name, value in item.parameters
                ],
                "expression": item.expression,
                "reason_codes": [reason.value for reason in item.reason_codes],
                "canonical_expression": item.canonical_expression,
                "duplicate_of_candidate_id": item.duplicate_of_candidate_id,
            }
            for item in inventory.rejected
        ],
        "warnings": list(inventory.warnings),
        "research_only": inventory.research_only,
        "market_data_accessed": False,
        "llm_accessed": False,
        "holdout_accessed": False,
        "online_configuration_changed": False,
    }


def _ashare_research_runs(
    action: str,
    run_db: str,
    run_id: str | None,
    run_type: str | None,
    limit: int,
) -> dict[str, object]:
    """Read retained run outputs without creating an absent registry."""

    from gribuki_trade.storage import SQLiteResearchRunStore

    if action not in {"list", "get"}:
        raise ValueError("unsupported research run action")
    if limit < 1:
        raise ValueError("limit must be positive")
    if action == "get" and (run_id is None or not run_id.strip()):
        raise ValueError("--run-id is required for get")
    if action == "list" and run_id is not None:
        raise ValueError("--run-id is only valid for get")
    if action == "get" and run_type is not None:
        raise ValueError("--run-type is only valid for list")

    path = Path(run_db).resolve()
    if not path.is_file():
        return {
            "ok": False,
            "action": action,
            "error_code": "RESEARCH_RUN_DATABASE_NOT_FOUND",
            "run_database": str(path),
            "run": None,
            "runs": [],
        }

    with SQLiteResearchRunStore(path) as store:
        if action == "list":
            runs = store.list_runs(run_type=run_type, limit=limit)
            return {
                "ok": True,
                "action": action,
                "run_database": str(path),
                "run_type": run_type,
                "count": len(runs),
                "runs": [_research_run_summary(item) for item in runs],
            }
        assert run_id is not None
        stored = store.get(run_id.strip())
        if stored is None:
            return {
                "ok": False,
                "action": action,
                "error_code": "RESEARCH_RUN_NOT_FOUND",
                "run_database": str(path),
                "run_id": run_id.strip(),
                "run": None,
            }
        return {
            "ok": True,
            "action": action,
            "run_database": str(path),
            "run": {
                **_research_run_summary(stored),
                "config": stored.config_document(),
                "payload": stored.payload_document(),
            },
        }


def _research_run_summary(item: StoredResearchRun) -> dict[str, object]:
    return {
        "run_id": item.run_id,
        "run_type": item.run_type,
        "logical_key": item.logical_key,
        "strategy_version": item.strategy_version,
        "status": item.status,
        "started_at": item.started_at.isoformat(),
        "completed_at": item.completed_at.isoformat(),
        "source_revisions": [
            {"source": source, "revision": revision}
            for source, revision in item.source_revisions
        ],
        "config_sha256": item.config_sha256,
        "payload_sha256": item.payload_sha256,
        "record_version": item.record_version,
    }


async def _deepseek_status() -> dict[str, object]:
    from gribuki_trade.adapters.llm import DeepSeekHealthClient
    from gribuki_trade.security.config import SecretValue

    result = await DeepSeekHealthClient(
        SecretValue(_required_local_secret(DEEPSEEK_API_KEY_SECRET))
    ).check()
    return {
        "available_model_ids": list(result.available_model_ids),
        "default_model_id": DEFAULT_DEEPSEEK_MODEL,
        "default_model_available": result.default_model_available,
        "deepseek_v4_flash_available": result.deepseek_v4_flash_available,
        "deepseek_v4_pro_available": result.deepseek_v4_pro_available,
        "error_code": None if result.error_code is None else result.error_code.value,
        "ok": result.ok,
    }


def _ashare_snapshot(symbol: str) -> dict[str, object]:
    from gribuki_trade.adapters import AKShareMarketDataAdapter

    snapshot = AKShareMarketDataAdapter().fetch_spot_snapshot(symbol)
    return {
        "amount": _decimal_text(snapshot.amount),
        "degraded": snapshot.meta.degraded,
        "fetched_at": snapshot.meta.fetched_at.isoformat(),
        "freshness": snapshot.meta.freshness.value,
        "high": _decimal_text(snapshot.high),
        "last": _decimal_text(snapshot.last),
        "low": _decimal_text(snapshot.low),
        "name": snapshot.name,
        "open": _decimal_text(snapshot.open),
        "previous_close": _decimal_text(snapshot.previous_close),
        "provider": snapshot.meta.provider,
        "semantics": snapshot.meta.semantics.value,
        "symbol": snapshot.symbol,
        "turnover_percent": _decimal_text(snapshot.turnover_percent),
        "volume_lots": snapshot.volume_lots,
        "warnings": list(snapshot.meta.warnings),
    }


def _ashare_bars(
    symbol: str,
    interval_value: str,
    lookback_minutes: int,
) -> dict[str, object]:
    from gribuki_trade.adapters import AKShareMarketDataAdapter
    from gribuki_trade.ports.market_data import MinuteInterval

    interval = (
        MinuteInterval.ONE_MINUTE
        if interval_value == "1m"
        else MinuteInterval.FIVE_MINUTES
    )
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    bars = AKShareMarketDataAdapter().fetch_intraday_bars(
        symbol,
        now - timedelta(minutes=lookback_minutes),
        now,
        interval=interval,
        completed_only=True,
    )
    latest = bars[-1] if bars else None
    return {
        "bar_count": len(bars),
        "interval": interval.value,
        "latest": (
            None
            if latest is None
            else {
                "amount": _decimal_text(latest.amount),
                "close": _decimal_text(latest.close),
                "end_at": latest.end_at.isoformat(),
                "freshness": latest.meta.freshness.value,
                "high": _decimal_text(latest.high),
                "low": _decimal_text(latest.low),
                "open": _decimal_text(latest.open),
                "semantics": latest.meta.semantics.value,
                "start_at": latest.start_at.isoformat(),
                "volume_lots": latest.volume_lots,
                "warnings": list(latest.meta.warnings),
            }
        ),
        "provider": "AKShare/Eastmoney",
        "symbol": symbol.upper(),
    }


def _ashare_daily(symbol: str, days: int) -> dict[str, object]:
    from gribuki_trade.adapters import BaoStockDailyAdapter

    end = date.today()
    start = end - timedelta(days=days)
    bars = BaoStockDailyAdapter().fetch_daily_bars(symbol, start, end)
    latest = bars[-1] if bars else None
    return {
        "bar_count": len(bars),
        "latest": (
            None
            if latest is None
            else {
                "amount": _decimal_text(latest.amount),
                "close": _decimal_text(latest.close),
                "is_st": latest.is_st,
                "is_trading": latest.is_trading,
                "trade_date": latest.trade_date.isoformat(),
                "volume": latest.volume,
            }
        ),
        "provider": "BaoStock",
        "symbol": symbol.upper(),
    }


async def _ashare_news(
    feed_value: str,
    symbol: str | None,
    limit: int,
    archive_dir: str,
) -> dict[str, object]:
    from gribuki_trade.ingest import (
        AKShareNewsConfig,
        AKShareNewsFeed,
        AKShareNewsSource,
    )
    from gribuki_trade.storage import FileRawDocumentStore

    feed = AKShareNewsFeed(feed_value)
    config = AKShareNewsConfig(feed=feed, symbol=symbol)
    batch = await AKShareNewsSource(config).collect()
    store = FileRawDocumentStore(Path(archive_dir))
    stored = [store.save(document) for document in batch.documents]
    events = sorted(batch.events, key=lambda item: item.available_at, reverse=True)
    return {
        "archive_created": sum(item.created for item in stored),
        "event_count": len(events),
        "feed": feed.value,
        "latest": [
            {
                "available_at": event.available_at.isoformat(),
                "canonical_url": event.canonical_url,
                "event_id": event.event_id,
                "first_seen_at": event.first_seen_at.isoformat(),
                "published_at": (
                    event.published_at.isoformat()
                    if event.published_at is not None
                    else None
                ),
                "source_id": event.source_id,
                "summary": event.summary,
                "title": event.title,
            }
            for event in events[:limit]
        ],
        "not_modified": batch.not_modified,
        "raw_document_count": len(batch.documents),
        "source_id": batch.source_id,
    }


async def _ashare_news_watch(
    feed_values: Sequence[str] | None,
    symbols: Sequence[str] | None,
    interval_seconds: float,
    cycles: int,
    runtime_dir: str,
) -> dict[str, object]:
    if not 0 < interval_seconds < float("inf"):
        raise ValueError("interval_seconds must be positive and finite")
    if cycles < 0:
        raise ValueError("cycles must be non-negative")

    from gribuki_trade.ingest import (
        AKShareNewsConfig,
        AKShareNewsFeed,
        AKShareNewsSource,
    )
    from gribuki_trade.services import (
        NewsCollectionService,
        SourceCollectionObservation,
        SourceRunStatus,
    )
    from gribuki_trade.storage import (
        FileRawDocumentStore,
        ProviderRun,
        ProviderRunStatus,
        SQLiteEventStore,
        SQLiteSourceHealthStore,
    )

    selected_feeds = tuple(feed_values or ("global_sina", "global_cailianpress"))
    root = Path(runtime_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    sources: dict[str, AKShareNewsSource] = {}
    for value in selected_feeds:
        config = AKShareNewsConfig(AKShareNewsFeed(value))
        sources[config.source_id] = AKShareNewsSource(config)
    for symbol in symbols or ():
        config = AKShareNewsConfig(
            AKShareNewsFeed.INDIVIDUAL_EASTMONEY,
            symbol=symbol,
        )
        sources[config.source_id] = AKShareNewsSource(config)
    if not sources:
        raise ValueError("at least one news feed or symbol is required")

    completed_cycles = 0
    latest: list[dict[str, object]] = []
    with (
        SQLiteEventStore(root / "events.sqlite3") as event_store,
        SQLiteSourceHealthStore(root / "source_health.sqlite3") as health_store,
    ):
        def observe(item: SourceCollectionObservation) -> None:
            result = item.result
            successful = result.status in {
                SourceRunStatus.SUCCESS,
                SourceRunStatus.NOT_MODIFIED,
            }
            health_store.append(
                ProviderRun(
                    run_id=uuid4().hex,
                    source_id=result.source_id,
                    operation="news_collect",
                    started_at=item.started_at,
                    finished_at=item.finished_at,
                    status=(
                        ProviderRunStatus.SUCCESS
                        if successful
                        else ProviderRunStatus.FAILURE
                    ),
                    error_code=result.error_code,
                    item_count=(
                        result.events_new
                        + result.events_revised
                        + result.events_duplicate
                    ),
                    degraded=result.status is SourceRunStatus.BACKOFF,
                    stale=False,
                    latency_ms=item.latency_ms,
                    adapter_version="akshare-news:1",
                )
            )

        service = NewsCollectionService(
            sources,
            raw_store=FileRawDocumentStore(root / "raw"),
            event_store=event_store,
            observer=observe,
        )
        while cycles == 0 or completed_cycles < cycles:
            results = await service.run_once()
            completed_cycles += 1
            latest = [
                {
                    "documents_saved": item.documents_saved,
                    "error_code": item.error_code,
                    "events_duplicate": item.events_duplicate,
                    "events_new": item.events_new,
                    "events_revised": item.events_revised,
                    "source_id": item.source_id,
                    "status": item.status.value,
                }
                for item in results
            ]
            print(
                json.dumps(
                    {
                        "cycle": completed_cycles,
                        "sources": latest,
                        "timestamp": datetime.now(UTC).isoformat(),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                flush=True,
            )
            if cycles != 0 and completed_cycles >= cycles:
                break
            await asyncio.sleep(interval_seconds)
        retained_events = len(event_store.latest(limit=10_000))
    return {
        "completed_cycles": completed_cycles,
        "latest_sources": latest,
        "retained_latest_event_count": retained_events,
        "runtime_dir": str(root),
    }


def _ashare_source_health(
    source_id: str | None,
    operation: str,
    days: int,
    runtime_dir: str,
) -> dict[str, object]:
    """Summarize append-only collection telemetry without network access."""

    from gribuki_trade.storage import SQLiteSourceHealthStore

    if days < 1:
        raise ValueError("days must be positive")
    if not operation.strip():
        raise ValueError("operation must not be empty")
    root = Path(runtime_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)
    start = now - timedelta(days=days)
    with SQLiteSourceHealthStore(root / "source_health.sqlite3") as health_store:
        if source_id is None:
            source_ids = sorted(
                {
                    item.source_id
                    for item in health_store.list_runs(limit=10_000)
                    if item.operation == operation
                }
            )
        else:
            source_ids = [source_id.strip().lower()]
        summaries = [
            health_store.summarize(
                item,
                window_start=start,
                window_end=now,
                operation=operation,
            )
            for item in source_ids
        ]
    return {
        "operation": operation,
        "runtime_dir": str(root),
        "window_end": now.isoformat(),
        "window_start": start.isoformat(),
        "sources": [
            {
                "degraded_rate": item.degraded_rate,
                "failed_runs": item.failed_runs,
                "latest_failure": (
                    None
                    if item.latest_failure is None
                    else item.latest_failure.finished_at.isoformat()
                ),
                "latest_success": (
                    None
                    if item.latest_success is None
                    else item.latest_success.finished_at.isoformat()
                ),
                "p50_latency_ms": item.p50_latency_ms,
                "p95_latency_ms": item.p95_latency_ms,
                "source_id": item.source_id,
                "stale_rate": item.stale_rate,
                "success_rate": item.success_rate,
                "successful_runs": item.successful_runs,
                "total_runs": item.total_runs,
            }
            for item in summaries
        ],
    }


async def _ashare_disclosures(
    symbol: str,
    lookback_days: int,
    category: str,
    runtime_dir: str,
) -> dict[str, object]:
    from gribuki_trade.ingest import (
        AKShareDisclosureConfig,
        AKShareDisclosureSource,
    )
    from gribuki_trade.services import NewsCollectionService
    from gribuki_trade.storage import FileRawDocumentStore, SQLiteEventStore

    if lookback_days < 1:
        raise ValueError("lookback_days must be positive")
    today = date.today()
    config = AKShareDisclosureConfig(
        symbol=symbol,
        start_date=today - timedelta(days=lookback_days),
        end_date=today,
        category=category,
    )
    root = Path(runtime_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    source = AKShareDisclosureSource(config)
    with SQLiteEventStore(root / "events.sqlite3") as event_store:
        service = NewsCollectionService(
            {config.source_id: source},
            raw_store=FileRawDocumentStore(root / "raw"),
            event_store=event_store,
            max_concurrency=1,
        )
        result = (await service.run_once())[0]
        latest = tuple(
            event
            for event in event_store.latest(limit=200)
            if event.source_id == config.source_id
        )
    return {
        "documents_saved": result.documents_saved,
        "error_code": result.error_code,
        "events_duplicate": result.events_duplicate,
        "events_new": result.events_new,
        "events_revised": result.events_revised,
        "latest": [
            {
                "available_at": event.available_at.isoformat(),
                "event_id": event.event_id,
                "published_at": (
                    None
                    if event.published_at is None
                    else event.published_at.isoformat()
                ),
                "title": event.title,
                "url": event.canonical_url,
            }
            for event in latest[:20]
        ],
        "runtime_dir": str(root),
        "source_id": config.source_id,
        "status": result.status.value,
        "symbol": symbol.strip().upper(),
    }


def _ashare_watchlist(config_path: str) -> dict[str, object]:
    from gribuki_trade.watchlists import load_research_watchlist

    path = Path(config_path).resolve()
    watchlist = load_research_watchlist(path)
    default_set = frozenset(watchlist.default_symbols)
    return {
        "config": str(path),
        "default_symbol_count": len(watchlist.default_symbols),
        "instrument_count": len(watchlist.instruments),
        "instruments": [
            {
                "asset_type": item.asset_type.value,
                "board": item.board.value,
                "default_monitor": item.symbol in default_set,
                "industry": item.industry,
                "name": item.name,
                "risk_tags": list(item.risk_tags),
                "role": item.role,
                "size_tier": item.size_tier.value,
                "styles": list(item.styles),
                "symbol": item.symbol,
            }
            for item in watchlist.instruments
        ],
        "title": watchlist.title,
        "verified_on": watchlist.verified_on.isoformat(),
        "watchlist_id": watchlist.watchlist_id,
    }


async def _ashare_market_screen_once(
    top_n: int,
    factor_budget: int,
    min_listing_days: int,
    min_session_amount_cny: Decimal,
    min_average_amount_20_cny: Decimal,
    min_market_cap_cny: Decimal,
    candidate_store_path: str | None = None,
    run_store_path: str | None = None,
    *,
    data_source: AsyncAShareScreeningData | None = None,
    decision_at: datetime | None = None,
) -> dict[str, object]:
    """Run one same-day post-close full-market screen without an order path."""

    if top_n < 1:
        raise ValueError("top_n must be positive")
    if factor_budget < 1:
        raise ValueError("factor_budget must be positive")
    if top_n > factor_budget:
        raise ValueError("top_n must not exceed factor_budget")
    if factor_budget > 300:
        raise ValueError("factor_budget must not exceed the adapter safety limit of 300")
    if min_listing_days < 0:
        raise ValueError("min_listing_days must not be negative")
    for name, value in (
        ("min_session_amount_cny", min_session_amount_cny),
        ("min_average_amount_20_cny", min_average_amount_20_cny),
        ("min_market_cap_cny", min_market_cap_cny),
    ):
        if not value.is_finite() or value < 0:
            raise ValueError(f"{name} must be finite and non-negative")
    run_config = {
        "factor_budget": factor_budget,
        "min_average_amount_20_cny": min_average_amount_20_cny,
        "min_listing_days": min_listing_days,
        "min_market_cap_cny": min_market_cap_cny,
        "min_session_amount_cny": min_session_amount_cny,
        "top_n": top_n,
    }

    from gribuki_trade.adapters.ashare_screening import (
        AKShareAShareScreeningAdapter,
        AKShareScreeningCoverageError,
        AKShareScreeningDataError,
        AKShareScreeningPayloadError,
        AKShareScreeningPointInTimeError,
        AKShareScreeningSourcesExhaustedError,
    )
    from gribuki_trade.features.ashare_screening import AShareScreeningConfig
    from gribuki_trade.services.ashare_screening import (
        AShareScreeningPointInTimeError,
        AShareScreeningService,
    )

    resolved_decision_at = decision_at or datetime.now(UTC)
    if (
        resolved_decision_at.tzinfo is None
        or resolved_decision_at.utcoffset() is None
    ):
        raise ValueError("decision_at must be timezone-aware")
    local_decision = resolved_decision_at.astimezone(ZoneInfo("Asia/Shanghai"))
    as_of = local_decision.date()
    if local_decision.time().replace(tzinfo=None) < datetime_time(15, 5):
        return _ashare_screening_failure_json(
            as_of=as_of,
            decision_at=resolved_decision_at,
            error_code="MARKET_NOT_CLOSED",
            retryable=True,
            run_store_path=run_store_path,
            run_config=run_config,
        )

    config = AShareScreeningConfig(
        min_listing_days=min_listing_days,
        min_session_amount_cny=min_session_amount_cny,
        min_average_amount_20_cny=min_average_amount_20_cny,
        min_market_cap_cny=min_market_cap_cny,
        max_factor_candidates=factor_budget,
        top_n=top_n,
    )
    resolved_source: AsyncAShareScreeningData
    if data_source is None:
        resolved_source = AKShareAShareScreeningAdapter(
            max_factor_symbols=factor_budget,
        )
    else:
        resolved_source = data_source

    try:
        run = await AShareScreeningService(
            resolved_source,
            config=config,
        ).run(
            as_of=as_of,
            decision_at=resolved_decision_at,
        )
    except AKShareScreeningSourcesExhaustedError:
        return _ashare_screening_failure_json(
            as_of=as_of,
            decision_at=resolved_decision_at,
            error_code="PROVIDER_SOURCES_EXHAUSTED",
            retryable=True,
            run_store_path=run_store_path,
            run_config=run_config,
        )
    except AKShareScreeningCoverageError:
        return _ashare_screening_failure_json(
            as_of=as_of,
            decision_at=resolved_decision_at,
            error_code="PROVIDER_COVERAGE_INSUFFICIENT",
            retryable=True,
            run_store_path=run_store_path,
            run_config=run_config,
        )
    except AKShareScreeningPayloadError:
        return _ashare_screening_failure_json(
            as_of=as_of,
            decision_at=resolved_decision_at,
            error_code="PROVIDER_PAYLOAD_INVALID",
            retryable=True,
            run_store_path=run_store_path,
            run_config=run_config,
        )
    except AKShareScreeningPointInTimeError:
        return _ashare_screening_failure_json(
            as_of=as_of,
            decision_at=resolved_decision_at,
            error_code="PROVIDER_POINT_IN_TIME_REJECTED",
            retryable=False,
            run_store_path=run_store_path,
            run_config=run_config,
        )
    except AKShareScreeningDataError:
        return _ashare_screening_failure_json(
            as_of=as_of,
            decision_at=resolved_decision_at,
            error_code="PROVIDER_FAILURE",
            retryable=True,
            run_store_path=run_store_path,
            run_config=run_config,
        )
    except AShareScreeningPointInTimeError as error:
        return _ashare_screening_failure_json(
            as_of=as_of,
            decision_at=resolved_decision_at,
            error_code="SCREENING_POINT_IN_TIME_REJECTED",
            retryable=False,
            failure_reason=error.code,
            run_store_path=run_store_path,
            run_config=run_config,
        )
    result = _ashare_screening_run_json(run)
    immutable_run_payload = dict(result)
    if candidate_store_path is not None:
        result.update(_persist_close_screen_candidates(run, candidate_store_path))
    else:
        result.update({"candidate_store": None, "candidates_appended": 0})
    if run_store_path is not None:
        from gribuki_trade.storage import research_run_document_sha256

        config_revision = research_run_document_sha256(run_config)
        result.update(
            _persist_operational_research_run(
                run_store_path=run_store_path,
                run_type="ashare_close_screen",
                logical_key=(
                    f"{run.as_of.isoformat()}/{run.strategy_version}/"
                    f"{run.universe_source_revision}/"
                    f"{run.factor_source_revision or 'no-factors'}/{config_revision}"
                ),
                status=run.status.value,
                started_at=resolved_decision_at,
                completed_at=run.decision_at,
                strategy_version=run.strategy_version,
                source_revisions=tuple(
                    item
                    for item in (
                        (run.universe_source_id, run.universe_source_revision),
                        (
                            None
                            if run.factor_source_id is None
                            or run.factor_source_revision is None
                            else (run.factor_source_id, run.factor_source_revision)
                        ),
                    )
                    if item is not None
                ),
                config=run_config,
                payload=immutable_run_payload,
            )
        )
    else:
        result.update({"run_registry": None, "run_appended": False})
    return result


def _persist_close_screen_candidates(
    run: object,
    candidate_store_path: str,
) -> dict[str, object]:
    from gribuki_trade.services.ashare_screening import AShareScreeningRun
    from gribuki_trade.services.candidate_universe import CandidateUniverseService
    from gribuki_trade.storage.candidate_store import SQLiteCandidateStore

    if not isinstance(run, AShareScreeningRun):
        raise TypeError("run must be an AShareScreeningRun")
    path = Path(candidate_store_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with SQLiteCandidateStore(path) as store:
        mutations = CandidateUniverseService(store).ingest_close_screening(run)
    return {
        "candidate_store": str(path),
        "candidates_appended": sum(item.appended for item in mutations),
        "candidates_observed": len(mutations),
    }


def _persist_operational_research_run(
    *,
    run_store_path: str,
    run_type: str,
    logical_key: str,
    status: str,
    started_at: datetime,
    completed_at: datetime,
    strategy_version: str | None,
    source_revisions: Sequence[tuple[str, str]],
    config: dict[str, object],
    payload: dict[str, object],
) -> dict[str, object]:
    """Append one output/lineage manifest without claiming raw-input replay."""

    from gribuki_trade.storage import SQLiteResearchRunStore, research_run_id

    path = Path(run_store_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with SQLiteResearchRunStore(path) as store:
        appended = store.append(
            run_type=run_type,
            logical_key=logical_key,
            status=status,
            started_at=started_at,
            completed_at=completed_at,
            strategy_version=strategy_version,
            source_revisions=source_revisions,
            config=config,
            payload=payload,
        )
    return {
        "run_appended": appended,
        "run_id": research_run_id(run_type, logical_key),
        "run_registry": str(path),
        "run_registry_scope": "OUTPUT_AND_LINEAGE_NOT_RAW_INPUT_ARCHIVE",
    }


def _ashare_screening_run_json(run: object) -> dict[str, object]:
    """Serialize one typed screening run without leaking provider objects."""

    from gribuki_trade.services.ashare_screening import AShareScreeningRun

    if not isinstance(run, AShareScreeningRun):
        raise TypeError("run must be an AShareScreeningRun")

    hard_filter_reason_counts: dict[str, int] = {}
    for hard_exclusion in run.hard_filter.excluded:
        for hard_reason in hard_exclusion.reasons:
            hard_filter_reason_counts[hard_reason.value] = (
                hard_filter_reason_counts.get(hard_reason.value, 0) + 1
            )

    factor_eligibility_reason_counts: dict[str, int] = {}
    for factor_exclusion in run.factor_ranking.factor_eligibility_exclusions:
        for factor_reason in factor_exclusion.reasons:
            factor_eligibility_reason_counts[factor_reason.value] = (
                factor_eligibility_reason_counts.get(factor_reason.value, 0) + 1
            )

    insufficient_reason_counts: dict[str, int] = {}
    for candidate in run.factor_ranking.insufficient_candidates:
        for degradation_reason in candidate.degradation_reasons:
            insufficient_reason_counts[degradation_reason] = (
                insufficient_reason_counts.get(degradation_reason, 0) + 1
            )

    factor_source: dict[str, object] | None = None
    if run.factor_source_id is not None:
        factor_source = {
            "feature_version": run.feature_version,
            "source_id": run.factor_source_id,
            "source_revision": run.factor_source_revision,
        }

    return {
        "as_of": run.as_of.isoformat(),
        "decision_at": run.decision_at.isoformat(),
        "eligible_count": run.eligible_count,
        "error_code": None,
        "exclusions": {
            "factor_budget_deferred": {
                "count": len(run.factor_budget_deferred),
                "reason": "FACTOR_BUDGET_DEFERRED",
            },
            "factor_eligibility": {
                "by_reason": dict(sorted(factor_eligibility_reason_counts.items())),
                "count": len(run.factor_ranking.factor_eligibility_exclusions),
            },
            "hard_filter": {
                "by_reason": dict(sorted(hard_filter_reason_counts.items())),
                "count": len(run.hard_filter.excluded),
            },
            "insufficient_candidates": {
                "by_reason": dict(sorted(insufficient_reason_counts.items())),
                "count": len(run.factor_ranking.insufficient_candidates),
            },
        },
        "factor_requested_count": run.factor_requested_count,
        "globally_unavailable_factors": [
            item.value for item in run.factor_ranking.globally_unavailable_factors
        ],
        "hard_filter_eligible_count": run.hard_filter_eligible_count,
        "ok": True,
        "ranked_count": run.ranked_count,
        "source": {
            "factors": factor_source,
            "universe": {
                "source_id": run.universe_source_id,
                "source_revision": run.universe_source_revision,
            },
        },
        "status": run.status.value,
        "strategy_version": run.strategy_version,
        "top_candidates": [
            {
                "board": candidate.board.value,
                "coverage": candidate.factor_weight_coverage,
                "data_status": candidate.data_status.value,
                "degradations": list(candidate.degradation_reasons),
                "factor_contributions": [
                    {
                        "contribution": factor.contribution,
                        "cross_section_observations": (
                            factor.cross_section_observations
                        ),
                        "directional_score": factor.directional_score,
                        "factor": factor.factor_id.value,
                        "percentile_rank": factor.percentile_rank,
                        "raw_value": factor.raw_value,
                        "status": factor.status.value,
                        "weight": factor.configured_weight,
                        "winsorized_value": factor.winsorized_value,
                    }
                    for factor in candidate.factor_contributions
                ],
                "industry": candidate.industry,
                "name": candidate.name,
                "rank": candidate.rank,
                "score": candidate.composite_score,
                "symbol": candidate.symbol,
            }
            for candidate in run.top_candidates
        ],
        "universe_count": run.universe_count,
        "warnings": list(run.warnings),
    }


def _ashare_screening_failure_json(
    *,
    as_of: date,
    decision_at: datetime,
    error_code: str,
    retryable: bool,
    failure_reason: str | None = None,
    run_store_path: str | None = None,
    run_config: dict[str, object] | None = None,
) -> dict[str, object]:
    """Return a stable failure envelope without provider exception details."""

    result: dict[str, object] = {
        "as_of": as_of.isoformat(),
        "decision_at": decision_at.isoformat(),
        "eligible_count": 0,
        "error_code": error_code,
        "exclusions": {
            "factor_budget_deferred": {"count": 0},
            "factor_eligibility": {"by_reason": {}, "count": 0},
            "hard_filter": {"by_reason": {}, "count": 0},
            "insufficient_candidates": {"by_reason": {}, "count": 0},
        },
        "factor_requested_count": 0,
        "failure_reason": failure_reason,
        "globally_unavailable_factors": [],
        "hard_filter_eligible_count": 0,
        "ok": False,
        "ranked_count": 0,
        "retryable": retryable,
        "source": {"factors": None, "universe": None},
        "status": "FAILED",
        "top_candidates": [],
        "universe_count": 0,
        "warnings": [],
    }
    if run_store_path is None:
        result.update({"run_registry": None, "run_appended": False})
        return result
    result.update(
        _persist_operational_research_run(
            run_store_path=run_store_path,
            run_type="ashare_close_screen",
            logical_key=(
                f"{as_of.isoformat()}/{decision_at.isoformat(timespec='microseconds')}/"
                f"FAILED/{error_code}"
            ),
            status="FAILED",
            started_at=decision_at,
            completed_at=decision_at,
            strategy_version=None,
            source_revisions=(),
            config=run_config or {},
            payload=result,
        )
    )
    return result


async def _ashare_intraday_scan_once(
    top_n: int,
    min_session_amount_cny: Decimal,
    candidate_store_path: str | None = None,
    run_store_path: str | None = None,
    *,
    data_source: AsyncAShareIntradayUniverseData | None = None,
    requested_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> dict[str, object]:
    """Run one intraday anomaly scan; candidates remain research-only inputs."""

    if top_n < 1:
        raise ValueError("top_n must be positive")
    if not min_session_amount_cny.is_finite() or min_session_amount_cny < 0:
        raise ValueError("min_session_amount_cny must be finite and non-negative")
    run_config = {
        "min_session_amount_cny": min_session_amount_cny,
        "top_n": top_n,
    }

    from gribuki_trade.adapters.ashare_surveillance import (
        AKShareAShareSurveillanceAdapter,
    )
    from gribuki_trade.features.ashare_surveillance import (
        AShareIntradaySurveillanceConfig,
    )
    from gribuki_trade.ports.ashare_surveillance import AShareSurveillanceDataError
    from gribuki_trade.services.ashare_surveillance import (
        AShareIntradaySurveillanceService,
        AShareMarketSessionError,
    )

    resolved_requested_at = requested_at or datetime.now(UTC)
    if (
        resolved_requested_at.tzinfo is None
        or resolved_requested_at.utcoffset() is None
    ):
        raise ValueError("requested_at must be timezone-aware")
    local = resolved_requested_at.astimezone(ZoneInfo("Asia/Shanghai"))
    resolved_source = data_source or AKShareAShareSurveillanceAdapter()
    service = AShareIntradaySurveillanceService(
        resolved_source,
        config=AShareIntradaySurveillanceConfig(
            top_n=top_n,
            min_session_amount_cny=min_session_amount_cny,
        ),
        clock=(lambda: completed_at or datetime.now(UTC)),
    )
    try:
        run = await service.run_once(
            session_date=local.date(),
            requested_at=resolved_requested_at,
        )
    except AShareMarketSessionError as error:
        return _ashare_intraday_failure_json(
            session_date=local.date(),
            requested_at=resolved_requested_at,
            error_code=error.code,
            retryable=error.code in {"STALE_SNAPSHOT", "INCOMPLETE_UNIVERSE"},
            run_store_path=run_store_path,
            run_config=run_config,
        )
    except AShareSurveillanceDataError as error:
        return _ashare_intraday_failure_json(
            session_date=local.date(),
            requested_at=resolved_requested_at,
            error_code=error.code,
            retryable=error.code not in {"SESSION_DATE_MISMATCH", "MARKET_NOT_OPEN"},
            run_store_path=run_store_path,
            run_config=run_config,
        )

    result = _ashare_intraday_run_json(run)
    immutable_run_payload = dict(result)
    if candidate_store_path is None:
        result.update({"candidate_store": None, "candidates_appended": 0})
    else:
        from gribuki_trade.services.candidate_universe import CandidateUniverseService
        from gribuki_trade.storage.candidate_store import SQLiteCandidateStore

        path = Path(candidate_store_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with SQLiteCandidateStore(path) as store:
            mutations = CandidateUniverseService(store).ingest_intraday_surveillance(run)
        result.update(
            {
                "candidate_store": str(path),
                "candidates_appended": sum(item.appended for item in mutations),
                "candidates_observed": len(mutations),
            }
        )
    if run_store_path is not None:
        from gribuki_trade.storage import research_run_document_sha256

        config_revision = research_run_document_sha256(run_config)
        result.update(
            _persist_operational_research_run(
                run_store_path=run_store_path,
                run_type="ashare_intraday_surveillance",
                logical_key=(
                    f"{run.session_date.isoformat()}/"
                    f"{run.requested_at.isoformat(timespec='microseconds')}/"
                    f"{run.strategy_version}/{run.source_revision}/{config_revision}"
                ),
                status=run.status.value,
                started_at=run.requested_at,
                completed_at=run.decision_at,
                strategy_version=run.strategy_version,
                source_revisions=((run.source_id, run.source_revision),),
                config=run_config,
                payload=immutable_run_payload,
            )
        )
    else:
        result.update({"run_registry": None, "run_appended": False})
    return result


def _ashare_intraday_run_json(run: object) -> dict[str, object]:
    from gribuki_trade.services.ashare_surveillance import AShareSurveillanceRun

    if not isinstance(run, AShareSurveillanceRun):
        raise TypeError("run must be an AShareSurveillanceRun")
    exclusion_counts: dict[str, int] = {}
    for item in run.ranking.excluded:
        for reason in item.reasons:
            exclusion_counts[reason.value] = exclusion_counts.get(reason.value, 0) + 1
    return {
        "ok": True,
        "session_date": run.session_date.isoformat(),
        "requested_at": run.requested_at.isoformat(),
        "decision_at": run.decision_at.isoformat(),
        "status": run.status.value,
        "strategy_version": run.strategy_version,
        "source": {
            "source_id": run.source_id,
            "source_revision": run.source_revision,
        },
        "universe_count": run.universe_count,
        "eligible_count": run.ranking.eligible_count,
        "exclusions": dict(sorted(exclusion_counts.items())),
        "globally_unavailable_factors": [
            item.value for item in run.ranking.globally_unavailable_factors
        ],
        "candidates": [
            {
                "symbol": candidate.symbol,
                "name": candidate.name,
                "rank": candidate.rank,
                "candidate_class": candidate.candidate_class.value,
                "anomaly_score": candidate.anomaly_score,
                "factor_weight_coverage": candidate.factor_weight_coverage,
                "last_price": str(candidate.last_price),
                "change_percent": str(candidate.change_percent),
                "session_amount_cny": str(candidate.session_amount_cny),
                "reason_codes": list(candidate.reason_codes),
                "factors": [
                    {
                        "factor": factor.factor_id.value,
                        "raw_value": factor.raw_value,
                        "winsorized_value": factor.winsorized_value,
                        "percentile_rank": factor.percentile_rank,
                        "directional_score": factor.directional_score,
                        "weight": factor.configured_weight,
                        "contribution": factor.contribution,
                        "cross_section_observations": (
                            factor.cross_section_observations
                        ),
                    }
                    for factor in candidate.factors
                ],
            }
            for candidate in run.ranking.candidates
        ],
        "warnings": list(run.warnings),
        "error_code": None,
    }


def _ashare_intraday_failure_json(
    *,
    session_date: date,
    requested_at: datetime,
    error_code: str,
    retryable: bool,
    run_store_path: str | None = None,
    run_config: dict[str, object] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "ok": False,
        "session_date": session_date.isoformat(),
        "requested_at": requested_at.isoformat(),
        "status": "FAILED",
        "error_code": error_code,
        "retryable": retryable,
        "universe_count": 0,
        "eligible_count": 0,
        "candidates": [],
        "warnings": [],
        "candidate_store": None,
        "candidates_appended": 0,
    }
    if run_store_path is None:
        result.update({"run_registry": None, "run_appended": False})
        return result
    result.update(
        _persist_operational_research_run(
            run_store_path=run_store_path,
            run_type="ashare_intraday_surveillance",
            logical_key=(
                f"{session_date.isoformat()}/"
                f"{requested_at.isoformat(timespec='microseconds')}/FAILED/{error_code}"
            ),
            status="FAILED",
            started_at=requested_at,
            completed_at=requested_at,
            strategy_version=None,
            source_revisions=(),
            config=run_config or {},
            payload=result,
        )
    )
    return result


def _ashare_candidates(
    action: str,
    symbol: str | None,
    reason: str,
    candidate_store_path: str,
    cooling_minutes: int,
    limit: int,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """Manage the research candidate event store without any execution path."""

    from gribuki_trade.domain.candidates import CandidateSource
    from gribuki_trade.services.candidate_universe import (
        CandidateDiscovery,
        CandidateUniverseService,
    )
    from gribuki_trade.storage.candidate_store import SQLiteCandidateStore

    if action not in {"list", "add", "cool", "activate", "remove"}:
        raise ValueError("unsupported candidate action")
    if (action == "list") != (symbol is None):
        raise ValueError("--symbol is required for mutations and omitted for list")
    if not reason.strip():
        raise ValueError("reason must not be empty")
    if cooling_minutes < 1 or limit < 1:
        raise ValueError("cooling_minutes and limit must be positive")
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None or resolved_now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    path = Path(candidate_store_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with SQLiteCandidateStore(path) as store:
        service = CandidateUniverseService(store, clock=lambda: resolved_now)
        if action == "list":
            candidates = service.tracking_candidates(as_of=resolved_now, limit=limit)
            mutation_appended: bool | None = None
        else:
            assert symbol is not None
            if action == "add":
                mutation = service.upsert(
                    CandidateDiscovery(
                        symbol=symbol,
                        source=CandidateSource.MANUAL,
                        source_run_id=(
                            f"manual-{resolved_now.astimezone(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
                        ),
                        discovered_at=resolved_now,
                        observed_at=resolved_now,
                        reason_codes=(reason.strip(),),
                    )
                )
            elif action == "cool":
                mutation = service.cool(
                    symbol,
                    reason_code=reason,
                    at=resolved_now,
                    until=resolved_now + timedelta(minutes=cooling_minutes),
                )
            elif action == "activate":
                mutation = service.activate(symbol, reason_code=reason, at=resolved_now)
            else:
                mutation = service.remove(symbol, reason_code=reason, at=resolved_now)
            candidates = (mutation.candidate,)
            mutation_appended = mutation.appended
    return {
        "ok": True,
        "action": action,
        "as_of": resolved_now.astimezone(UTC).isoformat(),
        "candidate_store": str(path),
        "mutation_appended": mutation_appended,
        "candidates": [
            {
                "symbol": item.symbol,
                "status": item.status.value,
                "priority": item.priority.name,
                "sources": [source.value for source in item.sources],
                "reason_codes": list(item.reason_codes),
                "evidence_ids": list(item.evidence_ids),
                "discovered_at": item.discovered_at.isoformat(),
                "first_observed_at": item.first_observed_at.isoformat(),
                "last_observed_at": item.last_observed_at.isoformat(),
                "expires_at": (
                    None if item.expires_at is None else item.expires_at.isoformat()
                ),
                "cooling_until": (
                    None
                    if item.cooling_until is None
                    else item.cooling_until.isoformat()
                ),
                "provenance_count": len(item.provenance),
            }
            for item in candidates
        ],
    }


def _ashare_review(
    action: str,
    review_db: str,
    research_db: str,
    candidate_db: str,
    recommendation_id: str | None,
    case_id: str | None,
    reasons: Sequence[str] | None,
    limit: int,
    confirmation: str | None,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """Operate the research-review state machine without execution side effects."""

    from gribuki_trade.domain.review_cases import RecommendationReviewCase, ReviewActor
    from gribuki_trade.services.recommendation_review import (
        RecommendationReviewService,
    )
    from gribuki_trade.storage.candidate_store import SQLiteCandidateStore
    from gribuki_trade.storage.research_store import SQLiteResearchStore
    from gribuki_trade.storage.review_case_store import SQLiteReviewCaseStore

    supported = {"open", "list", "get", "confirm", "reject", "cancel"}
    if action not in supported:
        raise ValueError("unsupported review action")
    if limit < 1 or limit > 500:
        raise ValueError("limit must be between 1 and 500")
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None or resolved_now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    resolved_now = resolved_now.astimezone(UTC)
    recommendation_id = _optional_cli_identifier(
        recommendation_id, "recommendation_id"
    )
    case_id = _optional_cli_identifier(case_id, "case_id")
    if action == "open":
        if recommendation_id is None or case_id is not None:
            raise ValueError("open requires only --recommendation-id")
    elif action == "get":
        if (recommendation_id is None) == (case_id is None):
            raise ValueError("get requires exactly one of --case-id/--recommendation-id")
    elif action in {"confirm", "reject", "cancel"}:
        if case_id is None or recommendation_id is not None:
            raise ValueError(f"{action} requires only --case-id")
    elif recommendation_id is not None or case_id is not None:
        raise ValueError("list does not accept recommendation or case identifiers")
    if action == "confirm" and confirmation != "RESEARCH_ONLY":
        raise ValueError("confirm requires --confirm RESEARCH_ONLY")
    if action != "confirm" and confirmation is not None:
        raise ValueError("--confirm is valid only for the confirm action")

    default_reasons = {
        "open": ("DETAILED_REVIEW_REQUIRED",),
        "confirm": ("LOCAL_RESEARCH_CONFIRMED",),
        "reject": ("LOCAL_RESEARCH_REJECTED",),
        "cancel": ("LOCAL_REVIEW_CANCELLED",),
    }
    reason_codes = _bounded_review_reasons(
        reasons,
        default=default_reasons.get(action, ()),
    )
    if action in {"list", "get"} and reason_codes:
        raise ValueError(f"{action} does not accept --reason")

    review_path = Path(review_db).resolve()
    review_path.parent.mkdir(parents=True, exist_ok=True)
    with SQLiteReviewCaseStore(review_path) as repository:
        service = RecommendationReviewService(repository, clock=lambda: resolved_now)
        appended: bool | None = None
        cases: tuple[RecommendationReviewCase, ...]
        if action == "open":
            assert recommendation_id is not None
            existing = service.get_for_recommendation(
                recommendation_id,
                as_of=resolved_now,
            )
            if existing is not None:
                cases = (existing,)
                appended = False
            else:
                research_path = Path(research_db).resolve()
                if not research_path.is_file():
                    raise ValueError("research database does not exist")
                with SQLiteResearchStore(research_path) as research_store:
                    recommendation = research_store.get_recommendation(recommendation_id)
                if recommendation is None:
                    raise ValueError("recommendation does not exist")
                candidate = None
                candidate_path = Path(candidate_db).resolve()
                if candidate_path.is_file():
                    with SQLiteCandidateStore(candidate_path) as candidate_store:
                        candidate = candidate_store.get_candidate(
                            recommendation.symbol,
                            as_of=resolved_now,
                        )
                mutation = service.open_case(
                    recommendation,
                    candidate=candidate,
                    actor=ReviewActor.LOCAL_USER,
                    reason_codes=reason_codes,
                    at=resolved_now,
                )
                cases = (mutation.case,)
                appended = mutation.appended
        elif action == "list":
            cases = repository.list_cases(as_of=resolved_now, limit=limit)
        elif action == "get":
            case = (
                service.get(case_id, as_of=resolved_now)
                if case_id is not None
                else service.get_for_recommendation(
                    cast(str, recommendation_id), as_of=resolved_now
                )
            )
            if case is None:
                raise ValueError("review case does not exist")
            cases = (case,)
        else:
            assert case_id is not None
            transition = getattr(service, action)
            mutation = transition(
                case_id,
                actor=ReviewActor.LOCAL_USER,
                reason_codes=reason_codes,
                at=resolved_now,
            )
            cases = (mutation.case,)
            appended = mutation.appended
    return {
        "ok": True,
        "action": action,
        "as_of": resolved_now.isoformat(),
        "review_store": str(review_path),
        "mutation_appended": appended,
        "research_only": True,
        "execution_authorized": False,
        "cases": [_ashare_review_case_json(item) for item in cases],
    }


def _optional_cli_identifier(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized or len(normalized) > 160:
        raise ValueError(f"{field_name} must be between 1 and 160 characters")
    if any(character.isspace() or ord(character) < 32 for character in normalized):
        raise ValueError(f"{field_name} must not contain whitespace or control characters")
    return normalized


def _bounded_review_reasons(
    reasons: Sequence[str] | None,
    *,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    values = default if reasons is None else tuple(reasons)
    normalized = tuple(sorted({item.strip() for item in values}))
    if len(normalized) > 12:
        raise ValueError("at most 12 review reason codes are allowed")
    if any(
        not item
        or len(item) > 120
        or any(ord(character) < 32 for character in item)
        for item in normalized
    ):
        raise ValueError("review reasons must be non-empty bounded single-line values")
    return normalized


def _ashare_review_case_json(item: object) -> dict[str, object]:
    from gribuki_trade.domain.review_cases import RecommendationReviewCase

    case = cast(RecommendationReviewCase, item)
    provenance = case.candidate_provenance
    resolution = case.resolution
    return {
        "case_id": case.case_id,
        "recommendation_id": case.recommendation_id,
        "symbol": case.symbol,
        "status": case.status.value,
        "recommendation_as_of": case.recommendation_as_of.isoformat(),
        "opened_at": case.opened_at.isoformat(),
        "expires_at": case.expires_at.isoformat(),
        "as_of": case.as_of.isoformat(),
        "opened_by": case.opened_by.value,
        "open_reason_codes": list(case.open_reason_codes),
        "evidence_count": len(case.evidence_ids),
        "candidate_provenance": (
            None
            if provenance is None
            else {
                "status": provenance.candidate_status.value,
                "priority": provenance.priority.name,
                "sources": [source.value for source in provenance.sources],
                "reason_codes": list(provenance.reason_codes),
                "observation_count": len(provenance.observation_ids),
                "source_run_count": len(provenance.source_run_ids),
                "evidence_count": len(provenance.evidence_ids),
                "candidate_as_of": provenance.candidate_as_of.isoformat(),
            }
        ),
        "research_confirmed": case.is_research_approved,
        "execution_authorized": False,
        "resolution": (
            None
            if resolution is None
            else {
                "status": resolution.status.value,
                "actor": resolution.actor.value,
                "reason_codes": list(resolution.reason_codes),
                "occurred_at": resolution.occurred_at.isoformat(),
                "operation_id": resolution.operation_id,
            }
        ),
    }


def _ashare_paper(
    action: str,
    ledger_db: str,
    account_id: str,
    initial_cash: Decimal | None,
    session_date: date | None,
    symbol: str | None,
    side: str | None,
    quantity: int | None,
    price: Decimal | None,
    instrument: str | None,
    source: str,
    fill_id: str | None,
    executed_at: datetime | None,
    external_order_id: str | None,
    note: str | None,
    actual_commission: Decimal | None,
    actual_transfer_fee: Decimal | None,
    actual_stamp_tax: Decimal | None,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """Operate the execution-led PAPER ledger; no market fill is invented."""

    from gribuki_trade.domain.orders import Side
    from gribuki_trade.domain.paper_trading import (
        ASharePaperFill,
        PaperFillFees,
        PaperFillReceipt,
        PaperFillSource,
        PaperInstrumentType,
    )
    from gribuki_trade.services.ashare_paper import ASharePaperTradingService
    from gribuki_trade.storage.paper_ledger import SQLitePaperLedger

    if action not in {"open", "snapshot", "rollover", "fill", "fills"}:
        raise ValueError("unsupported paper action")
    if not account_id.strip():
        raise ValueError("account_id must not be empty")
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None or resolved_now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    local_now = resolved_now.astimezone(ZoneInfo("Asia/Shanghai"))
    path = Path(ledger_db).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    with SQLitePaperLedger(path) as ledger:
        service = ASharePaperTradingService(ledger)
        receipt: PaperFillReceipt | None = None
        if action == "open":
            if initial_cash is None:
                raise ValueError("--initial-cash is required for open")
            snapshot = service.open_account(
                account_id,
                initial_cash=initial_cash,
                session_date=session_date or local_now.date(),
                opened_at=resolved_now,
            )
        elif action == "rollover":
            if session_date is None:
                raise ValueError("--session-date is required for rollover")
            snapshot = service.rollover_session(
                account_id,
                target_session_date=session_date,
                occurred_at=resolved_now,
            )
        elif action == "fill":
            missing = tuple(
                name
                for name, value in (
                    ("--symbol", symbol),
                    ("--side", side),
                    ("--quantity", quantity),
                    ("--price", price),
                    ("--instrument", instrument),
                    ("--fill-id", fill_id),
                )
                if value is None
            )
            if missing:
                raise ValueError(f"fill requires {', '.join(missing)}")
            actual_fees = (
                actual_commission,
                actual_transfer_fee,
                actual_stamp_tax,
            )
            if any(item is not None for item in actual_fees) and not all(
                item is not None for item in actual_fees
            ):
                raise ValueError("all three actual fee fields must be supplied together")
            if any(item is not None for item in actual_fees) and source != "MANUAL":
                raise ValueError("actual fee override is permitted only for MANUAL fills")
            assert symbol is not None
            assert side is not None
            assert quantity is not None
            assert price is not None
            assert instrument is not None
            assert fill_id is not None
            resolved_executed_at = executed_at or resolved_now
            trading_date = session_date or resolved_executed_at.astimezone(
                ZoneInfo("Asia/Shanghai")
            ).date()
            override = None
            if all(item is not None for item in actual_fees):
                assert actual_commission is not None
                assert actual_transfer_fee is not None
                assert actual_stamp_tax is not None
                override = PaperFillFees(
                    commission=actual_commission,
                    transfer_fee=actual_transfer_fee,
                    stamp_tax=actual_stamp_tax,
                )
            receipt = service.record_fill(
                ASharePaperFill(
                    account_id=account_id,
                    fill_id=fill_id,
                    symbol=symbol,
                    side=Side(side),
                    quantity=quantity,
                    price=price,
                    instrument_type=PaperInstrumentType(instrument),
                    trading_date=trading_date,
                    executed_at=resolved_executed_at,
                    source=PaperFillSource(source),
                    fee_override=override,
                    external_order_id=external_order_id,
                    note=note,
                ),
                recorded_at=resolved_now,
            )
            snapshot = receipt.snapshot
        else:
            snapshot = service.snapshot(account_id)
        fills = service.fills(account_id) if action == "fills" else ()
    result = {
        "ok": True,
        "action": action,
        "ledger_db": str(path),
        "account": _paper_snapshot_json(snapshot),
        "fills": [_paper_fill_json(item) for item in fills],
        "execution_model": "CONFIRMED_FILL_LEDGER_ONLY",
        "matching_implemented": False,
    }
    if receipt is not None:
        result["fill_receipt"] = {
            "applied_new": receipt.applied_new,
            "event_sequence": receipt.event_sequence,
            "fill": _paper_fill_json(receipt.applied_fill),
        }
    return result


def _paper_snapshot_json(snapshot: object) -> dict[str, object]:
    from gribuki_trade.domain.paper_trading import PaperAccountSnapshot

    if not isinstance(snapshot, PaperAccountSnapshot):
        raise TypeError("snapshot must be a PaperAccountSnapshot")
    return {
        "account_id": snapshot.account_id,
        "session_date": snapshot.session_date.isoformat(),
        "cash": str(snapshot.cash),
        "opened_at": snapshot.opened_at.isoformat(),
        "updated_at": snapshot.updated_at.isoformat(),
        "last_sequence": snapshot.last_sequence,
        "positions": [
            {
                "symbol": item.symbol,
                "instrument_type": item.instrument_type.value,
                "quantity": item.quantity,
                "available_to_sell": item.available_to_sell,
                "today_buy": item.today_buy,
                "average_cost": str(item.average_cost),
                "realized_pnl": str(item.realized_pnl),
            }
            for item in snapshot.positions
        ],
    }


def _paper_fill_json(applied: object) -> dict[str, object]:
    from gribuki_trade.domain.paper_trading import AppliedPaperFill

    if not isinstance(applied, AppliedPaperFill):
        raise TypeError("applied must be an AppliedPaperFill")
    fill = applied.fill
    return {
        "fill_id": fill.fill_id,
        "symbol": fill.symbol,
        "side": fill.side.value,
        "quantity": fill.quantity,
        "price": str(fill.price),
        "instrument_type": fill.instrument_type.value,
        "trading_date": fill.trading_date.isoformat(),
        "executed_at": fill.executed_at.isoformat(),
        "source": fill.source.value,
        "trade_value": str(applied.trade_value),
        "fees": {
            "commission": str(applied.fees.commission),
            "transfer_fee": str(applied.fees.transfer_fee),
            "stamp_tax": str(applied.fees.stamp_tax),
            "total": str(applied.fees.total),
        },
        "cash_change": str(applied.cash_change),
        "realized_pnl_change": str(applied.realized_pnl_change),
        "external_order_id": fill.external_order_id,
        "note": fill.note,
    }


async def _ashare_research_once(
    symbol: str,
    interval_value: str,
    lookback_minutes: int,
    events_db: str,
    research_db: str,
    outbox_db: str,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    notify_target_kind: str | None,
    notify_target_id: str | None,
) -> dict[str, object]:
    from gribuki_trade.adapters import AKShareMarketDataAdapter
    from gribuki_trade.adapters.llm import (
        DeepSeekChatMacroAnalyzer,
        OpenAIResponsesMacroAnalyzer,
    )
    from gribuki_trade.domain.recommendations import RecommendationHorizon
    from gribuki_trade.ports.llm_analyzer import MacroAnalyzer
    from gribuki_trade.ports.market_data import MinuteInterval
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.security.config import SecretValue
    from gribuki_trade.services import (
        AShareResearchRequest,
        AShareResearchService,
        MacroResearchService,
        ResearchNotificationTarget,
        select_macro_evidence,
    )
    from gribuki_trade.storage import (
        SQLiteEventStore,
        SQLiteOutbox,
        SQLiteResearchStore,
    )

    if lookback_minutes < 1:
        raise ValueError("lookback_minutes must be positive")
    if model is not None and not model.strip():
        raise ValueError("model must not be empty")
    if macro_provider not in {"deepseek", "openai"}:
        raise ValueError("macro_provider must be deepseek or openai")
    if (notify_target_kind is None) != (notify_target_id is None):
        raise ValueError("notification target kind and ID must be supplied together")
    resolved_model = model or (
        DEFAULT_DEEPSEEK_MODEL if macro_provider == "deepseek" else "gpt-5.6"
    )

    window_end = datetime.now(UTC)
    event_path = Path(events_db).resolve()
    events: tuple[NormalizedEvent, ...] = ()
    if event_path.is_file():
        with SQLiteEventStore(event_path) as event_store:
            events = event_store.latest(limit=1_000)

    interval = (
        MinuteInterval.ONE_MINUTE
        if interval_value == "1m"
        else MinuteInterval.FIVE_MINUTES
    )
    market_data = AKShareMarketDataAdapter()
    collection_request = AShareResearchRequest(
        symbol=symbol,
        start=window_end - timedelta(minutes=lookback_minutes),
        end=window_end,
        interval=interval,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
    )
    collection = await AShareResearchService(market_data).collect_market_data(
        collection_request
    )
    # The decision boundary must be after the provider response.  Freezing it
    # before network I/O makes every real fetched_at look like future data.
    decision_time = datetime.now(UTC)

    macro = None
    if macro_enabled and collection.failure_code is None:
        analyzer: MacroAnalyzer
        if macro_provider == "deepseek":
            analyzer = DeepSeekChatMacroAnalyzer(
                SecretValue(_required_local_secret(DEEPSEEK_API_KEY_SECRET)),
                model=resolved_model,
            )
        else:
            analyzer = OpenAIResponsesMacroAnalyzer(
                SecretValue(_required_local_secret(OPENAI_API_KEY_SECRET)),
                model=resolved_model,
            )
        macro_run = await MacroResearchService(analyzer).analyze(
            symbol=symbol,
            as_of=decision_time,
            horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS.value,
            technical_summary=("deterministic completed-bar breakout analysis",),
            events=events,
        )
        macro = macro_run.analysis
        selection = macro_run.selection
        macro_failure_code = macro_run.failure_code
    else:
        selection = select_macro_evidence(symbol, decision_time, events)
        macro_failure_code = (
            "SKIPPED_MARKET_DATA_UNAVAILABLE"
            if macro_enabled
            else None
        )
    research_path = Path(research_db).resolve()
    outbox_path = Path(outbox_db).resolve()
    research_path.parent.mkdir(parents=True, exist_ok=True)
    outbox_path.parent.mkdir(parents=True, exist_ok=True)
    notification_target = (
        None
        if notify_target_id is None or notify_target_kind is None
        else ResearchNotificationTarget(
            target_id=notify_target_id,
            target_kind=NotificationTargetKind(notify_target_kind),
        )
    )
    with SQLiteOutbox(outbox_path) as outbox:
        service = AShareResearchService(
            market_data,
            outbox=outbox,
        )
        run = service.evaluate_collection(
            AShareResearchRequest(
                symbol=symbol,
                start=window_end - timedelta(minutes=lookback_minutes),
                end=window_end,
                decision_time=decision_time,
                interval=interval,
                horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
                evidence=selection.references,
                macro=macro,
            ),
            collection,
            notification_target=notification_target,
        )
    recommendation = run.recommendation
    with SQLiteResearchStore(research_path) as research_store:
        stored = research_store.append_recommendation(recommendation)

    return {
        "as_of": recommendation.as_of.isoformat(),
        "confidence": recommendation.confidence.value,
        "combined_score": _three_decimal_text(
            getattr(recommendation, "combined_score", recommendation.technical_score)
        ),
        "fusion_reason_codes": list(
            getattr(recommendation, "fusion_reason_codes", ())
        ),
        "fusion_version": getattr(recommendation, "fusion_version", None),
        "technical_fusion_weight": _three_decimal_text(
            getattr(recommendation, "technical_fusion_weight", None)
        ),
        "macro_fusion_weight": _three_decimal_text(
            getattr(recommendation, "macro_fusion_weight", None)
        ),
        "macro_evidence_coverage": _three_decimal_text(
            getattr(recommendation, "macro_evidence_coverage", None)
        ),
        "decision": recommendation.decision.value,
        "evidence": [
            {
                "evidence_id": item.evidence_id,
                "title": item.title,
                "url": item.canonical_url,
            }
            for item in recommendation.evidence
        ],
        "macro_enabled": macro_enabled,
        "macro_failure_code": macro_failure_code,
        "macro_model": resolved_model if macro_enabled else None,
        "macro_provider": macro_provider if macro_enabled else None,
        "macro_score": _decimal_text(recommendation.macro_score),
        "market_data_failure_code": run.failure_code,
        "notification_enqueued": run.notification_enqueued,
        "reason_codes": list(recommendation.reason_codes),
        "recommendation_id": recommendation.recommendation_id,
        "reference_price": _decimal_text(recommendation.reference_price),
        "research_db": str(research_path),
        "stored_new": stored,
        "symbol": recommendation.symbol,
        "technical_score": _decimal_text(recommendation.technical_score),
        "uncertainties": list(recommendation.uncertainties),
    }


async def _ashare_close_research_batch(
    symbols: Sequence[str] | None,
    candidate_store_path: str | None,
    limit: int,
    history_days: int,
    session_date: date | None,
    next_session: date | None,
    news_runtime_dir: str,
    research_db: str,
    market_evidence_dir: str,
    outbox_db: str,
    news_feeds: Sequence[str] | None,
    refresh_news: bool,
    search_discovery: bool,
    searxng_url: str | None,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    macro_weight: Decimal,
    held_symbols: Sequence[str] | None,
    notify_target_kind: str | None,
    notify_target_id: str | None,
    report_dir: str | None,
) -> dict[str, object]:
    """Run a bounded, failure-isolated close-analysis batch sequentially.

    Sequential execution is intentional: the current public providers are not
    documented for high fan-out, while BaoStock also owns process-global
    session state.  The batch is a research callback and has no order path.
    """

    from gribuki_trade.domain.candidates import (
        CandidateStatus,
        canonical_ashare_symbol,
    )
    from gribuki_trade.storage import SQLiteCandidateStore

    if limit < 1 or limit > 30:
        raise ValueError("batch limit must be between 1 and 30")
    if (notify_target_kind is None) != (notify_target_id is None):
        raise ValueError("notification target kind and ID must be supplied together")

    requested: list[str] = []
    for symbol in symbols or ():
        canonical = canonical_ashare_symbol(symbol)
        if canonical not in requested:
            requested.append(canonical)

    if candidate_store_path is not None:
        candidate_path = Path(candidate_store_path).resolve()
        if not candidate_path.is_file():
            return {
                "error_code": "CANDIDATE_STORE_NOT_FOUND",
                "ok": False,
                "candidate_db": str(candidate_path),
                "results": [],
            }
        with SQLiteCandidateStore(candidate_path) as candidate_store:
            candidates = candidate_store.list_candidates(
                as_of=datetime.now(UTC),
                statuses=frozenset({CandidateStatus.ACTIVE}),
                limit=500,
            )
        for candidate in candidates:
            if candidate.symbol not in requested:
                requested.append(candidate.symbol)

    if not requested:
        return {
            "error_code": "NO_RESEARCH_SYMBOLS",
            "ok": False,
            "results": [],
        }

    held = {
        canonical_ashare_symbol(symbol) for symbol in (held_symbols or ())
    }
    selected = requested[:limit]
    deferred = requested[limit:]
    results: list[dict[str, object]] = []
    for symbol in selected:
        try:
            result = await _ashare_close_research_once(
                symbol,
                history_days,
                session_date,
                next_session,
                news_runtime_dir,
                research_db,
                market_evidence_dir,
                outbox_db,
                news_feeds,
                refresh_news,
                search_discovery,
                searxng_url,
                macro_enabled,
                macro_provider,
                model,
                macro_weight,
                symbol in held,
                notify_target_kind,
                notify_target_id,
                report_dir,
            )
        except Exception:
            result = {
                "error_code": "UNEXPECTED_RESEARCH_FAILURE",
                "ok": False,
                "symbol": symbol,
            }
        results.append(_close_batch_result_summary(symbol, result))

    succeeded = sum(item.get("ok") is True for item in results)
    return {
        "candidate_db": (
            None
            if candidate_store_path is None
            else str(Path(candidate_store_path).resolve())
        ),
        "deferred_symbols": deferred,
        "failed_count": len(results) - succeeded,
        "ok": succeeded == len(results),
        "requested_count": len(requested),
        "results": results,
        "selected_count": len(selected),
        "succeeded_count": succeeded,
    }


def _close_batch_result_summary(
    symbol: str,
    result: dict[str, object],
) -> dict[str, object]:
    """Keep batch stdout bounded while retaining each durable result identity."""

    retained_fields = (
        "analysis_mode",
        "as_of",
        "combined_score",
        "decision",
        "error_code",
        "latest_completed_session",
        "macro_failure_code",
        "market_data_failure_code",
        "next_session",
        "notification_enqueued",
        "ok",
        "recommendation_id",
        "report_markdown",
        "stored_new",
        "technical_score",
    )
    summary = {key: result[key] for key in retained_fields if key in result}
    summary["symbol"] = str(result.get("symbol", symbol))
    return summary


async def _ashare_close_research_once(
    symbol: str,
    history_days: int,
    session_date: date | None,
    next_session: date | None,
    news_runtime_dir: str,
    research_db: str,
    market_evidence_dir: str,
    outbox_db: str,
    news_feeds: Sequence[str] | None,
    refresh_news: bool,
    search_discovery: bool,
    searxng_url: str | None,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    macro_weight: Decimal,
    held: bool,
    notify_target_kind: str | None,
    notify_target_id: str | None,
    report_dir: str | None = None,
) -> dict[str, object]:
    """Run one calendar-verified after-close analysis without any order path."""

    from gribuki_trade.adapters import (
        AKShareAShareBreadthAdapter,
        AKShareCrossMarketAdapter,
        AKShareCrossMarketHistoryAdapter,
        AKShareDailyAssetType,
        AKShareETFContextAdapter,
        AKShareHistoricalDailyAdapter,
        AKShareIFContextAdapter,
        AKShareLiquidityContextAdapter,
        ArchivedHistoricalDailyAdapter,
        BaoStockDailyAdapter,
        CboeVIXDailyAdapter,
        HistoricalDailyFallbackRouter,
        HistoricalDailyTailStitchPolicy,
        OfficialShiborAdapter,
        SafeCentralParityAdapter,
        SSEETFShareAdapter,
        SSEOptionRiskAdapter,
    )
    from gribuki_trade.adapters.llm import (
        DeepSeekChatMacroAnalyzer,
        OpenAIResponsesMacroAnalyzer,
    )
    from gribuki_trade.domain.recommendations import (
        EvidenceReference,
        RecommendationDecision,
    )
    from gribuki_trade.policy.recommendation_gate import RecommendationGateConfig
    from gribuki_trade.ports.ashare_breadth import AShareBreadthDataError
    from gribuki_trade.ports.ashare_context import (
        AShareContextDataError,
        ETFContextSnapshot,
        IFDailyContextSnapshot,
        LiquidityContextSnapshot,
    )
    from gribuki_trade.ports.ashare_derivatives import (
        SSEETFShareDataError,
        SSEETFShareObservation,
        SSEOptionRiskDataError,
        SSEOptionRiskSnapshot,
    )
    from gribuki_trade.ports.cross_market import CrossMarketDataError
    from gribuki_trade.ports.cross_market_history import CrossMarketHistoryDataError
    from gribuki_trade.ports.global_risk import GlobalRiskDataError
    from gribuki_trade.ports.llm_analyzer import MacroAnalyzer
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.ports.official_rates import OfficialRatesDataError
    from gribuki_trade.security.config import SecretValue
    from gribuki_trade.services import (
        AShareCloseAnalysisRequest,
        AShareCloseAnalysisService,
        AShareCloseSessionResolver,
        CloseSessionResolutionError,
        ResearchNotificationTarget,
        build_ashare_breadth_evidence,
        build_ashare_context_evidence,
        build_ashare_derivatives_evidence,
        build_official_rates_evidence,
        build_vix_evidence,
        format_close_analysis_notification,
    )
    from gribuki_trade.storage import (
        SQLiteOutbox,
        SQLiteResearchStore,
        archive_daily_bar_evidence,
    )

    if history_days < 1:
        raise ValueError("history_days must be positive")
    if model is not None and not model.strip():
        raise ValueError("model must not be empty")
    if macro_provider not in {"deepseek", "openai"}:
        raise ValueError("macro_provider must be deepseek or openai")
    if not Decimal("0") <= macro_weight <= Decimal("0.40"):
        raise ValueError("macro_weight must be between zero and 0.40")
    if (notify_target_kind is None) != (notify_target_id is None):
        raise ValueError("notification target kind and ID must be supplied together")

    calendar_provider = BaoStockDailyAdapter(max_attempts=1, timeout_seconds=20.0)
    try:
        sessions = await AShareCloseSessionResolver(calendar_provider).resolve(
            datetime.now(UTC),
            latest_completed_session=session_date,
            next_session=next_session,
        )
    except CloseSessionResolutionError as error:
        return {
            "error_code": error.code,
            "ok": False,
            "symbol": symbol.strip().upper(),
        }

    history_start = sessions.latest_completed_session - timedelta(days=history_days)
    instrument_type = _infer_close_instrument_type(symbol)
    instrument_profile, instrument_profile_failure_code = (
        await _resolve_close_instrument_profile_for_run(symbol)
    )
    if instrument_profile is None:
        return {
            "error_code": "INSTRUMENT_PROFILE_UNAVAILABLE",
            "instrument_profile_failure_code": instrument_profile_failure_code,
            "ok": False,
            "symbol": symbol.strip().upper(),
        }
    canonical_symbol = instrument_profile.symbol
    network_daily_provider = HistoricalDailyFallbackRouter(
        calendar_provider,
        AKShareHistoricalDailyAdapter(
            asset_types={
                canonical_symbol: AKShareDailyAssetType(instrument_profile.asset_type)
            },
            timeout_seconds=20.0,
            minimum_source_bars=201,
        ),
        minimum_bars=201,
        primary_name="BaoStock",
        fallback_name="AKShare",
        tail_stitch_policy=HistoricalDailyTailStitchPolicy(
            minimum_overlap_sessions=20,
        ),
    )
    archive_daily_provider = ArchivedHistoricalDailyAdapter(
        Path(market_evidence_dir).resolve(),
        as_of=datetime.now(UTC),
        minimum_bars=201,
    )
    daily_provider = HistoricalDailyFallbackRouter(
        network_daily_provider,
        archive_daily_provider,
        minimum_bars=201,
        primary_name="Network",
        fallback_name="LocalImmutableArchive",
    )
    initial_request = AShareCloseAnalysisRequest(
        symbol=symbol,
        history_start=history_start,
        latest_completed_session=sessions.latest_completed_session,
        next_session=sessions.next_session,
        is_currently_held=held,
        instrument_type=instrument_type,
        instrument_profile=instrument_profile,
        calendar_verified=sessions.calendar_verified,
    )
    collection_service = AShareCloseAnalysisService(daily_provider)
    collection = await collection_service.collect_market_data(initial_request)
    preliminary = collection_service.assess_collection(initial_request, collection)

    market_evidence = None
    market_archive_path: str | None = None
    if collection.bars:
        if (collection.source_name or "").startswith("LocalImmutableArchive/"):
            retained = await archive_daily_provider.fetch_daily_bars_async_with_archive(
                initial_request.canonical_symbol,
                history_start,
                sessions.latest_completed_session,
            )
            market_evidence = EvidenceReference(
                evidence_id=retained.document_id,
                title=(
                    "Retained unadjusted daily bars for "
                    f"{initial_request.canonical_symbol} through "
                    f"{sessions.latest_completed_session.isoformat()}"
                ),
                canonical_url=(
                    f"local://market-evidence/{retained.document_id}"
                ),
                published_at=datetime.combine(
                    sessions.latest_completed_session,
                    datetime_time(15, 5),
                    tzinfo=ZoneInfo("Asia/Shanghai"),
                ),
                first_seen_at=retained.first_seen_at,
                source_tier=2,
            )
            market_archive_path = str(
                Path(market_evidence_dir).resolve()
                / "records"
                / retained.document_id[:2]
                / f"{retained.document_id}.json"
            )
        else:
            archived = archive_daily_bar_evidence(
                Path(market_evidence_dir).resolve(),
                symbol=initial_request.canonical_symbol,
                latest_completed_session=sessions.latest_completed_session,
                target_session=sessions.next_session,
                fetched_at=collection.fetched_at,
                bars=collection.bars,
                provider_id=_daily_evidence_provider_id(collection.source_name),
                provider_name=collection.source_name or "BaoStock",
            )
            market_evidence = archived.reference
            market_archive_path = str(archived.stored.metadata_path)

    news_root = Path(news_runtime_dir).resolve()
    news_root.mkdir(parents=True, exist_ok=True)
    if refresh_news:
        events, news_sources = await _collect_close_research_news(
            initial_request.canonical_symbol,
            news_root,
            news_feeds,
            entity_aliases=(instrument_profile.name,) if instrument_profile else (),
            industry=instrument_profile.industry,
            lookback_start=sessions.latest_completed_session - timedelta(days=14),
            lookback_end=datetime.now(ZoneInfo("Asia/Shanghai")).date(),
            search_discovery=search_discovery,
            tavily_api_key=(
                _optional_local_secret(TAVILY_API_KEY_SECRET)
                if search_discovery
                else None
            ),
            searxng_base_url=searxng_url if search_discovery else None,
            searxng_bearer_token=(
                _optional_local_secret(SEARXNG_BEARER_TOKEN_SECRET)
                if search_discovery and searxng_url is not None
                else None
            ),
        )
    else:
        events = _load_retained_news_events(news_root / "events.sqlite3")
        news_sources = []

    cross_market_snapshot = None
    cross_market_failure_code: str | None = None
    try:
        cross_market_snapshot = await AKShareCrossMarketAdapter(
            timeout_seconds=15.0
        ).fetch_cross_market_snapshot_async()
    except CrossMarketDataError:
        cross_market_failure_code = "CROSS_MARKET_DATA_UNAVAILABLE"

    cross_market_history = None
    cross_market_history_failure_code: str | None = None
    history_cutoff = datetime.now(UTC)
    try:
        cross_market_history = await AKShareCrossMarketHistoryAdapter(
            timeout_seconds=15.0
        ).fetch_cross_market_history_async(as_of=history_cutoff)
    except CrossMarketHistoryDataError:
        cross_market_history_failure_code = "CROSS_MARKET_HISTORY_UNAVAILABLE"

    async def collect_context(
        context_id: str,
        operation: Awaitable[object],
    ) -> tuple[str, object | None, str | None]:
        try:
            result = await operation
        except AShareContextDataError:
            return context_id, None, f"{context_id}_UNAVAILABLE"
        return context_id, result, None

    context_jobs = [
        collect_context(
            "LIQUIDITY_CONTEXT",
            AKShareLiquidityContextAdapter(
                timeout_seconds=15.0
            ).fetch_liquidity_context_async(
                start_date=sessions.latest_completed_session - timedelta(days=14),
                end_date=sessions.latest_completed_session,
            ),
        ),
        collect_context(
            "IF_CONTEXT",
            AKShareIFContextAdapter(timeout_seconds=20.0).fetch_if_daily_context_async(
                sessions.latest_completed_session
            ),
        ),
    ]
    if instrument_type is CloseInstrumentType.ETF:
        context_jobs.append(
            collect_context(
                "ETF_CONTEXT",
                AKShareETFContextAdapter(
                    timeout_seconds=30.0
                ).fetch_etf_context_async(canonical_symbol),
            )
        )
    context_results = await asyncio.gather(*context_jobs)
    context_values = {
        context_id: value for context_id, value, _failure in context_results
    }
    ashare_context_failure_codes = tuple(
        failure
        for _context_id, _value, failure in context_results
        if failure is not None
    )
    etf_context = cast(
        ETFContextSnapshot | None,
        context_values.get("ETF_CONTEXT"),
    )
    liquidity_context = cast(
        LiquidityContextSnapshot | None,
        context_values.get("LIQUIDITY_CONTEXT"),
    )
    if_context = cast(
        IFDailyContextSnapshot | None,
        context_values.get("IF_CONTEXT"),
    )

    ashare_breadth_snapshot = None
    ashare_breadth_failure_codes: tuple[str, ...] = ()
    try:
        ashare_breadth_snapshot = await AKShareAShareBreadthAdapter(
            timeout_seconds=20.0
        ).fetch_close_breadth_async(sessions.latest_completed_session)
    except AShareBreadthDataError:
        ashare_breadth_failure_codes = ("ASHARE_CLOSE_BREADTH_UNAVAILABLE",)

    global_risk = None
    global_risk_failure_codes: tuple[str, ...] = ()
    vix_visibility_cutoff = datetime.now(UTC)
    try:
        vix_history = await CboeVIXDailyAdapter(
            timeout_seconds=15.0
        ).fetch_vix_daily_history(as_of=vix_visibility_cutoff)
        global_risk = build_vix_evidence(
            vix_history,
            as_of=datetime.now(UTC),
        )
    except GlobalRiskDataError:
        global_risk_failure_codes = ("CBOE_VIX_EOD_UNAVAILABLE",)

    official_rates = None
    official_rates_failure_codes: list[str] = []
    official_rates_cutoff = datetime.now(UTC)
    safe_history = None
    shibor_history = None
    rate_start = sessions.latest_completed_session - timedelta(days=14)
    try:
        safe_history = await SafeCentralParityAdapter(
            timeout_seconds=15.0
        ).fetch_usd_cny_history(
            start_date=rate_start,
            end_date=sessions.latest_completed_session,
            as_of=official_rates_cutoff,
        )
    except OfficialRatesDataError:
        official_rates_failure_codes.append("SAFE_USD_CNY_UNAVAILABLE")
    try:
        shibor_history = await OfficialShiborAdapter(
            timeout_seconds=15.0
        ).fetch_shibor_history(
            start_date=rate_start,
            end_date=sessions.latest_completed_session,
            as_of=official_rates_cutoff,
        )
    except OfficialRatesDataError:
        official_rates_failure_codes.append("OFFICIAL_SHIBOR_UNAVAILABLE")
    official_rates = build_official_rates_evidence(
        safe_history,
        shibor_history,
        as_of=datetime.now(UTC),
    )

    etf_share_observation: SSEETFShareObservation | None = None
    option_risk_snapshot: SSEOptionRiskSnapshot | None = None
    ashare_derivatives_failure_codes: list[str] = []
    if (
        instrument_type is CloseInstrumentType.ETF
        and instrument_profile.exchange == "sse"
    ):
        local_symbol = canonical_symbol.split(".", maxsplit=1)[0]
        try:
            etf_share_observation = await SSEETFShareAdapter(
                timeout_seconds=15.0
            ).fetch_etf_shares(
                local_symbol,
                sessions.latest_completed_session,
                allow_latest_available=True,
            )
        except SSEETFShareDataError:
            ashare_derivatives_failure_codes.append(
                "SSE_ETF_OFFICIAL_SHARES_UNAVAILABLE"
            )
        try:
            option_risk_snapshot = await SSEOptionRiskAdapter(
                timeout_seconds=15.0
            ).fetch_option_risk(
                local_symbol,
                sessions.latest_completed_session,
                allow_latest_available=True,
            )
        except SSEOptionRiskDataError:
            ashare_derivatives_failure_codes.append(
                "SSE_OPTION_OFFICIAL_RISK_UNAVAILABLE"
            )

    # The report decision timestamp must be captured after every network fetch.
    # Otherwise a source fetched a few milliseconds later than the earlier
    # timestamp is correctly rejected by the PIT evidence gate as "future".
    decision_time = datetime.now(UTC)
    local_decision_time = decision_time.astimezone(ZoneInfo("Asia/Shanghai"))
    if (
        sessions.next_session == local_decision_time.date()
        and local_decision_time.time().replace(tzinfo=None) >= datetime_time(9, 30)
    ):
        return {
            "error_code": "NEXT_SESSION_ALREADY_OPENED",
            "ok": False,
            "symbol": initial_request.canonical_symbol,
        }
    ashare_context = build_ashare_context_evidence(
        etf_context,
        liquidity_context,
        if_context,
        cross_market_snapshot,
        decision_time,
        etf_expected=instrument_type is CloseInstrumentType.ETF,
    )
    ashare_breadth = build_ashare_breadth_evidence(
        ashare_breadth_snapshot,
        decision_time,
    )
    ashare_derivatives = build_ashare_derivatives_evidence(
        etf_share_observation,
        option_risk_snapshot,
        as_of=decision_time,
    )

    analyzer: MacroAnalyzer | None = None
    resolved_model = model or (
        DEFAULT_DEEPSEEK_MODEL if macro_provider == "deepseek" else "gpt-5.6"
    )
    if macro_enabled and preliminary.decision is not RecommendationDecision.ABSTAIN:
        if macro_provider == "deepseek":
            analyzer = DeepSeekChatMacroAnalyzer(
                SecretValue(_required_local_secret(DEEPSEEK_API_KEY_SECRET)),
                model=resolved_model,
            )
        else:
            analyzer = OpenAIResponsesMacroAnalyzer(
                SecretValue(_required_local_secret(OPENAI_API_KEY_SECRET)),
                model=resolved_model,
            )

    final_request = AShareCloseAnalysisRequest(
        symbol=initial_request.canonical_symbol,
        history_start=history_start,
        latest_completed_session=sessions.latest_completed_session,
        next_session=sessions.next_session,
        events=events,
        market_evidence=market_evidence,
        as_of=decision_time,
        is_currently_held=held,
        instrument_type=instrument_type,
        instrument_profile=instrument_profile,
        ashare_context=ashare_context,
        ashare_context_failure_codes=ashare_context_failure_codes,
        ashare_breadth=ashare_breadth,
        ashare_breadth_snapshot=ashare_breadth_snapshot,
        ashare_breadth_failure_codes=ashare_breadth_failure_codes,
        global_risk=global_risk,
        global_risk_failure_codes=global_risk_failure_codes,
        official_rates=official_rates,
        official_rates_failure_codes=tuple(official_rates_failure_codes),
        ashare_derivatives=ashare_derivatives,
        ashare_derivatives_failure_codes=tuple(
            ashare_derivatives_failure_codes
        ),
        cross_market_snapshot=cross_market_snapshot,
        cross_market_failure_code=cross_market_failure_code,
        cross_market_history=cross_market_history,
        cross_market_history_failure_code=cross_market_history_failure_code,
        calendar_verified=sessions.calendar_verified,
    )
    target = (
        None
        if notify_target_kind is None or notify_target_id is None
        else ResearchNotificationTarget(
            target_id=notify_target_id,
            target_kind=NotificationTargetKind(notify_target_kind),
            decisions=frozenset(RecommendationDecision),
            max_characters=7_500,
        )
    )
    research_path = Path(research_db).resolve()
    outbox_path = Path(outbox_db).resolve()
    research_path.parent.mkdir(parents=True, exist_ok=True)
    outbox_path.parent.mkdir(parents=True, exist_ok=True)
    with SQLiteOutbox(outbox_path) as outbox:
        service = AShareCloseAnalysisService(
            daily_provider,
            macro_analyzer=analyzer,
            gate_config=RecommendationGateConfig(
                technical_weight=Decimal("1") - macro_weight,
                macro_weight=macro_weight,
            ),
            outbox=outbox,
        )
        run = await service.evaluate_collection(
            final_request,
            collection,
            notification_target=target,
        )
    with SQLiteResearchStore(research_path) as research_store:
        stored = research_store.append_recommendation(run.recommendation)

    recommendation = run.recommendation
    report_markdown: str | None = None
    report_images: list[str] = []
    report_render_failure_code: str | None = None
    if report_dir is not None:
        from gribuki_trade.reporting import export_report_artifacts

        local_report_target = ResearchNotificationTarget(
            target_id="1",
            target_kind=NotificationTargetKind.PRIVATE,
            decisions=frozenset(RecommendationDecision),
            max_characters=100_000,
        )
        report_notification = format_close_analysis_notification(
            recommendation,
            run.assessment,
            run.macro,
            local_report_target,
            calendar_verified=sessions.calendar_verified,
            ashare_context_report_lines=run.ashare_context_report_lines,
            ashare_context_failure_codes=run.ashare_context_failure_codes,
            ashare_breadth_report_lines=run.ashare_breadth_report_lines,
            ashare_breadth_failure_codes=run.ashare_breadth_failure_codes,
            global_risk_report_lines=run.global_risk_report_lines,
            global_risk_failure_codes=run.global_risk_failure_codes,
            official_rates_report_lines=run.official_rates_report_lines,
            official_rates_failure_codes=run.official_rates_failure_codes,
            ashare_derivatives_report_lines=(
                run.ashare_derivatives_report_lines
            ),
            ashare_derivatives_failure_codes=(
                run.ashare_derivatives_failure_codes
            ),
            cross_market_report_lines=run.cross_market_report_lines,
            cross_market_failure_code=run.cross_market_failure_code,
            cross_market_relation_report_lines=run.cross_market_relation_report_lines,
            cross_market_history_failure_code=run.cross_market_history_failure_code,
            evidence_selection=run.evidence_selection,
        )
        report_root = Path(report_dir).resolve()
        report_stem = (
            f"{recommendation.symbol}-{sessions.next_session.strftime('%Y%m%d')}-"
            f"{recommendation.recommendation_id[:12]}"
        )
        try:
            artifacts = export_report_artifacts(
                report_notification.text,
                report_root,
                report_stem,
                overwrite=True,
            )
        except Exception:
            report_render_failure_code = "PNG_RENDER_FAILED"
            artifacts = export_report_artifacts(
                report_notification.text,
                report_root,
                report_stem,
                render_images=False,
                overwrite=True,
            )
        report_markdown = str(artifacts.markdown_path)
        report_images = [str(path) for path in artifacts.image_paths]
    return {
        "analysis_mode": sessions.analysis_mode.value,
        "as_of": recommendation.as_of.isoformat(),
        "calendar_verified": sessions.calendar_verified,
        "confidence": recommendation.confidence.value,
        "combined_score": _three_decimal_text(recommendation.combined_score),
        "fusion_reason_codes": list(recommendation.fusion_reason_codes),
        "fusion_version": recommendation.fusion_version,
        "technical_fusion_weight": _three_decimal_text(
            recommendation.technical_fusion_weight
        ),
        "macro_fusion_weight": _three_decimal_text(
            recommendation.macro_fusion_weight
        ),
        "macro_evidence_coverage": _three_decimal_text(
            recommendation.macro_evidence_coverage
        ),
        "ashare_context_evidence": len(ashare_context.items),
        "ashare_context_failure_codes": list(run.ashare_context_failure_codes),
        "ashare_breadth_evidence": len(ashare_breadth.items),
        "ashare_breadth_failure_codes": list(run.ashare_breadth_failure_codes),
        "cross_market_failure_code": run.cross_market_failure_code,
        "global_risk_evidence": 0 if global_risk is None else len(global_risk.items),
        "global_risk_failure_codes": list(run.global_risk_failure_codes),
        "official_rates_evidence": len(official_rates.items),
        "official_rates_failure_codes": list(run.official_rates_failure_codes),
        "ashare_derivatives_evidence": len(ashare_derivatives.items),
        "ashare_derivatives_failure_codes": list(
            run.ashare_derivatives_failure_codes
        ),
        "cross_market_history_failure_code": run.cross_market_history_failure_code,
        "cross_market_history_missing": (
            0 if cross_market_history is None else len(cross_market_history.missing)
        ),
        "cross_market_history_series": (
            0 if cross_market_history is None else len(cross_market_history.series)
        ),
        "cross_market_observations": len(run.cross_market_report_lines),
        "cross_market_relations": len(run.cross_market_relation_report_lines),
        "daily_bar_count": run.daily_bar_count,
        "decision": recommendation.decision.value,
        "evidence": [
            {
                "evidence_id": item.evidence_id,
                "title": item.title,
                "url": item.canonical_url,
            }
            for item in recommendation.evidence
        ],
        "instrument_type": instrument_type.value,
        "instrument_profile": _instrument_profile_document(instrument_profile),
        "instrument_profile_failure_code": instrument_profile_failure_code,
        "latest_completed_session": sessions.latest_completed_session.isoformat(),
        "macro": _macro_analysis_document(run.macro),
        "macro_enabled": macro_enabled,
        "macro_failure_code": run.macro_failure_code,
        "macro_score": _three_decimal_text(recommendation.macro_score),
        "macro_model": resolved_model if macro_enabled else None,
        "macro_provider": macro_provider if macro_enabled else None,
        "macro_weight": _three_decimal_text(macro_weight),
        "market_archive": market_archive_path,
        "market_data_failure_code": run.market_data_failure_code,
        "market_data_source": collection.source_name,
        "news_refreshed": refresh_news,
        "news_sources": news_sources,
        "search_discovery_enabled": search_discovery,
        "search_discovery_hint_count": sum(
            event.event_type == DISCOVERY_HINT_EVENT_TYPE for event in events
        ),
        "search_discovery_confirmed_count": sum(
            event.event_type == DISCOVERY_CONFIRMED_EVENT_TYPE for event in events
        ),
        "next_session": sessions.next_session.isoformat(),
        "notification_enqueued": run.notification_enqueued,
        "notification_parts": len(run.notifications),
        "ok": True,
        "outbox_db": str(outbox_path),
        "reason_codes": list(recommendation.reason_codes),
        "recommendation_id": recommendation.recommendation_id,
        "reference_price": _three_decimal_text(recommendation.reference_price),
        "report_images": report_images,
        "report_markdown": report_markdown,
        "report_render_failure_code": report_render_failure_code,
        "research_db": str(research_path),
        "stored_new": stored,
        "symbol": recommendation.symbol,
        "technical_decision": run.assessment.decision.value,
        "technical_metrics": {
            name: _three_decimal_text(value) for name, value in run.assessment.metrics
        },
        "technical_score": _three_decimal_text(recommendation.technical_score),
        "uncertainties": list(recommendation.uncertainties),
    }


def _infer_close_instrument_type(symbol: str) -> CloseInstrumentType:
    """Resolve stock/ETF semantics from the validated watchlist, then code family."""

    from gribuki_trade.watchlists import WatchlistAssetType, load_research_watchlist

    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        value = f"{value}.{'SH' if value.startswith(('5', '6', '9')) else 'SZ'}"
    try:
        watchlist = load_research_watchlist(Path(DEFAULT_ASHARE_WATCHLIST))
    except (OSError, ValueError):
        watchlist = None
    if watchlist is not None:
        matched = next(
            (item for item in watchlist.instruments if item.symbol == value),
            None,
        )
        if matched is not None:
            return CloseInstrumentType(matched.asset_type.value)

    code, _, exchange = value.partition(".")
    is_etf = (exchange == "SH" and code.startswith("5")) or (
        exchange == "SZ" and code.startswith(("15", "16"))
    )
    inferred = WatchlistAssetType.ETF if is_etf else WatchlistAssetType.STOCK
    return CloseInstrumentType(inferred.value)


def _resolve_close_instrument_profile(
    symbol: str,
) -> ResearchInstrumentProfile | None:
    """Return the retained watchlist profile without making it an order input."""

    from gribuki_trade.watchlists import load_research_watchlist

    try:
        watchlist = load_research_watchlist(Path(DEFAULT_ASHARE_WATCHLIST))
        return watchlist.instrument_profile(symbol)
    except (OSError, ValueError):
        return None


async def _resolve_close_instrument_profile_for_run(
    symbol: str,
) -> tuple[ResearchInstrumentProfile | None, str | None]:
    """Prefer the retained profile, then fetch a current dynamic snapshot.

    The live fallback is intentionally unsuitable for historical replay.  Its
    adapter enforces a near-current point-in-time cutoff and the resulting
    profile is persisted with the recommendation for later reproduction.
    """

    retained = _resolve_close_instrument_profile(symbol)
    if retained is not None:
        return retained, None

    from gribuki_trade.adapters import (
        AKShareInstrumentProfileAdapter,
        InstrumentProfileDataError,
    )

    try:
        profile_cutoff = datetime.now(UTC)
        profile = await AKShareInstrumentProfileAdapter(
            timeout_seconds=20.0
        ).fetch(symbol, known_at=profile_cutoff)
    except InstrumentProfileDataError as error:
        return None, error.code
    except ValueError:
        return None, "INVALID_ASHARE_SYMBOL"
    return profile, None


def _instrument_profile_document(
    profile: ResearchInstrumentProfile | None,
) -> dict[str, object] | None:
    if profile is None:
        return None
    return {
        "asset_type": profile.asset_type,
        "background_facts": list(profile.background_facts),
        "board": profile.board,
        "exchange": profile.exchange,
        "industry": profile.industry,
        "market": profile.market,
        "name": profile.name,
        "research_role": profile.research_role,
        "risk_tags": list(profile.risk_tags),
        "size_tier": profile.size_tier,
        "source_id": profile.source_id,
        "styles": list(profile.styles),
        "symbol": profile.symbol,
        "verified_on": profile.verified_on.isoformat(),
    }


def _daily_evidence_provider_id(source_name: str | None) -> str:
    """Map retained route diagnostics to a stable, non-URL source identifier."""

    if source_name is None or source_name == "BaoStock":
        return "baostock.daily"
    if source_name.startswith("MIXED/TAIL_STITCH"):
        return "mixed.tail_stitch.daily"
    if source_name.startswith("AKShare"):
        return "akshare.daily"
    return "other.research.daily"


async def _collect_close_research_news(
    symbol: str,
    root: Path,
    feed_values: Sequence[str] | None,
    *,
    entity_aliases: tuple[str, ...] = (),
    industry: str | None = None,
    lookback_start: date,
    lookback_end: date,
    search_discovery: bool = False,
    tavily_api_key: str | None = None,
    searxng_base_url: str | None = None,
    searxng_bearer_token: str | None = None,
) -> tuple[tuple[NormalizedEvent, ...], list[dict[str, object]]]:
    from gribuki_trade.ingest import (
        AKShareDisclosureConfig,
        AKShareDisclosureSource,
        AKShareNewsConfig,
        AKShareNewsFeed,
        AKShareNewsSource,
        HttpxNewsTransport,
        MultiProviderDiscoverySource,
        SearXNGSearchProvider,
        TavilySearchProvider,
        build_default_official_macro_sources,
    )
    from gribuki_trade.ports.news import DiscoveryQuery, NewsSearchProvider
    from gribuki_trade.services import NewsCollectionService
    from gribuki_trade.storage import FileRawDocumentStore, SQLiteEventStore

    selected = tuple(
        feed_values
        or (
            "global_sina",
            "global_cailianpress",
            "global_eastmoney",
            "global_10jqka",
        )
    )
    sources: dict[str, NewsSource] = {}
    for value in selected:
        config = AKShareNewsConfig(
            AKShareNewsFeed(value),
            timeout_seconds=15.0,
            max_attempts=1,
        )
        sources[config.source_id] = AKShareNewsSource(config)
    individual = AKShareNewsConfig(
        AKShareNewsFeed.INDIVIDUAL_EASTMONEY,
        symbol=symbol,
        entity_aliases=entity_aliases,
        timeout_seconds=15.0,
        max_attempts=1,
    )
    sources[individual.source_id] = AKShareNewsSource(individual)
    code = symbol.split(".", maxsplit=1)[0]
    if not code.startswith(("5", "159")):
        disclosure = AKShareDisclosureConfig(
            symbol=code,
            start_date=lookback_start,
            end_date=lookback_end,
            timeout_seconds=20.0,
            max_attempts=1,
        )
        sources[disclosure.source_id] = AKShareDisclosureSource(disclosure)

    official_sources = build_default_official_macro_sources()
    collisions = sources.keys() & official_sources.keys()
    if collisions:
        raise ValueError(f"duplicate news source IDs: {sorted(collisions)}")
    sources.update(official_sources)

    discovery_unconfigured = False
    if search_discovery:
        transport = HttpxNewsTransport()
        search_providers: list[NewsSearchProvider] = []
        if tavily_api_key is not None:
            search_providers.append(
                TavilySearchProvider(transport, api_key=tavily_api_key)
            )
        if searxng_base_url is not None:
            search_providers.append(
                SearXNGSearchProvider(
                    transport,
                    base_url=searxng_base_url,
                    bearer_token=searxng_bearer_token,
                )
            )
        if search_providers:
            instrument_name = entity_aliases[0] if entity_aliases else symbol
            query_aliases = tuple(
                alias for alias in entity_aliases[1:] if alias != instrument_name
            )
            discovery_queries = [
                DiscoveryQuery.for_stock(
                    symbol=symbol,
                    name=instrument_name,
                    industry=industry,
                    aliases=query_aliases,
                    max_results=8,
                ),
                DiscoveryQuery.for_macro(
                    "中国货币财政监管与A股流动性",
                    related_entities=(symbol, instrument_name),
                    max_results=6,
                ),
                DiscoveryQuery.for_macro(
                    "美联储美元美债美股与中国资产风险传导",
                    related_entities=(symbol, instrument_name),
                    max_results=6,
                ),
                DiscoveryQuery.for_macro(
                    "人民币港股商品能源地缘风险与A股行业传导",
                    related_entities=(symbol, instrument_name),
                    max_results=6,
                ),
            ]
            if industry is not None and industry.strip():
                discovery_queries.append(
                    DiscoveryQuery.for_industry(
                        industry,
                        related_entities=(symbol, instrument_name),
                        max_results=8,
                    )
                )
            discovery_source_id = (
                f"discovery.search.{symbol.split('.', maxsplit=1)[0]}"
            )
            if discovery_source_id in sources:
                raise ValueError(f"duplicate news source ID: {discovery_source_id}")
            sources[discovery_source_id] = MultiProviderDiscoverySource(
                search_providers,
                discovery_queries,
                source_id=discovery_source_id,
            )
        else:
            discovery_unconfigured = True

    event_path = root / "events.sqlite3"
    with SQLiteEventStore(event_path) as event_store:
        service = NewsCollectionService(
            sources,
            raw_store=FileRawDocumentStore(root / "raw"),
            event_store=event_store,
            max_concurrency=4,
        )
        results = await service.run_once()
        events = event_store.latest(limit=2_000)
    diagnostics: list[dict[str, object]] = [
        {
            "documents_saved": item.documents_saved,
            "error_code": item.error_code,
            "events_duplicate": item.events_duplicate,
            "events_new": item.events_new,
            "events_revised": item.events_revised,
            "source_id": item.source_id,
            "status": item.status.value,
        }
        for item in results
    ]
    if discovery_unconfigured:
        diagnostics.append(
            {
                "documents_saved": 0,
                "error_code": "SEARCH_DISCOVERY_PROVIDERS_NOT_CONFIGURED",
                "events_duplicate": 0,
                "events_new": 0,
                "events_revised": 0,
                "source_id": "discovery.search",
                "status": "UNCONFIGURED",
            }
        )
    return events, diagnostics


def _load_retained_news_events(path: Path) -> tuple[NormalizedEvent, ...]:
    from gribuki_trade.storage import SQLiteEventStore

    if not path.is_file():
        return ()
    with SQLiteEventStore(path) as event_store:
        return event_store.latest(limit=2_000)


def _macro_analysis_document(value: object | None) -> dict[str, object] | None:
    from gribuki_trade.analysis.schemas import MacroAnalysis

    if not isinstance(value, MacroAnalysis):
        return None
    return {
        "claims": [
            {
                "contradictions": list(item.contradictions),
                "evidence_ids": list(item.evidence_ids),
                "text": item.text,
            }
            for item in value.claims
        ],
        "data_gaps": list(value.data_gaps),
        "decision": value.decision.value,
        "invalidation_conditions": list(value.invalidation_conditions),
        "macro_impact": _three_decimal_text(value.macro_impact),
        "regime": value.regime,
        "reported_confidence": value.reported_confidence,
        "scenarios": [
            {
                "drivers": list(item.drivers),
                "evidence_ids": list(item.evidence_ids),
                "name": item.name,
                "probability": _three_decimal_text(item.probability),
            }
            for item in value.scenarios
        ],
        "technical_alignment": _three_decimal_text(value.technical_alignment),
        "uncertainties": list(value.uncertainties),
    }


async def _ashare_research_watch(
    symbols: Sequence[str] | None,
    watchlist_path: str,
    watchlist_all: bool,
    interval_value: str,
    lookback_minutes: int,
    interval_seconds: float,
    cycles: int,
    events_db: str,
    research_db: str,
    outbox_db: str,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    notify_target_kind: str | None,
    notify_target_id: str | None,
    candidate_store_path: str | None = None,
    candidates_only: bool = False,
) -> dict[str, object]:
    """Run bounded research cycles with per-symbol failure isolation."""

    from gribuki_trade.services import ResearchWatchService

    if cycles < 1:
        raise ValueError("cycles must be positive")
    if not 0 <= interval_seconds < float("inf"):
        raise ValueError("interval_seconds must be non-negative and finite")
    if lookback_minutes < 1:
        raise ValueError("lookback_minutes must be positive")
    if (notify_target_kind is None) != (notify_target_id is None):
        raise ValueError("notification target kind and ID must be supplied together")
    if candidates_only and candidate_store_path is None:
        raise ValueError("--candidates-only requires --candidate-db")

    resolved_symbols = _resolve_dynamic_research_symbols(
        symbols=symbols,
        watchlist_path=watchlist_path,
        watchlist_all=watchlist_all,
        candidate_store_path=candidate_store_path,
        candidates_only=candidates_only,
        as_of=datetime.now(UTC),
    )

    async def run_symbol(symbol: str) -> dict[str, object]:
        return await _ashare_research_once(
            symbol,
            interval_value,
            lookback_minutes,
            events_db,
            research_db,
            outbox_db,
            macro_enabled,
            macro_provider,
            model,
            notify_target_kind,
            notify_target_id,
        )

    service: ResearchWatchService[dict[str, object]] = ResearchWatchService(
        resolved_symbols,
        run_symbol,
    )
    statistics = await service.run(
        max_cycles=cycles,
        interval_seconds=interval_seconds,
    )
    return {
        "completed_at": statistics.completed_at.isoformat(),
        "cycles": [
            {
                "completed_at": cycle.completed_at.isoformat(),
                "cycle_number": cycle.cycle_number,
                "failed": cycle.failed,
                "started_at": cycle.started_at.isoformat(),
                "succeeded": cycle.succeeded,
                "symbols": [
                    {
                        "error_code": item.error_code,
                        "result": item.result,
                        "status": item.status.value,
                        "symbol": item.symbol,
                    }
                    for item in cycle.symbol_runs
                ],
            }
            for cycle in statistics.cycles
        ],
        "cycles_completed": statistics.cycles_completed,
        "failed": statistics.failed,
        "reached_cycle_limit": statistics.reached_cycle_limit,
        "started_at": statistics.started_at.isoformat(),
        "stop_requested": statistics.stop_requested,
        "succeeded": statistics.succeeded,
        "symbols": list(service.symbols),
        "symbols_attempted": statistics.symbols_attempted,
    }


def _resolve_ashare_research_symbols(
    symbols: Sequence[str] | None,
    *,
    watchlist_path: str,
    watchlist_all: bool,
) -> tuple[str, ...]:
    if symbols:
        return tuple(symbols)

    from gribuki_trade.watchlists import load_research_watchlist

    watchlist = load_research_watchlist(Path(watchlist_path).resolve())
    return watchlist.symbols if watchlist_all else watchlist.default_symbols


def _resolve_dynamic_research_symbols(
    *,
    symbols: Sequence[str] | None,
    watchlist_path: str,
    watchlist_all: bool,
    candidate_store_path: str | None,
    candidates_only: bool,
    as_of: datetime,
) -> tuple[str, ...]:
    resolved: list[str] = []
    if not candidates_only:
        resolved.extend(
            _resolve_ashare_research_symbols(
                symbols,
                watchlist_path=watchlist_path,
                watchlist_all=watchlist_all,
            )
        )
    if candidate_store_path is not None:
        from gribuki_trade.services.candidate_universe import CandidateUniverseService
        from gribuki_trade.storage.candidate_store import SQLiteCandidateStore

        path = Path(candidate_store_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with SQLiteCandidateStore(path) as store:
            candidates = CandidateUniverseService(store).tracking_candidates(
                as_of=as_of,
                include_cooling=False,
            )
        resolved.extend(item.symbol for item in candidates)
    unique = tuple(dict.fromkeys(resolved))
    if not unique:
        raise ValueError("no ACTIVE symbols are available for research monitoring")
    return unique


def _napcat_configure(
    runtime_dir: str,
    onebot_port: int,
    webui_port: int,
    force: bool,
) -> dict[str, object]:
    from gribuki_trade.napcat_setup import configure_portable_napcat_runtime

    result = configure_portable_napcat_runtime(
        Path(runtime_dir).resolve(),
        KeyringSecretProvider(),
        onebot_port=onebot_port,
        webui_port=webui_port,
        force=force,
    )
    return {
        "configured": True,
        "onebot_base_url": f"http://127.0.0.1:{result.onebot_port}",
        "onebot_config": str(result.onebot_config_path),
        "runtime_dir": str(result.runtime_dir),
        "tokens_stored_in_keyring": True,
        "webui_config": str(result.webui_config_path),
        "webui_url": f"http://127.0.0.1:{result.webui_port}",
    }


async def _napcat_status(base_url: str) -> dict[str, object]:
    from gribuki_trade.adapters.notifiers import (
        OneBotConfig,
        OneBotError,
        OneBotNotifier,
    )

    try:
        token = _required_local_secret(NAPCAT_ACCESS_TOKEN_SECRET)
    except (RuntimeError, SecretProviderError):
        return {
            "app_name": "unknown",
            "base_url": base_url,
            "error_code": "LOCAL_SECRET_UNAVAILABLE",
            "good": False,
            "next_action": (
                ".\\.venv\\Scripts\\python.exe -m gribuki_trade "
                "secret-set napcat.onebot.access_token"
            ),
            "online": False,
            "protocol_version": "unknown",
            "retryable": False,
        }
    try:
        async with OneBotNotifier(
            OneBotConfig(access_token=token, base_url=base_url)
        ) as notifier:
            status = await notifier.get_status()
            version = await notifier.get_version_info()
    except OneBotError as error:
        if error.code == "transport_error":
            next_action = (
                "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
                ".\\scripts\\start_napcat.ps1"
            )
        elif error.code == "authentication_rejected":
            next_action = (
                ".\\.venv\\Scripts\\python.exe -m gribuki_trade "
                "secret-set napcat.onebot.access_token"
            )
        else:
            next_action = "inspect the local NapCat console and WebUI"
        return {
            "app_name": "unknown",
            "base_url": base_url,
            "error_code": error.code,
            "good": False,
            "next_action": next_action,
            "online": False,
            "protocol_version": "unknown",
            "retryable": error.retryable,
        }
    return {
        "app_name": str(version.get("app_name", "unknown")),
        "base_url": base_url,
        "error_code": None,
        "good": bool(status.get("good", False)),
        "next_action": None,
        "online": bool(status.get("online", False)),
        "protocol_version": str(version.get("protocol_version", "unknown")),
        "retryable": False,
    }


async def _napcat_dispatch(
    base_url: str,
    target_kind_value: str,
    target_id: str,
    outbox_path: str,
    cycles: int,
    poll_interval: float,
) -> dict[str, object]:
    """Run a finite outbound-only OneBot outbox worker."""

    if cycles < 1:
        raise ValueError("cycles must be positive")
    if not 0 <= poll_interval < float("inf"):
        raise ValueError("poll_interval must be non-negative and finite")

    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.services import NotificationDispatchService
    from gribuki_trade.storage import SQLiteOutbox

    target_kind = NotificationTargetKind(target_kind_value)
    token = _required_local_secret(NAPCAT_ACCESS_TOKEN_SECRET)
    allowlist = frozenset({target_id})
    config = OneBotConfig(
        access_token=token,
        base_url=base_url,
        private_target_ids=(
            allowlist if target_kind is NotificationTargetKind.PRIVATE else frozenset()
        ),
        group_target_ids=(
            allowlist if target_kind is NotificationTargetKind.GROUP else frozenset()
        ),
    )
    path = Path(outbox_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with SQLiteOutbox(path) as outbox:
        async with OneBotNotifier(config) as notifier:
            service = NotificationDispatchService(
                outbox,
                {notifier.channel: notifier},
            )
            statistics = await service.poll(
                max_cycles=cycles,
                poll_interval=poll_interval,
            )
    return {
        "base_url": base_url,
        "claimed": statistics.claimed,
        "cycles_completed": statistics.cycles_completed,
        "dead": statistics.dead,
        "expired": statistics.expired,
        "outbox_path": str(path),
        "poll_interval": poll_interval,
        "reached_cycle_limit": statistics.reached_cycle_limit,
        "retry_scheduled": statistics.retry_scheduled,
        "sent": statistics.sent,
        "stop_requested": statistics.stop_requested,
        "target_id": target_id,
        "target_kind": target_kind.value,
    }


async def _napcat_send_test(
    base_url: str,
    target_kind_value: str,
    target_id: str,
) -> dict[str, object]:
    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.ports.notifier import (
        NotificationTargetKind,
        OutboundNotification,
    )

    target_kind = NotificationTargetKind(target_kind_value)
    token = _required_local_secret(NAPCAT_ACCESS_TOKEN_SECRET)
    allowlist = frozenset({target_id})
    config = OneBotConfig(
        access_token=token,
        base_url=base_url,
        private_target_ids=(
            allowlist if target_kind is NotificationTargetKind.PRIVATE else frozenset()
        ),
        group_target_ids=(
            allowlist if target_kind is NotificationTargetKind.GROUP else frozenset()
        ),
    )
    created_at = datetime.now(UTC)
    notification = OutboundNotification(
        idempotency_key=f"manual-health-test:{target_kind.value}:{int(created_at.timestamp())}",
        channel="onebot",
        target_kind=target_kind,
        target_id=target_id,
        text="Gribuki Trade 通知链路测试：仅验证 NapCat/OneBot 出站消息，不包含交易指令。",
        created_at=created_at,
        expires_at=created_at + timedelta(minutes=5),
    )
    async with OneBotNotifier(config) as notifier:
        receipt = await notifier.send(notification)
    return {
        "channel": receipt.channel,
        "delivered": True,
        "provider_message_id": receipt.provider_message_id,
        "target_kind": target_kind.value,
    }


async def _napcat_send_artifact(
    base_url: str,
    target_kind_value: str,
    target_id: str,
    artifact_kind: str,
    artifact_root: str,
    artifact: str,
) -> dict[str, object]:
    """Explicitly send one locally generated report artifact through NapCat."""

    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.ports.notifier import NotificationTargetKind

    target_kind = NotificationTargetKind(target_kind_value)
    if artifact_kind not in {"image", "file"}:
        raise ValueError("artifact_kind must be image or file")
    token = _required_local_secret(NAPCAT_ACCESS_TOKEN_SECRET)
    allowlist = frozenset({target_id})
    root = Path(artifact_root).resolve()
    config = OneBotConfig(
        access_token=token,
        base_url=base_url,
        private_target_ids=(
            allowlist if target_kind is NotificationTargetKind.PRIVATE else frozenset()
        ),
        group_target_ids=(
            allowlist if target_kind is NotificationTargetKind.GROUP else frozenset()
        ),
        artifact_root=root,
    )
    async with OneBotNotifier(config) as notifier:
        if artifact_kind == "image":
            receipt = (
                await notifier.send_private_image(target_id, artifact)
                if target_kind is NotificationTargetKind.PRIVATE
                else await notifier.send_group_image(target_id, artifact)
            )
            provider_identifier = receipt.provider_message_id
        else:
            file_receipt = (
                await notifier.upload_private_file(target_id, artifact)
                if target_kind is NotificationTargetKind.PRIVATE
                else await notifier.upload_group_file(target_id, artifact)
            )
            provider_identifier = file_receipt.provider_file_id
    return {
        "artifact_kind": artifact_kind,
        "channel": "onebot",
        "delivered": True,
        "provider_identifier": provider_identifier,
        "target_kind": target_kind.value,
    }


def _testnet_gateway() -> BinanceSpotGateway:
    provider = KeyringSecretProvider()
    credentials = load_binance_credentials(provider, BinanceEnvironment.TESTNET)
    return BinanceSpotGateway(
        environment=BinanceEnvironment.TESTNET,
        credentials=credentials,
    )


async def _binance_history_sync(
    symbol: str,
    interval: str,
    days: int,
    environment_value: str,
    database: str,
) -> dict[str, object]:
    """Collect only completed public Spot bars into the immutable archive."""

    from gribuki_trade.adapters.binance import (
        BinanceKlineArchive,
        BinanceKlineCollector,
    )

    environment = BinanceEnvironment(environment_value)
    gateway = BinanceSpotGateway(
        environment=environment,
        allow_live=environment is BinanceEnvironment.LIVE,
    )
    await gateway.synchronize_time()
    now_ms = time.time_ns() // 1_000_000 + gateway.server_time_offset_ms
    start_ms = now_ms - days * 86_400_000
    with BinanceKlineArchive(Path(database)) as archive:
        collector = BinanceKlineCollector(
            gateway,
            archive,
            environment=environment,
            clock_ms=lambda: now_ms,
        )
        result = await collector.sync(
            symbol,
            interval,
            start_time_ms=start_ms,
            end_time_ms=now_ms,
        )
    return {
        "database": str(Path(database)),
        "environment": result.dataset.environment.value,
        "first_open_time_ms": result.dataset.first_open_time_ms,
        "gaps": len(result.integrity.gaps),
        "inserted_rows": result.inserted_rows,
        "interval": result.dataset.interval,
        "last_close_time_ms": result.dataset.last_close_time_ms,
        "received_rows": result.received_rows,
        "requested_pages": result.requested_pages,
        "row_count": result.dataset.row_count,
        "sha256": result.dataset.sha256,
        "skipped_open_rows": result.skipped_open_rows,
        "symbol": result.dataset.symbol,
    }


def _binance_backtest(
    symbol: str,
    interval: str,
    environment_value: str,
    database: str,
    base_asset: str,
    quote_asset: str,
    initial_quote: Decimal,
    fast_window: int,
    slow_window: int,
    target_position: Decimal,
    rebalance_band: Decimal,
    maker_fee: Decimal,
    taker_fee: Decimal,
    slippage: Decimal,
) -> dict[str, object]:
    """Run one reproducible baseline replay over the immutable archive."""

    from gribuki_trade.adapters.binance import BinanceKlineArchive
    from gribuki_trade.backtest import CryptoFeeConfig
    from gribuki_trade.services.crypto_research import (
        CryptoResearchRequest,
        CryptoResearchService,
    )
    from gribuki_trade.strategy import CryptoTrendConfig

    if fast_window >= slow_window:
        raise ValueError("fast_window must be less than slow_window")
    trend = CryptoTrendConfig(
        fast_window=fast_window,
        slow_window=slow_window,
        minimum_history=slow_window,
        target_position_fraction=target_position,
        rebalance_tolerance_fraction=rebalance_band,
    )
    request = CryptoResearchRequest(
        environment=environment_value,
        symbol=symbol,
        interval=interval,
        base_asset=base_asset,
        quote_asset=quote_asset,
        initial_quote_balance=initial_quote,
        trend=trend,
        fees=CryptoFeeConfig(maker_rate=maker_fee, taker_rate=taker_fee),
        market_slippage_rate=slippage,
    )
    with BinanceKlineArchive(Path(database)) as archive:
        run = CryptoResearchService(archive).run(request)
    result: dict[str, object] = dict(run.summary.as_dict())
    result["assumptions"] = {
        "fast_window": fast_window,
        "maker_fee": format(maker_fee, "f"),
        "market_slippage": format(slippage, "f"),
        "rebalance_band": format(rebalance_band, "f"),
        "slow_window": slow_window,
        "taker_fee": format(taker_fee, "f"),
        "target_position_fraction": format(target_position, "f"),
    }
    return result


async def _binance_shadow_run(
    symbol: str,
    interval: str,
    environment_value: str,
    database: str,
    closed_bars: int,
    initial_quote: Decimal,
    fast_window: int,
    slow_window: int,
    target_position: Decimal,
    rebalance_band: Decimal,
    maximum_order_notional: Decimal,
) -> dict[str, object]:
    """Run public Binance data through the local-only PAPER shadow engine."""

    from gribuki_trade.adapters.binance import BinanceSpotGateway
    from gribuki_trade.services import BinanceShadowConfig, BinanceShadowSession
    from gribuki_trade.services.crypto_research import binance_klines_to_crypto_bars
    from gribuki_trade.strategy import CryptoTrendConfig

    if fast_window >= slow_window:
        raise ValueError("fast_window must be less than slow_window")
    environment = BinanceEnvironment(environment_value)
    history_gateway = BinanceSpotGateway(
        environment=environment,
        allow_live=environment is BinanceEnvironment.LIVE,
    )
    await history_gateway.synchronize_time()
    exchange_now_ms = time.time_ns() // 1_000_000 + history_gateway.server_time_offset_ms
    seed_rows = await history_gateway.klines(
        symbol,
        interval,
        limit=min(1_000, max(slow_window + 5, 100)),
    )
    closed_seed = tuple(row for row in seed_rows if row.close_time_ms < exchange_now_ms)
    if len(closed_seed) < slow_window:
        raise RuntimeError(
            f"Binance returned only {len(closed_seed)} closed seed bars; need {slow_window}"
        )
    for previous, current in zip(closed_seed, closed_seed[1:], strict=False):
        if previous.close_time_ms + 1 != current.open_time_ms:
            raise RuntimeError("Binance seed history contains a closed-kline gap")
    initial_history = binance_klines_to_crypto_bars(symbol, closed_seed[-slow_window:])

    config = BinanceShadowConfig(
        symbol=symbol,
        interval=interval,
        account_id=(
            f"binance-shadow-{environment.value.lower()}-"
            f"{symbol.lower()}-{interval.lower()}"
        ),
        initial_balances={"BTC": "0", "ETH": "0", "USDT": initial_quote},
        maximum_order_notional=maximum_order_notional,
    )
    trend = CryptoTrendConfig(
        fast_window=fast_window,
        slow_window=slow_window,
        minimum_history=slow_window,
        target_position_fraction=target_position,
        quantity_step=config.quantity_step,
        minimum_order_quantity=config.minimum_order_quantity,
        rebalance_tolerance_fraction=rebalance_band,
    )
    database_path = Path(database).expanduser().resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)

    def exchange_clock() -> datetime:
        return datetime.now(UTC) + timedelta(
            milliseconds=history_gateway.server_time_offset_ms
        )

    store = SQLiteOrderManagementStore(database_path)
    try:
        session = BinanceShadowSession.public_stream(
            store,
            environment=environment,
            config=config,
            trend_config=trend,
            initial_history=initial_history,
            clock=exchange_clock,
        )
        report = await session.run(
            maximum_closed_bars=None if closed_bars == 0 else closed_bars,
        )
        open_paper_order_count = len(store.open_orders(account_id=config.account_id))
        oms_fill_count = len(store.fills())
    finally:
        store.close()
    return {
        "adapter": {
            "capped_to_maximum_notional": report.adapter.capped_to_maximum_notional,
            "generated_signals": report.adapter.generated_signals,
            "observed_closed_bars": report.adapter.observed_closed_bars,
            "skipped_below_minimum": report.adapter.skipped_below_minimum,
            "skipped_open_order": report.adapter.skipped_open_order,
            "skipped_warmup_or_tolerance": report.adapter.skipped_warmup_or_tolerance,
        },
        "balances": [
            {
                "asset": value.asset,
                "free": format(value.free, "f"),
                "locked": format(value.locked, "f"),
            }
            for value in report.balances
        ],
        "clock_offset_ms": history_gateway.server_time_offset_ms,
        "database": str(database_path),
        "detected_bar_gaps": report.detected_bar_gaps,
        "environment": environment.value,
        "failure_reason": report.failure_reason,
        "final_equity_quote": _decimal_text(report.final_equity_quote),
        "ignored_duplicate_closed_bars": report.ignored_duplicate_closed_bars,
        "oms_fill_count": oms_fill_count,
        "open_paper_order_count": open_paper_order_count,
        "paper_engine": {
            "fill_count": report.paper_engine.fill_count,
            "processed_closed_bars": report.paper_engine.processed_closed_bars,
            "processed_market_events": report.paper_engine.processed_market_events,
            "rejected_signals": report.paper_engine.rejected_signals,
            "stale_market_events": report.paper_engine.stale_market_events,
            "submitted_orders": report.paper_engine.submitted_orders,
        },
        "public_market_only": True,
        "recovered_open_orders": report.recovered_open_orders,
        "remote_order_submission_enabled": False,
        "seed_bar_count": len(initial_history),
        "stale_market_events": report.stale_market_events,
        "symbol": symbol,
        "termination": report.termination.value,
        "watermark": report.watermark.label,
    }


async def _binance_futures_demo_status(
    product_value: str,
    symbol: str | None,
    validate_order_test: bool = False,
    confirm: str | None = None,
    quantity: Decimal | None = None,
    side: str = "BUY",
) -> dict[str, object]:
    """Probe Futures Demo, optionally using isolated credentials for safe checks."""

    from gribuki_trade.adapters.binance import (
        BinanceFuturesRestClient,
        BinanceProduct,
    )

    product = BinanceProduct(product_value)
    resolved_symbol = symbol or (
        "BTCUSDT" if product is BinanceProduct.USDS_FUTURES else "BTCUSD_PERP"
    )
    if validate_order_test and confirm != "FUTURES_DEMO_TEST":
        raise RuntimeError(
            "--validate-order-test requires --confirm FUTURES_DEMO_TEST"
        )
    if not validate_order_test and confirm is not None:
        raise RuntimeError("--confirm is accepted only with --validate-order-test")

    resolved_quantity = (
        quantity
        if quantity is not None
        else (
            Decimal("0.001")
            if product is BinanceProduct.USDS_FUTURES
            else Decimal("1")
        )
    )
    if (
        product is BinanceProduct.COIN_FUTURES
        and resolved_quantity != resolved_quantity.to_integral_value()
    ):
        raise ValueError("COIN-M quantity must be a whole contract count")

    provider = KeyringSecretProvider()
    names = binance_futures_demo_secret_names(product)
    try:
        api_key_configured = provider.get_secret(names.api_key) is not None
        secret_key_configured = provider.get_secret(names.secret_key) is not None
    except SecretProviderError:
        if validate_order_test:
            raise RuntimeError(
                "the system keyring is required for authenticated Futures Demo checks"
            ) from None
        credential_store_available = False
        api_key_configured = False
        secret_key_configured = False
    else:
        credential_store_available = True
    credentials = None
    if api_key_configured or secret_key_configured or validate_order_test:
        credentials = load_binance_futures_demo_credentials(provider, product)

    client = BinanceFuturesRestClient(product=product, credentials=credentials)
    await client.ping()
    server_time_ms = await client.server_time()
    clock_offset_ms = None
    account_summary: dict[str, object] | None = None
    position_risk_count: int | None = None
    if credentials is not None:
        clock_offset_ms = await client.synchronize_time()
        account = await client.account()
        positions = await client.position_risk(resolved_symbol)
        assets = account.get("assets")
        declared_positions = account.get("positions")
        account_summary = {
            "asset_count": len(assets) if isinstance(assets, list) else None,
            "can_trade": account.get("canTrade"),
            "declared_position_count": (
                len(declared_positions) if isinstance(declared_positions, list) else None
            ),
        }
        position_risk_count = len(positions)

    ticker = await client.ticker_price(resolved_symbol)
    exchange_info = await client.exchange_info()
    symbols = exchange_info.get("symbols")
    order_test_validated = False
    if validate_order_test:
        await client.validate_order(
            symbol=resolved_symbol,
            side=side,
            order_type="MARKET",
            quantity=resolved_quantity,
        )
        order_test_validated = True
    return {
        "account": account_summary,
        "authenticated": credentials is not None,
        "base_url": client.base_url,
        "clock_offset_ms": clock_offset_ms,
        "credential_pair_configured": api_key_configured and secret_key_configured,
        "credential_store_available": credential_store_available,
        "environment": client.stage.value,
        "order_test": {
            "called": order_test_validated,
            "creates_order": False,
            "order_type": "MARKET" if order_test_validated else None,
            "quantity": format(resolved_quantity, "f") if order_test_validated else None,
            "quantity_unit": (
                "base_asset"
                if product is BinanceProduct.USDS_FUTURES
                else "contracts"
            ),
            "side": side if order_test_validated else None,
        },
        "ping": "ok",
        "position_risk_count": position_risk_count,
        "product": client.product.value,
        "server_time_ms": server_time_ms,
        "symbol": ticker.symbol,
        "symbol_count": len(symbols) if isinstance(symbols, list) else None,
        "ticker_price": format(ticker.price, "f"),
    }


async def _binance_testnet_status(symbol: str) -> dict[str, object]:
    gateway = _testnet_gateway()
    await gateway.ping()
    await gateway.synchronize_time()
    account = await gateway.account()
    ticker = await gateway.ticker_price(symbol)
    nonzero_assets = sum(
        balance.free != 0 or balance.locked != 0 for balance in account.balances
    )
    return {
        "account_type": account.account_type,
        "can_trade": account.can_trade,
        "clock_offset_ms": gateway.server_time_offset_ms,
        "endpoint": gateway.base_url,
        "environment": gateway.environment.value,
        "nonzero_asset_count": nonzero_assets,
        "ping": "ok",
        "symbol": ticker.symbol,
        "ticker_price": format(ticker.price, "f"),
    }


async def _build_test_order(
    gateway: BinanceSpotGateway,
    symbol: str,
    target_notional: Decimal,
    *,
    resting: bool,
) -> OrderIntent:
    rules = await gateway.symbol_rules(symbol)
    if resting:
        book = await gateway.order_book(symbol, limit=5)
        if not book.bids:
            raise RuntimeError("Binance Testnet returned an empty bid book")
        raw_price = book.bids[0].price * Decimal("0.95")
    else:
        raw_price = (await gateway.ticker_price(symbol)).price
    price = (
        raw_price / rules.tick_size
    ).to_integral_value(rounding=ROUND_FLOOR) * rules.tick_size
    notional = max(
        target_notional,
        (rules.min_notional or Decimal("0")) * Decimal("2"),
    )
    quantity = (
        notional / price / rules.step_size
    ).to_integral_value(rounding=ROUND_CEILING) * rules.step_size
    return OrderIntent(
        client_order_id=f"gri-cli-{time.time_ns():x}-{uuid4().hex[:8]}",
        account_id=DEFAULT_TESTNET_ACCOUNT,
        strategy_id="authenticated-smoke",
        symbol=rules.symbol,
        side=Side.BUY,
        quantity=quantity,
        limit_price=price,
        created_at=datetime.now(UTC),
    )


async def _build_marketable_test_order(
    gateway: BinanceSpotGateway,
    symbol: str,
    target_notional: Decimal,
) -> OrderIntent:
    """Build a small TESTNET BUY with a bounded marketable limit price."""

    if gateway.environment is not BinanceEnvironment.TESTNET:
        raise RuntimeError("marketable smoke orders are restricted to Binance TESTNET")
    rules = await gateway.symbol_rules(symbol)
    book = await gateway.order_book(symbol, limit=5)
    if not book.asks:
        raise RuntimeError("Binance Testnet returned an empty ask book")
    best_ask = book.asks[0].price
    raw_limit_price = best_ask * Decimal("1.005")
    limit_price = (
        raw_limit_price / rules.tick_size
    ).to_integral_value(rounding=ROUND_CEILING) * rules.tick_size
    notional = max(
        target_notional,
        (rules.min_notional or Decimal("0")) * Decimal("1.10"),
    )
    quantity = (
        notional / limit_price / rules.step_size
    ).to_integral_value(rounding=ROUND_CEILING) * rules.step_size
    quantity = max(quantity, rules.min_quantity)
    rules.validate_limit_order(
        quantity=quantity,
        price=limit_price,
        side=Side.BUY.value,
        weighted_average_price=best_ask,
    )
    return OrderIntent(
        client_order_id=f"gri-fill-{time.time_ns():x}-{uuid4().hex[:8]}",
        account_id=DEFAULT_TESTNET_ACCOUNT,
        strategy_id="authenticated-fill-smoke",
        symbol=rules.symbol,
        side=Side.BUY,
        quantity=quantity,
        limit_price=limit_price,
        created_at=datetime.now(UTC),
    )


async def _binance_testnet_order_test(
    symbol: str,
    target_notional: Decimal,
) -> dict[str, object]:
    gateway = _testnet_gateway()
    await gateway.connect()
    try:
        await gateway.synchronize_time()
        order = await _build_test_order(
            gateway,
            symbol,
            target_notional,
            resting=False,
        )
        await gateway.validate_order_on_exchange(order)
        return {
            "endpoint": gateway.base_url,
            "entered_matching_engine": False,
            "environment": gateway.environment.value,
            "notional": format(order.quantity * order.limit_price, "f"),
            "order_test": "accepted",
            "symbol": order.symbol,
        }
    finally:
        await gateway.disconnect()


async def _binance_testnet_cycle(
    symbol: str,
    target_notional: Decimal,
) -> dict[str, object]:
    gateway = _testnet_gateway()
    await gateway.connect()
    user_stream: BinanceSpotUserDataStream | None = None
    consumer: asyncio.Task[None] | None = None
    submission_started = False
    order: OrderIntent | None = None
    try:
        await gateway.synchronize_time()
        account = await gateway.account()
        if not account.can_trade:
            raise RuntimeError("Binance Spot Testnet account cannot trade")
        order = await _build_test_order(
            gateway,
            symbol,
            target_notional,
            resting=True,
        )
        credentials = load_binance_credentials(
            KeyringSecretProvider(),
            BinanceEnvironment.TESTNET,
        )
        user_stream = BinanceSpotUserDataStream(
            credentials,
            clock_ms=lambda: (
                time.time_ns() // 1_000_000 + gateway.server_time_offset_ms
            ),
        )
        reports: asyncio.Queue[BinanceExecutionReport] = asyncio.Queue()

        async def consume_reports() -> None:
            assert order is not None and user_stream is not None
            async for event in user_stream:
                if isinstance(event, BinanceExecutionReport) and (
                    event.client_order_id == order.client_order_id
                    or event.original_client_order_id == order.client_order_id
                ):
                    await reports.put(event)

        consumer = asyncio.create_task(consume_reports())
        async with asyncio.timeout(15):
            while user_stream.subscription_id is None:
                await asyncio.sleep(0.05)
        await gateway.submit_order(order)
        submission_started = True
        submitted_update = gateway.order_update(order.client_order_id)
        if submitted_update is None:
            raise RuntimeError("Binance Testnet submission produced no local order state")
        accepted_report = await asyncio.wait_for(reports.get(), timeout=15)
        snapshot = await gateway.query_order(order.client_order_id)
        final_report: BinanceExecutionReport | None = None
        if snapshot.status in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED}:
            await gateway.cancel_order(order.client_order_id)
            final_report = await asyncio.wait_for(reports.get(), timeout=15)
        final = gateway.order_update(order.client_order_id)
        return {
            "client_order_id": order.client_order_id,
            "endpoint": gateway.base_url,
            "environment": gateway.environment.value,
            "final_status": (final.status if final else snapshot.status).value,
            "notional": format(order.quantity * order.limit_price, "f"),
            "queried_status": snapshot.status.value,
            "submitted_status": submitted_update.status.value,
            "symbol": order.symbol,
            "user_stream_final_execution": (
                final_report.execution_type if final_report is not None else None
            ),
            "user_stream_initial_execution": accepted_report.execution_type,
            "user_stream_subscription": "active",
        }
    finally:
        if submission_started and order is not None:
            with suppress(Exception):
                snapshot = await gateway.query_order(order.client_order_id)
                if snapshot.status in {
                    OrderStatus.ACCEPTED,
                    OrderStatus.PARTIALLY_FILLED,
                }:
                    await gateway.cancel_order(order.client_order_id)
        if user_stream is not None:
            await user_stream.aclose()
        if consumer is not None:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
        await gateway.disconnect()


async def _binance_testnet_oms_cycle(
    symbol: str,
    target_notional: Decimal,
    database: str,
) -> dict[str, object]:
    """Run one durable Spot Testnet submit/cancel/reconciliation cycle.

    The function deliberately has no environment parameter.  The REST gateway,
    private stream, and execution service are all constructed for TESTNET, while
    the durable outbox is opened before any order is sent to Binance.
    """

    database_path = Path(database).expanduser().resolve()
    gateway = _testnet_gateway()
    if gateway.environment is not BinanceEnvironment.TESTNET:
        raise RuntimeError("Testnet fill command cannot target Binance LIVE")
    database_path.parent.mkdir(parents=True, exist_ok=True)
    store = SQLiteOrderManagementStore(database_path)
    user_stream: BinanceSpotUserDataStream | None = None
    service: BinanceSpotTestnetExecutionService | None = None
    consumer: asyncio.Task[None] | None = None
    order: OrderIntent | None = None
    reports: asyncio.Queue[BinanceExecutionReport] = asyncio.Queue()
    try:
        await gateway.synchronize_time()
        account = await gateway.account()
        if not account.can_trade:
            raise RuntimeError("Binance Spot Testnet account cannot trade")
        order = await _build_test_order(
            gateway,
            symbol,
            target_notional,
            resting=True,
        )
        credentials = load_binance_credentials(
            KeyringSecretProvider(),
            BinanceEnvironment.TESTNET,
        )
        user_stream = BinanceSpotUserDataStream(
            credentials,
            clock_ms=lambda: (
                time.time_ns() // 1_000_000 + gateway.server_time_offset_ms
            ),
        )
        service = BinanceSpotTestnetExecutionService(
            gateway,
            store,
            account_id=DEFAULT_TESTNET_ACCOUNT,
            symbols=(order.symbol,),
            user_stream=user_stream,
        )
        startup = await service.start()

        async def consume_private_events() -> None:
            assert order is not None and user_stream is not None and service is not None
            async for event in user_stream.events():
                await service.consume_user_event(event)
                if isinstance(event, BinanceExecutionReport) and (
                    event.client_order_id == order.client_order_id
                    or event.original_client_order_id == order.client_order_id
                ):
                    await reports.put(event)

        consumer = asyncio.create_task(consume_private_events())
        async with asyncio.timeout(15):
            while user_stream.subscription_id is None:
                if consumer.done():
                    await consumer
                await asyncio.sleep(0.05)

        submitted = await service.submit(order)
        new_report = await _wait_for_testnet_execution(
            reports,
            execution_type="NEW",
            timeout_seconds=15,
        )
        after_new = store.require_order(order.client_order_id)
        if after_new.status not in {
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
        }:
            raise RuntimeError(
                "resting Testnet order was not active after its NEW execution report"
            )
        canceled = await service.cancel(order.client_order_id)
        canceled_report = await _wait_for_testnet_execution(
            reports,
            execution_type="CANCELED",
            timeout_seconds=15,
        )
        reconciliation, reconciliation_attempts = (
            await _retry_testnet_reconciliation(service)
        )
        final = store.require_order(order.client_order_id)
        commands = tuple(
            command
            for command in store.commands()
            if command.client_order_id == order.client_order_id
        )
        return {
            "client_order_id": order.client_order_id,
            "database": str(database_path),
            "endpoint": gateway.base_url,
            "environment": gateway.environment.value,
            "notional": format(order.quantity * order.limit_price, "f"),
            "oms": {
                "command_statuses": [
                    {
                        "attempt_count": command.attempt_count,
                        "command_id": command.command_id,
                        "error_code": command.last_error_code,
                        "status": command.status.value,
                        "type": command.command_type.value,
                    }
                    for command in commands
                ],
                "fill_count": len(store.fills(client_order_id=order.client_order_id)),
                "order_statuses": {
                    "after_new": after_new.status.value,
                    "after_reconciliation": final.status.value,
                    "after_submit": submitted.status.value,
                    "after_cancel": canceled.status.value,
                },
            },
            "reconciliation": {
                **_testnet_reconciliation_payload(reconciliation),
                "attempts": reconciliation_attempts,
            },
            "startup_reconciliation": _testnet_reconciliation_payload(startup),
            "symbol": order.symbol,
            "user_stream": {
                "cancel_execution": canceled_report.execution_type,
                "new_execution": new_report.execution_type,
                "subscription": "active",
            },
        }
    finally:
        if order is not None and service is not None and service.started:
            current = store.order(order.client_order_id)
            if current is not None and current.status in {
                OrderStatus.ACCEPTED,
                OrderStatus.PARTIALLY_FILLED,
            }:
                with suppress(Exception):
                    await service.cancel(order.client_order_id)
                    await service.reconcile_startup()
        if service is not None and service.started:
            with suppress(Exception):
                await service.stop()
        elif user_stream is not None:
            with suppress(Exception):
                await user_stream.aclose()
        if consumer is not None:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
        store.close()


def _assert_testnet_oms_database_idle(store: SQLiteOrderManagementStore) -> None:
    """Fail closed instead of dispatching work left by another CLI process."""

    open_ids = {
        snapshot.order.client_order_id
        for snapshot in store.open_orders(account_id=DEFAULT_TESTNET_ACCOUNT)
    }
    blocking_commands = {
        command.client_order_id
        for command in store.commands()
        if command.status.value in {"PENDING", "IN_FLIGHT", "UNKNOWN"}
    }
    conflicts = sorted(open_ids | blocking_commands)
    if conflicts:
        raise RuntimeError(
            "Testnet OMS database contains active or unresolved work: "
            + ", ".join(conflicts)
        )


async def _wait_for_testnet_fill_reports(
    reports: asyncio.Queue[BinanceExecutionReport],
    consumer: asyncio.Task[None],
    *,
    timeout_seconds: float,
) -> tuple[BinanceExecutionReport, ...]:
    """Accept either NEW->TRADE or a direct terminal TRADE execution sequence."""

    received: list[BinanceExecutionReport] = []
    async with asyncio.timeout(timeout_seconds):
        while True:
            if consumer.done():
                await consumer
                raise RuntimeError("Binance Testnet user-data stream ended before the fill")
            try:
                report = await asyncio.wait_for(reports.get(), timeout=0.25)
            except TimeoutError:
                continue
            received.append(report)
            execution_type = report.execution_type.upper()
            if execution_type == "TRADE" and report.status is OrderStatus.FILLED:
                return tuple(received)
            if report.status in {
                OrderStatus.CANCELED,
                OrderStatus.BROKER_REJECTED,
                OrderStatus.EXPIRED,
            }:
                raise RuntimeError(
                    "Binance Testnet order became terminal before filling: "
                    f"{report.status.value}"
                )


def _testnet_balance_diff(
    before: BinanceAccount,
    after: BinanceAccount,
) -> list[dict[str, str]]:
    before_values = {item.asset: (item.free, item.locked) for item in before.balances}
    after_values = {item.asset: (item.free, item.locked) for item in after.balances}
    changed: list[dict[str, str]] = []
    for asset in sorted(before_values.keys() | after_values.keys()):
        before_free, before_locked = before_values.get(
            asset, (Decimal("0"), Decimal("0"))
        )
        after_free, after_locked = after_values.get(
            asset, (Decimal("0"), Decimal("0"))
        )
        if (before_free, before_locked) == (after_free, after_locked):
            continue
        changed.append(
            {
                "asset": asset,
                "before_free": format(before_free, "f"),
                "before_locked": format(before_locked, "f"),
                "after_free": format(after_free, "f"),
                "after_locked": format(after_locked, "f"),
                "delta_free": format(after_free - before_free, "f"),
                "delta_locked": format(after_locked - before_locked, "f"),
                "delta_total": format(
                    after_free + after_locked - before_free - before_locked,
                    "f",
                ),
            }
        )
    return changed


async def _binance_testnet_oms_fill(
    symbol: str,
    target_notional: Decimal,
    database: str,
) -> dict[str, object]:
    """Execute one real virtual fill through a Testnet-only durable OMS path."""

    gateway = _testnet_gateway()
    if gateway.environment is not BinanceEnvironment.TESTNET:
        raise RuntimeError("binance-testnet-oms-fill cannot target Binance LIVE")
    database_path = Path(database).expanduser().resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    store = SQLiteOrderManagementStore(database_path)
    user_stream: BinanceSpotUserDataStream | None = None
    service: BinanceSpotTestnetExecutionService | None = None
    consumer: asyncio.Task[None] | None = None
    order: OrderIntent | None = None
    reports: asyncio.Queue[BinanceExecutionReport] = asyncio.Queue()
    try:
        _assert_testnet_oms_database_idle(store)
        await gateway.synchronize_time()
        account = await gateway.account()
        if not account.can_trade:
            raise RuntimeError("Binance Spot Testnet account cannot trade")
        normalized_symbol = symbol.strip().upper()
        credentials = load_binance_credentials(
            KeyringSecretProvider(),
            BinanceEnvironment.TESTNET,
        )
        user_stream = BinanceSpotUserDataStream(
            credentials,
            clock_ms=lambda: (
                time.time_ns() // 1_000_000 + gateway.server_time_offset_ms
            ),
        )
        service = BinanceSpotTestnetExecutionService(
            gateway,
            store,
            account_id=DEFAULT_TESTNET_ACCOUNT,
            symbols=(normalized_symbol,),
            user_stream=user_stream,
        )
        startup = await service.start()
        if startup.unresolved_order_ids:
            raise RuntimeError(
                "Testnet OMS startup reconciliation left unresolved orders: "
                + ", ".join(startup.unresolved_order_ids)
            )
        before_account = await gateway.account()
        order = await _build_marketable_test_order(
            gateway,
            normalized_symbol,
            target_notional,
        )

        async def consume_private_events() -> None:
            assert order is not None and user_stream is not None and service is not None
            async for event in user_stream.events():
                await service.consume_user_event(event)
                if isinstance(event, BinanceExecutionReport) and (
                    event.client_order_id == order.client_order_id
                    or event.original_client_order_id == order.client_order_id
                ):
                    await reports.put(event)

        consumer = asyncio.create_task(consume_private_events())
        async with asyncio.timeout(15):
            while user_stream.subscription_id is None:
                if consumer.done():
                    await consumer
                await asyncio.sleep(0.05)

        submitted = await service.submit(order)
        stream_reports = await _wait_for_testnet_fill_reports(
            reports,
            consumer,
            timeout_seconds=30,
        )
        reconciliation, reconciliation_attempts = (
            await _retry_testnet_reconciliation(service)
        )
        final = store.require_order(order.client_order_id)
        if final.status is not OrderStatus.FILLED:
            raise RuntimeError(
                "Binance Testnet order did not reconcile to FILLED: "
                f"{final.status.value}"
            )
        fills = store.fills(client_order_id=order.client_order_id)
        if not fills:
            raise RuntimeError("Binance Testnet reported FILLED without a persisted fill")
        after_account = await gateway.account()
        commands = tuple(
            command
            for command in store.commands()
            if command.client_order_id == order.client_order_id
        )
        fee_totals: dict[str, Decimal] = {}
        for fill in fills:
            if fill.fee_asset is not None:
                fee_totals[fill.fee_asset] = (
                    fee_totals.get(fill.fee_asset, Decimal("0")) + fill.fee_amount
                )
        return {
            "client_order_id": order.client_order_id,
            "database": str(database_path),
            "endpoint": gateway.base_url,
            "environment": gateway.environment.value,
            "final_status": final.status.value,
            "limit_notional": format(order.quantity * order.limit_price, "f"),
            "symbol": order.symbol,
            "balance_changes": _testnet_balance_diff(before_account, after_account),
            "fills": [
                {
                    "fee_amount": format(fill.fee_amount, "f"),
                    "fee_asset": fill.fee_asset,
                    "fill_id": fill.fill_id,
                    "price": format(fill.price, "f"),
                    "quantity": format(fill.quantity, "f"),
                    "quote_quantity": format(fill.price * fill.quantity, "f"),
                }
                for fill in fills
            ],
            "fee_assets": {
                asset: format(amount, "f") for asset, amount in sorted(fee_totals.items())
            },
            "oms": {
                "command_statuses": [
                    {
                        "attempt_count": command.attempt_count,
                        "command_id": command.command_id,
                        "error_code": command.last_error_code,
                        "status": command.status.value,
                        "type": command.command_type.value,
                    }
                    for command in commands
                ],
                "fill_count": len(fills),
                "submitted_status": submitted.status.value,
                "unresolved_order_ids": list(reconciliation.unresolved_order_ids),
            },
            "reconciliation": {
                **_testnet_reconciliation_payload(reconciliation),
                "attempts": reconciliation_attempts,
            },
            "startup_reconciliation": _testnet_reconciliation_payload(startup),
            "user_stream": {
                "executions": [report.execution_type for report in stream_reports],
                "subscription": "active",
            },
        }
    finally:
        if order is not None and service is not None and service.started:
            current = store.order(order.client_order_id)
            if current is not None and current.status is OrderStatus.UNKNOWN:
                with suppress(Exception):
                    await _retry_testnet_reconciliation(service)
                current = store.order(order.client_order_id)
            if current is not None and current.status in {
                OrderStatus.ACCEPTED,
                OrderStatus.PARTIALLY_FILLED,
            }:
                with suppress(Exception):
                    await service.cancel(order.client_order_id)
                    await _retry_testnet_reconciliation(service)
        if service is not None and service.started:
            with suppress(Exception):
                await service.stop()
        elif user_stream is not None:
            with suppress(Exception):
                await user_stream.aclose()
        if consumer is not None:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
        store.close()


async def _wait_for_testnet_execution(
    reports: asyncio.Queue[BinanceExecutionReport],
    *,
    execution_type: str,
    timeout_seconds: float,
) -> BinanceExecutionReport:
    """Wait for one matching private execution without accepting stale reports."""

    async with asyncio.timeout(timeout_seconds):
        while True:
            report = await reports.get()
            if report.execution_type == execution_type:
                return report


def _testnet_reconciliation_payload(
    report: BinanceStartupReconciliation,
) -> dict[str, object]:
    return {
        "dispatched_pending_commands": report.dispatched_pending_commands,
        "exchange_history_orders": report.exchange_history_orders,
        "exchange_open_orders": report.exchange_open_orders,
        "exchange_trades": report.exchange_trades,
        "reconciled_orders": report.reconciled_orders,
        "recorded_balances": report.recorded_balances,
        "recorded_fills": report.recorded_fills,
        "recovered_commands": report.recovered_commands,
        "unresolved_order_ids": list(report.unresolved_order_ids),
    }


async def _retry_testnet_reconciliation(
    service: BinanceSpotTestnetExecutionService,
    *,
    attempts: int = 3,
) -> tuple[BinanceStartupReconciliation, int]:
    """Retry read-only final reconciliation after transient HTTP failures."""

    if attempts <= 0:
        raise ValueError("attempts must be positive")
    for attempt in range(1, attempts + 1):
        try:
            return await service.reconcile_startup(), attempt
        except BinanceTransportError:
            if attempt == attempts:
                raise
            await asyncio.sleep(0.25 * (2 ** (attempt - 1)))
    raise AssertionError("unreachable reconciliation retry state")
