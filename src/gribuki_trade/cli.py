"""用于本地集成检查的运维命令行入口。"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import tempfile
import time
from collections.abc import Awaitable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from datetime import UTC, date, datetime, timedelta
from datetime import time as datetime_time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast
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
    BinanceAPIError,
    BinanceEnvironment,
    BinanceExecutionReport,
    BinanceProtocolError,
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
from gribuki_trade.cli_parsing import (
    _add_order_arguments,
    _iso_date,
    _iso_datetime,
    _macro_weight_decimal,
    _non_negative_decimal,
    _non_negative_float,
    _non_negative_integer,
    _positive_decimal,
    _positive_float,
    _positive_integer,
    _positive_integer_or_unlimited,
    _unit_fraction_decimal,
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
from gribuki_trade.runtime import (
    LIVE_CONFIRMATION_PHRASE,
    BrokerOperation,
    LiveTradingGuard,
    TradingMode,
)
from gribuki_trade.runtime.integration_settings import (
    IntegrationSettingsError,
    load_integration_settings,
)
from gribuki_trade.runtime.integration_settings import (
    validate_model_id as validate_runtime_model_id,
)
from gribuki_trade.security import KeyringSecretProvider, SecretProviderError
from gribuki_trade.security.post_close_urls import (
    PostCloseSearxngURLValidationError,
    validate_post_close_searxng_url,
)
from gribuki_trade.services.binance_execution import (
    BinanceSpotExecutionService,
    BinanceSpotTestnetExecutionService,
    BinanceStartupReconciliation,
)
from gribuki_trade.sqlite_runtime import sqlite_runtime_status
from gribuki_trade.trading import SQLiteOrderManagementStore, SQLiteSpotOrderListStore

if TYPE_CHECKING:
    from gribuki_trade.domain.paper_trading import PaperPosition
    from gribuki_trade.domain.post_close import PostCloseInstrumentResearch
    from gribuki_trade.ports.ashare_screening import AsyncAShareScreeningData
    from gribuki_trade.ports.ashare_surveillance import AsyncAShareIntradayUniverseData
    from gribuki_trade.ports.market_data import AsyncTradingCalendar
    from gribuki_trade.reporting.paper_day_summary import PaperDayExecutiveProjection
    from gribuki_trade.services.ashare_close_sessions import CloseSessionResolution
    from gribuki_trade.services.ashare_intraday_llm import IntradayLLMConfig
    from gribuki_trade.services.ashare_paper import ASharePaperTradingService
    from gribuki_trade.services.ashare_paper_day import (
        PaperDayDeepExitAssessmentProvider,
        PaperDayIntradayLLMPlanFactory,
    )
    from gribuki_trade.services.ashare_post_close import PostCloseOrchestrationResult
    from gribuki_trade.services.macro_research import MacroResearchService
    from gribuki_trade.storage.candidate_store import SQLiteCandidateStore
    from gribuki_trade.storage.research_runs import StoredResearchRun
    from gribuki_trade.strategy_lab.discovery import FactorCandidateInventory

DEFAULT_TESTNET_ACCOUNT = "binance-testnet"
DEFAULT_LIVE_ACCOUNT = "binance-live"
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
    from gribuki_trade.reporting.contracts import ReportKind

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

    status = commands.add_parser(
        "binance-testnet-status",
        help="perform authenticated read-only Spot Testnet checks",
    )
    status.add_argument("--symbol", default="BTCUSDT")

    live_status = commands.add_parser(
        "binance-live-status",
        help="perform guarded authenticated read-only Spot LIVE checks",
    )
    live_status.add_argument("--symbol", default="BTCUSDT")
    live_status.add_argument(
        "--confirm",
        required=True,
        choices=(LIVE_CONFIRMATION_PHRASE,),
        help="must be exactly ENABLE LIVE TRADING; no order is submitted",
    )

    for balance_command, description in (
        ("binance-live-balance", "查询现货 LIVE 账户的可用和冻结余额"),
        ("binance-live-futures-balance", "查询 USD-M Futures LIVE 账户余额和保证金"),
    ):
        live_balance = commands.add_parser(balance_command, help=description)
        live_balance.add_argument(
            "--asset", action="append", default=[], help="仅显示指定资产，可重复；包含零余额"
        )
        live_balance.add_argument(
            "--include-zero", action="store_true", help="同时显示零余额资产"
        )
        live_balance.add_argument(
            "--confirm", required=True, choices=(LIVE_CONFIRMATION_PHRASE,),
            help="进程内 LIVE 确认；此命令只查询余额，不提交订单",
        )

    live_order_test = commands.add_parser(
        "binance-live-order-test",
        help="validate a Spot LIVE order through /order/test without creating an order",
    )
    _add_order_arguments(live_order_test)
    live_order_test.add_argument(
        "--confirm",
        required=True,
        choices=(LIVE_CONFIRMATION_PHRASE,),
        help="must be exactly ENABLE LIVE TRADING; /order/test never enters the matching engine",
    )

    live_order = commands.add_parser(
        "binance-live-order",
        help="submit or cancel one guarded Spot LIVE order (explicit confirmation required)",
    )
    live_order.add_argument("action", choices=("submit", "cancel"))
    live_order.add_argument("--symbol", default="BTCUSDT")
    live_order.add_argument("--notional", type=_positive_decimal, default=Decimal("20"))
    live_order.add_argument("--client-order-id")
    live_order.add_argument(
        "--database",
        default="runtime/binance/live-oms.sqlite3",
        help="durable SQLite OMS database",
    )
    live_order.add_argument(
        "--confirm",
        required=True,
        choices=(LIVE_CONFIRMATION_PHRASE,),
        help="must be exactly ENABLE LIVE TRADING",
    )

    futures_live_status = commands.add_parser(
        "binance-live-futures-status",
        help="perform guarded authenticated read-only USD-M Futures LIVE checks",
    )
    futures_live_status.add_argument("--symbol", default="BTCUSDT")
    futures_live_status.add_argument(
        "--confirm",
        required=True,
        choices=(LIVE_CONFIRMATION_PHRASE,),
        help="must be exactly ENABLE LIVE TRADING; no order is submitted",
    )

    futures_live_test = commands.add_parser(
        "binance-live-futures-order-test",
        help="validate a USD-M Futures LIVE order through /order/test",
    )
    futures_live_test.add_argument("--symbol", default="BTCUSDT")
    futures_live_test.add_argument("--side", choices=("BUY", "SELL"), default="BUY")
    futures_live_test.add_argument(
        "--position-side",
        choices=("BOTH", "LONG", "SHORT"),
        help="持仓方向；单向持仓使用 BOTH，双向持仓必须明确 LONG 或 SHORT",
    )
    futures_live_test.add_argument("--quantity", type=_positive_decimal, default=Decimal("0.001"))
    futures_live_test.add_argument("--order-type", choices=("MARKET", "LIMIT"), default="MARKET")
    futures_live_test.add_argument("--price", type=_positive_decimal)
    futures_live_test.add_argument(
        "--confirm",
        required=True,
        choices=(LIVE_CONFIRMATION_PHRASE,),
        help="must be exactly ENABLE LIVE TRADING; no order is submitted",
    )

    futures_live_order = commands.add_parser(
        "binance-live-futures-order",
        help="submit or cancel one guarded USD-M Futures LIVE order",
    )
    futures_live_order.add_argument("action", choices=("submit", "cancel"))
    futures_live_order.add_argument("--symbol", default="BTCUSDT")
    futures_live_order.add_argument("--side", choices=("BUY", "SELL"), default="BUY")
    futures_live_order.add_argument(
        "--position-side",
        choices=("BOTH", "LONG", "SHORT"),
        help="持仓方向；单向持仓使用 BOTH，双向持仓必须明确 LONG 或 SHORT",
    )
    futures_live_order.add_argument("--quantity", type=_positive_decimal, default=Decimal("0.001"))
    futures_live_order.add_argument("--order-type", choices=("MARKET", "LIMIT"), default="MARKET")
    futures_live_order.add_argument("--price", type=_positive_decimal)
    futures_live_order.add_argument("--order-id")
    futures_live_order.add_argument("--client-order-id")
    futures_live_order.add_argument(
        "--confirm",
        required=True,
        choices=(LIVE_CONFIRMATION_PHRASE,),
        help="must be exactly ENABLE LIVE TRADING",
    )

    futures_live_stream = commands.add_parser(
        "binance-live-futures-stream",
        help="run the guarded USD-M Futures private stream and durable recovery loop",
    )
    futures_live_stream.add_argument(
        "--symbol", action="append", default=["BTCUSDT"],
        help="symbol to reconcile; repeat for multiple symbols",
    )
    futures_live_stream.add_argument(
        "--database", default="runtime/binance/live-futures-oms.sqlite3",
        help="durable Futures OMS database",
    )
    futures_live_stream.add_argument(
        "--max-events", type=int,
        help="stop after this many private events; omit for continuous operation",
    )
    futures_live_stream.add_argument(
        "--confirm", required=True, choices=(LIVE_CONFIRMATION_PHRASE,),
        help="must be exactly ENABLE LIVE TRADING; this command does not submit orders",
    )

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

    exit_evaluate = commands.add_parser(
        "strategy-exit-evaluate",
        help="对冻结 A 股退出样本运行仅研究的 walk-forward/holdout 实验",
    )
    exit_evaluate.add_argument("--dataset", required=True, help="冻结样本 JSON 文件")
    exit_evaluate.add_argument(
        "--specification",
        required=True,
        help="预注册搜索空间、成本和 walk-forward 规格 JSON 文件",
    )
    exit_evaluate.add_argument(
        "--output",
        required=True,
        help="完整不可变 trial registry 的原子输出路径",
    )
    exit_evaluate.add_argument(
        "--created-at",
        type=_iso_datetime,
        help="可选的可重放实验时点；必须含时区",
    )
    exit_evaluate.add_argument(
        "--overwrite",
        action="store_true",
        help="显式允许替换同一路径的研究产物；绝不修改生产策略",
    )
    exit_evaluate.add_argument(
        "--confirm",
        choices=("RESEARCH_ONLY",),
        help="写入试验登记前必须精确确认 RESEARCH_ONLY",
    )

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

    live_sync = commands.add_parser(
        "live-sync",
        help="接收实盘成交、检查账本，或执行一次应用级保护分析与行情跟踪",
    )
    live_sync.add_argument("action", choices=("ingest", "status", "cycle"))
    live_sync.add_argument(
        "--ledger-db",
        default="runtime/live/observed-live.sqlite3",
        help="与 PAPER 完全隔离的实盘观察 SQLite 账本",
    )
    live_sync.add_argument(
        "--allowed-sender",
        action="append",
        help="ingest 必填；可重复指定允许同步成交的 QQ 号",
    )
    live_sync.add_argument(
        "--event-json",
        help="ingest 必填；OneBot v11 私聊事件 JSON 文件，使用 - 从标准输入读取",
    )
    live_sync.add_argument("--account", help="status 必填；要检查的实盘观察账户")
    live_sync.add_argument(
        "--received-at",
        type=_iso_datetime,
        help="可选接收时点；仅用于可重放导入，必须含时区",
    )
    live_sync.add_argument(
        "--exit-plan-db",
        default="runtime/live/exit-plans.sqlite3",
        help="cycle 使用的独立退出计划哈希账本",
    )
    live_sync.add_argument(
        "--outbox-path",
        default="runtime/live/outbox.sqlite3",
        help="cycle 使用的单目标 NapCat 出站队列",
    )
    live_sync.add_argument("--target-kind", choices=("private", "group"))
    live_sync.add_argument("--target-id")
    live_sync.add_argument(
        "--base-url",
        default=None,
        help="OneBot 地址；cycle 省略时读取 GUI 共享配置",
    )
    live_sync.add_argument(
        "--llm-provider",
        choices=("deepseek", "openai"),
        default=None,
        help="cycle 双轨分析 provider；省略时读取 GUI 共享配置",
    )
    live_sync.add_argument(
        "--llm-model",
        default=None,
        help="cycle 双轨分析模型；省略时读取 GUI 共享配置",
    )
    live_sync.add_argument("--work-limit", type=_positive_integer, default=20)
    live_sync.add_argument(
        "--quick-timeout-seconds",
        type=_positive_float,
        default=45.0,
        help="ingest 确认 BUY 后建立 QUICK 及单次跟踪各自允许的有限秒数",
    )
    live_sync.add_argument(
        "--deep-timeout-seconds",
        type=_positive_float,
        default=600.0,
        help="单轮等待 DEEP 的有限秒数；超时后持久任务自动释放以便下轮重试",
    )
    live_sync.add_argument(
        "--tracking-pump-interval",
        type=_positive_float,
        default=30.0,
        help="等待 DEEP 时继续观察完整 K 线的间隔秒数",
    )
    live_sync.add_argument(
        "--tracking-pump-limit",
        type=_positive_integer,
        default=20,
        help="等待 DEEP 时最多追加的持仓观察轮数",
    )
    live_sync.add_argument(
        "--dispatch-cycles",
        type=_positive_integer,
        default=3,
        help="cycle 最多执行的 NapCat outbox 派发轮数",
    )
    live_sync.add_argument(
        "--dispatch-poll-interval",
        type=_non_negative_float,
        default=0.5,
    )
    live_sync.add_argument(
        "--confirm",
        choices=("LIVE_SYNC_CYCLE",),
        help="cycle 必须显式确认；该动作只分析、跟踪和提醒，绝不下单",
    )

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
    close_batch.add_argument("--research-db", default="runtime/research/research.sqlite3")
    close_batch.add_argument("--market-evidence-dir", default="runtime/research/market_evidence")
    close_batch.add_argument("--outbox-db", default="runtime/notifications/outbox.sqlite3")
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
    return parser


def _apply_integration_runtime_defaults(args: argparse.Namespace) -> None:
    """把 GUI 共享配置应用到未被命令行显式覆盖的集成参数。"""

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
    settings = load_integration_settings()
    if (command in base_url_commands or paper_day_run or post_close_run or live_onebot) and getattr(
        args, "base_url", None
    ) is None:
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


def main(argv: Sequence[str] | None = None) -> int:
    _configure_terminal_encoding()
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "gui"
    try:
        _apply_integration_runtime_defaults(args)
    except IntegrationSettingsError:
        print(
            json.dumps(
                {
                    "error_code": "INTEGRATION_SETTINGS_INVALID",
                    "ok": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
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
    elif command == "temp-root":
        result = _temp_root(args.action, args.temp_dir)
    elif command == "binance-testnet-status":
        result = asyncio.run(_binance_testnet_status(args.symbol))
    elif command == "binance-live-status":
        result = asyncio.run(_binance_live_status(args.symbol, args.confirm))
    elif command == "binance-live-balance":
        result = asyncio.run(_binance_live_balance(args.asset, args.include_zero, args.confirm))
    elif command == "binance-live-futures-balance":
        result = asyncio.run(
            _binance_live_futures_balance(args.asset, args.include_zero, args.confirm)
        )
    elif command == "binance-live-order-test":
        result = asyncio.run(_binance_live_order_test(args.symbol, args.notional, args.confirm))
    elif command == "binance-live-order":
        result = asyncio.run(
            _binance_live_order(
                args.action,
                args.symbol,
                args.notional,
                args.client_order_id,
                args.database,
                args.confirm,
            )
        )
    elif command == "binance-live-futures-status":
        result = asyncio.run(_binance_live_futures_status(args.symbol, args.confirm))
    elif command == "binance-live-futures-order-test":
        result = asyncio.run(
            _binance_live_futures_order_test(
                args.symbol,
                args.side,
                args.position_side,
                args.quantity,
                args.order_type,
                args.price,
                args.confirm,
            )
        )
    elif command == "binance-live-futures-order":
        result = asyncio.run(
            _binance_live_futures_order(
                args.action,
                args.symbol,
                args.side,
                args.position_side,
                args.quantity,
                args.order_type,
                args.price,
                args.order_id,
                args.client_order_id,
                args.confirm,
            )
        )
    elif command == "binance-live-futures-stream":
        result = asyncio.run(
            _binance_live_futures_stream(
                args.symbol, args.database, args.max_events, args.confirm
            )
        )
    elif command == "binance-testnet-order-test":
        result = asyncio.run(_binance_testnet_order_test(args.symbol, args.notional))
    elif command == "binance-testnet-cycle":
        result = asyncio.run(_binance_testnet_cycle(args.symbol, args.notional))
    elif command == "binance-testnet-oms-cycle":
        result = asyncio.run(_binance_testnet_oms_cycle(args.symbol, args.notional, args.database))
    elif command == "binance-testnet-oms-fill":
        result = asyncio.run(_binance_testnet_oms_fill(args.symbol, args.notional, args.database))
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
        result = asyncio.run(_ashare_news(args.feed, args.symbol, args.limit, args.archive_dir))
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
    elif command == "strategy-exit-evaluate":
        result = _strategy_exit_evaluate(
            dataset=args.dataset,
            specification=args.specification,
            output=args.output,
            created_at=args.created_at,
            overwrite=args.overwrite,
            confirmation=args.confirm,
        )
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
    elif command == "live-sync":
        if args.action == "cycle":
            result = asyncio.run(
                _live_sync_cycle(
                    ledger_db=args.ledger_db,
                    exit_plan_db=args.exit_plan_db,
                    outbox_path=args.outbox_path,
                    target_kind_value=args.target_kind,
                    target_id=args.target_id,
                    base_url=args.base_url,
                    llm_provider=args.llm_provider,
                    llm_model=args.llm_model,
                    work_limit=args.work_limit,
                    deep_timeout_seconds=args.deep_timeout_seconds,
                    tracking_pump_interval=args.tracking_pump_interval,
                    tracking_pump_limit=args.tracking_pump_limit,
                    dispatch_cycles=args.dispatch_cycles,
                    dispatch_poll_interval=args.dispatch_poll_interval,
                    confirmation=args.confirm,
                )
            )
        else:
            result = _live_sync(
                args.action,
                args.ledger_db,
                args.allowed_sender,
                args.event_json,
                args.account,
                args.received_at,
                exit_plan_db=args.exit_plan_db,
                outbox_path=args.outbox_path,
                target_kind_value=args.target_kind,
                target_id=args.target_id,
                quick_timeout_seconds=args.quick_timeout_seconds,
                base_url=args.base_url,
                dispatch_cycles=args.dispatch_cycles,
                dispatch_poll_interval=args.dispatch_poll_interval,
            )
    elif command == "ashare-paper-day":
        result = asyncio.run(
            _ashare_paper_day(
                args.action,
                args.runtime_dir,
                args.session_date,
                args.account,
                args.initial_cash,
                args.target_kind,
                args.target_id,
                args.base_url,
                args.confirm,
                args.recover_after_abort,
                args.maximum_positions,
                args.confirm_risk_policy_change,
                args.intraday_llm,
                args.intraday_llm_review_top_n,
                args.intraday_llm_review_ttl_minutes,
                args.intraday_llm_max_calls,
                args.intraday_llm_events_db,
                args.intraday_llm_provider,
                args.intraday_llm_model,
                args.report_artifact_recovery_action,
                args.confirm_report_artifact_recovery,
                args.report_artifact_provider_id,
            )
        )
    elif command == "ashare-post-close":
        result = asyncio.run(
            _ashare_post_close(
                args.action,
                args.runtime_dir,
                args.session_date,
                args.account,
                args.target_kind,
                args.target_id,
                args.base_url,
                args.candidate_db,
                args.history_days,
                args.news_runtime_dir,
                args.research_db,
                args.market_evidence_dir,
                args.news_feeds,
                args.refresh_news,
                args.search_discovery,
                args.searxng_url,
                args.macro,
                args.macro_provider,
                args.model,
                args.macro_weight,
                args.dispatch_cycles,
                args.dispatch_poll_interval,
                args.confirm,
                args.recover_analysis,
                args.recover_delivery,
            )
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
        result = asyncio.run(_napcat_send_test(args.base_url, args.target_kind, args.target_id))
    elif command == "napcat-send-artifact":
        result = asyncio.run(
            _napcat_send_artifact(
                args.base_url,
                args.target_kind,
                args.target_id,
                args.artifact_kind,
                args.report_kind,
                args.artifact_root,
                args.artifact,
                args.receipt_db,
            )
        )
    else:  # pragma: no cover - argparse owns the accepted command set
        parser.error(f"unknown command: {command}")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if (
        command
        in {
            "ashare-paper-day",
            "ashare-post-close",
            "live-sync",
            "napcat-send-artifact",
            "strategy-exit-evaluate",
        }
        and result.get("ok") is False
    ):
        return 1
    return 0


def _configure_terminal_encoding() -> None:
    """在 Windows 旧版控制台代码页下保持中文命令行输出可读。"""

    if os.name != "nt":
        return
    for stream in (sys.stdout, sys.stderr):
        if not hasattr(stream, "reconfigure"):
            continue
        stream.reconfigure(encoding="utf-8", errors="replace")


def _set_secret(name: str) -> None:
    from gribuki_trade.security import InteractiveSecretManager

    InteractiveSecretManager(KeyringSecretProvider()).set_secret(name)


def _secret_status() -> dict[str, bool]:
    provider = KeyringSecretProvider()
    return {name: provider.get_secret(name) is not None for name in KNOWN_SECRET_STATUS_NAMES}


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


def _temp_root(action: str, explicit: str | None) -> dict[str, object]:
    from gribuki_trade.runtime.temp_root import TempRootResolver

    if action not in {"status", "prepare"}:
        raise ValueError("unsupported temp-root action")
    resolved = TempRootResolver().resolve(
        explicit,
        create=action == "prepare",
    )
    return {
        "action": action,
        **resolved.audit_document(),
        "exists": resolved.path.is_dir(),
        "ok": True,
    }


def _required_local_secret(name: str) -> str:
    value = KeyringSecretProvider().get_secret(name)
    if value is None:
        raise RuntimeError(f"required local secret {name!r} is not configured; use secret-set")
    return value


def _optional_local_secret(name: str) -> str | None:
    """读取可选集成密钥，同时不将对应供应商变为必需依赖。"""

    try:
        return KeyringSecretProvider().get_secret(name)
    except SecretProviderError:
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _three_decimal_text(value: Decimal | None) -> str | None:
    """面向人员的研究文档所采用的稳定展示精度。"""

    return None if value is None else format(value.quantize(Decimal("0.001")), "f")


def _atomic_write_cli_json(path: Path, payload: object) -> None:
    """在同一文件系统上持久替换一个可选 CLI JSON 产物。"""

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
    """在不写入数据或配置的情况下扩展受控因子语法。"""

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


def _strategy_exit_evaluate(
    *,
    dataset: str,
    specification: str,
    output: str,
    created_at: datetime | None,
    overwrite: bool,
    confirmation: str | None,
) -> dict[str, object]:
    """运行冻结退出策略实验并返回不含逐笔大对象的可读摘要。"""

    if confirmation != "RESEARCH_ONLY":
        return {
            "error_code": "EXIT_POLICY_RESEARCH_CONFIRMATION_REQUIRED",
            "execution_authority": False,
            "ok": False,
            "promotion_authorized": False,
            "research_only": True,
        }
    from gribuki_trade.strategy_lab import run_frozen_exit_policy_experiment

    try:
        artifact = run_frozen_exit_policy_experiment(
            Path(dataset),
            Path(specification),
            Path(output),
            created_at=created_at,
            overwrite=overwrite,
        )
    except FileExistsError:
        return {
            "error_code": "EXIT_POLICY_ARTIFACT_ALREADY_EXISTS",
            "execution_authority": False,
            "ok": False,
            "promotion_authorized": False,
            "research_only": True,
        }
    except (OSError, TypeError, ValueError, ArithmeticError):
        return {
            "error_code": "EXIT_POLICY_EXPERIMENT_INPUT_INVALID",
            "execution_authority": False,
            "ok": False,
            "promotion_authorized": False,
            "research_only": True,
        }
    registry = artifact.registry
    selected = registry.selected_holdout.metrics
    baseline = registry.baseline_holdout.metrics
    return {
        "artifact_sha256": artifact.artifact_sha256,
        "baseline_holdout": {
            "maximum_drawdown": str(baseline.maximum_drawdown),
            "net_return": str(baseline.net_return),
        },
        "dataset_file_sha256": artifact.dataset_file_sha256,
        "execution_authority": False,
        "ok": True,
        "output": str(artifact.destination),
        "promotion_authorized": False,
        "registry_sha256": registry.registry_sha256,
        "research_only": True,
        "selected_holdout": {
            "maximum_drawdown": str(selected.maximum_drawdown),
            "net_return": str(selected.net_return),
        },
        "selected_trial_id": registry.selected_trial_id,
        "specification_file_sha256": artifact.specification_file_sha256,
        "trial_count": len(registry.trials),
    }


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
                "parameters": [{"name": name, "value": value} for name, value in item.parameters],
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
                "parameters": [{"name": name, "value": value} for name, value in item.parameters],
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
    """读取已保留运行输出，且不创建原本不存在的注册表。"""

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
            {"source": source, "revision": revision} for source, revision in item.source_revisions
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

    interval = MinuteInterval.ONE_MINUTE if interval_value == "1m" else MinuteInterval.FIVE_MINUTES
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
                    event.published_at.isoformat() if event.published_at is not None else None
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
                    status=(ProviderRunStatus.SUCCESS if successful else ProviderRunStatus.FAILURE),
                    error_code=result.error_code,
                    item_count=(
                        result.events_new + result.events_revised + result.events_duplicate
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
    """在不访问网络的情况下汇总仅追加的采集遥测。"""

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
            event for event in event_store.latest(limit=200) if event.source_id == config.source_id
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
                    None if event.published_at is None else event.published_at.isoformat()
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
    """运行一次当日收盘后全市场筛选，且不包含订单路径。"""

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
    if resolved_decision_at.tzinfo is None or resolved_decision_at.utcoffset() is None:
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
                            if run.factor_source_id is None or run.factor_source_revision is None
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
    """追加一份输出/血缘清单，且不声称能够回放原始输入。"""

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
    """序列化一次类型化筛选运行，且不泄露供应商对象。"""

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
                        "cross_section_observations": (factor.cross_section_observations),
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
    """返回不含供应商异常细节的稳定失败信封。"""

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
    """运行一次盘中异常扫描；候选标的始终只是研究输入。"""

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
    if resolved_requested_at.tzinfo is None or resolved_requested_at.utcoffset() is None:
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
                        "cross_section_observations": (factor.cross_section_observations),
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
    """管理研究候选事件存储，且不包含任何执行路径。"""

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
                "expires_at": (None if item.expires_at is None else item.expires_at.isoformat()),
                "cooling_until": (
                    None if item.cooling_until is None else item.cooling_until.isoformat()
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
    """操作研究复核状态机，且不产生执行副作用。"""

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
    recommendation_id = _optional_cli_identifier(recommendation_id, "recommendation_id")
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
        not item or len(item) > 120 or any(ord(character) < 32 for character in item)
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


def _live_sync(
    action: str,
    ledger_db: str,
    allowed_senders: Sequence[str] | None,
    event_json: str | None,
    account_id: str | None,
    received_at: datetime | None,
    *,
    exit_plan_db: str = "runtime/live/exit-plans.sqlite3",
    outbox_path: str = "runtime/live/outbox.sqlite3",
    target_kind_value: str | None = None,
    target_id: str | None = None,
    quick_timeout_seconds: float = 45.0,
    base_url: str | None = None,
    dispatch_cycles: int = 3,
    dispatch_poll_interval: float = 0.5,
) -> dict[str, object]:
    """单次处理 OneBot 私聊事件；没有监听端口或操作系统常驻任务。"""

    from gribuki_trade.services.live_trade_records import (
        LiveTradeRecordError,
        LiveTradeRecordService,
        parse_onebot_private_message,
        project_live_account,
    )
    from gribuki_trade.storage.live_records import SQLiteLiveRecordStore

    try:
        supplied_ledger = Path(ledger_db)
        if supplied_ledger.exists() and supplied_ledger.is_symlink():
            return {"error_code": "LIVE_LEDGER_SYMLINK_REJECTED", "ok": False}
        ledger = supplied_ledger.resolve()
        ledger.parent.mkdir(parents=True, exist_ok=True)
        immediate_request: tuple[str, str, str] | None = None
        with SQLiteLiveRecordStore(ledger) as store:
            if action == "status":
                if account_id is None or not account_id.strip():
                    return {"error_code": "LIVE_ACCOUNT_REQUIRED", "ok": False}
                snapshot = project_live_account(account_id, store.events(account_id))
                tracking = store.tracking(account_id, active_only=False)
                work_items = store.work_items(account_id)
                return {
                    "account_id": snapshot.account_id,
                    "confirmed_fill_count": snapshot.confirmed_fill_count,
                    "integrity_verified": True,
                    "last_sequence": snapshot.last_sequence,
                    "ok": True,
                    "positions": [
                        {
                            "average_cost": str(item.average_cost),
                            "instrument_type": item.instrument_type.value,
                            "quantity": item.quantity,
                            "realized_pnl": str(item.realized_pnl),
                            "symbol": item.symbol,
                        }
                        for item in snapshot.positions
                    ],
                    "protection_tracking": [
                        {
                            "buy_command_id": item.buy_command_id,
                            "plan_ready": item.plan_ready,
                            "plan_stream_id": item.plan_stream_id,
                            "protection_id": item.protection_id,
                            "remaining_quantity": item.remaining_quantity,
                            "symbol": item.symbol,
                        }
                        for item in tracking
                    ],
                    "total_fees": str(snapshot.total_fees),
                    "work_items": [
                        {
                            "attempts": item.attempts,
                            "error_code": item.error_code,
                            "kind": item.kind.value,
                            "protection_id": item.protection_id,
                            "result_code": item.result_code,
                            "status": item.status.value,
                            "work_id": item.work_id,
                        }
                        for item in work_items
                    ],
                }
            if action != "ingest":
                return {"error_code": "LIVE_ACTION_INVALID", "ok": False}
            senders = frozenset(allowed_senders or ())
            if not senders:
                return {"error_code": "LIVE_ALLOWED_SENDER_REQUIRED", "ok": False}
            if event_json is None:
                return {"error_code": "LIVE_EVENT_JSON_REQUIRED", "ok": False}
            try:
                payload_text = _read_live_event_json(event_json)
                payload = json.loads(payload_text)
                if not isinstance(payload, dict) or any(
                    not isinstance(key, str) for key in payload
                ):
                    raise ValueError("OneBot payload must be an object")
                message = parse_onebot_private_message(cast(dict[str, object], payload))
                if message.sender_id not in senders:
                    return {"error_code": "SENDER_NOT_ALLOWED", "ok": False}
                service = LiveTradeRecordService(
                    store,
                    allowed_sender_ids=senders,
                    # 服务先完成发送者、消息时效和命令校验；仅合法 proposal
                    # 访问日历，确认/取消不会重复联网解释已经冻结的事实。
                    execution_session_validator=_verify_live_execution_session,
                )
                outcome = service.ingest(message, received_at=received_at)
            except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
                return {"error_code": "LIVE_EVENT_INVALID", "ok": False}
            except LiveTradeRecordError as error:
                return {"error_code": error.code, "ok": False}
            result: dict[str, object] = {
                "account_id": outcome.account_id,
                "analysis_required": outcome.analysis_required,
                "command_id": outcome.command_id,
                "event_sequence": outcome.event_sequence,
                "fingerprint": outcome.fingerprint,
                "ok": True,
                "protection_id": outcome.protection_id,
                "protection_work_id": outcome.protection_work_id,
                "response_text": outcome.response_text,
                "status": outcome.status.value,
            }
            if outcome.analysis_required and outcome.protection_id is not None:
                if outcome.protection_work_id is None:  # pragma: no cover - domain invariant
                    return {"error_code": "LIVE_LEDGER_INTEGRITY_FAILURE", "ok": False}
                immediate_request = (
                    message.sender_id,
                    outcome.protection_id,
                    outcome.protection_work_id,
                )
        if immediate_request is not None:
            sender_id, protection_id, protection_work_id = immediate_request
            try:
                immediate = asyncio.run(
                    _live_sync_post_confirm_quick_and_track(
                        ledger_db=str(ledger),
                        exit_plan_db=exit_plan_db,
                        outbox_path=outbox_path,
                        account_id=cast(str, result["account_id"]),
                        protection_id=protection_id,
                        protection_work_id=protection_work_id,
                        confirming_sender_id=sender_id,
                        target_kind_value=target_kind_value,
                        target_id=target_id,
                        quick_timeout_seconds=quick_timeout_seconds,
                        base_url=base_url,
                        dispatch_cycles=dispatch_cycles,
                        dispatch_poll_interval=dispatch_poll_interval,
                    )
                )
            except (OSError, RuntimeError, TypeError, ValueError, ArithmeticError):
                immediate = {
                    "error_code": "LIVE_IMMEDIATE_PROTECTION_FAILED",
                    "execution_authority": False,
                    "ok": False,
                    "protection_work_id": protection_work_id,
                }
            result["immediate_protection"] = immediate
            result["response_text"] = _append_live_immediate_protection_receipt(
                cast(str, result["response_text"]),
                immediate,
            )
        return result
    except (OSError, RuntimeError):
        return {"error_code": "LIVE_LEDGER_UNAVAILABLE", "ok": False}


def _verify_live_execution_session(executed_at: datetime) -> bool:
    """用 BaoStock 的精确自然日记录判断外部成交日是否开市。"""

    from gribuki_trade.adapters.baostock import BaoStockDailyAdapter

    if executed_at.tzinfo is None or executed_at.utcoffset() is None:
        raise ValueError("executed_at must be timezone-aware")
    local_date = executed_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
    days = tuple(
        BaoStockDailyAdapter(max_attempts=2, timeout_seconds=20.0).fetch_trade_calendar(
            local_date,
            local_date,
        )
    )
    if (
        len(days) != 1
        or days[0].calendar_date != local_date
        or type(days[0].is_trading_day) is not bool
    ):
        raise RuntimeError("BaoStock returned an invalid one-day trading calendar")
    return days[0].is_trading_day


def _read_live_event_json(source: str) -> str:
    """限制 OneBot 单事件大小，避免把任意大文件读入应用进程。"""

    maximum_bytes = 64 * 1024
    if source == "-":
        text = sys.stdin.read(maximum_bytes + 1)
        if len(text.encode("utf-8")) > maximum_bytes:
            raise ValueError("OneBot event exceeds size limit")
        return text
    path = Path(source)
    if path.is_symlink() or not path.is_file():
        raise ValueError("OneBot event path must be a regular file")
    if path.stat().st_size > maximum_bytes:
        raise ValueError("OneBot event exceeds size limit")
    return path.read_text(encoding="utf-8")


async def _live_sync_post_confirm_quick_and_track(
    *,
    ledger_db: str,
    exit_plan_db: str,
    outbox_path: str,
    account_id: str,
    protection_id: str,
    protection_work_id: str,
    confirming_sender_id: str,
    target_kind_value: str | None,
    target_id: str | None,
    quick_timeout_seconds: float,
    base_url: str | None = None,
    dispatch_cycles: int = 3,
    dispatch_poll_interval: float = 0.5,
) -> dict[str, object]:
    """成交提交后，只为本次 BUY 尝试一次有限 QUICK 与行情观察。"""

    if not 0 < quick_timeout_seconds < float("inf"):
        return {
            "error_code": "LIVE_IMMEDIATE_TIMEOUT_INVALID",
            "execution_authority": False,
            "ok": False,
            "protection_work_id": protection_work_id,
        }
    supplied_paths = tuple(Path(value) for value in (ledger_db, exit_plan_db, outbox_path))
    paths = tuple(path.resolve() for path in supplied_paths)
    if len(set(paths)) != len(paths) or any(path.is_symlink() for path in supplied_paths):
        return {
            "error_code": "LIVE_RUNTIME_PATH_INVALID",
            "execution_authority": False,
            "ok": False,
            "protection_work_id": protection_work_id,
        }
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)

    from gribuki_trade.adapters.akshare import AKShareMarketDataAdapter
    from gribuki_trade.adapters.baostock import BaoStockDailyAdapter
    from gribuki_trade.domain.live_records import LiveWorkKind
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.services.exit_plan_lifecycle import ExitPlanLifecycleService
    from gribuki_trade.services.live_market_tracking import LiveMarketTrackingCycleService
    from gribuki_trade.services.live_protection_inputs import (
        PublicMarketLiveProtectionInputProvider,
    )
    from gribuki_trade.services.live_trade_orchestration import (
        LiveTradeOrchestrationService,
    )
    from gribuki_trade.storage.exit_plans import SQLiteExitPlanStore
    from gribuki_trade.storage.live_records import SQLiteLiveRecordStore
    from gribuki_trade.storage.outbox import SQLiteOutbox

    target_fallback = (target_kind_value is None) != (target_id is None)
    resolved_target_kind = NotificationTargetKind(
        "private" if target_fallback else target_kind_value or "private"
    )
    resolved_target_id = (
        confirming_sender_id if target_fallback else target_id or confirming_sender_id
    ).strip()
    if not resolved_target_id:
        return {
            "error_code": "LIVE_NOTIFICATION_TARGET_REQUIRED",
            "execution_authority": False,
            "ok": False,
            "protection_work_id": protection_work_id,
        }
    market_data = AKShareMarketDataAdapter(
        timeout_seconds=12.0,
        max_attempts=1,
        intraday_stale_after_seconds=180.0,
    )
    protection_inputs = PublicMarketLiveProtectionInputProvider(
        market_data=market_data,
        calendar=BaoStockDailyAdapter(max_attempts=2, timeout_seconds=20.0),
        semantic_analyzer=None,
    )
    try:
        with (
            SQLiteLiveRecordStore(paths[0]) as live_store,
            SQLiteExitPlanStore(paths[1]) as exit_store,
            SQLiteOutbox(paths[2]) as outbox,
        ):
            orchestration = LiveTradeOrchestrationService(
                live_store=live_store,
                exit_lifecycle=ExitPlanLifecycleService(exit_store),
                protection_inputs=protection_inputs,
                outbox=outbox,
                notification_target_kind=resolved_target_kind,
                notification_target_id=resolved_target_id,
            )
            quick = await orchestration.process_due_work(
                kinds=frozenset({LiveWorkKind.BUILD_PROTECTION}),
                work_ids=frozenset({protection_work_id}),
                limit=1,
                work_timeout_seconds=quick_timeout_seconds,
            )
            work = next(
                (
                    item
                    for item in live_store.work_items(account_id)
                    if item.work_id == protection_work_id
                ),
                None,
            )
            tracking_state = next(
                (
                    item
                    for item in live_store.tracking(account_id, active_only=False)
                    if item.protection_id == protection_id
                ),
                None,
            )
            if work is None or tracking_state is None:
                raise RuntimeError("confirmed protection state disappeared")
            deep_work = next(
                (
                    item
                    for item in live_store.work_items(account_id)
                    if item.kind is LiveWorkKind.BUILD_DEEP_PROTECTION
                    and item.protection_id == protection_id
                ),
                None,
            )
            tracking_result: dict[str, object]
            tracking_ok = True
            if tracking_state.plan_ready and tracking_state.remaining_quantity > 0:
                tracker = LiveMarketTrackingCycleService(
                    live_store=live_store,
                    market_data=market_data,
                    orchestration=orchestration,
                    maximum_concurrency=1,
                )
                try:
                    observed = await asyncio.wait_for(
                        tracker.run_once(
                            delivery_limit=100,
                            protection_ids=frozenset({protection_id}),
                        ),
                        timeout=quick_timeout_seconds,
                    )
                except TimeoutError:
                    tracking_ok = False
                    tracking_result = {
                        "attempted": True,
                        "error_code": "LIVE_IMMEDIATE_TRACKING_TIMEOUT",
                        "ok": False,
                    }
                else:
                    delivery_ok = (
                        observed.delivery.retried == 0
                        and observed.delivery.dead == 0
                        and observed.delivery.completed == observed.queued_alerts
                    )
                    tracking_ok = not observed.failures and delivery_ok
                    tracking_result = {
                        "attempted": True,
                        "barrier_observations": observed.barrier_observations,
                        "failures": [
                            {
                                "account_id": item.account_id,
                                "error_code": item.error_code,
                                "symbol": item.symbol,
                            }
                            for item in observed.failures
                        ],
                        "fetched_bars": observed.fetched_bars,
                        "outbox_delivery": {
                            "completed": observed.delivery.completed,
                            "dead": observed.delivery.dead,
                            "retried": observed.delivery.retried,
                        },
                        "ok": tracking_ok,
                        "queued_alerts": observed.queued_alerts,
                        "target_count": observed.target_count,
                    }
                    if not tracking_ok:
                        tracking_result["error_code"] = (
                            "LIVE_IMMEDIATE_TRACKING_INCOMPLETE"
                            if observed.failures
                            else "LIVE_IMMEDIATE_ALERT_OUTBOX_PENDING"
                        )
            else:
                tracking_result = {
                    "attempted": False,
                    "ok": tracking_state.remaining_quantity == 0,
                    "reason_code": (
                        "POSITION_ALREADY_CLOSED"
                        if tracking_state.remaining_quantity == 0
                        else "QUICK_PLAN_NOT_READY"
                    ),
                }
                tracking_ok = tracking_state.remaining_quantity == 0
            quick_ok = tracking_state.plan_ready or tracking_state.remaining_quantity == 0
            result: dict[str, object] = {
                "deep": {
                    "queued": deep_work is not None,
                    "status": None if deep_work is None else deep_work.status.value,
                    "work_id": None if deep_work is None else deep_work.work_id,
                },
                "execution_authority": False,
                "ok": quick_ok and tracking_ok,
                "notification_target_fallback": target_fallback,
                "protection_id": protection_id,
                "protection_work_id": protection_work_id,
                "quick": {
                    "claimed": quick.claimed,
                    "completed": quick.completed,
                    "dead": quick.dead,
                    "error_code": work.error_code,
                    "plan_ready": tracking_state.plan_ready,
                    "remaining_quantity": tracking_state.remaining_quantity,
                    "result_code": work.result_code,
                    "retried": quick.retried,
                    "status": work.status.value,
                },
                "tracking": tracking_result,
            }
            if not result["ok"]:
                result["error_code"] = (
                    work.error_code
                    or tracking_result.get("error_code")
                    or "LIVE_IMMEDIATE_PROTECTION_PENDING"
                )
            raw_outbox_delivery = tracking_result.get("outbox_delivery")
            delivered_to_outbox: Mapping[str, object] | None = (
                raw_outbox_delivery if isinstance(raw_outbox_delivery, Mapping) else None
            )
            if (
                base_url is not None
                and delivered_to_outbox is not None
                and delivered_to_outbox.get("completed") == tracking_result.get("queued_alerts")
                and delivered_to_outbox.get("completed") != 0
            ):
                try:
                    dispatched = await _napcat_dispatch(
                        base_url,
                        resolved_target_kind.value,
                        resolved_target_id,
                        str(paths[2]),
                        dispatch_cycles,
                        dispatch_poll_interval,
                    )
                except asyncio.CancelledError:
                    raise
                except (OSError, RuntimeError, TypeError, ValueError):
                    result["dispatch"] = {
                        "attempted": True,
                        "error_code": "LIVE_IMMEDIATE_ALERT_DISPATCH_FAILED",
                        "ok": False,
                    }
                else:
                    dispatch_ok = (
                        dispatched["dead"] == 0
                        and dispatched["retry_scheduled"] == 0
                    )
                    result["dispatch"] = {
                        "attempted": True,
                        "dead": dispatched["dead"],
                        "ok": dispatch_ok,
                        "retry_scheduled": dispatched["retry_scheduled"],
                        "sent": dispatched["sent"],
                    }
                    if not dispatch_ok:
                        cast(dict[str, object], result["dispatch"])["error_code"] = (
                            "LIVE_IMMEDIATE_ALERT_DISPATCH_PENDING"
                        )
            else:
                result["dispatch"] = {
                    "attempted": False,
                    "ok": True,
                    "reason_code": (
                        "NAPCAT_NOT_CONFIGURED"
                        if base_url is None
                        else "NO_DURABLE_ALERT_READY"
                    ),
                }
            return result
    except asyncio.CancelledError:
        raise
    except (OSError, RuntimeError, TypeError, ValueError, ArithmeticError):
        return {
            "error_code": "LIVE_IMMEDIATE_PROTECTION_FAILED",
            "execution_authority": False,
            "ok": False,
            "protection_id": protection_id,
            "protection_work_id": protection_work_id,
        }


def _append_live_immediate_protection_receipt(
    response_text: str,
    immediate: Mapping[str, object],
) -> str:
    """把提交后尝试的真实状态追加到成交回执，而不改写成交结论。"""

    quick = immediate.get("quick")
    tracking = immediate.get("tracking")
    if (
        isinstance(quick, Mapping)
        and quick.get("plan_ready") is True
        and isinstance(tracking, Mapping)
        and tracking.get("ok") is True
    ):
        detail = "QUICK 已生成并已完成一次有限行情跟踪；DEEP 已持久排队。"
    elif isinstance(quick, Mapping) and quick.get("plan_ready") is True:
        error_code = immediate.get("error_code") or "LIVE_IMMEDIATE_TRACKING_INCOMPLETE"
        detail = f"QUICK 已生成，但本次有限跟踪未完整完成（{error_code}）；后续 cycle 将继续恢复。"
    elif immediate.get("ok") is True:
        detail = "持仓已在并发同步中关闭，无需再激活保护计划。"
    else:
        error_code = immediate.get("error_code") or "LIVE_IMMEDIATE_PROTECTION_PENDING"
        detail = f"本次有限 QUICK/跟踪未完整完成（{error_code}）；持久工作仍由后续 cycle 恢复。"
    dispatch = immediate.get("dispatch")
    if isinstance(dispatch, Mapping) and dispatch.get("attempted") is True:
        if dispatch.get("ok") is True:
            detail += f" NapCat 有限派发已完成，发送 {dispatch.get('sent', 0)} 条。"
        else:
            dispatch_code = dispatch.get("error_code") or "LIVE_IMMEDIATE_ALERT_DISPATCH_PENDING"
            detail += f" NapCat 派发未完成（{dispatch_code}），提醒仍保留在 durable outbox。"
    return response_text.rstrip() + "\n即时保护结果：" + detail


async def _live_sync_cycle(
    *,
    ledger_db: str,
    exit_plan_db: str,
    outbox_path: str,
    target_kind_value: str | None,
    target_id: str | None,
    base_url: str | None,
    llm_provider: str | None,
    llm_model: str | None,
    work_limit: int,
    dispatch_cycles: int,
    dispatch_poll_interval: float,
    confirmation: str | None,
    deep_timeout_seconds: float = 600.0,
    tracking_pump_interval: float = 30.0,
    tracking_pump_limit: int = 20,
) -> dict[str, object]:
    """执行一次有限的实盘保护构建、行情观察和 NapCat 派发。"""

    if confirmation != "LIVE_SYNC_CYCLE":
        return {"error_code": "LIVE_SYNC_CYCLE_CONFIRMATION_REQUIRED", "ok": False}
    if (
        not 0 < deep_timeout_seconds < float("inf")
        or not 0 < tracking_pump_interval < float("inf")
        or isinstance(tracking_pump_limit, bool)
        or not isinstance(tracking_pump_limit, int)
        or tracking_pump_limit < 1
        or tracking_pump_interval * tracking_pump_limit < deep_timeout_seconds
    ):
        return {"error_code": "LIVE_DEEP_PUMP_CONFIG_INVALID", "ok": False}
    if target_kind_value is None or target_id is None or not target_id.strip():
        return {"error_code": "LIVE_NOTIFICATION_TARGET_REQUIRED", "ok": False}
    if base_url is None:
        return {"error_code": "LIVE_ONEBOT_URL_REQUIRED", "ok": False}
    provider = (llm_provider or "").strip().casefold()
    if provider not in {"deepseek", "openai"}:
        return {"error_code": "LIVE_LLM_PROVIDER_INVALID", "ok": False}
    try:
        model = validate_runtime_model_id(llm_model or "")
    except (TypeError, ValueError):
        return {"error_code": "LIVE_LLM_MODEL_INVALID", "ok": False}
    try:
        api_key = _required_local_secret(
            DEEPSEEK_API_KEY_SECRET if provider == "deepseek" else OPENAI_API_KEY_SECRET
        )
    except (RuntimeError, SecretProviderError):
        return {"error_code": "LIVE_LLM_API_KEY_NOT_CONFIGURED", "ok": False}

    paths = tuple(Path(value).resolve() for value in (ledger_db, exit_plan_db, outbox_path))
    if len(set(paths)) != len(paths) or any(path.exists() and path.is_symlink() for path in paths):
        return {"error_code": "LIVE_RUNTIME_PATH_INVALID", "ok": False}
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)

    import httpx

    from gribuki_trade.adapters.akshare import AKShareMarketDataAdapter
    from gribuki_trade.adapters.baostock import BaoStockDailyAdapter
    from gribuki_trade.adapters.llm import (
        DeepSeekChatMacroAnalyzer,
        OpenAIResponsesMacroAnalyzer,
    )
    from gribuki_trade.domain.live_records import LiveWorkKind
    from gribuki_trade.ports.llm_analyzer import MacroAnalyzer
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.security.config import SecretValue
    from gribuki_trade.services.exit_plan_lifecycle import ExitPlanLifecycleService
    from gribuki_trade.services.live_market_tracking import (
        LiveMarketTrackingCycleService,
    )
    from gribuki_trade.services.live_protection_inputs import (
        ProductionLiveDualExitSemanticAnalyzer,
        PublicMarketLiveProtectionInputProvider,
    )
    from gribuki_trade.services.live_trade_orchestration import (
        LiveTradeOrchestrationService,
    )
    from gribuki_trade.services.llm_production import (
        ProductionLLMProfile,
        build_production_dual_track_analyzer,
    )
    from gribuki_trade.storage.exit_plans import SQLiteExitPlanStore
    from gribuki_trade.storage.live_records import SQLiteLiveRecordStore
    from gribuki_trade.storage.outbox import SQLiteOutbox

    target_kind = NotificationTargetKind(target_kind_value)
    client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=6, max_keepalive_connections=6),
        timeout=httpx.Timeout(180.0),
    )
    owned_dual = None
    try:
        baseline: MacroAnalyzer
        if provider == "deepseek":
            baseline = DeepSeekChatMacroAnalyzer(
                SecretValue(api_key),
                model=model,
                client=client,
            )
        else:
            baseline = OpenAIResponsesMacroAnalyzer(
                SecretValue(api_key),
                model=model,
                timeout_seconds=180.0,
                client=client,
            )
        owned_dual = build_production_dual_track_analyzer(
            baseline,
            audit_path=paths[0].parent / "live-adversarial-audit.sqlite3",
            profile=ProductionLLMProfile.DEEP,
            maximum_calls_per_session=None,
        )
        market_data = AKShareMarketDataAdapter(
            timeout_seconds=12.0,
            max_attempts=1,
            intraday_stale_after_seconds=180.0,
        )
        protection_inputs = PublicMarketLiveProtectionInputProvider(
            market_data=market_data,
            calendar=BaoStockDailyAdapter(max_attempts=2, timeout_seconds=20.0),
            semantic_analyzer=ProductionLiveDualExitSemanticAnalyzer(owned_dual),
        )
        with (
            SQLiteLiveRecordStore(paths[0]) as live_store,
            SQLiteExitPlanStore(paths[1]) as exit_store,
            SQLiteOutbox(paths[2]) as outbox,
        ):
            orchestration = LiveTradeOrchestrationService(
                live_store=live_store,
                exit_lifecycle=ExitPlanLifecycleService(exit_store),
                protection_inputs=protection_inputs,
                outbox=outbox,
                notification_target_kind=target_kind,
                notification_target_id=target_id.strip(),
            )
            tracker = LiveMarketTrackingCycleService(
                live_store=live_store,
                market_data=market_data,
                orchestration=orchestration,
                maximum_concurrency=1,
            )
            tracking_runs = [await tracker.run_once(delivery_limit=max(100, work_limit))]
            dispatch_failed = False
            try:
                dispatch = await _napcat_dispatch(
                    base_url,
                    target_kind.value,
                    target_id.strip(),
                    str(paths[2]),
                    dispatch_cycles,
                    dispatch_poll_interval,
                )
            except asyncio.CancelledError:
                raise
            except (OSError, RuntimeError, TypeError, ValueError):
                # 通知链路故障只能使 outbox 保持待派发，不能阻断 QUICK/DEEP
                # 保护、成交关闭或后续 barrier 观察。
                dispatch_failed = True
                dispatch = {"dead": 0, "retry_scheduled": 0, "sent": 0}
            close_work = await orchestration.process_due_work(
                kinds=frozenset({LiveWorkKind.CLOSE_PROTECTION}),
                limit=work_limit,
            )
            build_work = await orchestration.process_due_work(
                kinds=frozenset({LiveWorkKind.BUILD_PROTECTION}),
                limit=1,
            )
            deep_task = asyncio.create_task(
                orchestration.process_due_work(
                    kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
                    limit=1,
                    work_timeout_seconds=deep_timeout_seconds,
                )
            )
            try:
                for _ in range(tracking_pump_limit):
                    if deep_task.done():
                        break
                    done, _pending = await asyncio.wait(
                        {deep_task},
                        timeout=tracking_pump_interval,
                    )
                    if done:
                        break
                    tracking_run = await tracker.run_once(delivery_limit=max(100, work_limit))
                    tracking_runs.append(tracking_run)
                    if not tracking_run.queued_alerts:
                        continue
                    try:
                        later_dispatch = await _napcat_dispatch(
                            base_url,
                            target_kind.value,
                            target_id.strip(),
                            str(paths[2]),
                            dispatch_cycles,
                            dispatch_poll_interval,
                        )
                    except asyncio.CancelledError:
                        raise
                    except (OSError, RuntimeError, TypeError, ValueError):
                        dispatch_failed = True
                    else:
                        dispatch = {
                            key: cast(int, dispatch[key]) + cast(int, later_dispatch[key])
                            for key in ("dead", "retry_scheduled", "sent")
                        }
                deep_work = await deep_task
            finally:
                if not deep_task.done():
                    deep_task.cancel()
                    await asyncio.gather(deep_task, return_exceptions=True)
    except asyncio.CancelledError:
        raise
    except (OSError, RuntimeError, TypeError, ValueError, ArithmeticError):
        return {"error_code": "LIVE_SYNC_CYCLE_FAILED", "ok": False}
    finally:
        if owned_dual is not None:
            owned_dual.close()
        await client.aclose()

    result: dict[str, object] = {
        "analysis": {
            "build": {
                "claimed": build_work.claimed,
                "completed": build_work.completed,
                "dead": build_work.dead,
                "retried": build_work.retried,
            },
            "build_limit": 1,
            "deep": {
                "claimed": deep_work.claimed,
                "completed": deep_work.completed,
                "dead": deep_work.dead,
                "retried": deep_work.retried,
            },
            "deep_timeout_seconds": deep_timeout_seconds,
            "close": {
                "claimed": close_work.claimed,
                "completed": close_work.completed,
                "dead": close_work.dead,
                "retried": close_work.retried,
            },
        },
        "dispatch": {
            "dead": dispatch["dead"],
            "retry_scheduled": dispatch["retry_scheduled"],
            "sent": dispatch["sent"],
        },
        "execution_authority": False,
        "ok": not dispatch_failed,
        "tracking": {
            "barrier_observations": sum(item.barrier_observations for item in tracking_runs),
            "failures": [
                {
                    "account_id": item.account_id,
                    "error_code": item.error_code,
                    "symbol": item.symbol,
                }
                for run in tracking_runs
                for item in run.failures
            ],
            "fetched_bars": sum(item.fetched_bars for item in tracking_runs),
            "pump_runs": len(tracking_runs) - 1,
            "queued_alerts": sum(item.queued_alerts for item in tracking_runs),
            "target_count": max(item.target_count for item in tracking_runs),
        },
    }
    if dispatch_failed:
        result["error_code"] = "LIVE_ALERT_DISPATCH_FAILED"
    return result


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
    """操作由成交驱动的 PAPER 账本；不虚构任何市场成交。"""

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
            trading_date = (
                session_date or resolved_executed_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
            )
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


class _PaperDayCLIError(RuntimeError):
    """消息中不含供应商数据的稳定生产边界错误。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"A-share PAPER day unavailable ({code})")


class _PaperDayResultProjectionSource(Protocol):
    @property
    def completed(self) -> bool: ...

    @property
    def notification_required(self) -> int: ...

    @property
    def notification_sent(self) -> int: ...

    @property
    def notification_gaps(self) -> int: ...

    @property
    def artifact_delivery_status(self) -> str: ...

    @property
    def artifact_delivery_complete(self) -> bool: ...

    @property
    def daily_review_delivery_complete(self) -> bool: ...


def _paper_day_delivery_projection(
    result: _PaperDayResultProjectionSource,
) -> dict[str, object]:
    """把交易终态与 DAILY_REVIEW 双交付状态分开投影给 CLI。"""

    completed = bool(result.completed)
    notification_required = int(result.notification_required)
    notification_sent = int(result.notification_sent)
    notification_gaps = int(result.notification_gaps)
    artifact_delivery_status = str(
        getattr(result, "artifact_delivery_status", "LEGACY_UNREPORTED")
    )
    artifact_delivery_complete = bool(
        getattr(
            result,
            "artifact_delivery_complete",
            False,
        )
    )
    daily_review_delivery_complete = bool(
        getattr(
            result,
            "daily_review_delivery_complete",
            False,
        )
    )
    return {
        "ok": completed and daily_review_delivery_complete,
        "notification_required": notification_required,
        "notification_sent": notification_sent,
        "notification_gaps": notification_gaps,
        "text_notification_required": int(
            getattr(result, "text_notification_required", notification_required)
        ),
        "text_notification_sent": int(
            getattr(result, "text_notification_sent", notification_sent)
        ),
        "text_notification_gaps": int(
            getattr(result, "text_notification_gaps", notification_gaps)
        ),
        "artifact_delivery_status": artifact_delivery_status,
        "artifact_delivery_complete": artifact_delivery_complete,
        "daily_review_delivery_complete": daily_review_delivery_complete,
    }


async def _ashare_paper_day(
    action: str,
    runtime_dir: str,
    session_date: date | None,
    account_id: str,
    initial_cash: Decimal,
    target_kind_value: str | None,
    target_id: str | None,
    base_url: str,
    confirmation: str | None,
    recover_after_abort: bool = False,
    maximum_positions: int | None = None,
    risk_policy_change_confirmation: str | None = None,
    intraday_llm_enabled: bool = True,
    intraday_llm_review_top_n: int = 6,
    intraday_llm_review_ttl_minutes: int = 20,
    intraday_llm_max_calls: int | None = None,
    intraday_llm_events_db: str = "runtime/news/events.sqlite3",
    intraday_llm_provider: str | None = "deepseek",
    intraday_llm_model: str | None = DEFAULT_DEEPSEEK_MODEL,
    report_artifact_recovery_action: str | None = None,
    report_artifact_recovery_confirmation: str | None = None,
    report_artifact_provider_identifier: str | None = None,
    *,
    now: datetime | None = None,
    calendar_provider: AsyncTradingCalendar | None = None,
) -> dict[str, object]:
    """运行或检查一个隔离的单进程 A 股 PAPER 会话。

    ``status`` 与 ``report`` 刻意只检查伴随文件；``summary`` 读取相同伴随文件，并以
    原子方式写入增强 Markdown 投影。这些操作都不会打开实时日志、发件箱或账本数据库，
    从而在受共享 WAL 重置竞态影响的 SQLite 运行时上保持其安全。
    """

    if action not in {"run", "status", "report", "summary"}:
        raise ValueError("unsupported A-share PAPER-day action")
    if not isinstance(recover_after_abort, bool):
        raise TypeError("recover_after_abort must be bool")
    if maximum_positions is not None and (
        isinstance(maximum_positions, bool)
        or not isinstance(maximum_positions, int)
        or maximum_positions <= 0
    ):
        raise ValueError("maximum_positions must be a positive integer or None")
    if risk_policy_change_confirmation not in {None, "PAPER_RISK_POLICY_CHANGE"}:
        raise ValueError("unsupported risk-policy change confirmation")
    artifact_recovery_values = (
        report_artifact_recovery_action,
        report_artifact_recovery_confirmation,
        report_artifact_provider_identifier,
    )
    if action != "run" and any(value is not None for value in artifact_recovery_values):
        raise ValueError("report-artifact recovery is only available for run")
    if report_artifact_recovery_action not in {
        None,
        "MARK_SENT_AFTER_PROVIDER_VERIFICATION",
        "RESEND_AFTER_PROVIDER_NON_RECEIPT_VERIFICATION",
    }:
        raise ValueError("unsupported report-artifact recovery action")
    if report_artifact_recovery_action is None:
        if any(value is not None for value in artifact_recovery_values[1:]):
            raise ValueError("report-artifact recovery options require an action")
    elif report_artifact_recovery_confirmation != "PAPER_REPORT_ARTIFACT_RECOVERY":
        raise ValueError("report-artifact recovery requires explicit confirmation")
    elif (
        report_artifact_recovery_action == "MARK_SENT_AFTER_PROVIDER_VERIFICATION"
        and not report_artifact_provider_identifier
    ):
        raise ValueError("mark-sent recovery requires a provider file ID")
    elif (
        report_artifact_recovery_action == "RESEND_AFTER_PROVIDER_NON_RECEIPT_VERIFICATION"
        and report_artifact_provider_identifier is not None
    ):
        raise ValueError("resend recovery does not accept a provider file ID")
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None or resolved_now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    local_today = resolved_now.astimezone(ZoneInfo("Asia/Shanghai")).date()
    resolved_session = session_date or local_today
    session_root = Path(runtime_dir).resolve() / resolved_session.isoformat()

    if action == "status":
        return _ashare_paper_day_status(session_root, resolved_session)
    if action == "report":
        return _ashare_paper_day_report(session_root, resolved_session)
    if action == "summary":
        return _ashare_paper_day_summary(session_root, resolved_session)

    if not isinstance(intraday_llm_enabled, bool):
        raise TypeError("intraday_llm_enabled must be bool")
    for name, value in (
        ("intraday_llm_review_top_n", intraday_llm_review_top_n),
        ("intraday_llm_review_ttl_minutes", intraday_llm_review_ttl_minutes),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if intraday_llm_max_calls is not None and (
        isinstance(intraday_llm_max_calls, bool)
        or not isinstance(intraday_llm_max_calls, int)
        or intraday_llm_max_calls <= 0
    ):
        raise ValueError("intraday_llm_max_calls must be a positive integer or None")
    if not isinstance(intraday_llm_events_db, str) or not intraday_llm_events_db.strip():
        raise ValueError("intraday_llm_events_db must not be empty")
    intraday_llm_provider = (intraday_llm_provider or "deepseek").strip().casefold()
    if intraday_llm_provider not in {"deepseek", "openai"}:
        raise ValueError("intraday_llm_provider must be deepseek or openai")
    default_model = DEFAULT_DEEPSEEK_MODEL if intraday_llm_provider == "deepseek" else "gpt-5.6"
    intraday_llm_model = validate_runtime_model_id(intraday_llm_model or default_model)

    if confirmation != "PAPER_DAY":
        return _ashare_paper_day_error(
            action,
            resolved_session,
            session_root,
            "PAPER_DAY_CONFIRMATION_REQUIRED",
        )
    if target_kind_value is None or target_id is None or not target_id.strip():
        return _ashare_paper_day_error(
            action,
            resolved_session,
            session_root,
            "NOTIFICATION_TARGET_REQUIRED",
        )
    if resolved_session != local_today:
        return _ashare_paper_day_error(
            action,
            resolved_session,
            session_root,
            "SESSION_DATE_NOT_TODAY",
        )
    if not account_id.strip():
        return _ashare_paper_day_error(
            action,
            resolved_session,
            session_root,
            "ACCOUNT_ID_INVALID",
        )

    try:
        (
            latest_completed,
            future_trading_sessions,
            calendar_revision_sha256,
        ) = await _load_ashare_paper_day_calendar_window(
            resolved_session,
            now=resolved_now,
            calendar_provider=calendar_provider,
        )
    except _PaperDayCLIError as error:
        return _ashare_paper_day_error(
            action,
            resolved_session,
            session_root,
            error.code,
        )

    try:
        token = _required_local_secret(NAPCAT_ACCESS_TOKEN_SECRET)
    except (RuntimeError, SecretProviderError):
        return _ashare_paper_day_error(
            action,
            resolved_session,
            session_root,
            "NAPCAT_TOKEN_NOT_CONFIGURED",
        )

    llm_api_key: str | None = None
    if intraday_llm_enabled:
        try:
            llm_api_key = _required_local_secret(
                DEEPSEEK_API_KEY_SECRET
                if intraday_llm_provider == "deepseek"
                else OPENAI_API_KEY_SECRET
            )
        except (RuntimeError, SecretProviderError):
            return _ashare_paper_day_error(
                action,
                resolved_session,
                session_root,
                "INTRADAY_LLM_API_KEY_NOT_CONFIGURED",
            )

    try:
        return await _run_ashare_paper_day(
            session_root=session_root,
            session_date=resolved_session,
            latest_completed_session=latest_completed,
            future_trading_sessions=future_trading_sessions,
            calendar_revision_sha256=calendar_revision_sha256,
            account_id=account_id,
            initial_cash=initial_cash,
            target_kind_value=target_kind_value,
            target_id=target_id.strip(),
            base_url=base_url,
            access_token=token,
            started_at=resolved_now,
            recover_after_abort=recover_after_abort,
            maximum_positions=maximum_positions,
            risk_policy_change_confirmation=risk_policy_change_confirmation,
            intraday_llm_enabled=intraday_llm_enabled,
            intraday_llm_review_top_n=intraday_llm_review_top_n,
            intraday_llm_review_ttl_minutes=(intraday_llm_review_ttl_minutes),
            intraday_llm_max_calls=intraday_llm_max_calls,
            intraday_llm_events_db=intraday_llm_events_db,
            intraday_llm_provider=intraday_llm_provider,
            intraday_llm_model=intraday_llm_model,
            intraday_llm_api_key=llm_api_key,
            report_artifact_recovery_action=report_artifact_recovery_action,
            report_artifact_recovery_confirmation=(
                report_artifact_recovery_confirmation
            ),
            report_artifact_provider_identifier=(
                report_artifact_provider_identifier
            ),
        )
    except asyncio.CancelledError:
        raise
    except _PaperDayCLIError as error:
        return _ashare_paper_day_error(
            action,
            resolved_session,
            session_root,
            error.code,
        )
    except Exception:
        return _ashare_paper_day_error(
            action,
            resolved_session,
            session_root,
            "PAPER_DAY_RUN_FAILED",
        )


async def _verify_ashare_paper_day_calendar(
    session_date: date,
    *,
    now: datetime,
    calendar_provider: AsyncTradingCalendar | None = None,
) -> date:
    """从完整真实日历中返回相邻的上一交易日。"""

    latest_completed, _, _ = await _load_ashare_paper_day_calendar_window(
        session_date,
        now=now,
        calendar_provider=calendar_provider,
    )
    return latest_completed


async def _load_ashare_paper_day_calendar_window(
    session_date: date,
    *,
    now: datetime,
    calendar_provider: AsyncTradingCalendar | None = None,
) -> tuple[date, tuple[date, ...], str]:
    """冻结前一交易日及未来退出时间门所需的真实 A 股交易日历。"""

    from gribuki_trade.adapters.baostock import BaoStockDailyAdapter

    provider = calendar_provider or BaoStockDailyAdapter(
        max_attempts=1,
        timeout_seconds=20.0,
    )
    start = session_date - timedelta(days=45)
    end = session_date + timedelta(days=45)
    try:
        supplied = tuple(await provider.fetch_trade_calendar_async(start, end))
    except asyncio.CancelledError:
        raise
    except Exception:
        raise _PaperDayCLIError("TRADING_CALENDAR_UNAVAILABLE") from None
    expected_dates = tuple(
        start + timedelta(days=offset) for offset in range((end - start).days + 1)
    )
    supplied_dates = tuple(item.calendar_date for item in supplied)
    if supplied_dates != expected_dates or any(
        type(item.is_trading_day) is not bool for item in supplied
    ):
        raise _PaperDayCLIError("TRADING_CALENDAR_INVALID")
    trading_dates = tuple(item.calendar_date for item in supplied if item.is_trading_day)
    if session_date not in trading_dates:
        raise _PaperDayCLIError("TODAY_NOT_TRADING_SESSION")
    prior = tuple(item for item in trading_dates if item < session_date)
    if not prior:
        raise _PaperDayCLIError("PREVIOUS_TRADING_SESSION_MISSING")
    future = tuple(item for item in trading_dates if item > session_date)
    if len(future) < 5:
        raise _PaperDayCLIError("FUTURE_TRADING_SESSIONS_MISSING")
    local_now = now.astimezone(ZoneInfo("Asia/Shanghai"))
    if local_now.date() != session_date:
        raise _PaperDayCLIError("SESSION_DATE_NOT_TODAY")
    calendar_revision_sha256 = hashlib.sha256(
        json.dumps(
            [
                {
                    "date": item.calendar_date.isoformat(),
                    "is_trading_day": item.is_trading_day,
                }
                for item in supplied
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return prior[-1], future, calendar_revision_sha256


async def _run_ashare_paper_day(
    *,
    session_root: Path,
    session_date: date,
    latest_completed_session: date,
    future_trading_sessions: tuple[date, ...] = (),
    calendar_revision_sha256: str | None = None,
    account_id: str,
    initial_cash: Decimal,
    target_kind_value: str,
    target_id: str,
    base_url: str,
    access_token: str,
    started_at: datetime,
    recover_after_abort: bool = False,
    maximum_positions: int | None = None,
    risk_policy_change_confirmation: str | None = None,
    intraday_llm_enabled: bool = True,
    intraday_llm_review_top_n: int = 6,
    intraday_llm_review_ttl_minutes: int = 20,
    intraday_llm_max_calls: int | None = None,
    intraday_llm_events_db: str = "runtime/news/events.sqlite3",
    intraday_llm_provider: str = "deepseek",
    intraday_llm_model: str = DEFAULT_DEEPSEEK_MODEL,
    intraday_llm_api_key: str | None = None,
    report_artifact_recovery_action: str | None = None,
    report_artifact_recovery_confirmation: str | None = None,
    report_artifact_provider_identifier: str | None = None,
) -> dict[str, object]:
    """冻结 LLM 证据、管理其共享客户端，再进入日内运行时。"""

    import httpx

    from gribuki_trade.adapters.llm import (
        DeepSeekChatMacroAnalyzer,
        OpenAIResponsesMacroAnalyzer,
    )
    from gribuki_trade.ports.llm_analyzer import MacroAnalyzer
    from gribuki_trade.security.config import SecretValue
    from gribuki_trade.services.ashare_intraday_llm import (
        IntradayLLMConfig,
    )
    from gribuki_trade.services.llm_production import (
        OwnedProductionDualTrackAnalyzer,
        PaperDayDualTrackDeepExitAssessmentProvider,
        ProductionLLMProfile,
        build_production_dual_track_analyzer,
        recommended_intraday_review_timeout,
    )
    from gribuki_trade.services.macro_research import MacroResearchService

    if not isinstance(intraday_llm_enabled, bool):
        raise TypeError("intraday_llm_enabled must be bool")
    for name, value in (
        ("intraday_llm_review_top_n", intraday_llm_review_top_n),
        ("intraday_llm_review_ttl_minutes", intraday_llm_review_ttl_minutes),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if intraday_llm_max_calls is not None and (
        isinstance(intraday_llm_max_calls, bool)
        or not isinstance(intraday_llm_max_calls, int)
        or intraday_llm_max_calls <= 0
    ):
        raise ValueError("intraday_llm_max_calls must be a positive integer or None")
    if not isinstance(intraday_llm_events_db, str) or not (intraday_llm_events_db.strip()):
        raise ValueError("intraday_llm_events_db must not be empty")
    intraday_llm_provider = intraday_llm_provider.strip().casefold()
    if intraday_llm_provider not in {"deepseek", "openai"}:
        raise ValueError("intraday_llm_provider must be deepseek or openai")
    intraday_llm_model = validate_runtime_model_id(intraday_llm_model)
    if started_at.tzinfo is None or started_at.utcoffset() is None:
        raise ValueError("started_at must be timezone-aware")

    events_path = Path(intraday_llm_events_db).resolve()
    if not intraday_llm_enabled:
        return await _run_ashare_paper_day_owned(
            session_root=session_root,
            session_date=session_date,
            latest_completed_session=latest_completed_session,
            future_trading_sessions=future_trading_sessions,
            calendar_revision_sha256=calendar_revision_sha256,
            account_id=account_id,
            initial_cash=initial_cash,
            target_kind_value=target_kind_value,
            target_id=target_id,
            base_url=base_url,
            access_token=access_token,
            started_at=started_at,
            recover_after_abort=recover_after_abort,
            maximum_positions=maximum_positions,
            risk_policy_change_confirmation=risk_policy_change_confirmation,
            intraday_research=None,
            intraday_llm_config=None,
            intraday_events_path=events_path,
            deep_exit_assessment_provider=None,
            report_artifact_recovery_action=report_artifact_recovery_action,
            report_artifact_recovery_confirmation=(
                report_artifact_recovery_confirmation
            ),
            report_artifact_provider_identifier=(
                report_artifact_provider_identifier
            ),
        )

    if not isinstance(intraday_llm_api_key, str) or not intraday_llm_api_key.strip():
        raise _PaperDayCLIError("INTRADAY_LLM_API_KEY_NOT_CONFIGURED")

    shared_client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=4, max_keepalive_connections=4),
        timeout=httpx.Timeout(8.0),
    )
    owned_dual: OwnedProductionDualTrackAnalyzer | None = None
    try:
        baseline: MacroAnalyzer
        if intraday_llm_provider == "deepseek":
            baseline = DeepSeekChatMacroAnalyzer.for_intraday(
                SecretValue(intraday_llm_api_key),
                model=intraday_llm_model,
                client=shared_client,
            )
        else:
            baseline = OpenAIResponsesMacroAnalyzer(
                SecretValue(intraday_llm_api_key),
                model=intraday_llm_model,
                timeout_seconds=15.0,
                client=shared_client,
            )
        owned_dual = build_production_dual_track_analyzer(
            baseline,
            audit_path=session_root / "llm-adversarial.sqlite3",
            profile=ProductionLLMProfile.INTRADAY,
            maximum_calls_per_session=intraday_llm_max_calls,
        )
        research = MacroResearchService(owned_dual)
        deep_exit_assessment_provider = PaperDayDualTrackDeepExitAssessmentProvider(owned_dual)
        llm_config = IntradayLLMConfig(
            enabled=True,
            required_for_buy=True,
            review_top_n=intraday_llm_review_top_n,
            review_ttl=timedelta(minutes=intraday_llm_review_ttl_minutes),
            per_review_timeout=recommended_intraday_review_timeout(),
            maximum_reviews_per_session=intraday_llm_max_calls,
        )
        return await _run_ashare_paper_day_owned(
            session_root=session_root,
            session_date=session_date,
            latest_completed_session=latest_completed_session,
            future_trading_sessions=future_trading_sessions,
            calendar_revision_sha256=calendar_revision_sha256,
            account_id=account_id,
            initial_cash=initial_cash,
            target_kind_value=target_kind_value,
            target_id=target_id,
            base_url=base_url,
            access_token=access_token,
            started_at=started_at,
            recover_after_abort=recover_after_abort,
            maximum_positions=maximum_positions,
            risk_policy_change_confirmation=risk_policy_change_confirmation,
            intraday_research=research,
            intraday_llm_config=llm_config,
            intraday_events_path=events_path,
            deep_exit_assessment_provider=deep_exit_assessment_provider,
            report_artifact_recovery_action=report_artifact_recovery_action,
            report_artifact_recovery_confirmation=(
                report_artifact_recovery_confirmation
            ),
            report_artifact_provider_identifier=(
                report_artifact_provider_identifier
            ),
        )
    finally:
        # 所拥有运行时会在返回前关闭/取消其协调器；之后才可拆除会话级传输池。
        if owned_dual is not None:
            owned_dual.close()
        await shared_client.aclose()


async def _run_ashare_paper_day_owned(
    *,
    session_root: Path,
    session_date: date,
    latest_completed_session: date,
    future_trading_sessions: tuple[date, ...] = (),
    calendar_revision_sha256: str | None = None,
    account_id: str,
    initial_cash: Decimal,
    target_kind_value: str,
    target_id: str,
    base_url: str,
    access_token: str,
    started_at: datetime,
    recover_after_abort: bool,
    maximum_positions: int | None,
    risk_policy_change_confirmation: str | None,
    intraday_research: MacroResearchService | None,
    intraday_llm_config: IntradayLLMConfig | None,
    intraday_events_path: Path,
    deep_exit_assessment_provider: PaperDayDeepExitAssessmentProvider | None,
    report_artifact_recovery_action: str | None = None,
    report_artifact_recovery_confirmation: str | None = None,
    report_artifact_provider_identifier: str | None = None,
) -> dict[str, object]:
    """构造并管理阻塞式日内循环的每项可变依赖。"""

    from gribuki_trade.adapters.akshare import AKShareMarketDataAdapter
    from gribuki_trade.adapters.ashare_preopen_screening import (
        AKSharePreopenScreeningAdapter,
    )
    from gribuki_trade.adapters.ashare_surveillance import (
        AKShareAShareSurveillanceAdapter,
    )
    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.domain.paper_day import (
        PaperDayRunManifest,
        paper_day_target_hash,
    )
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.runtime import (
        PaperAccountChainError,
        SystemAwakeGuard,
        prepare_paper_day_ledger,
    )
    from gribuki_trade.services.ashare_intraday_llm_plans import (
        FrozenPITIntradayLLMPlanFactory,
        ReplayGuardedIntradayLLMCoordinator,
        build_preopen_context,
        load_frozen_pit_event_snapshot,
        replay_frozen_pit_event_snapshot,
    )
    from gribuki_trade.services.ashare_intraday_paper import IntradayPaperRiskConfig
    from gribuki_trade.services.ashare_paper import ASharePaperTradingService
    from gribuki_trade.services.ashare_paper_day import (
        ASharePaperDayConfig,
        ASharePaperDayRunner,
        PaperDayAbortRecoveryRequiredError,
        PaperDayEventPublisher,
        PaperDayRiskPolicyChangeError,
        intraday_llm_evidence_manifest_document,
        intraday_llm_manifest_document,
    )
    from gribuki_trade.services.ashare_preopen_screening import (
        ASharePreopenScreeningService,
    )
    from gribuki_trade.services.ashare_surveillance import (
        AShareIntradaySurveillanceService,
    )
    from gribuki_trade.services.notification_dispatch import (
        NotificationDispatchService,
    )
    from gribuki_trade.storage.outbox import SQLiteOutbox
    from gribuki_trade.storage.paper_day import (
        PaperDayStoreLeaseError,
        SQLitePaperDayStore,
    )
    from gribuki_trade.storage.paper_ledger import SQLitePaperLedger

    target_kind = NotificationTargetKind(target_kind_value)
    session_root.mkdir(parents=True, exist_ok=True)
    report_dir = (session_root / "reports").resolve()
    report_dir.mkdir(parents=True, exist_ok=True)
    status_path = session_root / "status.json"
    llm_enabled = intraday_llm_config is not None
    if llm_enabled != (intraday_research is not None):
        raise ValueError("intraday LLM runtime dependencies must be supplied together")
    config = ASharePaperDayConfig(
        initial_cash=initial_cash,
        intraday_llm_enabled=llm_enabled,
        intraday_llm_required_for_buy=llm_enabled,
        exit_plan_trading_sessions=future_trading_sessions,
        exit_plan_calendar_sha256=calendar_revision_sha256,
    )
    risk_config = IntradayPaperRiskConfig(
        initial_equity=initial_cash,
        maximum_positions=maximum_positions,
    )
    base_manifest_config = {
        **config.audit_document(),
        "calendar_provider": "BaoStock",
        "calendar_verified": True,
        "latest_completed_session": latest_completed_session.isoformat(),
        "notification_channel": "onebot",
        "notification_preflight_policy": "GET_STATUS_GOOD_AND_ONLINE",
        "notification_preflight_required": True,
        "notification_target_kind": target_kind.value,
        "intraday_risk_policy": risk_config.audit_document(),
    }
    target_hash = paper_day_target_hash(
        channel="onebot",
        target_kind=target_kind.value,
        target_id=target_id,
    )
    owner_id = f"paper-day-{uuid4().hex}"
    day_store_path = session_root / "journal.sqlite3"
    outbox_path = session_root / "outbox.sqlite3"
    ledger_path = session_root / "ledger.sqlite3"
    ledger_lineage: dict[str, object] | None = None

    notifier_config = OneBotConfig(
        access_token=access_token,
        base_url=base_url,
        private_target_ids=(
            frozenset({target_id}) if target_kind is NotificationTargetKind.PRIVATE else frozenset()
        ),
        group_target_ids=(
            frozenset({target_id}) if target_kind is NotificationTargetKind.GROUP else frozenset()
        ),
        artifact_root=report_dir,
    )

    # 将主机的正常显示策略与所有外部凭据都留在领域代码之外。进程级防护会在每条退出路径
    # 恢复 Windows 睡眠策略，并且绝不请求保持显示器唤醒。
    with SystemAwakeGuard():
        async with OneBotNotifier(notifier_config) as notifier:
            try:
                notification_status = await notifier.get_status()
            except asyncio.CancelledError:
                raise
            except Exception:
                raise _PaperDayCLIError("PAPER_DAY_NOTIFICATION_PREFLIGHT_FAILED") from None
            if not isinstance(notification_status, Mapping) or not (
                notification_status.get("good") is True
                and notification_status.get("online") is True
            ):
                raise _PaperDayCLIError("PAPER_DAY_NOTIFICATION_PREFLIGHT_FAILED")
            try:
                prepared_ledger = prepare_paper_day_ledger(
                    session_root,
                    session_date=session_date,
                    account_id=account_id,
                )
            except PaperAccountChainError:
                raise _PaperDayCLIError("PAPER_ACCOUNT_CONTINUITY_INVALID") from None
            ledger_path = prepared_ledger.ledger_path
            ledger_lineage = prepared_ledger.audit_document()
            base_manifest_config["paper_account_continuity"] = ledger_lineage
            with SQLitePaperDayStore(day_store_path) as day_store:
                scoped = tuple(
                    item
                    for item in day_store.list_runs()
                    if item.session_date == session_date and item.account_id == account_id
                )
                if len(scoped) > 1:
                    raise _PaperDayCLIError("PAPER_DAY_RUN_SCOPE_CONFLICT")
                retained = scoped[0] if scoped else None
                if retained is not None and retained.target_hash != target_hash:
                    raise _PaperDayCLIError("NOTIFICATION_TARGET_CONFLICT")

                if llm_enabled:
                    assert intraday_research is not None
                    assert intraday_llm_config is not None
                    retained_evidence_binding = (
                        None
                        if retained is None
                        else retained.config.get("intraday_llm_evidence_snapshot")
                    )
                    if retained is None:
                        evidence = load_frozen_pit_event_snapshot(
                            intraday_events_path,
                            as_of=started_at,
                        )
                        retained_factory_audit = None
                    elif isinstance(retained_evidence_binding, dict):
                        nested_audit = retained_evidence_binding.get("audit_document")
                        retained_factory_audit = (
                            cast(dict[str, object], nested_audit)
                            if isinstance(nested_audit, dict)
                            else cast(
                                dict[str, object],
                                retained_evidence_binding,
                            )
                        )
                        evidence = replay_frozen_pit_event_snapshot(
                            intraday_events_path,
                            retained_audit=retained_factory_audit,
                            fallback_as_of=started_at,
                        )
                    else:
                        raise _PaperDayCLIError("PAPER_DAY_RUNTIME_POLICY_CONFLICT")
                    intraday_llm = ReplayGuardedIntradayLLMCoordinator(
                        intraday_research,
                        config=intraday_llm_config,
                        journal_restore_allowed=evidence.available,
                    )
                    intraday_plan_factory = FrozenPITIntradayLLMPlanFactory(
                        intraday_research,
                        evidence,
                        retained_audit=retained_factory_audit,
                    )
                    manifest_evidence_audit = intraday_llm_evidence_manifest_document(
                        intraday_plan_factory
                    )
                else:
                    path_sha256 = hashlib.sha256(
                        str(intraday_events_path.resolve()).encode("utf-8")
                    ).hexdigest()
                    retained_disabled_evidence = (
                        None
                        if retained is None
                        else retained.config.get("intraday_llm_evidence_snapshot")
                    )
                    manifest_evidence_audit = (
                        dict(retained_disabled_evidence)
                        if isinstance(retained_disabled_evidence, dict)
                        else {
                            "as_of": started_at.astimezone(UTC),
                            "database_path_sha256": path_sha256,
                            "event_count": 0,
                            "failure_code": "INTRADAY_LLM_OPERATOR_DISABLED",
                            "operator_opt_out": True,
                            "source": "OPERATOR_CLI",
                            "status": "DISABLED",
                        }
                    )
                    evidence = None
                    intraday_llm = None
                    intraday_plan_factory = None

                manifest_config = {
                    **base_manifest_config,
                    "intraday_llm_policy": intraday_llm_manifest_document(intraday_llm),
                    "intraday_llm_evidence_snapshot": manifest_evidence_audit,
                }
                proposed = PaperDayRunManifest.create(
                    session_date=session_date,
                    account_id=account_id,
                    config=manifest_config,
                    created_at=started_at,
                    target_hash=target_hash,
                    initial_cash=initial_cash,
                )
                created_new_run = retained is None
                if retained is None:
                    day_store.create_run(proposed)
                    manifest = proposed
                else:
                    if (
                        retained.initial_cash != initial_cash
                        or not _paper_day_resume_config_compatible(
                            retained=retained.config,
                            proposed=proposed.config,
                            intraday_llm_enabled=llm_enabled,
                        )
                    ):
                        raise _PaperDayCLIError("PAPER_DAY_RUNTIME_POLICY_CONFLICT")
                    manifest = retained

                llm_preopen_context = None
                retained_event_types = {
                    item.event_type for item in day_store.events(manifest.run_id)
                }
                preopen_already_decided = bool(
                    retained_event_types
                    & {
                        "LLM_PREOPEN_CONTEXT_FROZEN",
                        "LLM_PREOPEN_CONTEXT_FAILED",
                    }
                )
                if (
                    intraday_llm is not None
                    and intraday_plan_factory is not None
                    and intraday_research is not None
                    and created_new_run
                    and not preopen_already_decided
                    and evidence is not None
                    and evidence.available
                ):
                    market_open_at = datetime.combine(
                        session_date,
                        config.market_open,
                        tzinfo=ZoneInfo("Asia/Shanghai"),
                    ).astimezone(UTC)
                    if started_at.astimezone(UTC) < market_open_at:
                        try:
                            preopen_plan = intraday_plan_factory.prepare_preopen(
                                session_date=session_date
                            )
                            if preopen_plan.eligible_for_analysis:
                                preopen_run = await intraday_research.execute(preopen_plan)
                                known_at = max(
                                    datetime.now(UTC),
                                    started_at.astimezone(UTC),
                                )
                                if known_at < market_open_at:
                                    valid_until = datetime.combine(
                                        session_date,
                                        config.finalization_time,
                                        tzinfo=ZoneInfo("Asia/Shanghai"),
                                    ).astimezone(UTC)
                                    llm_preopen_context = build_preopen_context(
                                        preopen_plan,
                                        preopen_run,
                                        session_date=session_date,
                                        known_at=known_at,
                                        valid_until=valid_until,
                                    )
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            # 供应商与证据细节保持脱敏。运行器会记录缺失上下文，并且只关闭
                            # 新买入，绝不关闭监控或卖出。
                            llm_preopen_context = None
                with (
                    SQLiteOutbox(outbox_path) as outbox,
                    SQLitePaperLedger(ledger_path) as ledger,
                ):
                    paper = ASharePaperTradingService(ledger)
                    dispatcher = NotificationDispatchService(
                        outbox,
                        {notifier.channel: notifier},
                        target_kind=target_kind,
                        target_id=target_id,
                    )
                    publisher = PaperDayEventPublisher(
                        manifest=manifest,
                        store=day_store,
                        outbox=outbox,
                        dispatcher=dispatcher,
                        target_kind=target_kind,
                        target_id=target_id,
                        owner_id=owner_id,
                        clock=lambda: datetime.now(UTC),
                        status_path=status_path,
                    )
                    preopen = ASharePreopenScreeningService(
                        AKSharePreopenScreeningAdapter(
                            latest_completed_session,
                            timeout_seconds=35.0,
                            history_timeout_seconds=18.0,
                            history_concurrency=4,
                        )
                    )
                    surveillance = AShareIntradaySurveillanceService(
                        AKShareAShareSurveillanceAdapter(
                            timeout_seconds=35.0,
                        )
                    )
                    market_data = AKShareMarketDataAdapter(
                        timeout_seconds=10.0,
                        max_attempts=1,
                        intraday_stale_after_seconds=180.0,
                    )
                    runner = ASharePaperDayRunner(
                        manifest=manifest,
                        latest_completed_session=latest_completed_session,
                        owner_id=owner_id,
                        store=day_store,
                        publisher=publisher,
                        preopen_screening=preopen,
                        surveillance=surveillance,
                        market_data=market_data,
                        paper=paper,
                        outbox=outbox,
                        report_dir=report_dir,
                        config=config,
                        risk_config=risk_config,
                        intraday_llm=intraday_llm,
                        intraday_llm_plan_factory=cast(
                            "PaperDayIntradayLLMPlanFactory | None",
                            intraday_plan_factory,
                        ),
                        llm_preopen_context=llm_preopen_context,
                        deep_exit_assessment_provider=deep_exit_assessment_provider,
                        artifact_notifier=notifier,
                        artifact_target_kind=target_kind,
                        artifact_target_id=target_id,
                        report_artifact_recovery_action=(
                            report_artifact_recovery_action
                        ),
                        report_artifact_recovery_confirmation=(
                            report_artifact_recovery_confirmation
                        ),
                        report_artifact_provider_identifier=(
                            report_artifact_provider_identifier
                        ),
                        preopen_recovery_path=(session_root / "preopen-recovery-seed.json"),
                        recover_after_abort=recover_after_abort,
                        risk_policy_change_confirmation=(risk_policy_change_confirmation),
                    )
                    try:
                        result = await runner.run()
                    except PaperDayAbortRecoveryRequiredError:
                        raise _PaperDayCLIError("DAY_ABORTED_OPERATOR_RECOVERY_REQUIRED") from None
                    except PaperDayRiskPolicyChangeError as error:
                        raise _PaperDayCLIError(error.code) from None
                    except PaperDayStoreLeaseError:
                        raise _PaperDayCLIError("PAPER_DAY_WRITER_LEASE_UNAVAILABLE") from None
                    finally:
                        if intraday_llm is not None:
                            await intraday_llm.close()
    delivery_projection = _paper_day_delivery_projection(result)
    return {
        **delivery_projection,
        "action": "run",
        "run_id": result.run_id,
        "session_date": result.session_date.isoformat(),
        "completed": result.completed,
        "event_count": result.event_count,
        "account": _paper_snapshot_json(result.final_snapshot),
        "report_path": str(result.report_path),
        "runtime_dir": str(session_root),
        "execution_mode": "PAPER_ONLY_NO_BROKER",
        "system_awake": "PROCESS_SCOPED_SYSTEM_ONLY",
        "intraday_llm": {
            "enabled": llm_enabled,
            "required_for_buy": llm_enabled,
            "runtime_evidence_failure_code": (None if evidence is None else evidence.failure_code),
            "runtime_evidence_status": ("DISABLED" if evidence is None else evidence.status),
        },
        "paper_account_continuity": ledger_lineage,
    }


def _paper_day_resume_config_compatible(
    *,
    retained: Mapping[str, object],
    proposed: Mapping[str, object],
    intraday_llm_enabled: bool,
) -> bool:
    """比较不可变运行时策略，同时委托风险迁移。"""

    from gribuki_trade.services.ashare_paper_day import (
        intraday_llm_manifest_compatible,
    )

    old = dict(retained)
    new = dict(proposed)
    old.pop("intraday_risk_policy", None)
    new.pop("intraday_risk_policy", None)
    old_policy = old.pop("intraday_llm_policy", None)
    new_policy = new.pop("intraday_llm_policy", None)
    old_evidence = old.pop("intraday_llm_evidence_snapshot", None)
    new_evidence = new.pop("intraday_llm_evidence_snapshot", None)
    old_continuity = old.pop("paper_account_continuity", None)
    new_continuity = new.pop("paper_account_continuity", None)
    if old_continuity is None:
        # 引入跨交易日血缘前创建的运行已经拥有日期本地账本。它们只能通过由同一不可变
        # 数据库合成的显式旧版来源恢复。
        if not (
            isinstance(new_continuity, dict)
            and new_continuity.get("origin")
            in {
                "LEGACY_SESSION_LOCAL",
                "LEGACY_EMPTY_SESSION_LEDGER",
                "NEW_ACCOUNT",
            }
        ):
            return False
    elif old_continuity != new_continuity:
        return False
    if intraday_llm_enabled:
        if (
            not isinstance(new_policy, Mapping)
            or not intraday_llm_manifest_compatible(old_policy, new_policy)
            or old_evidence != new_evidence
        ):
            return False
    else:
        if old_policy is not None and (
            not isinstance(new_policy, Mapping)
            or not intraday_llm_manifest_compatible(old_policy, new_policy)
        ):
            return False
        if old_evidence is not None and old_evidence != new_evidence:
            return False
        # LLM 门控出现前创建的运行隐式将两个值都设为 false。显式选择退出时可以恢复这些
        # 不可变日志；运行器会在恢复检查后追加可审计的运维禁用事件，且不重写旧版清单。
        for key in (
            "intraday_llm_enabled",
            "intraday_llm_required_for_buy",
        ):
            if key not in old and new.get(key) is False:
                new.pop(key, None)
    return old == new


def _ashare_paper_day_status(
    session_root: Path,
    session_date: date,
) -> dict[str, object]:
    status_path = session_root / "status.json"
    if not status_path.is_file():
        return _ashare_paper_day_error(
            "status",
            session_date,
            session_root,
            "STATUS_NOT_AVAILABLE",
        )
    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return _ashare_paper_day_error(
            "status",
            session_date,
            session_root,
            "STATUS_FILE_INVALID",
        )
    if not isinstance(payload, dict):
        return _ashare_paper_day_error(
            "status",
            session_date,
            session_root,
            "STATUS_FILE_INVALID",
        )
    delivery = _paper_day_delivery_sidecar_projection(session_root, payload)
    return {
        "ok": True,
        "action": "status",
        "artifact_delivery_status": delivery["artifact_delivery_status"],
        "daily_review_delivery_complete": delivery[
            "daily_review_delivery_complete"
        ],
        "delivery": delivery,
        "operationally_complete": delivery["daily_review_delivery_complete"],
        "read_only_sidecar": True,
        "runtime_dir": str(session_root),
        "status_path": str(status_path),
        "status": payload,
    }


def _paper_day_delivery_sidecar_projection(
    session_root: Path,
    status: Mapping[str, object],
) -> dict[str, object]:
    """只读投影 PAPER 日报的文本与附件交付状态，不打开 SQLite。"""

    allowed_statuses = {"PENDING", "SENT", "AMBIGUOUS", "NOT_CONFIGURED"}
    artifact_status = status.get("artifact_delivery_status")
    if isinstance(artifact_status, str) and artifact_status in allowed_statuses:
        artifact_complete = status.get("artifact_delivery_complete") is True
        daily_complete = status.get("daily_review_delivery_complete") is True
        return {
            "artifact_delivery_complete": artifact_complete,
            "artifact_delivery_status": artifact_status,
            "daily_review_delivery_complete": daily_complete,
            "notification_gaps": _non_negative_int_or_none(
                status.get("notification_gaps")
            ),
            "notification_required": _non_negative_int_or_none(
                status.get("notification_required")
            ),
            "notification_sent": _non_negative_int_or_none(
                status.get("notification_sent")
            ),
            "projection_exact": all(
                (
                    isinstance(status.get("artifact_delivery_complete"), bool),
                    isinstance(status.get("daily_review_delivery_complete"), bool),
                    _non_negative_int_or_none(status.get("notification_required"))
                    is not None,
                    _non_negative_int_or_none(status.get("notification_sent"))
                    is not None,
                    _non_negative_int_or_none(status.get("notification_gaps"))
                    is not None,
                    _non_negative_int_or_none(status.get("text_notification_required"))
                    is not None,
                    _non_negative_int_or_none(status.get("text_notification_sent"))
                    is not None,
                    _non_negative_int_or_none(status.get("text_notification_gaps"))
                    is not None,
                )
            ),
            "projection_source": "status.json",
            "text_notification_gaps": _non_negative_int_or_none(
                status.get("text_notification_gaps")
            ),
            "text_notification_required": _non_negative_int_or_none(
                status.get("text_notification_required")
            ),
            "text_notification_sent": _non_negative_int_or_none(
                status.get("text_notification_sent")
            ),
        }

    event_status = _latest_paper_day_artifact_event_status(
        session_root / "session.log.jsonl"
    )
    return {
        "artifact_delivery_complete": event_status == "SENT",
        "artifact_delivery_status": event_status,
        "daily_review_delivery_complete": False,
        "notification_gaps": None,
        "notification_required": None,
        "notification_sent": None,
        "projection_exact": False,
        "projection_source": (
            "session.log.jsonl" if event_status != "NOT_REPORTED" else "unavailable"
        ),
        "text_notification_gaps": None,
        "text_notification_required": None,
        "text_notification_sent": None,
    }


def _latest_paper_day_artifact_event_status(event_log_path: Path) -> str:
    """从追加式 sidecar 中读取最后一个完整附件状态；未知时失败关闭。"""

    try:
        lines = event_log_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return "NOT_REPORTED"
    event_status = "NOT_REPORTED"
    by_type = {
        "REPORT_ARTIFACT_DELIVERY_AMBIGUOUS": "AMBIGUOUS",
        "REPORT_ARTIFACT_DELIVERY_NOT_CONFIGURED": "NOT_CONFIGURED",
        "REPORT_ARTIFACT_DELIVERY_PENDING": "PENDING",
        "REPORT_ARTIFACT_DELIVERY_SENT": "SENT",
        "REPORT_ARTIFACT_LEGACY_FAILURE_AMBIGUOUS": "AMBIGUOUS",
    }
    for line in lines:
        if not line.strip():
            continue
        try:
            document = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(document, dict):
            continue
        event_type = document.get("event_type")
        if isinstance(event_type, str) and event_type in by_type:
            event_status = by_type[event_type]
            continue
        if event_type == "REPORT_UPLOADED":
            payload = document.get("payload")
            if isinstance(payload, dict) and payload.get("delivered") is True:
                event_status = "SENT"
        elif event_type == "REPORT_UPLOAD_FAILED":
            event_status = "AMBIGUOUS"
    return event_status


def _non_negative_int_or_none(value: object) -> int | None:
    """只接受不是布尔值的非负整数。"""

    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _ashare_paper_day_report(
    session_root: Path,
    session_date: date,
) -> dict[str, object]:
    report_dir = session_root / "reports"
    reports = (
        tuple(
            sorted(
                report_dir.glob(f"ashare-paper-day-{session_date.isoformat()}-*.md"),
                key=lambda item: item.name,
            )
        )
        if report_dir.is_dir()
        else ()
    )
    if not reports:
        return _ashare_paper_day_error(
            "report",
            session_date,
            session_root,
            "REPORT_NOT_AVAILABLE",
        )
    report_path = reports[-1].resolve()
    try:
        content = report_path.read_bytes()
        text_content = content.decode("utf-8")
        modified_at = datetime.fromtimestamp(report_path.stat().st_mtime, UTC)
    except (OSError, UnicodeError):
        return _ashare_paper_day_error(
            "report",
            session_date,
            session_root,
            "REPORT_FILE_INVALID",
        )
    return {
        "ok": True,
        "action": "report",
        "read_only_sidecar": True,
        "runtime_dir": str(session_root),
        "report": {
            "path": str(report_path),
            "name": report_path.name,
            "bytes": len(content),
            "line_count": len(text_content.splitlines()),
            "modified_at": modified_at.isoformat(),
            "sha256": hashlib.sha256(content).hexdigest(),
        },
    }


def _ashare_paper_day_summary(
    session_root: Path,
    session_date: date,
) -> dict[str, object]:
    """从伴随文件重新生成增强报告，且不打开 SQLite。"""

    from gribuki_trade.reporting.paper_day_summary import (
        PaperDaySidecarError,
        project_paper_day_sidecars,
        write_paper_day_summary,
    )

    try:
        projection = project_paper_day_sidecars(session_root)
    except PaperDaySidecarError as error:
        return _ashare_paper_day_error(
            "summary",
            session_date,
            session_root,
            error.code,
        )
    if projection.session_date != session_date:
        return _ashare_paper_day_error(
            "summary",
            session_date,
            session_root,
            "SUMMARY_SESSION_CONFLICT",
        )
    try:
        summary_path = write_paper_day_summary(projection)
        content = summary_path.read_bytes()
        modified_at = datetime.fromtimestamp(summary_path.stat().st_mtime, UTC)
    except OSError:
        return _ashare_paper_day_error(
            "summary",
            session_date,
            session_root,
            "SUMMARY_WRITE_FAILED",
        )
    return {
        "ok": True,
        "action": "summary",
        "sidecar_only": True,
        "sqlite_opened": False,
        "runtime_dir": str(session_root),
        "summary": {
            "path": str(summary_path),
            "name": summary_path.name,
            "bytes": len(content),
            "line_count": len(content.decode("utf-8").splitlines()),
            "modified_at": modified_at.isoformat(),
            "sha256": hashlib.sha256(content).hexdigest(),
            "lifecycle": projection.lifecycle,
            "coverage": projection.coverage,
            "event_count": projection.sidecar_event_count,
            "warning_count": len(projection.warnings),
        },
    }


class _PostCloseCLIError(RuntimeError):
    """不含供应商或凭据细节的稳定盘后 CLI 失败。"""

    def __init__(self, code: str, *, retryable: bool = False) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(f"A-share post-close command failed ({code})")


class _FixedPostCloseSessions:
    """在单次运行中复用一份已经核验的日历结果。"""

    def __init__(self, sessions: CloseSessionResolution) -> None:
        self._sessions = sessions

    async def resolve(self, now: datetime) -> CloseSessionResolution:
        del now
        return self._sessions


class _CLIExistingCloseResearch:
    """将生产收盘研究辅助器适配到 PAPER 持仓。"""

    def __init__(
        self,
        *,
        profiles: Mapping[str, ResearchInstrumentProfile],
        history_days: int,
        news_runtime_dir: str,
        research_db: str,
        market_evidence_dir: str,
        analysis_outbox_db: str,
        news_feeds: Sequence[str] | None,
        refresh_news: bool,
        search_discovery: bool,
        searxng_url: str | None,
        macro_enabled: bool,
        macro_provider: str,
        model: str | None,
        macro_weight: Decimal,
    ) -> None:
        self._profiles = dict(profiles)
        self._history_days = history_days
        self._news_runtime_dir = news_runtime_dir
        self._research_db = research_db
        self._market_evidence_dir = market_evidence_dir
        self._analysis_outbox_db = analysis_outbox_db
        self._news_feeds = news_feeds
        self._refresh_news = refresh_news
        self._search_discovery = search_discovery
        self._searxng_url = searxng_url
        self._macro_enabled = macro_enabled
        self._macro_provider = macro_provider
        self._model = model
        self._macro_weight = macro_weight
        # 除日线适配器外，AKShare 还包含嵌入式 V8 路由。生产进程中必须串行执行每个持仓的
        # 完整研究调用；更窄的供应商锁不足以保证安全。
        self._research_lock = asyncio.Lock()

    async def research(
        self,
        position: PaperPosition,
        *,
        sessions: CloseSessionResolution,
    ) -> PostCloseInstrumentResearch:
        async with self._research_lock:
            return await self._research_serial(position, sessions=sessions)

    async def _research_serial(
        self,
        position: PaperPosition,
        *,
        sessions: CloseSessionResolution,
    ) -> PostCloseInstrumentResearch:
        from gribuki_trade.domain.post_close import (
            PostCloseInstrumentResearch,
            PostCloseResearchStatus,
        )

        profile = self._profiles.get(position.symbol)
        if profile is None:
            return PostCloseInstrumentResearch(
                symbol=position.symbol,
                status=PostCloseResearchStatus.FAILED,
                failure_code="PAPER_PROFILE_NOT_AVAILABLE",
            )
        try:
            result = await _ashare_close_research_once(
                position.symbol,
                self._history_days,
                sessions.latest_completed_session,
                sessions.next_session,
                self._news_runtime_dir,
                self._research_db,
                self._market_evidence_dir,
                self._analysis_outbox_db,
                self._news_feeds,
                self._refresh_news,
                self._search_discovery,
                self._searxng_url,
                self._macro_enabled,
                self._macro_provider,
                self._model,
                self._macro_weight,
                True,
                None,
                None,
                None,
                profile,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return PostCloseInstrumentResearch(
                symbol=position.symbol,
                status=PostCloseResearchStatus.FAILED,
                failure_code="CLOSE_RESEARCH_FAILED",
            )
        if result.get("ok") is not True:
            code = result.get("error_code")
            return PostCloseInstrumentResearch(
                symbol=position.symbol,
                status=PostCloseResearchStatus.FAILED,
                failure_code=(
                    str(code) if isinstance(code, str) and code.strip() else "CLOSE_RESEARCH_FAILED"
                ),
            )
        decision = result.get("decision")
        if not isinstance(decision, str) or not decision.strip():
            return PostCloseInstrumentResearch(
                symbol=position.symbol,
                status=PostCloseResearchStatus.FAILED,
                failure_code="CLOSE_RESEARCH_RESULT_INVALID",
            )
        dual_track = _post_close_mapping(result.get("macro_dual_track"))
        baseline_track = _post_close_mapping(dual_track.get("baseline"))
        adversarial_track = _post_close_mapping(dual_track.get("adversarial"))
        return PostCloseInstrumentResearch(
            symbol=position.symbol,
            status=PostCloseResearchStatus.COMPLETED,
            decision=decision,
            technical_score=_post_close_optional_decimal(result.get("technical_score")),
            reference_price=_post_close_optional_decimal(result.get("reference_price")),
            invalidation_price=_post_close_optional_decimal(result.get("invalidation_price")),
            reason_codes=_post_close_string_tuple(result.get("reason_codes")),
            uncertainties=_post_close_string_tuple(result.get("uncertainties")),
            daily_bar_count=_post_close_optional_integer(result.get("daily_bar_count")),
            technical_decision=_post_close_optional_string(result.get("technical_decision")),
            combined_score=_post_close_optional_decimal(result.get("combined_score")),
            macro_score=_post_close_optional_decimal(result.get("macro_score")),
            macro_evidence_coverage=_post_close_optional_decimal(
                result.get("macro_evidence_coverage")
            ),
            macro_provider=_post_close_optional_string(result.get("macro_provider")),
            macro_model=_post_close_optional_string(result.get("macro_model")),
            macro_failure_code=_post_close_optional_string(result.get("macro_failure_code")),
            market_data_failure_code=_post_close_optional_string(
                result.get("market_data_failure_code")
            ),
            macro_analysis_id=_post_close_optional_string(dual_track.get("analysis_id")),
            macro_selected_track=_post_close_optional_string(dual_track.get("selected_track")),
            macro_audit_record_sha256=_post_close_optional_string(
                dual_track.get("audit_record_sha256")
            ),
            baseline_macro_decision=_post_close_optional_string(baseline_track.get("decision")),
            baseline_macro_regime=_post_close_optional_string(baseline_track.get("regime")),
            baseline_macro_score=_post_close_optional_decimal(baseline_track.get("macro_impact")),
            baseline_macro_evidence_coverage=_post_close_optional_decimal(
                baseline_track.get("evidence_coverage")
            ),
            baseline_macro_model=_post_close_optional_string(baseline_track.get("model_version")),
            adversarial_macro_decision=_post_close_optional_string(
                adversarial_track.get("decision")
            ),
            adversarial_macro_regime=_post_close_optional_string(adversarial_track.get("regime")),
            adversarial_macro_score=_post_close_optional_decimal(
                adversarial_track.get("macro_impact")
            ),
            adversarial_macro_evidence_coverage=_post_close_optional_decimal(
                adversarial_track.get("evidence_coverage")
            ),
            adversarial_macro_model=_post_close_optional_string(
                adversarial_track.get("model_version")
            ),
        )


def _post_close_optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        resolved = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return resolved if resolved.is_finite() else None


def _post_close_optional_integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        resolved = int(str(value))
    except (TypeError, ValueError):
        return None
    return resolved if resolved >= 0 else None


def _post_close_optional_string(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _post_close_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _post_close_string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(item for item in value if isinstance(item, str) and item.strip())


def _post_close_analysis_outcome(
    *,
    held_count: int,
    research_completed: int,
    research_failed: int,
) -> str:
    if min(held_count, research_completed, research_failed) < 0:
        raise _PostCloseCLIError("POST_CLOSE_RESEARCH_RESULT_INVALID")
    if research_completed + research_failed != held_count:
        raise _PostCloseCLIError("POST_CLOSE_RESEARCH_RESULT_INVALID")
    if held_count == 0:
        return "NOT_APPLICABLE"
    if research_completed == 0:
        return "FAILED"
    return "PARTIAL" if research_failed else "COMPLETE"


def _paper_session_instrument_profiles(
    projection: PaperDayExecutiveProjection,
    instrument_types: Mapping[str, str],
) -> dict[str, ResearchInstrumentProfile]:
    """只构建在不可变 PAPER 伴随文件中明确归档的档案。"""

    names: dict[str, set[str]] = {}
    boards: dict[str, set[str]] = {}
    archived_instrument_types: dict[str, set[str]] = {}
    for event in projection.events:
        payload = event.payload
        collections: list[object] = []
        if event.event_type in {
            "PREOPEN_SCREEN_COMPLETED",
            "PREOPEN_SCREEN_RECOVERED",
            "SURVEILLANCE_SCAN_COMPLETED",
        }:
            collections.append(payload.get("candidates"))
        elif event.event_type == "WATCHLIST_UPDATED":
            collections.append(payload.get("watchlist"))
        elif event.event_type == "FILL_STARTED":
            raw_fill = payload.get("fill")
            if isinstance(raw_fill, dict):
                raw_symbol = raw_fill.get("symbol")
                raw_instrument_type = raw_fill.get("instrument_type")
                if isinstance(raw_symbol, str) and isinstance(raw_instrument_type, str):
                    canonical = raw_symbol.strip().upper()
                    archived_type = raw_instrument_type.strip().lower()
                    if canonical and archived_type in {"stock", "etf"}:
                        archived_instrument_types.setdefault(canonical, set()).add(archived_type)
        for collection in collections:
            if not isinstance(collection, list):
                continue
            for raw in collection:
                if not isinstance(raw, dict):
                    continue
                symbol = raw.get("symbol")
                if not isinstance(symbol, str):
                    continue
                canonical = symbol.strip().upper()
                if not canonical:
                    continue
                name = raw.get("name")
                board = raw.get("board")
                if isinstance(name, str) and name.strip():
                    names.setdefault(canonical, set()).add(name.strip())
                if isinstance(board, str) and board.strip():
                    boards.setdefault(canonical, set()).add(board.strip().upper())

    profiles: dict[str, ResearchInstrumentProfile] = {}
    board_values = {
        "SSE_MAIN": ("sse", "sse_main"),
        "SZSE_MAIN": ("szse", "szse_main"),
        "CHINEXT": ("szse", "chinext"),
        "STAR": ("sse", "star"),
        "BSE": ("bse", "bse"),
    }
    for symbol in sorted(set(names) | set(boards)):
        retained_names = names.get(symbol, set())
        retained_boards = boards.get(symbol, set())
        if len(retained_names) != 1 or len(retained_boards) != 1:
            continue
        board = next(iter(retained_boards))
        exchange_and_board = board_values.get(board)
        if exchange_and_board is None:
            continue
        asset_type = instrument_types.get(symbol)
        if asset_type not in {"stock", "etf"}:
            continue
        archived_types = archived_instrument_types.get(symbol, set())
        if len(archived_types) > 1 or (archived_types and archived_types != {asset_type}):
            continue
        profiles[symbol] = ResearchInstrumentProfile(
            symbol=symbol,
            name=next(iter(retained_names)),
            market="A-share",
            asset_type=asset_type,
            exchange=exchange_and_board[0],
            board=exchange_and_board[1],
            size_tier="unknown-not-provided",
            industry="unknown-not-provided",
            styles=("paper-session-archive", "degraded-profile"),
            research_role="held-position-post-close-review",
            risk_tags=(
                "DEGRADED_PROFILE",
                "INDUSTRY_NOT_PROVIDED",
                "SIZE_NOT_PROVIDED",
            ),
            source_id="PAPER_SESSION_ARCHIVE",
            verified_on=projection.session_date,
            background_facts=(
                f"PAPER archived name: {next(iter(retained_names))}",
                f"PAPER archived board: {board}",
                "Industry and size were not provided by the PAPER session archive.",
            ),
        )
    return profiles


@contextmanager
def _post_close_process_lock(path: Path) -> Iterator[None]:
    """获取进程级非阻塞锁，并在进程终止时释放。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    try:
        if stream.seek(0, os.SEEK_END) == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover - Windows 下采用原子替换的兼容分支
                import fcntl

                fcntl.flock(  # type: ignore[attr-defined]
                    stream.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,  # type: ignore[attr-defined]
                )
        except (OSError, BlockingIOError):
            raise _PostCloseCLIError("POST_CLOSE_ALREADY_RUNNING", retryable=True) from None
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:  # pragma: no cover
                fcntl.flock(  # type: ignore[attr-defined]
                    stream.fileno(),
                    fcntl.LOCK_UN,  # type: ignore[attr-defined]
                )
    finally:
        stream.close()


def _read_post_close_json(path: Path, code: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise _PostCloseCLIError(code) from None
    if not isinstance(value, dict):
        raise _PostCloseCLIError(code)
    return cast(dict[str, object], value)


def _post_close_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _append_post_close_audit(
    path: Path,
    *,
    event: str,
    run_id: str,
    target_hash: str,
    details: Mapping[str, object] | None = None,
) -> None:
    document = {
        "event": event,
        "occurred_at": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "target_hash": target_hash,
        **dict(details or {}),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(document, ensure_ascii=False, sort_keys=True, allow_nan=False))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


async def _ashare_post_close(
    action: str,
    runtime_dir: str,
    session_date: date | None,
    account_id: str,
    target_kind_value: str | None,
    target_id: str | None,
    base_url: str,
    candidate_db: str,
    history_days: int,
    news_runtime_dir: str,
    research_db: str,
    market_evidence_dir: str,
    news_feeds: Sequence[str] | None,
    refresh_news: bool,
    search_discovery: bool,
    searxng_url: str | None,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    macro_weight: Decimal,
    dispatch_cycles: int,
    dispatch_poll_interval: float,
    confirmation: str | None,
    recover_analysis: bool = False,
    recover_delivery: bool = False,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """一次幂等盘后复核与交付的生产边界。"""

    if action not in {"run", "status", "report"}:
        raise ValueError("unsupported A-share post-close action")
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None or resolved_now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    local_today = resolved_now.astimezone(ZoneInfo("Asia/Shanghai")).date()
    resolved_session = session_date or local_today
    session_root = Path(runtime_dir).resolve() / resolved_session.isoformat()
    post_root = session_root / "post-close"

    if action == "status":
        return _ashare_post_close_status(post_root, resolved_session)
    if action == "report":
        return _ashare_post_close_report(post_root, resolved_session)
    if confirmation != "POST_CLOSE":
        return _post_close_error(
            action,
            resolved_session,
            post_root,
            "POST_CLOSE_CONFIRMATION_REQUIRED",
        )
    if target_kind_value is None or target_id is None or not target_id.strip():
        return _post_close_error(
            action,
            resolved_session,
            post_root,
            "NOTIFICATION_TARGET_REQUIRED",
        )
    try:
        return await _ashare_post_close_run(
            session_root=session_root,
            session_date=resolved_session,
            account_id=account_id,
            target_kind_value=target_kind_value,
            target_id=target_id.strip(),
            base_url=base_url,
            candidate_db=candidate_db,
            history_days=history_days,
            news_runtime_dir=news_runtime_dir,
            research_db=research_db,
            market_evidence_dir=market_evidence_dir,
            news_feeds=news_feeds,
            refresh_news=refresh_news,
            search_discovery=search_discovery,
            searxng_url=searxng_url,
            macro_enabled=macro_enabled,
            macro_provider=macro_provider,
            model=model,
            macro_weight=macro_weight,
            dispatch_cycles=dispatch_cycles,
            dispatch_poll_interval=dispatch_poll_interval,
            recover_analysis=recover_analysis,
            recover_delivery=recover_delivery,
            now=resolved_now,
        )
    except asyncio.CancelledError:
        raise
    except _PostCloseCLIError as error:
        return _post_close_error(
            action,
            resolved_session,
            post_root,
            error.code,
            retryable=error.retryable,
        )
    except Exception:
        return _post_close_error(
            action,
            resolved_session,
            post_root,
            "POST_CLOSE_RUN_FAILED",
            retryable=True,
        )


async def _ashare_post_close_run(
    *,
    session_root: Path,
    session_date: date,
    account_id: str,
    target_kind_value: str,
    target_id: str,
    base_url: str,
    candidate_db: str,
    history_days: int,
    news_runtime_dir: str,
    research_db: str,
    market_evidence_dir: str,
    news_feeds: Sequence[str] | None,
    refresh_news: bool,
    search_discovery: bool,
    searxng_url: str | None,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    macro_weight: Decimal,
    dispatch_cycles: int,
    dispatch_poll_interval: float,
    recover_analysis: bool,
    recover_delivery: bool,
    now: datetime,
) -> dict[str, object]:
    try:
        validated_searxng_url = validate_post_close_searxng_url(searxng_url)
    except PostCloseSearxngURLValidationError:
        raise _PostCloseCLIError("SEARXNG_URL_INVALID") from None

    from gribuki_trade.adapters.baostock import BaoStockDailyAdapter
    from gribuki_trade.domain.paper_day import paper_day_target_hash
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.reporting.paper_day_summary import (
        PaperDaySidecarError,
        project_paper_day_sidecars,
    )
    from gribuki_trade.services.ashare_close_sessions import (
        AShareCloseSessionResolver,
        CloseAnalysisMode,
        CloseSessionResolutionError,
    )
    from gribuki_trade.services.ashare_paper import ASharePaperTradingService
    from gribuki_trade.services.ashare_post_close import PostCloseOrchestrationError
    from gribuki_trade.storage.candidate_store import SQLiteCandidateStore
    from gribuki_trade.storage.paper_day import SQLitePaperDayStore
    from gribuki_trade.storage.paper_ledger import SQLitePaperLedger

    local_now = now.astimezone(ZoneInfo("Asia/Shanghai"))
    if session_date != local_now.date():
        raise _PostCloseCLIError("SESSION_DATE_NOT_TODAY")
    if local_now.time().replace(tzinfo=None) < datetime_time(15, 5):
        return _post_close_skipped(session_date, session_root / "post-close", "BEFORE_1505")
    if not account_id.strip():
        raise _PostCloseCLIError("ACCOUNT_ID_INVALID")
    if dispatch_cycles < 1 or not 0 <= dispatch_poll_interval < float("inf"):
        raise ValueError("dispatch policy is invalid")

    target_kind = NotificationTargetKind(target_kind_value)
    _validate_post_close_target(base_url, target_kind, target_id)
    calendar = BaoStockDailyAdapter(max_attempts=3, timeout_seconds=20.0)
    try:
        sessions = await AShareCloseSessionResolver(calendar).resolve(now)
    except CloseSessionResolutionError as error:
        if error.code == "MARKET_SESSION_NOT_CLOSED":
            return _post_close_skipped(session_date, session_root / "post-close", error.code)
        raise _PostCloseCLIError(error.code, retryable=True) from None
    if (
        sessions.analysis_mode is not CloseAnalysisMode.POST_CLOSE
        or sessions.latest_completed_session != session_date
    ):
        return _post_close_skipped(
            session_date,
            session_root / "post-close",
            "CURRENT_DATE_NOT_TRADING_SESSION",
        )

    try:
        projection = project_paper_day_sidecars(session_root)
    except PaperDaySidecarError as error:
        raise _PostCloseCLIError(error.code, retryable=True) from None
    if projection.session_date != session_date or projection.lifecycle != "COMPLETED":
        raise _PostCloseCLIError("PAPER_DAY_NOT_COMPLETED", retryable=True)

    target_hash = paper_day_target_hash(
        channel="onebot",
        target_kind=target_kind.value,
        target_id=target_id,
    )
    journal_path = session_root / "journal.sqlite3"
    if not journal_path.is_file():
        raise _PostCloseCLIError("PAPER_DAY_JOURNAL_NOT_AVAILABLE", retryable=True)
    with SQLitePaperDayStore(journal_path) as day_store:
        scoped = tuple(
            run
            for run in day_store.list_runs()
            if run.session_date == session_date and run.account_id == account_id.strip()
        )
    if len(scoped) != 1:
        raise _PostCloseCLIError("PAPER_DAY_RUN_SCOPE_CONFLICT")
    if scoped[0].target_hash != target_hash:
        raise _PostCloseCLIError("NOTIFICATION_TARGET_CONFLICT")

    config = {
        "account_id": account_id.strip(),
        "base_url": base_url,
        "candidate_db": str(Path(candidate_db).resolve()),
        "history_days": history_days,
        "macro_enabled": macro_enabled,
        "macro_provider": macro_provider,
        "macro_weight": format(macro_weight, "f"),
        "market_evidence_dir": str(Path(market_evidence_dir).resolve()),
        "model": model,
        "news_feeds": list(news_feeds or ()),
        "news_runtime_dir": str(Path(news_runtime_dir).resolve()),
        "refresh_news": refresh_news,
        "research_db": str(Path(research_db).resolve()),
        "schema_version": 1,
        "search_discovery": search_discovery,
        "searxng_url": (
            None if validated_searxng_url is None else validated_searxng_url.manifest_document
        ),
        "session_date": session_date.isoformat(),
        "target_hash": target_hash,
        "target_kind": target_kind.value,
    }
    config_json = json.dumps(
        config,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    config_sha256 = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
    run_id = hashlib.sha256(
        f"ashare-post-close@1\0{session_date.isoformat()}\0{config_sha256}".encode()
    ).hexdigest()
    post_root = session_root / "post-close"
    manifest_path = post_root / "manifest.json"
    status_path = post_root / "status.json"
    audit_path = post_root / "audit.jsonl"
    expected_manifest: dict[str, object] = {
        "config": config,
        "config_sha256": config_sha256,
        "run_id": run_id,
        "schema_version": 1,
    }

    post_root.mkdir(parents=True, exist_ok=True)
    with _post_close_process_lock(post_root / "run.lock"):
        if manifest_path.is_file():
            retained_manifest = _read_post_close_json(
                manifest_path,
                "POST_CLOSE_MANIFEST_INVALID",
            )
            if retained_manifest != expected_manifest:
                raise _PostCloseCLIError("POST_CLOSE_MANIFEST_CONFLICT")
        else:
            _atomic_write_cli_json(manifest_path, expected_manifest)
        status = (
            _read_post_close_json(status_path, "POST_CLOSE_STATUS_INVALID")
            if status_path.is_file()
            else {
                "attempt": 0,
                "phase": "NEW",
                "run_id": run_id,
                "schema_version": 1,
                "session_date": session_date.isoformat(),
                "target_hash": target_hash,
            }
        )
        if status.get("run_id") != run_id or status.get("target_hash") != target_hash:
            raise _PostCloseCLIError("POST_CLOSE_STATUS_CONFLICT")
        phase = status.get("phase")
        if phase == "COMPLETE":
            return _post_close_completed_result(post_root, status, idempotent_replay=True)
        if phase in {"ANALYSIS_IN_PROGRESS", "ANALYSIS_AMBIGUOUS"}:
            if not recover_analysis:
                raise _PostCloseCLIError("POST_CLOSE_ANALYSIS_RECOVERY_REQUIRES_OPERATOR")
            _append_post_close_audit(
                audit_path,
                event="ANALYSIS_RECOVERY_AUTHORIZED",
                run_id=run_id,
                target_hash=target_hash,
                details={
                    "authorization": "EXPLICIT_RECOVER_ANALYSIS_FLAG",
                    "prior_phase": phase,
                },
            )
            status.update(
                {
                    "error_code": None,
                    "phase": "NEW",
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
            for key in (
                "analysis_outcome",
                "artifact_path",
                "artifact_sha256",
                "held_count",
                "next_session",
                "paper_run_id",
                "research_completed",
                "research_failed",
            ):
                status.pop(key, None)
            _atomic_write_cli_json(status_path, status)
            phase = "NEW"
        if phase in {"DELIVERY_IN_PROGRESS", "DELIVERY_AMBIGUOUS"}:
            if not recover_delivery:
                raise _PostCloseCLIError("POST_CLOSE_DELIVERY_RECOVERY_REQUIRES_OPERATOR")
            _append_post_close_audit(
                audit_path,
                event="DELIVERY_RECOVERY_AUTHORIZED",
                run_id=run_id,
                target_hash=target_hash,
                details={
                    "authorization": "EXPLICIT_RECOVER_DELIVERY_FLAG",
                    "prior_phase": phase,
                },
            )
            status.update(
                {
                    "artifact_delivery": "PENDING",
                    "delivery_recovery_authorized": True,
                    "error_code": None,
                    "phase": "DELIVERY_PENDING",
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
            _atomic_write_cli_json(status_path, status)
            phase = "DELIVERY_PENDING"
        if phase == "DELIVERY_FAILED":
            raise _PostCloseCLIError("POST_CLOSE_TEXT_DELIVERY_FAILED")
        if phase not in {
            "NEW",
            "ANALYSIS_FAILED",
            "ANALYSIS_FAILED_RETRYABLE",
            "ANALYSIS_COMPLETE",
            "DELIVERY_PENDING",
        }:
            raise _PostCloseCLIError("POST_CLOSE_STATUS_INVALID")

        artifact_path: Path | None = None
        if phase in {"ANALYSIS_COMPLETE", "DELIVERY_PENDING"}:
            artifact_value = status.get("artifact_path")
            artifact_sha256 = status.get("artifact_sha256")
            if not isinstance(artifact_value, str) or not isinstance(artifact_sha256, str):
                raise _PostCloseCLIError("POST_CLOSE_STATUS_INVALID")
            artifact_path = _validated_post_close_artifact(
                session_root,
                Path(artifact_value),
                artifact_sha256,
            )
        else:
            status.update(
                {
                    "attempt": (_post_close_optional_integer(status.get("attempt")) or 0) + 1,
                    "phase": "ANALYSIS_IN_PROGRESS",
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
            _atomic_write_cli_json(status_path, status)
            _append_post_close_audit(
                audit_path,
                event="ANALYSIS_STARTED",
                run_id=run_id,
                target_hash=target_hash,
            )
            ledger_path = session_root / "ledger.sqlite3"
            if not ledger_path.is_file():
                status.update(
                    {
                        "error_code": "PAPER_LEDGER_NOT_AVAILABLE",
                        "phase": "ANALYSIS_FAILED_RETRYABLE",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise _PostCloseCLIError(
                    "PAPER_LEDGER_NOT_AVAILABLE",
                    retryable=True,
                )
            try:
                with SQLitePaperLedger(ledger_path) as ledger:
                    paper = ASharePaperTradingService(ledger)
                    account = paper.snapshot(account_id.strip())
                    instrument_types = {
                        position.symbol: position.instrument_type.value.lower()
                        for position in account.positions
                        if position.quantity > 0
                    }
                    profiles = _paper_session_instrument_profiles(
                        projection,
                        instrument_types,
                    )
                    candidate_path = Path(candidate_db).resolve()
                    if candidate_path.is_file():
                        with SQLiteCandidateStore(candidate_path) as candidates:
                            result = await _run_post_close_orchestrator(
                                sessions=sessions,
                                projection=projection,
                                paper=paper,
                                candidates=candidates,
                                profiles=profiles,
                                session_root=session_root,
                                account_id=account_id.strip(),
                                history_days=history_days,
                                news_runtime_dir=news_runtime_dir,
                                research_db=research_db,
                                market_evidence_dir=market_evidence_dir,
                                analysis_outbox_db=str(post_root / "analysis-outbox.sqlite3"),
                                news_feeds=news_feeds,
                                refresh_news=refresh_news,
                                search_discovery=search_discovery,
                                searxng_url=(
                                    None
                                    if validated_searxng_url is None
                                    else validated_searxng_url.runtime_url
                                ),
                                macro_enabled=macro_enabled,
                                macro_provider=macro_provider,
                                model=model,
                                macro_weight=macro_weight,
                                now=now,
                            )
                    else:
                        result = await _run_post_close_orchestrator(
                            sessions=sessions,
                            projection=projection,
                            paper=paper,
                            candidates=None,
                            profiles=profiles,
                            session_root=session_root,
                            account_id=account_id.strip(),
                            history_days=history_days,
                            news_runtime_dir=news_runtime_dir,
                            research_db=research_db,
                            market_evidence_dir=market_evidence_dir,
                            analysis_outbox_db=str(post_root / "analysis-outbox.sqlite3"),
                            news_feeds=news_feeds,
                            refresh_news=refresh_news,
                            search_discovery=search_discovery,
                            searxng_url=(
                                None
                                if validated_searxng_url is None
                                else validated_searxng_url.runtime_url
                            ),
                            macro_enabled=macro_enabled,
                            macro_provider=macro_provider,
                            model=model,
                            macro_weight=macro_weight,
                            now=now,
                        )
            except asyncio.CancelledError:
                raise
            except PostCloseOrchestrationError as error:
                status.update(
                    {
                        "error_code": error.code,
                        "phase": "ANALYSIS_FAILED",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise _PostCloseCLIError(error.code) from None
            except _PostCloseCLIError:
                raise
            except Exception:
                status.update(
                    {
                        "error_code": "POST_CLOSE_ANALYSIS_FAILED",
                        "phase": "ANALYSIS_AMBIGUOUS",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise _PostCloseCLIError("POST_CLOSE_ANALYSIS_FAILED") from None

            artifact_path = _validated_post_close_artifact(
                session_root,
                result.artifact_path,
                _post_close_sha256(result.artifact_path),
            )
            artifact_digest = _post_close_sha256(artifact_path)
            held_count = len(result.review.held_symbols)
            research_completed = sum(
                item.status.value == "COMPLETED" for item in result.review.research
            )
            research_failed = sum(item.status.value == "FAILED" for item in result.review.research)
            try:
                analysis_outcome = _post_close_analysis_outcome(
                    held_count=held_count,
                    research_completed=research_completed,
                    research_failed=research_failed,
                )
            except _PostCloseCLIError as error:
                status.update(
                    {
                        "error_code": error.code,
                        "phase": "ANALYSIS_FAILED",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise
            if analysis_outcome == "FAILED":
                status.update(
                    {
                        "analysis_outcome": "FAILED",
                        "artifact_path": str(artifact_path),
                        "artifact_sha256": artifact_digest,
                        "error_code": "ALL_HELD_RESEARCH_FAILED",
                        "held_count": held_count,
                        "next_session": result.review.next_session.isoformat(),
                        "paper_run_id": result.review.paper_run_id,
                        "phase": "ANALYSIS_FAILED_RETRYABLE",
                        "research_completed": research_completed,
                        "research_failed": research_failed,
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                _append_post_close_audit(
                    audit_path,
                    event="ANALYSIS_FAILED",
                    run_id=run_id,
                    target_hash=target_hash,
                    details={
                        "error_code": "ALL_HELD_RESEARCH_FAILED",
                        "held_count": held_count,
                        "research_failed": research_failed,
                    },
                )
                raise _PostCloseCLIError(
                    "ALL_HELD_RESEARCH_FAILED",
                    retryable=True,
                )
            status.update(
                {
                    "analysis_outcome": analysis_outcome,
                    "artifact_path": str(artifact_path),
                    "artifact_sha256": artifact_digest,
                    "held_count": held_count,
                    "next_session": result.review.next_session.isoformat(),
                    "paper_run_id": result.review.paper_run_id,
                    "phase": "ANALYSIS_COMPLETE",
                    "research_completed": research_completed,
                    "research_failed": research_failed,
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
            _atomic_write_cli_json(status_path, status)
            _append_post_close_audit(
                audit_path,
                event="ANALYSIS_COMPLETED",
                run_id=run_id,
                target_hash=target_hash,
                details={
                    "analysis_outcome": analysis_outcome,
                    "artifact_sha256": artifact_digest,
                    "research_completed": research_completed,
                    "research_failed": research_failed,
                },
            )

        assert artifact_path is not None
        delivered = await _deliver_post_close_artifact(
            post_root=post_root,
            status=status,
            status_path=status_path,
            audit_path=audit_path,
            run_id=run_id,
            target_hash=target_hash,
            target_kind=target_kind,
            target_id=target_id,
            base_url=base_url,
            artifact_path=artifact_path,
            dispatch_cycles=dispatch_cycles,
            dispatch_poll_interval=dispatch_poll_interval,
        )
        if not delivered:
            raise _PostCloseCLIError("POST_CLOSE_DELIVERY_PENDING", retryable=True)
        return _post_close_completed_result(post_root, status, idempotent_replay=False)


async def _run_post_close_orchestrator(
    *,
    sessions: CloseSessionResolution,
    projection: PaperDayExecutiveProjection,
    paper: ASharePaperTradingService,
    candidates: SQLiteCandidateStore | None,
    profiles: Mapping[str, ResearchInstrumentProfile],
    session_root: Path,
    account_id: str,
    history_days: int,
    news_runtime_dir: str,
    research_db: str,
    market_evidence_dir: str,
    analysis_outbox_db: str,
    news_feeds: Sequence[str] | None,
    refresh_news: bool,
    search_discovery: bool,
    searxng_url: str | None,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    macro_weight: Decimal,
    now: datetime,
) -> PostCloseOrchestrationResult:
    from gribuki_trade.services.ashare_post_close import (
        ASharePostCloseOrchestrator,
        PostCloseOrchestrationRequest,
    )

    close_research = _CLIExistingCloseResearch(
        profiles=profiles,
        history_days=history_days,
        news_runtime_dir=news_runtime_dir,
        research_db=research_db,
        market_evidence_dir=market_evidence_dir,
        analysis_outbox_db=analysis_outbox_db,
        news_feeds=news_feeds,
        refresh_news=refresh_news,
        search_discovery=search_discovery,
        searxng_url=searxng_url,
        macro_enabled=macro_enabled,
        macro_provider=macro_provider,
        model=model,
        macro_weight=macro_weight,
    )
    orchestrator = ASharePostCloseOrchestrator(
        session_resolver=_FixedPostCloseSessions(sessions),
        paper_account=paper,
        close_research=close_research,
        candidate_reader=candidates,
        sidecar_loader=lambda _path: projection,
        research_timeout_seconds=600.0,
    )
    return await orchestrator.run_once(
        PostCloseOrchestrationRequest(
            session_root=session_root,
            account_id=account_id,
            now=now,
        )
    )


def _validate_post_close_target(
    base_url: str,
    target_kind: object,
    target_id: str,
) -> None:
    from gribuki_trade.adapters.notifiers import OneBotConfig
    from gribuki_trade.ports.notifier import NotificationTargetKind

    if not isinstance(target_kind, NotificationTargetKind):
        raise TypeError("target_kind must be a NotificationTargetKind")
    allowlist = frozenset({target_id})
    try:
        OneBotConfig(
            access_token="validation-only-not-a-credential",
            base_url=base_url,
            private_target_ids=(
                allowlist if target_kind is NotificationTargetKind.PRIVATE else frozenset()
            ),
            group_target_ids=(
                allowlist if target_kind is NotificationTargetKind.GROUP else frozenset()
            ),
        )
    except (TypeError, ValueError):
        raise _PostCloseCLIError("NOTIFICATION_TARGET_INVALID") from None


def _validated_post_close_artifact(
    session_root: Path,
    artifact_path: Path,
    expected_sha256: str,
) -> Path:
    report_root_input = session_root / "reports"
    try:
        if report_root_input.is_symlink() or artifact_path.is_symlink():
            raise ValueError
        report_root = report_root_input.resolve(strict=True)
        resolved = artifact_path.resolve(strict=True)
        resolved.relative_to(report_root)
    except (OSError, RuntimeError, ValueError):
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID") from None
    if not resolved.is_file() or resolved.suffix.casefold() != ".md":
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID")
    if _post_close_sha256(resolved) != expected_sha256:
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_DIGEST_MISMATCH")
    return resolved


def _post_close_delivery_summary(
    text: str,
    *,
    artifact_name: str,
    artifact_sha256: str,
    run_id: str,
) -> str:
    """从已验约的盘后日报提取短摘要；长文始终以 Markdown 文件交付。"""

    from gribuki_trade.reporting.contracts import (
        ReportKind,
        render_stable_text_report,
        report_contract,
        validate_markdown_report_contract,
    )

    try:
        validate_markdown_report_contract(ReportKind.DAILY_REVIEW, text)
    except (TypeError, ValueError):
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID") from None
    contract = report_contract(ReportKind.DAILY_REVIEW)
    sections = {
        name: _post_close_section_excerpt(text, name) for name in contract.required_sections
    }
    sections["执行摘要"] = (
        f"{sections['执行摘要']}\n完整报告文件：{artifact_name}；"
        f"内容摘要：{artifact_sha256[:12]}；运行标识：{run_id[-12:]}。"
    )
    summary = render_stable_text_report(
        ReportKind.DAILY_REVIEW,
        title="A股盘后日报交付摘要",
        sections=sections,
    )
    if len(summary) > 7_000:
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID")
    return summary


def _post_close_section_excerpt(text: str, name: str, limit: int = 850) -> str:
    """提取一个必需二级章节的开头，避免 QQ 摘要复制整份长报告。"""

    marker = f"\n## {name}\n"
    start = text.find(marker)
    if start < 0:
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID")
    content_start = start + len(marker)
    next_heading = text.find("\n## ", content_start)
    content_end = len(text) if next_heading < 0 else next_heading
    meaningful = tuple(
        line.strip() for line in text[content_start:content_end].splitlines() if line.strip()
    )
    if not meaningful:
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID")
    excerpt = "\n".join(meaningful)
    if len(excerpt) <= limit:
        return excerpt
    shortened = excerpt[: limit - 1]
    last_break = shortened.rfind("\n")
    if last_break >= limit // 2:
        shortened = shortened[:last_break]
    return shortened.rstrip() + "…"


async def _deliver_post_close_artifact(
    *,
    post_root: Path,
    status: dict[str, object],
    status_path: Path,
    audit_path: Path,
    run_id: str,
    target_hash: str,
    target_kind: object,
    target_id: str,
    base_url: str,
    artifact_path: Path,
    dispatch_cycles: int,
    dispatch_poll_interval: float,
) -> bool:
    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.ports.notifier import (
        NotificationTargetKind,
        OutboundNotification,
    )
    from gribuki_trade.services.notification_dispatch import NotificationDispatchService
    from gribuki_trade.storage.outbox import OutboxStatus, SQLiteOutbox

    if not isinstance(target_kind, NotificationTargetKind):
        raise TypeError("target_kind must be a NotificationTargetKind")
    if status.get("artifact_delivery") == "IN_PROGRESS":
        raise _PostCloseCLIError("POST_CLOSE_RECOVERY_REQUIRES_OPERATOR")
    try:
        report_text = artifact_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID") from None
    artifact_sha256 = _post_close_sha256(artifact_path)
    summary = _post_close_delivery_summary(
        report_text,
        artifact_name=artifact_path.name,
        artifact_sha256=artifact_sha256,
        run_id=run_id,
    )
    messages = (summary,)
    created_value = status.get("delivery_created_at")
    if isinstance(created_value, str):
        try:
            created_at = datetime.fromisoformat(created_value).astimezone(UTC)
        except ValueError:
            raise _PostCloseCLIError("POST_CLOSE_STATUS_INVALID") from None
    else:
        created_at = datetime.now(UTC)
        status["delivery_created_at"] = created_at.isoformat()
    expires_at = created_at + timedelta(hours=24)
    outbox_path = post_root / "delivery-outbox.sqlite3"
    keys = tuple(
        f"post-close:{run_id}:summary:{index:02d}-of-{len(messages):02d}"
        for index in range(1, len(messages) + 1)
    )
    with SQLiteOutbox(outbox_path) as outbox:
        for key, message in zip(keys, messages, strict=True):
            outbox.enqueue(
                OutboundNotification(
                    idempotency_key=key,
                    channel="onebot",
                    target_kind=target_kind,
                    target_id=target_id,
                    text=message,
                    created_at=created_at,
                    expires_at=expires_at,
                )
            )
        status.update(
            {
                "phase": "DELIVERY_PENDING",
                "text_part_count": len(messages),
                "updated_at": datetime.now(UTC).isoformat(),
            }
        )
        _atomic_write_cli_json(status_path, status)
        retained_before_dispatch = tuple(outbox.get_by_key(key) for key in keys)
        if any(item is None for item in retained_before_dispatch):
            raise _PostCloseCLIError("POST_CLOSE_OUTBOX_INTEGRITY_ERROR")
        if (
            any(
                item is not None and item.status is OutboxStatus.IN_FLIGHT
                for item in retained_before_dispatch
            )
            and status.get("delivery_recovery_authorized") is not True
        ):
            status.update(
                {
                    "error_code": "POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS",
                    "phase": "DELIVERY_AMBIGUOUS",
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
            _atomic_write_cli_json(status_path, status)
            _append_post_close_audit(
                audit_path,
                event="DELIVERY_AMBIGUOUS",
                run_id=run_id,
                target_hash=target_hash,
                details={"error_code": "POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS"},
            )
            raise _PostCloseCLIError("POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS")
        try:
            access_token = _required_local_secret(NAPCAT_ACCESS_TOKEN_SECRET)
        except (RuntimeError, SecretProviderError):
            _append_post_close_audit(
                audit_path,
                event="DELIVERY_DEFERRED",
                run_id=run_id,
                target_hash=target_hash,
                details={"error_code": "NAPCAT_TOKEN_NOT_CONFIGURED"},
            )
            return False
        allowlist = frozenset({target_id})
        config = OneBotConfig(
            access_token=access_token,
            base_url=base_url,
            private_target_ids=(
                allowlist if target_kind is NotificationTargetKind.PRIVATE else frozenset()
            ),
            group_target_ids=(
                allowlist if target_kind is NotificationTargetKind.GROUP else frozenset()
            ),
            artifact_root=artifact_path.parent,
        )
        async with OneBotNotifier(config) as notifier:
            if not await _post_close_napcat_ready(notifier):
                # NapCat 的启动和登录必须由 GUI 显式完成。盘后流程只观察
                # 健康状态并保留 durable outbox，绝不偷偷启动 OS 侧车。
                _append_post_close_audit(
                    audit_path,
                    event="DELIVERY_DEFERRED",
                    run_id=run_id,
                    target_hash=target_hash,
                    details={"error_code": "NAPCAT_NOT_READY_GUI_ACTION_REQUIRED"},
                )
                return False
            service = NotificationDispatchService(
                outbox,
                {notifier.channel: notifier},
                target_kind=target_kind,
                target_id=target_id,
            )
            try:
                await service.poll(
                    max_cycles=dispatch_cycles,
                    poll_interval=dispatch_poll_interval,
                )
            except asyncio.CancelledError:
                status.update(
                    {
                        "error_code": "POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS",
                        "phase": "DELIVERY_AMBIGUOUS",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise
            except Exception:
                status.update(
                    {
                        "error_code": "POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS",
                        "phase": "DELIVERY_AMBIGUOUS",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise _PostCloseCLIError("POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS") from None
            retained = tuple(outbox.get_by_key(key) for key in keys)
            if any(item is None for item in retained):
                raise _PostCloseCLIError("POST_CLOSE_OUTBOX_INTEGRITY_ERROR")
            statuses = tuple(item.status for item in retained if item is not None)
            if any(item in {OutboxStatus.DEAD, OutboxStatus.EXPIRED} for item in statuses):
                status.update(
                    {
                        "error_code": "POST_CLOSE_TEXT_DELIVERY_FAILED",
                        "phase": "DELIVERY_FAILED",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise _PostCloseCLIError("POST_CLOSE_TEXT_DELIVERY_FAILED")
            if any(item is not OutboxStatus.SENT for item in statuses):
                return False

            if status.get("artifact_delivery") != "SENT":
                status.update(
                    {
                        "artifact_delivery": "IN_PROGRESS",
                        "phase": "DELIVERY_IN_PROGRESS",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                _append_post_close_audit(
                    audit_path,
                    event="ARTIFACT_DELIVERY_STARTED",
                    run_id=run_id,
                    target_hash=target_hash,
                    details={"artifact_sha256": artifact_sha256},
                )
                try:
                    receipt = (
                        await notifier.upload_private_file(target_id, artifact_path.name)
                        if target_kind is NotificationTargetKind.PRIVATE
                        else await notifier.upload_group_file(target_id, artifact_path.name)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    status.update(
                        {
                            "artifact_delivery": "AMBIGUOUS",
                            "error_code": "POST_CLOSE_ARTIFACT_DELIVERY_AMBIGUOUS",
                            "phase": "DELIVERY_AMBIGUOUS",
                            "updated_at": datetime.now(UTC).isoformat(),
                        }
                    )
                    _atomic_write_cli_json(status_path, status)
                    raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_DELIVERY_AMBIGUOUS") from None
                status.update(
                    {
                        "artifact_delivery": "SENT",
                        "artifact_provider_file_id": receipt.provider_file_id,
                    }
                )

    status.update(
        {
            "completed_at": datetime.now(UTC).isoformat(),
            "error_code": None,
            "phase": "COMPLETE",
            "updated_at": datetime.now(UTC).isoformat(),
        }
    )
    _atomic_write_cli_json(status_path, status)
    _append_post_close_audit(
        audit_path,
        event="DELIVERY_COMPLETED",
        run_id=run_id,
        target_hash=target_hash,
        details={"text_part_count": len(messages)},
    )
    return True


async def _post_close_napcat_ready(notifier: object) -> bool:
    try:
        status = await notifier.get_status()  # type: ignore[attr-defined]
    except asyncio.CancelledError:
        raise
    except Exception:
        return False
    return (
        isinstance(status, Mapping) and status.get("good") is True and status.get("online") is True
    )


def _ashare_post_close_status(
    post_root: Path,
    session_date: date,
) -> dict[str, object]:
    status_path = post_root / "status.json"
    if not status_path.is_file():
        return _post_close_error(
            "status",
            session_date,
            post_root,
            "POST_CLOSE_STATUS_NOT_AVAILABLE",
        )
    try:
        status = _read_post_close_json(status_path, "POST_CLOSE_STATUS_INVALID")
    except _PostCloseCLIError as error:
        return _post_close_error("status", session_date, post_root, error.code)
    return {
        "action": "status",
        "ok": True,
        "read_only_sidecar": True,
        "runtime_dir": str(post_root),
        "status": status,
        "status_path": str(status_path.resolve()),
    }


def _ashare_post_close_report(
    post_root: Path,
    session_date: date,
) -> dict[str, object]:
    status_path = post_root / "status.json"
    if not status_path.is_file():
        return _post_close_error(
            "report",
            session_date,
            post_root,
            "POST_CLOSE_REPORT_NOT_AVAILABLE",
        )
    try:
        status = _read_post_close_json(status_path, "POST_CLOSE_STATUS_INVALID")
        path_value = status.get("artifact_path")
        digest = status.get("artifact_sha256")
        if not isinstance(path_value, str) or not isinstance(digest, str):
            raise _PostCloseCLIError("POST_CLOSE_REPORT_NOT_AVAILABLE")
        report_path = _validated_post_close_artifact(
            post_root.parent,
            Path(path_value),
            digest,
        )
        content = report_path.read_bytes()
        content.decode("utf-8")
    except (_PostCloseCLIError, OSError, UnicodeError) as error:
        code = (
            error.code if isinstance(error, _PostCloseCLIError) else "POST_CLOSE_ARTIFACT_INVALID"
        )
        return _post_close_error("report", session_date, post_root, code)
    return {
        "action": "report",
        "ok": True,
        "read_only_sidecar": True,
        "report": {
            "bytes": len(content),
            "name": report_path.name,
            "path": str(report_path),
            "sha256": digest,
        },
        "runtime_dir": str(post_root),
    }


def _post_close_completed_result(
    post_root: Path,
    status: Mapping[str, object],
    *,
    idempotent_replay: bool,
) -> dict[str, object]:
    retained = {
        key: status.get(key)
        for key in (
            "artifact_path",
            "artifact_sha256",
            "analysis_outcome",
            "completed_at",
            "held_count",
            "next_session",
            "paper_run_id",
            "phase",
            "research_completed",
            "research_failed",
            "run_id",
            "session_date",
            "target_hash",
            "text_part_count",
        )
    }
    return {
        "action": "run",
        "idempotent_replay": idempotent_replay,
        "ok": True,
        "runtime_dir": str(post_root),
        **retained,
    }


def _post_close_skipped(
    session_date: date,
    post_root: Path,
    code: str,
) -> dict[str, object]:
    return {
        "action": "run",
        "error_code": code,
        "ok": True,
        "runtime_dir": str(post_root),
        "session_date": session_date.isoformat(),
        "skipped": True,
    }


def _post_close_error(
    action: str,
    session_date: date,
    post_root: Path,
    code: str,
    *,
    retryable: bool = False,
) -> dict[str, object]:
    return {
        "action": action,
        "error_code": code,
        "ok": False,
        "retryable": retryable,
        "runtime_dir": str(post_root),
        "session_date": session_date.isoformat(),
    }


def _ashare_paper_day_error(
    action: str,
    session_date: date,
    session_root: Path,
    error_code: str,
) -> dict[str, object]:
    return {
        "ok": False,
        "action": action,
        "error_code": error_code,
        "session_date": session_date.isoformat(),
        "runtime_dir": str(session_root),
    }


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
        ProductionLLMProfile,
        ResearchNotificationTarget,
        build_production_dual_track_analyzer,
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

    interval = MinuteInterval.ONE_MINUTE if interval_value == "1m" else MinuteInterval.FIVE_MINUTES
    market_data = AKShareMarketDataAdapter()
    collection_request = AShareResearchRequest(
        symbol=symbol,
        start=window_end - timedelta(minutes=lookback_minutes),
        end=window_end,
        interval=interval,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
    )
    collection = await AShareResearchService(market_data).collect_market_data(collection_request)
    # 决策边界必须位于供应商响应之后。在网络 I/O 前冻结会使每个真实 fetched_at
    # 看起来都像未来数据。
    decision_time = datetime.now(UTC)

    research_path = Path(research_db).resolve()
    macro = None
    baseline_macro = None
    adversarial_macro = None
    macro_selected_track = None
    macro_audit_record_sha256 = None
    if macro_enabled and collection.failure_code is None:
        baseline_analyzer: MacroAnalyzer
        if macro_provider == "deepseek":
            baseline_analyzer = DeepSeekChatMacroAnalyzer(
                SecretValue(_required_local_secret(DEEPSEEK_API_KEY_SECRET)),
                model=resolved_model,
            )
        else:
            baseline_analyzer = OpenAIResponsesMacroAnalyzer(
                SecretValue(_required_local_secret(OPENAI_API_KEY_SECRET)),
                model=resolved_model,
            )
        with build_production_dual_track_analyzer(
            baseline_analyzer,
            audit_path=research_path.with_name("llm-adversarial.sqlite3"),
            profile=ProductionLLMProfile.STANDARD,
            maximum_calls_per_session=None,
        ) as dual_analyzer:
            macro_run = await MacroResearchService(dual_analyzer).analyze(
                symbol=symbol,
                as_of=decision_time,
                horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS.value,
                technical_summary=("基于已完成分钟线的确定性突破分析",),
                events=events,
            )
        macro = macro_run.analysis
        baseline_macro = macro_run.baseline_analysis
        adversarial_macro = macro_run.adversarial_analysis
        macro_selected_track = macro_run.selected_track
        macro_audit_record_sha256 = macro_run.dual_audit_record_sha256
        selection = macro_run.selection
        macro_failure_code = macro_run.failure_code
    else:
        selection = select_macro_evidence(symbol, decision_time, events)
        macro_failure_code = "SKIPPED_MARKET_DATA_UNAVAILABLE" if macro_enabled else None
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
                baseline_macro=baseline_macro,
                adversarial_macro=adversarial_macro,
                macro_selected_track=macro_selected_track,
                macro_audit_record_sha256=macro_audit_record_sha256,
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
        "fusion_reason_codes": list(getattr(recommendation, "fusion_reason_codes", ())),
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
        "macro_dual_track": {
            "selected_track": macro_selected_track,
            "audit_record_sha256": macro_audit_record_sha256,
            "baseline": _macro_analysis_document(baseline_macro),
            "adversarial": _macro_analysis_document(adversarial_macro),
        }
        if baseline_macro is not None and adversarial_macro is not None
        else None,
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
    """顺序运行有界且故障隔离的收盘分析批次。

    顺序执行是有意设计：当前公共供应商未声明支持高扇出，而 BaoStock 还拥有进程全局
    会话状态。该批次是研究回调，不包含订单路径。
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

    held = {canonical_ashare_symbol(symbol) for symbol in (held_symbols or ())}
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
            None if candidate_store_path is None else str(Path(candidate_store_path).resolve())
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
    """限制批次标准输出大小，同时保留每个持久化结果标识。"""

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
    instrument_profile_override: ResearchInstrumentProfile | None = None,
) -> dict[str, object]:
    """运行一次经日历核验且不含任何订单路径的盘后分析。"""

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
        OwnedProductionDualTrackAnalyzer,
        ProductionLLMProfile,
        ResearchNotificationTarget,
        build_ashare_breadth_evidence,
        build_ashare_context_evidence,
        build_ashare_derivatives_evidence,
        build_official_rates_evidence,
        build_production_dual_track_analyzer,
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
    if instrument_profile_override is None:
        (
            instrument_profile,
            instrument_profile_failure_code,
        ) = await _resolve_close_instrument_profile_for_run(symbol)
    else:
        requested_symbol = symbol.strip().upper()
        if instrument_profile_override.symbol != requested_symbol:
            raise ValueError("instrument profile override must match the requested symbol")
        expected_asset_type = instrument_type.value
        if instrument_profile_override.asset_type != expected_asset_type:
            raise ValueError("instrument profile override asset type does not match the symbol")
        instrument_profile = instrument_profile_override
        instrument_profile_failure_code = None
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
            asset_types={canonical_symbol: AKShareDailyAssetType(instrument_profile.asset_type)},
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
                canonical_url=(f"local://market-evidence/{retained.document_id}"),
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
                _optional_local_secret(TAVILY_API_KEY_SECRET) if search_discovery else None
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

    context_results = [
        await collect_context(
            "LIQUIDITY_CONTEXT",
            AKShareLiquidityContextAdapter(timeout_seconds=15.0).fetch_liquidity_context_async(
                start_date=sessions.latest_completed_session - timedelta(days=14),
                end_date=sessions.latest_completed_session,
            ),
        ),
        await collect_context(
            "IF_CONTEXT",
            AKShareIFContextAdapter(timeout_seconds=20.0).fetch_if_daily_context_async(
                sessions.latest_completed_session
            ),
        ),
    ]
    if instrument_type is CloseInstrumentType.ETF:
        context_results.append(
            await collect_context(
                "ETF_CONTEXT",
                AKShareETFContextAdapter(timeout_seconds=30.0).fetch_etf_context_async(
                    canonical_symbol
                ),
            )
        )
    context_values = {context_id: value for context_id, value, _failure in context_results}
    ashare_context_failure_codes = tuple(
        failure for _context_id, _value, failure in context_results if failure is not None
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
        vix_history = await CboeVIXDailyAdapter(timeout_seconds=15.0).fetch_vix_daily_history(
            as_of=vix_visibility_cutoff
        )
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
        safe_history = await SafeCentralParityAdapter(timeout_seconds=15.0).fetch_usd_cny_history(
            start_date=rate_start,
            end_date=sessions.latest_completed_session,
            as_of=official_rates_cutoff,
        )
    except OfficialRatesDataError:
        official_rates_failure_codes.append("SAFE_USD_CNY_UNAVAILABLE")
    try:
        shibor_history = await OfficialShiborAdapter(timeout_seconds=15.0).fetch_shibor_history(
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
    if instrument_type is CloseInstrumentType.ETF and instrument_profile.exchange == "sse":
        local_symbol = canonical_symbol.split(".", maxsplit=1)[0]
        try:
            etf_share_observation = await SSEETFShareAdapter(timeout_seconds=15.0).fetch_etf_shares(
                local_symbol,
                sessions.latest_completed_session,
                allow_latest_available=True,
            )
        except SSEETFShareDataError:
            ashare_derivatives_failure_codes.append("SSE_ETF_OFFICIAL_SHARES_UNAVAILABLE")
        try:
            option_risk_snapshot = await SSEOptionRiskAdapter(
                timeout_seconds=15.0
            ).fetch_option_risk(
                local_symbol,
                sessions.latest_completed_session,
                allow_latest_available=True,
            )
        except SSEOptionRiskDataError:
            ashare_derivatives_failure_codes.append("SSE_OPTION_OFFICIAL_RISK_UNAVAILABLE")

    # 报告决策时间戳必须在所有网络拉取完成后捕获，否则比先前时间戳晚几毫秒拉取的来源
    # 会被时点证据门控正确地拒绝为“未来”数据。
    decision_time = datetime.now(UTC)
    local_decision_time = decision_time.astimezone(ZoneInfo("Asia/Shanghai"))
    if sessions.next_session == local_decision_time.date() and local_decision_time.time().replace(
        tzinfo=None
    ) >= datetime_time(9, 30):
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

    research_path = Path(research_db).resolve()
    outbox_path = Path(outbox_db).resolve()
    research_path.parent.mkdir(parents=True, exist_ok=True)
    outbox_path.parent.mkdir(parents=True, exist_ok=True)

    analyzer: MacroAnalyzer | None = None
    owned_dual: OwnedProductionDualTrackAnalyzer | None = None
    resolved_model = model or (
        DEFAULT_DEEPSEEK_MODEL if macro_provider == "deepseek" else "gpt-5.6"
    )
    if macro_enabled and preliminary.decision is not RecommendationDecision.ABSTAIN:
        baseline_analyzer: MacroAnalyzer
        if macro_provider == "deepseek":
            baseline_analyzer = DeepSeekChatMacroAnalyzer(
                SecretValue(_required_local_secret(DEEPSEEK_API_KEY_SECRET)),
                model=resolved_model,
            )
        else:
            baseline_analyzer = OpenAIResponsesMacroAnalyzer(
                SecretValue(_required_local_secret(OPENAI_API_KEY_SECRET)),
                model=resolved_model,
            )
        owned_dual = build_production_dual_track_analyzer(
            baseline_analyzer,
            audit_path=research_path.with_name("llm-adversarial.sqlite3"),
            profile=ProductionLLMProfile.DEEP,
            maximum_calls_per_session=None,
        )
        analyzer = owned_dual

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
        ashare_derivatives_failure_codes=tuple(ashare_derivatives_failure_codes),
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
    try:
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
    finally:
        if owned_dual is not None:
            owned_dual.close()
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
            ashare_derivatives_report_lines=(run.ashare_derivatives_report_lines),
            ashare_derivatives_failure_codes=(run.ashare_derivatives_failure_codes),
            cross_market_report_lines=run.cross_market_report_lines,
            cross_market_failure_code=run.cross_market_failure_code,
            cross_market_relation_report_lines=run.cross_market_relation_report_lines,
            cross_market_history_failure_code=run.cross_market_history_failure_code,
            evidence_selection=run.evidence_selection,
            baseline_macro=run.baseline_macro,
            adversarial_macro=run.adversarial_macro,
            selected_track=run.macro_selected_track,
            audit_record_sha256=run.macro_audit_record_sha256,
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
        "technical_fusion_weight": _three_decimal_text(recommendation.technical_fusion_weight),
        "macro_fusion_weight": _three_decimal_text(recommendation.macro_fusion_weight),
        "macro_evidence_coverage": _three_decimal_text(recommendation.macro_evidence_coverage),
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
        "ashare_derivatives_failure_codes": list(run.ashare_derivatives_failure_codes),
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
        "invalidation_price": _three_decimal_text(recommendation.invalidation_price),
        "latest_completed_session": sessions.latest_completed_session.isoformat(),
        "macro": _macro_analysis_document(run.macro),
        "macro_enabled": macro_enabled,
        "macro_failure_code": run.macro_failure_code,
        "macro_score": _three_decimal_text(recommendation.macro_score),
        "macro_dual_track": {
            "analysis_id": _macro_dual_analysis_id(
                run.baseline_macro,
                run.adversarial_macro,
            ),
            "selected_track": run.macro_selected_track,
            "audit_record_sha256": run.macro_audit_record_sha256,
            "baseline": _macro_track_document(
                run.baseline_macro,
                recommendation.evidence,
            ),
            "adversarial": _macro_track_document(
                run.adversarial_macro,
                recommendation.evidence,
            ),
        }
        if run.baseline_macro is not None or run.adversarial_macro is not None
        else None,
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
    """先从已校验观察列表、再从代码族解析股票/ETF 语义。"""

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
    """返回已保留观察列表档案，且不将其变为订单输入。"""

    from gribuki_trade.watchlists import load_research_watchlist

    try:
        watchlist = load_research_watchlist(Path(DEFAULT_ASHARE_WATCHLIST))
        return watchlist.instrument_profile(symbol)
    except (OSError, ValueError):
        return None


async def _resolve_close_instrument_profile_for_run(
    symbol: str,
) -> tuple[ResearchInstrumentProfile | None, str | None]:
    """优先使用已保留档案，否则拉取当前动态快照。

    实时回退刻意不适合历史回放。其适配器强制执行接近当前时刻的时点截点，所得档案
    会随推荐一同持久化，供日后复现。
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
        profile = await AKShareInstrumentProfileAdapter(timeout_seconds=20.0).fetch(
            symbol, known_at=profile_cutoff
        )
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
    """将已保留路由诊断映射为稳定且不含 URL 的来源标识。"""

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
            search_providers.append(TavilySearchProvider(transport, api_key=tavily_api_key))
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
            query_aliases = tuple(alias for alias in entity_aliases[1:] if alias != instrument_name)
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
            discovery_source_id = f"discovery.search.{symbol.split('.', maxsplit=1)[0]}"
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
            # 多条 AKShare 路由会实例化嵌入式 V8 运行时，因此 Windows 生产路径中即使只有
            # 一个持仓标的研究调用，也必须串行采集所有来源。
            max_concurrency=1,
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


def _macro_track_document(
    value: object | None,
    evidence: Sequence[object],
) -> dict[str, object] | None:
    """投影一条 LLM 轨道，并独立计算它实际引用的冻结证据比例。"""

    from gribuki_trade.analysis.schemas import MacroAnalysis

    if not isinstance(value, MacroAnalysis):
        return None
    document = _macro_analysis_document(value)
    if document is None:  # pragma: no cover - 由上方类型检查保证
        return None
    available = {
        evidence_id
        for item in evidence
        if isinstance((evidence_id := getattr(item, "evidence_id", None)), str) and evidence_id
    }
    referenced = {evidence_id for claim in value.claims for evidence_id in claim.evidence_ids}
    referenced.update(
        evidence_id for scenario in value.scenarios for evidence_id in scenario.evidence_ids
    )
    coverage = (
        Decimal("0")
        if not available
        else Decimal(len(referenced & available)) / Decimal(len(available))
    )
    return {
        **document,
        "analysis_id": value.analysis_id,
        "evidence_coverage": _three_decimal_text(coverage),
        "model_version": value.model_version,
    }


def _macro_dual_analysis_id(baseline: object | None, adversarial: object | None) -> str | None:
    from gribuki_trade.analysis.schemas import MacroAnalysis

    for analysis in (baseline, adversarial):
        if isinstance(analysis, MacroAnalysis) and analysis.analysis_id.strip():
            return analysis.analysis_id
    return None


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
    """运行有界研究周期，并按标的隔离失败。"""

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
        async with OneBotNotifier(OneBotConfig(access_token=token, base_url=base_url)) as notifier:
            status = await notifier.get_status()
            version = await notifier.get_version_info()
    except OneBotError as error:
        if error.code == "transport_error":
            next_action = "在 GUI 的‘集成管理’页显式启动 NapCat 并完成 QQ 登录"
        elif error.code == "authentication_rejected":
            next_action = (
                ".\\.venv\\Scripts\\python.exe -m gribuki_trade "
                "secret-set napcat.onebot.access_token"
            )
        else:
            next_action = "在 GUI 的‘集成管理’页检查 NapCat 状态和本机 WebUI"
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
    """运行有限且仅出站的 OneBot 发件箱工作器。"""

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
                target_kind=target_kind,
                target_id=target_id,
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
    from gribuki_trade.reporting.contracts import (
        ReportKind,
        render_stable_text_report,
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
    target_label = "私聊" if target_kind is NotificationTargetKind.PRIVATE else "群聊"
    health_text = render_stable_text_report(
        ReportKind.SYSTEM_HEALTH,
        title="Gribuki Trade｜NapCat 通知链路测试",
        sections={
            "总体状态": "正在执行一次显式、只读的通知链路测试；不包含交易指令。",
            "数据源": "本测试不读取行情、新闻或账户数据。",
            "模型与通知": (
                f"仅验证本地 NapCat/OneBot 到{target_label}目标的出站消息；不调用 LLM。"
            ),
            "缺口与恢复动作": (
                f"测试发起时点：{created_at.isoformat(timespec='seconds')}；"
                "若未收到，请在 GUI 集成管理页检查 NapCat 与 QQ 登录状态。"
            ),
        },
    )
    notification = OutboundNotification(
        idempotency_key=f"manual-health-test:{target_kind.value}:{int(created_at.timestamp())}",
        channel="onebot",
        target_kind=target_kind,
        target_id=target_id,
        text=health_text,
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
    report_kind_value: str,
    artifact_root: str,
    artifact: str,
    receipt_database: str,
) -> dict[str, object]:
    """经报告契约和持久交付边界显式发送一份 Markdown 报告。"""

    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.reporting.contracts import (
        ReportKind,
        validate_markdown_report_contract,
    )
    from gribuki_trade.storage.report_artifact_outbox import (
        ReportArtifactOutboxError,
        ReportArtifactStatus,
        SQLiteReportArtifactOutbox,
    )

    target_kind = NotificationTargetKind(target_kind_value)
    if artifact_kind != "file":
        raise ValueError("napcat-send-artifact only accepts Markdown report files")
    report_kind = ReportKind(report_kind_value)
    root, resolved = _validated_markdown_report_artifact(artifact_root, artifact)
    report_bytes = resolved.read_bytes()
    report_text = report_bytes.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    validate_markdown_report_contract(report_kind, report_text)
    artifact_sha256 = hashlib.sha256(report_bytes).hexdigest()
    key_material = "\0".join(
        (
            "napcat-report-artifact@1",
            report_kind.value,
            target_kind.value,
            target_id,
            artifact_sha256,
        )
    )
    idempotency_key = "report-artifact:" + hashlib.sha256(key_material.encode("utf-8")).hexdigest()
    receipt_path = Path(receipt_database).expanduser().resolve()
    created_at = datetime.now(UTC)
    claimed = False

    try:
        with SQLiteReportArtifactOutbox(receipt_path) as outbox:
            delivery = outbox.enqueue(
                idempotency_key=idempotency_key,
                report_kind=report_kind.value,
                target_kind=target_kind,
                target_id=target_id,
                artifact_name=resolved.name,
                artifact_sha256=artifact_sha256,
                created_at=created_at,
            )
            if delivery.status is ReportArtifactStatus.SENT:
                return _report_artifact_delivery_result(
                    delivery.provider_identifier,
                    artifact_sha256=artifact_sha256,
                    artifact_kind=artifact_kind,
                    report_kind=report_kind.value,
                    target_kind=target_kind.value,
                    already_sent=True,
                )

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
                artifact_root=root,
            )
            async with OneBotNotifier(config) as notifier:
                delivery = outbox.claim(idempotency_key, claimed_at=datetime.now(UTC))
                if delivery.status is ReportArtifactStatus.SENT:
                    return _report_artifact_delivery_result(
                        delivery.provider_identifier,
                        artifact_sha256=artifact_sha256,
                        artifact_kind=artifact_kind,
                        report_kind=report_kind.value,
                        target_kind=target_kind.value,
                        already_sent=True,
                    )
                claimed = True
                if _post_close_sha256(resolved) != artifact_sha256:
                    outbox.mark_ambiguous(idempotency_key)
                    raise ReportArtifactOutboxError("REPORT_ARTIFACT_CHANGED")
                try:
                    file_receipt = (
                        await notifier.upload_private_file(target_id, str(resolved))
                        if target_kind is NotificationTargetKind.PRIVATE
                        else await notifier.upload_group_file(target_id, str(resolved))
                    )
                    provider_identifier = file_receipt.provider_file_id
                    if provider_identifier is None:
                        raise ReportArtifactOutboxError("REPORT_ARTIFACT_PROVIDER_RECEIPT_MISSING")
                    delivery = outbox.mark_sent(
                        idempotency_key,
                        provider_identifier=provider_identifier,
                        sent_at=datetime.now(UTC),
                    )
                except BaseException:
                    with suppress(Exception):
                        outbox.mark_ambiguous(idempotency_key)
                    raise
    except asyncio.CancelledError:
        raise
    except ReportArtifactOutboxError as error:
        return _report_artifact_failure_result(
            error.code,
            artifact_sha256=artifact_sha256,
            artifact_kind=artifact_kind,
            report_kind=report_kind.value,
            target_kind=target_kind.value,
        )
    except Exception:
        return _report_artifact_failure_result(
            (
                "REPORT_ARTIFACT_DELIVERY_AMBIGUOUS"
                if claimed
                else "REPORT_ARTIFACT_DELIVERY_FAILED"
            ),
            artifact_sha256=artifact_sha256,
            artifact_kind=artifact_kind,
            report_kind=report_kind.value,
            target_kind=target_kind.value,
        )

    return _report_artifact_delivery_result(
        delivery.provider_identifier,
        artifact_sha256=artifact_sha256,
        artifact_kind=artifact_kind,
        report_kind=report_kind.value,
        target_kind=target_kind.value,
        already_sent=False,
    )


def _validated_markdown_report_artifact(
    artifact_root: str,
    artifact: str,
) -> tuple[Path, Path]:
    root_input = Path(artifact_root).expanduser()
    if root_input.is_symlink():
        raise ValueError("artifact_root must not be a symbolic link")
    try:
        root = root_input.resolve(strict=True)
    except (OSError, RuntimeError):
        raise ValueError("artifact_root must be an existing directory") from None
    if not root.is_dir():
        raise ValueError("artifact_root must be an existing directory")
    supplied = Path(artifact).expanduser()
    candidate = supplied if supplied.is_absolute() else root / supplied
    lexical = Path(os.path.abspath(candidate))
    try:
        relative = lexical.relative_to(root)
    except ValueError:
        raise ValueError("artifact must remain inside artifact_root") from None
    current = root
    try:
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise ValueError("artifact path must not contain symbolic links")
        resolved = lexical.resolve(strict=True)
        resolved.relative_to(root)
    except ValueError:
        raise
    except (OSError, RuntimeError):
        raise ValueError("artifact must be an existing regular file") from None
    if not resolved.is_file() or resolved.suffix.casefold() not in {".md", ".markdown"}:
        raise ValueError("artifact must be an existing Markdown file")
    return root, resolved


def _report_artifact_delivery_result(
    provider_identifier: str | None,
    *,
    artifact_sha256: str,
    artifact_kind: str,
    report_kind: str,
    target_kind: str,
    already_sent: bool,
) -> dict[str, object]:
    return {
        "already_sent": already_sent,
        "artifact_kind": artifact_kind,
        "artifact_sha256": artifact_sha256,
        "channel": "onebot",
        "delivered": True,
        "ok": True,
        "provider_identifier": provider_identifier,
        "report_kind": report_kind,
        "target_kind": target_kind,
    }


def _report_artifact_failure_result(
    error_code: str,
    *,
    artifact_sha256: str,
    artifact_kind: str,
    report_kind: str,
    target_kind: str,
) -> dict[str, object]:
    return {
        "artifact_kind": artifact_kind,
        "artifact_sha256": artifact_sha256,
        "channel": "onebot",
        "delivered": False,
        "error_code": error_code,
        "ok": False,
        "report_kind": report_kind,
        "target_kind": target_kind,
    }


def _testnet_gateway() -> BinanceSpotGateway:
    provider = KeyringSecretProvider()
    credentials = load_binance_credentials(provider, BinanceEnvironment.TESTNET)
    return BinanceSpotGateway(
        environment=BinanceEnvironment.TESTNET,
        credentials=credentials,
    )


def _live_guard() -> LiveTradingGuard:
    """创建 Binance 账户别名对应的进程内 LIVE 守卫。"""

    return LiveTradingGuard(
        TradingMode.LIVE,
        allowed_accounts=(DEFAULT_LIVE_ACCOUNT,),
        allowed_exchanges=("BINANCE",),
    )


def _live_gateway() -> BinanceSpotGateway:
    """显式加载 LIVE 凭据，绝不回退到测试网凭据名。"""

    credentials = load_binance_credentials(
        KeyringSecretProvider(),
        BinanceEnvironment.LIVE,
    )
    return BinanceSpotGateway(
        environment=BinanceEnvironment.LIVE,
        credentials=credentials,
        allow_live=True,
    )


async def _binance_live_status(symbol: str, confirmation: str) -> dict[str, object]:
    """解锁进程内守卫后执行签名 LIVE 检查。"""

    guard = _live_guard()
    guard.confirm_live_trading(confirmation)
    guard.assert_broker_operation("BINANCE", DEFAULT_LIVE_ACCOUNT, BrokerOperation.CONNECT)
    gateway = _live_gateway()
    await gateway.connect()
    try:
        guard.assert_broker_operation("BINANCE", DEFAULT_LIVE_ACCOUNT, BrokerOperation.QUERY)
        await gateway.ping()
        await gateway.synchronize_time()
        account = await gateway.account()
        ticker = await gateway.ticker_price(symbol)
        rules = await gateway.symbol_rules(symbol)
        nonzero_assets = sum(
            balance.free != 0 or balance.locked != 0 for balance in account.balances
        )
        return {
            "account_type": account.account_type,
            "can_deposit": account.can_deposit,
            "can_trade": account.can_trade,
            "can_withdraw": account.can_withdraw,
            "clock_offset_ms": gateway.server_time_offset_ms,
            "time_sync_rtt_ms": gateway.last_time_sync_rtt_ms,
            "endpoint": gateway.base_url,
            "environment": gateway.environment.value,
            "nonzero_asset_count": nonzero_assets,
            "permissions": list(account.permissions),
            "ping": "ok",
            "symbol": rules.symbol,
            "ticker_price": format(ticker.price, "f"),
        }
    finally:
        await gateway.disconnect()


async def _binance_live_balance(
    assets: Sequence[str], include_zero: bool, confirmation: str,
) -> dict[str, object]:
    """只读查询现货余额，使用十进制字符串保留币种精度。"""

    selected = {asset.strip().upper() for asset in assets}
    guard = _live_guard()
    guard.confirm_live_trading(confirmation)
    guard.assert_broker_operation("BINANCE", DEFAULT_LIVE_ACCOUNT, BrokerOperation.CONNECT)
    gateway = _live_gateway()
    try:
        await gateway.connect()
        guard.assert_broker_operation("BINANCE", DEFAULT_LIVE_ACCOUNT, BrokerOperation.QUERY)
        await gateway.synchronize_time()
        account = await gateway.account()
        rows = [
            {
                "asset": balance.asset,
                "free": format(balance.free, "f"),
                "locked": format(balance.locked, "f"),
                "total": format(balance.free + balance.locked, "f"),
            }
            for balance in account.balances
            if (not selected or balance.asset.upper() in selected)
            and (selected or include_zero or balance.free != 0 or balance.locked != 0)
        ]
        returned_assets = {balance.asset.upper() for balance in account.balances}
        return {
            "account_type": account.account_type,
            "environment": gateway.environment.value,
            "endpoint": gateway.base_url,
            "observed_at": datetime.now(UTC).isoformat(),
            "update_time_ms": account.update_time_ms,
            "clock_offset_ms": gateway.server_time_offset_ms,
            "time_sync_rtt_ms": gateway.last_time_sync_rtt_ms,
            "asset_count": len(account.balances),
            "nonzero_asset_count": sum(
                balance.free != 0 or balance.locked != 0 for balance in account.balances
            ),
            "balances": rows,
            "missing_assets": sorted(selected - returned_assets),
        }
    finally:
        await gateway.disconnect()


async def _binance_live_order_test(
    symbol: str,
    target_notional: Decimal,
    confirmation: str,
) -> dict[str, object]:
    """通过币安不进入撮合的接口校验 LIVE 订单。"""

    guard = _live_guard()
    guard.confirm_live_trading(confirmation)
    guard.assert_broker_operation("BINANCE", DEFAULT_LIVE_ACCOUNT, BrokerOperation.CONNECT)
    gateway = _live_gateway()
    await gateway.connect()
    try:
        guard.assert_broker_operation("BINANCE", DEFAULT_LIVE_ACCOUNT, BrokerOperation.QUERY)
        await gateway.synchronize_time()
        order = await _build_live_order(gateway, symbol, target_notional)
        await gateway.validate_order_on_exchange(order)
        return {
            "endpoint": gateway.base_url,
            "entered_matching_engine": False,
            "environment": gateway.environment.value,
            "order_test": "accepted",
            "symbol": order.symbol,
        }
    finally:
        await gateway.disconnect()


async def _build_live_order(
    gateway: BinanceSpotGateway,
    symbol: str,
    target_notional: Decimal,
) -> OrderIntent:
    rules = await gateway.symbol_rules(symbol)
    raw_price = (await gateway.ticker_price(symbol)).price
    price = (raw_price / rules.tick_size).to_integral_value(rounding=ROUND_FLOOR) * rules.tick_size
    notional = max(target_notional, (rules.min_notional or Decimal("0")) * Decimal("2"))
    quantity = (notional / price / rules.step_size).to_integral_value(rounding=ROUND_CEILING)
    quantity *= rules.step_size
    return OrderIntent(
        client_order_id=f"gri-live-{time.time_ns():x}-{uuid4().hex[:8]}",
        account_id=DEFAULT_LIVE_ACCOUNT,
        strategy_id="live-cli",
        symbol=rules.symbol,
        side=Side.BUY,
        quantity=quantity,
        limit_price=price,
        created_at=datetime.now(UTC),
    )


async def _binance_live_order(
    action: str,
    symbol: str,
    target_notional: Decimal,
    client_order_id: str | None,
    database: str,
    confirmation: str,
) -> dict[str, object]:
    """显式解锁后通过持久化 LIVE 服务提交或撤销订单。"""

    guard = _live_guard()
    guard.confirm_live_trading(confirmation)
    gateway = _live_gateway()
    database_path = Path(database).expanduser().resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    store = SQLiteOrderManagementStore(database_path)
    order_list_store = SQLiteSpotOrderListStore(database_path)
    service = BinanceSpotExecutionService(
        gateway,
        store,
        account_id=DEFAULT_LIVE_ACCOUNT,
        symbols=(symbol.upper(),),
        order_list_store=order_list_store,
        guard=guard,
    )
    try:
        startup = await service.start()
        if action == "submit":
            order = await _build_live_order(gateway, symbol, target_notional)
            guard.assert_broker_operation(
                "BINANCE", DEFAULT_LIVE_ACCOUNT, BrokerOperation.SUBMIT_ORDER
            )
            snapshot = await service.submit(order)
        else:
            if not client_order_id:
                raise ValueError("--client-order-id is required for cancel")
            guard.assert_broker_operation(
                "BINANCE", DEFAULT_LIVE_ACCOUNT, BrokerOperation.CANCEL_ORDER
            )
            snapshot = await service.cancel(client_order_id)
        return {
            "action": action,
            "client_order_id": snapshot.order.client_order_id,
            "database": str(database_path),
            "environment": gateway.environment.value,
            "status": snapshot.status.value,
            "reason": snapshot.reason,
            "broker_error_code": snapshot.broker_error_code,
            "startup_reconciliation": startup.reconciled_orders,
            "symbol": snapshot.order.symbol,
        }
    finally:
        if service.started:
            await service.stop()
        store.close()
        order_list_store.close()


def _live_futures_service() -> tuple[LiveTradingGuard, Any]:
    """构造受 LIVE 守卫保护的 USD-M Futures 客户端和服务。"""

    from gribuki_trade.adapters.binance import (
        BinanceFuturesRestClient,
        BinanceProduct,
        BinanceStage,
    )
    from gribuki_trade.services import BinanceFuturesExecutionService

    credentials = load_binance_credentials(
        KeyringSecretProvider(),
        BinanceEnvironment.LIVE,
    )
    client = BinanceFuturesRestClient(
        product=BinanceProduct.USDS_FUTURES,
        stage=BinanceStage.LIVE,
        credentials=credentials,
        allow_live=True,
    )
    guard = _live_guard()
    service = BinanceFuturesExecutionService(
        client,
        account_id=DEFAULT_LIVE_ACCOUNT,
        guard=guard,
    )
    return guard, service


async def _binance_live_futures_stream(
    symbols: Sequence[str],
    database: str,
    maximum_events: int | None,
    confirmation: str,
) -> dict[str, object]:
    """启动可恢复的 USD-M 私有流；本命令不提交或撤销订单。"""

    from gribuki_trade.adapters.binance import BinanceFuturesUserDataStream
    from gribuki_trade.services import BinanceFuturesUnattendedExecutionService
    from gribuki_trade.trading import FuturesOrderManagementStore

    if maximum_events is not None and maximum_events <= 0:
        raise ValueError("--max-events must be positive")
    guard, basic_service = _live_futures_service()
    guard.confirm_live_trading(confirmation)
    database_path = Path(database).expanduser().resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    stream = BinanceFuturesUserDataStream(
        basic_service.client,
        account_id=DEFAULT_LIVE_ACCOUNT,
        guard=guard,
    )
    with FuturesOrderManagementStore(database_path) as oms:
        service = BinanceFuturesUnattendedExecutionService(
            basic_service.client,
            oms,
            account_id=DEFAULT_LIVE_ACCOUNT,
            symbols=tuple(symbols),
            user_stream=stream,
            guard=guard,
        )
        try:
            startup = await service.start()
            received = await service.run_user_stream(maximum_events=maximum_events)
            return {
                "base_url": basic_service.client.base_url,
                "database": str(database_path),
                "environment": basic_service.client.stage.value,
                "product": basic_service.client.product.value,
                "received_events": received,
                "startup": {
                    "balances": startup.balances,
                    "history_algo_orders": startup.history_algo_orders,
                    "history_orders": startup.history_orders,
                    "open_algo_orders": startup.open_algo_orders,
                    "open_orders": startup.open_orders,
                    "positions": startup.positions,
                    "recovered_commands": startup.recovered_commands,
                    "unresolved_protection_plans": list(
                        startup.unresolved_protection_plans
                    ),
                },
                "status": "stream_completed",
                "symbols": list(service.symbols),
            }
        finally:
            if service.started:
                await service.stop()
            else:
                await stream.aclose()


async def _binance_live_futures_status(symbol: str, confirmation: str) -> dict[str, object]:
    """执行 USD-M Futures LIVE 的签名只读检查。"""

    guard, service = _live_futures_service()
    guard.confirm_live_trading(confirmation)
    clock_offset_ms = await service.connect()
    try:
        account = await service.account()
        positions = await service.position_risk(symbol)
        hedge_mode = await service.position_side_mode()
        ticker = await service.ticker_price(symbol)
        exchange_info = await service.exchange_info()
        assets = account.get("assets")
        declared_positions = account.get("positions")
        symbols = exchange_info.get("symbols")
        return {
            "account": {
                "asset_count": len(assets) if isinstance(assets, list) else None,
                "can_trade": account.get("canTrade"),
                "declared_position_count": (
                    len(declared_positions) if isinstance(declared_positions, list) else None
                ),
            },
            "base_url": service.client.base_url,
            "clock_offset_ms": clock_offset_ms,
            "time_sync_rtt_ms": service.client.last_time_sync_rtt_ms,
            "environment": service.client.stage.value,
            "exchange_info_symbol_count": len(symbols) if isinstance(symbols, list) else None,
            "ping": "ok",
            "position_risk_count": len(positions),
            "position_mode": "HEDGE" if hedge_mode else "ONE_WAY",
            "dual_side_position": hedge_mode,
            "product": service.client.product.value,
            "symbol": ticker.symbol,
            "ticker_price": format(ticker.price, "f"),
        }
    finally:
        await service.disconnect()


async def _binance_live_futures_balance(
    assets: Sequence[str], include_zero: bool, confirmation: str,
) -> dict[str, object]:
    """只读展示合约账户汇总和分币种余额，不对不同币种求和。"""

    selected = {asset.strip().upper() for asset in assets}
    guard, service = _live_futures_service()
    guard.confirm_live_trading(confirmation)
    try:
        clock_offset_ms = await service.connect()
        account = await service.account()
        asset_values = account.get("assets")
        if not isinstance(asset_values, list):
            raise BinanceProtocolError("Binance Futures account assets are malformed")
        amount_fields = (
            "walletBalance", "unrealizedProfit", "marginBalance", "maintMargin",
            "initialMargin", "positionInitialMargin", "openOrderInitialMargin",
            "crossWalletBalance", "crossUnPnl", "availableBalance", "maxWithdrawAmount",
        )
        rows: list[dict[str, object]] = []
        returned_assets: set[str] = set()
        nonzero_assets = 0
        wallet_nonzero_assets = 0
        for item in asset_values:
            if not isinstance(item, Mapping) or not isinstance(item.get("asset"), str):
                raise BinanceProtocolError("Binance Futures account asset is malformed")
            asset = item["asset"]
            returned_assets.add(asset.upper())
            row: dict[str, object] = {"asset": asset}
            nonzero = False
            wallet_value: Decimal | None = None
            for field in amount_fields:
                if field not in item:
                    continue
                value = _binance_balance_decimal(item[field], field)
                row[field] = format(value, "f")
                nonzero = nonzero or value != 0
                if field == "walletBalance":
                    wallet_value = value
            if "updateTime" in item:
                row["updateTime"] = item["updateTime"]
            nonzero_assets += int(nonzero)
            wallet_nonzero_assets += int(wallet_value is not None and wallet_value != 0)
            if (not selected or asset.upper() in selected) and (
                selected or include_zero or nonzero
            ):
                rows.append(row)
        total_fields = (
            "totalInitialMargin", "totalMaintMargin", "totalWalletBalance",
            "totalUnrealizedProfit", "totalMarginBalance", "totalPositionInitialMargin",
            "totalOpenOrderInitialMargin", "totalCrossWalletBalance", "totalCrossUnPnl",
            "availableBalance", "maxWithdrawAmount",
        )
        totals = {
            field: format(_binance_balance_decimal(account[field], field), "f")
            for field in total_fields if field in account
        }
        return {
            "base_url": service.client.base_url,
            "environment": service.client.stage.value,
            "product": service.client.product.value,
            "observed_at": datetime.now(UTC).isoformat(),
            "clock_offset_ms": clock_offset_ms,
            "time_sync_rtt_ms": service.client.last_time_sync_rtt_ms,
            "asset_count": len(asset_values),
            "nonzero_asset_count": nonzero_assets,
            "wallet_nonzero_asset_count": wallet_nonzero_assets,
            "nonzero_asset_definition": "any reported margin or PnL field is non-zero",
            "totals": totals,
            "totals_scope": "account",
            "assets": rows,
            "missing_assets": sorted(selected - returned_assets),
        }
    finally:
        await service.disconnect()


def _binance_balance_decimal(value: object, field: str) -> Decimal:
    """拒绝无效余额，避免把缺失、非有限数或协议变化当作零。"""

    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise BinanceProtocolError(f"Binance Futures balance field {field} is malformed") from None
    if not number.is_finite():
        raise BinanceProtocolError(f"Binance Futures balance field {field} is not finite")
    return number


async def _binance_live_futures_order_test(
    symbol: str,
    side: str,
    position_side: str | None,
    quantity: Decimal,
    order_type: str,
    price: Decimal | None,
    confirmation: str,
) -> dict[str, object]:
    """调用 USD-M Futures order/test；该接口不会创建真实订单。"""

    if order_type == "LIMIT" and price is None:
        raise ValueError("--price is required for LIMIT order tests")
    guard, service = _live_futures_service()
    guard.confirm_live_trading(confirmation)
    await service.connect()
    try:
        result = await service.validate_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            time_in_force="GTC" if order_type == "LIMIT" else None,
            position_side=position_side,
        )
        return {
            "base_url": service.client.base_url,
            "environment": service.client.stage.value,
            "order_test": "accepted",
            "creates_order": False,
            "response_fields": sorted(result),
            "symbol": symbol.upper(),
        }
    except BinanceAPIError as exc:
        return {
            "base_url": service.client.base_url,
            "environment": service.client.stage.value,
            "order_test": "rejected",
            "creates_order": False,
            "error_code": exc.code,
            "reason": str(exc),
            "symbol": symbol.upper(),
        }
    except ValueError as exc:
        return {
            "base_url": service.client.base_url,
            "environment": service.client.stage.value,
            "order_test": "rejected",
            "creates_order": False,
            "reason": str(exc),
            "symbol": symbol.upper(),
        }
    finally:
        await service.disconnect()


async def _binance_live_futures_order(
    action: str,
    symbol: str,
    side: str,
    position_side: str | None,
    quantity: Decimal,
    order_type: str,
    price: Decimal | None,
    order_id: str | None,
    client_order_id: str | None,
    confirmation: str,
) -> dict[str, object]:
    """提交或撤销一笔 USD-M Futures LIVE 订单。"""

    if action == "submit" and order_type == "LIMIT" and price is None:
        raise ValueError("--price is required for LIMIT submissions")
    if action == "cancel" and (order_id is None) == (client_order_id is None):
        raise ValueError("cancel requires exactly one of --order-id or --client-order-id")
    guard, service = _live_futures_service()
    guard.confirm_live_trading(confirmation)
    await service.connect()
    try:
        if action == "submit":
            result = await service.submit_order(
                symbol=symbol,
                side=side,
                type=order_type,
                quantity=quantity,
                price=price,
                time_in_force="GTC" if order_type == "LIMIT" else None,
                position_side=position_side,
            )
        else:
            result = await service.cancel_order(
                symbol,
                order_id=order_id,
                client_order_id=client_order_id,
            )
        return {
            "action": action,
            "base_url": service.client.base_url,
            "environment": service.client.stage.value,
            "product": service.client.product.value,
            "symbol": symbol.upper(),
            "result": result,
        }
    except BinanceAPIError as exc:
        return {
            "action": action,
            "base_url": service.client.base_url,
            "environment": service.client.stage.value,
            "product": service.client.product.value,
            "symbol": symbol.upper(),
            "status": "BROKER_REJECTED",
            "error_code": exc.code,
            "reason": str(exc),
        }
    except ValueError as exc:
        return {
            "action": action,
            "base_url": service.client.base_url,
            "environment": service.client.stage.value,
            "product": service.client.product.value,
            "symbol": symbol.upper(),
            "status": "LOCAL_REJECTED",
            "reason": str(exc),
        }
    finally:
        await service.disconnect()


async def _binance_history_sync(
    symbol: str,
    interval: str,
    days: int,
    environment_value: str,
    database: str,
) -> dict[str, object]:
    """仅将已完成公共现货行情柱采集到不可变归档。"""

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
    """在不可变归档上运行一次可复现基线回放。"""

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
    """通过纯本地 PAPER 影子引擎运行 Binance 公共数据。"""

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
            f"binance-shadow-{environment.value.lower()}-{symbol.lower()}-{interval.lower()}"
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
        return datetime.now(UTC) + timedelta(milliseconds=history_gateway.server_time_offset_ms)

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
    """探测期货模拟环境，并可选使用隔离凭据进行安全检查。"""

    from gribuki_trade.adapters.binance import (
        BinanceFuturesRestClient,
        BinanceProduct,
    )

    product = BinanceProduct(product_value)
    resolved_symbol = symbol or (
        "BTCUSDT" if product is BinanceProduct.USDS_FUTURES else "BTCUSD_PERP"
    )
    if validate_order_test and confirm != "FUTURES_DEMO_TEST":
        raise RuntimeError("--validate-order-test requires --confirm FUTURES_DEMO_TEST")
    if not validate_order_test and confirm is not None:
        raise RuntimeError("--confirm is accepted only with --validate-order-test")

    resolved_quantity = (
        quantity
        if quantity is not None
        else (Decimal("0.001") if product is BinanceProduct.USDS_FUTURES else Decimal("1"))
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
                "base_asset" if product is BinanceProduct.USDS_FUTURES else "contracts"
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
    nonzero_assets = sum(balance.free != 0 or balance.locked != 0 for balance in account.balances)
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
    price = (raw_price / rules.tick_size).to_integral_value(rounding=ROUND_FLOOR) * rules.tick_size
    notional = max(
        target_notional,
        (rules.min_notional or Decimal("0")) * Decimal("2"),
    )
    quantity = (notional / price / rules.step_size).to_integral_value(
        rounding=ROUND_CEILING
    ) * rules.step_size
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
    """构建一张限价有界且可成交的小额测试网买单。"""

    if gateway.environment is not BinanceEnvironment.TESTNET:
        raise RuntimeError("marketable smoke orders are restricted to Binance TESTNET")
    rules = await gateway.symbol_rules(symbol)
    book = await gateway.order_book(symbol, limit=5)
    if not book.asks:
        raise RuntimeError("Binance Testnet returned an empty ask book")
    best_ask = book.asks[0].price
    raw_limit_price = best_ask * Decimal("1.005")
    limit_price = (raw_limit_price / rules.tick_size).to_integral_value(
        rounding=ROUND_CEILING
    ) * rules.tick_size
    notional = max(
        target_notional,
        (rules.min_notional or Decimal("0")) * Decimal("1.10"),
    )
    quantity = (notional / limit_price / rules.step_size).to_integral_value(
        rounding=ROUND_CEILING
    ) * rules.step_size
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
            clock_ms=lambda: time.time_ns() // 1_000_000 + gateway.server_time_offset_ms,
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
    """运行一次持久化现货测试网提交/撤单/对账周期。

    本函数刻意不设环境参数。REST 网关、私有数据流与执行服务全部为测试网构造，
    并且在向 Binance 发送任何订单前先打开持久化发件箱。
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
            clock_ms=lambda: time.time_ns() // 1_000_000 + gateway.server_time_offset_ms,
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
        reconciliation, reconciliation_attempts = await _retry_testnet_reconciliation(service)
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
    """按失败关闭处理，而不分发其他 CLI 进程遗留的工作。"""

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
            "Testnet OMS database contains active or unresolved work: " + ", ".join(conflicts)
        )


async def _wait_for_testnet_fill_reports(
    reports: asyncio.Queue[BinanceExecutionReport],
    consumer: asyncio.Task[None],
    *,
    timeout_seconds: float,
) -> tuple[BinanceExecutionReport, ...]:
    """接受 NEW→TRADE 或直接进入终态 TRADE 的成交序列。"""

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
                    f"Binance Testnet order became terminal before filling: {report.status.value}"
                )


def _testnet_balance_diff(
    before: BinanceAccount,
    after: BinanceAccount,
) -> list[dict[str, str]]:
    before_values = {item.asset: (item.free, item.locked) for item in before.balances}
    after_values = {item.asset: (item.free, item.locked) for item in after.balances}
    changed: list[dict[str, str]] = []
    for asset in sorted(before_values.keys() | after_values.keys()):
        before_free, before_locked = before_values.get(asset, (Decimal("0"), Decimal("0")))
        after_free, after_locked = after_values.get(asset, (Decimal("0"), Decimal("0")))
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
    """通过仅限测试网的持久化 OMS 路径执行一笔真实虚拟成交。"""

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
            clock_ms=lambda: time.time_ns() // 1_000_000 + gateway.server_time_offset_ms,
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
        reconciliation, reconciliation_attempts = await _retry_testnet_reconciliation(service)
        final = store.require_order(order.client_order_id)
        if final.status is not OrderStatus.FILLED:
            raise RuntimeError(
                f"Binance Testnet order did not reconcile to FILLED: {final.status.value}"
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
    """等待一笔匹配的私有成交，且不接受过期报告。"""

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
    """在瞬时 HTTP 失败后重试只读最终对账。"""

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
