"""命令注册共享的常量、参数转换器和类型。"""

from __future__ import annotations

import argparse
from decimal import Decimal

from gribuki_trade.adapters.binance import (
    BINANCE_COIN_FUTURES_DEMO_API_KEY_SECRET,
    BINANCE_COIN_FUTURES_DEMO_SECRET_KEY_SECRET,
    BINANCE_LIVE_API_KEY_SECRET,
    BINANCE_LIVE_SECRET_KEY_SECRET,
    BINANCE_TESTNET_API_KEY_SECRET,
    BINANCE_TESTNET_SECRET_KEY_SECRET,
    BINANCE_USDS_FUTURES_DEMO_API_KEY_SECRET,
    BINANCE_USDS_FUTURES_DEMO_SECRET_KEY_SECRET,
)
from gribuki_trade.adapters.llm import DEEPSEEK_API_KEY_SECRET, OPENAI_API_KEY_SECRET
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
from gribuki_trade.napcat_setup import NAPCAT_WEBUI_TOKEN_SECRET
from gribuki_trade.reporting.contracts import ReportKind

DEFAULT_ASHARE_WATCHLIST = "config/ashare_research_watchlist.toml"
LIVE_CONFIRMATION_PHRASE = "ENABLE LIVE TRADING"
TAVILY_API_KEY_SECRET = "search.tavily.api_key"
SEARXNG_BEARER_TOKEN_SECRET = "search.searxng.bearer_token"
KNOWN_SECRET_NAMES = (
    BINANCE_TESTNET_API_KEY_SECRET, BINANCE_TESTNET_SECRET_KEY_SECRET,
    BINANCE_LIVE_API_KEY_SECRET, BINANCE_LIVE_SECRET_KEY_SECRET,
    BINANCE_USDS_FUTURES_DEMO_API_KEY_SECRET, BINANCE_USDS_FUTURES_DEMO_SECRET_KEY_SECRET,
    BINANCE_COIN_FUTURES_DEMO_API_KEY_SECRET, BINANCE_COIN_FUTURES_DEMO_SECRET_KEY_SECRET,
    SCHWAB_CLIENT_ID_SECRET, SCHWAB_CLIENT_SECRET_SECRET, DEEPSEEK_API_KEY_SECRET,
    OPENAI_API_KEY_SECRET, NAPCAT_ACCESS_TOKEN_SECRET, TAVILY_API_KEY_SECRET,
    SEARXNG_BEARER_TOKEN_SECRET,
)

__all__ = [
    "argparse", "Decimal", "ReportKind", "_add_order_arguments", "_iso_date",
    "_iso_datetime", "_macro_weight_decimal", "_non_negative_decimal",
    "_non_negative_float", "_non_negative_integer", "_positive_decimal",
    "_positive_float", "_positive_integer", "_positive_integer_or_unlimited",
    "_unit_fraction_decimal", "DEFAULT_ASHARE_WATCHLIST", "LIVE_CONFIRMATION_PHRASE",
    "KNOWN_SECRET_NAMES", "SCHWAB_OAUTH_TOKEN_SECRET", "NAPCAT_WEBUI_TOKEN_SECRET",
]
