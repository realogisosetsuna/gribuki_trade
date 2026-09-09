"""用于本地集成检查的运维命令行入口。"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections.abc import Awaitable, Mapping, Sequence
from contextlib import contextmanager, suppress  # noqa: F401 - handler facade hook
from datetime import UTC, date, datetime, timedelta
from datetime import time as datetime_time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4  # noqa: F401 - lazy handler facade compatibility
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
    BinanceFuturesRestClient,
    BinanceFuturesUserDataStream,
    BinanceKlineArchive,
    BinanceProduct,
    BinanceProtocolError,
    BinanceSpotGateway,
    BinanceSpotUserDataStream,
    BinanceStage,
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
from gribuki_trade.backtest import CryptoFeeConfig
from gribuki_trade.cli_commands.ashare_paper_day_results import (  # noqa: F401
    _ashare_paper_day_report,
    _ashare_paper_day_status,
    _ashare_paper_day_summary,
    _latest_paper_day_artifact_event_status,
    _non_negative_int_or_none,
    _paper_day_delivery_projection,
    _paper_day_delivery_sidecar_projection,
    _PaperDayResultProjectionSource,
)
from gribuki_trade.cli_commands.close_research_payloads import (
    _close_batch_result_summary,
    _daily_evidence_provider_id,
    _instrument_profile_document,
    _paper_session_instrument_profiles,  # noqa: F401 - historical CLI facade export
)
from gribuki_trade.cli_commands.handlers.ashare import (
    _ashare_bars,
    _ashare_daily,
    _ashare_intraday_run_json,
    _ashare_research_runs,
    _ashare_screening_run_json,
    _ashare_snapshot,
    _ashare_source_health,
    _ashare_watchlist,
)
from gribuki_trade.cli_commands.handlers.ashare_news import (
    _ashare_disclosures,
    _ashare_news,
    _ashare_news_watch,
)
from gribuki_trade.cli_commands.handlers.ashare_paper_day import (  # noqa: F401
    _ashare_paper_day,
    _load_ashare_paper_day_calendar_window,
    _PaperDayCLIError,
    _run_ashare_paper_day,
    _verify_ashare_paper_day_calendar,
)  # noqa: F401
from gribuki_trade.cli_commands.handlers.ashare_paper_day_run import (  # noqa: F401
    _paper_day_resume_config_compatible,
    _run_ashare_paper_day_owned,
)
from gribuki_trade.cli_commands.handlers.ashare_post_close import (  # noqa: F401
    _ashare_post_close,
    _ashare_post_close_report,
    _ashare_post_close_run,
    _ashare_post_close_status,
    _CLIExistingCloseResearch,
    _deliver_post_close_artifact,
    _post_close_delivery_summary,
    _post_close_napcat_ready,
    _post_close_process_lock,
    _post_close_section_excerpt,
    _post_close_sha256,
    _read_post_close_json,
    _run_post_close_orchestrator,
    _validate_post_close_target,
    _validated_post_close_artifact,
)
from gribuki_trade.cli_commands.handlers.ashare_research import (  # noqa: F401
    _ashare_research_watch,
    _resolve_ashare_research_symbols,
    _resolve_dynamic_research_symbols,
)
from gribuki_trade.cli_commands.handlers.ashare_review import (
    _ashare_candidates,
    _ashare_review,
)
from gribuki_trade.cli_commands.handlers.binance import (
    _assert_testnet_oms_database_idle,
    _binance_backtest,
    _binance_balance_decimal,
    _binance_futures_demo_status,
    _binance_history_sync,
    _binance_live_balance,
    _binance_live_futures_balance,
    _binance_live_futures_order,
    _binance_live_futures_order_test,
    _binance_live_futures_status,
    _binance_live_futures_stream,
    _binance_live_order,
    _binance_live_order_test,
    _binance_live_status,
    _binance_shadow_run,
    _binance_testnet_cycle,
    _binance_testnet_oms_cycle,
    _binance_testnet_oms_fill,
    _binance_testnet_order_test,
    _binance_testnet_status,
    _build_live_order,
    _build_marketable_test_order,
    _build_test_order,
    _live_futures_service,
    _live_gateway,
    _live_guard,
    _retry_testnet_reconciliation,
    _testnet_balance_diff,
    _testnet_gateway,
    _testnet_reconciliation_payload,
    _wait_for_testnet_execution,
    _wait_for_testnet_fill_reports,
)
from gribuki_trade.cli_commands.handlers.napcat import (
    _napcat_configure,
    _napcat_dispatch,
    _napcat_send_artifact,
    _napcat_send_test,
    _napcat_status,
)
from gribuki_trade.cli_commands.handlers.strategy_lab import (
    _strategy_exit_evaluate,
    _strategy_factor_discover,
)
from gribuki_trade.cli_commands.live_sync_payloads import (
    _append_live_immediate_protection_receipt,
    _live_cycle_payload,
    _live_immediate_protection_payload,
    _live_ingest_payload,
    _live_status_payload,
)
from gribuki_trade.cli_commands.post_close_results import (  # noqa: F401 - facade exports
    _post_close_analysis_outcome,
    _post_close_completed_result,
    _post_close_error,
    _post_close_mapping,
    _post_close_optional_decimal,
    _post_close_optional_integer,
    _post_close_optional_string,
    _post_close_skipped,
    _post_close_string_tuple,
    _PostCloseCLIError,
)
from gribuki_trade.cli_commands.runtime import (
    _apply_integration_runtime_defaults,
    _configure_terminal_encoding,
    _optional_local_secret,
    _required_local_secret,
    _secret_status,
    _set_secret,
    _sqlite_runtime_status,
    _temp_root,
)
from gribuki_trade.cli_output import (
    _atomic_write_cli_json,
    _decimal_text,
    _three_decimal_text,
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
    load_integration_settings,  # noqa: F401 - historical monkeypatch surface
)
from gribuki_trade.runtime.integration_settings import (
    validate_model_id as validate_runtime_model_id,
)
from gribuki_trade.security import (
    KeyringSecretProvider,  # noqa: F401 - historical NapCat monkeypatch surface
    SecretProviderError,
)
from gribuki_trade.security.post_close_urls import (  # noqa: F401 - facade exports
    PostCloseSearxngURLValidationError,
    validate_post_close_searxng_url,
)
from gribuki_trade.services.binance_execution import (
    BinanceSpotExecutionService,
    BinanceSpotTestnetExecutionService,
    BinanceStartupReconciliation,
)
from gribuki_trade.services.binance_futures_execution import BinanceFuturesExecutionService
from gribuki_trade.services.binance_futures_unattended import (
    BinanceFuturesUnattendedExecutionService,
)
from gribuki_trade.services.binance_shadow import BinanceShadowConfig, BinanceShadowSession
from gribuki_trade.services.crypto_research import (
    CryptoResearchRequest,
    CryptoResearchService,
    binance_klines_to_crypto_bars,
)
from gribuki_trade.sqlite_runtime import sqlite_runtime_status  # noqa: F401 - facade hook
from gribuki_trade.strategy import CryptoTrendConfig
from gribuki_trade.trading import (
    FuturesOrderManagementStore,
    SQLiteOrderManagementStore,
    SQLiteSpotOrderListStore,
)

_BINANCE_HANDLER_EXPORTS = (
    _testnet_gateway,
    _live_guard,
    _live_gateway,
    _binance_live_status,
    _binance_live_balance,
    _binance_live_order_test,
    _build_live_order,
    _binance_live_order,
    _live_futures_service,
    _binance_live_futures_stream,
    _binance_live_futures_status,
    _binance_live_futures_balance,
    _binance_balance_decimal,
    _binance_live_futures_order_test,
    _binance_live_futures_order,
    _binance_history_sync,
    _binance_backtest,
    _binance_shadow_run,
    _binance_futures_demo_status,
    _binance_testnet_status,
    _build_test_order,
    _build_marketable_test_order,
    _binance_testnet_order_test,
    _binance_testnet_cycle,
    _binance_testnet_oms_cycle,
    _assert_testnet_oms_database_idle,
    _wait_for_testnet_fill_reports,
    _testnet_balance_diff,
    _binance_testnet_oms_fill,
    _wait_for_testnet_execution,
    _testnet_reconciliation_payload,
    _retry_testnet_reconciliation,
)

_CLI_BINANCE_COMPATIBILITY = (
    BinanceAccount,
    BinanceAPIError,
    BinanceEnvironment,
    BinanceExecutionReport,
    BinanceFuturesRestClient,
    BinanceFuturesUserDataStream,
    BinanceKlineArchive,
    BinanceProduct,
    BinanceProtocolError,
    BinanceSpotGateway,
    BinanceSpotUserDataStream,
    BinanceStage,
    BinanceTransportError,
    BinanceSpotExecutionService,
    BinanceSpotTestnetExecutionService,
    BinanceStartupReconciliation,
    SQLiteOrderManagementStore,
    SQLiteSpotOrderListStore,
)

_CLI_LEGACY_COMPATIBILITY = (
    LiveTradingGuard,
    TradingMode,
    BrokerOperation,
    OrderIntent,
    OrderStatus,
    binance_futures_demo_secret_names,
    load_binance_credentials,
    load_binance_futures_demo_credentials,
    FuturesOrderManagementStore,
    BinanceFuturesUserDataStream,
    BinanceFuturesExecutionService,
    BinanceFuturesUnattendedExecutionService,
    # 处理器通过 facade 访问该异常类型，保留历史兼容属性。
    InvalidOperation,
    BinanceKlineArchive,
    BinanceShadowConfig,
    BinanceShadowSession,
    CryptoTrendConfig,
    CryptoResearchRequest,
    CryptoResearchService,
    CryptoFeeConfig,
    binance_klines_to_crypto_bars,
    time,
    ROUND_FLOOR,
    ROUND_CEILING,
    Any,
)

if TYPE_CHECKING:
    from gribuki_trade.ports.ashare_screening import AsyncAShareScreeningData
    from gribuki_trade.ports.ashare_surveillance import AsyncAShareIntradayUniverseData

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

_CLI_PARSING_COMPATIBILITY = (
    LIVE_CONFIRMATION_PHRASE,
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


def build_parser() -> argparse.ArgumentParser:
    """兼容入口；命令树由按职责拆分的注册模块组装。"""

    from gribuki_trade.cli_commands.parser import build_parser as _build_parser

    return _build_parser()



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
                return _live_status_payload(snapshot, tracking, work_items)
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
            result = _live_ingest_payload(outcome)
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
            result = _live_immediate_protection_payload(
                protection_id=protection_id,
                protection_work_id=protection_work_id,
                deep_work=deep_work,
                quick=quick,
                work=work,
                tracking_state=tracking_state,
                tracking_result=tracking_result,
                quick_ok=quick_ok,
                tracking_ok=tracking_ok,
                notification_target_fallback=target_fallback,
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

    return _live_cycle_payload(
        build_work=build_work,
        deep_work=deep_work,
        close_work=close_work,
        tracking_runs=tracking_runs,
        dispatch=dispatch,
        dispatch_failed=dispatch_failed,
        deep_timeout_seconds=deep_timeout_seconds,
    )


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
