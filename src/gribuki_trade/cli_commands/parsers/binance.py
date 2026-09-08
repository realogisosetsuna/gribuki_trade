from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    LIVE_CONFIRMATION_PHRASE,
    Decimal,
    _add_order_arguments,
    _non_negative_decimal,
    _non_negative_integer,
    _positive_decimal,
    _positive_integer,
    _unit_fraction_decimal,
)


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 binance 命令族。"""

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
