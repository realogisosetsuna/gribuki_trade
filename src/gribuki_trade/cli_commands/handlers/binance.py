"""币安命令处理器。

处理器通过 CLI facade 读取运行时依赖，保留统一的安全守卫、凭据加载和测试替换点；
命令注册位于同级 parser 模块，避免把参数树与交易执行混在一起。
"""

from __future__ import annotations

from contextlib import suppress
from typing import Any

from gribuki_trade.cli_commands import binance_results as _results

# LIVE 处理器在独立模块实现；这里重新导出，保持 cli.py 和既有嵌入调用方的历史导入路径。
from .binance_live import (
    _binance_live_balance,
    _binance_live_futures_balance,
    _binance_live_futures_order,
    _binance_live_futures_order_test,
    _binance_live_futures_status,
    _binance_live_futures_stream,
    _binance_live_order,
    _binance_live_order_test,
    _binance_live_status,
    _build_live_order,
    _LazyCliFacade,
    _live_futures_service,
    _live_gateway,
    _live_guard,
)

__all__ = (
    "_LazyCliFacade",
    "_binance_live_balance",
    "_binance_live_futures_balance",
    "_binance_live_futures_order",
    "_binance_live_futures_order_test",
    "_binance_live_futures_status",
    "_binance_live_futures_stream",
    "_binance_live_order",
    "_binance_live_order_test",
    "_binance_live_status",
    "_build_live_order",
    "_live_futures_service",
    "_live_gateway",
    "_live_guard",
)

_binance_balance_decimal = _results._binance_balance_decimal
_testnet_balance_diff = _results._testnet_balance_diff
_testnet_reconciliation_payload = _results._testnet_reconciliation_payload

# 处理器通过延迟 CLI facade 解析兼容钩子，避免处理器被单独导入时循环依赖。
_cli: Any = _LazyCliFacade()


def _testnet_gateway() -> _cli.BinanceSpotGateway:
    provider = _cli.KeyringSecretProvider()
    credentials = _cli.load_binance_credentials(provider, _cli.BinanceEnvironment.TESTNET)
    return _cli.BinanceSpotGateway(
        environment=_cli.BinanceEnvironment.TESTNET, credentials=credentials
    )


async def _binance_history_sync(
    symbol: str, interval: str, days: int, environment_value: str, database: str
) -> dict[str, object]:
    """仅将已完成公共现货行情柱采集到不可变归档。"""

    environment = _cli.BinanceEnvironment(environment_value)
    gateway = _cli.BinanceSpotGateway(
        environment=environment, allow_live=environment is _cli.BinanceEnvironment.LIVE
    )
    await gateway.synchronize_time()
    now_ms = _cli.time.time_ns() // 1000000 + gateway.server_time_offset_ms
    start_ms = now_ms - days * 86400000
    with _cli.BinanceKlineArchive(_cli.Path(database)) as archive:
        collector = _cli.BinanceKlineCollector(
            gateway, archive, environment=environment, clock_ms=lambda: now_ms
        )
        result = await collector.sync(symbol, interval, start_time_ms=start_ms, end_time_ms=now_ms)
    return {
        "database": str(_cli.Path(database)),
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
    initial_quote: _cli.Decimal,
    fast_window: int,
    slow_window: int,
    target_position: _cli.Decimal,
    rebalance_band: _cli.Decimal,
    maker_fee: _cli.Decimal,
    taker_fee: _cli.Decimal,
    slippage: _cli.Decimal,
) -> dict[str, object]:
    """在不可变归档上运行一次可复现基线回放。"""

    if fast_window >= slow_window:
        raise ValueError("fast_window must be less than slow_window")
    trend = _cli.CryptoTrendConfig(
        fast_window=fast_window,
        slow_window=slow_window,
        minimum_history=slow_window,
        target_position_fraction=target_position,
        rebalance_tolerance_fraction=rebalance_band,
    )
    request = _cli.CryptoResearchRequest(
        environment=environment_value,
        symbol=symbol,
        interval=interval,
        base_asset=base_asset,
        quote_asset=quote_asset,
        initial_quote_balance=initial_quote,
        trend=trend,
        fees=_cli.CryptoFeeConfig(maker_rate=maker_fee, taker_rate=taker_fee),
        market_slippage_rate=slippage,
    )
    with _cli.BinanceKlineArchive(_cli.Path(database)) as archive:
        run = _cli.CryptoResearchService(archive).run(request)
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
    initial_quote: _cli.Decimal,
    fast_window: int,
    slow_window: int,
    target_position: _cli.Decimal,
    rebalance_band: _cli.Decimal,
    maximum_order_notional: _cli.Decimal,
) -> dict[str, object]:
    """通过纯本地 PAPER 影子引擎运行 Binance 公共数据。"""

    if fast_window >= slow_window:
        raise ValueError("fast_window must be less than slow_window")
    environment = _cli.BinanceEnvironment(environment_value)
    history_gateway = _cli.BinanceSpotGateway(
        environment=environment, allow_live=environment is _cli.BinanceEnvironment.LIVE
    )
    await history_gateway.synchronize_time()
    exchange_now_ms = _cli.time.time_ns() // 1000000 + history_gateway.server_time_offset_ms
    seed_rows = await history_gateway.klines(
        symbol, interval, limit=min(1000, max(slow_window + 5, 100))
    )
    closed_seed = tuple(row for row in seed_rows if row.close_time_ms < exchange_now_ms)
    if len(closed_seed) < slow_window:
        raise RuntimeError(
            f"Binance returned only {len(closed_seed)} closed seed bars; need {slow_window}"
        )
    for previous, current in zip(closed_seed, closed_seed[1:], strict=False):
        if previous.close_time_ms + 1 != current.open_time_ms:
            raise RuntimeError("Binance seed history contains a closed-kline gap")
    initial_history = _cli.binance_klines_to_crypto_bars(symbol, closed_seed[-slow_window:])
    config = _cli.BinanceShadowConfig(
        symbol=symbol,
        interval=interval,
        account_id=f"binance-shadow-{environment.value.lower()}-{symbol.lower()}-{interval.lower()}",
        initial_balances={"BTC": "0", "ETH": "0", "USDT": initial_quote},
        maximum_order_notional=maximum_order_notional,
    )
    trend = _cli.CryptoTrendConfig(
        fast_window=fast_window,
        slow_window=slow_window,
        minimum_history=slow_window,
        target_position_fraction=target_position,
        quantity_step=config.quantity_step,
        minimum_order_quantity=config.minimum_order_quantity,
        rebalance_tolerance_fraction=rebalance_band,
    )
    database_path = _cli.Path(database).expanduser().resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)

    def exchange_clock() -> _cli.datetime:
        return _cli.datetime.now(_cli.UTC) + _cli.timedelta(
            milliseconds=history_gateway.server_time_offset_ms
        )

    store = _cli.SQLiteOrderManagementStore(database_path)
    try:
        session = _cli.BinanceShadowSession.public_stream(
            store,
            environment=environment,
            config=config,
            trend_config=trend,
            initial_history=initial_history,
            clock=exchange_clock,
        )
        report = await session.run(maximum_closed_bars=None if closed_bars == 0 else closed_bars)
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
        "final_equity_quote": _cli._decimal_text(report.final_equity_quote),
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
    quantity: _cli.Decimal | None = None,
    side: str = "BUY",
) -> dict[str, object]:
    """探测期货模拟环境，并可选使用隔离凭据进行安全检查。"""
    # 在函数内部读取公开适配器，保留测试和运行时的热替换点。
    from gribuki_trade.adapters.binance import BinanceFuturesRestClient

    product = _cli.BinanceProduct(product_value)
    resolved_symbol = symbol or (
        "BTCUSDT" if product is _cli.BinanceProduct.USDS_FUTURES else "BTCUSD_PERP"
    )
    if validate_order_test and confirm != "FUTURES_DEMO_TEST":
        raise RuntimeError("--validate-order-test requires --confirm FUTURES_DEMO_TEST")
    if not validate_order_test and confirm is not None:
        raise RuntimeError("--confirm is accepted only with --validate-order-test")
    resolved_quantity = (
        quantity
        if quantity is not None
        else _cli.Decimal("0.001")
        if product is _cli.BinanceProduct.USDS_FUTURES
        else _cli.Decimal("1")
    )
    if (
        product is _cli.BinanceProduct.COIN_FUTURES
        and resolved_quantity != resolved_quantity.to_integral_value()
    ):
        raise ValueError("COIN-M quantity must be a whole contract count")
    provider = _cli.KeyringSecretProvider()
    names = _cli.binance_futures_demo_secret_names(product)
    try:
        api_key_configured = provider.get_secret(names.api_key) is not None
        secret_key_configured = provider.get_secret(names.secret_key) is not None
    except _cli.SecretProviderError:
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
        credentials = _cli.load_binance_futures_demo_credentials(provider, product)
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
            "declared_position_count": len(declared_positions)
            if isinstance(declared_positions, list)
            else None,
        }
        position_risk_count = len(positions)
    ticker = await client.ticker_price(resolved_symbol)
    exchange_info = await client.exchange_info()
    symbols = exchange_info.get("symbols")
    order_test_validated = False
    if validate_order_test:
        await client.validate_order(
            symbol=resolved_symbol, side=side, order_type="MARKET", quantity=resolved_quantity
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
            "quantity_unit": "base_asset"
            if product is _cli.BinanceProduct.USDS_FUTURES
            else "contracts",
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
    gateway = _cli._testnet_gateway()
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
    gateway: _cli.BinanceSpotGateway, symbol: str, target_notional: _cli.Decimal, *, resting: bool
) -> _cli.OrderIntent:
    rules = await gateway.symbol_rules(symbol)
    if resting:
        book = await gateway.order_book(symbol, limit=5)
        if not book.bids:
            raise RuntimeError("Binance Testnet returned an empty bid book")
        raw_price = book.bids[0].price * _cli.Decimal("0.95")
    else:
        raw_price = (await gateway.ticker_price(symbol)).price
    price = (raw_price / rules.tick_size).to_integral_value(
        rounding=_cli.ROUND_FLOOR
    ) * rules.tick_size
    notional = max(target_notional, (rules.min_notional or _cli.Decimal("0")) * _cli.Decimal("2"))
    quantity = (notional / price / rules.step_size).to_integral_value(
        rounding=_cli.ROUND_CEILING
    ) * rules.step_size
    return _cli.OrderIntent(
        client_order_id=f"gri-cli-{_cli.time.time_ns():x}-{_cli.uuid4().hex[:8]}",
        account_id=_cli.DEFAULT_TESTNET_ACCOUNT,
        strategy_id="authenticated-smoke",
        symbol=rules.symbol,
        side=_cli.Side.BUY,
        quantity=quantity,
        limit_price=price,
        created_at=_cli.datetime.now(_cli.UTC),
    )


async def _build_marketable_test_order(
    gateway: _cli.BinanceSpotGateway, symbol: str, target_notional: _cli.Decimal
) -> _cli.OrderIntent:
    """构建一张限价有界且可成交的小额测试网买单。"""
    if gateway.environment is not _cli.BinanceEnvironment.TESTNET:
        raise RuntimeError("marketable smoke orders are restricted to Binance TESTNET")
    rules = await gateway.symbol_rules(symbol)
    book = await gateway.order_book(symbol, limit=5)
    if not book.asks:
        raise RuntimeError("Binance Testnet returned an empty ask book")
    best_ask = book.asks[0].price
    raw_limit_price = best_ask * _cli.Decimal("1.005")
    limit_price = (raw_limit_price / rules.tick_size).to_integral_value(
        rounding=_cli.ROUND_CEILING
    ) * rules.tick_size
    notional = max(
        target_notional, (rules.min_notional or _cli.Decimal("0")) * _cli.Decimal("1.10")
    )
    quantity = (notional / limit_price / rules.step_size).to_integral_value(
        rounding=_cli.ROUND_CEILING
    ) * rules.step_size
    quantity = max(quantity, rules.min_quantity)
    rules.validate_limit_order(
        quantity=quantity,
        price=limit_price,
        side=_cli.Side.BUY.value,
        weighted_average_price=best_ask,
    )
    return _cli.OrderIntent(
        client_order_id=f"gri-fill-{_cli.time.time_ns():x}-{_cli.uuid4().hex[:8]}",
        account_id=_cli.DEFAULT_TESTNET_ACCOUNT,
        strategy_id="authenticated-fill-smoke",
        symbol=rules.symbol,
        side=_cli.Side.BUY,
        quantity=quantity,
        limit_price=limit_price,
        created_at=_cli.datetime.now(_cli.UTC),
    )


async def _binance_testnet_order_test(
    symbol: str, target_notional: _cli.Decimal
) -> dict[str, object]:
    gateway = _cli._testnet_gateway()
    await gateway.connect()
    try:
        await gateway.synchronize_time()
        order = await _cli._build_test_order(gateway, symbol, target_notional, resting=False)
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


async def _binance_testnet_cycle(symbol: str, target_notional: _cli.Decimal) -> dict[str, object]:
    gateway = _cli._testnet_gateway()
    await gateway.connect()
    user_stream: _cli.BinanceSpotUserDataStream | None = None
    consumer: _cli.asyncio.Task[None] | None = None
    submission_started = False
    order: _cli.OrderIntent | None = None
    try:
        await gateway.synchronize_time()
        account = await gateway.account()
        if not account.can_trade:
            raise RuntimeError("Binance Spot Testnet account cannot trade")
        order = await _cli._build_test_order(gateway, symbol, target_notional, resting=True)
        credentials = _cli.load_binance_credentials(
            _cli.KeyringSecretProvider(), _cli.BinanceEnvironment.TESTNET
        )
        user_stream = _cli.BinanceSpotUserDataStream(
            credentials,
            clock_ms=lambda: _cli.time.time_ns() // 1000000 + gateway.server_time_offset_ms,
        )
        reports: _cli.asyncio.Queue[_cli.BinanceExecutionReport] = _cli.asyncio.Queue()

        async def consume_reports() -> None:
            assert order is not None and user_stream is not None
            async for event in user_stream:
                if isinstance(event, _cli.BinanceExecutionReport) and (
                    event.client_order_id == order.client_order_id
                    or event.original_client_order_id == order.client_order_id
                ):
                    await reports.put(event)

        consumer = _cli.asyncio.create_task(consume_reports())
        async with _cli.asyncio.timeout(15):
            while user_stream.subscription_id is None:
                await _cli.asyncio.sleep(0.05)
        await gateway.submit_order(order)
        submission_started = True
        submitted_update = gateway.order_update(order.client_order_id)
        if submitted_update is None:
            raise RuntimeError("Binance Testnet submission produced no local order state")
        accepted_report = await _cli.asyncio.wait_for(reports.get(), timeout=15)
        snapshot = await gateway.query_order(order.client_order_id)
        final_report: _cli.BinanceExecutionReport | None = None
        if snapshot.status in {_cli.OrderStatus.ACCEPTED, _cli.OrderStatus.PARTIALLY_FILLED}:
            await gateway.cancel_order(order.client_order_id)
            final_report = await _cli.asyncio.wait_for(reports.get(), timeout=15)
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
            "user_stream_final_execution": final_report.execution_type
            if final_report is not None
            else None,
            "user_stream_initial_execution": accepted_report.execution_type,
            "user_stream_subscription": "active",
        }
    finally:
        if submission_started and order is not None:
            with suppress(Exception):
                snapshot = await gateway.query_order(order.client_order_id)
                if snapshot.status in {
                    _cli.OrderStatus.ACCEPTED,
                    _cli.OrderStatus.PARTIALLY_FILLED,
                }:
                    await gateway.cancel_order(order.client_order_id)
        if user_stream is not None:
            await user_stream.aclose()
        if consumer is not None:
            consumer.cancel()
            await _cli.asyncio.gather(consumer, return_exceptions=True)
        await gateway.disconnect()


async def _binance_testnet_oms_cycle(
    symbol: str, target_notional: _cli.Decimal, database: str
) -> dict[str, object]:
    """运行一次持久化现货测试网提交/撤单/对账周期。

    本函数刻意不设环境参数。REST 网关、私有数据流与执行服务全部为测试网构造，
    并且在向 Binance 发送任何订单前先打开持久化发件箱。
    """
    database_path = _cli.Path(database).expanduser().resolve()
    gateway = _cli._testnet_gateway()
    if gateway.environment is not _cli.BinanceEnvironment.TESTNET:
        raise RuntimeError("Testnet fill command cannot target Binance LIVE")
    database_path.parent.mkdir(parents=True, exist_ok=True)
    store = _cli.SQLiteOrderManagementStore(database_path)
    user_stream: _cli.BinanceSpotUserDataStream | None = None
    service: _cli.BinanceSpotTestnetExecutionService | None = None
    consumer: _cli.asyncio.Task[None] | None = None
    order: _cli.OrderIntent | None = None
    reports: _cli.asyncio.Queue[_cli.BinanceExecutionReport] = _cli.asyncio.Queue()
    try:
        await gateway.synchronize_time()
        account = await gateway.account()
        if not account.can_trade:
            raise RuntimeError("Binance Spot Testnet account cannot trade")
        order = await _cli._build_test_order(gateway, symbol, target_notional, resting=True)
        credentials = _cli.load_binance_credentials(
            _cli.KeyringSecretProvider(), _cli.BinanceEnvironment.TESTNET
        )
        user_stream = _cli.BinanceSpotUserDataStream(
            credentials,
            clock_ms=lambda: _cli.time.time_ns() // 1000000 + gateway.server_time_offset_ms,
        )
        service = _cli.BinanceSpotTestnetExecutionService(
            gateway,
            store,
            account_id=_cli.DEFAULT_TESTNET_ACCOUNT,
            symbols=(order.symbol,),
            user_stream=user_stream,
        )
        startup = await service.start()

        async def consume_private_events() -> None:
            assert order is not None and user_stream is not None and (service is not None)
            async for event in user_stream.events():
                await service.consume_user_event(event)
                if isinstance(event, _cli.BinanceExecutionReport) and (
                    event.client_order_id == order.client_order_id
                    or event.original_client_order_id == order.client_order_id
                ):
                    await reports.put(event)

        consumer = _cli.asyncio.create_task(consume_private_events())
        async with _cli.asyncio.timeout(15):
            while user_stream.subscription_id is None:
                if consumer.done():
                    await consumer
                await _cli.asyncio.sleep(0.05)
        submitted = await service.submit(order)
        new_report = await _cli._wait_for_testnet_execution(
            reports, execution_type="NEW", timeout_seconds=15
        )
        after_new = store.require_order(order.client_order_id)
        if after_new.status not in {_cli.OrderStatus.ACCEPTED, _cli.OrderStatus.PARTIALLY_FILLED}:
            raise RuntimeError(
                "resting Testnet order was not active after its NEW execution report"
            )
        canceled = await service.cancel(order.client_order_id)
        canceled_report = await _cli._wait_for_testnet_execution(
            reports, execution_type="CANCELED", timeout_seconds=15
        )
        reconciliation, reconciliation_attempts = await _cli._retry_testnet_reconciliation(service)
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
                **_cli._testnet_reconciliation_payload(reconciliation),
                "attempts": reconciliation_attempts,
            },
            "startup_reconciliation": _cli._testnet_reconciliation_payload(startup),
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
                _cli.OrderStatus.ACCEPTED,
                _cli.OrderStatus.PARTIALLY_FILLED,
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
            await _cli.asyncio.gather(consumer, return_exceptions=True)
        store.close()


def _assert_testnet_oms_database_idle(store: _cli.SQLiteOrderManagementStore) -> None:
    """按失败关闭处理，而不分发其他 CLI 进程遗留的工作。"""
    open_ids = {
        snapshot.order.client_order_id
        for snapshot in store.open_orders(account_id=_cli.DEFAULT_TESTNET_ACCOUNT)
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
    reports: _cli.asyncio.Queue[_cli.BinanceExecutionReport],
    consumer: _cli.asyncio.Task[None],
    *,
    timeout_seconds: float,
) -> tuple[_cli.BinanceExecutionReport, ...]:
    """接受 NEW→TRADE 或直接进入终态 TRADE 的成交序列。"""
    received: list[_cli.BinanceExecutionReport] = []
    async with _cli.asyncio.timeout(timeout_seconds):
        while True:
            if consumer.done():
                await consumer
                raise RuntimeError("Binance Testnet user-data stream ended before the fill")
            try:
                report = await _cli.asyncio.wait_for(reports.get(), timeout=0.25)
            except _cli.TimeoutError:
                continue
            received.append(report)
            execution_type = report.execution_type.upper()
            if execution_type == "TRADE" and report.status is _cli.OrderStatus.FILLED:
                return tuple(received)
            if report.status in {
                _cli.OrderStatus.CANCELED,
                _cli.OrderStatus.BROKER_REJECTED,
                _cli.OrderStatus.EXPIRED,
            }:
                raise RuntimeError(
                    f"Binance Testnet order became terminal before filling: {report.status.value}"
                )


async def _binance_testnet_oms_fill(
    symbol: str, target_notional: _cli.Decimal, database: str
) -> dict[str, object]:
    """通过仅限测试网的持久化 OMS 路径执行一笔真实虚拟成交。"""
    gateway = _cli._testnet_gateway()
    if gateway.environment is not _cli.BinanceEnvironment.TESTNET:
        raise RuntimeError("binance-testnet-oms-fill cannot target Binance LIVE")
    database_path = _cli.Path(database).expanduser().resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    store = _cli.SQLiteOrderManagementStore(database_path)
    user_stream: _cli.BinanceSpotUserDataStream | None = None
    service: _cli.BinanceSpotTestnetExecutionService | None = None
    consumer: _cli.asyncio.Task[None] | None = None
    order: _cli.OrderIntent | None = None
    reports: _cli.asyncio.Queue[_cli.BinanceExecutionReport] = _cli.asyncio.Queue()
    try:
        _cli._assert_testnet_oms_database_idle(store)
        await gateway.synchronize_time()
        account = await gateway.account()
        if not account.can_trade:
            raise RuntimeError("Binance Spot Testnet account cannot trade")
        normalized_symbol = symbol.strip().upper()
        credentials = _cli.load_binance_credentials(
            _cli.KeyringSecretProvider(), _cli.BinanceEnvironment.TESTNET
        )
        user_stream = _cli.BinanceSpotUserDataStream(
            credentials,
            clock_ms=lambda: _cli.time.time_ns() // 1000000 + gateway.server_time_offset_ms,
        )
        service = _cli.BinanceSpotTestnetExecutionService(
            gateway,
            store,
            account_id=_cli.DEFAULT_TESTNET_ACCOUNT,
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
        order = await _cli._build_marketable_test_order(gateway, normalized_symbol, target_notional)

        async def consume_private_events() -> None:
            assert order is not None and user_stream is not None and (service is not None)
            async for event in user_stream.events():
                await service.consume_user_event(event)
                if isinstance(event, _cli.BinanceExecutionReport) and (
                    event.client_order_id == order.client_order_id
                    or event.original_client_order_id == order.client_order_id
                ):
                    await reports.put(event)

        consumer = _cli.asyncio.create_task(consume_private_events())
        async with _cli.asyncio.timeout(15):
            while user_stream.subscription_id is None:
                if consumer.done():
                    await consumer
                await _cli.asyncio.sleep(0.05)
        submitted = await service.submit(order)
        stream_reports = await _cli._wait_for_testnet_fill_reports(
            reports, consumer, timeout_seconds=30
        )
        reconciliation, reconciliation_attempts = await _cli._retry_testnet_reconciliation(service)
        final = store.require_order(order.client_order_id)
        if final.status is not _cli.OrderStatus.FILLED:
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
        fee_totals: dict[str, _cli.Decimal] = {}
        for fill in fills:
            if fill.fee_asset is not None:
                fee_totals[fill.fee_asset] = (
                    fee_totals.get(fill.fee_asset, _cli.Decimal("0")) + fill.fee_amount
                )
        return {
            "client_order_id": order.client_order_id,
            "database": str(database_path),
            "endpoint": gateway.base_url,
            "environment": gateway.environment.value,
            "final_status": final.status.value,
            "limit_notional": format(order.quantity * order.limit_price, "f"),
            "symbol": order.symbol,
            "balance_changes": _cli._testnet_balance_diff(before_account, after_account),
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
                **_cli._testnet_reconciliation_payload(reconciliation),
                "attempts": reconciliation_attempts,
            },
            "startup_reconciliation": _cli._testnet_reconciliation_payload(startup),
            "user_stream": {
                "executions": [report.execution_type for report in stream_reports],
                "subscription": "active",
            },
        }
    finally:
        if order is not None and service is not None and service.started:
            current = store.order(order.client_order_id)
            if current is not None and current.status is _cli.OrderStatus.UNKNOWN:
                with suppress(Exception):
                    await _cli._retry_testnet_reconciliation(service)
                current = store.order(order.client_order_id)
            if current is not None and current.status in {
                _cli.OrderStatus.ACCEPTED,
                _cli.OrderStatus.PARTIALLY_FILLED,
            }:
                with suppress(Exception):
                    await service.cancel(order.client_order_id)
                    await _cli._retry_testnet_reconciliation(service)
        if service is not None and service.started:
            with suppress(Exception):
                await service.stop()
        elif user_stream is not None:
            with suppress(Exception):
                await user_stream.aclose()
        if consumer is not None:
            consumer.cancel()
            await _cli.asyncio.gather(consumer, return_exceptions=True)
        store.close()


async def _wait_for_testnet_execution(
    reports: _cli.asyncio.Queue[_cli.BinanceExecutionReport],
    *,
    execution_type: str,
    timeout_seconds: float,
) -> _cli.BinanceExecutionReport:
    """等待一笔匹配的私有成交，且不接受过期报告。"""
    async with _cli.asyncio.timeout(timeout_seconds):
        while True:
            report = await reports.get()
            if report.execution_type == execution_type:
                return report


async def _retry_testnet_reconciliation(
    service: _cli.BinanceSpotTestnetExecutionService, *, attempts: int = 3
) -> tuple[_cli.BinanceStartupReconciliation, int]:
    """在瞬时 HTTP 失败后重试只读最终对账。"""
    if attempts <= 0:
        raise ValueError("attempts must be positive")
    for attempt in range(1, attempts + 1):
        try:
            return (await service.reconcile_startup(), attempt)
        except _cli.BinanceTransportError:
            if attempt == attempts:
                raise
            await _cli.asyncio.sleep(0.25 * 2 ** (attempt - 1))
    raise _cli.AssertionError("unreachable reconciliation retry state")
