"""币安 LIVE 交易 CLI 处理器。

本模块只负责 LIVE 守卫、账户查询、订单验证与交易执行编排；测试网、
历史同步和回测命令保留在兄弟处理器中，避免把不同运行模式混在一个文件。
"""

from __future__ import annotations

from typing import Any

from gribuki_trade.cli_commands import binance_results as _results

_binance_balance_decimal = _results._binance_balance_decimal

class _LazyCliFacade:
    """延迟解析 CLI facade，允许处理器在 facade 之前被单独导入。"""

    def __getattr__(self, name: str) -> Any:
        from gribuki_trade import cli

        return getattr(cli, name)


# 通过同一个延迟 facade 读取依赖，保留既有 monkeypatch 与嵌入入口。
_cli: Any = _LazyCliFacade()

def _live_guard() -> _cli.LiveTradingGuard:
    """创建 Binance 账户别名对应的进程内 LIVE 守卫。"""
    return _cli.LiveTradingGuard(
        _cli.TradingMode.LIVE,
        allowed_accounts=(_cli.DEFAULT_LIVE_ACCOUNT,),
        allowed_exchanges=("BINANCE",),
    )


def _live_gateway() -> _cli.BinanceSpotGateway:
    """显式加载 LIVE 凭据，绝不回退到测试网凭据名。"""
    credentials = _cli.load_binance_credentials(
        _cli.KeyringSecretProvider(), _cli.BinanceEnvironment.LIVE
    )
    return _cli.BinanceSpotGateway(
        environment=_cli.BinanceEnvironment.LIVE, credentials=credentials, allow_live=True
    )


async def _binance_live_status(symbol: str, confirmation: str) -> dict[str, object]:
    """解锁进程内守卫后执行签名 LIVE 检查。"""
    guard = _cli._live_guard()
    guard.confirm_live_trading(confirmation)
    guard.assert_broker_operation(
        "BINANCE", _cli.DEFAULT_LIVE_ACCOUNT, _cli.BrokerOperation.CONNECT
    )
    gateway = _cli._live_gateway()
    await gateway.connect()
    try:
        guard.assert_broker_operation(
            "BINANCE", _cli.DEFAULT_LIVE_ACCOUNT, _cli.BrokerOperation.QUERY
        )
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
    assets: _cli.Sequence[str], include_zero: bool, confirmation: str
) -> dict[str, object]:
    """只读查询现货余额，使用十进制字符串保留币种精度。"""
    selected = {asset.strip().upper() for asset in assets}
    guard = _cli._live_guard()
    guard.confirm_live_trading(confirmation)
    guard.assert_broker_operation(
        "BINANCE", _cli.DEFAULT_LIVE_ACCOUNT, _cli.BrokerOperation.CONNECT
    )
    gateway = _cli._live_gateway()
    try:
        await gateway.connect()
        guard.assert_broker_operation(
            "BINANCE", _cli.DEFAULT_LIVE_ACCOUNT, _cli.BrokerOperation.QUERY
        )
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
            and (selected or include_zero or balance.free != 0 or (balance.locked != 0))
        ]
        returned_assets = {balance.asset.upper() for balance in account.balances}
        return {
            "account_type": account.account_type,
            "environment": gateway.environment.value,
            "endpoint": gateway.base_url,
            "observed_at": _cli.datetime.now(_cli.UTC).isoformat(),
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
    symbol: str, target_notional: _cli.Decimal, confirmation: str
) -> dict[str, object]:
    """通过币安不进入撮合的接口校验 LIVE 订单。"""
    guard = _cli._live_guard()
    guard.confirm_live_trading(confirmation)
    guard.assert_broker_operation(
        "BINANCE", _cli.DEFAULT_LIVE_ACCOUNT, _cli.BrokerOperation.CONNECT
    )
    gateway = _cli._live_gateway()
    await gateway.connect()
    try:
        guard.assert_broker_operation(
            "BINANCE", _cli.DEFAULT_LIVE_ACCOUNT, _cli.BrokerOperation.QUERY
        )
        await gateway.synchronize_time()
        order = await _cli._build_live_order(gateway, symbol, target_notional)
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
    gateway: _cli.BinanceSpotGateway, symbol: str, target_notional: _cli.Decimal
) -> _cli.OrderIntent:
    rules = await gateway.symbol_rules(symbol)
    raw_price = (await gateway.ticker_price(symbol)).price
    price = (raw_price / rules.tick_size).to_integral_value(
        rounding=_cli.ROUND_FLOOR
    ) * rules.tick_size
    notional = max(target_notional, (rules.min_notional or _cli.Decimal("0")) * _cli.Decimal("2"))
    quantity = (notional / price / rules.step_size).to_integral_value(rounding=_cli.ROUND_CEILING)
    quantity *= rules.step_size
    return _cli.OrderIntent(
        client_order_id=f"gri-live-{_cli.time.time_ns():x}-{_cli.uuid4().hex[:8]}",
        account_id=_cli.DEFAULT_LIVE_ACCOUNT,
        strategy_id="live-cli",
        symbol=rules.symbol,
        side=_cli.Side.BUY,
        quantity=quantity,
        limit_price=price,
        created_at=_cli.datetime.now(_cli.UTC),
    )


async def _binance_live_order(
    action: str,
    symbol: str,
    target_notional: _cli.Decimal,
    client_order_id: str | None,
    database: str,
    confirmation: str,
) -> dict[str, object]:
    """显式解锁后通过持久化 LIVE 服务提交或撤销订单。"""
    guard = _cli._live_guard()
    guard.confirm_live_trading(confirmation)
    gateway = _cli._live_gateway()
    database_path = _cli.Path(database).expanduser().resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    store = _cli.SQLiteOrderManagementStore(database_path)
    order_list_store = _cli.SQLiteSpotOrderListStore(database_path)
    service = _cli.BinanceSpotExecutionService(
        gateway,
        store,
        account_id=_cli.DEFAULT_LIVE_ACCOUNT,
        symbols=(symbol.upper(),),
        order_list_store=order_list_store,
        guard=guard,
    )
    try:
        startup = await service.start()
        if action == "submit":
            order = await _cli._build_live_order(gateway, symbol, target_notional)
            guard.assert_broker_operation(
                "BINANCE", _cli.DEFAULT_LIVE_ACCOUNT, _cli.BrokerOperation.SUBMIT_ORDER
            )
            snapshot = await service.submit(order)
        else:
            if not client_order_id:
                raise ValueError("--client-order-id is required for cancel")
            guard.assert_broker_operation(
                "BINANCE", _cli.DEFAULT_LIVE_ACCOUNT, _cli.BrokerOperation.CANCEL_ORDER
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


def _live_futures_service() -> tuple[_cli.LiveTradingGuard, _cli.Any]:
    """构造受 LIVE 守卫保护的 USD-M Futures 客户端和服务。"""

    credentials = _cli.load_binance_credentials(
        _cli.KeyringSecretProvider(), _cli.BinanceEnvironment.LIVE
    )
    client = _cli.BinanceFuturesRestClient(
        product=_cli.BinanceProduct.USDS_FUTURES,
        stage=_cli.BinanceStage.LIVE,
        credentials=credentials,
        allow_live=True,
    )
    guard = _cli._live_guard()
    service = _cli.BinanceFuturesExecutionService(
        client, account_id=_cli.DEFAULT_LIVE_ACCOUNT, guard=guard
    )
    return (guard, service)


async def _binance_live_futures_stream(
    symbols: _cli.Sequence[str], database: str, maximum_events: int | None, confirmation: str
) -> dict[str, object]:
    """启动可恢复的 USD-M 私有流；本命令不提交或撤销订单。"""

    if maximum_events is not None and maximum_events <= 0:
        raise ValueError("--max-events must be positive")
    guard, basic_service = _cli._live_futures_service()
    guard.confirm_live_trading(confirmation)
    database_path = _cli.Path(database).expanduser().resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    stream = _cli.BinanceFuturesUserDataStream(
        basic_service.client, account_id=_cli.DEFAULT_LIVE_ACCOUNT, guard=guard
    )
    with _cli.FuturesOrderManagementStore(database_path) as oms:
        service = _cli.BinanceFuturesUnattendedExecutionService(
            basic_service.client,
            oms,
            account_id=_cli.DEFAULT_LIVE_ACCOUNT,
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
                    "unresolved_protection_plans": list(startup.unresolved_protection_plans),
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
    guard, service = _cli._live_futures_service()
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
                "declared_position_count": len(declared_positions)
                if isinstance(declared_positions, list)
                else None,
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
    assets: _cli.Sequence[str], include_zero: bool, confirmation: str
) -> dict[str, object]:
    """只读展示合约账户汇总和分币种余额，不对不同币种求和。"""
    selected = {asset.strip().upper() for asset in assets}
    guard, service = _cli._live_futures_service()
    guard.confirm_live_trading(confirmation)
    try:
        clock_offset_ms = await service.connect()
        account = await service.account()
        asset_values = account.get("assets")
        if not isinstance(asset_values, list):
            raise _cli.BinanceProtocolError("Binance Futures account assets are malformed")
        amount_fields = (
            "walletBalance",
            "unrealizedProfit",
            "marginBalance",
            "maintMargin",
            "initialMargin",
            "positionInitialMargin",
            "openOrderInitialMargin",
            "crossWalletBalance",
            "crossUnPnl",
            "availableBalance",
            "maxWithdrawAmount",
        )
        rows: list[dict[str, object]] = []
        returned_assets: set[str] = set()
        nonzero_assets = 0
        wallet_nonzero_assets = 0
        for item in asset_values:
            if not isinstance(item, _cli.Mapping) or not isinstance(item.get("asset"), str):
                raise _cli.BinanceProtocolError("Binance Futures account asset is malformed")
            asset = item["asset"]
            returned_assets.add(asset.upper())
            row: dict[str, object] = {"asset": asset}
            nonzero = False
            wallet_value: _cli.Decimal | None = None
            for field in amount_fields:
                if field not in item:
                    continue
                value = _cli._binance_balance_decimal(item[field], field)
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
            "totalInitialMargin",
            "totalMaintMargin",
            "totalWalletBalance",
            "totalUnrealizedProfit",
            "totalMarginBalance",
            "totalPositionInitialMargin",
            "totalOpenOrderInitialMargin",
            "totalCrossWalletBalance",
            "totalCrossUnPnl",
            "availableBalance",
            "maxWithdrawAmount",
        )
        totals = {
            field: format(_cli._binance_balance_decimal(account[field], field), "f")
            for field in total_fields
            if field in account
        }
        return {
            "base_url": service.client.base_url,
            "environment": service.client.stage.value,
            "product": service.client.product.value,
            "observed_at": _cli.datetime.now(_cli.UTC).isoformat(),
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



async def _binance_live_futures_order_test(
    symbol: str,
    side: str,
    position_side: str | None,
    quantity: _cli.Decimal,
    order_type: str,
    price: _cli.Decimal | None,
    confirmation: str,
) -> dict[str, object]:
    """调用 USD-M Futures order/test；该接口不会创建真实订单。"""
    if order_type == "LIMIT" and price is None:
        raise ValueError("--price is required for LIMIT order tests")
    guard, service = _cli._live_futures_service()
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
    except _cli.BinanceAPIError as exc:
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
    quantity: _cli.Decimal,
    order_type: str,
    price: _cli.Decimal | None,
    order_id: str | None,
    client_order_id: str | None,
    confirmation: str,
) -> dict[str, object]:
    """提交或撤销一笔 USD-M Futures LIVE 订单。"""
    if action == "submit" and order_type == "LIMIT" and (price is None):
        raise ValueError("--price is required for LIMIT submissions")
    if action == "cancel" and (order_id is None) == (client_order_id is None):
        raise ValueError("cancel requires exactly one of --order-id or --client-order-id")
    guard, service = _cli._live_futures_service()
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
                symbol, order_id=order_id, client_order_id=client_order_id
            )
        return {
            "action": action,
            "base_url": service.client.base_url,
            "environment": service.client.stage.value,
            "product": service.client.product.value,
            "symbol": symbol.upper(),
            "result": result,
        }
    except _cli.BinanceAPIError as exc:
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


