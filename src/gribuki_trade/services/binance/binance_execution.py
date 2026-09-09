"""可恢复的 Binance 现货测试网执行编排。

本服务刻意只允许测试网。它将 Binance REST 与用户数据适配器连接到持久化 SQLite OMS，
不增加实盘开关或端点回退。每次提交都在调用适配器前存储并获取租约；无法证明的结果
会成为 ``UNKNOWN``，只能由权威交易所事件或 REST 对账解决。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime

from gribuki_trade.adapters.binance import (
    BinanceAccount,
    BinanceAPIError,
    BinanceBalanceUpdate,
    BinanceEnvironment,
    BinanceEventStreamTerminated,
    BinanceExecutionReport,
    BinanceListStatus,
    BinanceOrderListSnapshot,
    BinanceOrderSnapshot,
    BinanceOutboundAccountPosition,
    BinanceTrade,
    BinanceUserDataEvent,
)
from gribuki_trade.domain.orders import OrderIntent, OrderStatus
from gribuki_trade.runtime import BrokerOperation, LiveTradingGuard
from gribuki_trade.trading import (
    AssetBalance,
    BalanceValue,
    ExecutionFill,
    OrderSnapshot,
    SpotOrderListRecord,
    SQLiteOrderManagementStore,
    SQLiteSpotOrderListStore,
    TradingCommand,
    TradingCommandStatus,
    TradingCommandType,
)

from .binance_execution_models import (
    BinanceSpotExecutionGateway,
    BinanceStartupReconciliation,
    BinanceTestnetOnlyError,
    BinanceUserDataSource,
)
from .binance_execution_policy import (
    environment_of as _environment_of,
)
from .binance_execution_policy import (
    merge_exchange_orders as _merge_exchange_orders_policy,
)
from .binance_execution_policy import (
    normalize_now as _normalize_now,
)
from .binance_execution_policy import (
    validate_order as _validate_order_policy,
)
from .binance_execution_records import (
    average_price as _average_price,
)
from .binance_execution_records import (
    balance_digest as _balance_digest,
)
from .binance_execution_records import (
    datetime_from_ms as _datetime_from_ms,
)
from .binance_execution_records import (
    fill_from_trade as _fill_from_trade,
)
from .binance_execution_records import (
    milliseconds as _milliseconds,
)
from .binance_execution_records import (
    order_list_rank as _order_list_rank,
)
from .binance_execution_records import (
    reconciliation_event_id as _reconciliation_event_id,
)
from .binance_execution_records import (
    spot_order_list_record as _spot_order_list_record,
)


class BinanceSpotTestnetExecutionService:
    """持久化、提交、监听并对账 Binance 现货测试网订单。

    构造器不存在实盘逃生口。两个具体适配器都必须声明 ``TESTNET``，且每张订单都必须
    属于已配置的本地账户与标的允许列表。
    """

    def __init__(
        self,
        gateway: BinanceSpotExecutionGateway,
        oms: SQLiteOrderManagementStore,
        *,
        account_id: str,
        symbols: Sequence[str],
        user_stream: BinanceUserDataSource | None = None,
        order_list_store: SQLiteSpotOrderListStore | None = None,
        clock: Callable[[], datetime] | None = None,
        _allow_live: bool = False,
        guard: LiveTradingGuard | None = None,
        exchange: str = "BINANCE",
    ) -> None:
        _require_binance_environment(gateway, "gateway", allow_live=_allow_live)
        if user_stream is not None:
            _require_binance_environment(user_stream, "user_stream", allow_live=_allow_live)
        if _allow_live and guard is None:
            raise ValueError("a LiveTradingGuard is required for live Binance execution")
        normalized_account = account_id.strip()
        if not normalized_account:
            raise ValueError("account_id must not be empty")
        normalized_symbols = tuple(dict.fromkeys(symbol.strip().upper() for symbol in symbols))
        if not normalized_symbols or any(not symbol for symbol in normalized_symbols):
            raise ValueError("symbols must contain at least one non-empty symbol")

        self._gateway = gateway
        self._oms = oms
        self._account_id = normalized_account
        self._symbols = normalized_symbols
        self._symbol_set = frozenset(normalized_symbols)
        self._user_stream = user_stream
        self._order_list_store = order_list_store
        self._clock = clock or (lambda: datetime.now(UTC))
        self._guard = guard
        self._exchange = exchange
        self._operation_lock = asyncio.Lock()
        self._started = False
        self._stopping = False

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def symbols(self) -> tuple[str, ...]:
        return self._symbols

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> BinanceStartupReconciliation:
        """连接并对账所有权威来源，然后发送安全的待处理工作。"""

        async with self._operation_lock:
            if self._started:
                raise RuntimeError("Binance Testnet execution service is already started")
            self._stopping = False
            self._assert_operation(BrokerOperation.CONNECT)
            recovered = self._oms.recover_after_restart(
                now=self._now(),
                account_id=self._account_id,
                symbols=self._symbols,
            )
            await self._gateway.connect()
            try:
                result = await self._reconcile_startup(recovered_commands=len(recovered))
                if result.unresolved_order_ids:
                    dispatched = 0
                else:
                    dispatched = await self._dispatch_pending()
            except BaseException:
                await self._gateway.disconnect()
                raise
            self._started = True
            unresolved = tuple(
                order.order.client_order_id
                for order in self._oms.orders_requiring_reconciliation(
                    account_id=self._account_id, symbols=self._symbols
                )
            )
            return replace(
                result,
                dispatched_pending_commands=dispatched,
                unresolved_order_ids=unresolved,
            )

    async def stop(self) -> None:
        """停止私有数据流并断开测试网 REST 适配器。"""

        self._stopping = True
        if self._user_stream is not None:
            await self._user_stream.aclose()
        async with self._operation_lock:
            await self._gateway.disconnect()
            self._started = False

    async def submit(self, order: OrderIntent) -> OrderSnapshot:
        """调用测试网 REST 前以原子方式创建订单并获取租约。"""

        self._validate_order(order)
        async with self._operation_lock:
            self._require_started()
            self._assert_operation(BrokerOperation.SUBMIT_ORDER, account_id=order.account_id)
            persisted = self._oms.create_order(order)
            command = self._submit_command(order.client_order_id)
            if command.status is TradingCommandStatus.PENDING:
                claimed = self._oms.claim_command(command.command_id, now=self._now())
                await self._dispatch_submit(claimed)
            return self._oms.require_order(persisted.order.client_order_id)

    async def cancel(self, client_order_id: str) -> OrderSnapshot:
        """跨越 REST 边界前持久化一次测试网撤单并获取租约。"""

        async with self._operation_lock:
            self._require_started()
            current = self._oms.require_order(client_order_id)
            self._validate_order(current.order)
            self._assert_operation(BrokerOperation.CANCEL_ORDER)
            existing = self._command(f"cancel:{client_order_id}")
            if existing is not None:
                # SENT 与 UNKNOWN 在此处都是最终交付决定：二者都不允许盲目发送第二次撤单请求。
                return current
            if current.status not in {
                OrderStatus.ACCEPTED,
                OrderStatus.PARTIALLY_FILLED,
            }:
                raise ValueError(
                    f"order {client_order_id!r} cannot be canceled from {current.status.value}"
                )
            command = self._oms.enqueue_cancel(
                client_order_id,
                occurred_at=self._now(),
            )
            claimed = self._oms.claim_command(command.command_id, now=self._now())
            await self._dispatch_cancel(claimed)
            return self._oms.require_order(client_order_id)

    async def reconcile_startup(self) -> BinanceStartupReconciliation:
        """不分发命令地重复执行四来源 REST 对账。"""

        async with self._operation_lock:
            self._require_started()
            self._assert_operation(BrokerOperation.QUERY)
            return await self._reconcile_startup(recovered_commands=0)

    async def consume_user_event(self, event: BinanceUserDataEvent) -> bool:
        """持久化一条私有流事件，并返回其是否影响本账户。"""

        async with self._operation_lock:
            self._require_started()
            if isinstance(event, BinanceExecutionReport):
                return self._consume_execution_report(event)
            if isinstance(event, BinanceListStatus):
                self._record_list_status(event)
                # 订单列表事件是各成员订单状态的边界；通过同一事务边界的
                # REST 对账刷新每个子订单，避免把列表状态误投影为单个成交状态。
                await self._reconcile_startup(recovered_commands=0)
                return True
            if isinstance(event, BinanceOutboundAccountPosition):
                self._consume_account_position(event)
                return True
            if isinstance(event, BinanceBalanceUpdate):
                await self._refresh_balances_for_delta(event)
                return True
            if isinstance(event, BinanceEventStreamTerminated):
                return False
        raise TypeError(f"unsupported Binance user-data event: {type(event).__name__}")

    async def run_user_stream(self, *, maximum_events: int | None = None) -> int:
        """消费已配置私有数据流，直至停止或达到测试限制。"""

        self._require_started()
        self._assert_operation(BrokerOperation.SUBSCRIBE)
        if self._user_stream is None:
            raise RuntimeError("no Binance user-data stream is configured")
        if maximum_events is not None and maximum_events <= 0:
            raise ValueError("maximum_events must be positive")
        received = 0
        iterator = self._user_stream.events()
        observed_epoch = getattr(self._user_stream, "connection_epoch", None)
        try:
            async for event in iterator:
                if self._stopping:
                    break
                current_epoch = getattr(self._user_stream, "connection_epoch", None)
                if (
                    observed_epoch is not None
                    and current_epoch is not None
                    and current_epoch != observed_epoch
                ):
                    reconciliation = await self.reconcile_startup()
                    if reconciliation.unresolved_order_ids:
                        raise RuntimeError(
                            "Binance private-stream reconnection left unresolved orders"
                        )
                    observed_epoch = current_epoch
                await self.consume_user_event(event)
                received += 1
                if maximum_events is not None and received >= maximum_events:
                    break
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()
        return received

    async def _dispatch_pending(self) -> int:
        dispatched = 0
        while True:
            claimed = self._oms.claim_commands(
                now=self._now(),
                limit=1,
                account_id=self._account_id,
                symbols=self._symbols,
            )
            if not claimed:
                return dispatched
            command = claimed[0]
            if command.command_type is TradingCommandType.SUBMIT_ORDER:
                await self._dispatch_submit(command)
            elif command.command_type is TradingCommandType.CANCEL_ORDER:
                await self._dispatch_cancel(command)
            else:
                self._oms.mark_command_unknown(
                    command.command_id,
                    occurred_at=self._now(),
                    error_code="unsupported_command_type",
                )
                raise RuntimeError(
                    "Binance Testnet execution service received an unknown command type"
                )
            dispatched += 1

    async def _dispatch_submit(self, command: TradingCommand) -> None:
        self._assert_operation(BrokerOperation.SUBMIT_ORDER)
        order = self._oms.require_order(command.client_order_id).order
        try:
            await self._gateway.submit_order(order)
            update = self._gateway.order_update(order.client_order_id)
            if update is None or update.status in {
                OrderStatus.CREATED,
                OrderStatus.VALIDATED,
                OrderStatus.SUBMITTING,
                OrderStatus.CANCEL_PENDING,
                OrderStatus.UNKNOWN,
            }:
                self._oms.mark_command_unknown(
                    command.command_id,
                    occurred_at=self._now(),
                    error_code="ambiguous_submission",
                )
                return
            if update.order != order:
                raise RuntimeError("Binance gateway returned an update for a different order")
            occurred_at = update.occurred_at or self._now()
            self._oms.record_order_update(
                order.client_order_id,
                event_id=(f"binance-rest-submit:{command.command_id}:{command.attempt_count}"),
                status=update.status,
                occurred_at=occurred_at,
                filled_quantity=update.executed_quantity,
                exchange_order_id=update.exchange_order_id,
                reason=update.reason,
                broker_error_code=update.error_code,
            )
            # 在确认持久化命令前先持久化交易所结果。
            self._oms.mark_command_sent(command.command_id, occurred_at=occurred_at)
        except asyncio.CancelledError:
            self._quarantine_in_flight(command, "submission_cancelled")
            raise
        except BaseException:
            self._quarantine_in_flight(command, "submission_exception")
            raise

    async def _dispatch_cancel(self, command: TradingCommand) -> None:
        self._assert_operation(BrokerOperation.CANCEL_ORDER)
        try:
            order = self._oms.require_order(command.client_order_id).order
            snapshot = await self._gateway.cancel_order_by_client_id(
                order.symbol,
                command.client_order_id,
            )
            if snapshot.symbol != order.symbol or (
                snapshot.client_order_id is not None
                and snapshot.client_order_id != command.client_order_id
            ):
                raise RuntimeError("Binance gateway returned a cancel result for another order")
            if snapshot.status in {
                OrderStatus.CREATED,
                OrderStatus.VALIDATED,
                OrderStatus.SUBMITTING,
                OrderStatus.CANCEL_PENDING,
                OrderStatus.UNKNOWN,
            }:
                self._oms.mark_command_unknown(
                    command.command_id,
                    occurred_at=self._now(),
                    error_code="ambiguous_cancellation",
                )
                return
            occurred_at = _datetime_from_ms(snapshot.transact_time_ms, fallback=self._now())
            self._oms.record_order_update(
                command.client_order_id,
                event_id=(f"binance-rest-cancel:{command.command_id}:{command.attempt_count}"),
                status=snapshot.status,
                occurred_at=occurred_at,
                filled_quantity=snapshot.executed_quantity,
                average_fill_price=_average_price(snapshot),
                exchange_order_id=snapshot.order_id,
            )
            self._oms.mark_command_sent(command.command_id, occurred_at=occurred_at)
        except asyncio.CancelledError:
            self._quarantine_in_flight(command, "cancellation_cancelled")
            raise
        except BaseException:
            self._quarantine_in_flight(command, "cancellation_exception")
            raise

    async def _reconcile_startup(self, *, recovered_commands: int) -> BinanceStartupReconciliation:
        self._assert_operation(BrokerOperation.QUERY)
        now = self._now()
        account = await self._gateway.account()
        balances = self._record_account_snapshot(account, now=now, source="startup")
        open_orders = await self._gateway.open_orders()
        histories: list[BinanceOrderSnapshot] = []
        trades: list[BinanceTrade] = []
        for symbol in self._symbols:
            histories.extend(await self._gateway.all_orders(symbol, limit=1_000))
            trades.extend(await self._gateway.account_trades(symbol, limit=1_000))

        by_client_id = self._merge_exchange_orders((*histories, *open_orders))
        candidates = {
            snapshot.order.client_order_id: snapshot
            for snapshot in self._oms.open_orders(account_id=self._account_id)
            if snapshot.status not in {OrderStatus.CREATED, OrderStatus.VALIDATED}
        }
        candidates.update(
            {
                snapshot.order.client_order_id: snapshot
                for snapshot in self._oms.orders_requiring_reconciliation(
                    account_id=self._account_id, symbols=self._symbols
                )
            }
        )

        for client_order_id, candidate_snapshot in candidates.items():
            if client_order_id in by_client_id:
                continue
            try:
                exact = await self._gateway.get_order(
                    candidate_snapshot.order.symbol,
                    client_order_id=client_order_id,
                )
            except BinanceAPIError as error:
                if error.code == -2013:
                    continue
                raise
            if exact.status is not OrderStatus.UNKNOWN:
                by_client_id[client_order_id] = exact

        reconciled = 0
        order_id_to_client: dict[int, str] = {}
        for client_order_id, exchange in by_client_id.items():
            local_snapshot: OrderSnapshot | None = self._oms.order(client_order_id)
            if local_snapshot is None or local_snapshot.order.account_id != self._account_id:
                continue
            if exchange.status is OrderStatus.UNKNOWN:
                continue
            occurred_at = _datetime_from_ms(exchange.transact_time_ms, fallback=now)
            average = _average_price(exchange)
            self._oms.reconcile_order(
                client_order_id,
                status=exchange.status,
                occurred_at=occurred_at,
                filled_quantity=exchange.executed_quantity,
                average_fill_price=average,
                exchange_order_id=exchange.order_id,
                reason="Binance Testnet startup reconciliation",
                event_id=_reconciliation_event_id(exchange, occurred_at),
            )
            reconciled += 1
            if exchange.order_id is not None:
                order_id_to_client[exchange.order_id] = client_order_id

        known_fill_ids = {fill.fill_id for fill in self._oms.fills()}
        recorded_fills = 0
        for trade in trades:
            trade_client_order_id = order_id_to_client.get(trade.order_id)
            if trade_client_order_id is None:
                continue
            local_order = self._oms.require_order(trade_client_order_id).order
            fill = _fill_from_trade(trade, local_order)
            self._oms.record_fill(fill)
            if fill.fill_id not in known_fill_ids:
                known_fill_ids.add(fill.fill_id)
                recorded_fills += 1

        unresolved = tuple(
            order.order.client_order_id
            for order in self._oms.orders_requiring_reconciliation(
                account_id=self._account_id, symbols=self._symbols
            )
        )
        list_count, reconciled_lists = await self._reconcile_order_lists(now=now)
        return BinanceStartupReconciliation(
            recovered_commands=recovered_commands,
            exchange_open_orders=len(open_orders),
            exchange_history_orders=len(histories),
            exchange_trades=len(trades),
            reconciled_orders=reconciled,
            recorded_fills=recorded_fills,
            recorded_balances=len(balances),
            unresolved_order_ids=unresolved,
            exchange_order_lists=list_count,
            reconciled_order_lists=reconciled_lists,
        )

    async def _reconcile_order_lists(self, *, now: datetime) -> tuple[int, int]:
        """从公开的订单列表 REST 快照恢复独立列表投影。"""

        if self._order_list_store is None:
            return 0, 0
        open_reader = getattr(self._gateway, "open_order_lists", None)
        history_reader = getattr(self._gateway, "all_order_lists", None)
        if not callable(open_reader) or not callable(history_reader):
            raise RuntimeError("Binance gateway lacks order-list reconciliation routes")
        snapshots = list(await open_reader())
        snapshots.extend(await history_reader(limit=1_000))
        latest: dict[str, tuple[BinanceOrderListSnapshot, SpotOrderListRecord]] = {}
        for snapshot in snapshots:
            record = _spot_order_list_record(
                self._account_id, snapshot, source="REST", occurred_at=now
            )
            current = latest.get(record.key)
            if current is None or _order_list_rank(record) >= _order_list_rank(current[1]):
                latest[record.key] = (snapshot, record)
        persisted = 0
        for record in (value[1] for value in latest.values()):
            event_id = (
                f"binance-order-list-rest:{record.key}:{record.transaction_time_ms}:"
                f"{record.list_order_status}"
            )
            if self._order_list_store.upsert(
                record,
                event_id=event_id,
                event_type="ORDER_LIST_RECONCILED",
                occurred_at=now,
            ):
                persisted += 1
        return len(latest), persisted

    def _record_list_status(self, event: BinanceListStatus) -> None:
        """先落盘 listStatus，再进行成员订单 REST 对账。"""

        if self._order_list_store is None:
            return
        record = SpotOrderListRecord(
            account_id=self._account_id,
            order_list_id=event.order_list_id,
            list_client_order_id=event.list_client_order_id,
            symbol=event.symbol,
            contingency_type=event.contingency_type,
            list_status_type=event.list_status_type,
            list_order_status=event.list_order_status,
            order_ids=event.order_ids,
            client_order_ids=event.client_order_ids,
            updated_at=_datetime_from_ms(event.transaction_time_ms, fallback=self._now()),
            transaction_time_ms=event.transaction_time_ms,
            source="USER_STREAM",
        )
        self._order_list_store.upsert(
            record,
            event_id=(
                f"binance-list-status:{event.symbol}:{event.order_list_id}:"
                f"{event.transaction_time_ms}:{event.list_status_type}:{event.list_order_status}"
            ),
            event_type="LIST_STATUS",
            payload={
                "event_time_ms": event.event_time_ms,
                "transaction_time_ms": event.transaction_time_ms,
                "order_ids": list(event.order_ids),
                "client_order_ids": list(event.client_order_ids),
            },
            occurred_at=record.updated_at,
        )

    def _consume_execution_report(self, event: BinanceExecutionReport) -> bool:
        client_order_id = self._local_client_order_id(event)
        if client_order_id is None:
            return False
        current = self._oms.require_order(client_order_id)
        order = current.order
        if event.symbol != order.symbol or event.side is not order.side:
            raise ValueError("Binance executionReport identity does not match the local order")
        occurred_at = _datetime_from_ms(event.transaction_time_ms, fallback=self._now())
        average = (
            event.cumulative_quote_quantity / event.cumulative_filled_quantity
            if event.cumulative_filled_quantity > 0 and event.cumulative_quote_quantity > 0
            else None
        )
        event_id = (
            f"binance-execution:{event.symbol}:{event.order_id}:"
            f"{event.execution_id}:{event.execution_type}"
        )
        reason = (
            event.reject_reason
            if event.reject_reason and event.reject_reason.upper() != "NONE"
            else None
        )
        if current.status is OrderStatus.UNKNOWN and event.status is not OrderStatus.UNKNOWN:
            self._oms.reconcile_order(
                client_order_id,
                status=event.status,
                occurred_at=occurred_at,
                filled_quantity=event.cumulative_filled_quantity,
                average_fill_price=average,
                exchange_order_id=event.order_id,
                reason=reason,
                event_id=event_id,
            )
        else:
            self._oms.record_order_update(
                client_order_id,
                event_id=event_id,
                status=event.status,
                occurred_at=occurred_at,
                filled_quantity=event.cumulative_filled_quantity,
                average_fill_price=average,
                exchange_order_id=event.order_id,
                reason=reason,
            )

        if event.execution_type.upper() == "TRADE":
            if event.last_executed_quantity <= 0 or event.last_executed_price <= 0:
                raise ValueError("Binance TRADE report must contain a positive fill")
            fill_id = (
                f"binance-spot:{event.symbol}:trade:{event.trade_id}"
                if event.trade_id >= 0
                else (
                    f"binance-spot:{event.symbol}:order:{event.order_id}:"
                    f"execution:{event.execution_id}"
                )
            )
            self._oms.record_fill(
                ExecutionFill(
                    fill_id=fill_id,
                    client_order_id=client_order_id,
                    account_id=self._account_id,
                    symbol=event.symbol,
                    side=event.side,
                    quantity=event.last_executed_quantity,
                    price=event.last_executed_price,
                    occurred_at=occurred_at,
                    fee_asset=event.commission_asset,
                    fee_amount=event.commission_amount,
                    exchange_order_id=str(event.order_id),
                )
            )
        return True

    def _consume_account_position(self, event: BinanceOutboundAccountPosition) -> None:
        balances = tuple(
            BalanceValue(item.asset, item.free, item.locked) for item in event.balances
        )
        occurred_at = _datetime_from_ms(event.last_account_update_ms, fallback=self._now())
        digest = _balance_digest(balances)
        self._oms.record_balance_snapshot(
            self._account_id,
            balances,
            event_id=(
                f"binance-balance-position:{event.event_time_ms}:"
                f"{event.last_account_update_ms}:{digest}"
            ),
            occurred_at=occurred_at,
            full_snapshot=False,
        )

    async def _refresh_balances_for_delta(self, event: BinanceBalanceUpdate) -> None:
        account = await self._gateway.account()
        fallback = _datetime_from_ms(event.clear_time_ms, fallback=self._now())
        self._record_account_snapshot(
            account,
            now=fallback,
            source=f"delta-{event.asset}-{event.clear_time_ms}",
        )

    def _record_account_snapshot(
        self, account: BinanceAccount, *, now: datetime, source: str
    ) -> tuple[AssetBalance, ...]:
        balances = tuple(
            BalanceValue(item.asset, item.free, item.locked) for item in account.balances
        )
        occurred_at = _datetime_from_ms(account.update_time_ms, fallback=now)
        digest = _balance_digest(balances)
        return self._oms.record_balance_snapshot(
            self._account_id,
            balances,
            event_id=f"binance-account-{source}:{_milliseconds(occurred_at)}:{digest}",
            occurred_at=occurred_at,
            full_snapshot=True,
        )

    def _merge_exchange_orders(
        self, orders: Sequence[BinanceOrderSnapshot]
    ) -> dict[str, BinanceOrderSnapshot]:
        return _merge_exchange_orders_policy(orders)

    def _local_client_order_id(self, event: BinanceExecutionReport) -> str | None:
        if self._is_owned_order(event.client_order_id):
            return event.client_order_id
        original = event.original_client_order_id
        if original is not None and self._is_owned_order(original):
            return original
        return None

    def _is_owned_order(self, client_order_id: str) -> bool:
        snapshot = self._oms.order(client_order_id)
        return snapshot is not None and (
            snapshot.order.account_id == self._account_id
            and snapshot.order.symbol in self._symbol_set
        )

    def _submit_command(self, client_order_id: str) -> TradingCommand:
        command = self._command(f"submit:{client_order_id}")
        if command is not None:
            return command
        raise RuntimeError("durable submit command disappeared")

    def _command(self, command_id: str) -> TradingCommand | None:
        return next(
            (command for command in self._oms.commands() if command.command_id == command_id),
            None,
        )

    def _quarantine_in_flight(self, command: TradingCommand, error_code: str) -> None:
        current = next(
            (value for value in self._oms.commands() if value.command_id == command.command_id),
            None,
        )
        if current is not None and current.status is TradingCommandStatus.IN_FLIGHT:
            self._oms.mark_command_unknown(
                command.command_id,
                occurred_at=self._now(),
                error_code=error_code,
            )

    def _validate_order(self, order: OrderIntent) -> None:
        _validate_order_policy(order, account_id=self._account_id, symbols=self._symbol_set)

    def _require_started(self) -> None:
        if not self._started:
            raise RuntimeError("Binance execution service is not started")

    def _assert_operation(
        self,
        operation: BrokerOperation,
        *,
        account_id: str | None = None,
    ) -> None:
        if self._guard is None:
            return
        self._guard.assert_broker_operation(
            self._exchange,
            account_id or self._account_id,
            operation,
        )

    def _now(self) -> datetime:
        return _normalize_now(self._clock())


def _require_testnet(component: object, label: str) -> None:
    try:
        environment = _environment_of(component, label, allow_live=True)
    except ValueError:
        raise BinanceTestnetOnlyError(
            f"{label} must explicitly advertise Binance TESTNET"
        ) from None
    if environment is not BinanceEnvironment.TESTNET:
        raise BinanceTestnetOnlyError(
            f"{label} targets {environment.value}; this service is TESTNET-only"
        )


def _require_binance_environment(component: object, label: str, *, allow_live: bool) -> None:
    try:
        environment = _environment_of(component, label, allow_live=allow_live)
    except ValueError:
        raise BinanceTestnetOnlyError(
            f"{label} must explicitly advertise Binance TESTNET or LIVE"
        ) from None
    if environment is BinanceEnvironment.LIVE and not allow_live:
        raise BinanceTestnetOnlyError(
            f"{label} targets LIVE; this service is TESTNET-only; use "
            "BinanceSpotExecutionService with a LiveTradingGuard"
        )


class BinanceSpotExecutionService(BinanceSpotTestnetExecutionService):
    """受守卫保护的 Binance 现货执行服务，支持测试网或显式解锁的实盘。

    该服务复用测试网服务的持久化 OMS 流程。只有网关和用户数据流明确指向
    LIVE，且进程内的 :class:`LiveTradingGuard` 允许每项操作时，才会访问实盘。
    """

    def __init__(
        self,
        gateway: BinanceSpotExecutionGateway,
        oms: SQLiteOrderManagementStore,
        *,
        account_id: str,
        symbols: Sequence[str],
        guard: LiveTradingGuard,
        user_stream: BinanceUserDataSource | None = None,
        order_list_store: SQLiteSpotOrderListStore | None = None,
        clock: Callable[[], datetime] | None = None,
        exchange: str = "BINANCE",
    ) -> None:
        super().__init__(
            gateway,
            oms,
            account_id=account_id,
            symbols=symbols,
            user_stream=user_stream,
            order_list_store=order_list_store,
            clock=clock,
            _allow_live=True,
            guard=guard,
            exchange=exchange,
        )


__all__ = [
    "BinanceSpotExecutionGateway",
    "BinanceStartupReconciliation",
    "BinanceTestnetOnlyError",
    "BinanceUserDataSource",
    "BinanceSpotTestnetExecutionService",
    "BinanceSpotExecutionService",
]
