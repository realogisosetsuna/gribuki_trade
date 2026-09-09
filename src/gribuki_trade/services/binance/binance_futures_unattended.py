"""无人值守 USDⓈ-M 合约执行、私有流和持久化恢复服务。"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from gribuki_trade.adapters.binance.futures.client import BinanceFuturesRestClient
from gribuki_trade.adapters.binance.futures.user_stream import (
    BinanceFuturesUserDataStream,
    normalize_futures_user_event,
)
from gribuki_trade.adapters.binance.futures.user_stream import (
    FuturesUserEvent as AdapterFuturesUserEvent,
)
from gribuki_trade.runtime.guard import BrokerOperation, LiveTradingGuard
from gribuki_trade.services.binance.binance_futures_unattended_models import (
    FuturesStartupReconciliation,
)
from gribuki_trade.trading.futures.futures_models import (
    FuturesBalanceSnapshot,
    FuturesCommandStatus,
    FuturesConfigSnapshot,
    FuturesFill,
    FuturesOrderKind,
    FuturesOrderSnapshot,
    FuturesOrderStatus,
    FuturesPositionSnapshot,
    FuturesUserEvent,
)
from gribuki_trade.trading.futures.futures_oms import FuturesOrderManagementStore

__all__ = ["BinanceFuturesUnattendedExecutionService", "FuturesStartupReconciliation"]


class BinanceFuturesUnattendedExecutionService:
    """把合约 REST、私有流和 Futures OMS 组合成失败关闭式边界。"""

    def __init__(
        self,
        client: BinanceFuturesRestClient,
        oms: FuturesOrderManagementStore,
        *,
        account_id: str,
        symbols: Sequence[str],
        user_stream: BinanceFuturesUserDataStream,
        guard: LiveTradingGuard | None = None,
        exchange: str = "BINANCE",
        owner_id: str | None = None,
        clock: Any | None = None,
    ) -> None:
        account = account_id.strip()
        if not account:
            raise ValueError("account_id must not be blank")
        selected = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols))
        if not selected or any(not symbol for symbol in selected):
            raise ValueError("symbols must contain at least one non-empty symbol")
        if client.profile.is_live and guard is None:
            raise ValueError("LIVE Futures execution requires a LiveTradingGuard")
        self._client = client
        self._oms = oms
        self._account_id = account
        self._symbols = selected
        self._user_stream = user_stream
        self._guard = guard
        self._exchange = exchange
        self._owner_id = owner_id or f"futures-{uuid4().hex}"
        self._clock = clock or (lambda: datetime.now(UTC))
        self._started = False
        self._stopping = False
        self._fencing_token: int | None = None
        self._operation_lock = asyncio.Lock()
        self._last_epoch = 0
        self._lease_task: asyncio.Task[None] | None = None

    @property
    def client(self) -> BinanceFuturesRestClient:
        return self._client

    @property
    def oms(self) -> FuturesOrderManagementStore:
        return self._oms

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def symbols(self) -> tuple[str, ...]:
        return self._symbols

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> FuturesStartupReconciliation:
        """取得单进程租约、恢复 UNKNOWN 命令、对账后再打开私有流。"""

        async with self._operation_lock:
            if self._started:
                raise RuntimeError("Futures unattended service is already started")
            self._stopping = False
            self._assert(BrokerOperation.CONNECT)
            scope = self._scope()
            self._fencing_token = self._oms.acquire_owner(
                account_id=self._account_id,
                environment=scope[0],
                product=scope[1],
                owner_id=self._owner_id,
                now=self._now(),
            )
            recovered = self._oms.recover_inflight(
                account_id=self._account_id,
                environment=scope[0],
                product=scope[1],
            )
            await self._client.ping()
            await self._client.synchronize_time()
            try:
                result = await self.reconcile()
                await self._user_stream.connect()
                wait_ready = getattr(self._user_stream, "wait_until_ready", None)
                if wait_ready is not None:
                    await wait_ready()
            except BaseException:
                with suppress(RuntimeError):
                    self._oms.release_owner(
                        account_id=self._account_id, environment=scope[0], product=scope[1],
                        owner_id=self._owner_id, fencing_token=self._fencing_token,
                    )
                self._fencing_token = None
                raise
            self._started = True
            self._mark_health("CONNECTED", None, self._last_epoch)
            self._lease_task = asyncio.create_task(self._renew_lease_loop())
            return FuturesStartupReconciliation(
                recovered_commands=len(recovered),
                open_orders=result.open_orders,
                open_algo_orders=result.open_algo_orders,
                history_orders=result.history_orders,
                history_algo_orders=result.history_algo_orders,
                positions=result.positions,
                balances=result.balances,
                unresolved_protection_plans=result.unresolved_protection_plans,
            )

    async def stop(self) -> None:
        """先关闭私有流，再释放本地运行状态。"""

        self._stopping = True
        if self._lease_task is not None:
            self._lease_task.cancel()
            await asyncio.gather(self._lease_task, return_exceptions=True)
            self._lease_task = None
        await self._user_stream.aclose()
        async with self._operation_lock:
            if self._fencing_token is not None:
                scope = self._scope()
                self._oms.release_owner(
                    account_id=self._account_id,
                    environment=scope[0],
                    product=scope[1],
                    owner_id=self._owner_id,
                    fencing_token=self._fencing_token,
                )
            self._started = False
            self._fencing_token = None

    async def reconcile(self) -> FuturesStartupReconciliation:
        """从账户、仓位、普通订单和 Algo 历史重建本地快照。"""

        self._assert(BrokerOperation.QUERY)
        scope = self._scope()
        account = await self._client.account()
        balances = self._record_account(account)
        positions = await self._client.position_risk()
        position_count = self._record_positions(positions, replace=True)
        open_orders = await self._client.open_orders()
        open_algos = await self._client.open_algo_orders()
        history_orders: list[dict[str, Any]] = []
        history_algos: list[dict[str, Any]] = []
        for symbol in self._symbols:
            history_orders.extend(await self._client.all_orders(symbol, limit=1_000))
            history_algos.extend(await self._client.all_algo_orders(symbol, limit=1_000))
        for item in (*history_orders, *open_orders):
            self._record_order(item, kind=FuturesOrderKind.NORMAL)
        for item in (*history_algos, *open_algos):
            self._record_order(item, kind=FuturesOrderKind.ALGO)
        unresolved = tuple(
            plan.plan_id
            for plan in self._oms.protection_plans(
                account_id=self._account_id, environment=scope[0], product=scope[1]
            )
            if plan.coverage_state not in {"COVERED", "CLOSED"}
        )
        self._last_epoch = getattr(self._user_stream, "connection_epoch", self._last_epoch)
        return FuturesStartupReconciliation(
            recovered_commands=0,
            open_orders=len(open_orders),
            open_algo_orders=len(open_algos),
            history_orders=len(history_orders),
            history_algo_orders=len(history_algos),
            positions=position_count,
            balances=balances,
            unresolved_protection_plans=unresolved,
        )

    async def run_user_stream(self, *, maximum_events: int | None = None) -> int:
        """消费私有流；换代后先对账，未知事件使服务降级。"""

        if not self._started:
            raise RuntimeError("Futures unattended service is not started")
        self._assert(BrokerOperation.SUBSCRIBE)
        if maximum_events is not None and maximum_events <= 0:
            raise ValueError("maximum_events must be positive")
        received = 0
        async for event in self._user_stream.events():
            if self._stopping:
                break
            if event.connection_epoch != self._last_epoch:
                self._mark_health("RECONCILING", "connection_epoch_changed", event.connection_epoch)
                result = await self.reconcile()
                self._last_epoch = event.connection_epoch
                if result.unresolved_protection_plans:
                    self._mark_health(
                        "DEGRADED", "unresolved_protection_plan", event.connection_epoch
                    )
                    raise RuntimeError("Futures stream recovery left protection plans unresolved")
            await self.consume_user_event(event)
            received += 1
            if maximum_events is not None and received >= maximum_events:
                break
        return received

    async def consume_user_event(self, event: AdapterFuturesUserEvent) -> bool:
        """原子写入私有事件，再更新订单、成交、余额或仓位投影。"""

        normalized = normalize_futures_user_event(event)
        scope = self._scope()
        durable = FuturesUserEvent(
            event_type=event.event_type,
            event_time_ms=event.event_time_ms,
            transaction_time_ms=event.transaction_time_ms,
            received_time_ms=event.received_time_ms,
            payload=event.payload,
            connection_epoch=event.connection_epoch,
        )
        if not self._oms.append_event(
            durable,
            account_id=self._account_id,
            environment=scope[0],
            product=scope[1],
        ):
            return False
        kind = normalized["kind"]
        if kind in {"order", "algo"}:
            payload = normalized.get("payload")
            order_payload = payload.get("o") if isinstance(payload, Mapping) else None
            item = dict(order_payload) if isinstance(order_payload, Mapping) else dict(normalized)
            item["T"] = normalized.get("transaction_time_ms") or normalized["event_time_ms"]
            self._record_order_event(item, kind)
        elif kind == "trade":
            payload = normalized.get("payload")
            item = dict(payload) if isinstance(payload, Mapping) else dict(normalized)
            item["T"] = normalized.get("transaction_time_ms") or normalized["event_time_ms"]
            item["x"] = "TRADE"
            item.setdefault("z", item.get("q"))
            self._record_order_event(item, "normal")
        elif kind == "account":
            self._record_account_event(normalized)
        elif kind == "config":
            self._record_config_event(normalized)
        elif kind in {"margin_call", "trigger_reject", "expired", "unknown"}:
            self._mark_health("DEGRADED", f"{event.event_type}", event.connection_epoch)
        return True

    async def submit_order(
        self, *, command_id: str | None = None, **kwargs: object
    ) -> dict[str, Any]:
        """通过 Futures OMS 发出普通订单，未知结果绝不自动重试。"""

        return await self._submit_durable(
            "SUBMIT_ORDER", kwargs, FuturesOrderKind.NORMAL, command_id=command_id
        )

    async def submit_algo_order(
        self, *, command_id: str | None = None, **kwargs: object
    ) -> dict[str, Any]:
        """通过 Futures OMS 发出 Algo Order，保存独立算法身份。"""

        return await self._submit_durable(
            "SUBMIT_ALGO", kwargs, FuturesOrderKind.ALGO, command_id=command_id
        )

    async def cancel_algo_order(
        self, symbol: str, *, algo_id: int | str | None = None, client_algo_id: str | None = None
    ) -> dict[str, Any]:
        """以持久化命令方式撤销一个 Algo Order。"""

        if (algo_id is None) == (client_algo_id is None):
            raise ValueError("provide exactly one of algo_id or client_algo_id")
        payload = {"symbol": symbol, "algo_id": algo_id, "client_algo_id": client_algo_id}
        return await self._submit_durable(
            "CANCEL_ALGO", payload, FuturesOrderKind.ALGO,
            command_id=f"cancel_algo:{symbol}:{algo_id or client_algo_id}",
        )

    async def _submit_durable(
        self,
        command_type: str,
        payload: Mapping[str, object],
        kind: FuturesOrderKind,
        *,
        command_id: str | None = None,
    ) -> dict[str, Any]:
        if not self._started:
            raise RuntimeError("Futures unattended service is not started")
        self._assert(
            BrokerOperation.SUBMIT_ORDER
            if command_type.startswith("SUBMIT")
            else BrokerOperation.CANCEL_ORDER
        )
        if not self._user_stream.connected or not self._user_stream.healthy:
            raise RuntimeError("Futures private stream is not healthy; order changes are paused")
        scope = self._scope()
        command_payload = dict(payload)
        if command_type == "SUBMIT_ORDER" and not any(
            command_payload.get(key)
            for key in ("new_client_order_id", "client_order_id", "newClientOrderId")
        ):
            command_payload["new_client_order_id"] = f"gri-fut-{uuid4().hex[:24]}"
        if command_type == "SUBMIT_ALGO" and not any(
            command_payload.get(key) for key in ("client_algo_id", "clientAlgoId")
        ):
            command_payload["client_algo_id"] = f"gri-algo-{uuid4().hex[:24]}"
        command_id = command_id or _stable_command_id(command_type, command_payload)
        command = self._oms.enqueue_command(
            account_id=self._account_id,
            environment=scope[0],
            product=scope[1],
            command_id=command_id,
            command_type=command_type,
            payload=command_payload,
            occurred_at=self._now(),
        )
        if command.status is not FuturesCommandStatus.PENDING:
            raise RuntimeError(
                f"durable Futures command {command.command_id!r} is already "
                f"{command.status.value}; reconcile before retrying"
            )
        if self._fencing_token is None:
            raise RuntimeError("Futures OMS owner lease is not available")
        claimed = self._oms.claim_command(
            command.command_id,
            account_id=self._account_id,
            environment=scope[0],
            product=scope[1],
            owner_id=self._owner_id,
            fencing_token=self._fencing_token,
            now=self._now(),
        )
        if claimed is None:
            raise RuntimeError("Futures command was not claimable")
        try:
            if command_type == "SUBMIT_ORDER":
                result = await self._client.submit_order(**command_payload)
            elif command_type == "SUBMIT_ALGO":
                result = await self._client.submit_algo_order(**command_payload)
            else:
                algo_id = payload.get("algo_id")
                client_algo_id = payload.get("client_algo_id")
                result = await self._client.cancel_algo_order(
                    str(payload["symbol"]),
                    algo_id=algo_id if isinstance(algo_id, (int, str)) else None,
                    client_algo_id=client_algo_id if isinstance(client_algo_id, str) else None,
                )
            self._record_order(result, kind=kind)
            self._oms.finish_command(
                command.command_id,
                account_id=self._account_id,
                environment=scope[0],
                product=scope[1],
                owner_id=self._owner_id,
                fencing_token=self._fencing_token,
                status=FuturesCommandStatus.SENT,
                now=self._now(),
            )
            return result
        except asyncio.CancelledError:
            self._finish_unknown(command.command_id, "cancelled")
            raise
        except BaseException:
            self._finish_unknown(command.command_id, "ambiguous_or_rejected")
            raise

    def _finish_unknown(self, command_id: str, error_code: str) -> None:
        scope = self._scope()
        if self._fencing_token is None:
            return
        with suppress(RuntimeError, KeyError):
            self._oms.finish_command(
                command_id,
                account_id=self._account_id,
                environment=scope[0],
                product=scope[1],
                owner_id=self._owner_id,
                fencing_token=self._fencing_token,
                status=FuturesCommandStatus.UNKNOWN,
                now=self._now(),
                error_code=error_code,
            )

    def _record_order(self, item: Mapping[str, Any], *, kind: FuturesOrderKind) -> None:
        symbol = str(item.get("symbol") or item.get("s") or "").upper()
        if not symbol or symbol not in self._symbols:
            return
        order_id = item.get("orderId", item.get("i", item.get("actualOrderId", item.get("ai"))))
        algo_id = item.get("algoId", item.get("aid"))
        order_key = f"algo:{algo_id}" if algo_id is not None else f"order:{order_id}"
        if order_id is None and algo_id is None:
            client = item.get("clientOrderId", item.get("c", item.get("clientAlgoId")))
            if client is None:
                return
            order_key = f"client:{client}"
        status_value = str(item.get("status", item.get("X", item.get("algoStatus", "NEW"))))
        status = _order_status(status_value)
        snapshot = FuturesOrderSnapshot(
            account_id=self._account_id,
            environment=self._scope()[0],
            product=self._scope()[1],
            order_key=order_key,
            symbol=symbol,
            side=str(item.get("side", item.get("S", ""))),
            position_side=str(item.get("positionSide", item.get("ps", "BOTH"))),
            kind=kind,
            status=status,
            client_order_id=_str(item.get("clientOrderId", item.get("c"))),
            exchange_order_id=_str(order_id),
            algo_id=_str(algo_id),
            client_algo_id=_str(item.get("clientAlgoId", item.get("caid"))),
            parent_order_key=self._parent_order_key(
                symbol=symbol,
                order_id=_str(order_id),
                kind=kind,
            ),
            order_type=_str(item.get("type", item.get("o", item.get("orderType")))),
            execution_type=_str(item.get("executionType", item.get("x"))),
            quantity=_decimal(item.get("origQty", item.get("q", item.get("quantity")))),
            filled_quantity=_decimal(
                item.get("executedQty", item.get("z", item.get("executedQuantity")))
            ),
            average_price=_decimal_or_none(item.get("avgPrice", item.get("ap"))),
            trigger_price=_decimal_or_none(
                item.get("triggerPrice", item.get("tp", item.get("sp")))
            ),
            activate_price=_decimal_or_none(item.get("activatePrice", item.get("AP"))),
            callback_rate=_decimal_or_none(item.get("callbackRate", item.get("cr"))),
            reduce_only=_bool(item.get("reduceOnly", item.get("R"))),
            close_position=_bool(item.get("closePosition", item.get("cp"))),
            working_type=_str(item.get("workingType", item.get("wt"))),
            realized_pnl=_decimal(item.get("realizedProfit", item.get("rp"))),
            status_time_ms=_int(item.get("updateTime", item.get("T")), default=self._now_ms()),
            updated_at=self._now(),
            extra={"source": dict(item)},
        )
        self._oms.upsert_order(snapshot)
        if (
            kind is FuturesOrderKind.NORMAL
            and str(item.get("executionType", item.get("x", ""))).upper() == "TRADE"
        ):
            quantity = _decimal_or_none(item.get("lastFilledQuantity", item.get("l")))
            price = _decimal_or_none(item.get("lastFilledPrice", item.get("L")))
            if quantity is not None and price is not None and quantity > 0 and price > 0:
                trade_id = _str(item.get("tradeId", item.get("t")))
                self._oms.record_fill(
                    FuturesFill(
                        account_id=self._account_id,
                        environment=self._scope()[0],
                        product=self._scope()[1],
                        fill_id=f"{order_key}:trade:{trade_id or item.get('T')}",
                        symbol=symbol,
                        side=str(item.get("side", item.get("S", ""))),
                        position_side=str(item.get("positionSide", item.get("ps", "BOTH"))),
                        quantity=quantity,
                        price=price,
                        trade_id=trade_id,
                        order_key=order_key,
                        exchange_order_id=_str(order_id),
                        fee_asset=_str(item.get("commissionAsset", item.get("N"))),
                        fee_amount=_decimal(item.get("commission", item.get("n"))),
                        realized_pnl=_decimal(item.get("realizedProfit", item.get("rp"))),
                        occurred_at=self._datetime_ms(
                            _int(item.get("tradeTime", item.get("T")), default=self._now_ms())
                        ),
                        extra={"source": dict(item)},
                    )
                )

    def _parent_order_key(
        self, *, symbol: str, order_id: str | None, kind: FuturesOrderKind
    ) -> str | None:
        """关联 Algo 记录产生的实际普通订单，避免保护链断裂。"""

        if kind is FuturesOrderKind.ALGO or order_id is None:
            return None
        scope = self._scope()
        for candidate in self._oms.orders(
            account_id=self._account_id,
            environment=scope[0],
            product=scope[1],
            kind=FuturesOrderKind.ALGO,
        ):
            if candidate.symbol == symbol and candidate.exchange_order_id == order_id:
                return candidate.order_key
        return None

    def _record_order_event(self, event: Mapping[str, Any], kind: str) -> None:
        self._record_order(
            event, kind=FuturesOrderKind.ALGO if kind == "algo" else FuturesOrderKind.NORMAL
        )

    def _record_account(self, account: Mapping[str, Any]) -> int:
        assets = account.get("assets")
        if not isinstance(assets, list):
            raise ValueError("Futures account assets are malformed")
        snapshots = []
        for item in assets:
            if isinstance(item, Mapping) and item.get("asset"):
                snapshots.append(
                    FuturesBalanceSnapshot(
                        account_id=self._account_id,
                        environment=self._scope()[0],
                        product=self._scope()[1],
                        asset=str(item["asset"]),
                        wallet_balance=_decimal(item.get("walletBalance")),
                        available_balance=_decimal(item.get("availableBalance")),
                        cross_wallet_balance=_decimal(item.get("crossWalletBalance")),
                        updated_at=self._now(),
                        extra=dict(item),
                    )
                )
        self._oms.record_balances(snapshots, full_snapshot=True)
        return len(snapshots)

    def _record_positions(
        self, positions: Sequence[Mapping[str, Any]], *, replace: bool = False
    ) -> int:
        snapshots: list[FuturesPositionSnapshot] = []
        count = 0
        for item in positions:
            symbol = str(item.get("symbol", item.get("s", ""))).upper()
            if symbol not in self._symbols:
                continue
            snapshot = FuturesPositionSnapshot(
                account_id=self._account_id,
                environment=self._scope()[0],
                product=self._scope()[1],
                symbol=symbol,
                position_side=str(item.get("positionSide", item.get("ps", "BOTH"))),
                quantity=_decimal(item.get("positionAmt", item.get("pa"))),
                entry_price=_decimal(item.get("entryPrice", item.get("ep"))),
                break_even_price=_decimal(item.get("breakEvenPrice", item.get("bep"))),
                realized_pnl=_decimal(item.get("realizedProfit", item.get("cr"))),
                unrealized_pnl=_decimal(item.get("unRealizedProfit", item.get("up"))),
                margin_type=_str(item.get("marginType", item.get("mt"))),
                isolated_wallet=_decimal(item.get("isolatedWallet", item.get("iw"))),
                leverage=_int_or_none(item.get("leverage")),
                updated_at=self._now(),
                extra=dict(item),
            )
            snapshots.append(snapshot)
            count += 1
        if replace:
            self._oms.replace_positions(
                snapshots,
                account_id=self._account_id,
                environment=self._scope()[0],
                product=self._scope()[1],
                cutoff_at=self._now(),
            )
        else:
            for snapshot in snapshots:
                self._oms.upsert_position(snapshot)
        return count

    def _record_account_event(self, event: Mapping[str, Any]) -> None:
        payload = event.get("payload", {})
        account = payload.get("a", {}) if isinstance(payload, Mapping) else {}
        if not isinstance(account, Mapping):
            return
        balances = account.get("B", [])
        snapshots = []
        if isinstance(balances, list):
            for item in balances:
                if isinstance(item, Mapping) and item.get("a"):
                    snapshots.append(
                        FuturesBalanceSnapshot(
                            account_id=self._account_id,
                            environment=self._scope()[0],
                            product=self._scope()[1],
                            asset=str(item["a"]),
                            wallet_balance=_decimal(item.get("wb")),
                            available_balance=_decimal(item.get("cw")),
                            cross_wallet_balance=_decimal(item.get("cw")),
                            updated_at=self._datetime_ms(event["event_time_ms"]),
                            extra=dict(item),
                        )
                    )
        self._oms.record_balances(snapshots)
        positions = account.get("P", [])
        if isinstance(positions, list):
            self._record_positions(tuple(item for item in positions if isinstance(item, Mapping)))

    def _record_config_event(self, event: Mapping[str, Any]) -> None:
        payload = event.get("payload", {})
        config = payload.get("ac", {}) if isinstance(payload, Mapping) else {}
        if not isinstance(config, Mapping):
            return
        symbol = str(config.get("s", "*")).upper()
        self._oms.upsert_config(
            FuturesConfigSnapshot(
                account_id=self._account_id,
                environment=self._scope()[0],
                product=self._scope()[1],
                symbol=symbol,
                leverage=_int_or_none(config.get("l")),
                extra=dict(config),
                updated_at=self._now(),
            )
        )

    def _mark_health(self, state: str, reason: str | None, epoch: int) -> None:
        from gribuki_trade.trading.futures.futures_models import FuturesStreamHealth

        self._oms.set_stream_health(
            FuturesStreamHealth(
                account_id=self._account_id,
                environment=self._scope()[0],
                product=self._scope()[1],
                state=state,
                connection_epoch=epoch,
                reason=reason,
                updated_at=self._now(),
            )
        )

    def _scope(self) -> tuple[str, str]:
        return (self._client.stage.value, self._client.product.value)

    async def _renew_lease_loop(self) -> None:
        while not self._stopping:
            await asyncio.sleep(10)
            if self._fencing_token is None:
                continue
            scope = self._scope()
            try:
                self._oms.renew_owner(
                    account_id=self._account_id,
                    environment=scope[0],
                    product=scope[1],
                    owner_id=self._owner_id,
                    fencing_token=self._fencing_token,
                    now=self._now(),
                )
            except (RuntimeError, OSError):
                self._mark_health("DEGRADED", "owner_lease_lost", self._last_epoch)
                self._stopping = True
                return

    def _assert(self, operation: BrokerOperation) -> None:
        if self._guard is not None:
            self._guard.assert_broker_operation(self._exchange, self._account_id, operation)

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    def _now_ms(self) -> int:
        return int(self._now().timestamp() * 1000)

    def _datetime_ms(self, value: int) -> datetime:
        return datetime.fromtimestamp(value / 1000, tz=UTC)


def _str(value: object) -> str | None:
    return None if value is None else str(value)


def _decimal(value: object | None) -> Decimal:
    try:
        number = Decimal("0" if value is None else str(value))
    except Exception as exc:
        raise ValueError("Futures decimal field is malformed") from exc
    if not number.is_finite():
        raise ValueError("Futures decimal field is not finite")
    return number


def _decimal_or_none(value: object | None) -> Decimal | None:
    return None if value is None else _decimal(value)


def _int(value: object | None, *, default: int) -> int:
    try:
        return default if value is None else int(str(value))
    except (TypeError, ValueError):
        return default


def _int_or_none(value: object | None) -> int | None:
    return None if value is None else _int(value, default=0)


def _bool(value: object | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() == "true"


def _order_status(value: str) -> FuturesOrderStatus:
    normalized = value.upper()
    if normalized in {
        "NEW",
        "PARTIALLY_FILLED",
        "FILLED",
        "CANCELED",
        "EXPIRED",
        "REJECTED",
        "TRIGGERED",
        "TRIGGERING",
        "FINISHED",
        "EXPIRED_IN_MATCH",
    }:
        return FuturesOrderStatus(normalized)
    return FuturesOrderStatus.UNKNOWN


def _stable_command_id(command_type: str, payload: Mapping[str, object]) -> str:
    """根据客户端幂等标识生成跨进程稳定的命令号。"""

    identity = (
        payload.get("new_client_order_id")
        or payload.get("client_order_id")
        or payload.get("client_algo_id")
        or payload.get("newClientOrderId")
    )
    if identity is not None and str(identity).strip():
        return f"{command_type.lower()}:{identity}"
    canonical = repr(sorted((str(key), str(value)) for key, value in payload.items()))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    return f"{command_type.lower()}:{digest}:{uuid4().hex[:8]}"
