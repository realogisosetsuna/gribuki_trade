"""受守卫保护的 Binance 现货原生条件订单服务接口。"""

from __future__ import annotations

from decimal import Decimal

from gribuki_trade.adapters.binance import (
    BinanceCancelReplaceResult,
    BinanceEnvironment,
    BinanceOrderListSnapshot,
    BinanceOrderSnapshot,
    BinanceSpotGateway,
    BinanceSpotOcoRequest,
    BinanceSpotOrderLeg,
    BinanceSpotOtocoRequest,
    BinanceSpotOtoRequest,
)
from gribuki_trade.domain.orders import Side
from gribuki_trade.runtime.guard import BrokerOperation, LiveTradingGuard


class BinanceSpotAdvancedExecutionService:
    """策略侧使用的现货 OCO/OTO/移动止盈接口。

    适配器负责 Binance 参数编码，本服务负责运行时守卫，策略无需直接调用
    券商网关。
    """

    def __init__(
        self,
        gateway: BinanceSpotGateway,
        *,
        account_id: str,
        guard: LiveTradingGuard | None = None,
        exchange: str = "BINANCE",
    ) -> None:
        if not account_id.strip():
            raise ValueError("account_id must not be blank")
        if gateway.environment is BinanceEnvironment.LIVE and guard is None:
            raise ValueError("Spot LIVE requires a LiveTradingGuard")
        self._gateway = gateway
        self._account_id = account_id
        self._guard = guard
        self._exchange = exchange

    @property
    def gateway(self) -> BinanceSpotGateway:
        return self._gateway

    async def connect(self) -> int:
        self._assert(BrokerOperation.CONNECT)
        await self._gateway.connect()
        return self._gateway.server_time_offset_ms

    async def disconnect(self) -> None:
        self._assert(BrokerOperation.DISCONNECT)
        await self._gateway.disconnect()

    async def submit_order(
        self,
        *,
        symbol: str,
        side: Side,
        order_type: str,
        quantity: Decimal | None = None,
        quote_order_quantity: Decimal | None = None,
        price: Decimal | None = None,
        stop_price: Decimal | None = None,
        trailing_delta: int | None = None,
        time_in_force: str | None = None,
        client_order_id: str | None = None,
        iceberg_quantity: Decimal | None = None,
        strategy_id: int | None = None,
        strategy_type: int | None = None,
        self_trade_prevention_mode: str | None = None,
        response_type: str = "RESULT",
    ) -> BinanceOrderSnapshot:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._gateway.submit_spot_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            quote_order_quantity=quote_order_quantity,
            price=price,
            stop_price=stop_price,
            trailing_delta=trailing_delta,
            time_in_force=time_in_force,
            client_order_id=client_order_id,
            iceberg_quantity=iceberg_quantity,
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            self_trade_prevention_mode=self_trade_prevention_mode,
            response_type=response_type,
        )

    async def submit_oco(self, request: BinanceSpotOcoRequest) -> BinanceOrderListSnapshot:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._gateway.submit_oco(request)

    async def submit_oto(self, request: BinanceSpotOtoRequest) -> BinanceOrderListSnapshot:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._gateway.submit_oto(request)

    async def submit_otoco(self, request: BinanceSpotOtocoRequest) -> BinanceOrderListSnapshot:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._gateway.submit_otoco(request)

    async def cancel_order_list(
        self,
        symbol: str,
        *,
        order_list_id: int | None = None,
        list_client_order_id: str | None = None,
        new_client_order_id: str | None = None,
    ) -> BinanceOrderListSnapshot:
        self._assert(BrokerOperation.CANCEL_ORDER)
        return await self._gateway.cancel_order_list(
            symbol,
            order_list_id=order_list_id,
            list_client_order_id=list_client_order_id,
            new_client_order_id=new_client_order_id,
        )

    async def cancel_all_open_orders(self, symbol: str) -> tuple[BinanceOrderSnapshot, ...]:
        self._assert(BrokerOperation.CANCEL_ORDER)
        return await self._gateway.cancel_all_open_orders(symbol)

    async def open_order_lists(self) -> tuple[BinanceOrderListSnapshot, ...]:
        """读取当前打开的订单列表，供重启恢复使用。"""

        self._assert(BrokerOperation.QUERY)
        return await self._gateway.open_order_lists()

    async def all_order_lists(
        self,
        *,
        from_id: int | None = None,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> tuple[BinanceOrderListSnapshot, ...]:
        """读取订单列表历史，供持久化 OMS 对账。"""

        self._assert(BrokerOperation.QUERY)
        return await self._gateway.all_order_lists(
            from_id=from_id,
            limit=limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    async def amend_order_keep_priority(
        self,
        *,
        symbol: str,
        new_quantity: Decimal,
        order_id: int | None = None,
        client_order_id: str | None = None,
        new_client_order_id: str | None = None,
    ) -> BinanceOrderSnapshot:
        self._assert(BrokerOperation.REPLACE_ORDER)
        return await self._gateway.amend_order_keep_priority(
            symbol=symbol,
            new_quantity=new_quantity,
            order_id=order_id,
            client_order_id=client_order_id,
            new_client_order_id=new_client_order_id,
        )

    async def cancel_replace(
        self,
        *,
        symbol: str,
        new_order: BinanceSpotOrderLeg,
        cancel_order_id: int | None = None,
        cancel_client_order_id: str | None = None,
        cancel_replace_mode: str = "STOP_ON_FAILURE",
    ) -> BinanceCancelReplaceResult:
        self._assert(BrokerOperation.REPLACE_ORDER)
        return await self._gateway.cancel_replace(
            symbol=symbol,
            cancel_order_id=cancel_order_id,
            cancel_client_order_id=cancel_client_order_id,
            new_order=new_order,
            cancel_replace_mode=cancel_replace_mode,
        )

    def _assert(self, operation: BrokerOperation) -> None:
        if self._guard is not None:
            self._guard.assert_broker_operation(self._exchange, self._account_id, operation)
