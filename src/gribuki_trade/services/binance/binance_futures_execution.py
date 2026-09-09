"""受运行时交易守卫保护的 Binance USD-M/COIN-M 合约执行服务。"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from gribuki_trade.adapters.binance.auth.envs import BinanceStage
from gribuki_trade.adapters.binance.futures.client import (
    BinanceFuturesProtectionOrder,
    BinanceFuturesRestClient,
)
from gribuki_trade.adapters.binance.transport.gateway import BinanceConfigurationError
from gribuki_trade.runtime.guard import BrokerOperation, LiveTradingGuard


class BinanceFuturesExecutionService:
    """统一封装合约账户查询、下单、撤单和启动对账。

    LIVE 客户端必须显式使用 ``allow_live=True`` 构造，并且必须传入
    :class:`LiveTradingGuard`。SHADOW 只能查询，PAPER 会被守卫拒绝访问。
    DEMO 环境可用于离线或模拟交易流程，不会自动切换到 LIVE。
    """

    def __init__(
        self,
        client: BinanceFuturesRestClient,
        *,
        account_id: str,
        guard: LiveTradingGuard | None = None,
        exchange: str = "BINANCE",
    ) -> None:
        if not account_id.strip():
            raise ValueError("account_id must not be blank")
        if client.stage is BinanceStage.LIVE and guard is None:
            raise BinanceConfigurationError(
                "Binance Futures LIVE requires a LiveTradingGuard"
            )
        self._client = client
        self._account_id = account_id
        self._guard = guard
        self._exchange = exchange

    @property
    def client(self) -> BinanceFuturesRestClient:
        return self._client

    async def connect(self) -> int:
        """检查连接并同步服务器时钟，返回时钟偏移毫秒数。"""

        self._assert(BrokerOperation.CONNECT)
        await self._client.ping()
        return await self._client.synchronize_time()

    async def disconnect(self) -> None:
        self._assert(BrokerOperation.DISCONNECT)

    async def account(self) -> dict[str, Any]:
        self._assert(BrokerOperation.QUERY)
        return await self._client.account()

    async def exchange_info(self) -> dict[str, Any]:
        self._assert(BrokerOperation.QUERY)
        return await self._client.exchange_info()

    async def ticker_price(self, symbol: str) -> Any:
        self._assert(BrokerOperation.QUERY)
        return await self._client.ticker_price(symbol)

    async def validate_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal | str | None = None,
        price: Decimal | str | None = None,
        time_in_force: str | None = None,
        position_side: str | None = None,
        reduce_only: bool | None = None,
        stop_price: Decimal | str | None = None,
        close_position: bool | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """调用交易所 order/test 校验，不创建真实订单。"""

        self._assert(BrokerOperation.QUERY)
        return await self._client.validate_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            time_in_force=time_in_force,
            position_side=position_side,
            reduce_only=reduce_only,
            stop_price=stop_price,
            close_position=close_position,
            client_order_id=client_order_id,
        )

    async def position_risk(self, symbol: str | None = None) -> tuple[dict[str, Any], ...]:
        self._assert(BrokerOperation.QUERY)
        return await self._client.position_risk(symbol)

    async def position_side_mode(self) -> bool:
        """查询账户持仓模式；仅允许通过查询守卫访问。"""

        self._assert(BrokerOperation.QUERY)
        return await self._client.position_side_mode()

    async def position_mode(self) -> bool:
        """返回账户持仓模式的兼容别名。"""

        return await self.position_side_mode()

    async def set_position_mode(self, dual_side_position: bool) -> dict[str, Any]:
        self._assert(BrokerOperation.CHANGE_RISK)
        return await self._client.set_position_mode(dual_side_position)

    async def set_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        self._assert(BrokerOperation.CHANGE_RISK)
        return await self._client.set_leverage(symbol, leverage)

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        self._assert(BrokerOperation.CHANGE_RISK)
        return await self._client.set_margin_type(symbol, margin_type)

    async def multi_assets_mode(self) -> bool:
        self._assert(BrokerOperation.QUERY)
        return await self._client.multi_assets_mode()

    async def set_multi_assets_mode(self, multi_assets_margin: bool) -> dict[str, Any]:
        self._assert(BrokerOperation.CHANGE_RISK)
        return await self._client.set_multi_assets_mode(multi_assets_margin)

    async def open_orders(self, symbol: str | None = None) -> tuple[dict[str, Any], ...]:
        self._assert(BrokerOperation.QUERY)
        return await self._client.open_orders(symbol)

    async def get_order(
        self,
        symbol: str,
        *,
        order_id: int | str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        self._assert(BrokerOperation.QUERY)
        return await self._client.get_order(
            symbol, order_id=order_id, client_order_id=client_order_id
        )

    async def submit_order(self, **kwargs: object) -> dict[str, Any]:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._client.submit_order(**kwargs)

    async def submit_stop_loss(self, **kwargs: object) -> dict[str, Any]:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._client.submit_stop_loss(**kwargs)  # type: ignore[arg-type]

    async def submit_take_profit(self, **kwargs: object) -> dict[str, Any]:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._client.submit_take_profit(**kwargs)  # type: ignore[arg-type]

    async def submit_trailing_stop(self, **kwargs: object) -> dict[str, Any]:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._client.submit_trailing_stop(**kwargs)  # type: ignore[arg-type]

    async def submit_protection_order(
        self, request: BinanceFuturesProtectionOrder
    ) -> dict[str, Any]:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._client.submit_protection_order(request)

    async def submit_algo_order(self, **kwargs: object) -> dict[str, Any]:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._client.submit_algo_order(**kwargs)

    async def submit_algo_trailing_stop(self, **kwargs: object) -> dict[str, Any]:
        self._assert(BrokerOperation.SUBMIT_ORDER)
        return await self._client.submit_algo_trailing_stop(**kwargs)  # type: ignore[arg-type]

    async def get_algo_order(
        self,
        symbol: str,
        *,
        algo_id: int | str | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        self._assert(BrokerOperation.QUERY)
        return await self._client.get_algo_order(
            symbol, algo_id=algo_id, client_algo_id=client_algo_id
        )

    async def open_algo_orders(self, symbol: str | None = None) -> tuple[dict[str, Any], ...]:
        self._assert(BrokerOperation.QUERY)
        return await self._client.open_algo_orders(symbol)

    async def cancel_algo_order(
        self,
        symbol: str,
        *,
        algo_id: int | str | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        self._assert(BrokerOperation.CANCEL_ORDER)
        return await self._client.cancel_algo_order(
            symbol, algo_id=algo_id, client_algo_id=client_algo_id
        )

    async def cancel_all_algo_orders(self, symbol: str) -> dict[str, Any]:
        self._assert(BrokerOperation.CANCEL_ORDER)
        return await self._client.cancel_all_algo_orders(symbol)

    async def replace_protection_order(
        self,
        symbol: str,
        *,
        order_id: int | str | None = None,
        client_order_id: str | None = None,
        protection: str,
        **kwargs: object,
    ) -> dict[str, Any]:
        """撤销旧保护单并创建新保护单，返回可审计的旧/新订单结果。

        条件单没有通用的原地修改接口；策略层应在成交/取消事件后调用此方法，
        并在调用失败时立即重新对账。
        """

        self._assert(BrokerOperation.REPLACE_ORDER)
        old_order = await self._client.cancel_order(
            symbol, order_id=order_id, client_order_id=client_order_id
        )
        methods = {
            "stop_loss": self._client.submit_stop_loss,
            "take_profit": self._client.submit_take_profit,
            "trailing_stop": self._client.submit_trailing_stop,
        }
        try:
            submit = methods[protection]
        except KeyError:
            raise ValueError(
                "protection must be stop_loss, take_profit, or trailing_stop"
            ) from None
        new_order = await submit(**kwargs)  # type: ignore[operator]
        return {"canceled_order": old_order, "new_order": new_order}

    async def cancel_order(
        self,
        symbol: str,
        *,
        order_id: int | str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        self._assert(BrokerOperation.CANCEL_ORDER)
        return await self._client.cancel_order(
            symbol, order_id=order_id, client_order_id=client_order_id
        )

    async def cancel_all_orders(self, symbol: str) -> dict[str, Any]:
        self._assert(BrokerOperation.CANCEL_ORDER)
        return await self._client.cancel_all_orders(symbol)

    async def reconcile(
        self, symbols: Sequence[str]
    ) -> dict[str, Any]:
        """获取账户、持仓、挂单、历史订单和成交，供本地 OMS 对账。"""

        self._assert(BrokerOperation.QUERY)
        checked_symbols = tuple(symbols)
        account = await self._client.account()
        positions = await self._client.position_risk()
        open_orders = await self._client.open_orders()
        histories: list[dict[str, Any]] = []
        trades: list[dict[str, Any]] = []
        for symbol in checked_symbols:
            histories.extend(await self._client.all_orders(symbol))
            trades.extend(await self._client.account_trades(symbol))
        return {
            "account": account,
            "positions": positions,
            "open_orders": open_orders,
            "orders": tuple(histories),
            "trades": tuple(trades),
        }

    def _assert(self, operation: BrokerOperation) -> None:
        if self._guard is not None:
            self._guard.assert_broker_operation(
                self._exchange, self._account_id, operation
            )
