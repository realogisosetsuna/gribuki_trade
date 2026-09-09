"""无需凭据的 Binance 现货 PAPER/SHADOW 运行时。

本模块刻意不依赖 Binance 私有网关。它可以消费测试网或实盘的公共市场数据，但生成的
每张订单都只路由至本地 :class:`PaperBroker` 与持久化 SQLite OMS。每份运行报告携带的
环境水印会向调用方与 GUI 明示该安全边界。
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import deque
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import ROUND_DOWN, ROUND_UP, Decimal

from gribuki_trade.adapters.binance.models import BinanceEnvironment
from gribuki_trade.adapters.binance.stream import (
    BinanceBookTickerEvent,
    BinanceKlineEvent,
    BinanceMarketEvent,
    BinanceSpotMarketStream,
    book_ticker_stream,
    kline_stream,
)
from gribuki_trade.adapters.paper import PaperBroker
from gribuki_trade.adapters.paper_account import (
    PaperFeeSchedule,
    PaperSpotAccount,
)
from gribuki_trade.backtest.crypto import BacktestBarEvent, CryptoBar, PortfolioSnapshot
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.services.binance.binance_paper import (
    BinanceMarketSource,
    BinancePaperEngine,
    PaperEngineSnapshot,
    PaperRiskLimits,
)
from gribuki_trade.services.binance.binance_shadow_models import (
    _ALLOWED_SYMBOL_ASSETS,
    BinanceShadowConfig,
    BinanceShadowStatistics,
    ShadowAdapterSnapshot,
    ShadowEnvironmentWatermark,
    ShadowMarketIntegrityError,
    ShadowQuote,
    ShadowRecoveryError,
    ShadowTermination,
)
from gribuki_trade.strategy.crypto_trend import (
    CryptoTrendConfig,
    MovingAverageCryptoTrendStrategy,
)
from gribuki_trade.trading import (
    BalanceValue,
    SQLiteOrderManagementStore,
    TradingCommandStatus,
    TradingCommandType,
)

__all__ = [
    "BinanceShadowConfig",
    "BinanceShadowSession",
    "BinanceShadowStatistics",
    "ShadowAdapterSnapshot",
    "ShadowEnvironmentWatermark",
    "ShadowMarketIntegrityError",
    "ShadowQuote",
    "ShadowRecoveryError",
    "ShadowTermination",
    "ShadowTrendDecisionAdapter",
]

class _GuardedShadowMarket(BinanceMarketSource):
    """带报价缓存及失败关闭行情柱检查的可中断公共数据源。"""

    def __init__(
        self,
        source: BinanceMarketSource,
        *,
        config: BinanceShadowConfig,
        clock: Callable[[], datetime],
        maximum_closed_bars: int | None,
        stop_event: asyncio.Event | None,
        seed_last_bar: CryptoBar | None,
    ) -> None:
        self._source = source
        self._config = config
        self._clock = clock
        self._maximum_closed_bars = maximum_closed_bars
        self._stop_event = stop_event
        self._latest_quote: ShadowQuote | None = None
        self._last_closed: BinanceKlineEvent | None = None
        self._seed_last_bar = seed_last_bar
        self._seed_close_ms = (
            _datetime_ms(seed_last_bar.close_time) if seed_last_bar is not None else None
        )
        self.closed_bars = 0
        self.ignored_duplicate_closed_bars = 0
        self.detected_bar_gaps = 0
        self.stale_market_events = 0
        self.termination = ShadowTermination.STREAM_ENDED

    @property
    def latest_quote(self) -> ShadowQuote | None:
        return self._latest_quote

    async def events(self) -> AsyncIterator[BinanceMarketEvent]:
        iterator = self._source.events()
        try:
            while True:
                event = await self._next_or_stop(iterator)
                if event is None:
                    return
                checked = self._check(event)
                if checked is None:
                    continue
                yield checked
                if isinstance(checked, BinanceKlineEvent) and checked.is_closed:
                    self.closed_bars += 1
                    if (
                        self._maximum_closed_bars is not None
                        and self.closed_bars >= self._maximum_closed_bars
                    ):
                        self.termination = ShadowTermination.MAXIMUM_CLOSED_BARS
                        return
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()

    async def _next_or_stop(
        self, iterator: AsyncIterator[BinanceMarketEvent]
    ) -> BinanceMarketEvent | None:
        if self._stop_event is None:
            return await self._read_next(iterator)
        if self._stop_event.is_set():
            self.termination = ShadowTermination.EXTERNAL_STOP
            return None
        next_task = asyncio.create_task(self._read_next(iterator))
        stop_task = asyncio.create_task(self._wait_for_stop())
        done, _pending = await asyncio.wait(
            {next_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if stop_task in done:
            self.termination = ShadowTermination.EXTERNAL_STOP
            next_task.cancel()
            await asyncio.gather(next_task, return_exceptions=True)
            return None
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        return next_task.result()

    @staticmethod
    async def _read_next(
        iterator: AsyncIterator[BinanceMarketEvent],
    ) -> BinanceMarketEvent | None:
        try:
            return await anext(iterator)
        except StopAsyncIteration:
            return None

    async def _wait_for_stop(self) -> BinanceMarketEvent | None:
        assert self._stop_event is not None
        await self._stop_event.wait()
        return None

    def _check(self, event: BinanceMarketEvent) -> BinanceMarketEvent | None:
        if event.symbol != self._config.symbol:
            raise ShadowMarketIntegrityError(
                f"unexpected symbol {event.symbol}; expected {self._config.symbol}"
            )
        event_time_ms = getattr(event, "event_time_ms", None)
        if event_time_ms is not None:
            age_ms = Decimal(str(self._clock().timestamp() * 1_000 - event_time_ms))
            if age_ms > self._config.maximum_market_age_seconds * Decimal("1000"):
                self.stale_market_events += 1
                raise ShadowMarketIntegrityError(
                    f"stale {event.symbol} public market event ({age_ms} ms old)"
                )
        if isinstance(event, BinanceBookTickerEvent):
            self._latest_quote = ShadowQuote(
                symbol=event.symbol,
                bid=event.bid_price,
                ask=event.ask_price,
                observed_at=self._clock(),
                update_id=event.update_id,
            )
            return event
        if not isinstance(event, BinanceKlineEvent) or not event.is_closed:
            return event
        if event.interval != self._config.interval:
            raise ShadowMarketIntegrityError(
                f"unexpected kline interval {event.interval}; expected {self._config.interval}"
            )
        previous = self._last_closed
        if previous is None and self._seed_last_bar is not None:
            seed = self._seed_last_bar
            if event.start_time_ms == _datetime_ms(seed.open_time):
                if _event_matches_seed(event, seed):
                    self.ignored_duplicate_closed_bars += 1
                    return None
                raise ShadowMarketIntegrityError("conflicting duplicate seeded closed kline")
        if previous is not None and event.start_time_ms == previous.start_time_ms:
            if _same_closed_event(event, previous):
                self.ignored_duplicate_closed_bars += 1
                return None
            raise ShadowMarketIntegrityError("conflicting duplicate closed kline")
        expected_start = (
            previous.close_time_ms + 1
            if previous is not None
            # CryptoBar 使用左闭右开 [open, close) 区间，而 Binance 线上协议的
            # closeTime 是最后一个包含在内的毫秒。
            else self._seed_close_ms
        )
        if expected_start is not None and event.start_time_ms != expected_start:
            self.detected_bar_gaps += 1
            raise ShadowMarketIntegrityError(
                f"closed-kline gap: expected {expected_start}, got {event.start_time_ms}"
            )
        self._last_closed = event
        return event


class ShadowTrendDecisionAdapter:
    """将具有时点约束的趋势目标转换为可成交的本地限价单。"""

    def __init__(
        self,
        *,
        config: BinanceShadowConfig,
        trend: MovingAverageCryptoTrendStrategy,
        account: PaperSpotAccount,
        oms: SQLiteOrderManagementStore,
        quote: Callable[[], ShadowQuote | None],
        clock: Callable[[], datetime],
        initial_history: Sequence[CryptoBar] = (),
    ) -> None:
        if config.history_capacity < trend.config.minimum_history:
            raise ValueError("history_capacity must cover trend minimum_history")
        history = tuple(initial_history)
        if any(bar.symbol != config.symbol or not bar.complete for bar in history):
            raise ValueError("initial_history must contain completed bars for the session symbol")
        if any(
            left.open_time >= right.open_time
            for left, right in zip(history, history[1:], strict=False)
        ):
            raise ValueError("initial_history must be strictly ordered and unique")
        self._config = config
        self._trend = trend
        self._account = account
        self._oms = oms
        self._quote = quote
        self._clock = clock
        self._history: deque[CryptoBar] = deque(history, maxlen=config.history_capacity)
        self._generated_signals = 0
        self._skipped_warmup_or_tolerance = 0
        self._skipped_open_order = 0
        self._skipped_below_minimum = 0
        self._capped_to_maximum_notional = 0

    @property
    def snapshot(self) -> ShadowAdapterSnapshot:
        return ShadowAdapterSnapshot(
            observed_closed_bars=len(self._history),
            generated_signals=self._generated_signals,
            skipped_warmup_or_tolerance=self._skipped_warmup_or_tolerance,
            skipped_open_order=self._skipped_open_order,
            skipped_below_minimum=self._skipped_below_minimum,
            capped_to_maximum_notional=self._capped_to_maximum_notional,
        )

    def __call__(
        self, event: BinanceKlineEvent, _snapshot: PaperEngineSnapshot
    ) -> tuple[OrderIntent, ...]:
        bar = _crypto_bar(event)
        self._history.append(bar)
        if self._oms.open_orders(account_id=self._config.account_id):
            self._skipped_open_order += 1
            return ()
        assets = self._config.assets
        portfolio = PortfolioSnapshot(
            (
                (assets.base_asset, self._account.balance(assets.base_asset).total),
                (assets.quote_asset, self._account.balance(assets.quote_asset).total),
            )
        )
        strategy_event = BacktestBarEvent(
            decision_time=bar.available_at,
            bar=bar,
            history=tuple(self._history),
            portfolio=portfolio,
        )
        decision = self._trend.evaluate(strategy_event)
        if decision.order is None:
            self._skipped_warmup_or_tolerance += 1
            return ()
        quote = self._quote()
        if quote is None:
            raise ShadowMarketIntegrityError("no top-of-book quote before strategy signal")
        quote_age = Decimal(str((self._clock() - quote.observed_at).total_seconds()))
        if quote_age > self._config.maximum_market_age_seconds:
            raise ShadowMarketIntegrityError("latest top-of-book quote is stale")

        side = decision.order.side
        offset = self._config.aggressive_limit_offset_bps / Decimal("10000")
        if side is Side.BUY:
            price = _round_step(
                quote.ask * (Decimal("1") + offset),
                self._config.price_step,
                up=True,
            )
        else:
            price = _round_step(quote.bid * (Decimal("1") - offset), self._config.price_step)
        quantity = _round_step(decision.order.quantity, self._config.quantity_step)
        maximum_quantity = _round_step(
            self._config.maximum_order_notional / price, self._config.quantity_step
        )
        if quantity > maximum_quantity:
            quantity = maximum_quantity
            self._capped_to_maximum_notional += 1
        if side is Side.BUY:
            assert self._account.fees.buy_fee_buffer_rate is not None
            affordable = self._account.balance(assets.quote_asset).free / (
                price * (Decimal("1") + self._account.fees.buy_fee_buffer_rate)
            )
        else:
            affordable = self._account.balance(assets.base_asset).free
        quantity = min(quantity, _round_step(affordable, self._config.quantity_step))
        if (
            quantity < self._config.minimum_order_quantity
            or quantity * price < self._config.minimum_order_notional
        ):
            self._skipped_below_minimum += 1
            return ()
        self._generated_signals += 1
        return (
            OrderIntent(
                client_order_id=_shadow_order_id(
                    self._config.account_id,
                    bar.open_time,
                    side,
                ),
                account_id=self._config.account_id,
                strategy_id=self._config.strategy_id,
                symbol=self._config.symbol,
                side=side,
                quantity=quantity,
                limit_price=price,
                created_at=bar.available_at,
            ),
        )


class BinanceShadowSession:
    """用于有界或长时间本地影子运行的单次监督器。"""

    def __init__(
        self,
        market: BinanceMarketSource,
        oms: SQLiteOrderManagementStore,
        *,
        environment: BinanceEnvironment = BinanceEnvironment.TESTNET,
        config: BinanceShadowConfig | None = None,
        trend_config: CryptoTrendConfig | None = None,
        fees: PaperFeeSchedule | None = None,
        broker: PaperBroker | None = None,
        initial_history: Sequence[CryptoBar] = (),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config or BinanceShadowConfig()
        self.watermark = ShadowEnvironmentWatermark(environment)
        self._market = market
        self._oms = oms
        self._broker = broker or PaperBroker()
        self._fees = fees or PaperFeeSchedule()
        self._initial_history = tuple(initial_history)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._trend_config = trend_config or CryptoTrendConfig(
            quantity_step=self.config.quantity_step,
            minimum_order_quantity=self.config.minimum_order_quantity,
        )
        if self._trend_config.quantity_step != self.config.quantity_step:
            raise ValueError("trend and shadow quantity_step must match")
        if self._trend_config.minimum_order_quantity != self.config.minimum_order_quantity:
            raise ValueError("trend and shadow minimum_order_quantity must match")
        self._account: PaperSpotAccount | None = None
        self._statistics: BinanceShadowStatistics | None = None
        self._has_run = False

    @classmethod
    def public_stream(
        cls,
        oms: SQLiteOrderManagementStore,
        *,
        environment: BinanceEnvironment = BinanceEnvironment.TESTNET,
        config: BinanceShadowConfig | None = None,
        trend_config: CryptoTrendConfig | None = None,
        fees: PaperFeeSchedule | None = None,
        initial_history: Sequence[CryptoBar] = (),
        clock: Callable[[], datetime] | None = None,
    ) -> BinanceShadowSession:
        """构造无需凭据的公共盘口/K 线组合会话。"""

        resolved = config or BinanceShadowConfig()
        market = BinanceSpotMarketStream(
            (book_ticker_stream(resolved.symbol), kline_stream(resolved.symbol, resolved.interval)),
            environment=environment,
            allow_live=environment is BinanceEnvironment.LIVE,
        )
        return cls(
            market,
            oms,
            environment=environment,
            config=resolved,
            trend_config=trend_config,
            fees=fees,
            initial_history=initial_history,
            clock=clock,
        )

    @property
    def account(self) -> PaperSpotAccount:
        if self._account is None:
            raise RuntimeError("shadow session has not initialized its paper account")
        return self._account

    @property
    def statistics(self) -> BinanceShadowStatistics | None:
        return self._statistics

    async def run(
        self,
        *,
        maximum_closed_bars: int | None = None,
        maximum_events: int | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> BinanceShadowStatistics:
        if self._has_run:
            raise RuntimeError("a BinanceShadowSession is one-shot")
        if maximum_closed_bars is not None and maximum_closed_bars <= 0:
            raise ValueError("maximum_closed_bars must be positive")
        if maximum_events is not None and maximum_events <= 0:
            raise ValueError("maximum_events must be positive")
        self._has_run = True
        started_at = self._clock()
        empty_engine = PaperEngineSnapshot(started_at, 0, 0, 0, 0, 0, 0)
        adapter_snapshot = ShadowAdapterSnapshot(0, 0, 0, 0, 0, 0)
        recovered_count = 0
        guard: _GuardedShadowMarket | None = None
        engine_snapshot = empty_engine
        termination = ShadowTermination.FAILED_CLOSED
        failure_reason: str | None = None
        broker_connected_before_engine = False
        try:
            self._account = self._restore_account()
            await self._broker.connect()
            broker_connected_before_engine = True
            recovered_count = await self._restore_open_orders()
            guard = _GuardedShadowMarket(
                self._market,
                config=self.config,
                clock=self._clock,
                maximum_closed_bars=maximum_closed_bars,
                stop_event=stop_event,
                seed_last_bar=self._initial_history[-1] if self._initial_history else None,
            )
            trend = MovingAverageCryptoTrendStrategy(
                symbol=self.config.symbol,
                base_asset=self.config.assets.base_asset,
                quote_asset=self.config.assets.quote_asset,
                config=self._trend_config,
            )
            adapter = ShadowTrendDecisionAdapter(
                config=self.config,
                trend=trend,
                account=self.account,
                oms=self._oms,
                quote=lambda: guard.latest_quote,
                clock=self._clock,
                initial_history=self._initial_history,
            )
            limits = PaperRiskLimits(
                allowed_symbols=frozenset({self.config.symbol}),
                maximum_order_notional=self.config.maximum_order_notional,
                maximum_open_orders=self.config.maximum_open_orders,
                maximum_actions_per_minute=self.config.maximum_actions_per_minute,
                maximum_market_age_seconds=self.config.maximum_market_age_seconds,
            )
            engine = BinancePaperEngine(
                guard,
                self._broker,
                self._oms,
                adapter,
                account=self.account,
                limits=limits,
                clock=self._clock,
            )
            engine_snapshot = await engine.run(maximum_events=maximum_events)
            broker_connected_before_engine = False
            adapter_snapshot = adapter.snapshot
            termination = guard.termination
            if (
                maximum_events is not None
                and engine_snapshot.processed_market_events >= maximum_events
                and termination is ShadowTermination.STREAM_ENDED
            ):
                termination = ShadowTermination.MAXIMUM_EVENTS
        except BaseException as error:
            failure_reason = f"{type(error).__name__}: {error}"
            if broker_connected_before_engine:
                await self._broker.disconnect()
            self._statistics = self._build_statistics(
                termination=ShadowTermination.FAILED_CLOSED,
                started_at=started_at,
                engine=engine_snapshot,
                adapter=adapter_snapshot,
                recovered_count=recovered_count,
                guard=guard,
                failure_reason=failure_reason,
            )
            raise
        self._statistics = self._build_statistics(
            termination=termination,
            started_at=started_at,
            engine=engine_snapshot,
            adapter=adapter_snapshot,
            recovered_count=recovered_count,
            guard=guard,
            failure_reason=None,
        )
        return self._statistics

    def _restore_account(self) -> PaperSpotAccount:
        persisted = self._oms.balances(self.config.account_id)
        open_orders = self._oms.open_orders(account_id=self.config.account_id)
        initial: Mapping[str, Decimal | str | int]
        if persisted:
            initial = {item.asset: item.free + item.locked for item in persisted}
        else:
            if open_orders:
                raise ShadowRecoveryError("open paper orders exist without a balance snapshot")
            initial = self.config.initial_balances
        account = PaperSpotAccount(
            account_id=self.config.account_id,
            initial_balances=initial,
            symbol_assets=_ALLOWED_SYMBOL_ASSETS,
            fees=self._fees,
        )
        for snapshot in open_orders:
            if snapshot.order.symbol != self.config.symbol:
                raise ShadowRecoveryError("persisted paper order does not match session symbol")
            if snapshot.filled_quantity != 0:
                raise ShadowRecoveryError(
                    "partially filled paper orders require manual reconciliation"
                )
            if snapshot.status is OrderStatus.CANCEL_PENDING:
                raise ShadowRecoveryError("cancel-pending paper order requires reconciliation")
            account.reserve_order(snapshot.order)
        if persisted:
            expected = {item.asset: (item.free, item.locked) for item in persisted}
            actual = {item.asset: (item.free, item.locked) for item in account.balances()}
            if actual != expected:
                raise ShadowRecoveryError("paper balance snapshot and open reservations disagree")
        else:
            self._persist_balances(account, event_id=f"shadow-initial:{self.config.account_id}")
        return account

    async def _restore_open_orders(self) -> int:
        open_orders = self._oms.open_orders(account_id=self.config.account_id)
        if not open_orders:
            return 0
        order_ids = {snapshot.order.client_order_id for snapshot in open_orders}
        for command in self._oms.commands(status=TradingCommandStatus.IN_FLIGHT):
            if command.client_order_id in order_ids:
                self._oms.mark_command_unknown(
                    command.command_id,
                    occurred_at=self._clock(),
                    error_code="paper_restart_recovery",
                )
        commands = self._oms.commands()
        by_order = {
            command.client_order_id: command
            for command in commands
            if command.command_type is TradingCommandType.SUBMIT_ORDER
            and command.client_order_id in order_ids
        }
        for snapshot in open_orders:
            recovered_command = by_order.get(snapshot.order.client_order_id)
            if recovered_command is None:
                raise ShadowRecoveryError("open paper order has no durable submit command")
            now = self._clock()
            claimed_command_id: str | None = None
            if recovered_command.status is TradingCommandStatus.PENDING:
                claimed = self._oms.claim_command(recovered_command.command_id, now=now)
                claimed_command_id = claimed.command_id
            try:
                # 此操作只恢复进程内 PaperBroker。此处 UNKNOWN 具有权威性，因为任何远程
                # 场所都不可能收到影子命令。
                await self._broker.submit_order(snapshot.order)
            except BaseException:
                if claimed_command_id is not None:
                    self._oms.mark_command_unknown(
                        claimed_command_id,
                        occurred_at=self._clock(),
                        error_code="paper_recovery_failed",
                    )
                raise
            if recovered_command.status is TradingCommandStatus.PENDING:
                self._oms.record_order_update(
                    snapshot.order.client_order_id,
                    event_id=f"shadow-recover-submit:{snapshot.order.client_order_id}",
                    status=OrderStatus.ACCEPTED,
                    occurred_at=now,
                )
                assert claimed_command_id is not None
                self._oms.mark_command_sent(claimed_command_id, occurred_at=now)
            elif recovered_command.status is TradingCommandStatus.UNKNOWN:
                self._oms.reconcile_order(
                    snapshot.order.client_order_id,
                    event_id=f"shadow-reconcile:{snapshot.order.client_order_id}",
                    status=OrderStatus.ACCEPTED,
                    occurred_at=now,
                )
            elif recovered_command.status not in {
                TradingCommandStatus.SENT,
                TradingCommandStatus.RESOLVED,
            }:
                raise ShadowRecoveryError(
                    f"unsupported durable command state: {recovered_command.status.value}"
                )
        return len(open_orders)

    def _persist_balances(self, account: PaperSpotAccount, *, event_id: str) -> None:
        self._oms.record_balance_snapshot(
            account.account_id,
            tuple(
                BalanceValue(value.asset, value.free, value.locked)
                for value in account.balances()
            ),
            event_id=event_id,
            occurred_at=self._clock(),
        )

    def _build_statistics(
        self,
        *,
        termination: ShadowTermination,
        started_at: datetime,
        engine: PaperEngineSnapshot,
        adapter: ShadowAdapterSnapshot,
        recovered_count: int,
        guard: _GuardedShadowMarket | None,
        failure_reason: str | None,
    ) -> BinanceShadowStatistics:
        balances = () if self._account is None else self._account.balances()
        quote = None if guard is None else guard.latest_quote
        final_equity: Decimal | None = None
        if self._account is not None and quote is not None:
            assets = self.config.assets
            midpoint = (quote.bid + quote.ask) / Decimal("2")
            final_equity = (
                self._account.balance(assets.quote_asset).total
                + self._account.balance(assets.base_asset).total * midpoint
            )
        return BinanceShadowStatistics(
            watermark=self.watermark,
            termination=termination,
            started_at=started_at,
            ended_at=self._clock(),
            paper_engine=engine,
            adapter=adapter,
            recovered_open_orders=recovered_count,
            ignored_duplicate_closed_bars=(
                0 if guard is None else guard.ignored_duplicate_closed_bars
            ),
            detected_bar_gaps=0 if guard is None else guard.detected_bar_gaps,
            stale_market_events=0 if guard is None else guard.stale_market_events,
            balances=balances,
            final_equity_quote=final_equity,
            failure_reason=failure_reason,
        )


def _crypto_bar(event: BinanceKlineEvent) -> CryptoBar:
    open_time = _ms_datetime(event.start_time_ms)
    close_time = _ms_datetime(event.close_time_ms + 1)
    event_time = _ms_datetime(event.event_time_ms)
    return CryptoBar(
        symbol=event.symbol,
        open_time=open_time,
        close_time=close_time,
        available_at=max(close_time, event_time),
        open=event.open,
        high=event.high,
        low=event.low,
        close=event.close,
        volume=event.volume,
        complete=event.is_closed,
    )


def _event_matches_seed(event: BinanceKlineEvent, seed: CryptoBar) -> bool:
    return (
        event.symbol == seed.symbol
        and event.start_time_ms == _datetime_ms(seed.open_time)
        and event.close_time_ms + 1 == _datetime_ms(seed.close_time)
        and event.open == seed.open
        and event.high == seed.high
        and event.low == seed.low
        and event.close == seed.close
        and event.volume == seed.volume
        and seed.complete
    )


def _same_closed_event(left: BinanceKlineEvent, right: BinanceKlineEvent) -> bool:
    return (
        left.stream == right.stream
        and left.symbol == right.symbol
        and left.start_time_ms == right.start_time_ms
        and left.close_time_ms == right.close_time_ms
        and left.interval == right.interval
        and left.first_trade_id == right.first_trade_id
        and left.last_trade_id == right.last_trade_id
        and left.open == right.open
        and left.close == right.close
        and left.high == right.high
        and left.low == right.low
        and left.volume == right.volume
        and left.trade_count == right.trade_count
        and left.is_closed == right.is_closed
        and left.quote_volume == right.quote_volume
        and left.taker_buy_base_volume == right.taker_buy_base_volume
        and left.taker_buy_quote_volume == right.taker_buy_quote_volume
    )


def _round_step(value: Decimal, step: Decimal, *, up: bool = False) -> Decimal:
    rounding = ROUND_UP if up else ROUND_DOWN
    return (value / step).to_integral_value(rounding=rounding) * step


def _ms_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000, tz=UTC)


def _datetime_ms(value: datetime) -> int:
    return int(value.timestamp() * 1_000)


def _shadow_order_id(account_id: str, bar_open: datetime, side: Side) -> str:
    """构建跨 PAPER 账户档案无冲突的稳定标识。"""

    account_digest = hashlib.sha256(account_id.encode("utf-8")).hexdigest()[:8]
    side_code = "b" if side is Side.BUY else "s"
    return f"sh-{account_digest}-{_datetime_ms(bar_open)}-{side_code}"
