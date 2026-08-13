"""Crash-recoverable saga around deterministic A-share PAPER matching.

The order-event database and the cash/position ledger are separate SQLite
files, so this module does not claim cross-database atomicity.  It records a
complete ``RUN_STARTED`` input before matching, relies on deterministic ledger
``fill_id`` idempotency, then records ``ORDER_FILL_APPLIED`` and
``RUN_COMPLETED``.  Recovery recomputes only an unfinished run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from gribuki_trade.domain.orders import OrderIntent, OrderType, Side
from gribuki_trade.domain.paper_orders import (
    ASharePaperOrderIntent,
    ExplicitPriceBand,
    PaperBarMatchRun,
    PaperMatchingConfig,
    PaperMatchReason,
    PaperOrderSnapshot,
    PaperOrderStatus,
    PaperTimeInForce,
    SimulatedDailyBar,
)
from gribuki_trade.domain.paper_trading import PaperInstrumentType
from gribuki_trade.services.ashare_paper import ASharePaperTradingService
from gribuki_trade.services.ashare_paper_matching import ASharePaperOrderMatcher
from gribuki_trade.storage.paper_orders import (
    PaperOrderEventType,
    PaperOrderStoreConflictError,
    SQLitePaperOrderStore,
)


class PaperRecoveryRequiredError(RuntimeError):
    """The in-memory matcher may have diverged and must be rebuilt."""


@dataclass(frozen=True, slots=True)
class PaperRecoverySummary:
    restored_orders: int
    recovered_run_ids: tuple[str, ...]


class DurableASharePaperOrderMatcher:
    """Persist submissions, state transitions and bar-run sagas.

    This remains a local PAPER simulator.  It has no broker route and makes no
    queue-position or fill-probability claim beyond the wrapped daily-bar model.
    """

    def __init__(
        self,
        accounts: ASharePaperTradingService,
        store: SQLitePaperOrderStore,
        *,
        owner_id: str,
        config: PaperMatchingConfig | None = None,
        lease_duration: timedelta = timedelta(minutes=10),
    ) -> None:
        if not owner_id.strip():
            raise ValueError("owner_id must not be empty")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        self._accounts = accounts
        self._store = store
        self._owner_id = owner_id.strip()
        self._config = config or PaperMatchingConfig()
        self._lease_duration = lease_duration
        self._matcher = ASharePaperOrderMatcher(accounts, config=self._config)
        self._requires_recovery = True

    @property
    def matcher(self) -> ASharePaperOrderMatcher:
        if self._requires_recovery:
            raise PaperRecoveryRequiredError("call recover() before using the matcher")
        return self._matcher

    def recover(self, *, recovered_at: datetime | None = None) -> PaperRecoverySummary:
        """Rebuild stable order state and finish the sole incomplete saga."""

        now = _aware_utc(recovered_at or datetime.now(UTC))
        self._renew_writer()
        matcher = ASharePaperOrderMatcher(self._accounts, config=self._config)
        events = self._store.events()
        completed_runs = {
            event.run_id
            for event in events
            if event.event_type is PaperOrderEventType.RUN_COMPLETED
        }
        stable: dict[str, tuple[int, PaperOrderSnapshot]] = {}
        submissions: dict[str, ASharePaperOrderIntent] = {}
        for event in events:
            if event.event_type is PaperOrderEventType.ORDER_SUBMISSION_STARTED:
                intent = _intent_from_document(_mapping(event.payload, "intent"))
                submissions[intent.order.client_order_id] = intent
            elif event.event_type is PaperOrderEventType.ORDER_STATE_APPLIED and (
                event.run_id is None or event.run_id in completed_runs
            ):
                snapshot = _snapshot_from_document(_mapping(event.payload, "snapshot"))
                stable[snapshot.intent.order.client_order_id] = (event.sequence, snapshot)
        for _sequence, snapshot in sorted(stable.values(), key=lambda item: item[0]):
            matcher.restore_order_snapshot(snapshot)
        for client_order_id, intent in submissions.items():
            if client_order_id in stable:
                continue
            snapshot = matcher.submit_order(intent)
            self._record_state(snapshot, run_id=None, phase="submission-recovery")

        recovered: list[str] = []
        incomplete = self._store.incomplete_runs()
        if len(incomplete) > 1:  # store prevents this; fail closed on corruption
            raise PaperOrderStoreConflictError("multiple incomplete paper runs exist")
        for record in incomplete:
            start = next(
                event
                for event in self._store.run_events(record.run_id)
                if event.event_type is PaperOrderEventType.RUN_STARTED
            )
            payload = start.payload
            stored_config = _config_from_document(_mapping(payload, "config"))
            if stored_config != self._config:
                raise PaperOrderStoreConflictError(
                    "unfinished run uses a different matching configuration"
                )
            matcher = ASharePaperOrderMatcher(self._accounts, config=self._config)
            for item in _list(payload, "orders_before"):
                matcher.restore_order_snapshot(_snapshot_from_document(_object(item)))
            bar = _bar_from_document(_mapping(payload, "bar"))
            self._store.begin_run(
                symbol=bar.symbol,
                trade_date=bar.trade_date,
                config_document=_config_document(self._config),
                run_document=payload,
                started_at=now,
                owner_id=self._owner_id,
                lease_duration=self._lease_duration,
            )
            run = matcher.process_bar(bar)
            self._finish_run(
                record.run_id,
                run,
                matcher,
                completed_at=bar.available_at,
            )
            recovered.append(record.run_id)

        self._matcher = matcher
        self._requires_recovery = False
        return PaperRecoverySummary(
            restored_orders=len(matcher.orders()),
            recovered_run_ids=tuple(recovered),
        )

    def submit_order(self, intent: ASharePaperOrderIntent) -> PaperOrderSnapshot:
        matcher = self.matcher
        self._renew_writer()
        client_order_id = intent.order.client_order_id
        self._store.append_event(
            event_type=PaperOrderEventType.ORDER_SUBMISSION_STARTED,
            stream_id=f"order:{client_order_id}",
            run_id=None,
            occurred_at=intent.order.created_at,
            idempotency_key=f"order-submit:{client_order_id}",
            payload={"intent": _intent_document(intent), "schema_version": 1},
        )
        snapshot = matcher.submit_order(intent)
        try:
            self._record_state(snapshot, run_id=None, phase="submission")
        except BaseException:
            self._requires_recovery = True
            raise
        return snapshot

    def cancel_order(
        self, client_order_id: str, *, cancelled_at: datetime
    ) -> PaperOrderSnapshot:
        self._renew_writer()
        snapshot = self.matcher.cancel_order(client_order_id, cancelled_at=cancelled_at)
        try:
            self._record_state(
                snapshot, run_id=None, phase=f"cancel:{cancelled_at.isoformat()}"
            )
        except BaseException:
            self._requires_recovery = True
            raise
        return snapshot

    def expire_order(
        self, client_order_id: str, *, expired_at: datetime
    ) -> PaperOrderSnapshot:
        self._renew_writer()
        snapshot = self.matcher.expire_order(client_order_id, expired_at=expired_at)
        try:
            self._record_state(
                snapshot, run_id=None, phase=f"expire:{expired_at.isoformat()}"
            )
        except BaseException:
            self._requires_recovery = True
            raise
        return snapshot

    def process_bar(self, bar: SimulatedDailyBar) -> PaperBarMatchRun:
        matcher = self.matcher
        self._renew_writer()
        prior = self._store.run_for_identity(
            symbol=bar.symbol,
            trade_date=bar.trade_date,
            config_document=_config_document(self._config),
        )
        if prior is not None:
            if not prior.completed:
                self._requires_recovery = True
                raise PaperRecoveryRequiredError("unfinished run exists; call recover()")
            return self._completed_replay(prior.run_id, bar)
        # Persist the complete matcher projection, not only this symbol.  A
        # recovered matcher must retain unrelated open reservations as well.
        orders_before = matcher.orders()
        run_document = {
            "bar": _bar_document(bar),
            "config": _config_document(self._config),
            "orders_before": [_snapshot_document(order) for order in orders_before],
            "schema_version": 1,
        }
        record, applied_new = self._store.begin_run(
            symbol=bar.symbol,
            trade_date=bar.trade_date,
            config_document=_config_document(self._config),
            run_document=run_document,
            started_at=bar.available_at,
            owner_id=self._owner_id,
            lease_duration=self._lease_duration,
        )
        if record.completed:
            local = matcher.process_bar(bar)
            return PaperBarMatchRun(
                bar=local.bar,
                applied_new=False,
                volume_capacity=local.volume_capacity,
                consumed_volume=local.consumed_volume,
                outcomes=local.outcomes,
            )
        if not applied_new:
            self._requires_recovery = True
            raise PaperRecoveryRequiredError("unfinished run exists; call recover()")
        try:
            run = matcher.process_bar(bar)
            self._finish_run(record.run_id, run, matcher, completed_at=bar.available_at)
        except BaseException:
            self._requires_recovery = True
            raise
        return run

    def _completed_replay(
        self,
        run_id: str,
        bar: SimulatedDailyBar,
    ) -> PaperBarMatchRun:
        """Return a stable receipt-less summary for an exact completed replay."""

        events = self._store.run_events(run_id)
        start = next(
            event
            for event in events
            if event.event_type is PaperOrderEventType.RUN_STARTED
        )
        completed = next(
            event
            for event in events
            if event.event_type is PaperOrderEventType.RUN_COMPLETED
        )
        stored_bar = _bar_from_document(_mapping(start.payload, "bar"))
        if stored_bar != bar:
            raise PaperOrderStoreConflictError(
                "symbol/date/config replay has different bar content or revision"
            )
        payload = completed.payload
        # Replays intentionally omit outcomes: receipts include historical
        # account projections and are not fabricated.  Durable event rows hold
        # every outcome and ledger fill reference for audit/reconstruction.
        return PaperBarMatchRun(
            bar=bar,
            applied_new=False,
            volume_capacity=_integer(payload, "volume_capacity"),
            consumed_volume=_integer(payload, "consumed_volume"),
            outcomes=(),
        )

    def _finish_run(
        self,
        run_id: str,
        run: PaperBarMatchRun,
        matcher: ASharePaperOrderMatcher,
        *,
        completed_at: datetime,
    ) -> None:
        for outcome in run.outcomes:
            if outcome.receipt is not None:
                applied = outcome.receipt.applied_fill
                self._store.append_event(
                    event_type=PaperOrderEventType.ORDER_FILL_APPLIED,
                    stream_id=f"order:{outcome.client_order_id}",
                    run_id=run_id,
                    occurred_at=completed_at,
                    idempotency_key=(
                        f"run-fill:{run_id}:{applied.fill.fill_id}"
                    ),
                    payload={
                        "account_id": applied.fill.account_id,
                        "client_order_id": outcome.client_order_id,
                        "fill_id": applied.fill.fill_id,
                        "ledger_event_sequence": outcome.receipt.event_sequence,
                        "schema_version": 1,
                    },
                )
            snapshot = matcher.order(outcome.client_order_id)
            if snapshot is None:  # pragma: no cover - matcher invariant
                raise PaperOrderStoreConflictError("matched order state disappeared")
            self._record_state(snapshot, run_id=run_id, phase="bar")
        self._store.complete_run(
            run_id=run_id,
            owner_id=self._owner_id,
            completed_at=completed_at,
            payload={
                "consumed_volume": run.consumed_volume,
                "outcomes": [
                    {
                        "client_order_id": outcome.client_order_id,
                        "filled_quantity": outcome.filled_quantity,
                        "reason": outcome.reason.value,
                        "status_after": outcome.status_after.value,
                    }
                    for outcome in run.outcomes
                ],
                "schema_version": 1,
                "volume_capacity": run.volume_capacity,
            },
        )

    def _record_state(
        self,
        snapshot: PaperOrderSnapshot,
        *,
        run_id: str | None,
        phase: str,
    ) -> None:
        client_order_id = snapshot.intent.order.client_order_id
        key_scope = run_id or phase
        self._store.append_event(
            event_type=PaperOrderEventType.ORDER_STATE_APPLIED,
            stream_id=f"order:{client_order_id}",
            run_id=run_id,
            occurred_at=snapshot.updated_at,
            idempotency_key=f"order-state:{key_scope}:{client_order_id}",
            payload={"schema_version": 1, "snapshot": _snapshot_document(snapshot)},
        )

    def _renew_writer(self) -> None:
        self._store.acquire_writer(
            owner_id=self._owner_id,
            now=datetime.now(UTC),
            lease_duration=self._lease_duration,
        )


def _intent_document(intent: ASharePaperOrderIntent) -> dict[str, object]:
    order = intent.order
    return {
        "account_id": order.account_id,
        "client_order_id": order.client_order_id,
        "created_at": order.created_at.astimezone(UTC).isoformat(),
        "decision_session_date": intent.decision_session_date.isoformat(),
        "expires_on": intent.expires_on.isoformat() if intent.expires_on else None,
        "instrument_type": intent.instrument_type.value,
        "limit_price": str(order.limit_price),
        "order_type": order.order_type.value,
        "quantity": str(order.quantity),
        "side": order.side.value,
        "strategy_id": order.strategy_id,
        "symbol": order.symbol,
        "time_in_force": intent.time_in_force.value,
    }


def _intent_from_document(value: dict[str, Any]) -> ASharePaperOrderIntent:
    return ASharePaperOrderIntent(
        order=OrderIntent(
            client_order_id=_string(value, "client_order_id"),
            account_id=_string(value, "account_id"),
            strategy_id=_string(value, "strategy_id"),
            symbol=_string(value, "symbol"),
            side=Side(_string(value, "side")),
            quantity=Decimal(_string(value, "quantity")),
            limit_price=Decimal(_string(value, "limit_price")),
            created_at=datetime.fromisoformat(_string(value, "created_at")),
            order_type=OrderType(_string(value, "order_type")),
        ),
        instrument_type=PaperInstrumentType(_string(value, "instrument_type")),
        decision_session_date=date.fromisoformat(
            _string(value, "decision_session_date")
        ),
        time_in_force=PaperTimeInForce(_string(value, "time_in_force")),
        expires_on=(
            date.fromisoformat(str(value["expires_on"]))
            if value.get("expires_on") is not None
            else None
        ),
    )


def _snapshot_document(snapshot: PaperOrderSnapshot) -> dict[str, object]:
    return {
        "average_fill_price": (
            str(snapshot.average_fill_price)
            if snapshot.average_fill_price is not None
            else None
        ),
        "filled_quantity": snapshot.filled_quantity,
        "intent": _intent_document(snapshot.intent),
        "last_processed_session": (
            snapshot.last_processed_session.isoformat()
            if snapshot.last_processed_session
            else None
        ),
        "reason": snapshot.reason.value if snapshot.reason else None,
        "reserved_cash": str(snapshot.reserved_cash),
        "reserved_quantity": snapshot.reserved_quantity,
        "status": snapshot.status.value,
        "submitted_at": snapshot.submitted_at.isoformat(),
        "updated_at": snapshot.updated_at.isoformat(),
    }


def _snapshot_from_document(value: dict[str, Any]) -> PaperOrderSnapshot:
    average = value.get("average_fill_price")
    last_session = value.get("last_processed_session")
    reason = value.get("reason")
    return PaperOrderSnapshot(
        intent=_intent_from_document(_mapping(value, "intent")),
        status=PaperOrderStatus(_string(value, "status")),
        filled_quantity=_integer(value, "filled_quantity"),
        average_fill_price=Decimal(str(average)) if average is not None else None,
        reserved_cash=Decimal(_string(value, "reserved_cash")),
        reserved_quantity=_integer(value, "reserved_quantity"),
        submitted_at=datetime.fromisoformat(_string(value, "submitted_at")),
        updated_at=datetime.fromisoformat(_string(value, "updated_at")),
        reason=PaperMatchReason(str(reason)) if reason is not None else None,
        last_processed_session=(
            date.fromisoformat(str(last_session)) if last_session is not None else None
        ),
    )


def _bar_document(bar: SimulatedDailyBar) -> dict[str, object]:
    return {
        "available_at": bar.available_at.isoformat(),
        "close": str(bar.close) if bar.close is not None else None,
        "high": str(bar.high) if bar.high is not None else None,
        "is_trading": bar.is_trading,
        "low": str(bar.low) if bar.low is not None else None,
        "open": str(bar.open) if bar.open is not None else None,
        "price_band": (
            None
            if bar.price_band is None
            else {
                "lower": str(bar.price_band.lower) if bar.price_band.lower else None,
                "upper": str(bar.price_band.upper) if bar.price_band.upper else None,
            }
        ),
        "source_revision": bar.source_revision,
        "symbol": bar.symbol,
        "trade_date": bar.trade_date.isoformat(),
        "volume_shares": bar.volume_shares,
    }


def _bar_from_document(value: dict[str, Any]) -> SimulatedDailyBar:
    band_value = value.get("price_band")
    band = None
    if band_value is not None:
        band_document = _object(band_value)
        lower = band_document.get("lower")
        upper = band_document.get("upper")
        band = ExplicitPriceBand(
            Decimal(str(lower)) if lower is not None else None,
            Decimal(str(upper)) if upper is not None else None,
        )
    return SimulatedDailyBar(
        symbol=_string(value, "symbol"),
        trade_date=date.fromisoformat(_string(value, "trade_date")),
        open=_optional_decimal(value.get("open")),
        high=_optional_decimal(value.get("high")),
        low=_optional_decimal(value.get("low")),
        close=_optional_decimal(value.get("close")),
        volume_shares=_integer(value, "volume_shares"),
        is_trading=_boolean(value, "is_trading"),
        available_at=datetime.fromisoformat(_string(value, "available_at")),
        source_revision=_string(value, "source_revision"),
        price_band=band,
    )


def _config_document(config: PaperMatchingConfig) -> dict[str, object]:
    return {
        "buy_lot_size": config.buy_lot_size,
        "etf_price_quantum": str(config.etf_price_quantum),
        "etf_slippage_rate": str(config.etf_slippage_rate),
        "stock_price_quantum": str(config.stock_price_quantum),
        "stock_slippage_rate": str(config.stock_slippage_rate),
        "volume_participation_rate": str(config.volume_participation_rate),
    }


def _config_from_document(value: dict[str, Any]) -> PaperMatchingConfig:
    return PaperMatchingConfig(
        volume_participation_rate=Decimal(
            _string(value, "volume_participation_rate")
        ),
        stock_slippage_rate=Decimal(_string(value, "stock_slippage_rate")),
        etf_slippage_rate=Decimal(_string(value, "etf_slippage_rate")),
        stock_price_quantum=Decimal(_string(value, "stock_price_quantum")),
        etf_price_quantum=Decimal(_string(value, "etf_price_quantum")),
        buy_lot_size=_integer(value, "buy_lot_size"),
    )


def _mapping(value: dict[str, Any], name: str) -> dict[str, Any]:
    return _object(value.get(name))


def _object(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PaperOrderStoreConflictError("stored paper-order payload is malformed")
    return value


def _list(value: dict[str, Any], name: str) -> list[object]:
    result = value.get(name)
    if not isinstance(result, list):
        raise PaperOrderStoreConflictError(f"stored {name} is malformed")
    return result


def _string(value: dict[str, Any], name: str) -> str:
    result = value.get(name)
    if not isinstance(result, str) or not result:
        raise PaperOrderStoreConflictError(f"stored {name} is malformed")
    return result


def _integer(value: dict[str, Any], name: str) -> int:
    result = value.get(name)
    if isinstance(result, bool) or not isinstance(result, int):
        raise PaperOrderStoreConflictError(f"stored {name} is malformed")
    return result


def _boolean(value: dict[str, Any], name: str) -> bool:
    result = value.get(name)
    if not isinstance(result, bool):
        raise PaperOrderStoreConflictError(f"stored {name} is malformed")
    return result


def _optional_decimal(value: object) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
