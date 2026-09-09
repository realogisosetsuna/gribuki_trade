from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from gribuki_trade.trading.futures.futures_models import (
    FuturesCommandStatus,
    FuturesFill,
    FuturesOrderSnapshot,
    FuturesOrderStatus,
    FuturesPositionSnapshot,
    FuturesStreamHealth,
    FuturesUserEvent,
)
from gribuki_trade.trading.futures.futures_oms import FuturesOrderManagementStore

NOW = datetime(2026, 9, 9, 1, 0, tzinfo=UTC)


def order(
    key: str = "entry-1",
    *,
    status: FuturesOrderStatus = FuturesOrderStatus.NEW,
    status_time_ms: int = 1,
    position_side: str = "LONG",
) -> FuturesOrderSnapshot:
    return FuturesOrderSnapshot(
        account_id="acct",
        environment="LIVE",
        product="USDS_FUTURES",
        order_key=key,
        symbol="BTCUSDT",
        side="BUY",
        position_side=position_side,
        status=status,
        status_time_ms=status_time_ms,
        quantity=Decimal("0.001"),
        client_order_id=key,
    )


class FuturesOMSTests(TestCase):
    def setUp(self) -> None:
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "futures.sqlite3"
        self.store = FuturesOrderManagementStore(self.path)
        self.addCleanup(self.store.close)
        self.scope = {"account_id": "acct", "environment": "LIVE", "product": "USDS_FUTURES"}

    def test_order_status_is_monotonic_and_optional_protection_fields_survive(self) -> None:
        first = FuturesOrderSnapshot(
            **_order_kwargs("algo-1"),
            kind="ALGO",
            status="NEW",
            status_time_ms=10,
            trigger_price=Decimal("70000"),
            activate_price=Decimal("70500"),
            callback_rate=Decimal("0.5"),
        )
        self.store.upsert_order(first)
        stale = FuturesOrderSnapshot(
            **_order_kwargs("algo-1"), kind="ALGO", status="NEW", status_time_ms=9
        )
        self.store.upsert_order(stale)
        current = self.store.order("algo-1", **self.scope)
        assert current is not None
        self.assertEqual(current.trigger_price, Decimal("70000"))
        self.assertEqual(current.callback_rate, Decimal("0.5"))
        self.store.upsert_order(
            FuturesOrderSnapshot(
                **_order_kwargs("algo-1"), kind="ALGO", status="FILLED", status_time_ms=20
            )
        )
        self.store.upsert_order(
            FuturesOrderSnapshot(
                **_order_kwargs("algo-1"), kind="ALGO", status="NEW", status_time_ms=21
            )
        )
        self.assertIs(self.store.order("algo-1", **self.scope).status, FuturesOrderStatus.FILLED)  # type: ignore[union-attr]

    def test_trade_id_and_event_identity_are_idempotent(self) -> None:
        fill = FuturesFill(
            **self.scope,
            fill_id="fill-1",
            trade_id="trade-1",
            symbol="BTCUSDT",
            side="BUY",
            position_side="LONG",
            quantity=Decimal("0.001"),
            price=Decimal("70000"),
            occurred_at=NOW,
        )
        self.store.record_fill(fill)
        duplicate = FuturesFill(
            **{
                **self.scope,
                "fill_id": "different-id",
                "trade_id": "trade-1",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "position_side": "LONG",
                "quantity": Decimal("0.001"),
                "price": Decimal("70000"),
                "occurred_at": NOW,
            }
        )
        self.store.record_fill(duplicate)
        self.assertEqual(len(self.store.fills(**self.scope)), 1)
        event = FuturesUserEvent("ACCOUNT_UPDATE", 100, 99, 101, {"x": "y"}, 1)
        self.assertTrue(self.store.append_event(event, **self.scope))
        self.assertFalse(self.store.append_event(event, **self.scope))
        self.assertEqual(len(self.store.events(**self.scope)), 1)

    def test_empty_position_snapshot_clears_old_rows_and_scope_isolation_holds(self) -> None:
        position = FuturesPositionSnapshot(
            **self.scope,
            symbol="BTCUSDT",
            position_side="LONG",
            quantity=Decimal("1"),
            updated_at=NOW,
        )
        self.store.upsert_position(position)
        self.store.replace_positions((), **self.scope)
        self.assertEqual(self.store.positions(**self.scope), ())
        other = FuturesPositionSnapshot(
            account_id="other",
            environment="LIVE",
            product="USDS_FUTURES",
            symbol="BTCUSDT",
            position_side="LONG",
            quantity=Decimal("1"),
            updated_at=NOW,
        )
        self.store.upsert_position(other)
        self.assertEqual(
            len(
                self.store.positions(account_id="other", environment="LIVE", product="USDS_FUTURES")
            ),
            1,
        )
        self.assertEqual(self.store.positions(**self.scope), ())

    def test_owner_fencing_prevents_duplicate_claim_and_recovery_keeps_unknown(self) -> None:
        token = self.store.acquire_owner(**self.scope, owner_id="worker-a", now=NOW)
        self.store.enqueue_command(
            **self.scope,
            command_id="cmd-1",
            command_type="SUBMIT_ALGO",
            payload={"order_key": "algo-1"},
            order=order("algo-1"),
            occurred_at=NOW,
        )
        claimed = self.store.claim_command(
            "cmd-1", **self.scope, owner_id="worker-a", fencing_token=token, now=NOW
        )
        assert claimed is not None
        self.assertIsNone(
            self.store.claim_command(
                "cmd-1", **self.scope, owner_id="worker-a", fencing_token=token, now=NOW
            )
        )
        self.store.close()
        self.store = FuturesOrderManagementStore(self.path)
        self.addCleanup(self.store.close)
        recovered = self.store.recover_inflight(**self.scope, now=NOW + timedelta(seconds=1))
        self.assertEqual(len(recovered), 1)
        self.assertIs(recovered[0].status, FuturesCommandStatus.UNKNOWN)
        self.assertIsNone(
            self.store.claim_command(
                "cmd-1",
                **self.scope,
                owner_id="worker-a",
                fencing_token=token,
                now=NOW + timedelta(seconds=2),
            )
        )

    def test_health_is_scoped_and_persisted(self) -> None:
        health = FuturesStreamHealth(
            **self.scope,
            state="DEGRADED",
            connection_epoch=2,
            gap_count=1,
            reason="reconnect",
            updated_at=NOW,
        )
        self.store.set_stream_health(health)
        self.assertEqual(self.store.health(**self.scope), health)

    def test_reconciliation_cutoff_preserves_newer_position_and_rejects_late_stale_update(
        self,
    ) -> None:
        before = FuturesPositionSnapshot(
            **self.scope,
            symbol="BTCUSDT",
            position_side="LONG",
            quantity=Decimal("1"),
            updated_at=NOW,
        )
        self.store.upsert_position(before)
        cutoff = NOW + timedelta(seconds=1)
        newer = FuturesPositionSnapshot(
            **self.scope,
            symbol="BTCUSDT",
            position_side="LONG",
            quantity=Decimal("2"),
            updated_at=cutoff + timedelta(seconds=1),
        )
        self.store.upsert_position(newer)
        self.store.replace_positions((), **self.scope, cutoff_at=cutoff)
        self.assertEqual(self.store.positions(**self.scope)[0].quantity, Decimal("2"))
        stale = FuturesPositionSnapshot(
            **self.scope,
            symbol="ETHUSDT",
            position_side="LONG",
            quantity=Decimal("7"),
            updated_at=cutoff,
        )
        self.store.upsert_position(stale)
        self.assertEqual(self.store.positions(**self.scope), (newer,))

    def test_trade_ids_are_symbol_scoped_and_unknown_command_requires_evidence(self) -> None:
        for symbol in ("BTCUSDT", "ETHUSDT"):
            self.store.record_fill(
                FuturesFill(
                    **self.scope,
                    fill_id=f"fill-{symbol}",
                    trade_id="same-trade-id",
                    symbol=symbol,
                    side="BUY",
                    position_side="LONG",
                    quantity=Decimal("0.001"),
                    price=Decimal("70000"),
                    occurred_at=NOW,
                )
            )
        self.assertEqual(len(self.store.fills(**self.scope)), 2)
        self.store.enqueue_command(
            **self.scope,
            command_id="cmd-unknown",
            command_type="SUBMIT_ALGO",
            payload={"symbol": "BTCUSDT"},
            occurred_at=NOW,
        )
        token = self.store.acquire_owner(**self.scope, owner_id="owner", now=NOW)
        self.store.claim_command(
            "cmd-unknown", **self.scope, owner_id="owner", fencing_token=token, now=NOW
        )
        self.store.finish_command(
            "cmd-unknown",
            **self.scope,
            owner_id="owner",
            fencing_token=token,
            status=FuturesCommandStatus.UNKNOWN,
            now=NOW,
        )
        with self.assertRaises(ValueError):
            self.store.resolve_command("cmd-unknown", **self.scope, evidence={}, now=NOW)
        resolved = self.store.resolve_command(
            "cmd-unknown", **self.scope, evidence={"orderId": "42"}, now=NOW
        )
        self.assertIs(resolved.status, FuturesCommandStatus.RESOLVED)


def _order_kwargs(key: str) -> dict[str, object]:
    return {
        "account_id": "acct",
        "environment": "LIVE",
        "product": "USDS_FUTURES",
        "order_key": key,
        "symbol": "BTCUSDT",
        "side": "SELL",
        "position_side": "LONG",
        "quantity": Decimal("0.001"),
        "client_order_id": key,
    }
