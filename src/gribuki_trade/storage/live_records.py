"""实盘观察账户的 SQLite 权威账本与可恢复工作队列。

成交事件永远只追加；命令索引、持仓投影、买入批次和工作租约是同一数据库
内的事务投影。确认成交时，事件、库存、外部成交去重键和后续保护任务要么
一起提交，要么全部回滚，避免“账本写了但保护任务丢了”或并发超卖。
"""

from __future__ import annotations

import hashlib
import sqlite3
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from os import PathLike
from pathlib import Path
from typing import cast

from gribuki_trade.domain.live_records import (
    ConfirmedLiveFill,
    LiveProtectionTracking,
    LiveRecordEvent,
    LiveRecordEventType,
    LiveWorkItem,
    LiveWorkKind,
    LiveWorkStatus,
    NewLiveRecordEvent,
)
from gribuki_trade.domain.orders import Side
from gribuki_trade.storage.live_record_codec import (
    _aware_utc,
    _canonical_json,
    _deep_protection_work_id,
    _digest_key,
    _error_code,
    _event_hash,
    _lease_attempt,
    _parse_time,
    _protection_id,
    _protection_work_id,
    _time,
)
from gribuki_trade.storage.live_record_errors import (
    LiveRecordConflictError,
    LiveRecordIntegrityError,
    LiveRecordStateError,
    LiveRecordStoreError,
)
from gribuki_trade.storage.live_record_integrity import (
    _json_object,
    _mapping,
    _text,
    _verify,
)
from gribuki_trade.storage.live_record_models import (
    LiveConfirmationCommit,
    StoredLiveCommand,
    _row_to_command,
    _row_to_event,
    _row_to_tracking,
    _row_to_work,
)
from gribuki_trade.storage.live_record_protection_policy import (
    SellLot,
    SellLotAllocationError,
    allocate_sell_lots,
    sellable_quantity_for_t1,
)
from gribuki_trade.storage.live_record_schema import ensure_live_record_schema
from gribuki_trade.storage.live_record_work_policy import (
    build_claim_due_work_query,
    normalize_lease_for,
    normalize_work_claim,
    normalize_work_failure,
    project_work_failure,
)


class SQLiteLiveRecordStore:
    """每个账户独立哈希链，并对跨进程写入使用 ``BEGIN IMMEDIATE``。"""

    def __init__(self, path: str | PathLike[str]) -> None:
        raw_path = str(path)
        if raw_path != ":memory:":
            ledger_path = Path(path)
            if ledger_path.is_symlink():
                raise LiveRecordStoreError("实盘账本路径不得是符号链接")
            if ledger_path.exists() and not ledger_path.is_file():
                raise LiveRecordStoreError("实盘账本路径必须是普通文件")
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            path,
            timeout=15.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 15000")
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._initialize()

    def _initialize(self) -> None:
        # ``executescript`` 会隐式结束现有事务，因此先单独完成幂等 DDL，
        # 再用真正的 IMMEDIATE 事务执行旧账本投影迁移。
        with self._lock:
            self._ensure_open()
            connection = self._connection
            ensure_live_record_schema(connection)
        with self._transaction() as connection:
            event_count = int(
                connection.execute("SELECT COUNT(*) FROM live_record_events").fetchone()[0]
            )
            command_count = int(
                connection.execute("SELECT COUNT(*) FROM live_commands").fetchone()[0]
            )
            if event_count and not command_count:
                self._rebuild_materialized_projection(connection)

    def register_proposal(
        self,
        *,
        fill: ConfirmedLiveFill,
        sender_id: str,
        source_message_id: str,
        event: NewLiveRecordEvent,
    ) -> tuple[LiveRecordEvent, bool]:
        """原子写入待确认命令及其只追加提案事件。"""

        if event.event_type is not LiveRecordEventType.COMMAND_PROPOSED:
            raise ValueError("proposal event type is invalid")
        if event.account_id != fill.account_id:
            raise ValueError("proposal account does not match fill")
        fill_json = fill.canonical_json()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM live_commands WHERE command_id = ?",
                (fill.command_id,),
            ).fetchone()
            if existing is not None:
                command = _row_to_command(existing)
                if (
                    command.account_id != fill.account_id
                    or command.sender_id != sender_id
                    or command.source_message_id != source_message_id
                    or command.fingerprint != fill.fingerprint
                    or command.fill_json != fill_json
                ):
                    raise LiveRecordStateError("COMMAND_ID_ALREADY_EXISTS")
                row = connection.execute(
                    "SELECT * FROM live_record_events WHERE event_id = ?",
                    (command.proposal_event_id,),
                ).fetchone()
                if row is None:
                    raise LiveRecordIntegrityError("proposal index points to a missing event")
                return _row_to_event(row), False

            stored = self._append_in_transaction(connection, event)
            try:
                connection.execute(
                    """
                    INSERT INTO live_commands (
                        command_id, account_id, sender_id, source_message_id,
                        fingerprint, fill_json, state, proposal_event_id,
                        terminal_event_id, proposed_at, terminal_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, NULL, ?, NULL)
                    """,
                    (
                        fill.command_id,
                        fill.account_id,
                        sender_id,
                        source_message_id,
                        fill.fingerprint,
                        fill_json,
                        stored.event_id,
                        _time(stored.occurred_at),
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise LiveRecordConflictError("proposal identity is already used") from error
            return stored, True

    def confirm_fill(
        self,
        *,
        fill: ConfirmedLiveFill,
        sender_id: str,
        fingerprint: str,
        event: NewLiveRecordEvent,
    ) -> LiveConfirmationCommit:
        """在一个事务中确认成交、校验库存、更新投影并创建保护任务。"""

        if event.event_type is not LiveRecordEventType.FILL_CONFIRMED:
            raise ValueError("confirmation event type is invalid")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM live_commands WHERE command_id = ?",
                (fill.command_id,),
            ).fetchone()
            if row is None:
                raise LiveRecordStateError("COMMAND_NOT_FOUND")
            command = _row_to_command(row)
            self._validate_confirmation(command, fill, sender_id, fingerprint)
            if command.state != "PENDING":
                if command.terminal_event_id is None:
                    raise LiveRecordIntegrityError("terminal command has no terminal event")
                terminal_row = connection.execute(
                    "SELECT * FROM live_record_events WHERE event_id = ?",
                    (command.terminal_event_id,),
                ).fetchone()
                if terminal_row is None:
                    raise LiveRecordIntegrityError("terminal event is missing")
                terminal = _row_to_event(terminal_row)
                is_buy_fill = (
                    terminal.event_type is LiveRecordEventType.FILL_CONFIRMED
                    and fill.side is Side.BUY
                )
                return LiveConfirmationCommit(
                    event=terminal,
                    created=False,
                    protection_id=(
                        _protection_id(fill.account_id, fill.command_id) if is_buy_fill else None
                    ),
                    protection_work_id=(
                        _protection_work_id(fill.account_id, fill.command_id)
                        if is_buy_fill
                        else None
                    ),
                )

            duplicate_fill = connection.execute(
                """
                SELECT command_id FROM live_external_fills
                WHERE account_id = ? AND external_fill_key = ?
                """,
                (fill.account_id, fill.external_fill_key),
            ).fetchone()
            if duplicate_fill is not None:
                raise LiveRecordStateError("EXTERNAL_FILL_ALREADY_RECORDED")

            position = connection.execute(
                """
                SELECT * FROM live_position_projection
                WHERE account_id = ? AND symbol = ?
                """,
                (fill.account_id, fill.symbol),
            ).fetchone()
            quantity = 0 if position is None else int(position["quantity"])
            average = Decimal("0") if position is None else Decimal(str(position["average_cost"]))
            realized = Decimal("0") if position is None else Decimal(str(position["realized_pnl"]))
            if (
                position is not None
                and str(position["instrument_type"]) != fill.instrument_type.value
            ):
                raise LiveRecordStateError("INSTRUMENT_TYPE_MISMATCH")
            if fill.side is Side.SELL and quantity < fill.quantity:
                raise LiveRecordStateError("RECORDED_POSITION_INSUFFICIENT")

            stored = self._append_in_transaction(connection, event)
            try:
                connection.execute(
                    """
                    INSERT INTO live_external_fills (
                        account_id, external_fill_key, command_id, event_id,
                        fill_json, confirmed_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fill.account_id,
                        fill.external_fill_key,
                        fill.command_id,
                        stored.event_id,
                        fill.canonical_json(),
                        _time(stored.occurred_at),
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise LiveRecordStateError("EXTERNAL_FILL_ALREADY_RECORDED") from error

            protection_id: str | None = None
            work_id: str | None = None
            if fill.side is Side.BUY:
                new_quantity = quantity + fill.quantity
                average = (average * quantity + fill.trade_value + fill.fees.total) / new_quantity
                quantity = new_quantity
                protection_id = _protection_id(fill.account_id, fill.command_id)
                work_id = _protection_work_id(fill.account_id, fill.command_id)
                connection.execute(
                    """
                    INSERT INTO live_protection_tracking (
                        protection_id, account_id, buy_command_id, symbol,
                        instrument_type, acquired_at, original_quantity,
                        remaining_quantity, plan_ready, plan_stream_id,
                        last_observed_bar_end, last_alert_bar_end
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL)
                    """,
                    (
                        protection_id,
                        fill.account_id,
                        fill.command_id,
                        fill.symbol,
                        fill.instrument_type.value,
                        _time(fill.executed_at),
                        fill.quantity,
                        fill.quantity,
                    ),
                )
                self._insert_work(
                    connection,
                    work_id=work_id,
                    kind=LiveWorkKind.BUILD_PROTECTION,
                    account_id=fill.account_id,
                    command_id=fill.command_id,
                    protection_id=protection_id,
                    payload={
                        "fill": fill.canonical_document(),
                        "protection_id": protection_id,
                        "schema_version": 1,
                    },
                    available_at=stored.occurred_at,
                )
                queued = NewLiveRecordEvent(
                    event_id=(
                        "live-protection-queued-" + _digest_key(fill.account_id, fill.command_id)
                    ),
                    account_id=fill.account_id,
                    event_type=LiveRecordEventType.PROTECTION_WORK_QUEUED,
                    occurred_at=stored.occurred_at,
                    idempotency_key=f"protect-queued:{fill.command_id}",
                    payload_json=_canonical_json(
                        {
                            "command_id": fill.command_id,
                            "protection_id": protection_id,
                            "work_id": work_id,
                        }
                    ),
                )
                self._append_in_transaction(connection, queued)
            else:
                realized += fill.trade_value - fill.fees.total - average * fill.quantity
                quantity -= fill.quantity
                if quantity == 0:
                    average = Decimal("0")
                closed_lots = self._allocate_sell_lots(connection, fill, stored.occurred_at)
                for protection_id, buy_command_id, allocated_quantity in closed_lots:
                    close_work_id = "live-work-close-" + _digest_key(
                        protection_id,
                        fill.command_id,
                    )
                    self._insert_work(
                        connection,
                        work_id=close_work_id,
                        kind=LiveWorkKind.CLOSE_PROTECTION,
                        account_id=fill.account_id,
                        command_id=fill.command_id,
                        protection_id=protection_id,
                        payload={
                            "allocated_quantity": allocated_quantity,
                            "buy_command_id": buy_command_id,
                            "protection_id": protection_id,
                            "sell_fill": fill.canonical_document(),
                        },
                        available_at=stored.occurred_at,
                    )
                    close_audit = NewLiveRecordEvent(
                        event_id="live-protection-close-queued-"
                        + _digest_key(protection_id, fill.command_id),
                        account_id=fill.account_id,
                        event_type=LiveRecordEventType.PROTECTION_CLOSE_WORK_QUEUED,
                        occurred_at=stored.occurred_at,
                        idempotency_key=(
                            "protect-close-queued:" + _digest_key(protection_id, fill.command_id)
                        ),
                        payload_json=_canonical_json(
                            {
                                "allocated_quantity": allocated_quantity,
                                "protection_id": protection_id,
                                "sell_command_id": fill.command_id,
                                "work_id": close_work_id,
                            }
                        ),
                    )
                    self._append_in_transaction(connection, close_audit)

            connection.execute(
                """
                INSERT INTO live_position_projection (
                    account_id, symbol, instrument_type, quantity,
                    average_cost, realized_pnl, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, symbol) DO UPDATE SET
                    instrument_type = excluded.instrument_type,
                    quantity = excluded.quantity,
                    average_cost = excluded.average_cost,
                    realized_pnl = excluded.realized_pnl,
                    updated_at = excluded.updated_at
                """,
                (
                    fill.account_id,
                    fill.symbol,
                    fill.instrument_type.value,
                    quantity,
                    str(average),
                    str(realized),
                    _time(stored.occurred_at),
                ),
            )
            connection.execute(
                """
                UPDATE live_commands
                SET state = 'CONFIRMED', terminal_event_id = ?, terminal_at = ?
                WHERE command_id = ? AND state = 'PENDING'
                """,
                (stored.event_id, _time(stored.occurred_at), fill.command_id),
            )
            return LiveConfirmationCommit(
                event=stored,
                created=True,
                protection_id=protection_id,
                protection_work_id=work_id,
            )

    def cancel_command(
        self,
        *,
        command_id: str,
        sender_id: str,
        event: NewLiveRecordEvent,
    ) -> tuple[LiveRecordEvent, bool]:
        """仅允许原提案发送者原子取消仍处于待确认状态的命令。"""

        if event.event_type is not LiveRecordEventType.COMMAND_CANCELLED:
            raise ValueError("cancellation event type is invalid")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM live_commands WHERE command_id = ?",
                (command_id,),
            ).fetchone()
            if row is None:
                raise LiveRecordStateError("COMMAND_NOT_FOUND")
            command = _row_to_command(row)
            if command.sender_id != sender_id:
                raise LiveRecordStateError("CANCELLING_SENDER_MISMATCH")
            if command.state != "PENDING":
                if command.terminal_event_id is None:
                    raise LiveRecordIntegrityError("terminal command has no terminal event")
                terminal_row = connection.execute(
                    "SELECT * FROM live_record_events WHERE event_id = ?",
                    (command.terminal_event_id,),
                ).fetchone()
                if terminal_row is None:
                    raise LiveRecordIntegrityError("terminal event is missing")
                return _row_to_event(terminal_row), False
            stored = self._append_in_transaction(connection, event)
            connection.execute(
                """
                UPDATE live_commands
                SET state = 'CANCELLED', terminal_event_id = ?, terminal_at = ?
                WHERE command_id = ? AND state = 'PENDING'
                """,
                (stored.event_id, _time(stored.occurred_at), command_id),
            )
            return stored, True

    def command(self, command_id: str) -> StoredLiveCommand | None:
        """按全局唯一命令号读取两阶段确认索引。"""

        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT * FROM live_commands WHERE command_id = ?",
                (command_id.strip(),),
            ).fetchone()
        return None if row is None else _row_to_command(row)

    def append(self, event: NewLiveRecordEvent) -> tuple[LiveRecordEvent, bool]:
        """追加工作流审计事件；成交三类事件必须走专用事务方法。"""

        if event.event_type in {
            LiveRecordEventType.COMMAND_PROPOSED,
            LiveRecordEventType.FILL_CONFIRMED,
            LiveRecordEventType.COMMAND_CANCELLED,
        }:
            raise ValueError("trade events require the transactional command API")
        with self._transaction() as connection:
            duplicate = self._event_by_key(connection, event.account_id, event.idempotency_key)
            if duplicate is not None:
                stored = _row_to_event(duplicate)
                if (
                    stored.event_type is not event.event_type
                    or stored.payload_json != event.payload_json
                ):
                    raise LiveRecordConflictError(
                        "idempotency key is bound to different live-record content"
                    )
                return stored, False
            return self._append_in_transaction(connection, event), True

    def events(self, account_id: str) -> tuple[LiveRecordEvent, ...]:
        normalized = account_id.strip()
        if not normalized:
            raise ValueError("account_id must not be empty")
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT * FROM live_record_events
                WHERE account_id = ? ORDER BY sequence
                """,
                (normalized,),
            ).fetchall()
        events = tuple(_row_to_event(row) for row in rows)
        _verify(events)
        return events

    def account_ids(self) -> tuple[str, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                "SELECT DISTINCT account_id FROM live_record_events ORDER BY account_id"
            ).fetchall()
        return tuple(str(row["account_id"]) for row in rows)

    def tracking(
        self,
        account_id: str,
        *,
        symbol: str | None = None,
        active_only: bool = True,
    ) -> tuple[LiveProtectionTracking, ...]:
        """读取待建计划或持续观察中的实盘买入批次。"""

        clauses = ["account_id = ?"]
        parameters: list[object] = [account_id.strip()]
        if symbol is not None:
            clauses.append("symbol = ?")
            parameters.append(symbol.strip().upper())
        if active_only:
            clauses.append("remaining_quantity > 0")
        query = (
            "SELECT * FROM live_protection_tracking WHERE "
            + " AND ".join(clauses)
            + " ORDER BY acquired_at, buy_command_id"
        )
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(query, tuple(parameters)).fetchall()
        return tuple(_row_to_tracking(row) for row in rows)

    def work_items(
        self,
        account_id: str,
        *,
        include_terminal: bool = True,
    ) -> tuple[LiveWorkItem, ...]:
        """读取账户的 durable saga 状态，供运行健康检查和恢复界面使用。"""

        normalized = account_id.strip()
        if not normalized:
            raise ValueError("account_id must not be empty")
        terminal_filter = "" if include_terminal else " AND status NOT IN ('COMPLETED','DEAD')"
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                "SELECT * FROM live_work_items WHERE account_id = ?"
                + terminal_filter
                + " ORDER BY created_at, work_id",
                (normalized,),
            ).fetchall()
        return tuple(_row_to_work(row) for row in rows)

    def sellable_quantity(
        self,
        protection_id: str,
        *,
        as_of: datetime,
    ) -> int:
        """按上海自然交易日计算该买入批次当前可卖数量（A 股 T+1）。"""

        moment = _aware_utc(as_of)
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT acquired_at, remaining_quantity FROM live_protection_tracking "
                "WHERE protection_id = ?",
                (protection_id,),
            ).fetchone()
        if row is None:
            raise LiveRecordStateError("PROTECTION_NOT_FOUND")
        return sellable_quantity_for_t1(
            _parse_time(str(row["acquired_at"])),
            int(row["remaining_quantity"]),
            as_of=moment,
        )

    def claim_due_work(
        self,
        *,
        now: datetime,
        lease_for: timedelta = timedelta(minutes=5),
        kinds: frozenset[LiveWorkKind] | None = None,
        work_ids: frozenset[str] | None = None,
        limit: int = 10,
    ) -> tuple[LiveWorkItem, ...]:
        """领取到期工作；过期租约可被另一应用进程安全接管。"""

        policy = normalize_work_claim(
            now=now,
            lease_for=lease_for,
            kinds=kinds,
            work_ids=work_ids,
            limit=limit,
        )
        if policy.work_ids == ():
            return ()
        with self._transaction() as connection:
            query, parameters = build_claim_due_work_query(policy)
            rows = connection.execute(
                query,
                parameters,
            ).fetchall()
            claimed: list[LiveWorkItem] = []
            lease_until = policy.moment + policy.lease_for
            for row in rows:
                work_id = str(row["work_id"])
                connection.execute(
                    """
                    UPDATE live_work_items SET
                        status = 'RUNNING', attempts = attempts + 1,
                        lease_until = ?, updated_at = ?, error_code = NULL
                    WHERE work_id = ?
                    """,
                    (_time(lease_until), _time(policy.moment), work_id),
                )
                claimed_row = connection.execute(
                    "SELECT * FROM live_work_items WHERE work_id = ?",
                    (work_id,),
                ).fetchone()
                if claimed_row is None:
                    raise LiveRecordIntegrityError("claimed work disappeared")
                claimed.append(_row_to_work(claimed_row))
            return tuple(claimed)

    def complete_protection_work(
        self,
        work_id: str,
        *,
        lease_attempt: int,
        completed_at: datetime,
        result_code: str,
        plan_id: str,
        plan_stream_id: str | None = None,
    ) -> LiveWorkItem:
        """按租约代际原子激活候选计划流；旧 worker 只能留下不可见候选。"""

        moment = _aware_utc(completed_at)
        result = _error_code(result_code)
        with self._transaction() as connection:
            row = self._work_for_completion(connection, work_id, None, lease_attempt)
            work = _row_to_work(row)
            if work.kind not in {
                LiveWorkKind.BUILD_PROTECTION,
                LiveWorkKind.BUILD_DEEP_PROTECTION,
            }:
                raise LiveRecordStateError("WORK_KIND_MISMATCH")
            if work.status is LiveWorkStatus.COMPLETED:
                return work
            if work.protection_id is None:
                raise LiveRecordIntegrityError("protection work has no protection id")
            tracking = connection.execute(
                "SELECT remaining_quantity, plan_ready FROM live_protection_tracking "
                "WHERE protection_id = ?",
                (work.protection_id,),
            ).fetchone()
            if tracking is None:
                raise LiveRecordIntegrityError("protection tracking disappeared")
            if int(tracking["remaining_quantity"]) == 0:
                self._mark_work_completed(
                    connection,
                    work_id,
                    moment,
                    "POSITION_CLOSED_CANDIDATE_NOT_ACTIVATED",
                )
                completed = connection.execute(
                    "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
                ).fetchone()
                if completed is None:  # pragma: no cover - 同一事务不变量
                    raise LiveRecordIntegrityError("completed work disappeared")
                return _row_to_work(completed)
            if work.kind is LiveWorkKind.BUILD_DEEP_PROTECTION and not bool(
                tracking["plan_ready"]
            ):
                raise LiveRecordStateError("QUICK_PLAN_NOT_READY")
            stream_id = (plan_stream_id or work.protection_id).strip()
            if not stream_id:
                raise ValueError("plan_stream_id must not be empty")
            connection.execute(
                """
                UPDATE live_protection_tracking
                SET plan_ready = 1, plan_stream_id = ?
                WHERE protection_id = ?
                """,
                (stream_id, work.protection_id),
            )
            audit = NewLiveRecordEvent(
                event_id="live-protection-ready-" + _digest_key(work.account_id, work.command_id),
                account_id=work.account_id,
                event_type=LiveRecordEventType.PROTECTION_READY,
                occurred_at=moment,
                idempotency_key=f"protect-ready:{work.command_id}",
                payload_json=_canonical_json(
                    {
                        "command_id": work.command_id,
                        "lease_generation": work.attempts,
                        "plan_id": plan_id,
                        "plan_stream_id": stream_id,
                        "protection_id": work.protection_id,
                        "work_id": work.work_id,
                    }
                ),
            )
            duplicate = self._event_by_key(connection, audit.account_id, audit.idempotency_key)
            if duplicate is None:
                self._append_in_transaction(connection, audit)
            self._mark_work_completed(connection, work_id, moment, result)
            completed = connection.execute(
                "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
            ).fetchone()
            if completed is None:
                raise LiveRecordIntegrityError("completed work disappeared")
            return _row_to_work(completed)

    def complete_quick_protection_work(
        self,
        work_id: str,
        *,
        lease_attempt: int,
        completed_at: datetime,
        plan_id: str,
        plan_stream_id: str,
    ) -> tuple[LiveWorkItem, LiveWorkItem]:
        """原子公开 QUICK、完成快速工作并排队独立的持久 DEEP 工作。"""

        moment = _aware_utc(completed_at)
        stream_id = plan_stream_id.strip()
        if not stream_id:
            raise ValueError("plan_stream_id must not be empty")
        with self._transaction() as connection:
            row = self._work_for_completion(
                connection,
                work_id,
                LiveWorkKind.BUILD_PROTECTION,
                lease_attempt,
            )
            work = _row_to_work(row)
            if work.protection_id is None:
                raise LiveRecordIntegrityError("protection work has no protection id")
            deep_work_id = _deep_protection_work_id(work.account_id, work.command_id)
            if work.status is not LiveWorkStatus.COMPLETED:
                connection.execute(
                    """
                    UPDATE live_protection_tracking
                    SET plan_ready = 1, plan_stream_id = ?
                    WHERE protection_id = ?
                    """,
                    (stream_id, work.protection_id),
                )
                quick_audit = NewLiveRecordEvent(
                    event_id="live-protection-quick-ready-"
                    + _digest_key(work.account_id, work.command_id),
                    account_id=work.account_id,
                    event_type=LiveRecordEventType.PROTECTION_QUICK_READY,
                    occurred_at=moment,
                    idempotency_key=f"protect-quick-ready:{work.command_id}",
                    payload_json=_canonical_json(
                        {
                            "command_id": work.command_id,
                            "plan_id": plan_id,
                            "plan_stream_id": stream_id,
                            "protection_id": work.protection_id,
                            "work_id": work.work_id,
                        }
                    ),
                )
                self._append_in_transaction(connection, quick_audit)
                existing_deep = connection.execute(
                    "SELECT * FROM live_work_items WHERE work_id = ?",
                    (deep_work_id,),
                ).fetchone()
                if existing_deep is None:
                    self._insert_work(
                        connection,
                        work_id=deep_work_id,
                        kind=LiveWorkKind.BUILD_DEEP_PROTECTION,
                        account_id=work.account_id,
                        command_id=work.command_id,
                        protection_id=work.protection_id,
                        payload=_json_object(work.payload_json),
                        available_at=moment,
                    )
                deep_audit = NewLiveRecordEvent(
                    event_id="live-protection-deep-queued-"
                    + _digest_key(work.account_id, work.command_id),
                    account_id=work.account_id,
                    event_type=LiveRecordEventType.PROTECTION_DEEP_WORK_QUEUED,
                    occurred_at=moment,
                    idempotency_key=f"protect-deep-queued:{work.command_id}",
                    payload_json=_canonical_json(
                        {
                            "command_id": work.command_id,
                            "protection_id": work.protection_id,
                            "quick_plan_id": plan_id,
                            "work_id": deep_work_id,
                        }
                    ),
                )
                self._append_in_transaction(connection, deep_audit)
                self._mark_work_completed(
                    connection,
                    work_id,
                    moment,
                    "QUICK_PLAN_READY_DEEP_QUEUED",
                )
            completed_row = connection.execute(
                "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
            ).fetchone()
            deep_row = connection.execute(
                "SELECT * FROM live_work_items WHERE work_id = ?", (deep_work_id,)
            ).fetchone()
            if completed_row is None or deep_row is None:
                raise LiveRecordIntegrityError("quick/deep work transition disappeared")
            return _row_to_work(completed_row), _row_to_work(deep_row)

    def renew_work_lease(
        self,
        work_id: str,
        *,
        lease_attempt: int,
        renewed_at: datetime,
        lease_for: timedelta = timedelta(minutes=5),
    ) -> LiveWorkItem:
        """按领取代际续租；过期后若已被接管，旧 worker 无法复活租约。"""

        moment = _aware_utc(renewed_at)
        lease_for = normalize_lease_for(lease_for)
        attempt = _lease_attempt(lease_attempt)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
            ).fetchone()
            if row is None:
                raise LiveRecordStateError("WORK_NOT_FOUND")
            work = _row_to_work(row)
            if work.attempts != attempt:
                raise LiveRecordStateError("WORK_LEASE_FENCED")
            if work.status is not LiveWorkStatus.RUNNING:
                raise LiveRecordStateError("WORK_NOT_CLAIMED")
            connection.execute(
                """
                UPDATE live_work_items
                SET lease_until = ?, updated_at = ?
                WHERE work_id = ? AND status = 'RUNNING' AND attempts = ?
                """,
                (_time(moment + lease_for), _time(moment), work_id, attempt),
            )
            renewed = connection.execute(
                "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
            ).fetchone()
            if renewed is None:
                raise LiveRecordIntegrityError("renewed work disappeared")
            return _row_to_work(renewed)

    def mark_quick_protection_ready(
        self,
        work_id: str,
        *,
        lease_attempt: int,
        ready_at: datetime,
        plan_id: str,
    ) -> LiveWorkItem:
        """在慢速 DEEP 分析前，原子公开可跟踪的 QUICK 保护计划。"""

        moment = _aware_utc(ready_at)
        with self._transaction() as connection:
            row = self._work_for_completion(
                connection,
                work_id,
                LiveWorkKind.BUILD_PROTECTION,
                lease_attempt,
            )
            work = _row_to_work(row)
            if work.protection_id is None:
                raise LiveRecordIntegrityError("protection work has no protection id")
            connection.execute(
                """
                UPDATE live_protection_tracking
                SET plan_ready = 1,
                    plan_stream_id = COALESCE(plan_stream_id, ?)
                WHERE protection_id = ?
                """,
                (work.protection_id, work.protection_id),
            )
            audit = NewLiveRecordEvent(
                event_id="live-protection-quick-ready-"
                + _digest_key(work.account_id, work.command_id),
                account_id=work.account_id,
                event_type=LiveRecordEventType.PROTECTION_QUICK_READY,
                occurred_at=moment,
                idempotency_key=f"protect-quick-ready:{work.command_id}",
                payload_json=_canonical_json(
                    {
                        "command_id": work.command_id,
                        "plan_id": plan_id,
                        "protection_id": work.protection_id,
                        "work_id": work.work_id,
                    }
                ),
            )
            duplicate = self._event_by_key(connection, audit.account_id, audit.idempotency_key)
            if duplicate is None:
                self._append_in_transaction(connection, audit)
            elif (
                str(duplicate["event_type"]) != audit.event_type.value
                or str(duplicate["payload_json"]) != audit.payload_json
            ):
                raise LiveRecordConflictError(
                    "idempotency key is bound to different live-record content"
                )
            return work

    def complete_work(
        self,
        work_id: str,
        *,
        lease_attempt: int,
        completed_at: datetime,
        result_code: str,
    ) -> LiveWorkItem:
        """完成非保护构建工作；重复确认返回原完成结果。"""

        moment = _aware_utc(completed_at)
        result = _error_code(result_code)
        with self._transaction() as connection:
            row = self._work_for_completion(connection, work_id, None, lease_attempt)
            work = _row_to_work(row)
            if work.status is not LiveWorkStatus.COMPLETED:
                self._mark_work_completed(connection, work_id, moment, result)
                row = connection.execute(
                    "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
                ).fetchone()
                if row is None:
                    raise LiveRecordIntegrityError("completed work disappeared")
            return _row_to_work(row)

    def fail_work(
        self,
        work_id: str,
        *,
        lease_attempt: int,
        failed_at: datetime,
        error_code: str,
        retryable: bool,
        retry_after: timedelta = timedelta(minutes=1),
        maximum_attempts: int = 5,
    ) -> LiveWorkItem:
        """释放失败任务；达到上限后进入 DEAD 并留下不可变审计事件。"""

        policy = normalize_work_failure(
            failed_at=failed_at,
            error_code=error_code,
            retry_after=retry_after,
            maximum_attempts=maximum_attempts,
            lease_attempt=lease_attempt,
        )
        moment = policy.moment
        code = policy.error_code
        attempt = policy.lease_attempt
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
            ).fetchone()
            if row is None:
                raise LiveRecordStateError("WORK_NOT_FOUND")
            work = _row_to_work(row)
            if work.attempts != attempt:
                raise LiveRecordStateError("WORK_LEASE_FENCED")
            if work.status in {LiveWorkStatus.COMPLETED, LiveWorkStatus.DEAD}:
                return work
            if work.status is not LiveWorkStatus.RUNNING:
                raise LiveRecordStateError("WORK_NOT_CLAIMED")
            projection = project_work_failure(
                attempts=work.attempts,
                retryable=retryable,
                policy=policy,
            )
            connection.execute(
                """
                UPDATE live_work_items SET
                    status = ?, available_at = ?, lease_until = NULL,
                    updated_at = ?, error_code = ?, result_code = NULL
                WHERE work_id = ?
                """,
                (
                    projection.status.value,
                    _time(projection.available_at),
                    _time(moment),
                    code,
                    work_id,
                ),
            )
            if projection.dead and work.kind in {
                LiveWorkKind.BUILD_PROTECTION,
                LiveWorkKind.BUILD_DEEP_PROTECTION,
            }:
                audit = NewLiveRecordEvent(
                    event_id="live-protection-failed-"
                    + _digest_key(work.account_id, work.command_id, str(work.attempts)),
                    account_id=work.account_id,
                    event_type=LiveRecordEventType.PROTECTION_FAILED,
                    occurred_at=moment,
                    idempotency_key=f"protect-failed:{work.command_id}:{work.attempts}",
                    payload_json=_canonical_json(
                        {
                            "attempts": work.attempts,
                            "command_id": work.command_id,
                            "error_code": code,
                            "protection_id": work.protection_id,
                            "work_id": work.work_id,
                        }
                    ),
                )
                self._append_in_transaction(connection, audit)
            updated = connection.execute(
                "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
            ).fetchone()
            if updated is None:
                raise LiveRecordIntegrityError("failed work disappeared")
            return _row_to_work(updated)

    def queue_exit_alert(
        self,
        *,
        protection_id: str,
        command_id: str,
        plan_stream_id: str,
        bar_end: datetime,
        observed_at: datetime,
        payload: Mapping[str, object],
    ) -> tuple[LiveWorkItem, bool]:
        """同一保护流同一根 K 线只创建一条可投递 NapCat 提醒。"""

        end = _aware_utc(bar_end)
        moment = _aware_utc(observed_at)
        stream_id = plan_stream_id.strip()
        if not stream_id:
            raise ValueError("plan_stream_id must not be empty")
        stamp = end.strftime("%Y%m%dT%H%M%S%fZ")
        # DEEP 切换前后的物理计划流不能共享提醒幂等键，否则旧计划同一根 bar
        # 的工作项可能遮蔽新计划产生的提醒。
        work_id = "live-work-alert-" + _digest_key(protection_id, stream_id, stamp)
        with self._transaction() as connection:
            tracking = connection.execute(
                "SELECT * FROM live_protection_tracking WHERE protection_id = ?",
                (protection_id,),
            ).fetchone()
            if tracking is None:
                raise LiveRecordStateError("PROTECTION_NOT_FOUND")
            if str(tracking["buy_command_id"]) != command_id:
                raise LiveRecordStateError("PROTECTION_COMMAND_MISMATCH")
            if int(tracking["remaining_quantity"]) == 0:
                raise LiveRecordStateError("PROTECTION_POSITION_CLOSED")
            if not bool(tracking["plan_ready"]):
                raise LiveRecordStateError("PROTECTION_PLAN_NOT_READY")
            if str(tracking["plan_stream_id"]) != stream_id:
                raise LiveRecordStateError("PROTECTION_PLAN_CHANGED")
            existing = connection.execute(
                "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
            ).fetchone()
            if existing is not None:
                return _row_to_work(existing), False
            self._insert_work(
                connection,
                work_id=work_id,
                kind=LiveWorkKind.DELIVER_EXIT_ALERT,
                account_id=str(tracking["account_id"]),
                command_id=command_id,
                protection_id=protection_id,
                payload=dict(payload),
                available_at=moment,
            )
            connection.execute(
                """
                UPDATE live_protection_tracking
                SET last_observed_bar_end = ?, last_alert_bar_end = ?
                WHERE protection_id = ?
                """,
                (_time(end), _time(end), protection_id),
            )
            alert_identity = _digest_key(protection_id, stream_id, stamp)
            audit = NewLiveRecordEvent(
                event_id="live-exit-alert-" + alert_identity,
                account_id=str(tracking["account_id"]),
                event_type=LiveRecordEventType.EXIT_ALERT_QUEUED,
                occurred_at=moment,
                idempotency_key=f"exit-alert:{alert_identity}",
                payload_json=_canonical_json(
                    {
                        "bar_end": end,
                        "command_id": command_id,
                        "plan_stream_id": stream_id,
                        "payload_sha256": hashlib.sha256(
                            _canonical_json(payload).encode("utf-8")
                        ).hexdigest(),
                        "protection_id": protection_id,
                        "work_id": work_id,
                    }
                ),
            )
            self._append_in_transaction(connection, audit)
            created = connection.execute(
                "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
            ).fetchone()
            if created is None:
                raise LiveRecordIntegrityError("exit-alert work disappeared")
            return _row_to_work(created), True

    def record_observed_bar(self, protection_id: str, *, bar_end: datetime) -> None:
        """没有触发 barrier 时也单调记录最后检查到的完整 K 线。"""

        end = _aware_utc(bar_end)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT last_observed_bar_end FROM live_protection_tracking "
                "WHERE protection_id = ?",
                (protection_id,),
            ).fetchone()
            if row is None:
                raise LiveRecordStateError("PROTECTION_NOT_FOUND")
            previous = (
                None
                if row["last_observed_bar_end"] is None
                else _parse_time(str(row["last_observed_bar_end"]))
            )
            if previous is None or end > previous:
                connection.execute(
                    "UPDATE live_protection_tracking SET last_observed_bar_end = ? "
                    "WHERE protection_id = ?",
                    (_time(end), protection_id),
                )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLiteLiveRecordStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _validate_confirmation(
        self,
        command: StoredLiveCommand,
        fill: ConfirmedLiveFill,
        sender_id: str,
        fingerprint: str,
    ) -> None:
        if command.account_id != fill.account_id or command.fill_json != fill.canonical_json():
            raise LiveRecordIntegrityError("command index contains a different fill")
        if command.sender_id != sender_id:
            raise LiveRecordStateError("CONFIRMING_SENDER_MISMATCH")
        if command.fingerprint != fingerprint:
            raise LiveRecordStateError("CONFIRMATION_FINGERPRINT_MISMATCH")

    def _allocate_sell_lots(
        self,
        connection: sqlite3.Connection,
        fill: ConfirmedLiveFill,
        allocated_at: datetime,
    ) -> tuple[tuple[str, str, int], ...]:
        rows = connection.execute(
            """
            SELECT * FROM live_protection_tracking
            WHERE account_id = ? AND symbol = ? AND remaining_quantity > 0
            ORDER BY acquired_at, buy_command_id
            """,
            (fill.account_id, fill.symbol),
        ).fetchall()
        lots = tuple(
            SellLot(
                protection_id=str(row["protection_id"]),
                buy_command_id=str(row["buy_command_id"]),
                remaining_quantity=int(row["remaining_quantity"]),
            )
            for row in rows
        )
        try:
            allocations = allocate_sell_lots(fill.quantity, lots)
        except SellLotAllocationError as error:
            raise LiveRecordIntegrityError(
                "position projection and buy lots disagree"
            ) from error
        closed: list[tuple[str, str, int]] = []
        for allocation in allocations:
            connection.execute(
                """
                INSERT INTO live_sell_allocations (
                    sell_command_id, buy_command_id, quantity, allocated_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    fill.command_id,
                    allocation.buy_command_id,
                    allocation.quantity,
                    _time(allocated_at),
                ),
            )
            connection.execute(
                """
                UPDATE live_protection_tracking
                SET remaining_quantity = remaining_quantity - ?
                WHERE protection_id = ? AND remaining_quantity >= ?
                """,
                (
                    allocation.quantity,
                    allocation.protection_id,
                    allocation.quantity,
                ),
            )
            if allocation.closes_lot:
                connection.execute(
                    """
                    UPDATE live_work_items SET
                        status = 'COMPLETED', lease_until = NULL, updated_at = ?,
                        result_code = 'POSITION_CLOSED_WORK_FENCED', error_code = NULL
                    WHERE protection_id = ?
                      AND kind IN ('BUILD_PROTECTION','BUILD_DEEP_PROTECTION')
                      AND status IN ('PENDING','RETRY','RUNNING')
                    """,
                    (_time(allocated_at), allocation.protection_id),
                )
                closed.append(
                    (
                        allocation.protection_id,
                        allocation.buy_command_id,
                        allocation.quantity,
                    )
                )
        return tuple(closed)

    def _insert_work(
        self,
        connection: sqlite3.Connection,
        *,
        work_id: str,
        kind: LiveWorkKind,
        account_id: str,
        command_id: str,
        protection_id: str | None,
        payload: Mapping[str, object],
        available_at: datetime,
    ) -> None:
        stamp = _time(available_at)
        connection.execute(
            """
            INSERT INTO live_work_items (
                work_id, kind, account_id, command_id, protection_id,
                payload_json, status, attempts, available_at, lease_until,
                created_at, updated_at, result_code, error_code
            ) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, NULL, ?, ?, NULL, NULL)
            """,
            (
                work_id,
                kind.value,
                account_id,
                command_id,
                protection_id,
                _canonical_json(payload),
                stamp,
                stamp,
                stamp,
            ),
        )

    def _work_for_completion(
        self,
        connection: sqlite3.Connection,
        work_id: str,
        expected_kind: LiveWorkKind | None,
        lease_attempt: int,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM live_work_items WHERE work_id = ?", (work_id,)
        ).fetchone()
        if row is None:
            raise LiveRecordStateError("WORK_NOT_FOUND")
        work = _row_to_work(row)
        if work.attempts != _lease_attempt(lease_attempt):
            raise LiveRecordStateError("WORK_LEASE_FENCED")
        if expected_kind is not None and work.kind is not expected_kind:
            raise LiveRecordStateError("WORK_KIND_MISMATCH")
        if work.status not in {LiveWorkStatus.RUNNING, LiveWorkStatus.COMPLETED}:
            raise LiveRecordStateError("WORK_NOT_CLAIMED")
        return cast(sqlite3.Row, row)

    def _mark_work_completed(
        self,
        connection: sqlite3.Connection,
        work_id: str,
        completed_at: datetime,
        result_code: str,
    ) -> None:
        connection.execute(
            """
            UPDATE live_work_items SET
                status = 'COMPLETED', lease_until = NULL, updated_at = ?,
                result_code = ?, error_code = NULL
            WHERE work_id = ?
            """,
            (_time(completed_at), result_code, work_id),
        )

    def _append_in_transaction(
        self,
        connection: sqlite3.Connection,
        event: NewLiveRecordEvent,
    ) -> LiveRecordEvent:
        account_rows = connection.execute(
            "SELECT * FROM live_record_events WHERE account_id = ? ORDER BY sequence",
            (event.account_id,),
        ).fetchall()
        _verify(tuple(_row_to_event(row) for row in account_rows))
        duplicate = self._event_by_key(connection, event.account_id, event.idempotency_key)
        if duplicate is not None:
            stored = _row_to_event(duplicate)
            if (
                stored.event_type is not event.event_type
                or stored.payload_json != event.payload_json
            ):
                raise LiveRecordConflictError(
                    "idempotency key is bound to different live-record content"
                )
            return stored
        digest = hashlib.sha256(event.payload_json.encode("utf-8")).hexdigest()
        previous = connection.execute(
            """
            SELECT event_hash FROM live_record_events
            WHERE account_id = ? ORDER BY sequence DESC LIMIT 1
            """,
            (event.account_id,),
        ).fetchone()
        previous_hash = None if previous is None else str(previous["event_hash"])
        event_hash = _event_hash(event, digest, previous_hash)
        try:
            cursor = connection.execute(
                """
                INSERT INTO live_record_events (
                    event_id, account_id, event_type, occurred_at,
                    idempotency_key, payload_json, payload_sha256,
                    previous_hash, event_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.account_id,
                    event.event_type.value,
                    event.occurred_at.isoformat(),
                    event.idempotency_key,
                    event.payload_json,
                    digest,
                    previous_hash,
                    event_hash,
                ),
            )
        except sqlite3.IntegrityError as error:
            raise LiveRecordConflictError("live-record event identity is already used") from error
        sequence = cursor.lastrowid
        if sequence is None:
            raise LiveRecordIntegrityError("inserted live-record event has no sequence")
        row = connection.execute(
            "SELECT * FROM live_record_events WHERE sequence = ?", (sequence,)
        ).fetchone()
        if row is None:
            raise LiveRecordIntegrityError("inserted live-record event disappeared")
        return _row_to_event(row)

    @staticmethod
    def _event_by_key(
        connection: sqlite3.Connection,
        account_id: str,
        idempotency_key: str,
    ) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            connection.execute(
                """
            SELECT * FROM live_record_events
            WHERE account_id = ? AND idempotency_key = ?
            """,
                (account_id, idempotency_key),
            ).fetchone(),
        )

    def _rebuild_materialized_projection(self, connection: sqlite3.Connection) -> None:
        """旧版数据库只含事件时重放；遇到歧义立即失败，不猜测成交事实。"""

        rows = connection.execute("SELECT * FROM live_record_events ORDER BY sequence").fetchall()
        proposals: set[str] = set()
        for row in rows:
            event = _row_to_event(row)
            document = _json_object(event.payload_json)
            if event.event_type is LiveRecordEventType.COMMAND_PROPOSED:
                fill_document = _mapping(document, "fill")
                command_id = _text(fill_document, "command_id")
                if command_id in proposals:
                    raise LiveRecordIntegrityError("legacy ledger repeats a command id")
                proposals.add(command_id)
                connection.execute(
                    """
                    INSERT INTO live_commands (
                        command_id, account_id, sender_id, source_message_id,
                        fingerprint, fill_json, state, proposal_event_id,
                        terminal_event_id, proposed_at, terminal_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, NULL, ?, NULL)
                    """,
                    (
                        command_id,
                        event.account_id,
                        _text(document, "sender_id"),
                        _text(document, "source_message_id"),
                        _text(document, "fingerprint"),
                        _canonical_json(fill_document),
                        event.event_id,
                        _time(event.occurred_at),
                    ),
                )
            elif event.event_type in {
                LiveRecordEventType.FILL_CONFIRMED,
                LiveRecordEventType.COMMAND_CANCELLED,
            }:
                # 自动猜测旧成交的批次分配会破坏真实性，因此明确要求离线迁移。
                raise LiveRecordIntegrityError(
                    "legacy confirmed live ledger requires explicit projection migration"
                )

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._ensure_open()
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("live-record store is closed")

__all__ = [
    "LiveConfirmationCommit",
    "LiveRecordConflictError",
    "LiveRecordIntegrityError",
    "LiveRecordStateError",
    "LiveRecordStoreError",
    "SQLiteLiveRecordStore",
    "StoredLiveCommand",
]
