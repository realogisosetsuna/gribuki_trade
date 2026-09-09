"""按日期隔离的 PAPER 账本跨会话连续性测试。"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import (
    ASharePaperFill,
    PaperFillSource,
    PaperInstrumentType,
)
from gribuki_trade.runtime.paper_account_chain import (
    PaperAccountChainError,
    prepare_paper_day_ledger,
)
from gribuki_trade.services.ashare.paper_day.ashare_paper import ASharePaperTradingService
from gribuki_trade.storage.paper.paper_ledger import (
    PaperLedgerConflictError,
    SQLitePaperLedger,
)

_ACCOUNT = "continuity-account"


def _open_prepared_account(root: Path, session_date: date) -> None:
    prepared = prepare_paper_day_ledger(
        root,
        session_date=session_date,
        account_id=_ACCOUNT,
    )
    with SQLitePaperLedger(prepared.ledger_path) as ledger:
        ASharePaperTradingService(ledger).open_account(
            _ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=session_date,
            opened_at=datetime.combine(session_date, datetime.min.time(), UTC),
        )


def _rollover(root: Path, session_date: date) -> None:
    with SQLitePaperLedger(root / "ledger.sqlite3") as ledger:
        ASharePaperTradingService(ledger).rollover_session(
            _ACCOUNT,
            target_session_date=session_date,
            occurred_at=datetime.combine(session_date, datetime.min.time(), UTC),
        )


def _subprocess_environment() -> dict[str, str]:
    environment = dict(os.environ)
    source_root = str(Path.cwd() / "src")
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        source_root if not existing else source_root + os.pathsep + existing
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return environment


def test_new_session_clones_verified_ledger_and_rolls_t1_forward(tmp_path) -> None:
    first_date = date(2026, 8, 14)
    second_date = date(2026, 8, 17)
    first_root = tmp_path / first_date.isoformat()
    first_root.mkdir()
    first_path = first_root / "ledger.sqlite3"
    with SQLitePaperLedger(first_path) as ledger:
        service = ASharePaperTradingService(ledger)
        service.open_account(
            _ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=first_date,
            opened_at=datetime(2026, 8, 14, 1, 0, tzinfo=UTC),
        )
        service.record_fill(
            ASharePaperFill(
                account_id=_ACCOUNT,
                fill_id="first-buy",
                symbol="600000.SH",
                side=Side.BUY,
                quantity=100,
                price=Decimal("10"),
                instrument_type=PaperInstrumentType.STOCK,
                trading_date=first_date,
                executed_at=datetime(2026, 8, 14, 2, 0, tzinfo=UTC),
                source=PaperFillSource.SIMULATED,
            ),
            recorded_at=datetime(2026, 8, 14, 2, 0, tzinfo=UTC),
        )
        prior_events = ledger.events(_ACCOUNT)

    second_root = tmp_path / second_date.isoformat()
    prepared = prepare_paper_day_ledger(
        second_root,
        session_date=second_date,
        account_id=_ACCOUNT,
    )

    assert prepared.origin == "VERIFIED_PRIOR_SESSION_CLONE"
    assert prepared.source_session_date == first_date
    assert prepared.source_event_count == len(prior_events)
    assert prepared.source_last_event_hash == prior_events[-1].event_hash
    assert (second_root / "ledger-lineage.json").is_file()

    with SQLitePaperLedger(prepared.ledger_path) as ledger:
        service = ASharePaperTradingService(ledger)
        before = service.snapshot(_ACCOUNT)
        assert before.session_date == first_date
        assert before.position("600000.SH").today_buy == 100  # type: ignore[union-attr]
        after = service.rollover_session(
            _ACCOUNT,
            target_session_date=second_date,
            occurred_at=datetime(2026, 8, 17, 1, 0, tzinfo=UTC),
        )
        assert after.cash == before.cash
        assert after.position("600000.SH").available_to_sell == 100  # type: ignore[union-attr]
        assert after.position("600000.SH").today_buy == 0  # type: ignore[union-attr]


def test_preparation_is_restart_stable(tmp_path) -> None:
    session_date = date(2026, 8, 14)
    session_root = tmp_path / session_date.isoformat()
    first = prepare_paper_day_ledger(
        session_root,
        session_date=session_date,
        account_id=_ACCOUNT,
    )
    with SQLitePaperLedger(first.ledger_path) as ledger:
        ASharePaperTradingService(ledger).open_account(
            _ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=session_date,
            opened_at=datetime(2026, 8, 14, 1, 0, tzinfo=UTC),
        )

    replay = prepare_paper_day_ledger(
        session_root,
        session_date=session_date,
        account_id=_ACCOUNT,
    )

    assert replay.audit_document() == first.audit_document()


def test_divergent_historical_ledgers_fail_closed(tmp_path) -> None:
    for session_date, initial_cash in (
        (date(2026, 8, 13), Decimal("100000")),
        (date(2026, 8, 14), Decimal("200000")),
    ):
        root = tmp_path / session_date.isoformat()
        root.mkdir()
        with SQLitePaperLedger(root / "ledger.sqlite3") as ledger:
            ASharePaperTradingService(ledger).open_account(
                _ACCOUNT,
                initial_cash=initial_cash,
                session_date=session_date,
                opened_at=datetime.combine(session_date, datetime.min.time(), UTC),
            )

    with pytest.raises(PaperAccountChainError, match="divergent"):
        prepare_paper_day_ledger(
            tmp_path / "2026-08-17",
            session_date=date(2026, 8, 17),
            account_id=_ACCOUNT,
        )


def test_lineage_is_bound_to_account(tmp_path) -> None:
    session_date = date(2026, 8, 14)
    root = tmp_path / session_date.isoformat()
    prepared = prepare_paper_day_ledger(
        root,
        session_date=session_date,
        account_id=_ACCOUNT,
    )
    with SQLitePaperLedger(prepared.ledger_path):
        pass

    with pytest.raises(PaperAccountChainError, match="account"):
        prepare_paper_day_ledger(
            root,
            session_date=session_date,
            account_id="different-account",
        )


def test_existing_lineage_is_rebound_to_actual_ledger_prefix(tmp_path) -> None:
    first_date = date(2026, 8, 14)
    first_root = tmp_path / first_date.isoformat()
    first_root.mkdir()
    with SQLitePaperLedger(first_root / "ledger.sqlite3") as ledger:
        ASharePaperTradingService(ledger).open_account(
            _ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=first_date,
            opened_at=datetime(2026, 8, 14, 1, 0, tzinfo=UTC),
        )

    second_date = date(2026, 8, 17)
    second_root = tmp_path / second_date.isoformat()
    prepare_paper_day_ledger(
        second_root,
        session_date=second_date,
        account_id=_ACCOUNT,
    )
    lineage_path = second_root / "ledger-lineage.json"
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
    lineage["source_last_event_hash"] = "0" * 64
    lineage_path.write_text(
        json.dumps(lineage, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    with pytest.raises(PaperAccountChainError, match="event prefix"):
        prepare_paper_day_ledger(
            second_root,
            session_date=second_date,
            account_id=_ACCOUNT,
        )


def test_concurrent_first_session_preparation_is_serialized_and_stable(tmp_path) -> None:
    session_date = date(2026, 8, 14)
    root = tmp_path / session_date.isoformat()

    def prepare() -> dict[str, object]:
        return prepare_paper_day_ledger(
            root,
            session_date=session_date,
            account_id=_ACCOUNT,
        ).audit_document()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda _index: prepare(), range(2)))

    assert results[0] == results[1]
    assert (root / "ledger.sqlite3").is_file()
    assert (root / "ledger-lineage.json").is_file()


def test_two_real_processes_serialize_the_same_preparation(tmp_path: Path) -> None:
    session_date = date(2026, 8, 14)
    root = tmp_path / session_date.isoformat()
    holder_entered = tmp_path / "holder-entered"
    follower_entered = tmp_path / "follower-entered"
    release_holder = tmp_path / "release-holder"
    script = textwrap.dedent(
        """
        import json
        import sys
        import time
        from datetime import date
        from pathlib import Path

        import gribuki_trade.runtime.paper_account_chain as chain

        root = Path(sys.argv[1])
        role = sys.argv[4]
        entered = Path(sys.argv[5])
        release = Path(sys.argv[6])
        original = chain._prepare_locked
        def observed_prepare(*args, **kwargs):
            entered.touch()
            if role == "holder":
                while not release.exists():
                    time.sleep(0.005)
            return original(*args, **kwargs)
        chain._prepare_locked = observed_prepare
        prepared = chain.prepare_paper_day_ledger(
            root,
            session_date=date.fromisoformat(sys.argv[2]),
            account_id=sys.argv[3],
        )
        print(json.dumps(prepared.audit_document(), sort_keys=True))
        """
    )
    common_arguments = (
        sys.executable,
        "-c",
        script,
        str(root),
        session_date.isoformat(),
        _ACCOUNT,
    )
    holder = subprocess.Popen(
        (*common_arguments, "holder", str(holder_entered), str(release_holder)),
        cwd=Path.cwd(),
        env=_subprocess_environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 10
    while not holder_entered.exists() and time.monotonic() < deadline:
        if holder.poll() is not None:
            break
        time.sleep(0.005)
    if not holder_entered.exists():
        release_holder.touch()
        _stdout, stderr = holder.communicate(timeout=5)
        pytest.fail(f"holder did not enter preparation: {stderr}")

    follower = subprocess.Popen(
        (*common_arguments, "follower", str(follower_entered), str(release_holder)),
        cwd=Path.cwd(),
        env=_subprocess_environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    # holder 已进入仅在外层锁内调用的 _prepare_locked；follower 必须还进不来。
    try:
        with suppress(subprocess.TimeoutExpired):
            follower.wait(timeout=0.25)
        assert not follower_entered.exists()
    finally:
        release_holder.touch()
    processes = (holder, follower)
    results = tuple(process.communicate(timeout=30) for process in processes)

    assert [process.returncode for process in processes] == [0, 0]
    assert [stderr for _stdout, stderr in results] == ["", ""]
    documents = tuple(json.loads(stdout) for stdout, _stderr in results)
    assert documents[0] == documents[1]
    assert json.loads((root / "ledger-lineage.json").read_text(encoding="utf-8")) == documents[0]


def test_real_process_crash_after_clone_recovers_without_deleting_orphan(
    tmp_path: Path,
) -> None:
    first_date = date(2026, 8, 14)
    second_date = date(2026, 8, 17)
    first_root = tmp_path / first_date.isoformat()
    second_root = tmp_path / second_date.isoformat()
    _open_prepared_account(first_root, first_date)
    script = textwrap.dedent(
        """
        import os
        import sys
        from datetime import date
        from pathlib import Path

        import gribuki_trade.runtime.paper_account_chain as chain

        original = chain._atomic_sqlite_backup
        def clone_then_crash(source, destination):
            original(source, destination)
            os._exit(73)
        chain._atomic_sqlite_backup = clone_then_crash
        chain.prepare_paper_day_ledger(
            Path(sys.argv[1]),
            session_date=date.fromisoformat(sys.argv[2]),
            account_id=sys.argv[3],
        )
        """
    )
    crashed = subprocess.run(
        (
            sys.executable,
            "-c",
            script,
            str(second_root),
            second_date.isoformat(),
            _ACCOUNT,
        ),
        cwd=Path.cwd(),
        env=_subprocess_environment(),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert crashed.returncode == 73
    assert (second_root / "ledger.sqlite3").is_file()
    assert not (second_root / "ledger-lineage.json").exists()

    recovered = prepare_paper_day_ledger(
        second_root,
        session_date=second_date,
        account_id=_ACCOUNT,
    )
    assert recovered.origin == "RECOVERED_ORPHAN_CLONE"
    assert recovered.source_session_date == first_date
    assert (second_root / "ledger-lineage.json").is_file()
    with (
        SQLitePaperLedger(recovered.ledger_path) as recovered_ledger,
        SQLitePaperLedger(first_root / "ledger.sqlite3") as source_ledger,
    ):
        assert recovered_ledger.events(_ACCOUNT) == source_ledger.events(_ACCOUNT)


def test_deleted_current_lineage_recovers_exact_source_binding(tmp_path: Path) -> None:
    first_date = date(2026, 8, 14)
    second_date = date(2026, 8, 17)
    first_root = tmp_path / first_date.isoformat()
    second_root = tmp_path / second_date.isoformat()
    _open_prepared_account(first_root, first_date)
    original = prepare_paper_day_ledger(
        second_root,
        session_date=second_date,
        account_id=_ACCOUNT,
    )
    lineage_path = second_root / "ledger-lineage.json"
    lineage_path.unlink()

    recovered = prepare_paper_day_ledger(
        second_root,
        session_date=second_date,
        account_id=_ACCOUNT,
    )

    assert recovered.audit_document() == original.audit_document()
    assert json.loads(lineage_path.read_text(encoding="utf-8")) == original.audit_document()


def test_current_lineage_tamper_cannot_erase_embedded_source_binding(
    tmp_path: Path,
) -> None:
    first_date = date(2026, 8, 14)
    second_date = date(2026, 8, 17)
    first_root = tmp_path / first_date.isoformat()
    second_root = tmp_path / second_date.isoformat()
    _open_prepared_account(first_root, first_date)
    original = prepare_paper_day_ledger(
        second_root,
        session_date=second_date,
        account_id=_ACCOUNT,
    )
    lineage_path = second_root / "ledger-lineage.json"
    document = json.loads(lineage_path.read_text(encoding="utf-8"))
    document.update(
        {
            "origin": "LEGACY_SESSION_LOCAL",
            "source_event_count": 0,
            "source_last_event_hash": None,
            "source_session_date": None,
        }
    )
    lineage_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(PaperAccountChainError, match="immutable ledger binding"):
        prepare_paper_day_ledger(
            second_root,
            session_date=second_date,
            account_id=_ACCOUNT,
        )

    # 删除不可信 sidecar 后，账本内不可变 binding 仍能恢复原始来源。
    lineage_path.unlink()
    recovered = prepare_paper_day_ledger(
        second_root,
        session_date=second_date,
        account_id=_ACCOUNT,
    )
    assert recovered.audit_document() == original.audit_document()


def test_historical_lineage_deletion_is_restored_before_it_can_be_a_source(
    tmp_path: Path,
) -> None:
    first_date = date(2026, 8, 14)
    second_date = date(2026, 8, 17)
    third_date = date(2026, 8, 18)
    first_root = tmp_path / first_date.isoformat()
    second_root = tmp_path / second_date.isoformat()
    _open_prepared_account(first_root, first_date)
    second = prepare_paper_day_ledger(
        second_root,
        session_date=second_date,
        account_id=_ACCOUNT,
    )
    _rollover(second_root, second_date)
    (second_root / "ledger-lineage.json").unlink()

    third = prepare_paper_day_ledger(
        tmp_path / third_date.isoformat(),
        session_date=third_date,
        account_id=_ACCOUNT,
    )

    assert third.source_session_date == second_date
    assert json.loads(
        (second_root / "ledger-lineage.json").read_text(encoding="utf-8")
    ) == second.audit_document()


def test_historical_lineage_corruption_and_contradiction_fail_closed(
    tmp_path: Path,
) -> None:
    first_date = date(2026, 8, 14)
    second_date = date(2026, 8, 17)
    third_date = date(2026, 8, 18)
    first_root = tmp_path / first_date.isoformat()
    second_root = tmp_path / second_date.isoformat()
    _open_prepared_account(first_root, first_date)
    prepare_paper_day_ledger(
        second_root,
        session_date=second_date,
        account_id=_ACCOUNT,
    )
    _rollover(second_root, second_date)
    lineage_path = second_root / "ledger-lineage.json"
    original = lineage_path.read_text(encoding="utf-8")
    lineage_path.write_text("{", encoding="utf-8")
    with pytest.raises(PaperAccountChainError, match="unreadable"):
        prepare_paper_day_ledger(
            tmp_path / third_date.isoformat(),
            session_date=third_date,
            account_id=_ACCOUNT,
        )

    lineage_path.write_text(original, encoding="utf-8")
    document = json.loads(original)
    document["source_last_event_hash"] = "0" * 64
    lineage_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(PaperAccountChainError, match="immutable ledger binding"):
        prepare_paper_day_ledger(
            tmp_path / third_date.isoformat(),
            session_date=third_date,
            account_id=_ACCOUNT,
        )


def test_historical_seal_detects_event_count_or_tail_change(tmp_path: Path) -> None:
    first_date = date(2026, 8, 14)
    second_date = date(2026, 8, 17)
    third_date = date(2026, 8, 18)
    first_root = tmp_path / first_date.isoformat()
    _open_prepared_account(first_root, first_date)
    prepare_paper_day_ledger(
        tmp_path / second_date.isoformat(),
        session_date=second_date,
        account_id=_ACCOUNT,
    )
    # day-1 已在成为 clone 来源时封存；普通 writer 会在数据库触发器处立即失败。
    with (
        SQLitePaperLedger(first_root / "ledger.sqlite3") as ledger,
        pytest.raises(PaperLedgerConflictError),
    ):
        ASharePaperTradingService(ledger).rollover_session(
            _ACCOUNT,
            target_session_date=second_date,
            occurred_at=datetime.combine(second_date, datetime.min.time(), UTC),
        )

    # 即便外部工具破坏 append-only trigger 后写入一条哈希合法的事件，下一次
    # 历史扫描仍会用 seal 的精确事件数/尾哈希检出，而不是静默提升为来源。
    connection = sqlite3.connect(first_root / "ledger.sqlite3")
    try:
        connection.execute("DROP TRIGGER paper_account_historical_seal_no_insert")
        connection.commit()
    finally:
        connection.close()
    with SQLitePaperLedger(first_root / "ledger.sqlite3") as ledger:
        ASharePaperTradingService(ledger).rollover_session(
            _ACCOUNT,
            target_session_date=second_date,
            occurred_at=datetime.combine(second_date, datetime.min.time(), UTC),
        )

    with pytest.raises(PaperAccountChainError, match="immutable seal"):
        prepare_paper_day_ledger(
            tmp_path / third_date.isoformat(),
            session_date=third_date,
            account_id=_ACCOUNT,
        )


def test_first_genuine_legacy_day_gets_explicit_migration_lineage(tmp_path: Path) -> None:
    first_date = date(2026, 8, 14)
    first_root = tmp_path / first_date.isoformat()
    first_root.mkdir()
    with SQLitePaperLedger(first_root / "ledger.sqlite3") as ledger:
        ASharePaperTradingService(ledger).open_account(
            _ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=first_date,
            opened_at=datetime.combine(first_date, datetime.min.time(), UTC),
        )
    assert not (first_root / "ledger-lineage.json").exists()

    second = prepare_paper_day_ledger(
        tmp_path / "2026-08-17",
        session_date=date(2026, 8, 17),
        account_id=_ACCOUNT,
    )
    migrated = json.loads(
        (first_root / "ledger-lineage.json").read_text(encoding="utf-8")
    )

    assert migrated["origin"] == "LEGACY_SESSION_LOCAL"
    assert migrated["source_event_count"] == 1
    assert second.origin == "VERIFIED_PRIOR_SESSION_CLONE"


def test_unbound_legacy_lineage_must_bind_the_complete_actual_ledger(
    tmp_path: Path,
) -> None:
    first_date = date(2026, 8, 14)
    first_root = tmp_path / first_date.isoformat()
    first_root.mkdir()
    with SQLitePaperLedger(first_root / "ledger.sqlite3") as ledger:
        ASharePaperTradingService(ledger).open_account(
            _ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=first_date,
            opened_at=datetime.combine(first_date, datetime.min.time(), UTC),
        )
    # 模拟升级前只有 sidecar、没有账本内 binding，且 sidecar 谎报空事件流。
    (first_root / "ledger-lineage.json").write_text(
        json.dumps(
            {
                "account_id_sha256": hashlib.sha256(_ACCOUNT.encode()).hexdigest(),
                "origin": "LEGACY_SESSION_LOCAL",
                "schema": "paper-account-chain@1",
                "session_date": first_date.isoformat(),
                "source_event_count": 0,
                "source_last_event_hash": None,
                "source_session_date": None,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PaperAccountChainError, match="complete migrated event stream"):
        prepare_paper_day_ledger(
            tmp_path / "2026-08-17",
            session_date=date(2026, 8, 17),
            account_id=_ACCOUNT,
        )


def test_historical_date_directory_symlink_fails_closed(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "2026-08-14"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except (NotImplementedError, OSError):
        pytest.skip("symbolic links are unavailable for this test account")

    with pytest.raises(PaperAccountChainError, match="session directory.*symlink"):
        prepare_paper_day_ledger(
            tmp_path / "2026-08-17",
            session_date=date(2026, 8, 17),
            account_id=_ACCOUNT,
        )


def test_current_lineage_symlink_fails_even_when_binding_is_valid(tmp_path: Path) -> None:
    session_date = date(2026, 8, 14)
    root = tmp_path / session_date.isoformat()
    prepared = prepare_paper_day_ledger(
        root,
        session_date=session_date,
        account_id=_ACCOUNT,
    )
    lineage_path = root / "ledger-lineage.json"
    external = tmp_path / "external-lineage.json"
    external.write_text(json.dumps(prepared.audit_document()), encoding="utf-8")
    lineage_path.unlink()
    try:
        lineage_path.symlink_to(external)
    except (NotImplementedError, OSError):
        pytest.skip("symbolic links are unavailable for this test account")

    with pytest.raises(PaperAccountChainError, match="lineage.*symlink"):
        prepare_paper_day_ledger(
            root,
            session_date=session_date,
            account_id=_ACCOUNT,
        )
