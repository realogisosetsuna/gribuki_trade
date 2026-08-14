"""应用级 OneBot 单次实盘同步 CLI 测试。"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import gribuki_trade.cli as cli
from gribuki_trade.cli import main
from gribuki_trade.reporting.contracts import ReportKind, validate_text_report_contract

_NOW = datetime(2026, 8, 14, 6, 0, tzinfo=UTC)


def _event(message_id: int, text: str, *, sender: int = 123456) -> dict[str, object]:
    return {
        "post_type": "message",
        "message_type": "private",
        "sub_type": "friend",
        "self_id": 654321,
        "user_id": sender,
        "message_id": message_id,
        "time": int(_NOW.timestamp()),
        "raw_message": text,
    }


def _write_event(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_live_sync_cli_ingests_two_phase_onebot_event_and_reports_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    monkeypatch.setattr(
        cli,
        "_verify_live_execution_session",
        lambda _executed_at: True,
    )
    ledger = tmp_path / "live.sqlite3"
    event_path = tmp_path / "event.json"
    proposal = (
        "GT-LIVE/1|command_id=cli-buy-1|account=live-main|side=BUY"
        "|symbol=600000.SH|quantity=100|price=10.20|instrument=STOCK"
        "|executed_at=2026-08-14T13:59:00+08:00"
        "|commission=5.00|transfer_fee=0.10|stamp_tax=0.00"
        "|external_order_id=cli-broker-1"
        "|external_fill_id=cli-execution-1"
    )
    _write_event(event_path, _event(1, proposal))
    common = [
        "live-sync",
        "ingest",
        "--ledger-db",
        str(ledger),
        "--allowed-sender",
        "123456",
        "--event-json",
        str(event_path),
        "--received-at",
        _NOW.isoformat(),
    ]
    assert main(common) == 0
    proposed = json.loads(capsys.readouterr().out)
    assert proposed["status"] == "PROPOSED"
    validate_text_report_contract(ReportKind.EXECUTION_RECEIPT, proposed["response_text"])

    def calendar_must_not_run_for_confirmation(_executed_at: datetime) -> bool:
        raise AssertionError("confirmation must reuse the frozen proposal fact")

    monkeypatch.setattr(
        cli,
        "_verify_live_execution_session",
        calendar_must_not_run_for_confirmation,
    )
    immediate_calls: list[dict[str, object]] = []

    async def failed_immediate_attempt(**kwargs: object) -> dict[str, object]:
        immediate_calls.append(dict(kwargs))
        return {
            "error_code": "LIVE_PROTECTION_MARKET_DATA_UNAVAILABLE",
            "execution_authority": False,
            "ok": False,
            "protection_work_id": kwargs["protection_work_id"],
        }

    monkeypatch.setattr(
        cli,
        "_live_sync_post_confirm_quick_and_track",
        failed_immediate_attempt,
    )
    confirmation = f"GT-LIVE-CONFIRM/1|command_id=cli-buy-1|fingerprint={proposed['fingerprint']}"
    _write_event(event_path, _event(2, confirmation))
    assert main(common) == 0
    confirmed = json.loads(capsys.readouterr().out)
    assert confirmed["status"] == "CONFIRMED"
    assert confirmed["protection_work_id"]
    assert confirmed["ok"] is True
    assert confirmed["immediate_protection"]["ok"] is False
    assert len(immediate_calls) == 1
    assert immediate_calls[0]["protection_work_id"] == confirmed["protection_work_id"]
    assert immediate_calls[0]["confirming_sender_id"] == "123456"
    assert "成交事务提交时仅表示任务可恢复" in confirmed["response_text"]
    assert "持久工作仍由后续 cycle 恢复" in confirmed["response_text"]

    assert (
        main(
            [
                "live-sync",
                "status",
                "--ledger-db",
                str(ledger),
                "--account",
                "live-main",
            ]
        )
        == 0
    )
    status = json.loads(capsys.readouterr().out)
    assert status["integrity_verified"] is True
    assert status["positions"][0]["quantity"] == 100
    assert status["protection_tracking"][0]["plan_ready"] is False
    assert status["work_items"][0]["kind"] == "BUILD_PROTECTION"
    assert status["work_items"][0]["status"] == "PENDING"


@pytest.mark.parametrize("failure_mode", ["none", "outbox", "napcat"])
def test_live_sync_post_confirm_builds_only_its_quick_without_llm_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure_mode: str,
) -> None:
    import gribuki_trade.adapters.akshare as akshare_module
    import gribuki_trade.adapters.baostock as baostock_module
    import gribuki_trade.services.live_protection_inputs as input_module
    from gribuki_trade.features.deep_exit_planning import (
        DeepExitTimeframe,
        aggregate_completed_bars,
    )
    from gribuki_trade.features.technical import TechnicalBar
    from gribuki_trade.ports.market_data import (
        FreshnessStatus,
        IntradayBar,
        MarketDataMeta,
        MinuteInterval,
        SourceSemantics,
    )
    from gribuki_trade.services.live_trade_orchestration import LiveProtectionInputs
    from gribuki_trade.services.live_trade_records import (
        LiveTradeRecordService,
        parse_onebot_private_message,
    )
    from gribuki_trade.storage.live_records import SQLiteLiveRecordStore
    from gribuki_trade.storage.outbox import SQLiteOutbox

    ledger = tmp_path / "live.sqlite3"
    exit_db = tmp_path / "exit.sqlite3"
    outbox = tmp_path / "outbox.sqlite3"
    with SQLiteLiveRecordStore(ledger) as store:
        service = LiveTradeRecordService(
            store,
            allowed_sender_ids=frozenset({"123456"}),
            execution_session_validator=lambda _executed_at: True,
        )
        proposal = (
            "GT-LIVE/1|command_id=immediate-buy-1|account=live-main|side=BUY"
            "|symbol=600000.SH|quantity=100|price=10.20|instrument=STOCK"
            "|executed_at=2026-08-14T13:59:00+08:00"
            "|commission=5.00|transfer_fee=0.10|stamp_tax=0.00"
            "|external_order_id=immediate-order-1"
            "|external_fill_id=immediate-execution-1"
        )
        proposed = service.ingest(
            parse_onebot_private_message(_event(10, proposal)),
            received_at=_NOW,
        )
        confirmed = service.ingest(
            parse_onebot_private_message(
                _event(
                    11,
                    "GT-LIVE-CONFIRM/1|command_id=immediate-buy-1"
                    f"|fingerprint={proposed.fingerprint}",
                )
            ),
            received_at=_NOW,
        )
    assert confirmed.protection_id is not None
    assert confirmed.protection_work_id is not None

    class Market:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def fetch_intraday_bars_async(
            self,
            symbol: str,
            _start: datetime,
            end: datetime,
            **_kwargs: object,
        ):
            bar_end = end - timedelta(seconds=10)
            return (
                IntradayBar(
                    symbol=symbol,
                    start_at=bar_end - timedelta(minutes=1),
                    end_at=bar_end,
                    interval=MinuteInterval.ONE_MINUTE,
                    open=Decimal("10.00"),
                    high=Decimal("100.00"),
                    low=Decimal("0.01"),
                    close=Decimal("0.01"),
                    volume_lots=1000,
                    amount=Decimal("10"),
                    vwap=Decimal("10.00"),
                    is_closed=True,
                    meta=MarketDataMeta(
                        provider="immediate-tracking-test",
                        semantics=SourceSemantics.AGGREGATED_MINUTE_BAR,
                        fetched_at=end - timedelta(seconds=5),
                        provider_timestamp=bar_end,
                        freshness=FreshnessStatus.CURRENT,
                    ),
                ),
            )

    class Calendar:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class QuickOnlyInputs:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def prepare(self, _fill, *, requested_at: datetime) -> LiveProtectionInputs:
            decision_at = requested_at.replace(second=0, microsecond=0)
            bars = tuple(
                TechnicalBar(
                    end_time=decision_at - timedelta(minutes=450 - index),
                    available_at=decision_at - timedelta(minutes=450 - index),
                    open=Decimal("10.00"),
                    high=Decimal("10.10"),
                    low=Decimal("9.90"),
                    close=Decimal("10.00"),
                    volume=1000 + index,
                )
                for index in range(450)
            )
            return LiveProtectionInputs(
                decision_at=decision_at,
                bars=bars,
                technical_invalidation_price=Decimal("9.70"),
                time_exit_at=decision_at + timedelta(days=7),
                strategy_version="immediate-quick-test@1",
                deep_timeframes=(
                    DeepExitTimeframe(
                        "1m",
                        aggregate_completed_bars(bars, interval_minutes=1),
                        Decimal("1"),
                        timedelta(minutes=5),
                    ),
                ),
            )

        async def assess_deep(self, *_args: object, **_kwargs: object):
            raise AssertionError("immediate confirmation must not call the LLM/DEEP path")

    monkeypatch.setattr(
        cli,
        "_required_local_secret",
        lambda _name: (_ for _ in ()).throw(AssertionError("LLM key must not be read")),
    )
    monkeypatch.setattr(akshare_module, "AKShareMarketDataAdapter", Market)
    monkeypatch.setattr(baostock_module, "BaoStockDailyAdapter", Calendar)
    monkeypatch.setattr(
        input_module,
        "PublicMarketLiveProtectionInputProvider",
        QuickOnlyInputs,
    )
    if failure_mode == "outbox":
        def fail_outbox_enqueue(*_args: object, **_kwargs: object) -> None:
            raise OSError("simulated immediate alert outbox failure")

        monkeypatch.setattr(SQLiteOutbox, "enqueue", fail_outbox_enqueue)
    dispatch_calls: list[tuple[object, ...]] = []

    async def fake_dispatch(*args: object, **_kwargs: object) -> dict[str, object]:
        dispatch_calls.append(args)
        if failure_mode == "napcat":
            raise OSError("simulated immediate NapCat failure")
        return {"dead": 0, "retry_scheduled": 0, "sent": 1}

    monkeypatch.setattr(cli, "_napcat_dispatch", fake_dispatch)

    result = asyncio.run(
        cli._live_sync_post_confirm_quick_and_track(
            ledger_db=str(ledger),
            exit_plan_db=str(exit_db),
            outbox_path=str(outbox),
            account_id="live-main",
            protection_id=confirmed.protection_id,
            protection_work_id=confirmed.protection_work_id,
            confirming_sender_id="123456",
            target_kind_value=None,
            target_id=None,
            quick_timeout_seconds=5.0,
            base_url="http://127.0.0.1:3000",
            dispatch_cycles=1,
            dispatch_poll_interval=0,
        )
    )

    assert result["ok"] is (failure_mode != "outbox")
    assert result["execution_authority"] is False
    assert result["quick"]["plan_ready"] is True
    assert result["quick"]["completed"] == 1
    assert result["deep"]["queued"] is True
    assert result["tracking"]["attempted"] is True
    assert result["tracking"]["queued_alerts"] == 1
    assert result["tracking"]["outbox_delivery"]["completed"] == (
        0 if failure_mode == "outbox" else 1
    )
    if failure_mode == "outbox":
        assert result["error_code"] == "LIVE_IMMEDIATE_ALERT_OUTBOX_PENDING"
        assert result["tracking"]["outbox_delivery"]["retried"] == 1
        assert dispatch_calls == []
    elif failure_mode == "napcat":
        assert result["dispatch"]["ok"] is False
        assert result["dispatch"]["error_code"] == "LIVE_IMMEDIATE_ALERT_DISPATCH_FAILED"
        assert len(dispatch_calls) == 1
    else:
        assert result["dispatch"]["ok"] is True
        assert result["dispatch"]["sent"] == 1
        assert len(dispatch_calls) == 1
    with SQLiteLiveRecordStore(ledger) as store:
        tracking = store.tracking("live-main", active_only=False)
        assert tracking[0].plan_ready is True
        kinds = {item.kind.value: item.status.value for item in store.work_items("live-main")}
        assert kinds["BUILD_PROTECTION"] == "COMPLETED"
        assert kinds["BUILD_DEEP_PROTECTION"] == "PENDING"
    if failure_mode != "outbox":
        with SQLiteOutbox(outbox) as stored_outbox:
            alert = stored_outbox.list_items()[0]
            assert alert.notification.target_id == "123456"
            assert alert.notification.target_kind.value == "private"


@pytest.mark.parametrize(
    ("calendar_result", "expected_code"),
    [
        (False, "EXECUTION_NOT_ON_TRADING_SESSION"),
        (RuntimeError("calendar offline"), "LIVE_TRADING_CALENDAR_UNAVAILABLE"),
    ],
)
def test_live_sync_cli_calendar_failures_close_before_proposal_is_stored(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
    calendar_result: bool | Exception,
    expected_code: str,
) -> None:
    def validate(_executed_at: datetime) -> bool:
        if isinstance(calendar_result, Exception):
            raise calendar_result
        return calendar_result

    monkeypatch.setattr(cli, "_verify_live_execution_session", validate)
    ledger = tmp_path / "live.sqlite3"
    event_path = tmp_path / "event.json"
    proposal = (
        "GT-LIVE/1|command_id=cli-calendar-1|account=live-main|side=BUY"
        "|symbol=600000.SH|quantity=100|price=10.20|instrument=STOCK"
        "|executed_at=2026-08-14T13:59:00+08:00"
        "|commission=5.00|transfer_fee=0.10|stamp_tax=0.00"
        "|external_order_id=cli-calendar-order-1"
        "|external_fill_id=cli-calendar-execution-1"
    )
    _write_event(event_path, _event(1, proposal))
    code = main(
        [
            "live-sync",
            "ingest",
            "--ledger-db",
            str(ledger),
            "--allowed-sender",
            "123456",
            "--event-json",
            str(event_path),
            "--received-at",
            _NOW.isoformat(),
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert code == 1
    assert result["error_code"] == expected_code

    from gribuki_trade.storage.live_records import SQLiteLiveRecordStore

    with SQLiteLiveRecordStore(ledger) as store:
        assert store.events("live-main") == ()


def test_live_execution_session_verifier_uses_exact_baostock_natural_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import gribuki_trade.adapters.baostock as baostock_module
    from gribuki_trade.ports.market_data import TradeCalendarDay

    calls: list[tuple[object, object]] = []

    class Calendar:
        def __init__(self, **kwargs: object) -> None:
            assert kwargs == {"max_attempts": 2, "timeout_seconds": 20.0}

        def fetch_trade_calendar(self, start, end):
            calls.append((start, end))
            return (TradeCalendarDay(start, False),)

    monkeypatch.setattr(baostock_module, "BaoStockDailyAdapter", Calendar)
    assert cli._verify_live_execution_session(_NOW) is False
    assert calls == [(_NOW.date(), _NOW.date())]


def test_live_sync_cli_rejects_untrusted_sender_without_echoing_payload(
    tmp_path: Path,
    capsys,
) -> None:
    event_path = tmp_path / "event.json"
    marker = "DO-NOT-ECHO-UNTRUSTED"
    _write_event(event_path, _event(1, marker, sender=999999))
    code = main(
        [
            "live-sync",
            "ingest",
            "--ledger-db",
            str(tmp_path / "live.sqlite3"),
            "--allowed-sender",
            "123456",
            "--event-json",
            str(event_path),
            "--received-at",
            _NOW.isoformat(),
        ]
    )
    rendered = capsys.readouterr().out
    assert code == 1
    assert json.loads(rendered)["error_code"] == "SENDER_NOT_ALLOWED"
    assert marker not in rendered


def test_live_sync_cycle_is_a_finite_application_entry_without_order_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys,
) -> None:
    captured: dict[str, object] = {}

    async def fake_cycle(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "execution_authority": False,
            "ok": True,
            "tracking": {"queued_alerts": 1},
        }

    monkeypatch.setattr(cli, "_live_sync_cycle", fake_cycle)
    code = main(
        [
            "live-sync",
            "cycle",
            "--ledger-db",
            str(tmp_path / "live.sqlite3"),
            "--exit-plan-db",
            str(tmp_path / "exit.sqlite3"),
            "--outbox-path",
            str(tmp_path / "outbox.sqlite3"),
            "--target-kind",
            "private",
            "--target-id",
            "123456",
            "--base-url",
            "http://127.0.0.1:3000",
            "--llm-provider",
            "deepseek",
            "--llm-model",
            "deepseek-v4-flash",
            "--confirm",
            "LIVE_SYNC_CYCLE",
        ]
    )

    result = json.loads(capsys.readouterr().out)
    assert code == 0
    assert result["execution_authority"] is False
    assert captured["confirmation"] == "LIVE_SYNC_CYCLE"
    assert captured["deep_timeout_seconds"] == 600.0
    assert captured["tracking_pump_interval"] == 30.0
    assert captured["tracking_pump_limit"] == 20
    assert captured["target_id"] == "123456"


def test_live_sync_cycle_confirmation_fails_before_credentials_or_network() -> None:
    result = asyncio.run(
        cli._live_sync_cycle(
            ledger_db="unused-ledger.sqlite3",
            exit_plan_db="unused-exit.sqlite3",
            outbox_path="unused-outbox.sqlite3",
            target_kind_value=None,
            target_id=None,
            base_url=None,
            llm_provider=None,
            llm_model=None,
            work_limit=1,
            dispatch_cycles=1,
            dispatch_poll_interval=0,
            confirmation=None,
        )
    )

    assert result == {
        "error_code": "LIVE_SYNC_CYCLE_CONFIRMATION_REQUIRED",
        "ok": False,
    }


def test_live_sync_cycle_tracks_and_dispatches_before_one_bounded_build(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import gribuki_trade.adapters.akshare as akshare_module
    import gribuki_trade.adapters.baostock as baostock_module
    import gribuki_trade.services.live_market_tracking as tracking_module
    import gribuki_trade.services.live_trade_orchestration as orchestration_module
    import gribuki_trade.services.llm_production as llm_module

    events: list[str] = []

    class _Dual:
        def close(self) -> None:
            events.append("dual-close")

    class _Market:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class _Calendar:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class _Orchestration:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def process_due_work(self, *, kinds, limit: int, **_kwargs: object):
            kind = next(iter(kinds)).value
            events.append(f"work:{kind}:{limit}")
            return SimpleNamespace(claimed=0, completed=0, dead=0, retried=0)

    class _Tracking:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def run_once(self, *, delivery_limit: int):
            events.append(f"tracking:{delivery_limit}")
            return SimpleNamespace(
                barrier_observations=0,
                failures=(),
                fetched_bars=0,
                queued_alerts=0,
                target_count=0,
            )

    async def fake_dispatch(*_args: object, **_kwargs: object) -> dict[str, int]:
        events.append("dispatch")
        return {"dead": 0, "retry_scheduled": 0, "sent": 0}

    monkeypatch.setattr(cli, "_required_local_secret", lambda _name: "test-secret")
    monkeypatch.setattr(cli, "_napcat_dispatch", fake_dispatch)
    monkeypatch.setattr(
        llm_module,
        "build_production_dual_track_analyzer",
        lambda *_args, **_kwargs: _Dual(),
    )
    monkeypatch.setattr(akshare_module, "AKShareMarketDataAdapter", _Market)
    monkeypatch.setattr(baostock_module, "BaoStockDailyAdapter", _Calendar)
    monkeypatch.setattr(
        orchestration_module,
        "LiveTradeOrchestrationService",
        _Orchestration,
    )
    monkeypatch.setattr(tracking_module, "LiveMarketTrackingCycleService", _Tracking)

    result = asyncio.run(
        cli._live_sync_cycle(
            ledger_db=str(tmp_path / "live.sqlite3"),
            exit_plan_db=str(tmp_path / "exit.sqlite3"),
            outbox_path=str(tmp_path / "outbox.sqlite3"),
            target_kind_value="private",
            target_id="123456",
            base_url="http://127.0.0.1:3000",
            llm_provider="deepseek",
            llm_model="deepseek-v4-flash",
            work_limit=20,
            dispatch_cycles=1,
            dispatch_poll_interval=0,
            confirmation="LIVE_SYNC_CYCLE",
        )
    )

    assert result["ok"] is True
    assert events[:4] == [
        "tracking:100",
        "dispatch",
        "work:CLOSE_PROTECTION:20",
        "work:BUILD_PROTECTION:1",
    ]
    assert result["analysis"]["build_limit"] == 1
    assert result["execution_authority"] is False


def test_live_sync_cycle_keeps_building_protection_when_napcat_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import gribuki_trade.adapters.akshare as akshare_module
    import gribuki_trade.adapters.baostock as baostock_module
    import gribuki_trade.services.live_market_tracking as tracking_module
    import gribuki_trade.services.live_trade_orchestration as orchestration_module
    import gribuki_trade.services.llm_production as llm_module

    work_kinds: list[str] = []

    class _Dual:
        def close(self) -> None:
            pass

    class _Provider:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class _Orchestration:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def process_due_work(self, *, kinds, **_kwargs: object):
            kind = next(iter(kinds)).value
            work_kinds.append(kind)
            return SimpleNamespace(claimed=1, completed=1, dead=0, retried=0)

    class _Tracking:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def run_once(self, *, delivery_limit: int):
            assert delivery_limit == 100
            return SimpleNamespace(
                barrier_observations=0,
                failures=(),
                fetched_bars=0,
                queued_alerts=1,
                target_count=1,
            )

    async def failed_dispatch(*_args: object, **_kwargs: object) -> dict[str, int]:
        raise RuntimeError("NapCat unavailable")

    monkeypatch.setattr(cli, "_required_local_secret", lambda _name: "test-secret")
    monkeypatch.setattr(cli, "_napcat_dispatch", failed_dispatch)
    monkeypatch.setattr(
        llm_module,
        "build_production_dual_track_analyzer",
        lambda *_args, **_kwargs: _Dual(),
    )
    monkeypatch.setattr(akshare_module, "AKShareMarketDataAdapter", _Provider)
    monkeypatch.setattr(baostock_module, "BaoStockDailyAdapter", _Provider)
    monkeypatch.setattr(
        orchestration_module,
        "LiveTradeOrchestrationService",
        _Orchestration,
    )
    monkeypatch.setattr(tracking_module, "LiveMarketTrackingCycleService", _Tracking)

    result = asyncio.run(
        cli._live_sync_cycle(
            ledger_db=str(tmp_path / "live.sqlite3"),
            exit_plan_db=str(tmp_path / "exit.sqlite3"),
            outbox_path=str(tmp_path / "outbox.sqlite3"),
            target_kind_value="private",
            target_id="123456",
            base_url="http://127.0.0.1:3000",
            llm_provider="deepseek",
            llm_model="deepseek-v4-flash",
            work_limit=20,
            dispatch_cycles=1,
            dispatch_poll_interval=0,
            confirmation="LIVE_SYNC_CYCLE",
        )
    )

    assert result["ok"] is False
    assert result["error_code"] == "LIVE_ALERT_DISPATCH_FAILED"
    assert work_kinds == [
        "CLOSE_PROTECTION",
        "BUILD_PROTECTION",
        "BUILD_DEEP_PROTECTION",
    ]
    assert result["analysis"]["build"]["completed"] == 1
    assert result["analysis"]["deep"]["completed"] == 1
    assert result["execution_authority"] is False


def test_live_sync_cycle_pumps_two_tracking_rounds_while_deep_is_slow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import gribuki_trade.adapters.akshare as akshare_module
    import gribuki_trade.adapters.baostock as baostock_module
    import gribuki_trade.services.live_market_tracking as tracking_module
    import gribuki_trade.services.live_trade_orchestration as orchestration_module
    import gribuki_trade.services.llm_production as llm_module

    events: list[str] = []
    tracking_calls = 0

    class _Dual:
        def close(self) -> None:
            events.append("dual-close")

    class _Market:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class _Calendar:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class _Orchestration:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def process_due_work(
            self,
            *,
            kinds,
            limit: int,
            work_timeout_seconds: float | None = None,
            **_kwargs: object,
        ):
            kind = next(iter(kinds)).value
            events.append(f"work:{kind}:{limit}")
            if kind == "BUILD_DEEP_PROTECTION":
                assert work_timeout_seconds == 0.01
                await asyncio.sleep(0.03)
                return SimpleNamespace(claimed=1, completed=0, dead=0, retried=1)
            return SimpleNamespace(claimed=0, completed=0, dead=0, retried=0)

    class _Tracking:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def run_once(self, *, delivery_limit: int):
            nonlocal tracking_calls
            tracking_calls += 1
            events.append(f"tracking:{tracking_calls}:{delivery_limit}")
            await asyncio.sleep(0)
            return SimpleNamespace(
                barrier_observations=1,
                failures=(),
                fetched_bars=1,
                queued_alerts=1,
                target_count=1,
            )

    async def fake_dispatch(*_args: object, **_kwargs: object) -> dict[str, int]:
        events.append("dispatch")
        return {"dead": 0, "retry_scheduled": 0, "sent": 1}

    monkeypatch.setattr(cli, "_required_local_secret", lambda _name: "test-secret")
    monkeypatch.setattr(cli, "_napcat_dispatch", fake_dispatch)
    monkeypatch.setattr(
        llm_module,
        "build_production_dual_track_analyzer",
        lambda *_args, **_kwargs: _Dual(),
    )
    monkeypatch.setattr(akshare_module, "AKShareMarketDataAdapter", _Market)
    monkeypatch.setattr(baostock_module, "BaoStockDailyAdapter", _Calendar)
    monkeypatch.setattr(
        orchestration_module,
        "LiveTradeOrchestrationService",
        _Orchestration,
    )
    monkeypatch.setattr(tracking_module, "LiveMarketTrackingCycleService", _Tracking)

    result = asyncio.run(
        cli._live_sync_cycle(
            ledger_db=str(tmp_path / "live.sqlite3"),
            exit_plan_db=str(tmp_path / "exit.sqlite3"),
            outbox_path=str(tmp_path / "outbox.sqlite3"),
            target_kind_value="private",
            target_id="123456",
            base_url="http://127.0.0.1:3000",
            llm_provider="deepseek",
            llm_model="deepseek-v4-flash",
            work_limit=20,
            dispatch_cycles=1,
            dispatch_poll_interval=0,
            confirmation="LIVE_SYNC_CYCLE",
            deep_timeout_seconds=0.01,
            tracking_pump_interval=0.005,
            tracking_pump_limit=2,
        )
    )

    assert result["ok"] is True
    assert result["execution_authority"] is False
    assert result["analysis"]["deep"] == {
        "claimed": 1,
        "completed": 0,
        "dead": 0,
        "retried": 1,
    }
    assert result["tracking"]["pump_runs"] == 2
    assert result["tracking"]["barrier_observations"] == 3
    assert tracking_calls == 3
    assert events.count("dispatch") == 3
