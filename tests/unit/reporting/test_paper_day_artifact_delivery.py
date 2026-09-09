from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import gribuki_trade.services.ashare_paper_day as paper_day_module
from gribuki_trade.domain.paper_day import (
    NewPaperDayEvent,
    PaperDayPhase,
    PaperDayRunManifest,
    PaperDaySeverity,
    paper_day_target_hash,
)
from gribuki_trade.ports.notifier import (
    DeliveryReceipt,
    NotificationTargetKind,
    OutboundNotification,
)
from gribuki_trade.reporting.contracts import (
    REPORT_CONTRACTS,
    ReportKind,
    render_stable_markdown_report,
)
from gribuki_trade.services.ashare_intraday_paper import IntradayPaperRiskConfig
from gribuki_trade.services.ashare_paper import ASharePaperTradingService
from gribuki_trade.services.ashare_paper_day import (
    PAPER_REPORT_ARTIFACT_MARK_SENT,
    PAPER_REPORT_ARTIFACT_RECOVERY_CONFIRMATION,
    PAPER_REPORT_ARTIFACT_RESEND,
    ASharePaperDayConfig,
    ASharePaperDayRunner,
    PaperDayEventPublisher,
)
from gribuki_trade.services.notification_dispatch import NotificationDispatchService
from gribuki_trade.storage.outbox import SQLiteOutbox
from gribuki_trade.storage.paper_day import SQLitePaperDayStore
from gribuki_trade.storage.paper_ledger import SQLitePaperLedger
from gribuki_trade.storage.report_artifact_outbox import (
    ReportArtifactStatus,
    SQLiteReportArtifactOutbox,
)

SESSION = date(2026, 8, 14)
ACCOUNT = "paper-artifact-test"
TARGET_ID = "10001"
NOW = datetime(2026, 8, 14, 8, 0, tzinfo=UTC)


class _TextNotifier:
    channel = "onebot"

    async def send(self, _notification: OutboundNotification) -> DeliveryReceipt:
        return DeliveryReceipt(channel=self.channel, provider_message_id="text-receipt")


class _Unused:
    async def run(self, **_kwargs: object) -> object:
        raise AssertionError("not used")

    async def run_once(self, **_kwargs: object) -> object:
        raise AssertionError("not used")


class _Market:
    async def fetch_intraday_bars_async(self, *_args: object, **_kwargs: object) -> tuple[()]:
        return ()

    async def fetch_trade_prints_async(self, *_args: object, **_kwargs: object) -> tuple[()]:
        return ()


class _ArtifactNotifier:
    def __init__(self, mode: str = "success") -> None:
        self.mode = mode
        self.calls = 0
        self.provider_received = 0

    async def upload_private_file(self, _target_id: str, _artifact: Path) -> object:
        self.calls += 1
        self.provider_received += 1
        if self.mode == "received_then_error":
            raise OSError("provider connection ended after accepting the request")
        if self.mode == "cancelled":
            raise asyncio.CancelledError
        if self.mode == "hard_crash":
            raise SystemExit(73)
        return SimpleNamespace(provider_file_id=f"provider-file-{self.calls}")

    async def upload_group_file(self, _target_id: str, _artifact: Path) -> object:
        raise AssertionError("group upload not expected")


@contextmanager
def _artifact_harness(
    tmp_path: Path,
    *,
    legacy_status: str | None = None,
) -> Iterator[
    tuple[
        SQLitePaperDayStore,
        SQLiteReportArtifactOutbox,
        Path,
        str,
        Callable[..., ASharePaperDayRunner],
    ]
]:
    with (
        SQLitePaperDayStore(tmp_path / "journal.sqlite3") as day_store,
        SQLiteOutbox(tmp_path / "notifications.sqlite3") as notification_outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
        SQLiteReportArtifactOutbox(tmp_path / "artifacts.sqlite3") as artifact_outbox,
    ):
        manifest = PaperDayRunManifest.create(
            session_date=SESSION,
            account_id=ACCOUNT,
            config={
                **ASharePaperDayConfig().audit_document(),
                "intraday_risk_policy": IntradayPaperRiskConfig().audit_document(),
            },
            created_at=NOW - timedelta(hours=8),
            target_hash=paper_day_target_hash(
                channel="onebot",
                target_kind="private",
                target_id=TARGET_ID,
            ),
            initial_cash=Decimal("200000"),
        )
        day_store.create_run(manifest)
        day_store.acquire_lease(
            manifest.run_id,
            "artifact-owner",
            now=NOW,
            lease_for=timedelta(hours=1),
        )
        paper = ASharePaperTradingService(ledger)
        paper.open_account(
            ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=SESSION,
            opened_at=manifest.created_at,
        )
        report_dir = tmp_path / "reports"
        report_dir.mkdir()
        report_path = report_dir / (
            f"ashare-paper-day-{SESSION.isoformat()}-{manifest.run_id[-10:]}.md"
        )
        report_path.write_text(
            render_stable_markdown_report(
                ReportKind.DAILY_REVIEW,
                title="PAPER 日报",
                sections={
                    section: f"{section}的可验证内容。"
                    for section in REPORT_CONTRACTS[ReportKind.DAILY_REVIEW].required_sections
                },
            ),
            encoding="utf-8",
        )
        digest = hashlib.sha256(report_path.read_bytes()).hexdigest()

        def append(event_key: str, event_type: str, payload: dict[str, object]) -> None:
            day_store.append_event(
                NewPaperDayEvent(
                    run_id=manifest.run_id,
                    event_key=event_key,
                    event_type=event_type,
                    phase=PaperDayPhase.POST_CLOSE,
                    severity=PaperDaySeverity.NOTICE,
                    occurred_at=NOW,
                    known_at=NOW,
                    notification_required=False,
                    payload=payload,
                ),
                owner_id="artifact-owner",
                lease_checked_at=NOW,
            )

        append("day-completed", "DAY_COMPLETED", {"completed": True})
        append(
            "report-generated",
            "REPORT_GENERATED",
            {"report_name": report_path.name, "sha256": digest},
        )
        if legacy_status == "sent":
            append(
                "report-upload-result",
                "REPORT_UPLOADED",
                {"delivered": True, "report_name": report_path.name},
            )
        elif legacy_status == "failed":
            append(
                "report-upload-result",
                "REPORT_UPLOAD_FAILED",
                {"delivered": False, "report_name": report_path.name},
            )

        def make_runner(
            notifier: _ArtifactNotifier | None,
            *,
            configured: bool = True,
            recovery_action: str | None = None,
            provider_identifier: str | None = None,
        ) -> ASharePaperDayRunner:
            publisher = PaperDayEventPublisher(
                manifest=manifest,
                store=day_store,
                outbox=notification_outbox,
                dispatcher=NotificationDispatchService(
                    notification_outbox,
                    {"onebot": _TextNotifier()},
                ),
                target_kind=NotificationTargetKind.PRIVATE,
                target_id=TARGET_ID,
                owner_id="artifact-owner",
                clock=lambda: NOW,
                status_path=tmp_path / "status.json",
            )
            return ASharePaperDayRunner(
                manifest=manifest,
                latest_completed_session=date(2026, 8, 13),
                owner_id="artifact-owner",
                store=day_store,
                publisher=publisher,
                preopen_screening=_Unused(),  # type: ignore[arg-type]
                surveillance=_Unused(),  # type: ignore[arg-type]
                market_data=_Market(),  # type: ignore[arg-type]
                paper=paper,
                outbox=notification_outbox,
                report_dir=report_dir,
                artifact_notifier=notifier if configured else None,
                artifact_target_kind=(
                    NotificationTargetKind.PRIVATE if configured else None
                ),
                artifact_target_id=TARGET_ID if configured else None,
                artifact_outbox=artifact_outbox if configured else None,
                report_artifact_recovery_action=recovery_action,
                report_artifact_recovery_confirmation=(
                    PAPER_REPORT_ARTIFACT_RECOVERY_CONFIRMATION
                    if recovery_action is not None
                    else None
                ),
                report_artifact_provider_identifier=provider_identifier,
                clock=lambda: NOW,
            )

        yield day_store, artifact_outbox, report_path, digest, make_runner


def _delivery_key(runner: ASharePaperDayRunner, digest: str) -> str:
    return paper_day_module._paper_report_artifact_key(
        run_id=runner._manifest.run_id,  # noqa: SLF001
        target_kind=NotificationTargetKind.PRIVATE,
        target_id=TARGET_ID,
        artifact_sha256=digest,
    )


def test_paper_daily_review_upload_is_durable_and_idempotent(tmp_path: Path) -> None:
    with _artifact_harness(tmp_path) as (store, _outbox, _path, _digest, make_runner):
        notifier = _ArtifactNotifier()
        result = asyncio.run(make_runner(notifier)._completed_result_if_available())  # noqa: SLF001
        repeated = asyncio.run(make_runner(notifier)._completed_result_if_available())  # noqa: SLF001

        assert result is not None and repeated is not None
        assert result.completed is True
        assert result.artifact_delivery_status == "SENT"
        assert result.artifact_delivery_complete is True
        assert result.daily_review_delivery_complete is True
        assert result.notification_required == 1
        assert result.notification_sent == 1
        assert result.notification_gaps == 0
        assert repeated.artifact_delivery_status == "SENT"
        assert notifier.calls == 1
        status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
        assert status["artifact_delivery_status"] == "SENT"
        assert status["artifact_delivery_complete"] is True
        assert status["daily_review_delivery_complete"] is True
        assert status["notification_gaps"] == 0
        assert sum(
            event.event_type == "REPORT_ARTIFACT_DELIVERY_SENT"
            for event in store.events(result.run_id)
        ) == 1


def test_provider_received_then_error_becomes_ambiguous_and_never_auto_retries(
    tmp_path: Path,
) -> None:
    with _artifact_harness(tmp_path) as (_store, outbox, _path, digest, make_runner):
        uncertain = _ArtifactNotifier("received_then_error")
        first_runner = make_runner(uncertain)
        first = asyncio.run(first_runner._completed_result_if_available())  # noqa: SLF001
        delivery = outbox.get(_delivery_key(first_runner, digest))

        retry_notifier = _ArtifactNotifier()
        second = asyncio.run(
            make_runner(retry_notifier)._completed_result_if_available()  # noqa: SLF001
        )

        assert first is not None and second is not None and delivery is not None
        assert uncertain.provider_received == 1
        assert delivery.status is ReportArtifactStatus.AMBIGUOUS
        assert first.artifact_delivery_status == "AMBIGUOUS"
        assert first.daily_review_delivery_complete is False
        assert first.notification_gaps == 1
        assert second.artifact_delivery_status == "AMBIGUOUS"
        assert retry_notifier.calls == 0
        status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
        assert status["artifact_delivery_status"] == "AMBIGUOUS"
        assert status["artifact_delivery_complete"] is False
        assert status["daily_review_delivery_complete"] is False
        assert status["notification_gaps"] == 1


def test_cancelled_upload_is_ambiguous_and_restart_does_not_resend(tmp_path: Path) -> None:
    with _artifact_harness(tmp_path) as (_store, outbox, path, digest, make_runner):
        cancelled = _ArtifactNotifier("cancelled")
        runner = make_runner(cancelled)
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(runner._upload_report(path))  # noqa: SLF001

        delivery = outbox.get(_delivery_key(runner, digest))
        assert delivery is not None
        assert delivery.status is ReportArtifactStatus.AMBIGUOUS
        restarted = _ArtifactNotifier()
        result = asyncio.run(
            make_runner(restarted)._completed_result_if_available()  # noqa: SLF001
        )
        assert result is not None
        assert result.artifact_delivery_status == "AMBIGUOUS"
        assert restarted.calls == 0


def test_hard_crash_after_claim_is_quarantined_on_restart(tmp_path: Path) -> None:
    with _artifact_harness(tmp_path) as (_store, outbox, path, digest, make_runner):
        crashing = _ArtifactNotifier("hard_crash")
        runner = make_runner(crashing)
        with pytest.raises(SystemExit, match="73"):
            asyncio.run(runner._upload_report(path))  # noqa: SLF001

        claimed = outbox.get(_delivery_key(runner, digest))
        assert claimed is not None
        assert claimed.status is ReportArtifactStatus.IN_FLIGHT

        restarted = _ArtifactNotifier()
        result = asyncio.run(
            make_runner(restarted)._completed_result_if_available()  # noqa: SLF001
        )
        retained = outbox.get(_delivery_key(runner, digest))
        assert result is not None and retained is not None
        assert retained.status is ReportArtifactStatus.AMBIGUOUS
        assert result.artifact_delivery_status == "AMBIGUOUS"
        assert restarted.calls == 0


def test_pending_delivery_resumes_and_sent_without_journal_is_reconciled(
    tmp_path: Path,
) -> None:
    with _artifact_harness(tmp_path) as (store, outbox, path, digest, make_runner):
        first_runner = make_runner(_ArtifactNotifier())
        key = _delivery_key(first_runner, digest)
        outbox.enqueue(
            idempotency_key=key,
            report_kind=ReportKind.DAILY_REVIEW.value,
            target_kind=NotificationTargetKind.PRIVATE,
            target_id=TARGET_ID,
            artifact_name=path.name,
            artifact_sha256=digest,
            created_at=NOW,
        )

        notifier = _ArtifactNotifier()
        resumed = asyncio.run(
            make_runner(notifier)._completed_result_if_available()  # noqa: SLF001
        )
        assert resumed is not None
        assert resumed.artifact_delivery_status == "SENT"
        assert notifier.calls == 1

        sent_events = tuple(
            event
            for event in store.events(resumed.run_id)
            if event.event_type == "REPORT_ARTIFACT_DELIVERY_SENT"
        )
        assert len(sent_events) == 1


def test_sent_outbox_without_journal_repairs_journal_without_upload(tmp_path: Path) -> None:
    with _artifact_harness(tmp_path) as (store, outbox, path, digest, make_runner):
        runner = make_runner(_ArtifactNotifier())
        key = _delivery_key(runner, digest)
        outbox.enqueue(
            idempotency_key=key,
            report_kind=ReportKind.DAILY_REVIEW.value,
            target_kind=NotificationTargetKind.PRIVATE,
            target_id=TARGET_ID,
            artifact_name=path.name,
            artifact_sha256=digest,
            created_at=NOW,
        )
        outbox.claim(key, claimed_at=NOW)
        outbox.mark_sent(key, provider_identifier="persisted-file", sent_at=NOW)
        notifier = _ArtifactNotifier()

        result = asyncio.run(
            make_runner(notifier)._completed_result_if_available()  # noqa: SLF001
        )

        assert result is not None
        assert result.artifact_delivery_status == "SENT"
        assert notifier.calls == 0
        assert any(
            event.event_type == "REPORT_ARTIFACT_DELIVERY_SENT"
            for event in store.events(result.run_id)
        )


@pytest.mark.parametrize(
    ("legacy_status", "expected_status"),
    (("sent", "SENT"), ("failed", "AMBIGUOUS")),
)
def test_legacy_upload_journal_is_migrated_without_retransmission(
    tmp_path: Path,
    legacy_status: str,
    expected_status: str,
) -> None:
    with _artifact_harness(tmp_path, legacy_status=legacy_status) as (
        _store,
        _outbox,
        _path,
        _digest,
        make_runner,
    ):
        notifier = _ArtifactNotifier()
        result = asyncio.run(
            make_runner(notifier)._completed_result_if_available()  # noqa: SLF001
        )

        assert result is not None
        assert result.artifact_delivery_status == expected_status
        assert notifier.calls == 0


def test_not_configured_attachment_is_an_explicit_daily_review_gap(tmp_path: Path) -> None:
    with _artifact_harness(tmp_path) as (_store, _outbox, _path, _digest, make_runner):
        result = asyncio.run(
            make_runner(None, configured=False)._completed_result_if_available()  # noqa: SLF001
        )

        assert result is not None
        assert result.completed is True
        assert result.artifact_delivery_status == "NOT_CONFIGURED"
        assert result.artifact_delivery_complete is False
        assert result.daily_review_delivery_complete is False
        assert result.notification_required == 1
        assert result.notification_sent == 0
        assert result.notification_gaps == 1


@pytest.mark.parametrize(
    "action",
    (PAPER_REPORT_ARTIFACT_MARK_SENT, PAPER_REPORT_ARTIFACT_RESEND),
)
def test_ambiguous_delivery_requires_explicit_operator_recovery(
    tmp_path: Path,
    action: str,
) -> None:
    with _artifact_harness(tmp_path) as (_store, _outbox, _path, _digest, make_runner):
        first = _ArtifactNotifier("received_then_error")
        ambiguous = asyncio.run(
            make_runner(first)._completed_result_if_available()  # noqa: SLF001
        )
        assert ambiguous is not None
        assert ambiguous.artifact_delivery_status == "AMBIGUOUS"

        notifier = _ArtifactNotifier()
        recovered = asyncio.run(
            make_runner(
                notifier,
                recovery_action=action,
                provider_identifier=(
                    "verified-provider-file"
                    if action == PAPER_REPORT_ARTIFACT_MARK_SENT
                    else None
                ),
            )._completed_result_if_available()  # noqa: SLF001
        )

        assert recovered is not None
        assert recovered.artifact_delivery_status == "SENT"
        assert notifier.calls == (1 if action == PAPER_REPORT_ARTIFACT_RESEND else 0)


def test_manual_resend_authorization_is_single_use_after_a_second_ambiguity(
    tmp_path: Path,
) -> None:
    with _artifact_harness(tmp_path) as (store, _outbox, _path, _digest, make_runner):
        initial = _ArtifactNotifier("received_then_error")
        first = asyncio.run(
            make_runner(initial)._completed_result_if_available()  # noqa: SLF001
        )
        assert first is not None and first.artifact_delivery_status == "AMBIGUOUS"

        uncertain_resend = _ArtifactNotifier("received_then_error")
        second = asyncio.run(
            make_runner(
                uncertain_resend,
                recovery_action=PAPER_REPORT_ARTIFACT_RESEND,
            )._completed_result_if_available()  # noqa: SLF001
        )
        assert second is not None and second.artifact_delivery_status == "AMBIGUOUS"
        assert uncertain_resend.calls == 1
        assert any(
            event.event_type == "REPORT_ARTIFACT_RECOVERY_CONSUMED"
            for event in store.events(second.run_id)
        )

        automatic = _ArtifactNotifier()
        replayed = asyncio.run(
            make_runner(automatic)._completed_result_if_available()  # noqa: SLF001
        )
        repeated_flag = asyncio.run(
            make_runner(
                automatic,
                recovery_action=PAPER_REPORT_ARTIFACT_RESEND,
            )._completed_result_if_available()  # noqa: SLF001
        )
        assert replayed is not None and repeated_flag is not None
        assert replayed.artifact_delivery_status == "AMBIGUOUS"
        assert repeated_flag.artifact_delivery_status == "AMBIGUOUS"
        assert automatic.calls == 0

        verified = asyncio.run(
            make_runner(
                automatic,
                recovery_action=PAPER_REPORT_ARTIFACT_MARK_SENT,
                provider_identifier="verified-after-resend",
            )._completed_result_if_available()  # noqa: SLF001
        )
        assert verified is not None
        assert verified.artifact_delivery_status == "SENT"
        assert automatic.calls == 0
