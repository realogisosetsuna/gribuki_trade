"""A 股盘后研究命令处理器。

该模块承载盘后研究的幂等状态机、报告校验和 NapCat durable outbox 交付。
历史上这些流程位于 ``gribuki_trade.cli``；这里通过 facade 延迟解析可替换
依赖，保留旧的导入和测试 monkeypatch 边界，同时不改变 LIVE/PAPER 权限。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from datetime import time as datetime_time
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from zoneinfo import ZoneInfo

from gribuki_trade.adapters.notifiers import NAPCAT_ACCESS_TOKEN_SECRET
from gribuki_trade.cli_commands.close_research_payloads import (
    _paper_session_instrument_profiles,
)
from gribuki_trade.cli_commands.post_close_results import (
    _post_close_analysis_outcome,
    _post_close_completed_result,
    _post_close_error,
    _post_close_mapping,
    _post_close_optional_decimal,
    _post_close_optional_integer,
    _post_close_optional_string,
    _post_close_skipped,
    _post_close_string_tuple,
    _PostCloseCLIError,
)
from gribuki_trade.security import SecretProviderError
from gribuki_trade.security.post_close_urls import (
    PostCloseSearxngURLValidationError,
    validate_post_close_searxng_url,
)

if TYPE_CHECKING:
    from gribuki_trade.domain.instruments import ResearchInstrumentProfile
    from gribuki_trade.domain.paper_trading import PaperPosition
    from gribuki_trade.domain.post_close import PostCloseInstrumentResearch
    from gribuki_trade.reporting.paper_day.paper_day_summary import PaperDayExecutiveProjection
    from gribuki_trade.services.ashare.close.ashare_close_sessions import CloseSessionResolution
    from gribuki_trade.services.ashare.close.ashare_post_close import PostCloseOrchestrationResult
    from gribuki_trade.services.ashare.paper_day.ashare_paper import ASharePaperTradingService
    from gribuki_trade.storage.research.candidate_store import SQLiteCandidateStore


def _cli() -> Any:
    """延迟取得 CLI facade，保证既能独立导入又能保留替换点。"""

    import gribuki_trade.cli as cli

    return cli


def _atomic_write_cli_json(*args: Any, **kwargs: Any) -> Any:
    return _cli()._atomic_write_cli_json(*args, **kwargs)


def _required_local_secret(name: str) -> str:
    return cast(str, _cli()._required_local_secret(name))


def _ashare_close_research_once(*args: Any, **kwargs: Any) -> Any:
    return _cli()._ashare_close_research_once(*args, **kwargs)


__all__ = [
    "_CLIExistingCloseResearch",
    "_FixedPostCloseSessions",
    "_ashare_post_close",
    "_ashare_post_close_report",
    "_ashare_post_close_run",
    "_ashare_post_close_status",
    "_deliver_post_close_artifact",
    "_post_close_delivery_summary",
    "_post_close_napcat_ready",
    "_post_close_process_lock",
    "_post_close_section_excerpt",
    "_post_close_sha256",
    "_read_post_close_json",
    "_run_post_close_orchestrator",
    "_validate_post_close_target",
    "_validated_post_close_artifact",
]


class _FixedPostCloseSessions:
    """在单次运行中复用一份已经核验的日历结果。"""

    def __init__(self, sessions: CloseSessionResolution) -> None:
        self._sessions = sessions

    async def resolve(self, now: datetime) -> CloseSessionResolution:
        del now
        return self._sessions


class _CLIExistingCloseResearch:
    """将生产收盘研究辅助器适配到 PAPER 持仓。"""

    def __init__(
        self,
        *,
        profiles: Mapping[str, ResearchInstrumentProfile],
        history_days: int,
        news_runtime_dir: str,
        research_db: str,
        market_evidence_dir: str,
        analysis_outbox_db: str,
        news_feeds: Sequence[str] | None,
        refresh_news: bool,
        search_discovery: bool,
        searxng_url: str | None,
        macro_enabled: bool,
        macro_provider: str,
        model: str | None,
        macro_weight: Decimal,
    ) -> None:
        self._profiles = dict(profiles)
        self._history_days = history_days
        self._news_runtime_dir = news_runtime_dir
        self._research_db = research_db
        self._market_evidence_dir = market_evidence_dir
        self._analysis_outbox_db = analysis_outbox_db
        self._news_feeds = news_feeds
        self._refresh_news = refresh_news
        self._search_discovery = search_discovery
        self._searxng_url = searxng_url
        self._macro_enabled = macro_enabled
        self._macro_provider = macro_provider
        self._model = model
        self._macro_weight = macro_weight
        # 除日线适配器外，AKShare 还包含嵌入式 V8 路由。生产进程中必须串行执行每个持仓的
        # 完整研究调用；更窄的供应商锁不足以保证安全。
        self._research_lock = asyncio.Lock()

    async def research(
        self,
        position: PaperPosition,
        *,
        sessions: CloseSessionResolution,
    ) -> PostCloseInstrumentResearch:
        async with self._research_lock:
            return await self._research_serial(position, sessions=sessions)

    async def _research_serial(
        self,
        position: PaperPosition,
        *,
        sessions: CloseSessionResolution,
    ) -> PostCloseInstrumentResearch:
        from gribuki_trade.domain.post_close import (
            PostCloseInstrumentResearch,
            PostCloseResearchStatus,
        )

        profile = self._profiles.get(position.symbol)
        if profile is None:
            return PostCloseInstrumentResearch(
                symbol=position.symbol,
                status=PostCloseResearchStatus.FAILED,
                failure_code="PAPER_PROFILE_NOT_AVAILABLE",
            )
        try:
            result = await _ashare_close_research_once(
                position.symbol,
                self._history_days,
                sessions.latest_completed_session,
                sessions.next_session,
                self._news_runtime_dir,
                self._research_db,
                self._market_evidence_dir,
                self._analysis_outbox_db,
                self._news_feeds,
                self._refresh_news,
                self._search_discovery,
                self._searxng_url,
                self._macro_enabled,
                self._macro_provider,
                self._model,
                self._macro_weight,
                True,
                None,
                None,
                None,
                profile,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return PostCloseInstrumentResearch(
                symbol=position.symbol,
                status=PostCloseResearchStatus.FAILED,
                failure_code="CLOSE_RESEARCH_FAILED",
            )
        if result.get("ok") is not True:
            code = result.get("error_code")
            return PostCloseInstrumentResearch(
                symbol=position.symbol,
                status=PostCloseResearchStatus.FAILED,
                failure_code=(
                    str(code) if isinstance(code, str) and code.strip() else "CLOSE_RESEARCH_FAILED"
                ),
            )
        decision = result.get("decision")
        if not isinstance(decision, str) or not decision.strip():
            return PostCloseInstrumentResearch(
                symbol=position.symbol,
                status=PostCloseResearchStatus.FAILED,
                failure_code="CLOSE_RESEARCH_RESULT_INVALID",
            )
        dual_track = _post_close_mapping(result.get("macro_dual_track"))
        baseline_track = _post_close_mapping(dual_track.get("baseline"))
        adversarial_track = _post_close_mapping(dual_track.get("adversarial"))
        return PostCloseInstrumentResearch(
            symbol=position.symbol,
            status=PostCloseResearchStatus.COMPLETED,
            decision=decision,
            technical_score=_post_close_optional_decimal(result.get("technical_score")),
            reference_price=_post_close_optional_decimal(result.get("reference_price")),
            invalidation_price=_post_close_optional_decimal(result.get("invalidation_price")),
            reason_codes=_post_close_string_tuple(result.get("reason_codes")),
            uncertainties=_post_close_string_tuple(result.get("uncertainties")),
            daily_bar_count=_post_close_optional_integer(result.get("daily_bar_count")),
            technical_decision=_post_close_optional_string(result.get("technical_decision")),
            combined_score=_post_close_optional_decimal(result.get("combined_score")),
            macro_score=_post_close_optional_decimal(result.get("macro_score")),
            macro_evidence_coverage=_post_close_optional_decimal(
                result.get("macro_evidence_coverage")
            ),
            macro_provider=_post_close_optional_string(result.get("macro_provider")),
            macro_model=_post_close_optional_string(result.get("macro_model")),
            macro_failure_code=_post_close_optional_string(result.get("macro_failure_code")),
            market_data_failure_code=_post_close_optional_string(
                result.get("market_data_failure_code")
            ),
            macro_analysis_id=_post_close_optional_string(dual_track.get("analysis_id")),
            macro_selected_track=_post_close_optional_string(dual_track.get("selected_track")),
            macro_audit_record_sha256=_post_close_optional_string(
                dual_track.get("audit_record_sha256")
            ),
            baseline_macro_decision=_post_close_optional_string(baseline_track.get("decision")),
            baseline_macro_regime=_post_close_optional_string(baseline_track.get("regime")),
            baseline_macro_score=_post_close_optional_decimal(baseline_track.get("macro_impact")),
            baseline_macro_evidence_coverage=_post_close_optional_decimal(
                baseline_track.get("evidence_coverage")
            ),
            baseline_macro_model=_post_close_optional_string(baseline_track.get("model_version")),
            adversarial_macro_decision=_post_close_optional_string(
                adversarial_track.get("decision")
            ),
            adversarial_macro_regime=_post_close_optional_string(adversarial_track.get("regime")),
            adversarial_macro_score=_post_close_optional_decimal(
                adversarial_track.get("macro_impact")
            ),
            adversarial_macro_evidence_coverage=_post_close_optional_decimal(
                adversarial_track.get("evidence_coverage")
            ),
            adversarial_macro_model=_post_close_optional_string(
                adversarial_track.get("model_version")
            ),
        )



@contextmanager
def _post_close_process_lock(path: Path) -> Iterator[None]:
    """获取进程级非阻塞锁，并在进程终止时释放。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    try:
        if stream.seek(0, os.SEEK_END) == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover - Windows 下采用原子替换的兼容分支
                import fcntl

                fcntl.flock(  # type: ignore[attr-defined]
                    stream.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,  # type: ignore[attr-defined]
                )
        except (OSError, BlockingIOError):
            raise _PostCloseCLIError("POST_CLOSE_ALREADY_RUNNING", retryable=True) from None
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:  # pragma: no cover
                fcntl.flock(  # type: ignore[attr-defined]
                    stream.fileno(),
                    fcntl.LOCK_UN,  # type: ignore[attr-defined]
                )
    finally:
        stream.close()


def _read_post_close_json(path: Path, code: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise _PostCloseCLIError(code) from None
    if not isinstance(value, dict):
        raise _PostCloseCLIError(code)
    return cast(dict[str, object], value)


def _post_close_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _append_post_close_audit(
    path: Path,
    *,
    event: str,
    run_id: str,
    target_hash: str,
    details: Mapping[str, object] | None = None,
) -> None:
    document = {
        "event": event,
        "occurred_at": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "target_hash": target_hash,
        **dict(details or {}),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(document, ensure_ascii=False, sort_keys=True, allow_nan=False))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


async def _ashare_post_close(
    action: str,
    runtime_dir: str,
    session_date: date | None,
    account_id: str,
    target_kind_value: str | None,
    target_id: str | None,
    base_url: str,
    candidate_db: str,
    history_days: int,
    news_runtime_dir: str,
    research_db: str,
    market_evidence_dir: str,
    news_feeds: Sequence[str] | None,
    refresh_news: bool,
    search_discovery: bool,
    searxng_url: str | None,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    macro_weight: Decimal,
    dispatch_cycles: int,
    dispatch_poll_interval: float,
    confirmation: str | None,
    recover_analysis: bool = False,
    recover_delivery: bool = False,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """一次幂等盘后复核与交付的生产边界。"""

    if action not in {"run", "status", "report"}:
        raise ValueError("unsupported A-share post-close action")
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None or resolved_now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    local_today = resolved_now.astimezone(ZoneInfo("Asia/Shanghai")).date()
    resolved_session = session_date or local_today
    session_root = Path(runtime_dir).resolve() / resolved_session.isoformat()
    post_root = session_root / "post-close"

    if action == "status":
        return _ashare_post_close_status(post_root, resolved_session)
    if action == "report":
        return _ashare_post_close_report(post_root, resolved_session)
    if confirmation != "POST_CLOSE":
        return _post_close_error(
            action,
            resolved_session,
            post_root,
            "POST_CLOSE_CONFIRMATION_REQUIRED",
        )
    if target_kind_value is None or target_id is None or not target_id.strip():
        return _post_close_error(
            action,
            resolved_session,
            post_root,
            "NOTIFICATION_TARGET_REQUIRED",
        )
    try:
        return await _ashare_post_close_run(
            session_root=session_root,
            session_date=resolved_session,
            account_id=account_id,
            target_kind_value=target_kind_value,
            target_id=target_id.strip(),
            base_url=base_url,
            candidate_db=candidate_db,
            history_days=history_days,
            news_runtime_dir=news_runtime_dir,
            research_db=research_db,
            market_evidence_dir=market_evidence_dir,
            news_feeds=news_feeds,
            refresh_news=refresh_news,
            search_discovery=search_discovery,
            searxng_url=searxng_url,
            macro_enabled=macro_enabled,
            macro_provider=macro_provider,
            model=model,
            macro_weight=macro_weight,
            dispatch_cycles=dispatch_cycles,
            dispatch_poll_interval=dispatch_poll_interval,
            recover_analysis=recover_analysis,
            recover_delivery=recover_delivery,
            now=resolved_now,
        )
    except asyncio.CancelledError:
        raise
    except _PostCloseCLIError as error:
        return _post_close_error(
            action,
            resolved_session,
            post_root,
            error.code,
            retryable=error.retryable,
        )
    except Exception:
        return _post_close_error(
            action,
            resolved_session,
            post_root,
            "POST_CLOSE_RUN_FAILED",
            retryable=True,
        )


async def _ashare_post_close_run(
    *,
    session_root: Path,
    session_date: date,
    account_id: str,
    target_kind_value: str,
    target_id: str,
    base_url: str,
    candidate_db: str,
    history_days: int,
    news_runtime_dir: str,
    research_db: str,
    market_evidence_dir: str,
    news_feeds: Sequence[str] | None,
    refresh_news: bool,
    search_discovery: bool,
    searxng_url: str | None,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    macro_weight: Decimal,
    dispatch_cycles: int,
    dispatch_poll_interval: float,
    recover_analysis: bool,
    recover_delivery: bool,
    now: datetime,
) -> dict[str, object]:
    try:
        validated_searxng_url = validate_post_close_searxng_url(searxng_url)
    except PostCloseSearxngURLValidationError:
        raise _PostCloseCLIError("SEARXNG_URL_INVALID") from None

    from gribuki_trade.adapters.market_data.baostock import BaoStockDailyAdapter
    from gribuki_trade.domain.paper_day import paper_day_target_hash
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.reporting.paper_day.paper_day_summary import (
        PaperDaySidecarError,
        project_paper_day_sidecars,
    )
    from gribuki_trade.services.ashare.close.ashare_close_sessions import (
        AShareCloseSessionResolver,
        CloseAnalysisMode,
        CloseSessionResolutionError,
    )
    from gribuki_trade.services.ashare.close.ashare_post_close import PostCloseOrchestrationError
    from gribuki_trade.services.ashare.paper_day.ashare_paper import ASharePaperTradingService
    from gribuki_trade.storage.paper.paper_day import SQLitePaperDayStore
    from gribuki_trade.storage.paper.paper_ledger import SQLitePaperLedger
    from gribuki_trade.storage.research.candidate_store import SQLiteCandidateStore

    local_now = now.astimezone(ZoneInfo("Asia/Shanghai"))
    if session_date != local_now.date():
        raise _PostCloseCLIError("SESSION_DATE_NOT_TODAY")
    if local_now.time().replace(tzinfo=None) < datetime_time(15, 5):
        return _post_close_skipped(session_date, session_root / "post-close", "BEFORE_1505")
    if not account_id.strip():
        raise _PostCloseCLIError("ACCOUNT_ID_INVALID")
    if dispatch_cycles < 1 or not 0 <= dispatch_poll_interval < float("inf"):
        raise ValueError("dispatch policy is invalid")

    target_kind = NotificationTargetKind(target_kind_value)
    _validate_post_close_target(base_url, target_kind, target_id)
    calendar = BaoStockDailyAdapter(max_attempts=3, timeout_seconds=20.0)
    try:
        sessions = await AShareCloseSessionResolver(calendar).resolve(now)
    except CloseSessionResolutionError as error:
        if error.code == "MARKET_SESSION_NOT_CLOSED":
            return _post_close_skipped(session_date, session_root / "post-close", error.code)
        raise _PostCloseCLIError(error.code, retryable=True) from None
    if (
        sessions.analysis_mode is not CloseAnalysisMode.POST_CLOSE
        or sessions.latest_completed_session != session_date
    ):
        return _post_close_skipped(
            session_date,
            session_root / "post-close",
            "CURRENT_DATE_NOT_TRADING_SESSION",
        )

    try:
        projection = project_paper_day_sidecars(session_root)
    except PaperDaySidecarError as error:
        raise _PostCloseCLIError(error.code, retryable=True) from None
    if projection.session_date != session_date or projection.lifecycle != "COMPLETED":
        raise _PostCloseCLIError("PAPER_DAY_NOT_COMPLETED", retryable=True)

    target_hash = paper_day_target_hash(
        channel="onebot",
        target_kind=target_kind.value,
        target_id=target_id,
    )
    journal_path = session_root / "journal.sqlite3"
    if not journal_path.is_file():
        raise _PostCloseCLIError("PAPER_DAY_JOURNAL_NOT_AVAILABLE", retryable=True)
    with SQLitePaperDayStore(journal_path) as day_store:
        scoped = tuple(
            run
            for run in day_store.list_runs()
            if run.session_date == session_date and run.account_id == account_id.strip()
        )
    if len(scoped) != 1:
        raise _PostCloseCLIError("PAPER_DAY_RUN_SCOPE_CONFLICT")
    if scoped[0].target_hash != target_hash:
        raise _PostCloseCLIError("NOTIFICATION_TARGET_CONFLICT")

    config = {
        "account_id": account_id.strip(),
        "base_url": base_url,
        "candidate_db": str(Path(candidate_db).resolve()),
        "history_days": history_days,
        "macro_enabled": macro_enabled,
        "macro_provider": macro_provider,
        "macro_weight": format(macro_weight, "f"),
        "market_evidence_dir": str(Path(market_evidence_dir).resolve()),
        "model": model,
        "news_feeds": list(news_feeds or ()),
        "news_runtime_dir": str(Path(news_runtime_dir).resolve()),
        "refresh_news": refresh_news,
        "research_db": str(Path(research_db).resolve()),
        "schema_version": 1,
        "search_discovery": search_discovery,
        "searxng_url": (
            None if validated_searxng_url is None else validated_searxng_url.manifest_document
        ),
        "session_date": session_date.isoformat(),
        "target_hash": target_hash,
        "target_kind": target_kind.value,
    }
    config_json = json.dumps(
        config,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    config_sha256 = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
    run_id = hashlib.sha256(
        f"ashare-post-close@1\0{session_date.isoformat()}\0{config_sha256}".encode()
    ).hexdigest()
    post_root = session_root / "post-close"
    manifest_path = post_root / "manifest.json"
    status_path = post_root / "status.json"
    audit_path = post_root / "audit.jsonl"
    expected_manifest: dict[str, object] = {
        "config": config,
        "config_sha256": config_sha256,
        "run_id": run_id,
        "schema_version": 1,
    }

    post_root.mkdir(parents=True, exist_ok=True)
    with _post_close_process_lock(post_root / "run.lock"):
        if manifest_path.is_file():
            retained_manifest = _read_post_close_json(
                manifest_path,
                "POST_CLOSE_MANIFEST_INVALID",
            )
            if retained_manifest != expected_manifest:
                raise _PostCloseCLIError("POST_CLOSE_MANIFEST_CONFLICT")
        else:
            _atomic_write_cli_json(manifest_path, expected_manifest)
        status = (
            _read_post_close_json(status_path, "POST_CLOSE_STATUS_INVALID")
            if status_path.is_file()
            else {
                "attempt": 0,
                "phase": "NEW",
                "run_id": run_id,
                "schema_version": 1,
                "session_date": session_date.isoformat(),
                "target_hash": target_hash,
            }
        )
        if status.get("run_id") != run_id or status.get("target_hash") != target_hash:
            raise _PostCloseCLIError("POST_CLOSE_STATUS_CONFLICT")
        phase = status.get("phase")
        if phase == "COMPLETE":
            return _post_close_completed_result(post_root, status, idempotent_replay=True)
        if phase in {"ANALYSIS_IN_PROGRESS", "ANALYSIS_AMBIGUOUS"}:
            if not recover_analysis:
                raise _PostCloseCLIError("POST_CLOSE_ANALYSIS_RECOVERY_REQUIRES_OPERATOR")
            _append_post_close_audit(
                audit_path,
                event="ANALYSIS_RECOVERY_AUTHORIZED",
                run_id=run_id,
                target_hash=target_hash,
                details={
                    "authorization": "EXPLICIT_RECOVER_ANALYSIS_FLAG",
                    "prior_phase": phase,
                },
            )
            status.update(
                {
                    "error_code": None,
                    "phase": "NEW",
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
            for key in (
                "analysis_outcome",
                "artifact_path",
                "artifact_sha256",
                "held_count",
                "next_session",
                "paper_run_id",
                "research_completed",
                "research_failed",
            ):
                status.pop(key, None)
            _atomic_write_cli_json(status_path, status)
            phase = "NEW"
        if phase in {"DELIVERY_IN_PROGRESS", "DELIVERY_AMBIGUOUS"}:
            if not recover_delivery:
                raise _PostCloseCLIError("POST_CLOSE_DELIVERY_RECOVERY_REQUIRES_OPERATOR")
            _append_post_close_audit(
                audit_path,
                event="DELIVERY_RECOVERY_AUTHORIZED",
                run_id=run_id,
                target_hash=target_hash,
                details={
                    "authorization": "EXPLICIT_RECOVER_DELIVERY_FLAG",
                    "prior_phase": phase,
                },
            )
            status.update(
                {
                    "artifact_delivery": "PENDING",
                    "delivery_recovery_authorized": True,
                    "error_code": None,
                    "phase": "DELIVERY_PENDING",
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
            _atomic_write_cli_json(status_path, status)
            phase = "DELIVERY_PENDING"
        if phase == "DELIVERY_FAILED":
            raise _PostCloseCLIError("POST_CLOSE_TEXT_DELIVERY_FAILED")
        if phase not in {
            "NEW",
            "ANALYSIS_FAILED",
            "ANALYSIS_FAILED_RETRYABLE",
            "ANALYSIS_COMPLETE",
            "DELIVERY_PENDING",
        }:
            raise _PostCloseCLIError("POST_CLOSE_STATUS_INVALID")

        artifact_path: Path | None = None
        if phase in {"ANALYSIS_COMPLETE", "DELIVERY_PENDING"}:
            artifact_value = status.get("artifact_path")
            artifact_sha256 = status.get("artifact_sha256")
            if not isinstance(artifact_value, str) or not isinstance(artifact_sha256, str):
                raise _PostCloseCLIError("POST_CLOSE_STATUS_INVALID")
            artifact_path = _validated_post_close_artifact(
                session_root,
                Path(artifact_value),
                artifact_sha256,
            )
        else:
            status.update(
                {
                    "attempt": (_post_close_optional_integer(status.get("attempt")) or 0) + 1,
                    "phase": "ANALYSIS_IN_PROGRESS",
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
            _atomic_write_cli_json(status_path, status)
            _append_post_close_audit(
                audit_path,
                event="ANALYSIS_STARTED",
                run_id=run_id,
                target_hash=target_hash,
            )
            ledger_path = session_root / "ledger.sqlite3"
            if not ledger_path.is_file():
                status.update(
                    {
                        "error_code": "PAPER_LEDGER_NOT_AVAILABLE",
                        "phase": "ANALYSIS_FAILED_RETRYABLE",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise _PostCloseCLIError(
                    "PAPER_LEDGER_NOT_AVAILABLE",
                    retryable=True,
                )
            try:
                with SQLitePaperLedger(ledger_path) as ledger:
                    paper = ASharePaperTradingService(ledger)
                    account = paper.snapshot(account_id.strip())
                    instrument_types = {
                        position.symbol: position.instrument_type.value.lower()
                        for position in account.positions
                        if position.quantity > 0
                    }
                    profiles = _paper_session_instrument_profiles(
                        projection,
                        instrument_types,
                    )
                    candidate_path = Path(candidate_db).resolve()
                    if candidate_path.is_file():
                        with SQLiteCandidateStore(candidate_path) as candidates:
                            result = await _run_post_close_orchestrator(
                                sessions=sessions,
                                projection=projection,
                                paper=paper,
                                candidates=candidates,
                                profiles=profiles,
                                session_root=session_root,
                                account_id=account_id.strip(),
                                history_days=history_days,
                                news_runtime_dir=news_runtime_dir,
                                research_db=research_db,
                                market_evidence_dir=market_evidence_dir,
                                analysis_outbox_db=str(post_root / "analysis-outbox.sqlite3"),
                                news_feeds=news_feeds,
                                refresh_news=refresh_news,
                                search_discovery=search_discovery,
                                searxng_url=(
                                    None
                                    if validated_searxng_url is None
                                    else validated_searxng_url.runtime_url
                                ),
                                macro_enabled=macro_enabled,
                                macro_provider=macro_provider,
                                model=model,
                                macro_weight=macro_weight,
                                now=now,
                            )
                    else:
                        result = await _run_post_close_orchestrator(
                            sessions=sessions,
                            projection=projection,
                            paper=paper,
                            candidates=None,
                            profiles=profiles,
                            session_root=session_root,
                            account_id=account_id.strip(),
                            history_days=history_days,
                            news_runtime_dir=news_runtime_dir,
                            research_db=research_db,
                            market_evidence_dir=market_evidence_dir,
                            analysis_outbox_db=str(post_root / "analysis-outbox.sqlite3"),
                            news_feeds=news_feeds,
                            refresh_news=refresh_news,
                            search_discovery=search_discovery,
                            searxng_url=(
                                None
                                if validated_searxng_url is None
                                else validated_searxng_url.runtime_url
                            ),
                            macro_enabled=macro_enabled,
                            macro_provider=macro_provider,
                            model=model,
                            macro_weight=macro_weight,
                            now=now,
                        )
            except asyncio.CancelledError:
                raise
            except PostCloseOrchestrationError as error:
                status.update(
                    {
                        "error_code": error.code,
                        "phase": "ANALYSIS_FAILED",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise _PostCloseCLIError(error.code) from None
            except _PostCloseCLIError:
                raise
            except Exception:
                status.update(
                    {
                        "error_code": "POST_CLOSE_ANALYSIS_FAILED",
                        "phase": "ANALYSIS_AMBIGUOUS",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise _PostCloseCLIError("POST_CLOSE_ANALYSIS_FAILED") from None

            artifact_path = _validated_post_close_artifact(
                session_root,
                result.artifact_path,
                _post_close_sha256(result.artifact_path),
            )
            artifact_digest = _post_close_sha256(artifact_path)
            held_count = len(result.review.held_symbols)
            research_completed = sum(
                item.status.value == "COMPLETED" for item in result.review.research
            )
            research_failed = sum(item.status.value == "FAILED" for item in result.review.research)
            try:
                analysis_outcome = _post_close_analysis_outcome(
                    held_count=held_count,
                    research_completed=research_completed,
                    research_failed=research_failed,
                )
            except _PostCloseCLIError as error:
                status.update(
                    {
                        "error_code": error.code,
                        "phase": "ANALYSIS_FAILED",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise
            if analysis_outcome == "FAILED":
                status.update(
                    {
                        "analysis_outcome": "FAILED",
                        "artifact_path": str(artifact_path),
                        "artifact_sha256": artifact_digest,
                        "error_code": "ALL_HELD_RESEARCH_FAILED",
                        "held_count": held_count,
                        "next_session": result.review.next_session.isoformat(),
                        "paper_run_id": result.review.paper_run_id,
                        "phase": "ANALYSIS_FAILED_RETRYABLE",
                        "research_completed": research_completed,
                        "research_failed": research_failed,
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                _append_post_close_audit(
                    audit_path,
                    event="ANALYSIS_FAILED",
                    run_id=run_id,
                    target_hash=target_hash,
                    details={
                        "error_code": "ALL_HELD_RESEARCH_FAILED",
                        "held_count": held_count,
                        "research_failed": research_failed,
                    },
                )
                raise _PostCloseCLIError(
                    "ALL_HELD_RESEARCH_FAILED",
                    retryable=True,
                )
            status.update(
                {
                    "analysis_outcome": analysis_outcome,
                    "artifact_path": str(artifact_path),
                    "artifact_sha256": artifact_digest,
                    "held_count": held_count,
                    "next_session": result.review.next_session.isoformat(),
                    "paper_run_id": result.review.paper_run_id,
                    "phase": "ANALYSIS_COMPLETE",
                    "research_completed": research_completed,
                    "research_failed": research_failed,
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
            _atomic_write_cli_json(status_path, status)
            _append_post_close_audit(
                audit_path,
                event="ANALYSIS_COMPLETED",
                run_id=run_id,
                target_hash=target_hash,
                details={
                    "analysis_outcome": analysis_outcome,
                    "artifact_sha256": artifact_digest,
                    "research_completed": research_completed,
                    "research_failed": research_failed,
                },
            )

        assert artifact_path is not None
        delivered = await _deliver_post_close_artifact(
            post_root=post_root,
            status=status,
            status_path=status_path,
            audit_path=audit_path,
            run_id=run_id,
            target_hash=target_hash,
            target_kind=target_kind,
            target_id=target_id,
            base_url=base_url,
            artifact_path=artifact_path,
            dispatch_cycles=dispatch_cycles,
            dispatch_poll_interval=dispatch_poll_interval,
        )
        if not delivered:
            raise _PostCloseCLIError("POST_CLOSE_DELIVERY_PENDING", retryable=True)
        return _post_close_completed_result(post_root, status, idempotent_replay=False)


async def _run_post_close_orchestrator(
    *,
    sessions: CloseSessionResolution,
    projection: PaperDayExecutiveProjection,
    paper: ASharePaperTradingService,
    candidates: SQLiteCandidateStore | None,
    profiles: Mapping[str, ResearchInstrumentProfile],
    session_root: Path,
    account_id: str,
    history_days: int,
    news_runtime_dir: str,
    research_db: str,
    market_evidence_dir: str,
    analysis_outbox_db: str,
    news_feeds: Sequence[str] | None,
    refresh_news: bool,
    search_discovery: bool,
    searxng_url: str | None,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    macro_weight: Decimal,
    now: datetime,
) -> PostCloseOrchestrationResult:
    from gribuki_trade.services.ashare.close.ashare_post_close import (
        ASharePostCloseOrchestrator,
        PostCloseOrchestrationRequest,
    )

    close_research = _CLIExistingCloseResearch(
        profiles=profiles,
        history_days=history_days,
        news_runtime_dir=news_runtime_dir,
        research_db=research_db,
        market_evidence_dir=market_evidence_dir,
        analysis_outbox_db=analysis_outbox_db,
        news_feeds=news_feeds,
        refresh_news=refresh_news,
        search_discovery=search_discovery,
        searxng_url=searxng_url,
        macro_enabled=macro_enabled,
        macro_provider=macro_provider,
        model=model,
        macro_weight=macro_weight,
    )
    orchestrator = ASharePostCloseOrchestrator(
        session_resolver=_FixedPostCloseSessions(sessions),
        paper_account=paper,
        close_research=close_research,
        candidate_reader=candidates,
        sidecar_loader=lambda _path: projection,
        research_timeout_seconds=600.0,
    )
    return await orchestrator.run_once(
        PostCloseOrchestrationRequest(
            session_root=session_root,
            account_id=account_id,
            now=now,
        )
    )


def _validate_post_close_target(
    base_url: str,
    target_kind: object,
    target_id: str,
) -> None:
    from gribuki_trade.adapters.notifiers import OneBotConfig
    from gribuki_trade.ports.notifier import NotificationTargetKind

    if not isinstance(target_kind, NotificationTargetKind):
        raise TypeError("target_kind must be a NotificationTargetKind")
    allowlist = frozenset({target_id})
    try:
        OneBotConfig(
            access_token="validation-only-not-a-credential",
            base_url=base_url,
            private_target_ids=(
                allowlist if target_kind is NotificationTargetKind.PRIVATE else frozenset()
            ),
            group_target_ids=(
                allowlist if target_kind is NotificationTargetKind.GROUP else frozenset()
            ),
        )
    except (TypeError, ValueError):
        raise _PostCloseCLIError("NOTIFICATION_TARGET_INVALID") from None


def _validated_post_close_artifact(
    session_root: Path,
    artifact_path: Path,
    expected_sha256: str,
) -> Path:
    report_root_input = session_root / "reports"
    try:
        if report_root_input.is_symlink() or artifact_path.is_symlink():
            raise ValueError
        report_root = report_root_input.resolve(strict=True)
        resolved = artifact_path.resolve(strict=True)
        resolved.relative_to(report_root)
    except (OSError, RuntimeError, ValueError):
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID") from None
    if not resolved.is_file() or resolved.suffix.casefold() != ".md":
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID")
    if _post_close_sha256(resolved) != expected_sha256:
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_DIGEST_MISMATCH")
    return resolved


def _post_close_delivery_summary(
    text: str,
    *,
    artifact_name: str,
    artifact_sha256: str,
    run_id: str,
) -> str:
    """从已验约的盘后日报提取短摘要；长文始终以 Markdown 文件交付。"""

    from gribuki_trade.reporting.contracts import (
        ReportKind,
        render_stable_text_report,
        report_contract,
        validate_markdown_report_contract,
    )

    try:
        validate_markdown_report_contract(ReportKind.DAILY_REVIEW, text)
    except (TypeError, ValueError):
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID") from None
    contract = report_contract(ReportKind.DAILY_REVIEW)
    sections = {
        name: _post_close_section_excerpt(text, name) for name in contract.required_sections
    }
    sections["执行摘要"] = (
        f"{sections['执行摘要']}\n完整报告文件：{artifact_name}；"
        f"内容摘要：{artifact_sha256[:12]}；运行标识：{run_id[-12:]}。"
    )
    summary = render_stable_text_report(
        ReportKind.DAILY_REVIEW,
        title="A股盘后日报交付摘要",
        sections=sections,
    )
    if len(summary) > 7_000:
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID")
    return summary


def _post_close_section_excerpt(text: str, name: str, limit: int = 850) -> str:
    """提取一个必需二级章节的开头，避免 QQ 摘要复制整份长报告。"""

    marker = f"\n## {name}\n"
    start = text.find(marker)
    if start < 0:
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID")
    content_start = start + len(marker)
    next_heading = text.find("\n## ", content_start)
    content_end = len(text) if next_heading < 0 else next_heading
    meaningful = tuple(
        line.strip() for line in text[content_start:content_end].splitlines() if line.strip()
    )
    if not meaningful:
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID")
    excerpt = "\n".join(meaningful)
    if len(excerpt) <= limit:
        return excerpt
    shortened = excerpt[: limit - 1]
    last_break = shortened.rfind("\n")
    if last_break >= limit // 2:
        shortened = shortened[:last_break]
    return shortened.rstrip() + "…"


async def _deliver_post_close_artifact(
    *,
    post_root: Path,
    status: dict[str, object],
    status_path: Path,
    audit_path: Path,
    run_id: str,
    target_hash: str,
    target_kind: object,
    target_id: str,
    base_url: str,
    artifact_path: Path,
    dispatch_cycles: int,
    dispatch_poll_interval: float,
) -> bool:
    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.ports.notifier import (
        NotificationTargetKind,
        OutboundNotification,
    )
    from gribuki_trade.services.communications.notification_dispatch import (
        NotificationDispatchService,
    )
    from gribuki_trade.storage.execution.outbox import OutboxStatus, SQLiteOutbox

    if not isinstance(target_kind, NotificationTargetKind):
        raise TypeError("target_kind must be a NotificationTargetKind")
    if status.get("artifact_delivery") == "IN_PROGRESS":
        raise _PostCloseCLIError("POST_CLOSE_RECOVERY_REQUIRES_OPERATOR")
    try:
        report_text = artifact_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_INVALID") from None
    artifact_sha256 = _post_close_sha256(artifact_path)
    summary = _post_close_delivery_summary(
        report_text,
        artifact_name=artifact_path.name,
        artifact_sha256=artifact_sha256,
        run_id=run_id,
    )
    messages = (summary,)
    created_value = status.get("delivery_created_at")
    if isinstance(created_value, str):
        try:
            created_at = datetime.fromisoformat(created_value).astimezone(UTC)
        except ValueError:
            raise _PostCloseCLIError("POST_CLOSE_STATUS_INVALID") from None
    else:
        created_at = datetime.now(UTC)
        status["delivery_created_at"] = created_at.isoformat()
    expires_at = created_at + timedelta(hours=24)
    outbox_path = post_root / "delivery-outbox.sqlite3"
    keys = tuple(
        f"post-close:{run_id}:summary:{index:02d}-of-{len(messages):02d}"
        for index in range(1, len(messages) + 1)
    )
    with SQLiteOutbox(outbox_path) as outbox:
        for key, message in zip(keys, messages, strict=True):
            outbox.enqueue(
                OutboundNotification(
                    idempotency_key=key,
                    channel="onebot",
                    target_kind=target_kind,
                    target_id=target_id,
                    text=message,
                    created_at=created_at,
                    expires_at=expires_at,
                )
            )
        status.update(
            {
                "phase": "DELIVERY_PENDING",
                "text_part_count": len(messages),
                "updated_at": datetime.now(UTC).isoformat(),
            }
        )
        _atomic_write_cli_json(status_path, status)
        retained_before_dispatch = tuple(outbox.get_by_key(key) for key in keys)
        if any(item is None for item in retained_before_dispatch):
            raise _PostCloseCLIError("POST_CLOSE_OUTBOX_INTEGRITY_ERROR")
        if (
            any(
                item is not None and item.status is OutboxStatus.IN_FLIGHT
                for item in retained_before_dispatch
            )
            and status.get("delivery_recovery_authorized") is not True
        ):
            status.update(
                {
                    "error_code": "POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS",
                    "phase": "DELIVERY_AMBIGUOUS",
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
            _atomic_write_cli_json(status_path, status)
            _append_post_close_audit(
                audit_path,
                event="DELIVERY_AMBIGUOUS",
                run_id=run_id,
                target_hash=target_hash,
                details={"error_code": "POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS"},
            )
            raise _PostCloseCLIError("POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS")
        try:
            access_token = _required_local_secret(NAPCAT_ACCESS_TOKEN_SECRET)
        except (RuntimeError, SecretProviderError):
            _append_post_close_audit(
                audit_path,
                event="DELIVERY_DEFERRED",
                run_id=run_id,
                target_hash=target_hash,
                details={"error_code": "NAPCAT_TOKEN_NOT_CONFIGURED"},
            )
            return False
        allowlist = frozenset({target_id})
        config = OneBotConfig(
            access_token=access_token,
            base_url=base_url,
            private_target_ids=(
                allowlist if target_kind is NotificationTargetKind.PRIVATE else frozenset()
            ),
            group_target_ids=(
                allowlist if target_kind is NotificationTargetKind.GROUP else frozenset()
            ),
            artifact_root=artifact_path.parent,
        )
        async with OneBotNotifier(config) as notifier:
            if not await _post_close_napcat_ready(notifier):
                # NapCat 的启动和登录必须由 GUI 显式完成。盘后流程只观察
                # 健康状态并保留 durable outbox，绝不偷偷启动 OS 侧车。
                _append_post_close_audit(
                    audit_path,
                    event="DELIVERY_DEFERRED",
                    run_id=run_id,
                    target_hash=target_hash,
                    details={"error_code": "NAPCAT_NOT_READY_GUI_ACTION_REQUIRED"},
                )
                return False
            service = NotificationDispatchService(
                outbox,
                {notifier.channel: notifier},
                target_kind=target_kind,
                target_id=target_id,
            )
            try:
                await service.poll(
                    max_cycles=dispatch_cycles,
                    poll_interval=dispatch_poll_interval,
                )
            except asyncio.CancelledError:
                status.update(
                    {
                        "error_code": "POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS",
                        "phase": "DELIVERY_AMBIGUOUS",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise
            except Exception:
                status.update(
                    {
                        "error_code": "POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS",
                        "phase": "DELIVERY_AMBIGUOUS",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise _PostCloseCLIError("POST_CLOSE_TEXT_DELIVERY_AMBIGUOUS") from None
            retained = tuple(outbox.get_by_key(key) for key in keys)
            if any(item is None for item in retained):
                raise _PostCloseCLIError("POST_CLOSE_OUTBOX_INTEGRITY_ERROR")
            statuses = tuple(item.status for item in retained if item is not None)
            if any(item in {OutboxStatus.DEAD, OutboxStatus.EXPIRED} for item in statuses):
                status.update(
                    {
                        "error_code": "POST_CLOSE_TEXT_DELIVERY_FAILED",
                        "phase": "DELIVERY_FAILED",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                raise _PostCloseCLIError("POST_CLOSE_TEXT_DELIVERY_FAILED")
            if any(item is not OutboxStatus.SENT for item in statuses):
                return False

            if status.get("artifact_delivery") != "SENT":
                status.update(
                    {
                        "artifact_delivery": "IN_PROGRESS",
                        "phase": "DELIVERY_IN_PROGRESS",
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
                _atomic_write_cli_json(status_path, status)
                _append_post_close_audit(
                    audit_path,
                    event="ARTIFACT_DELIVERY_STARTED",
                    run_id=run_id,
                    target_hash=target_hash,
                    details={"artifact_sha256": artifact_sha256},
                )
                try:
                    receipt = (
                        await notifier.upload_private_file(target_id, artifact_path.name)
                        if target_kind is NotificationTargetKind.PRIVATE
                        else await notifier.upload_group_file(target_id, artifact_path.name)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    status.update(
                        {
                            "artifact_delivery": "AMBIGUOUS",
                            "error_code": "POST_CLOSE_ARTIFACT_DELIVERY_AMBIGUOUS",
                            "phase": "DELIVERY_AMBIGUOUS",
                            "updated_at": datetime.now(UTC).isoformat(),
                        }
                    )
                    _atomic_write_cli_json(status_path, status)
                    raise _PostCloseCLIError("POST_CLOSE_ARTIFACT_DELIVERY_AMBIGUOUS") from None
                status.update(
                    {
                        "artifact_delivery": "SENT",
                        "artifact_provider_file_id": receipt.provider_file_id,
                    }
                )

    status.update(
        {
            "completed_at": datetime.now(UTC).isoformat(),
            "error_code": None,
            "phase": "COMPLETE",
            "updated_at": datetime.now(UTC).isoformat(),
        }
    )
    _atomic_write_cli_json(status_path, status)
    _append_post_close_audit(
        audit_path,
        event="DELIVERY_COMPLETED",
        run_id=run_id,
        target_hash=target_hash,
        details={"text_part_count": len(messages)},
    )
    return True


async def _post_close_napcat_ready(notifier: object) -> bool:
    try:
        status = await notifier.get_status()  # type: ignore[attr-defined]
    except asyncio.CancelledError:
        raise
    except Exception:
        return False
    return (
        isinstance(status, Mapping) and status.get("good") is True and status.get("online") is True
    )


def _ashare_post_close_status(
    post_root: Path,
    session_date: date,
) -> dict[str, object]:
    status_path = post_root / "status.json"
    if not status_path.is_file():
        return _post_close_error(
            "status",
            session_date,
            post_root,
            "POST_CLOSE_STATUS_NOT_AVAILABLE",
        )
    try:
        status = _read_post_close_json(status_path, "POST_CLOSE_STATUS_INVALID")
    except _PostCloseCLIError as error:
        return _post_close_error("status", session_date, post_root, error.code)
    return {
        "action": "status",
        "ok": True,
        "read_only_sidecar": True,
        "runtime_dir": str(post_root),
        "status": status,
        "status_path": str(status_path.resolve()),
    }


def _ashare_post_close_report(
    post_root: Path,
    session_date: date,
) -> dict[str, object]:
    status_path = post_root / "status.json"
    if not status_path.is_file():
        return _post_close_error(
            "report",
            session_date,
            post_root,
            "POST_CLOSE_REPORT_NOT_AVAILABLE",
        )
    try:
        status = _read_post_close_json(status_path, "POST_CLOSE_STATUS_INVALID")
        path_value = status.get("artifact_path")
        digest = status.get("artifact_sha256")
        if not isinstance(path_value, str) or not isinstance(digest, str):
            raise _PostCloseCLIError("POST_CLOSE_REPORT_NOT_AVAILABLE")
        report_path = _validated_post_close_artifact(
            post_root.parent,
            Path(path_value),
            digest,
        )
        content = report_path.read_bytes()
        content.decode("utf-8")
    except (_PostCloseCLIError, OSError, UnicodeError) as error:
        code = (
            error.code if isinstance(error, _PostCloseCLIError) else "POST_CLOSE_ARTIFACT_INVALID"
        )
        return _post_close_error("report", session_date, post_root, code)
    return {
        "action": "report",
        "ok": True,
        "read_only_sidecar": True,
        "report": {
            "bytes": len(content),
            "name": report_path.name,
            "path": str(report_path),
            "sha256": digest,
        },
        "runtime_dir": str(post_root),
    }
