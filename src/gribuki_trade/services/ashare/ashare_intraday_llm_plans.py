"""供 PAPER 日内 LLM 门使用的只读 PIT 证据与同步计划。

本模块有意不执行提供方 I/O。CLI 在运行器启动前加载一份不可变事件快照；
后续每次监看刷新只把该快照与当前候选元数据转换为 ``MacroResearchPlan``。
执行计划仍由协调器在后台负责。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from gribuki_trade.analysis.schemas import MacroAnalysisDecision
from gribuki_trade.domain.events import NormalizedEvent, SourceTier
from gribuki_trade.features.ashare_surveillance import IntradayCandidate
from gribuki_trade.services.ashare.ashare_intraday_llm import (
    IntradayLLMConfig,
    IntradayLLMCoordinator,
    IntradayLLMReview,
    JournaledIntradayLLMReview,
    intraday_llm_document_sha256,
)
from gribuki_trade.services.ashare.ashare_paper_day import PaperDayLLMPreopenContext
from gribuki_trade.services.ashare.ashare_surveillance import AShareSurveillanceRun
from gribuki_trade.services.macro_research import (
    MacroResearchPlan,
    MacroResearchRun,
    MacroResearchService,
)

_PREOPEN_ANCHOR_SYMBOL = "510300.SH"
_SNAPSHOT_SOURCE = "SQLITE_EVENT_STORE_READ_ONLY_PIT"
_PREOPEN_HORIZON = "ASHARE_PAPER_PREOPEN_SESSION_BASELINE"
_INTRADAY_HORIZON = "INTRADAY_BACKGROUND_REVIEW"


@dataclass(frozen=True, slots=True)
class FrozenPITEventSnapshot:
    """在不写数据库的情况下捕获的一份不可变最新版本视图。"""

    as_of: datetime
    events: tuple[NormalizedEvent, ...]
    snapshot_sha256: str
    database_path_sha256: str
    status: str = "READY"
    failure_code: str | None = None

    def __post_init__(self) -> None:
        as_of = _aware_utc(self.as_of, "as_of")
        if self.status not in {"READY", "UNAVAILABLE"}:
            raise ValueError("unsupported PIT evidence snapshot status")
        if (self.status == "READY") == (self.failure_code is not None):
            raise ValueError("snapshot status and failure_code disagree")
        if self.status == "UNAVAILABLE" and self.events:
            raise ValueError("an unavailable snapshot cannot contain events")
        if any(event.first_seen_at > as_of or event.available_at > as_of for event in self.events):
            raise ValueError("snapshot contains evidence unavailable at as_of")
        identifiers = tuple(event.revision_id for event in self.events)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("snapshot event revisions must be unique")
        for name in ("snapshot_sha256", "database_path_sha256"):
            value = getattr(self, name)
            if len(value) != 64 or any(item not in "0123456789abcdef" for item in value):
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")
        expected = _snapshot_sha256(self.events, as_of=as_of, status=self.status)
        if self.snapshot_sha256 != expected:
            raise ValueError("snapshot_sha256 does not match PIT evidence")
        object.__setattr__(self, "as_of", as_of)

    @property
    def available(self) -> bool:
        return self.status == "READY" and bool(self.events)

    def audit_document(self) -> dict[str, object]:
        """返回不含标题或来源正文、可安全写入清单的身份。"""

        return {
            "as_of": self.as_of,
            "database_path_sha256": self.database_path_sha256,
            "event_count": len(self.events),
            "failure_code": self.failure_code,
            "snapshot_sha256": self.snapshot_sha256,
            "source": _SNAPSHOT_SOURCE,
            "status": self.status,
        }


def load_frozen_pit_event_snapshot(
    path: Path,
    *,
    as_of: datetime,
    limit: int = 2_000,
) -> FrozenPITEventSnapshot:
    """通过只读 URI 读取 ``SQLiteEventStore.latest_as_of`` 语义。

    打开常规存储会初始化模式和 WAL pragma。生产 PAPER 启动只需要冻结观察，
    因此该路径使用 SQLite ``mode=ro`` 与 ``query_only``，绝不创建或修改数据库。
    提供方或 SQLite 异常字符串绝不会越过此边界。
    """

    cutoff = _aware_utc(as_of, "as_of")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")
    resolved = path.resolve()
    path_sha256 = hashlib.sha256(str(resolved).encode("utf-8")).hexdigest()
    if not resolved.is_file():
        return _unavailable_snapshot(
            cutoff,
            path_sha256,
            "LLM_EVIDENCE_DB_NOT_FOUND",
        )
    wal_path = Path(f"{resolved}-wal")
    try:
        if wal_path.is_file() and wal_path.stat().st_size > 0:
            # 不可变读取会有意忽略 WAL 内容。这里拒绝可能过期的快照，
            # 而不是静默遗漏已提交内容。
            return _unavailable_snapshot(
                cutoff,
                path_sha256,
                "LLM_EVIDENCE_DB_WAL_NOT_CHECKPOINTED",
            )
    except OSError:
        return _unavailable_snapshot(
            cutoff,
            path_sha256,
            "LLM_EVIDENCE_DB_UNREADABLE",
        )

    # ``immutable=1`` 连读取侧 WAL/SHM 都不会创建。它仅在文档约定的启动快照
    # 边界内安全：PAPER 启动前必须停止采集器并关闭其写连接。
    uri = f"{resolved.as_uri()}?mode=ro&immutable=1"
    try:
        connection = sqlite3.connect(
            uri,
            uri=True,
            timeout=5.0,
            isolation_level=None,
        )
    except sqlite3.Error:
        return _unavailable_snapshot(
            cutoff,
            path_sha256,
            "LLM_EVIDENCE_DB_UNREADABLE",
        )
    try:
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA query_only = ON")
            rows = connection.execute(
                """
                SELECT visible.* FROM normalized_event_revisions AS visible
                WHERE visible.first_seen_at <= ?
                  AND visible.available_at <= ?
                  AND visible.revision_number = (
                      SELECT MAX(candidate.revision_number)
                      FROM normalized_event_revisions AS candidate
                      WHERE candidate.event_id = visible.event_id
                        AND candidate.first_seen_at <= ?
                        AND candidate.available_at <= ?
                  )
                ORDER BY visible.available_at DESC, visible.event_id
                LIMIT ?
                """,
                (
                    cutoff.isoformat(),
                    cutoff.isoformat(),
                    cutoff.isoformat(),
                    cutoff.isoformat(),
                    limit,
                ),
            ).fetchall()
        finally:
            connection.close()
    except sqlite3.OperationalError:
        return _unavailable_snapshot(
            cutoff,
            path_sha256,
            "LLM_EVIDENCE_DB_INVALID",
        )
    except sqlite3.Error:
        return _unavailable_snapshot(
            cutoff,
            path_sha256,
            "LLM_EVIDENCE_DB_INVALID",
        )

    try:
        events = tuple(_row_to_event(row) for row in rows)
    except (TypeError, ValueError, json.JSONDecodeError):
        return _unavailable_snapshot(
            cutoff,
            path_sha256,
            "LLM_EVIDENCE_SNAPSHOT_INVALID",
        )
    status = "READY" if events else "UNAVAILABLE"
    failure_code = None if events else "LLM_EVIDENCE_SNAPSHOT_EMPTY"
    return FrozenPITEventSnapshot(
        as_of=cutoff,
        events=events,
        snapshot_sha256=_snapshot_sha256(events, as_of=cutoff, status=status),
        database_path_sha256=path_sha256,
        status=status,
        failure_code=failure_code,
    )


def replay_frozen_pit_event_snapshot(
    path: Path,
    *,
    retained_audit: Mapping[str, object],
    fallback_as_of: datetime,
    limit: int = 2_000,
) -> FrozenPITEventSnapshot:
    """精确重建清单绑定的快照，否则返回封闭失败状态。"""

    fallback = _aware_utc(fallback_as_of, "fallback_as_of")
    try:
        raw_as_of = retained_audit.get(
            "evidence_as_of",
            retained_audit.get("as_of"),
        )
        if raw_as_of is None:
            raise ValueError("retained evidence audit has no as_of")
        retained_as_of = _aware_utc(
            raw_as_of
            if isinstance(raw_as_of, datetime)
            else datetime.fromisoformat(str(raw_as_of)),
            "retained evidence as_of",
        )
    except (KeyError, TypeError, ValueError):
        return _unavailable_snapshot(
            fallback,
            _path_sha256(path),
            "LLM_EVIDENCE_SNAPSHOT_REPLAY_MISMATCH",
        )
    replayed = load_frozen_pit_event_snapshot(
        path,
        as_of=retained_as_of,
        limit=limit,
    )
    if not _snapshot_audit_matches(replayed, retained_audit):
        return _unavailable_snapshot(
            retained_as_of,
            _path_sha256(path),
            "LLM_EVIDENCE_SNAPSHOT_REPLAY_MISMATCH",
        )
    return replayed


class ReplayGuardedIntradayLLMCoordinator(IntradayLLMCoordinator):
    """冻结证据无法重放时，拒绝恢复可交易复核。"""

    def __init__(
        self,
        research: MacroResearchService,
        *,
        config: IntradayLLMConfig,
        journal_restore_allowed: bool,
    ) -> None:
        if not isinstance(journal_restore_allowed, bool):
            raise TypeError("journal_restore_allowed must be bool")
        super().__init__(research, config=config)
        self._journal_restore_allowed = journal_restore_allowed

    def restore_journaled(
        self,
        review: IntradayLLMReview,
        *,
        accepted_at: datetime,
        journal_event_id: str,
        journal_event_sha256: str,
    ) -> JournaledIntradayLLMReview:
        if not self._journal_restore_allowed:
            raise ValueError("frozen LLM evidence snapshot replay is unavailable")
        return super().restore_journaled(
            review,
            accepted_at=accepted_at,
            journal_event_id=journal_event_id,
            journal_event_sha256=journal_event_sha256,
        )


class FrozenPITIntradayLLMPlanFactory:
    """依据一组不可变证据准备盘前及逐次扫描计划。"""

    def __init__(
        self,
        research: MacroResearchService,
        snapshot: FrozenPITEventSnapshot,
        *,
        retained_audit: Mapping[str, object] | None = None,
    ) -> None:
        self._research = research
        self._snapshot = snapshot
        self._audit = (
            _factory_audit_document(snapshot)
            if retained_audit is None
            else dict(retained_audit)
        )
        if not isinstance(self._audit.get("snapshot_id"), str) or not str(
            self._audit["snapshot_id"]
        ).strip():
            raise ValueError("retained snapshot audit requires snapshot_id")
        if "evidence_as_of" not in self._audit:
            raise ValueError("retained snapshot audit requires evidence_as_of")

    @property
    def snapshot(self) -> FrozenPITEventSnapshot:
        return self._snapshot

    @property
    def available(self) -> bool:
        return self._snapshot.available

    def audit_document(self) -> dict[str, object]:
        return dict(self._audit)

    def prepare_preopen(self, *, session_date: date) -> MacroResearchPlan:
        return self._research.prepare(
            symbol=_PREOPEN_ANCHOR_SYMBOL,
            as_of=self._snapshot.as_of,
            horizon=_PREOPEN_HORIZON,
            technical_summary=(
                "ANALYSIS_PROFILE=ASHARE_PAPER_PREOPEN_MACRO_V1",
                f"TARGET_SESSION={session_date.isoformat()}",
                "EVIDENCE_SNAPSHOT=FROZEN_POINT_IN_TIME",
                "MODEL_HAS_NO_ORDER_PRICE_QUANTITY_OR_BROKER_AUTHORITY",
            ),
            events=self._snapshot.events,
        )

    def prepare_candidate_review(
        self,
        *,
        candidate: IntradayCandidate,
        scan: AShareSurveillanceRun,
        preopen_context: PaperDayLLMPreopenContext,
        requested_at: datetime,
    ) -> MacroResearchPlan | None:
        requested = _aware_utc(requested_at, "requested_at")
        if requested < self._snapshot.as_of:
            raise ValueError("candidate review cannot precede frozen evidence")
        if scan.session_date != preopen_context.known_at.astimezone(_shanghai_timezone()).date():
            raise ValueError("scan and frozen preopen context session mismatch")
        factor_codes = tuple(
            item.factor_id.value for item in candidate.factors if item.contribution is not None
        )
        preopen_dual_summary: tuple[str, ...]
        if preopen_context.has_dual_track:
            assert preopen_context.baseline_decision is not None
            assert preopen_context.baseline_macro_impact is not None
            assert preopen_context.baseline_model is not None
            assert preopen_context.adversarial_decision is not None
            assert preopen_context.adversarial_macro_impact is not None
            assert preopen_context.adversarial_model is not None
            assert preopen_context.selected_track is not None
            assert preopen_context.dual_audit_record_sha256 is not None
            preopen_dual_summary = (
                "PREOPEN_DUAL_TRACK=COMPLETE",
                f"PREOPEN_BASELINE_DECISION={preopen_context.baseline_decision.value}",
                f"PREOPEN_BASELINE_MACRO_IMPACT={preopen_context.baseline_macro_impact}",
                f"PREOPEN_BASELINE_MODEL={preopen_context.baseline_model}",
                f"PREOPEN_ADVERSARIAL_DECISION={preopen_context.adversarial_decision.value}",
                f"PREOPEN_ADVERSARIAL_MACRO_IMPACT={preopen_context.adversarial_macro_impact}",
                f"PREOPEN_ADVERSARIAL_MODEL={preopen_context.adversarial_model}",
                f"PREOPEN_SELECTED_TRACK={preopen_context.selected_track}",
                "PREOPEN_DUAL_AUDIT_RECORD_SHA256="
                f"{preopen_context.dual_audit_record_sha256}",
            )
        else:
            # 旧 journal 可以恢复，但不能据此补造两条从未持久化的模型结论。
            preopen_dual_summary = (
                "PREOPEN_DUAL_TRACK=LEGACY_RECORD_NOT_CARRIED",
            )
        return self._research.prepare(
            symbol=candidate.symbol,
            # 新闻与事件元组始终是唯一的启动快照，但请求决策时间戳会随扫描推进。
            # 这样既能保持协调器 TTL 的实际意义，也不会假装收集过启动后的新闻。
            as_of=requested,
            horizon=_INTRADAY_HORIZON,
            technical_summary=(
                "ANALYSIS_PROFILE=ASHARE_PAPER_INTRADAY_EVIDENCE_REVIEW_V1",
                f"FROZEN_EVIDENCE_AS_OF={self._snapshot.as_of.isoformat()}",
                f"FROZEN_EVIDENCE_SHA256={self._snapshot.snapshot_sha256}",
                f"PREOPEN_CONTEXT_ID={preopen_context.context_id}",
                f"PREOPEN_DECISION={preopen_context.decision.value}",
                f"PREOPEN_MACRO_IMPACT={preopen_context.macro_impact}",
                *preopen_dual_summary,
                f"SCAN_REVISION={scan.source_revision}",
                f"SCAN_STATUS={scan.status.value}",
                f"CANDIDATE_CLASS={candidate.candidate_class.value}",
                f"ANOMALY_SCORE={candidate.anomaly_score:.6f}",
                f"FACTOR_WEIGHT_COVERAGE={candidate.factor_weight_coverage:.6f}",
                "ACTIVE_FACTORS=" + (",".join(factor_codes) if factor_codes else "NONE"),
                "DETERMINISTIC_TECHNICAL_ENGINE_RETAINS_ENTRY_AUTHORITY",
                "MODEL_MAY_CONFIRM_VETO_OR_DOWNGRADE_BUT_CANNOT_CREATE_AN_ORDER",
            ),
            events=self._snapshot.events,
        )


def build_preopen_context(
    plan: MacroResearchPlan,
    run: MacroResearchRun,
    *,
    session_date: date,
    known_at: datetime,
    valid_until: datetime,
) -> PaperDayLLMPreopenContext | None:
    """把一条成功的提供方结果转换为运行器的冻结基线。"""

    known = _aware_utc(known_at, "known_at")
    expiry = _aware_utc(valid_until, "valid_until")
    local_timezone = _shanghai_timezone()
    if (
        known.astimezone(local_timezone).date() != session_date
        or expiry.astimezone(local_timezone).date() != session_date
        or not plan.request.as_of <= known < expiry
    ):
        return None
    if (
        run.plan != plan
        or run.request != plan.request
        or run.failure_code is not None
    ):
        return None
    analysis = run.analysis
    try:
        analysis.validate_against(plan.request)
    except ValueError:
        return None
    if analysis.decision is MacroAnalysisDecision.ABSTAIN:
        # 保持封闭失败边界；运行器会记录盘前上下文不可用，
        # 而不是把弃权当作可用的交易时段基线。
        return None
    identity = plan.analyzer_identity
    if identity is None:
        return None
    if analysis.model_version != identity.requested_model:
        return None
    dual_values = (
        run.baseline_analysis,
        run.adversarial_analysis,
        run.selected_track,
        run.dual_audit_record_sha256,
    )
    # 新盘前上下文必须来自完整、已持久审计的生产双轨；仅旧 journal 的恢复器
    # 可以接受历史 selected-only 文档，并且绝不会据此补造缺失分支。
    if any(value is None for value in dual_values):
        return None
    baseline = run.baseline_analysis
    adversarial = run.adversarial_analysis
    assert baseline is not None
    assert adversarial is not None
    try:
        baseline.validate_against(plan.request)
        adversarial.validate_against(plan.request)
    except ValueError:
        return None
    if (
        run.selected_track != "ADVERSARIAL"
        or adversarial.model_version != identity.requested_model
        or analysis.analysis_id != adversarial.analysis_id
        or analysis.as_of != adversarial.as_of
        or analysis.model_version != adversarial.model_version
        or analysis.macro_impact != adversarial.macro_impact
        or (
            analysis.decision != adversarial.decision
            and not (
                adversarial.decision is MacroAnalysisDecision.PUBLISH
                and analysis.decision is MacroAnalysisDecision.WATCH
            )
        )
    ):
        return None
    audit_sha256 = run.dual_audit_record_sha256
    assert audit_sha256 is not None
    if len(audit_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in audit_sha256
    ):
        return None
    dual_context_document: dict[str, object] = {
        "adversarial": {
            "decision": adversarial.decision.value,
            "macro_impact": adversarial.macro_impact,
            "model": adversarial.model_version,
        },
        "audit_record_sha256": audit_sha256,
        "baseline": {
            "decision": baseline.decision.value,
            "macro_impact": baseline.macro_impact,
            "model": baseline.model_version,
        },
        "selected_track": run.selected_track,
    }
    context_digest = intraday_llm_document_sha256(
        {
            "analysis_id": analysis.analysis_id,
            "decision": analysis.decision.value,
            "dual_track": dual_context_document,
            "macro_impact": analysis.macro_impact,
            "model_identity_sha256": identity.manifest_sha256,
            "plan_manifest_sha256": plan.manifest_sha256,
            "response_model": analysis.model_version,
            "session_date": session_date,
        }
    )
    return PaperDayLLMPreopenContext(
        context_id=f"llm-preopen-{context_digest[:40]}",
        evidence_as_of=plan.request.as_of,
        known_at=known,
        valid_until=expiry,
        analysis_id=analysis.analysis_id,
        decision=analysis.decision,
        macro_impact=analysis.macro_impact,
        evidence_pack_sha256=plan.evidence_pack_sha256,
        request_sha256=plan.request_sha256,
        plan_manifest_sha256=plan.manifest_sha256,
        analyzer_identity=identity,
        response_model=analysis.model_version,
        baseline_decision=baseline.decision,
        baseline_macro_impact=baseline.macro_impact,
        baseline_model=baseline.model_version,
        adversarial_decision=adversarial.decision,
        adversarial_macro_impact=adversarial.macro_impact,
        adversarial_model=adversarial.model_version,
        selected_track=run.selected_track,
        dual_audit_record_sha256=audit_sha256,
    )


def _unavailable_snapshot(
    as_of: datetime,
    path_sha256: str,
    failure_code: str,
) -> FrozenPITEventSnapshot:
    return FrozenPITEventSnapshot(
        as_of=as_of,
        events=(),
        snapshot_sha256=_snapshot_sha256((), as_of=as_of, status="UNAVAILABLE"),
        database_path_sha256=path_sha256,
        status="UNAVAILABLE",
        failure_code=failure_code,
    )


def _path_sha256(path: Path) -> str:
    return hashlib.sha256(str(path.resolve()).encode("utf-8")).hexdigest()


def _snapshot_audit_matches(
    snapshot: FrozenPITEventSnapshot,
    retained: Mapping[str, object],
) -> bool:
    actual = snapshot.audit_document()
    try:
        retained_as_of = _aware_utc(
            datetime.fromisoformat(
                str(retained.get("evidence_as_of", retained.get("as_of")))
            ),
            "retained evidence as_of",
        )
        event_count = retained["event_count"]
        if isinstance(event_count, bool) or not isinstance(event_count, int):
            return False
        expected_path = str(retained["database_path_sha256"])
        expected_snapshot = str(retained["snapshot_sha256"])
        expected_source = str(retained["source"])
        expected_status = str(retained["status"])
    except (KeyError, TypeError, ValueError):
        return False
    return (
        actual["as_of"] == retained_as_of
        and actual["database_path_sha256"] == expected_path
        and actual["event_count"] == event_count
        and actual["failure_code"] == retained.get("failure_code")
        and actual["snapshot_sha256"] == expected_snapshot
        and actual["source"] == expected_source
        and actual["status"] == expected_status
    )


def _factory_audit_document(
    snapshot: FrozenPITEventSnapshot,
) -> dict[str, object]:
    return {
        "database_path_sha256": snapshot.database_path_sha256,
        "event_count": len(snapshot.events),
        "evidence_as_of": snapshot.as_of,
        "failure_code": snapshot.failure_code,
        "schema_version": 1,
        "snapshot_id": f"pit-events-{snapshot.snapshot_sha256[:40]}",
        "snapshot_sha256": snapshot.snapshot_sha256,
        "source": _SNAPSHOT_SOURCE,
        "status": snapshot.status,
    }


def _snapshot_sha256(
    events: tuple[NormalizedEvent, ...],
    *,
    as_of: datetime,
    status: str,
) -> str:
    return intraday_llm_document_sha256(
        {
            "as_of": as_of,
            "events": [
                {
                    "available_at": item.available_at,
                    "content_sha256": item.content_sha256,
                    "first_seen_at": item.first_seen_at,
                    "revision_id": item.revision_id,
                    "source_id": item.source_id,
                }
                for item in events
            ],
            "source": _SNAPSHOT_SOURCE,
            "status": status,
        }
    )


def _row_to_event(row: sqlite3.Row) -> NormalizedEvent:
    entities = json.loads(str(row["entities_json"]))
    if not isinstance(entities, list) or any(not isinstance(item, str) for item in entities):
        raise ValueError("stored event entities are invalid")
    return NormalizedEvent(
        source_id=str(row["source_id"]),
        canonical_url=str(row["canonical_url"]),
        title=str(row["title"]),
        summary=str(row["summary"]),
        event_type=str(row["event_type"]),
        source_tier=SourceTier(str(row["source_tier"])),
        first_seen_at=datetime.fromisoformat(str(row["first_seen_at"])),
        retrieved_at=datetime.fromisoformat(str(row["retrieved_at"])),
        available_at=datetime.fromisoformat(str(row["available_at"])),
        published_at=(
            None
            if row["published_at"] is None
            else datetime.fromisoformat(str(row["published_at"]))
        ),
        external_id=None if row["external_id"] is None else str(row["external_id"]),
        raw_document_id=(None if row["raw_document_id"] is None else str(row["raw_document_id"])),
        entities=tuple(entities),
        content_sha256=str(row["content_sha256"]),
        event_id=str(row["event_id"]),
        revision_id=str(row["revision_id"]),
        revision_number=int(row["revision_number"]),
        supersedes_revision_id=(
            None if row["supersedes_revision_id"] is None else str(row["supersedes_revision_id"])
        ),
    )


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _shanghai_timezone() -> ZoneInfo:
    return ZoneInfo("Asia/Shanghai")
