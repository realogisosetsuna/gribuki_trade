"""持久化、幂等的研究建议与结果观测。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from os import PathLike

from gribuki_trade.backtest.recommendation_outcomes import (
    OutcomeStatus,
    RecommendationOutcome,
)
from gribuki_trade.domain.instruments import ResearchInstrumentProfile
from gribuki_trade.domain.recommendations import (
    ConfidenceBand,
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
    ResearchRecommendation,
)


class ResearchRecordCollisionError(ValueError):
    """同一不可变记录 ID 被复用于不同内容。"""


class SQLiteResearchStore:
    """带不可变建议标识的 SQLite WAL 存储。"""

    def __init__(self, path: str | PathLike[str]) -> None:
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            path,
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._initialize()

    def _initialize(self) -> None:
        with self._transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS research_recommendations (
                    recommendation_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    as_of TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_recommendation_as_of
                ON research_recommendations(as_of DESC, recommendation_id)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS recommendation_outcomes (
                    observation_id TEXT PRIMARY KEY,
                    recommendation_id TEXT NOT NULL,
                    evaluated_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    FOREIGN KEY(recommendation_id)
                        REFERENCES research_recommendations(recommendation_id)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_outcome_recommendation
                ON recommendation_outcomes(recommendation_id, evaluated_at DESC)
                """
            )

    def append_recommendation(self, item: ResearchRecommendation) -> bool:
        """仅插入一次；精确重放返回 False，并拒绝冲突。"""

        payload = _json_bytes(_recommendation_document(item))
        digest = hashlib.sha256(payload).hexdigest()
        with self._transaction() as connection:
            existing = connection.execute(
                """
                SELECT payload_sha256 FROM research_recommendations
                WHERE recommendation_id = ?
                """,
                (item.recommendation_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_sha256"]) != digest:
                    raise ResearchRecordCollisionError(
                        "recommendation ID was reused with different content"
                    )
                return False
            connection.execute(
                """
                INSERT INTO research_recommendations(
                    recommendation_id, symbol, as_of, decision,
                    payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    item.recommendation_id,
                    item.symbol,
                    item.as_of.isoformat(),
                    item.decision.value,
                    payload.decode("utf-8"),
                    digest,
                ),
            )
        return True

    def get_recommendation(self, recommendation_id: str) -> ResearchRecommendation | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                """
                SELECT payload_json FROM research_recommendations
                WHERE recommendation_id = ?
                """,
                (recommendation_id,),
            ).fetchone()
        return None if row is None else _recommendation_from_json(str(row["payload_json"]))

    def latest_recommendations(
        self,
        *,
        limit: int = 200,
        symbol: str | None = None,
    ) -> tuple[ResearchRecommendation, ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        query = "SELECT payload_json FROM research_recommendations"
        parameters: tuple[object, ...]
        if symbol is None:
            parameters = (limit,)
        else:
            query += " WHERE symbol = ?"
            parameters = (symbol.strip().upper(), limit)
        query += " ORDER BY as_of DESC, recommendation_id LIMIT ?"
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(query, parameters).fetchall()
        return tuple(_recommendation_from_json(str(row["payload_json"])) for row in rows)

    def append_outcome(
        self,
        outcome: RecommendationOutcome,
        *,
        evaluated_at: datetime,
    ) -> bool:
        _require_aware(evaluated_at, "evaluated_at")
        document = _outcome_document(outcome)
        payload = _json_bytes(document)
        digest = hashlib.sha256(payload).hexdigest()
        identity_material = (
            f"{outcome.recommendation_id}|{evaluated_at.isoformat()}|{digest}"
        ).encode()
        observation_id = hashlib.sha256(identity_material).hexdigest()
        with self._transaction() as connection:
            parent = connection.execute(
                """
                SELECT 1 FROM research_recommendations
                WHERE recommendation_id = ?
                """,
                (outcome.recommendation_id,),
            ).fetchone()
            if parent is None:
                raise ValueError("outcome requires a retained recommendation")
            existing = connection.execute(
                """
                SELECT payload_sha256 FROM recommendation_outcomes
                WHERE observation_id = ?
                """,
                (observation_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_sha256"]) != digest:
                    raise ResearchRecordCollisionError(
                        "outcome observation ID collision"
                    )
                return False
            connection.execute(
                """
                INSERT INTO recommendation_outcomes(
                    observation_id, recommendation_id, evaluated_at,
                    status, payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    observation_id,
                    outcome.recommendation_id,
                    evaluated_at.isoformat(),
                    outcome.status.value,
                    payload.decode("utf-8"),
                    digest,
                ),
            )
        return True

    def latest_outcomes(
        self,
        recommendation_id: str,
        *,
        limit: int = 20,
    ) -> tuple[RecommendationOutcome, ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json FROM recommendation_outcomes
                WHERE recommendation_id = ?
                ORDER BY evaluated_at DESC, observation_id
                LIMIT ?
                """,
                (recommendation_id, limit),
            ).fetchall()
        return tuple(_outcome_from_json(str(row["payload_json"])) for row in rows)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLiteResearchStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

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
            raise RuntimeError("research store is closed")


def _recommendation_document(item: ResearchRecommendation) -> dict[str, object]:
    return {
        "recommendation_id": item.recommendation_id,
        "symbol": item.symbol,
        "as_of": item.as_of.isoformat(),
        "expires_at": item.expires_at.isoformat(),
        "horizon": item.horizon.value,
        "decision": item.decision.value,
        "confidence": item.confidence.value,
        "technical_score": str(item.technical_score),
        "macro_score": None if item.macro_score is None else str(item.macro_score),
        "combined_score": (
            None if item.combined_score is None else str(item.combined_score)
        ),
        "fusion_reason_codes": list(item.fusion_reason_codes),
        "macro_evidence_coverage": (
            None
            if item.macro_evidence_coverage is None
            else str(item.macro_evidence_coverage)
        ),
        "fusion_version": item.fusion_version,
        "technical_fusion_weight": (
            None
            if item.technical_fusion_weight is None
            else str(item.technical_fusion_weight)
        ),
        "macro_fusion_weight": (
            None
            if item.macro_fusion_weight is None
            else str(item.macro_fusion_weight)
        ),
        "reference_price": (
            None if item.reference_price is None else str(item.reference_price)
        ),
        "invalidation_price": (
            None if item.invalidation_price is None else str(item.invalidation_price)
        ),
        "reason_codes": list(item.reason_codes),
        "uncertainties": list(item.uncertainties),
        "evidence": [
            {
                "evidence_id": evidence.evidence_id,
                "title": evidence.title,
                "canonical_url": evidence.canonical_url,
                "published_at": evidence.published_at.isoformat(),
                "first_seen_at": evidence.first_seen_at.isoformat(),
                "source_tier": evidence.source_tier,
            }
            for evidence in item.evidence
        ],
        "strategy_version": item.strategy_version,
        "model_version": item.model_version,
        "analysis_mode": item.analysis_mode,
        "target_session": _optional_date_text(item.target_session),
        "technical_metrics": [
            {"name": name, "value": str(value)}
            for name, value in item.technical_metrics
        ],
        "instrument_profile": _instrument_profile_document(item.instrument_profile),
    }


def _recommendation_from_json(payload: str) -> ResearchRecommendation:
    document = json.loads(payload)
    return ResearchRecommendation(
        recommendation_id=str(document["recommendation_id"]),
        symbol=str(document["symbol"]),
        as_of=datetime.fromisoformat(str(document["as_of"])),
        expires_at=datetime.fromisoformat(str(document["expires_at"])),
        horizon=RecommendationHorizon(str(document["horizon"])),
        decision=RecommendationDecision(str(document["decision"])),
        confidence=ConfidenceBand(str(document["confidence"])),
        technical_score=Decimal(str(document["technical_score"])),
        macro_score=_optional_decimal(document["macro_score"]),
        combined_score=_optional_decimal(document.get("combined_score")),
        fusion_reason_codes=tuple(
            str(item) for item in document.get("fusion_reason_codes", [])
        ),
        macro_evidence_coverage=_optional_decimal(
            document.get("macro_evidence_coverage")
        ),
        fusion_version=(
            None
            if document.get("fusion_version") is None
            else str(document["fusion_version"])
        ),
        technical_fusion_weight=_optional_decimal(
            document.get("technical_fusion_weight")
        ),
        macro_fusion_weight=_optional_decimal(document.get("macro_fusion_weight")),
        reference_price=_optional_decimal(document["reference_price"]),
        invalidation_price=_optional_decimal(document["invalidation_price"]),
        reason_codes=tuple(str(item) for item in document["reason_codes"]),
        uncertainties=tuple(str(item) for item in document["uncertainties"]),
        evidence=tuple(
            EvidenceReference(
                evidence_id=str(item["evidence_id"]),
                title=str(item["title"]),
                canonical_url=str(item["canonical_url"]),
                published_at=datetime.fromisoformat(str(item["published_at"])),
                first_seen_at=datetime.fromisoformat(str(item["first_seen_at"])),
                source_tier=int(item["source_tier"]),
            )
            for item in document["evidence"]
        ),
        strategy_version=str(document["strategy_version"]),
        model_version=(
            None if document["model_version"] is None else str(document["model_version"])
        ),
        analysis_mode=(
            None
            if document.get("analysis_mode") is None
            else str(document["analysis_mode"])
        ),
        target_session=_optional_date(document.get("target_session")),
        technical_metrics=tuple(
            (str(item["name"]), Decimal(str(item["value"])))
            for item in document.get("technical_metrics", [])
        ),
        instrument_profile=_instrument_profile_from_json(
            document.get("instrument_profile")
        ),
    )


def _instrument_profile_document(
    profile: ResearchInstrumentProfile | None,
) -> dict[str, object] | None:
    if profile is None:
        return None
    return {
        "symbol": profile.symbol,
        "name": profile.name,
        "market": profile.market,
        "asset_type": profile.asset_type,
        "exchange": profile.exchange,
        "board": profile.board,
        "size_tier": profile.size_tier,
        "industry": profile.industry,
        "styles": list(profile.styles),
        "research_role": profile.research_role,
        "risk_tags": list(profile.risk_tags),
        "source_id": profile.source_id,
        "verified_on": profile.verified_on.isoformat(),
        "background_facts": list(profile.background_facts),
    }


def _instrument_profile_from_json(value: object) -> ResearchInstrumentProfile | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("instrument_profile must be a JSON object")
    return ResearchInstrumentProfile(
        symbol=str(value["symbol"]),
        name=str(value["name"]),
        market=str(value["market"]),
        asset_type=str(value["asset_type"]),
        exchange=str(value["exchange"]),
        board=str(value["board"]),
        size_tier=str(value["size_tier"]),
        industry=str(value["industry"]),
        styles=tuple(str(item) for item in value["styles"]),
        research_role=str(value["research_role"]),
        risk_tags=tuple(str(item) for item in value["risk_tags"]),
        source_id=str(value["source_id"]),
        verified_on=date.fromisoformat(str(value["verified_on"])),
        background_facts=tuple(
            str(item) for item in value.get("background_facts", ())
        ),
    )


def _outcome_document(item: RecommendationOutcome) -> dict[str, object]:
    return {
        "recommendation_id": item.recommendation_id,
        "symbol": item.symbol,
        "decision": item.decision.value,
        "status": item.status.value,
        "horizon_sessions": item.horizon_sessions,
        "entry_date": _optional_date_text(item.entry_date),
        "exit_date": _optional_date_text(item.exit_date),
        "entry_price": _optional_decimal_text(item.entry_price),
        "exit_price": _optional_decimal_text(item.exit_price),
        "gross_return": _optional_decimal_text(item.gross_return),
        "net_return_after_costs": _optional_decimal_text(item.net_return_after_costs),
        "max_favorable_excursion": _optional_decimal_text(
            item.max_favorable_excursion
        ),
        "max_adverse_excursion": _optional_decimal_text(item.max_adverse_excursion),
        "direction_correct": item.direction_correct,
        "reason_code": item.reason_code,
    }


def _outcome_from_json(payload: str) -> RecommendationOutcome:
    document = json.loads(payload)
    return RecommendationOutcome(
        recommendation_id=str(document["recommendation_id"]),
        symbol=str(document["symbol"]),
        decision=RecommendationDecision(str(document["decision"])),
        status=OutcomeStatus(str(document["status"])),
        horizon_sessions=int(document["horizon_sessions"]),
        entry_date=_optional_date(document["entry_date"]),
        exit_date=_optional_date(document["exit_date"]),
        entry_price=_optional_decimal(document["entry_price"]),
        exit_price=_optional_decimal(document["exit_price"]),
        gross_return=_optional_decimal(document["gross_return"]),
        net_return_after_costs=_optional_decimal(document["net_return_after_costs"]),
        max_favorable_excursion=_optional_decimal(
            document["max_favorable_excursion"]
        ),
        max_adverse_excursion=_optional_decimal(document["max_adverse_excursion"]),
        direction_correct=document["direction_correct"],
        reason_code=(
            None if document["reason_code"] is None else str(document["reason_code"])
        ),
    )


def _json_bytes(document: dict[str, object]) -> bytes:
    return json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _optional_decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _optional_date(value: object) -> date | None:
    return None if value is None else date.fromisoformat(str(value))


def _optional_date_text(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
