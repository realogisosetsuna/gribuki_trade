"""纯 PAPER 账本 lineage、哈希和连续性校验。

该模块不打开数据库、不执行文件写入，也不依赖调度器或交易服务。它只描述
账本准备结果及其不可变 manifest，并负责校验事件哈希前缀和来源绑定。跨进程
锁、SQLite backup、metadata 表等副作用仍由 :mod:`paper_account_chain` 负责。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast

SCHEMA = "paper-account-chain@1"
CLONE_ORIGINS = frozenset({"VERIFIED_PRIOR_SESSION_CLONE", "RECOVERED_ORPHAN_CLONE"})
ALLOWED_ORIGINS = frozenset(
    {
        "NEW_ACCOUNT",
        *CLONE_ORIGINS,
        # 旧账本只允许在没有同账户历史来源的首日显式迁移，不能绕过前缀验证。
        "LEGACY_EMPTY_SESSION_LEDGER",
        "LEGACY_SESSION_LOCAL",
    }
)


class PaperAccountChainError(RuntimeError):
    """PAPER 账户历史无法被无歧义地延续。"""


@dataclass(frozen=True, slots=True)
class PaperAccountLedgerPreparation:
    """绑定进 PAPER-day manifest 的稳定 lineage。"""

    ledger_path: Path
    account_id: str
    session_date: date
    origin: str
    source_session_date: date | None
    source_event_count: int
    source_last_event_hash: str | None

    def audit_document(self) -> dict[str, object]:
        """返回不含路径、跨重启稳定的 manifest 内容。"""

        return {
            "account_id_sha256": account_hash(self.account_id),
            "origin": self.origin,
            "schema": SCHEMA,
            "session_date": self.session_date.isoformat(),
            "source_event_count": self.source_event_count,
            "source_last_event_hash": self.source_last_event_hash,
            "source_session_date": (
                None if self.source_session_date is None else self.source_session_date.isoformat()
            ),
        }


@dataclass(frozen=True, slots=True)
class LedgerState:
    """一次稳定读取得到的账户、事件哈希和投影日期。"""

    account_ids: tuple[str, ...]
    event_hashes: tuple[str, ...]
    projected_session_date: date | None


@dataclass(frozen=True, slots=True)
class LedgerCandidate:
    """可作为下一交易日来源的已验证历史账本。"""

    directory_date: date
    path: Path
    event_hashes: tuple[str, ...]
    projected_session_date: date
    lineage: PaperAccountLedgerPreparation


def canonical_json(document: Mapping[str, object], *, indent: int | None = None) -> str:
    """以稳定键顺序编码 manifest、seal 等审计文档。"""

    return json.dumps(
        dict(document),
        ensure_ascii=False,
        sort_keys=True,
        indent=indent,
        separators=None if indent is not None else (",", ":"),
    )


def account_hash(account_id: str) -> str:
    """返回账户标识的稳定 SHA-256，不暴露账户原文。"""

    return hashlib.sha256(account_id.encode("utf-8")).hexdigest()


def seal_payload(*, account_id: str, session_date: date, event_hashes: tuple[str, ...]) -> str:
    """构造历史账本 seal 的规范 JSON。"""

    return canonical_json(
        {
            "account_id_sha256": account_hash(account_id),
            "event_count": len(event_hashes),
            "last_event_hash": event_hashes[-1] if event_hashes else None,
            "schema": "paper-account-ledger-seal@1",
            "session_date": session_date.isoformat(),
        }
    )


def validate_count_and_tail(count: int, tail: object, *, context: str) -> None:
    """校验事件数量和尾哈希是否成对出现且格式正确。"""

    if tail is not None and (
        not isinstance(tail, str)
        or len(tail) != 64
        or any(character not in "0123456789abcdef" for character in tail)
    ):
        raise PaperAccountChainError(f"{context} tail hash is invalid")
    if (count == 0) != (tail is None):
        raise PaperAccountChainError(f"{context} event count and tail hash disagree")


def preparation_from_document(
    document: Mapping[str, object],
    *,
    ledger_path: Path,
    expected_session_date: date,
    expected_account_id: str,
) -> PaperAccountLedgerPreparation:
    """将 sidecar/SQLite binding 文档解析为已校验的 lineage。"""

    expected_fields = {
        "account_id_sha256",
        "origin",
        "schema",
        "session_date",
        "source_event_count",
        "source_last_event_hash",
        "source_session_date",
    }
    if set(document) != expected_fields:
        raise PaperAccountChainError("PAPER ledger lineage fields are invalid")
    if document.get("schema") != SCHEMA:
        raise PaperAccountChainError("PAPER ledger lineage schema is unsupported")
    if document.get("account_id_sha256") != account_hash(expected_account_id):
        raise PaperAccountChainError("PAPER ledger lineage account does not match")
    if document.get("session_date") != expected_session_date.isoformat():
        raise PaperAccountChainError("PAPER ledger lineage session does not match")

    origin = document.get("origin")
    event_count = document.get("source_event_count")
    last_hash = document.get("source_last_event_hash")
    source_date = document.get("source_session_date")
    if not isinstance(origin, str) or origin not in ALLOWED_ORIGINS:
        raise PaperAccountChainError("PAPER ledger lineage origin is invalid")
    if isinstance(event_count, bool) or not isinstance(event_count, int) or event_count < 0:
        raise PaperAccountChainError("PAPER ledger lineage event count is invalid")
    validate_count_and_tail(event_count, last_hash, context="PAPER ledger lineage")
    try:
        parsed_source_date = None if source_date is None else date.fromisoformat(str(source_date))
    except ValueError as error:
        raise PaperAccountChainError("PAPER ledger lineage source date is invalid") from error
    if parsed_source_date is not None and parsed_source_date >= expected_session_date:
        raise PaperAccountChainError("PAPER ledger lineage source date must precede session date")
    if origin in CLONE_ORIGINS and parsed_source_date is None:
        raise PaperAccountChainError("cloned PAPER lineage must identify its source date")
    if origin not in CLONE_ORIGINS and parsed_source_date is not None:
        raise PaperAccountChainError("non-cloned PAPER lineage must not identify a source date")
    if origin in {"NEW_ACCOUNT", "LEGACY_EMPTY_SESSION_LEDGER"} and event_count != 0:
        raise PaperAccountChainError("empty-origin PAPER lineage cannot contain events")
    return PaperAccountLedgerPreparation(
        ledger_path=ledger_path,
        account_id=expected_account_id,
        session_date=expected_session_date,
        origin=origin,
        source_session_date=parsed_source_date,
        source_event_count=event_count,
        source_last_event_hash=cast(str | None, last_hash),
    )


def clone_preparation(
    *,
    ledger_path: Path,
    session_date: date,
    account_id: str,
    source: LedgerCandidate,
    origin: str = "VERIFIED_PRIOR_SESSION_CLONE",
) -> PaperAccountLedgerPreparation:
    """从已验证来源生成新交易日 lineage。"""

    return PaperAccountLedgerPreparation(
        ledger_path=ledger_path,
        account_id=account_id,
        session_date=session_date,
        origin=origin,
        source_session_date=source.directory_date,
        source_event_count=len(source.event_hashes),
        source_last_event_hash=(source.event_hashes[-1] if source.event_hashes else None),
    )


def verify_lineage_source_binding(
    preparation: PaperAccountLedgerPreparation,
    *,
    state: LedgerState,
    prior_candidates: tuple[LedgerCandidate, ...],
) -> None:
    """将 lineage 来源日期、数量和尾哈希绑定到真实历史账本。"""

    if preparation.origin in CLONE_ORIGINS:
        if not prior_candidates:
            raise PaperAccountChainError("cloned PAPER lineage has no verified historical source")
        source = prior_candidates[-1]
        expected_tail = source.event_hashes[-1] if source.event_hashes else None
        if (
            preparation.source_session_date != source.directory_date
            or preparation.source_event_count != len(source.event_hashes)
            or preparation.source_last_event_hash != expected_tail
        ):
            raise PaperAccountChainError(
                "PAPER lineage source does not match the latest verified ledger"
            )
        if state.event_hashes[: len(source.event_hashes)] != source.event_hashes:
            raise PaperAccountChainError(
                "current PAPER ledger does not contain its declared source stream"
            )
        return
    if prior_candidates:
        raise PaperAccountChainError(
            "non-cloned PAPER lineage cannot discard an existing historical source"
        )


def verify_prefix_chain(candidates: tuple[LedgerCandidate, ...]) -> None:
    """确保历史账本事件哈希是单调扩展的前缀链。"""

    prior: LedgerCandidate | None = None
    for candidate in candidates:
        if candidate.projected_session_date != candidate.directory_date:
            raise PaperAccountChainError(
                "historical PAPER ledger projection does not match its directory"
            )
        if prior is not None:
            prefix = candidate.event_hashes[: len(prior.event_hashes)]
            if prefix != prior.event_hashes:
                raise PaperAccountChainError(
                    "historical PAPER account ledgers contain divergent event streams"
                )
        prior = candidate


def verify_existing_ledger_matches_lineage(
    state: LedgerState, preparation: PaperAccountLedgerPreparation
) -> None:
    """将 lineage 基线重新绑定到当前 SQLite 事件链。"""

    if not state.account_ids:
        if preparation.source_event_count != 0:
            raise PaperAccountChainError(
                "PAPER lineage declares events but current ledger is empty"
            )
        return
    if state.account_ids != (preparation.account_id,):
        raise PaperAccountChainError("current PAPER ledger is not isolated to lineage account")
    baseline_count = preparation.source_event_count
    if len(state.event_hashes) < baseline_count:
        raise PaperAccountChainError("current PAPER ledger is shorter than lineage")
    if baseline_count:
        expected_tail = preparation.source_last_event_hash
        actual_tail = state.event_hashes[baseline_count - 1]
        if actual_tail != expected_tail:
            raise PaperAccountChainError(
                "current PAPER ledger does not contain the lineage event prefix"
            )
    if preparation.origin == "LEGACY_SESSION_LOCAL" and baseline_count != len(state.event_hashes):
        raise PaperAccountChainError(
            "legacy PAPER lineage must bind the complete migrated event stream"
        )
    allowed_projection_dates = {preparation.session_date}
    if preparation.source_session_date is not None:
        allowed_projection_dates.add(preparation.source_session_date)
    if state.projected_session_date not in allowed_projection_dates:
        raise PaperAccountChainError("current PAPER ledger projection is incompatible with lineage")
