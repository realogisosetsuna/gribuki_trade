"""按交易日隔离、可恢复且可封存的 A 股 PAPER 账本连续性校验。

每个交易日使用独立 SQLite 文件，避免盘中循环与盘后分析共享可写数据库。
新交易日前会克隆最近一份通过验证的累计账本；历史链必须保持前缀一致。
lineage 同时写入 JSON sidecar 和账本内不可变绑定表，历史账本首次成为来源时
还会写入事件数量与尾哈希 seal。这样即使进程在 clone 与 sidecar 提交之间崩溃，
或当前 sidecar 被删除，也能从账本内绑定或唯一历史前缀恢复，而不会降级成无来源
的“本地旧账本”。任何账户、日期、lineage、seal 或事件哈希歧义均 fail-closed。
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import sqlite3
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import BinaryIO, Protocol, cast

from gribuki_trade.services.ashare_paper import replay_paper_account
from gribuki_trade.storage.paper_ledger import (
    PaperLedgerIntegrityError,
    SQLitePaperLedger,
)

_SCHEMA = "paper-account-chain@1"
_LEDGER_NAME = "ledger.sqlite3"
_LINEAGE_NAME = "ledger-lineage.json"
_PREPARATION_LOCK_NAME = ".ledger-preparation.lock"
_BINDING_TABLE = "paper_account_lineage_bindings"
_SEAL_TABLE = "paper_account_lineage_seals"
_SEALED_LEDGER_TRIGGER = "paper_account_historical_seal_no_insert"
_CLONE_ORIGINS = frozenset(
    {"VERIFIED_PRIOR_SESSION_CLONE", "RECOVERED_ORPHAN_CLONE"}
)
_ALLOWED_ORIGINS = frozenset(
    {
        "NEW_ACCOUNT",
        *_CLONE_ORIGINS,
        # 这两种 origin 只允许出现在没有任何同账户历史候选的第一天。
        # 它们是旧账本进入新协议的显式、可审计迁移规则，不是通用旁路。
        "LEGACY_EMPTY_SESSION_LEDGER",
        "LEGACY_SESSION_LOCAL",
    }
)


class _FcntlModule(Protocol):
    """只描述跨平台分支实际使用的 fcntl 最小接口。"""

    LOCK_EX: int
    LOCK_NB: int
    LOCK_UN: int
    flock: Callable[[int, int], object]


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
            "account_id_sha256": _account_hash(self.account_id),
            "origin": self.origin,
            "schema": _SCHEMA,
            "session_date": self.session_date.isoformat(),
            "source_event_count": self.source_event_count,
            "source_last_event_hash": self.source_last_event_hash,
            "source_session_date": (
                None
                if self.source_session_date is None
                else self.source_session_date.isoformat()
            ),
        }


@dataclass(frozen=True, slots=True)
class _LedgerState:
    account_ids: tuple[str, ...]
    event_hashes: tuple[str, ...]
    projected_session_date: date | None


@dataclass(frozen=True, slots=True)
class _Candidate:
    directory_date: date
    path: Path
    event_hashes: tuple[str, ...]
    projected_session_date: date
    lineage: PaperAccountLedgerPreparation


def prepare_paper_day_ledger(
    session_root: Path,
    *,
    session_date: date,
    account_id: str,
) -> PaperAccountLedgerPreparation:
    """在 ``session_root`` 内准备累计账本。

    已存在的当日数据库永不覆盖。新数据库通过 SQLite backup 克隆并原子安装；
    常规账户服务仍负责追加新交易日 rollover。独立准备锁覆盖历史验证、克隆、
    账本内 binding、JSON lineage 写入和复核，避免两个启动进程在 runner writer
    lease 之前相互覆盖。
    """

    normalized_account = account_id.strip()
    if not normalized_account:
        raise ValueError("account_id must not be empty")
    if session_root.is_symlink():
        raise PaperAccountChainError("PAPER session root must not be a symlink")
    resolved_root = session_root.resolve()
    if resolved_root.name != session_date.isoformat():
        raise ValueError("session_root name must match session_date")
    resolved_root.mkdir(parents=True, exist_ok=True)
    with _preparation_lock(resolved_root / _PREPARATION_LOCK_NAME):
        return _prepare_locked(
            resolved_root,
            session_date=session_date,
            account_id=normalized_account,
        )


def _prepare_locked(
    resolved_root: Path,
    *,
    session_date: date,
    account_id: str,
) -> PaperAccountLedgerPreparation:
    """在持有独立跨进程锁时完成历史验真、克隆和双份 lineage 提交。"""

    ledger_path = resolved_root / _LEDGER_NAME
    lineage_path = resolved_root / _LINEAGE_NAME
    candidates = _historical_candidates(
        resolved_root.parent,
        before=session_date,
        account_id=account_id,
    )
    _verify_prefix_chain(candidates)

    if ledger_path.is_symlink():
        raise PaperAccountChainError("current PAPER ledger must not be a symlink")
    if lineage_path.is_symlink():
        raise PaperAccountChainError("PAPER ledger lineage must not be a symlink")
    if ledger_path.exists():
        if not ledger_path.is_file():
            raise PaperAccountChainError("current PAPER ledger must be a regular file")
        return _existing_preparation(
            ledger_path,
            lineage_path=lineage_path,
            session_date=session_date,
            account_id=account_id,
            prior_candidates=candidates,
        )
    if lineage_path.exists():
        raise PaperAccountChainError("PAPER lineage exists without its current ledger")

    if not candidates:
        # 首日先创建空 schema；账本内 binding 先于 sidecar 提交。若进程恰在空
        # schema 创建后、binding 前崩溃，重启会以 LEGACY_EMPTY_SESSION_LEDGER
        # 显式记录这一不可区分的迁移窗口，而不会把它伪称为已提交 NEW_ACCOUNT。
        with SQLitePaperLedger(ledger_path):
            pass
        preparation = PaperAccountLedgerPreparation(
            ledger_path=ledger_path,
            account_id=account_id,
            session_date=session_date,
            origin="NEW_ACCOUNT",
            source_session_date=None,
            source_event_count=0,
            source_last_event_hash=None,
        )
        _persist_embedded_binding(ledger_path, preparation)
        _write_lineage(lineage_path, preparation)
        return preparation

    source = candidates[-1]
    # 再持有来源日准备锁完成 clone 前后复核，避免另一个跨日准备进程在历史扫描
    # 与 backup 之间补迁移元数据或改换来源。PAPER writer 不使用此锁，因此仍以
    # 两次真实事件流读取和历史 seal 检出不应继续写入的已结束交易日。
    with _preparation_lock(source.path.parent / _PREPARATION_LOCK_NAME):
        source_before_clone = _read_ledger_state(source.path, account_id=account_id)
        _verify_existing_historical_seal(
            source.path,
            account_id=account_id,
            session_date=source.directory_date,
            event_hashes=source_before_clone.event_hashes,
        )
        if (
            source_before_clone.account_ids != (account_id,)
            or source_before_clone.projected_session_date != source.directory_date
            or source_before_clone.event_hashes != source.event_hashes
        ):
            raise PaperAccountChainError(
                "prior PAPER ledger changed before it could be cloned"
            )
        _atomic_sqlite_backup(source.path, ledger_path)
        source_after_clone = _read_ledger_state(source.path, account_id=account_id)
        _verify_existing_historical_seal(
            source.path,
            account_id=account_id,
            session_date=source.directory_date,
            event_hashes=source_after_clone.event_hashes,
        )
        if source_after_clone != source_before_clone:
            raise PaperAccountChainError(
                "prior PAPER ledger changed while it was being cloned; retry preparation"
            )
    cloned = _read_ledger_state(ledger_path, account_id=account_id)
    if cloned.event_hashes != source.event_hashes:
        # 历史 writer 在验证与 backup 之间变化。保留无 sidecar 的 clone 作为可恢复
        # 状态；下一次调用会重新扫描历史并仅在前缀唯一时完成 lineage 提交。
        raise PaperAccountChainError(
            "prior PAPER ledger changed while it was being cloned; retry preparation"
        )
    preparation = _clone_preparation(
        ledger_path=ledger_path,
        session_date=session_date,
        account_id=account_id,
        source=source,
    )
    # binding 先提交，JSON 后提交；两个崩溃窗口都能由 _existing_preparation 恢复。
    _persist_embedded_binding(ledger_path, preparation)
    _write_lineage(lineage_path, preparation)
    return preparation


def _existing_preparation(
    ledger_path: Path,
    *,
    lineage_path: Path,
    session_date: date,
    account_id: str,
    prior_candidates: tuple[_Candidate, ...],
) -> PaperAccountLedgerPreparation:
    state = _read_ledger_state(ledger_path, account_id=account_id)
    preparation = _load_or_recover_lineage(
        ledger_path=ledger_path,
        lineage_path=lineage_path,
        session_date=session_date,
        account_id=account_id,
        state=state,
        prior_candidates=prior_candidates,
    )
    _verify_lineage_source_binding(
        preparation,
        state=state,
        prior_candidates=prior_candidates,
    )
    _verify_existing_ledger_matches_lineage(state, preparation)
    _commit_lineage(ledger_path, lineage_path, preparation)
    return preparation


def _historical_candidates(
    day_root: Path,
    *,
    before: date,
    account_id: str,
) -> tuple[_Candidate, ...]:
    """逐日读取、绑定、封存历史账本；任何可疑日期目录都不静默晋级来源。"""

    if not day_root.is_dir():
        return ()
    dated_directories: list[tuple[date, Path]] = []
    for directory in day_root.iterdir():
        try:
            directory_date = date.fromisoformat(directory.name)
        except ValueError:
            continue
        if directory_date >= before:
            continue
        if directory.is_symlink():
            raise PaperAccountChainError(
                "historical PAPER session directory must not be a symlink"
            )
        if not directory.is_dir():
            continue
        dated_directories.append((directory_date, directory))

    items: list[_Candidate] = []
    for directory_date, directory in sorted(dated_directories):
        ledger_path = directory / _LEDGER_NAME
        lineage_path = directory / _LINEAGE_NAME
        if ledger_path.is_symlink():
            raise PaperAccountChainError(
                "historical PAPER ledger must not be a symlink"
            )
        if lineage_path.is_symlink():
            raise PaperAccountChainError(
                "historical PAPER lineage must not be a symlink"
            )
        if not ledger_path.exists():
            if lineage_path.exists():
                raise PaperAccountChainError(
                    "historical PAPER lineage exists without its ledger"
                )
            continue
        if not ledger_path.is_file():
            raise PaperAccountChainError(
                "historical PAPER ledger must be a regular file"
            )

        # 缺失 lineage 的迁移会写历史目录，因此也必须使用该日自己的跨进程锁。
        with _preparation_lock(directory / _PREPARATION_LOCK_NAME):
            state = _read_ledger_state(ledger_path, account_id=account_id)
            if state.account_ids and state.account_ids != (account_id,):
                if account_id not in state.account_ids:
                    # 允许同一 runtime 根下存在另一个单账户目录；混账在上面的多账户
                    # 检查中已经拒绝。
                    continue
                raise PaperAccountChainError(
                    "historical PAPER ledger mixes multiple account identifiers"
                )
            _verify_existing_historical_seal(
                ledger_path,
                account_id=account_id,
                session_date=directory_date,
                event_hashes=state.event_hashes,
            )
            if not state.account_ids:
                # 空账本不能承载资金/持仓来源；若它属于本账户且已有 sidecar，仍校验
                # account/session/binding，避免损坏文件被完全忽略。
                if lineage_path.exists() or _embedded_binding_exists(
                    ledger_path, directory_date
                ):
                    preparation = _load_or_recover_lineage(
                        ledger_path=ledger_path,
                        lineage_path=lineage_path,
                        session_date=directory_date,
                        account_id=account_id,
                        state=state,
                        prior_candidates=tuple(items),
                    )
                    _verify_lineage_source_binding(
                        preparation,
                        state=state,
                        prior_candidates=tuple(items),
                    )
                    _verify_existing_ledger_matches_lineage(state, preparation)
                    _commit_lineage(ledger_path, lineage_path, preparation)
                    _seal_historical_ledger(
                        ledger_path,
                        account_id=account_id,
                        session_date=directory_date,
                        event_hashes=state.event_hashes,
                    )
                continue
            if state.projected_session_date != directory_date:
                raise PaperAccountChainError(
                    "historical PAPER ledger projection does not match its directory"
                )
            preparation = _load_or_recover_lineage(
                ledger_path=ledger_path,
                lineage_path=lineage_path,
                session_date=directory_date,
                account_id=account_id,
                state=state,
                prior_candidates=tuple(items),
            )
            _verify_lineage_source_binding(
                preparation,
                state=state,
                prior_candidates=tuple(items),
            )
            _verify_existing_ledger_matches_lineage(state, preparation)
            _commit_lineage(ledger_path, lineage_path, preparation)
            _seal_historical_ledger(
                ledger_path,
                account_id=account_id,
                session_date=directory_date,
                event_hashes=state.event_hashes,
            )
            items.append(
                _Candidate(
                    directory_date=directory_date,
                    path=ledger_path,
                    event_hashes=state.event_hashes,
                    projected_session_date=directory_date,
                    lineage=preparation,
                )
            )
    return tuple(items)


def _load_or_recover_lineage(
    *,
    ledger_path: Path,
    lineage_path: Path,
    session_date: date,
    account_id: str,
    state: _LedgerState,
    prior_candidates: tuple[_Candidate, ...],
) -> PaperAccountLedgerPreparation:
    """读取 sidecar/binding；仅缺失时允许从不可变证据确定性恢复。"""

    if lineage_path.is_symlink():
        raise PaperAccountChainError("PAPER ledger lineage must not be a symlink")
    embedded = _read_embedded_binding(
        ledger_path,
        session_date=session_date,
        account_id=account_id,
    )
    if lineage_path.exists():
        preparation = _read_lineage(
            lineage_path,
            ledger_path=ledger_path,
            expected_session_date=session_date,
            expected_account_id=account_id,
        )
        if embedded is not None and embedded.audit_document() != preparation.audit_document():
            raise PaperAccountChainError(
                "PAPER ledger lineage contradicts its immutable ledger binding or event prefix"
            )
        return preparation
    if embedded is not None:
        return embedded

    preparation = _infer_missing_lineage(
        ledger_path=ledger_path,
        session_date=session_date,
        account_id=account_id,
        state=state,
        prior_candidates=prior_candidates,
    )
    return preparation


def _commit_lineage(
    ledger_path: Path,
    lineage_path: Path,
    preparation: PaperAccountLedgerPreparation,
) -> None:
    """仅在来源和当前账本均验真后补齐不可变 binding 或缺失 sidecar。"""

    _persist_embedded_binding(ledger_path, preparation)
    if not lineage_path.exists():
        _write_lineage(lineage_path, preparation)


def _infer_missing_lineage(
    *,
    ledger_path: Path,
    session_date: date,
    account_id: str,
    state: _LedgerState,
    prior_candidates: tuple[_Candidate, ...],
) -> PaperAccountLedgerPreparation:
    """处理两类可证明状态：唯一历史 clone，或首个真正 legacy 日。"""

    if prior_candidates:
        source = prior_candidates[-1]
        if state.event_hashes[: len(source.event_hashes)] != source.event_hashes:
            raise PaperAccountChainError(
                "lineage-free PAPER ledger contains a divergent event stream instead of "
                "the latest verified source"
            )
        if len(state.event_hashes) < len(source.event_hashes):
            raise PaperAccountChainError(
                "lineage-free PAPER ledger is shorter than the latest verified source"
            )
        return _clone_preparation(
            ledger_path=ledger_path,
            session_date=session_date,
            account_id=account_id,
            source=source,
            origin="RECOVERED_ORPHAN_CLONE",
        )

    # 仅“没有任何同账户历史候选”的第一天可以进入 legacy 迁移。该 origin
    # 会持久写入 sidecar 和账本 binding，后续日绝不能再次使用这一旁路。
    if not state.account_ids:
        return PaperAccountLedgerPreparation(
            ledger_path=ledger_path,
            account_id=account_id,
            session_date=session_date,
            origin="LEGACY_EMPTY_SESSION_LEDGER",
            source_session_date=None,
            source_event_count=0,
            source_last_event_hash=None,
        )
    if state.projected_session_date != session_date:
        raise PaperAccountChainError(
            "legacy current ledger projection does not match session_date"
        )
    return PaperAccountLedgerPreparation(
        ledger_path=ledger_path,
        account_id=account_id,
        session_date=session_date,
        origin="LEGACY_SESSION_LOCAL",
        source_session_date=None,
        source_event_count=len(state.event_hashes),
        source_last_event_hash=(state.event_hashes[-1] if state.event_hashes else None),
    )


def _clone_preparation(
    *,
    ledger_path: Path,
    session_date: date,
    account_id: str,
    source: _Candidate,
    origin: str = "VERIFIED_PRIOR_SESSION_CLONE",
) -> PaperAccountLedgerPreparation:
    return PaperAccountLedgerPreparation(
        ledger_path=ledger_path,
        account_id=account_id,
        session_date=session_date,
        origin=origin,
        source_session_date=source.directory_date,
        source_event_count=len(source.event_hashes),
        source_last_event_hash=(source.event_hashes[-1] if source.event_hashes else None),
    )


def _verify_lineage_source_binding(
    preparation: PaperAccountLedgerPreparation,
    *,
    state: _LedgerState,
    prior_candidates: tuple[_Candidate, ...],
) -> None:
    """将 lineage 来源日期、数量和尾哈希绑定到真实的最近历史账本。"""

    if preparation.origin in _CLONE_ORIGINS:
        if not prior_candidates:
            raise PaperAccountChainError(
                "cloned PAPER lineage has no verified historical source"
            )
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


def _verify_prefix_chain(candidates: tuple[_Candidate, ...]) -> None:
    prior: _Candidate | None = None
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


@contextmanager
def _preparation_lock(path: Path, *, timeout_seconds: float = 15.0) -> Iterator[None]:
    """取得跨进程独占锁；锁超时即拒绝并发准备，不猜测另一进程状态。"""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if path.is_symlink():
        raise PaperAccountChainError("PAPER preparation lock must not be a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    try:
        handle_context = path.open("a+b")
    except OSError as error:
        raise PaperAccountChainError("PAPER preparation lock is unavailable") from error
    with handle_context as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        acquired = False
        while not acquired:
            try:
                _lock_file_byte(handle)
            except OSError:
                if time.monotonic() >= deadline:
                    raise PaperAccountChainError(
                        "PAPER ledger preparation lock is already held"
                    ) from None
                time.sleep(0.05)
            else:
                acquired = True
        try:
            yield
        finally:
            _unlock_file_byte(handle)


def _lock_file_byte(handle: BinaryIO) -> None:
    """锁定文件首字节，兼容 Windows 与 POSIX。"""

    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    fcntl = cast(_FcntlModule, importlib.import_module("fcntl"))
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file_byte(handle: BinaryIO) -> None:
    """释放由 ``_lock_file_byte`` 取得的锁。"""

    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    fcntl = cast(_FcntlModule, importlib.import_module("fcntl"))
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _read_ledger_state(ledger_path: Path, *, account_id: str) -> _LedgerState:
    """读取两次哈希流，拒绝验证期间仍在变化的历史/恢复账本。"""

    try:
        with SQLitePaperLedger(ledger_path) as ledger:
            before_accounts = ledger.account_ids()
            if len(before_accounts) > 1:
                raise PaperAccountChainError(
                    "PAPER ledger mixes multiple account identifiers"
                )
            if not before_accounts:
                return _LedgerState((), (), None)
            if before_accounts != (account_id,):
                return _LedgerState(before_accounts, (), None)
            first = ledger.events(account_id)
            second = ledger.events(account_id)
            after_accounts = ledger.account_ids()
    except PaperLedgerIntegrityError as error:
        raise PaperAccountChainError("PAPER ledger event hash chain is invalid") from error
    if before_accounts != after_accounts or first != second:
        raise PaperAccountChainError("PAPER ledger changed during lineage validation")
    snapshot = replay_paper_account(first)
    return _LedgerState(
        account_ids=before_accounts,
        event_hashes=tuple(event.event_hash for event in first),
        projected_session_date=snapshot.session_date,
    )


def _verify_existing_ledger_matches_lineage(
    state: _LedgerState,
    preparation: PaperAccountLedgerPreparation,
) -> None:
    """把 lineage 的来源基线重新绑定到当前 SQLite 事件链。"""

    if not state.account_ids:
        if preparation.source_event_count != 0:
            raise PaperAccountChainError(
                "PAPER lineage declares events but current ledger is empty"
            )
        return
    if state.account_ids != (preparation.account_id,):
        raise PaperAccountChainError(
            "current PAPER ledger is not isolated to lineage account"
        )
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
    if preparation.origin == "LEGACY_SESSION_LOCAL" and baseline_count != len(
        state.event_hashes
    ):
        raise PaperAccountChainError(
            "legacy PAPER lineage must bind the complete migrated event stream"
        )
    allowed_projection_dates = {preparation.session_date}
    if preparation.source_session_date is not None:
        allowed_projection_dates.add(preparation.source_session_date)
    if state.projected_session_date not in allowed_projection_dates:
        raise PaperAccountChainError(
            "current PAPER ledger projection is incompatible with lineage"
        )


def _atomic_sqlite_backup(source: Path, destination: Path) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            delete=False,
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
        ) as handle:
            temporary = Path(handle.name)
        source_connection = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True)
        target_connection = sqlite3.connect(temporary)
        try:
            source_connection.backup(target_connection)
            target_connection.commit()
        finally:
            target_connection.close()
            source_connection.close()
        with temporary.open("r+b") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        temporary = None
        _fsync_path_and_parent(destination)
    except sqlite3.Error as error:
        raise PaperAccountChainError("unable to clone prior PAPER ledger") from error
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _write_lineage(path: Path, preparation: PaperAccountLedgerPreparation) -> None:
    if path.is_symlink():
        raise PaperAccountChainError("PAPER ledger lineage must not be a symlink")
    if path.exists() and not path.is_file():
        raise PaperAccountChainError("PAPER ledger lineage must be a regular file")
    payload = (_canonical_json(preparation.audit_document(), indent=2) + "\n").encode(
        "utf-8"
    )
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            delete=False,
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        ) as handle:
            temporary = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
        _fsync_path_and_parent(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _read_lineage(
    path: Path,
    *,
    ledger_path: Path,
    expected_session_date: date,
    expected_account_id: str,
) -> PaperAccountLedgerPreparation:
    if path.is_symlink() or not path.is_file():
        raise PaperAccountChainError("PAPER ledger lineage must be a regular file")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PaperAccountChainError("PAPER ledger lineage is unreadable") from error
    if not isinstance(raw, dict) or any(not isinstance(key, str) for key in raw):
        raise PaperAccountChainError("PAPER ledger lineage must be a JSON object")
    return _preparation_from_document(
        cast(dict[str, object], raw),
        ledger_path=ledger_path,
        expected_session_date=expected_session_date,
        expected_account_id=expected_account_id,
    )


def _preparation_from_document(
    document: Mapping[str, object],
    *,
    ledger_path: Path,
    expected_session_date: date,
    expected_account_id: str,
) -> PaperAccountLedgerPreparation:
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
    if document.get("schema") != _SCHEMA:
        raise PaperAccountChainError("PAPER ledger lineage schema is unsupported")
    if document.get("account_id_sha256") != _account_hash(expected_account_id):
        raise PaperAccountChainError("PAPER ledger lineage account does not match")
    if document.get("session_date") != expected_session_date.isoformat():
        raise PaperAccountChainError("PAPER ledger lineage session does not match")
    origin = document.get("origin")
    event_count = document.get("source_event_count")
    last_hash = document.get("source_last_event_hash")
    source_date = document.get("source_session_date")
    if not isinstance(origin, str) or origin not in _ALLOWED_ORIGINS:
        raise PaperAccountChainError("PAPER ledger lineage origin is invalid")
    if isinstance(event_count, bool) or not isinstance(event_count, int) or event_count < 0:
        raise PaperAccountChainError("PAPER ledger lineage event count is invalid")
    _validate_count_and_tail(event_count, last_hash, context="PAPER ledger lineage")
    try:
        parsed_source_date = None if source_date is None else date.fromisoformat(str(source_date))
    except ValueError as error:
        raise PaperAccountChainError("PAPER ledger lineage source date is invalid") from error
    if parsed_source_date is not None and parsed_source_date >= expected_session_date:
        raise PaperAccountChainError(
            "PAPER ledger lineage source date must precede session date"
        )
    if origin in _CLONE_ORIGINS and parsed_source_date is None:
        raise PaperAccountChainError("cloned PAPER lineage must identify its source date")
    if origin not in _CLONE_ORIGINS and parsed_source_date is not None:
        raise PaperAccountChainError(
            "non-cloned PAPER lineage must not identify a source date"
        )
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


def _persist_embedded_binding(
    ledger_path: Path,
    preparation: PaperAccountLedgerPreparation,
) -> None:
    document = preparation.audit_document()
    payload = _canonical_json(document)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    try:
        with _metadata_transaction(ledger_path) as connection:
            existing = connection.execute(
                f"SELECT document_json, document_sha256 FROM {_BINDING_TABLE} "
                "WHERE session_date = ?",
                (preparation.session_date.isoformat(),),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) != payload or str(existing[1]) != digest:
                    raise PaperAccountChainError(
                        "immutable PAPER lineage binding conflicts with requested lineage"
                    )
                return
            connection.execute(
                f"INSERT INTO {_BINDING_TABLE} "
                "(session_date, document_json, document_sha256) VALUES (?, ?, ?)",
                (preparation.session_date.isoformat(), payload, digest),
            )
    except sqlite3.Error as error:
        raise PaperAccountChainError("unable to persist PAPER lineage binding") from error


def _read_embedded_binding(
    ledger_path: Path,
    *,
    session_date: date,
    account_id: str,
) -> PaperAccountLedgerPreparation | None:
    try:
        _initialize_metadata(ledger_path)
        connection = sqlite3.connect(ledger_path)
        try:
            row = connection.execute(
                f"SELECT document_json, document_sha256 FROM {_BINDING_TABLE} "
                "WHERE session_date = ?",
                (session_date.isoformat(),),
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error as error:
        raise PaperAccountChainError("unable to read PAPER lineage binding") from error
    if row is None:
        return None
    payload = str(row[0])
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    if str(row[1]) != digest:
        raise PaperAccountChainError("embedded PAPER lineage binding digest is invalid")
    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as error:
        raise PaperAccountChainError("embedded PAPER lineage binding is unreadable") from error
    if not isinstance(raw, dict) or any(not isinstance(key, str) for key in raw):
        raise PaperAccountChainError("embedded PAPER lineage binding is invalid")
    return _preparation_from_document(
        cast(dict[str, object], raw),
        ledger_path=ledger_path,
        expected_session_date=session_date,
        expected_account_id=account_id,
    )


def _embedded_binding_exists(ledger_path: Path, session_date: date) -> bool:
    try:
        _initialize_metadata(ledger_path)
        connection = sqlite3.connect(ledger_path)
        try:
            row = connection.execute(
                f"SELECT 1 FROM {_BINDING_TABLE} WHERE session_date = ?",
                (session_date.isoformat(),),
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error as error:
        raise PaperAccountChainError("unable to inspect PAPER lineage binding") from error
    return row is not None


def _seal_historical_ledger(
    ledger_path: Path,
    *,
    account_id: str,
    session_date: date,
    event_hashes: tuple[str, ...],
) -> None:
    payload = _seal_payload(
        account_id=account_id,
        session_date=session_date,
        event_hashes=event_hashes,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    try:
        with _metadata_transaction(ledger_path) as connection:
            existing = connection.execute(
                f"SELECT seal_json, seal_sha256 FROM {_SEAL_TABLE} WHERE session_date = ?",
                (session_date.isoformat(),),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) != payload or str(existing[1]) != digest:
                    raise PaperAccountChainError(
                        "historical PAPER ledger no longer matches its immutable seal"
                    )
                return
            connection.execute(
                f"INSERT INTO {_SEAL_TABLE} "
                "(session_date, seal_json, seal_sha256) VALUES (?, ?, ?)",
                (session_date.isoformat(), payload, digest),
            )
    except sqlite3.Error as error:
        raise PaperAccountChainError("unable to seal historical PAPER ledger") from error


def _verify_existing_historical_seal(
    ledger_path: Path,
    *,
    account_id: str,
    session_date: date,
    event_hashes: tuple[str, ...],
) -> None:
    """先验证已有历史封存，避免较宽松的投影检查掩盖账本变化。"""

    expected_payload = _seal_payload(
        account_id=account_id,
        session_date=session_date,
        event_hashes=event_hashes,
    )
    expected_digest = hashlib.sha256(expected_payload.encode("utf-8")).hexdigest()
    try:
        _initialize_metadata(ledger_path)
        connection = sqlite3.connect(ledger_path)
        try:
            existing = connection.execute(
                f"SELECT seal_json, seal_sha256 FROM {_SEAL_TABLE} "
                "WHERE session_date = ?",
                (session_date.isoformat(),),
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error as error:
        raise PaperAccountChainError("unable to inspect historical PAPER seal") from error
    if existing is None:
        return
    if str(existing[0]) != expected_payload or str(existing[1]) != expected_digest:
        raise PaperAccountChainError(
            "historical PAPER ledger no longer matches its immutable seal"
        )


def _seal_payload(
    *,
    account_id: str,
    session_date: date,
    event_hashes: tuple[str, ...],
) -> str:
    return _canonical_json(
        {
            "account_id_sha256": _account_hash(account_id),
            "event_count": len(event_hashes),
            "last_event_hash": event_hashes[-1] if event_hashes else None,
            "schema": "paper-account-ledger-seal@1",
            "session_date": session_date.isoformat(),
        }
    )


@contextmanager
def _metadata_transaction(ledger_path: Path) -> Iterator[sqlite3.Connection]:
    _initialize_metadata(ledger_path)
    connection = sqlite3.connect(ledger_path, timeout=15.0, isolation_level=None)
    try:
        connection.execute("PRAGMA busy_timeout = 15000")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield connection
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()
    finally:
        connection.close()


def _initialize_metadata(ledger_path: Path) -> None:
    try:
        connection = sqlite3.connect(ledger_path, timeout=15.0, isolation_level=None)
        try:
            connection.execute("PRAGMA busy_timeout = 15000")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_BINDING_TABLE} (
                    session_date TEXT PRIMARY KEY,
                    document_json TEXT NOT NULL,
                    document_sha256 TEXT NOT NULL
                )
                """
            )
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_SEAL_TABLE} (
                    session_date TEXT PRIMARY KEY,
                    seal_json TEXT NOT NULL,
                    seal_sha256 TEXT NOT NULL
                )
                """
            )
            for table in (_BINDING_TABLE, _SEAL_TABLE):
                connection.execute(
                    f"""
                    CREATE TRIGGER IF NOT EXISTS {table}_no_update
                    BEFORE UPDATE ON {table}
                    BEGIN
                        SELECT RAISE(ABORT, 'paper lineage metadata is append-only');
                    END
                    """
                )
                connection.execute(
                    f"""
                    CREATE TRIGGER IF NOT EXISTS {table}_no_delete
                    BEFORE DELETE ON {table}
                    BEGIN
                        SELECT RAISE(ABORT, 'paper lineage metadata is append-only');
                    END
                    """
                )
            # 只有事件自身 session 对应“已 binding、未 seal”的交易日才允许
            # 追加。旧日一经封存就直接拒绝；clone 在提交新日 binding 前也不能
            # 被 runner 抢先写入，因此不依赖目录扫描与 writer 的运行时序。
            connection.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS {_SEALED_LEDGER_TRIGGER}
                BEFORE INSERT ON paper_ledger_events
                WHEN NOT EXISTS (
                    SELECT 1 FROM {_BINDING_TABLE} AS binding
                    WHERE binding.session_date = NEW.session_date
                    AND NOT EXISTS (
                        SELECT 1 FROM {_SEAL_TABLE} AS seal
                        WHERE seal.session_date = binding.session_date
                    )
                )
                BEGIN
                    SELECT RAISE(ABORT, 'historical paper ledger is sealed');
                END
                """
            )
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()
    except sqlite3.Error as error:
        raise PaperAccountChainError("unable to initialize PAPER lineage metadata") from error


def _validate_count_and_tail(count: int, tail: object, *, context: str) -> None:
    if tail is not None and (
        not isinstance(tail, str)
        or len(tail) != 64
        or any(character not in "0123456789abcdef" for character in tail)
    ):
        raise PaperAccountChainError(f"{context} tail hash is invalid")
    if (count == 0) != (tail is None):
        raise PaperAccountChainError(f"{context} event count and tail hash disagree")


def _canonical_json(document: Mapping[str, object], *, indent: int | None = None) -> str:
    return json.dumps(
        dict(document),
        ensure_ascii=False,
        sort_keys=True,
        indent=indent,
        separators=None if indent is not None else (",", ":"),
    )


def _account_hash(account_id: str) -> str:
    return hashlib.sha256(account_id.encode("utf-8")).hexdigest()


def _fsync_path_and_parent(path: Path) -> None:
    # Windows 的 CRT 对只读 descriptor 调用 fsync 会返回 EBADF；文件由本进程
    # 创建且只做 flush，因此以 r+b 打开但不写入任何字节。
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
