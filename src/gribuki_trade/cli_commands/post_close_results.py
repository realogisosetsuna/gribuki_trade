"""A 股盘后 CLI 流程使用的纯结果契约。

命令 facade 继续向调用方和测试暴露历史私有名称；校验及结果整形规则集中
在这个不依赖服务、适配器或文件系统编排的小模块中。
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path


class _PostCloseCLIError(RuntimeError):
    """不泄露供应商或凭据细节的稳定盘后失败。"""

    def __init__(self, code: str, *, retryable: bool = False) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(f"A-share post-close command failed ({code})")


def _post_close_optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        resolved = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return resolved if resolved.is_finite() else None


def _post_close_optional_integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        resolved = int(str(value))
    except (TypeError, ValueError):
        return None
    return resolved if resolved >= 0 else None


def _post_close_optional_string(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _post_close_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _post_close_string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(item for item in value if isinstance(item, str) and item.strip())


def _post_close_analysis_outcome(
    *,
    held_count: int,
    research_completed: int,
    research_failed: int,
) -> str:
    if min(held_count, research_completed, research_failed) < 0:
        raise _PostCloseCLIError("POST_CLOSE_RESEARCH_RESULT_INVALID")
    if research_completed + research_failed != held_count:
        raise _PostCloseCLIError("POST_CLOSE_RESEARCH_RESULT_INVALID")
    if held_count == 0:
        return "NOT_APPLICABLE"
    if research_completed == 0:
        return "FAILED"
    return "PARTIAL" if research_failed else "COMPLETE"


def _post_close_completed_result(
    post_root: Path,
    status: Mapping[str, object],
    *,
    idempotent_replay: bool,
) -> dict[str, object]:
    retained = {
        key: status.get(key)
        for key in (
            "artifact_path",
            "artifact_sha256",
            "analysis_outcome",
            "completed_at",
            "held_count",
            "next_session",
            "paper_run_id",
            "phase",
            "research_completed",
            "research_failed",
            "run_id",
            "session_date",
            "target_hash",
            "text_part_count",
        )
    }
    return {
        "action": "run",
        "idempotent_replay": idempotent_replay,
        "ok": True,
        "runtime_dir": str(post_root),
        **retained,
    }


def _post_close_skipped(
    session_date: date,
    post_root: Path,
    code: str,
) -> dict[str, object]:
    return {
        "action": "run",
        "error_code": code,
        "ok": True,
        "runtime_dir": str(post_root),
        "session_date": session_date.isoformat(),
        "skipped": True,
    }


def _post_close_error(
    action: str,
    session_date: date,
    post_root: Path,
    code: str,
    *,
    retryable: bool = False,
) -> dict[str, object]:
    return {
        "action": action,
        "error_code": code,
        "ok": False,
        "retryable": retryable,
        "runtime_dir": str(post_root),
        "session_date": session_date.isoformat(),
    }
