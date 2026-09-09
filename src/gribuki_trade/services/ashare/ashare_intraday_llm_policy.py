"""A 股盘中 LLM 复核使用的纯身份与输入校验策略。

本模块不调用模型、不调度后台任务，也不修改复核状态；它只生成上下文/复核
身份，并集中处理稳定代码、股票代码、分数和 UTC 时间的边界校验。
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from gribuki_trade.services.ashare.ashare_intraday_llm_serialization import (
    intraday_llm_document_sha256,
)

_SYMBOL = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_FAILURE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,79}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def context_id(
    *,
    session_date: date,
    symbol: str,
    preopen_context_id: str,
    scan_revision: str,
    candidate_scope_sha256: str,
    evidence_as_of: datetime,
    valid_until: datetime,
    plan_manifest_sha256: str,
    config_sha256: str,
) -> str:
    """为冻结的盘中上下文输入生成稳定身份。"""

    digest = intraday_llm_document_sha256(
        {
            "candidate_scope_sha256": candidate_scope_sha256,
            "config_sha256": config_sha256,
            "evidence_as_of": evidence_as_of,
            "plan_manifest_sha256": plan_manifest_sha256,
            "preopen_context_id": preopen_context_id,
            "scan_revision": scan_revision,
            "schema_version": 1,
            "session_date": session_date,
            "symbol": symbol,
            "valid_until": valid_until,
        }
    )
    return "intraday-llm-context-" + digest[:40]


def review_id(
    *,
    context_id: str,
    analysis: Any,
    completed_at: datetime,
    failure_code: str | None,
) -> str:
    """为模型结果和完成时间生成稳定复核身份。"""

    digest = intraday_llm_document_sha256(
        {
            "analysis_id": analysis.analysis_id,
            "completed_at": completed_at,
            "context_id": context_id,
            "failure_code": failure_code,
            "macro_impact": analysis.macro_impact,
            "model_version": analysis.model_version,
            "schema_version": 1,
        }
    )
    return "intraday-llm-review-" + digest[:40]


def stable_failure_code(value: str) -> str:
    """规范化可持久化且不泄漏提供方文本的失败码。"""

    normalized = value.strip().upper()
    if not _FAILURE_CODE.fullmatch(normalized):
        raise ValueError("failure code must use stable uppercase identifier form")
    return normalized


def bounded_score(value: Decimal) -> Decimal:
    """把模型分数限制在协议允许的 [-1, 1] 范围。"""

    return max(Decimal("-1"), min(Decimal("1"), value))


def canonical_symbol(value: str) -> str:
    """校验 A 股代码的交易所后缀格式。"""

    normalized = value.strip().upper()
    if not _SYMBOL.fullmatch(normalized):
        raise ValueError("symbol must use canonical 000001.SZ form")
    return normalized


def aware_utc(value: datetime, name: str) -> datetime:
    """要求时间带时区并统一为 UTC。"""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "aware_utc",
    "bounded_score",
    "canonical_symbol",
    "context_id",
    "review_id",
    "stable_failure_code",
]
