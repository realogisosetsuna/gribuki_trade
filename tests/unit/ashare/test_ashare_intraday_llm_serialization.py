"""盘中 LLM 审计序列化模块的纯函数契约。"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

import pytest

from gribuki_trade.services.ashare import ashare_intraday_llm as facade
from gribuki_trade.services.ashare import ashare_intraday_llm_serialization as serialization


class _Decision(StrEnum):
    APPROVED = "APPROVED"


def test_facade_preserves_serialization_helpers_as_compatibility_aliases() -> None:
    assert facade.intraday_llm_document_sha256 is serialization.intraday_llm_document_sha256
    assert facade.intraday_llm_review_document is serialization.intraday_llm_review_document
    assert facade._normalize_json is serialization._normalize_json  # noqa: SLF001
    assert facade._macro_analysis_document is serialization._macro_analysis_document  # noqa: SLF001


def test_document_hash_is_stable_for_decimal_enum_and_timezone_values() -> None:
    first = {
        "when": datetime(2026, 9, 9, 12, 0, tzinfo=UTC),
        "score": Decimal("1.2500"),
        "decision": _Decision.APPROVED,
    }
    equivalent = {
        "decision": "APPROVED",
        "score": "1.2500",
        "when": "2026-09-09T12:00:00+00:00",
    }
    assert serialization.intraday_llm_document_sha256(first) == (
        serialization.intraday_llm_document_sha256(equivalent)
    )


def test_document_hash_rejects_non_finite_and_naive_values() -> None:
    with pytest.raises(ValueError, match="non-finite floats"):
        serialization.intraday_llm_document_sha256({"score": float("nan")})
    with pytest.raises(ValueError, match="timezone-aware"):
        serialization.intraday_llm_document_sha256(
            {"when": datetime(2026, 9, 9, 12, 0)}
        )
    with pytest.raises(ValueError, match="non-empty strings"):
        serialization.intraday_llm_document_sha256({"": "invalid"})
