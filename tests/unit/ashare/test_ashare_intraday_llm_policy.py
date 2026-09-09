from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from gribuki_trade.services.ashare.ashare_intraday_llm_policy import (
    aware_utc,
    bounded_score,
    canonical_symbol,
    context_id,
    stable_failure_code,
)


def test_scalar_policy_normalizes_and_bounds_values() -> None:
    assert canonical_symbol(" 600000.sh ") == "600000.SH"
    assert stable_failure_code(" llm_timeout ") == "LLM_TIMEOUT"
    assert bounded_score(Decimal("2")) == Decimal("1")
    assert aware_utc(datetime(2026, 8, 14, 9, tzinfo=UTC), "at").tzinfo is UTC


def test_context_identity_changes_when_frozen_input_changes() -> None:
    base = dict(
        session_date=date(2026, 8, 14),
        symbol="600000.SH",
        preopen_context_id="preopen-1",
        scan_revision="scan-1",
        candidate_scope_sha256="a" * 64,
        evidence_as_of=datetime(2026, 8, 14, 2, tzinfo=UTC),
        valid_until=datetime(2026, 8, 14, 2, 20, tzinfo=UTC),
        plan_manifest_sha256="b" * 64,
        config_sha256="c" * 64,
    )
    first = context_id(**base)
    second = context_id(**{**base, "scan_revision": "scan-2"})
    assert first.startswith("intraday-llm-context-")
    assert first != second


def test_scalar_policy_rejects_unsafe_values() -> None:
    with pytest.raises(ValueError):
        canonical_symbol("600000")
    with pytest.raises(ValueError):
        stable_failure_code("provider body")
