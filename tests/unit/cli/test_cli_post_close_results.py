from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.cli_commands.post_close_results import (
    _post_close_analysis_outcome,
    _post_close_completed_result,
    _post_close_error,
    _post_close_optional_decimal,
    _post_close_optional_integer,
    _post_close_optional_string,
    _post_close_skipped,
    _PostCloseCLIError,
)


def test_post_close_scalar_coercion_is_fail_closed() -> None:
    assert _post_close_optional_decimal("1.25") == Decimal("1.25")
    assert _post_close_optional_decimal("NaN") is None
    assert _post_close_optional_integer("3") == 3
    assert _post_close_optional_integer(True) is None
    assert _post_close_optional_integer("-1") is None
    assert _post_close_optional_string("  ready ") == "ready"
    assert _post_close_optional_string("  ") is None


@pytest.mark.parametrize(
    ("held", "completed", "failed", "expected"),
    [
        (0, 0, 0, "NOT_APPLICABLE"),
        (2, 2, 0, "COMPLETE"),
        (2, 1, 1, "PARTIAL"),
        (2, 0, 2, "FAILED"),
    ],
)
def test_post_close_analysis_outcome(held: int, completed: int, failed: int, expected: str) -> None:
    assert (
        _post_close_analysis_outcome(
            held_count=held,
            research_completed=completed,
            research_failed=failed,
        )
        == expected
    )


def test_post_close_analysis_outcome_rejects_inconsistent_counts() -> None:
    with pytest.raises(_PostCloseCLIError) as captured:
        _post_close_analysis_outcome(held_count=1, research_completed=1, research_failed=1)
    assert captured.value.code == "POST_CLOSE_RESEARCH_RESULT_INVALID"


def test_post_close_result_contracts_are_stable(tmp_path: Path) -> None:
    root = tmp_path / "post-close"
    completed = _post_close_completed_result(
        root,
        {"run_id": "run-1", "phase": "COMPLETED", "ignored": "value"},
        idempotent_replay=True,
    )
    assert completed["ok"] is True
    assert completed["idempotent_replay"] is True
    assert completed["run_id"] == "run-1"
    assert "ignored" not in completed

    skipped = _post_close_skipped(date(2026, 9, 9), root, "BEFORE_1505")
    assert skipped == {
        "action": "run",
        "error_code": "BEFORE_1505",
        "ok": True,
        "runtime_dir": str(root),
        "session_date": "2026-09-09",
        "skipped": True,
    }

    error = _post_close_error("report", date(2026, 9, 9), root, "MISSING", retryable=True)
    assert error["ok"] is False
    assert error["retryable"] is True
