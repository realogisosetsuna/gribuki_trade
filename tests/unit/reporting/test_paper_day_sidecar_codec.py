from datetime import date
from pathlib import Path

from gribuki_trade.reporting.paper_day.paper_day_sidecar_codec import (
    int_tuple,
    read_final_result,
    string_tuple,
)


def test_sidecar_tuple_codecs_filter_invalid_values() -> None:
    assert string_tuple(["600000", 1, None, "000001"]) == ("600000", "000001")
    assert int_tuple([1, True, "2", 3]) == (1, 3)
    assert string_tuple("not-an-array") == ()


def test_read_final_result_scans_stdout_and_checks_identity(tmp_path: Path) -> None:
    root = tmp_path
    (root / "runner.stdout.log").write_text(
        "log line\n"
        '{"ok":true,"action":"run","run_id":"other",'
        '"session_date":"2026-09-09"}\n'
        '{"ok":true,"action":"run","run_id":"run-1",'
        '"session_date":"2026-09-09","orders":2}\n',
        encoding="utf-8",
    )
    result = read_final_result(root, date(2026, 9, 9), "run-1")
    assert result is not None
    assert result["orders"] == 2


def test_read_final_result_ignores_mismatched_documents(tmp_path: Path) -> None:
    (tmp_path / "result.json").write_text(
        '{"ok":true,"action":"run","run_id":"other",'
        '"session_date":"2026-09-09"}',
        encoding="utf-8",
    )
    assert read_final_result(tmp_path, date(2026, 9, 9), "run-1") is None
