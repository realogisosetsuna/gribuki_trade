from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.reporting.paper_day_codec import (
    list_length,
    optional_bool,
    optional_decimal,
    optional_int,
    optional_object,
    optional_string,
    read_events,
    read_json_object,
    required_date,
    required_datetime,
    required_string,
)
from gribuki_trade.reporting.paper_day_formatting import (
    deep_selected_system_text,
    local_time,
    pairs,
    readable_code,
)
from gribuki_trade.reporting.paper_day_summary import PaperDaySidecarError


def test_scalar_codecs_reject_ambiguous_json_values() -> None:
    assert optional_string(" BTCUSDT ") == " BTCUSDT "
    assert optional_string(" ") is None
    assert optional_int(3) == 3
    assert optional_int(True) is None
    assert optional_bool(False) is False
    assert optional_decimal("1.25") == Decimal("1.25")
    assert optional_decimal("Infinity") is None
    assert optional_object({"symbol": "BTCUSDT"}) == {"symbol": "BTCUSDT"}
    assert optional_object([]) is None
    assert list_length([1, 2]) == 2
    assert list_length("not a list") is None


def test_required_codecs_normalize_timezone_and_validate_shape() -> None:
    assert required_string({"run_id": "run-1"}, "run_id", "INVALID") == "run-1"
    assert required_date({"session_date": "2026-08-14"}, "session_date", "INVALID") == date(
        2026, 8, 14
    )
    assert required_datetime(
        {"known_at": "2026-08-14T08:00:00+08:00"}, "known_at", 1
    ) == datetime(2026, 8, 14, 0, 0, tzinfo=UTC)
    with pytest.raises(PaperDaySidecarError, match="missing or invalid run_id"):
        required_string({}, "run_id", "INVALID")
    with pytest.raises(PaperDaySidecarError, match="naive known_at"):
        required_datetime({"known_at": "2026-08-14T08:00:00"}, "known_at", 1)


def test_event_reader_ignores_only_trailing_partial_append(tmp_path: Path) -> None:
    event = (
        '{"sequence":1,"event_id":"evt-1","event_type":"DAY_STARTED",'
        '"known_at":"2026-08-14T00:00:00Z",'
        '"occurred_at":"2026-08-14T00:00:00Z","payload":{},'
        '"phase":"BOOTSTRAP","severity":"INFO"}\n'
    )
    path = tmp_path / "session.log.jsonl"
    path.write_bytes((event + '{"sequence":2').encode("utf-8"))

    events, warnings = read_events(path)

    assert [item.event_id for item in events] == ["evt-1"]
    assert warnings == ("忽略了 session.log.jsonl 末尾一个尚未完成的并发追加片段。",)


def test_json_object_reader_reports_invalid_shape(tmp_path: Path) -> None:
    path = tmp_path / "status.json"
    path.write_text("[]", encoding="utf-8")

    with pytest.raises(PaperDaySidecarError, match="must contain a JSON object"):
        read_json_object(path, "STATUS_FILE_INVALID")


def test_formatting_boundary_keeps_audit_text_human_readable() -> None:
    assert readable_code("LLM_REVIEW_FAILED:model-x").endswith(
        "（审计参数：model-x）"
    )
    rendered_pairs = pairs((("A", 2), ("B", 1)))
    assert "=2" in rendered_pairs and "=1" in rendered_pairs and "、" in rendered_pairs
    assert deep_selected_system_text("BASELINE_LLM") == "原单分析器"
    assert local_time(datetime(2026, 8, 14, 0, 0, tzinfo=UTC)) == "08:00:00"
