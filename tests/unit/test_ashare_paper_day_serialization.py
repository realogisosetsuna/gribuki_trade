from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from gribuki_trade.domain.paper_day import PaperDayPhase, PaperDaySeverity
from gribuki_trade.services.ashare import ashare_paper_day as facade
from gribuki_trade.services.ashare import ashare_paper_day_serialization as serialization


def test_paper_day_facade_reexports_pure_serialization_helpers() -> None:
    """历史导入路径继续指向拆出的纯函数，便于逐步迁移调用方。"""

    assert facade._document_sha256 is serialization._document_sha256
    assert facade._event_jsonl is serialization._event_jsonl


def test_document_sha256_is_canonical_for_mapping_order() -> None:
    first = serialization._document_sha256({"b": 2, "a": 1})
    second = serialization._document_sha256({"a": 1, "b": 2})
    assert first == second
    assert len(first) == 64


def test_event_jsonl_contains_stable_event_fields() -> None:
    event = SimpleNamespace(
        correlation_id="corr-1",
        event_id="event-1",
        event_type="TEST",
        known_at=datetime(2026, 1, 2, 3, 4, tzinfo=UTC),
        occurred_at=datetime(2026, 1, 2, 3, 3, tzinfo=UTC),
        payload={"x": 1},
        phase=PaperDayPhase.MORNING,
        sequence=1,
        severity=PaperDaySeverity.INFO,
        symbol="000001.SZ",
    )

    encoded = serialization._event_jsonl(event)
    assert encoded.endswith("\n")
    assert '"event_id": "event-1"' in encoded
    assert '"phase": "MORNING"' in encoded
    assert '"severity": "INFO"' in encoded


def test_atomic_write_text_replaces_existing_file(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "status.json"
    serialization._atomic_write_text(target, "第一版")
    serialization._atomic_write_text(target, "第二版")
    assert target.read_text(encoding="utf-8") == "第二版"
