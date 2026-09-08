from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from gribuki_trade.cli_commands.close_research_payloads import (
    _close_batch_result_summary,
    _daily_evidence_provider_id,
    _instrument_profile_document,
    _paper_session_instrument_profiles,
)
from gribuki_trade.domain.instruments import ResearchInstrumentProfile


def test_close_batch_result_summary_keeps_contract_fields_only() -> None:
    result = _close_batch_result_summary(
        "600000.SH",
        {
            "ok": True,
            "decision": "HOLD",
            "recommendation_id": "rec-1",
            "private_detail": "must not leak",
            "symbol": " 600001.SZ ",
        },
    )
    assert result == {
        "decision": "HOLD",
        "ok": True,
        "recommendation_id": "rec-1",
        "symbol": " 600001.SZ ",
    }


def test_daily_evidence_provider_id_is_stable_and_url_free() -> None:
    assert _daily_evidence_provider_id(None) == "baostock.daily"
    assert _daily_evidence_provider_id("MIXED/TAIL_STITCH base=AKShare") == (
        "mixed.tail_stitch.daily"
    )
    assert _daily_evidence_provider_id("AKShare/Sina") == "akshare.daily"
    assert _daily_evidence_provider_id("provider:https://secret.example") == "other.research.daily"


def test_instrument_profile_document_is_json_friendly() -> None:
    profile = ResearchInstrumentProfile(
        symbol="600000.SH",
        name="浦发银行",
        market="A-share",
        asset_type="stock",
        exchange="sse",
        board="sse_main",
        size_tier="large",
        industry="banking",
        styles=("value",),
        research_role="held-position-post-close-review",
        risk_tags=("DEGRADED_PROFILE",),
        source_id="PAPER_SESSION_ARCHIVE",
        verified_on=date(2026, 9, 9),
        background_facts=("archived",),
    )
    document = _instrument_profile_document(profile)
    assert document is not None
    assert document["symbol"] == "600000.SH"
    assert document["verified_on"] == "2026-09-09"
    assert document["styles"] == ["value"]
    assert _instrument_profile_document(None) is None


def test_paper_session_instrument_profiles_reject_conflicting_archive_types() -> None:
    projection = SimpleNamespace(
        session_date=date(2026, 9, 9),
        events=(
            SimpleNamespace(
                event_type="WATCHLIST_UPDATED",
                payload={
                    "watchlist": [
                        {"symbol": "600000.SH", "name": "浦发银行", "board": "SSE_MAIN"}
                    ]
                },
            ),
            SimpleNamespace(
                event_type="FILL_STARTED",
                payload={
                    "fill": {"symbol": "600000.SH", "instrument_type": "etf"}
                },
            ),
        ),
    )
    assert _paper_session_instrument_profiles(projection, {"600000.SH": "stock"}) == {}
