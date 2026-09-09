from __future__ import annotations

import importlib

import pytest


def test_paper_day_facade_preserves_llm_payload_helper_aliases() -> None:
    facade = importlib.import_module("gribuki_trade.services.ashare_paper_day")
    payloads = importlib.import_module(
        "gribuki_trade.services.ashare.ashare_paper_day_llm_payloads"
    )

    assert facade._llm_review_from_document is payloads._llm_review_from_document
    assert facade._llm_preopen_context_from_document is payloads._llm_preopen_context_from_document
    assert facade._llm_analyzer_identity_document is payloads._llm_analyzer_identity_document
    assert facade._llm_gate_document is payloads.llm_gate_document


def test_llm_payload_type_guards_are_strict_and_side_effect_free() -> None:
    payloads = importlib.import_module(
        "gribuki_trade.services.ashare.ashare_paper_day_llm_payloads"
    )

    with pytest.raises(TypeError, match="claims must be a list"):
        payloads._llm_object_list({}, "claims")
    with pytest.raises(TypeError, match="name must be a string"):
        payloads._llm_string(1, "name")
    with pytest.raises(TypeError, match="count must be an integer"):
        payloads._llm_int(True, "count")
    with pytest.raises(ValueError, match="finite"):
        payloads._llm_decimal("NaN", "score")
