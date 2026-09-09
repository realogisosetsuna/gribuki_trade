"""PAPER lineage 的纯文档协议及连续性边界回归。"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from gribuki_trade.runtime import paper_account_chain as chain
from gribuki_trade.runtime import paper_account_manifest as manifest

_ACCOUNT = "测试账户"
_PRIOR_DATE = date(2026, 8, 14)
_SESSION_DATE = date(2026, 8, 17)
_HASHES = ("a" * 64, "b" * 64)


def _preparation() -> manifest.PaperAccountLedgerPreparation:
    return manifest.PaperAccountLedgerPreparation(
        ledger_path=Path("not-created") / "ledger.sqlite3",
        account_id=_ACCOUNT,
        session_date=_SESSION_DATE,
        origin="VERIFIED_PRIOR_SESSION_CLONE",
        source_session_date=_PRIOR_DATE,
        source_event_count=len(_HASHES),
        source_last_event_hash=_HASHES[-1],
    )


def _source() -> manifest.LedgerCandidate:
    return manifest.LedgerCandidate(
        directory_date=_PRIOR_DATE,
        path=Path("prior-not-created") / "ledger.sqlite3",
        event_hashes=_HASHES,
        projected_session_date=_PRIOR_DATE,
        lineage=replace(
            _preparation(),
            session_date=_PRIOR_DATE,
            origin="NEW_ACCOUNT",
            source_session_date=None,
            source_event_count=0,
            source_last_event_hash=None,
        ),
    )


def test_document_round_trip_preserves_bytes_and_compatibility_exports() -> None:
    prepared = _preparation()
    document = prepared.audit_document()
    parsed = manifest.preparation_from_document(
        document,
        ledger_path=Path("relocated") / "ledger.sqlite3",
        expected_session_date=_SESSION_DATE,
        expected_account_id=_ACCOUNT,
    )

    assert parsed.audit_document() == document
    assert "ledger_path" not in document
    assert "account_id" not in document
    assert manifest.canonical_json(document) == json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    assert chain.PaperAccountChainError is manifest.PaperAccountChainError
    assert chain.PaperAccountLedgerPreparation is manifest.PaperAccountLedgerPreparation
    assert chain._preparation_from_document is manifest.preparation_from_document
    assert chain._canonical_json is manifest.canonical_json
    assert chain._seal_payload is manifest.seal_payload


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("unknown", None, "fields"),
        ("schema", "paper-account-chain@2", "schema"),
        ("account_id_sha256", "c" * 64, "account"),
        ("session_date", "2026-08-18", "session"),
        ("origin", "UNVERIFIED", "origin"),
        ("source_event_count", True, "event count"),
        ("source_event_count", -1, "event count"),
        ("source_event_count", 0, "disagree"),
        ("source_last_event_hash", "B" * 64, "tail hash"),
        ("source_last_event_hash", None, "disagree"),
        ("source_session_date", None, "identify its source"),
        ("source_session_date", "2026-08-17", "precede"),
    ],
)
def test_untrusted_manifest_fields_fail_closed(field: str, value: object, message: str) -> None:
    document = _preparation().audit_document()
    document[field] = value

    with pytest.raises(chain.PaperAccountChainError, match=message):
        manifest.preparation_from_document(
            document,
            ledger_path=Path("not-created") / "ledger.sqlite3",
            expected_session_date=_SESSION_DATE,
            expected_account_id=_ACCOUNT,
        )


def test_full_source_prefix_is_verified_even_when_count_and_tail_match() -> None:
    prepared = _preparation()
    source = _source()
    state = manifest.LedgerState((_ACCOUNT,), (*_HASHES, "c" * 64), _SESSION_DATE)
    manifest.verify_lineage_source_binding(prepared, state=state, prior_candidates=(source,))
    manifest.verify_existing_ledger_matches_lineage(state, prepared)

    divergent = replace(state, event_hashes=("d" * 64, _HASHES[-1], "c" * 64))
    with pytest.raises(manifest.PaperAccountChainError, match="declared source stream"):
        manifest.verify_lineage_source_binding(
            prepared, state=divergent, prior_candidates=(source,)
        )


def test_historical_prefix_cannot_shrink_or_diverge() -> None:
    source = _source()
    following = replace(
        source,
        directory_date=_SESSION_DATE,
        projected_session_date=_SESSION_DATE,
        event_hashes=(*_HASHES, "c" * 64),
    )
    manifest.verify_prefix_chain((source, following))

    for hashes in ((_HASHES[0],), ("d" * 64, _HASHES[-1])):
        with pytest.raises(manifest.PaperAccountChainError, match="divergent"):
            manifest.verify_prefix_chain((source, replace(following, event_hashes=hashes)))


def test_legacy_lineage_cannot_replace_a_known_historical_source() -> None:
    legacy = replace(
        _preparation(), origin="LEGACY_SESSION_LOCAL", source_session_date=None
    )
    state = manifest.LedgerState((_ACCOUNT,), _HASHES, _SESSION_DATE)

    with pytest.raises(manifest.PaperAccountChainError, match="cannot discard"):
        manifest.verify_lineage_source_binding(legacy, state=state, prior_candidates=(_source(),))
