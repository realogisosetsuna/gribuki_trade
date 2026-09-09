from __future__ import annotations

from types import SimpleNamespace

from gribuki_trade.analysis.schemas import MacroAnalysisDecision
from gribuki_trade.services.macro import adversarial_macro as facade
from gribuki_trade.services.macro import adversarial_macro_policy as policy


def _opinion(role: str, *, claim: str = "claim") -> SimpleNamespace:
    analysis = SimpleNamespace(
        decision=MacroAnalysisDecision.PUBLISH,
        technical_alignment=0,
        macro_impact=0,
        claims=(SimpleNamespace(text=claim, evidence_ids=("e1",)),),
        invalidation_conditions=("condition",),
    )
    return SimpleNamespace(role=role, analysis=analysis)


def test_facade_keeps_extracted_policy_helpers_compatible() -> None:
    assert facade._role_output_failure is policy._role_output_failure
    assert facade._peer_document is policy._peer_document
    assert facade._rounds_are_stable is policy._rounds_are_stable
    assert facade._round_signature is policy._round_signature


def test_peer_document_is_bounded_and_excludes_receiving_role() -> None:
    previous = SimpleNamespace(
        round_number=1,
        opinions=(_opinion("A"), _opinion("B", claim=" peer\nclaim ")),
    )

    document = policy._peer_document(previous, receiving_role="A")

    assert document["round_number"] == 1
    arguments = document["arguments"]
    assert isinstance(arguments, list)
    assert len(arguments) == 1
    assert arguments[0]["role"] == "B"
    assert arguments[0]["claims"][0]["text"] == "peer claim"


def test_round_signature_is_ordered_and_stable_for_equal_values() -> None:
    first = SimpleNamespace(round_number=1, opinions=(_opinion("A"),))
    same = SimpleNamespace(round_number=99, opinions=(_opinion("A"),))
    changed = SimpleNamespace(round_number=1, opinions=(_opinion("A", claim="changed"),))

    assert policy._rounds_are_stable(first, same)
    assert not policy._rounds_are_stable(first, changed)
