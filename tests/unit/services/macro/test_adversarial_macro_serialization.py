from decimal import Decimal

from gribuki_trade.services import adversarial_macro as facade
from gribuki_trade.services import adversarial_macro_serialization as serialization


def test_adversarial_facade_reexports_pure_serialization_helpers() -> None:
    assert facade._document_sha256 is serialization._document_sha256
    assert facade._analysis_document is serialization._analysis_document
    assert facade._request_sha256 is serialization._request_sha256


def test_document_hash_and_scalar_helpers_are_deterministic() -> None:
    first = serialization._document_sha256({"b": 2, "a": 1})
    second = serialization._document_sha256({"a": 1, "b": 2})

    assert first == second
    assert serialization._single_line("  alpha\n beta  ") == "alpha beta"
    assert serialization._unique_text((" a ", "a", "b")) == ("a", "b")
    assert serialization._median((Decimal("1"), Decimal("3"))) == Decimal("2")
