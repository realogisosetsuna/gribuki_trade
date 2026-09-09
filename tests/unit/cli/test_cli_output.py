from decimal import Decimal

from gribuki_trade.cli_output import atomic_write_json, decimal_text, three_decimal_text


def test_decimal_output_helpers_are_stable() -> None:
    assert decimal_text(Decimal("1.2300")) == "1.2300"
    assert decimal_text(None) is None
    assert three_decimal_text(Decimal("1.2349")) == "1.235"


def test_atomic_json_output_uses_utf8_and_newline(tmp_path) -> None:
    target = tmp_path / "nested" / "result.json"
    atomic_write_json(target, {"消息": "可读", "value": "1"})
    assert target.read_text(encoding="utf-8").endswith("\n")
    assert '"消息": "可读"' in target.read_text(encoding="utf-8")
