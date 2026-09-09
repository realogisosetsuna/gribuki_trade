import asyncio
from decimal import Decimal
from types import SimpleNamespace

import gribuki_trade.cli as cli
from gribuki_trade.cli_commands.handlers import binance_live


class _Guard:
    def confirm_live_trading(self, _confirmation: str) -> None:
        return None


class _Service:
    client = SimpleNamespace(
        base_url="https://fapi.binance.com",
        stage=cli.BinanceStage.LIVE,
    )

    async def connect(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def validate_order(self, **_kwargs: object) -> dict[str, object]:
        raise cli.BinanceAPIError(
            status_code=400,
            code=-4061,
            message="position side mismatch",
        )


def test_futures_order_test_returns_structured_broker_rejection(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_live_futures_service", lambda: (_Guard(), _Service()))

    result = asyncio.run(
        binance_live._binance_live_futures_order_test(
            symbol="BTCUSDT",
            side="BUY",
            position_side="LONG",
            quantity=Decimal("0.001"),
            order_type="MARKET",
            price=None,
            confirmation="ENABLE LIVE TRADING",
        )
    )

    assert result["order_test"] == "rejected"
    assert result["error_code"] == -4061
    assert result["reason"] == (
        "Binance API error (HTTP 400, code -4061): position side mismatch"
    )
