from __future__ import annotations

from decimal import Decimal

from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import PaperInstrumentType
from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.services.ashare.ashare_intraday_paper import (
    IntradayPaperRiskConfig as FacadeRiskConfig,
)
from gribuki_trade.services.ashare.ashare_intraday_paper import (
    IntradayPriceAcceptance as FacadePriceAcceptance,
)
from gribuki_trade.services.ashare.ashare_intraday_policy import (
    IntradayPaperRiskConfig,
    IntradayPriceAcceptance,
    ashare_daily_price_band,
    build_intraday_buy_price_acceptance,
    build_intraday_sell_price_acceptance,
)


def test_policy_types_are_reexported_by_historical_execution_facade() -> None:
    assert FacadeRiskConfig is IntradayPaperRiskConfig
    assert FacadePriceAcceptance is IntradayPriceAcceptance


def test_price_band_uses_conservative_board_rules() -> None:
    assert ashare_daily_price_band(
        Decimal("10"), AShareBoard.SSE_MAIN, Decimal("0.01")
    ) == (Decimal("9.00"), Decimal("11.00"))
    assert ashare_daily_price_band(
        Decimal("10"), AShareBoard.STAR, Decimal("0.001")
    ) == (Decimal("8.000"), Decimal("12.000"))


def test_buy_and_sell_acceptance_are_closed_immutable_corridors() -> None:
    buy = build_intraday_buy_price_acceptance(
        reference_price=Decimal("10"),
        invalidation_price=Decimal("9.80"),
        previous_close=Decimal("10"),
        board=AShareBoard.SSE_MAIN,
        instrument_type=PaperInstrumentType.STOCK,
    )
    sell = build_intraday_sell_price_acceptance(
        reference_price=Decimal("10"),
        previous_close=Decimal("10"),
        board=AShareBoard.SSE_MAIN,
    )

    assert buy.side is Side.BUY
    assert buy.acceptable_lower == Decimal("9.81")
    assert buy.acceptable_upper == Decimal("10.01")
    assert sell.side is Side.SELL
    assert sell.acceptable_lower == Decimal("9.99")
    assert sell.acceptable_upper == Decimal("11.00")
    assert buy.contains(Decimal("9.81"))
    assert not buy.contains(Decimal("9.80"))
    assert sell.contains(Decimal("11.00"))
