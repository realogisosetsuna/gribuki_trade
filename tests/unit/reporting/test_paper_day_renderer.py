"""验证 PAPER 日报渲染职责的纯函数边界。"""

from decimal import Decimal

from gribuki_trade.reporting import paper_day_summary as facade
from gribuki_trade.reporting.paper_day_renderer import (
    _daily_band,
    _explained_codes,
    _position_limit_display,
    _price_range,
    render_paper_day_summary,
)
from gribuki_trade.reporting.paper_day_summary import PaperDayPriceAcceptanceProjection


def test_renderer_helpers_are_pure_and_preserve_facade_compatibility() -> None:
    acceptance = PaperDayPriceAcceptanceProjection(
        status="ACCEPTED",
        side="BUY",
        board="SSE_MAIN",
        acceptable_lower=Decimal("10.000"),
        acceptable_upper=Decimal("10.500"),
        exchange_lower=Decimal("9.900"),
        exchange_upper=Decimal("11.000"),
        invalidation_boundary=Decimal("9.800"),
        limit_price=Decimal("10.200"),
        reference_price=Decimal("10.000"),
        price_tick=Decimal("0.001"),
        policy_version="v1",
        price_cage_status="VERIFIED",
        real_broker_submission_allowed=False,
    )

    assert _price_range(acceptance) == "[10.000, 10.500]"
    assert _daily_band(acceptance) == "[9.900, 11.000]"
    assert _position_limit_display(None, False) == "无固定上限（计数熔断关闭）"
    assert _position_limit_display(3, True) == "3 个"
    assert _explained_codes(("UNKNOWN_STABLE_CODE",)).startswith("`UNKNOWN_STABLE_CODE`")

    # 历史私有名称仍通过 facade 的模块级兼容转发可用。
    assert facade._price_range(acceptance) == _price_range(acceptance)  # noqa: SLF001
    assert facade._position_limit_display(3, True) == "3 个"  # noqa: SLF001
    assert callable(render_paper_day_summary)
