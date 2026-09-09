from __future__ import annotations

import gribuki_trade.services.ashare.ashare_paper_day as paper_day
import gribuki_trade.services.ashare.ashare_paper_day_reasons as reasons


def test_reason_display_module_preserves_facade_identity_and_fallbacks() -> None:
    """原因映射可以独立导航，同时历史 runner facade 保留对象身份。"""

    for name in (
        "_ENTRY_REJECTION_EXPLANATIONS",
        "_MATCH_REASON_EXPLANATIONS",
        "_entry_rejection_display",
        "_match_reason_display",
    ):
        assert getattr(paper_day, name) is getattr(reasons, name)

    assert reasons._entry_rejection_display(None) == "入场门未通过，具体原因不可用"
    assert reasons._entry_rejection_display("UNKNOWN") == "入场门未通过"
    assert reasons._match_reason_display("UNKNOWN") == "未满足单分钟 IOC 成交条件"


def test_reason_display_module_keeps_known_audit_text() -> None:
    """已记录的原因码仍然使用原来的可读说明。"""

    assert (
        reasons._entry_rejection_display("SIGNAL_NOT_COMPLETED")
        == "信号所用分钟线尚未完整收盘"
    )
    assert (
        reasons._match_reason_display("LIMIT_NOT_TOUCHED")
        == "撮合分钟没有触及买入限价"
    )
