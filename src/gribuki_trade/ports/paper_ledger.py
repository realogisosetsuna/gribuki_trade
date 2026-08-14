"""A 股模拟账户事件流的持久化边界。"""

from __future__ import annotations

from typing import Protocol

from gribuki_trade.domain.paper_trading import NewPaperLedgerEvent, PaperLedgerEvent


class PaperLedger(Protocol):
    """模拟交易服务所需的最小追加与读取契约。"""

    def append(
        self,
        event: NewPaperLedgerEvent,
        *,
        expected_sequence: int,
    ) -> tuple[PaperLedgerEvent, bool]: ...

    def events(self, account_id: str) -> tuple[PaperLedgerEvent, ...]: ...

    def event_by_idempotency_key(
        self,
        account_id: str,
        idempotency_key: str,
    ) -> PaperLedgerEvent | None: ...
