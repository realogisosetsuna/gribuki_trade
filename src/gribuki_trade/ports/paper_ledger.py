"""Persistence boundary for A-share paper-account event streams."""

from __future__ import annotations

from typing import Protocol

from gribuki_trade.domain.paper_trading import NewPaperLedgerEvent, PaperLedgerEvent


class PaperLedger(Protocol):
    """Minimal append/read contract required by the paper trading service."""

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
