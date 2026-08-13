"""Offline CSV export for orders that require manual broker confirmation.

This module has no broker-client integration and performs no screen, keyboard,
or mouse automation.  Its only side effect is an atomic file export to a path
chosen by the caller.
"""

from __future__ import annotations

import csv
import os
import tempfile
from collections.abc import Iterable
from pathlib import Path

from gribuki_trade.domain.orders import OrderIntent

TICKET_COLUMNS = (
    "client_order_id",
    "symbol",
    "side",
    "quantity",
    "limit_price",
    "strategy_id",
    "created_at",
)


def export_manual_tickets(
    orders: Iterable[OrderIntent],
    path: str | os.PathLike[str],
    *,
    overwrite: bool = False,
) -> Path:
    """Atomically export deterministic UTF-8 CSV tickets for manual entry.

    ``client_order_id`` values must be unique within one export.  By default an
    existing destination is never replaced; callers must opt in explicitly via
    ``overwrite=True``.  A temporary file is fully flushed in the destination
    directory before it becomes visible under the requested filename.
    """

    destination = Path(path)
    ordered = sorted(orders, key=lambda order: order.client_order_id)
    _reject_duplicate_ids(ordered)

    if not overwrite and destination.exists():
        raise FileExistsError(f"manual ticket file already exists: {destination}")

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
            delete=False,
        ) as stream:
            temporary_path = Path(stream.name)
            writer = csv.DictWriter(stream, fieldnames=TICKET_COLUMNS, lineterminator="\n")
            writer.writeheader()
            for order in ordered:
                writer.writerow(
                    {
                        "client_order_id": order.client_order_id,
                        "symbol": order.symbol,
                        "side": order.side.value,
                        "quantity": str(order.quantity),
                        "limit_price": format(order.limit_price, "f"),
                        "strategy_id": order.strategy_id,
                        "created_at": order.created_at.isoformat(),
                    }
                )
            stream.flush()
            os.fsync(stream.fileno())

        if overwrite:
            os.replace(temporary_path, destination)
        else:
            _install_without_overwrite(temporary_path, destination)

        return destination
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _reject_duplicate_ids(orders: Iterable[OrderIntent]) -> None:
    seen: set[str] = set()
    for order in orders:
        if order.client_order_id in seen:
            raise ValueError(f"duplicate client_order_id: {order.client_order_id!r}")
        seen.add(order.client_order_id)


def _install_without_overwrite(temporary_path: Path, destination: Path) -> None:
    """Atomically publish a complete file while preserving an existing target."""

    try:
        os.link(temporary_path, destination)
    except FileExistsError as error:
        raise FileExistsError(f"manual ticket file already exists: {destination}") from error
