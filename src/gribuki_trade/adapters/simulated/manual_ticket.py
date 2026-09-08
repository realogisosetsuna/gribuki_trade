"""为需要人工券商确认的订单导出离线 CSV。

本模块不集成券商客户端，也不做屏幕、键盘或鼠标自动化。它唯一的副作用是
把结果原子写出到调用方指定的路径。
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
    """原子导出供人工录入的确定性 UTF-8 CSV 委托单。

    同一次导出内的 ``client_order_id`` 必须唯一。默认绝不替换已有目标文件；
    调用方必须通过 ``overwrite=True`` 明确选择覆盖。临时文件会先在目标目录中
    完整刷盘，之后才以所请求的文件名对外可见。
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
    """原子发布完整文件，同时保留已有目标文件。"""

    try:
        os.link(temporary_path, destination)
    except FileExistsError as error:
        raise FileExistsError(f"manual ticket file already exists: {destination}") from error
