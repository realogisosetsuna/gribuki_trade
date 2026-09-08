"""可复用的命令行参数解析器。

控制台入口继续负责命令分发和集成工作流；本模块只保存无副作用的值转换器，便于子命令
构造器复用，并可在不初始化完整 CLI 的情况下测试。``cli.py`` 继续导入下面的私有兼容
名称，以保持既有调用方和测试可用。
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from decimal import Decimal, InvalidOperation


def add_order_arguments(parser: argparse.ArgumentParser) -> None:
    """向 *parser* 添加通用的虚拟订单选项。

    这些选项由现货测试、循环、OMS 和成交命令共享。公开名称供后续命令模块使用；
    ``_add_order_arguments`` 兼容 ``cli.py`` 原有的私有入口。
    """

    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument(
        "--notional",
        type=positive_decimal,
        default=Decimal("20"),
        help="target virtual quote notional (default: 20)",
    )


def positive_decimal(value: str) -> Decimal:
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise argparse.ArgumentTypeError("notional must be a decimal number") from None
    if not number.is_finite() or number <= 0:
        raise argparse.ArgumentTypeError("notional must be positive and finite")
    return number


def non_negative_decimal(value: str) -> Decimal:
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise argparse.ArgumentTypeError("value must be a decimal number") from None
    if not number.is_finite() or number < 0:
        raise argparse.ArgumentTypeError("value must be finite and non-negative")
    return number


def unit_fraction_decimal(value: str) -> Decimal:
    number = non_negative_decimal(value)
    if number > 1:
        raise argparse.ArgumentTypeError("value must be between zero and one")
    return number


def macro_weight_decimal(value: str) -> Decimal:
    number = unit_fraction_decimal(value)
    if number > Decimal("0.40"):
        raise argparse.ArgumentTypeError(
            "macro weight must be between zero and 0.40 so technical evidence remains dominant"
        )
    return number


def positive_integer(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("value must be a positive integer") from None
    if number <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return number


def positive_integer_or_unlimited(value: str) -> int | None:
    if value.strip().lower() == "unlimited":
        return None
    return positive_integer(value)


def iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from None


def iso_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "datetime must be ISO-8601 and include a timezone"
        ) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("datetime must include a timezone offset")
    return parsed


def non_negative_integer(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("value must be a non-negative integer") from None
    if number < 0:
        raise argparse.ArgumentTypeError("value must be a non-negative integer")
    return number


def positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("value must be positive") from None
    if not 0 < number < float("inf"):
        raise argparse.ArgumentTypeError("value must be positive and finite")
    return number


def non_negative_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("value must be non-negative") from None
    if not 0 <= number < float("inf"):
        raise argparse.ArgumentTypeError("value must be non-negative and finite")
    return number


# 兼容 ``cli.py`` 中历史私有辅助函数的别名。
_add_order_arguments = add_order_arguments
_positive_decimal = positive_decimal
_non_negative_decimal = non_negative_decimal
_unit_fraction_decimal = unit_fraction_decimal
_macro_weight_decimal = macro_weight_decimal
_positive_integer = positive_integer
_positive_integer_or_unlimited = positive_integer_or_unlimited
_iso_date = iso_date
_iso_datetime = iso_datetime
_non_negative_integer = non_negative_integer
_positive_float = positive_float
_non_negative_float = non_negative_float
