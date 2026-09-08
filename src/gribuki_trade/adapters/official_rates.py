"""兼容历史导入路径；实现位于 ``adapters.macro.official_rates``。"""

import sys as _sys
from importlib import import_module as _import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gribuki_trade.adapters.macro.official_rates import *  # noqa: F403

_implementation = _import_module("gribuki_trade.adapters.macro.official_rates")
_sys.modules[__name__] = _implementation
globals().update(
    {name: value for name, value in vars(_implementation).items() if not name.startswith("__")}
)
