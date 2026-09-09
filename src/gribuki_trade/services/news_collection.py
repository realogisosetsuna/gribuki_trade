"""兼容历史导入路径；实现位于 ``services.communications.news_collection``。"""

import sys as _sys
from importlib import import_module as _import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gribuki_trade.services.communications.news_collection import *  # noqa: F403

_implementation = _import_module("gribuki_trade.services.communications.news_collection")
_sys.modules[__name__] = _implementation
globals().update(
    {name: value for name, value in vars(_implementation).items() if not name.startswith("__")}
)
