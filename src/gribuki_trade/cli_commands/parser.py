"""完整命令树的组装器；每个命令族在独立模块注册。"""

from __future__ import annotations

import argparse

from .parser_support import *  # noqa: F403
from .parsers.binance import register as register_binance
from .parsers.close_research import register as register_close_research
from .parsers.experiments import register as register_experiments
from .parsers.live_sync import register as register_live_sync
from .parsers.market_data import register as register_market_data
from .parsers.notifications import register as register_notifications
from .parsers.operations import register as register_operations
from .parsers.paper_day import register as register_paper_day
from .parsers.post_close import register as register_post_close
from .parsers.research import register as register_research
from .parsers.screening import register as register_screening


def build_parser() -> argparse.ArgumentParser:
    """按固定顺序组装完整命令行接口。"""

    parser = argparse.ArgumentParser(prog="gribuki-trade")
    commands = parser.add_subparsers(dest="command")
    register_operations(commands)
    register_binance(commands)
    register_market_data(commands)
    register_screening(commands)
    register_experiments(commands)
    register_research(commands)
    register_live_sync(commands)
    register_paper_day(commands)
    register_post_close(commands)
    register_close_research(commands)
    register_notifications(commands)
    return parser
