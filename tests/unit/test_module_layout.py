from __future__ import annotations

import importlib


def test_adapters_and_services_resolve_to_domain_directories() -> None:
    """兼容入口必须实际指向按职责归档的实现目录。"""

    modules = (
        ("gribuki_trade.adapters.akshare", "adapters/market_data/akshare.py"),
        ("gribuki_trade.adapters.ashare_screening", "adapters/ashare/screening.py"),
        ("gribuki_trade.adapters.paper", "adapters/simulated/paper.py"),
        ("gribuki_trade.services.ashare_paper_day", "services/ashare/ashare_paper_day.py"),
        ("gribuki_trade.services.binance_execution", "services/binance/binance_execution.py"),
    )
    for name, suffix in modules:
        module = importlib.import_module(name)
        assert module.__file__ is not None
        assert module.__file__.replace("\\", "/").endswith(suffix)


def test_cli_command_families_are_independent_registration_modules() -> None:
    """命令注册入口按业务族拆开，主 CLI 只负责组装。"""

    for family in ("operations", "binance", "market_data", "paper_day", "notifications"):
        module = importlib.import_module(f"gribuki_trade.cli_commands.parsers.{family}")
        assert callable(module.register)
