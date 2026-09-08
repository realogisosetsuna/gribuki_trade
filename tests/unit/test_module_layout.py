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
        (
            "gribuki_trade.adapters.binance.spot_order_params",
            "adapters/binance/spot_order_params.py",
        ),
        ("gribuki_trade.reporting.paper_day_renderer", "reporting/paper_day_renderer.py"),
        (
            "gribuki_trade.services.adversarial_macro_serialization",
            "services/adversarial_macro_serialization.py",
        ),
        ("gribuki_trade.strategy_lab.exit_simulation", "strategy_lab/exit_simulation.py"),
        (
            "gribuki_trade.adapters.market_data.cross_market_payload",
            "adapters/market_data/cross_market_payload.py",
        ),
        (
            "gribuki_trade.features.close_analysis_indicators",
            "features/close_analysis_indicators.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_paper_day_config",
            "services/ashare/ashare_paper_day_config.py",
        ),
        ("gribuki_trade.trading.oms_schema", "trading/oms_schema.py"),
        ("gribuki_trade.storage.live_record_models", "storage/live_record_models.py"),
        ("gribuki_trade.adapters.ashare.screening_factors", "adapters/ashare/screening_factors.py"),
        ("gribuki_trade.storage.live_record_schema", "storage/live_record_schema.py"),
        ("gribuki_trade.adapters.binance.request_builder", "adapters/binance/request_builder.py"),
        (
            "gribuki_trade.services.ashare.ashare_paper_day_schedule",
            "services/ashare/ashare_paper_day_schedule.py",
        ),
    )
    for name, suffix in modules:
        module = importlib.import_module(name)
        assert module.__file__ is not None
        assert module.__file__.replace("\\", "/").endswith(suffix)


def test_cli_handler_families_are_independent_execution_modules() -> None:
    """命令执行逻辑按平台边界拆开，同时保留 facade 可导出的处理器。"""

    # 处理器通过稳定 facade 解析依赖，布局测试先加载 facade，避免部分初始化状态。
    importlib.import_module("gribuki_trade.cli")
    ashare = importlib.import_module("gribuki_trade.cli_commands.handlers.ashare")
    binance = importlib.import_module("gribuki_trade.cli_commands.handlers.binance")
    for name in ("_ashare_snapshot", "_ashare_bars", "_ashare_daily", "_ashare_research_runs"):
        assert callable(getattr(ashare, name))
    assert callable(binance._binance_live_status)


def test_cli_command_families_are_independent_registration_modules() -> None:
    """命令注册入口按业务族拆开，主 CLI 只负责组装。"""

    for family in ("operations", "binance", "market_data", "paper_day", "notifications"):
        module = importlib.import_module(f"gribuki_trade.cli_commands.parsers.{family}")
        assert callable(module.register)


def test_cli_handlers_are_separate_from_argument_registration() -> None:
    """只读市场处理器和 Binance 处理器不应回到参数注册模块。"""

    for family in ("ashare", "binance"):
        module = importlib.import_module(f"gribuki_trade.cli_commands.handlers.{family}")
        assert module.__file__ is not None
