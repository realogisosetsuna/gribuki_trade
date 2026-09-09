from __future__ import annotations

import importlib
import subprocess
import sys


def test_adapters_and_services_resolve_to_domain_directories() -> None:
    """兼容入口必须实际指向按职责归档的实现目录。"""

    modules = (
        (
            "gribuki_trade.strategy_lab.ashare_evaluator_models",
            "strategy_lab/ashare_evaluator_models.py",
        ),
        (
            "gribuki_trade.strategy_lab.experiment_models",
            "strategy_lab/experiment_models.py",
        ),
        (
            "gribuki_trade.strategy_lab.discovery_models",
            "strategy_lab/discovery_models.py",
        ),
        (
            "gribuki_trade.services.binance.binance_shadow_models",
            "services/binance/binance_shadow_models.py",
        ),
        (
            "gribuki_trade.services.binance.binance_futures_unattended_models",
            "services/binance/binance_futures_unattended_models.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_paper_day_models",
            "services/ashare/ashare_paper_day_models.py",
        ),
        (
            "gribuki_trade.features.cross_market_models",
            "features/cross_market_models.py",
        ),
        (
            "gribuki_trade.services.exit_plan_lifecycle_models",
            "services/exit_plan_lifecycle_models.py",
        ),
        ("gribuki_trade.adapters.akshare", "adapters/market_data/akshare.py"),
        ("gribuki_trade.adapters.ashare_screening", "adapters/ashare/screening.py"),
        ("gribuki_trade.adapters.paper", "adapters/simulated/paper.py"),
        ("gribuki_trade.services.ashare_paper_day", "services/ashare/ashare_paper_day.py"),
        ("gribuki_trade.services.binance_execution", "services/binance/binance_execution.py"),
        (
            "gribuki_trade.services.live_trade_orchestration_models",
            "services/live_trade_orchestration_models.py",
        ),
        (
            "gribuki_trade.services.live_trade_records_parsing",
            "services/live_trade_records_parsing.py",
        ),
        (
            "gribuki_trade.services.macro_evidence_selection",
            "services/macro_evidence_selection.py",
        ),
        ("gribuki_trade.services.macro_models", "services/macro_models.py"),
        (
            "gribuki_trade.services.adversarial_macro_feature_flag",
            "services/adversarial_macro_feature_flag.py",
        ),
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
            "gribuki_trade.strategy_lab.exit_walk_forward",
            "strategy_lab/exit_walk_forward.py",
        ),
        (
            "gribuki_trade.strategy_lab.exit_models",
            "strategy_lab/exit_models.py",
        ),
        (
            "gribuki_trade.adapters.market_data.cross_market_payload",
            "adapters/market_data/cross_market_payload.py",
        ),
        (
            "gribuki_trade.features.close_analysis_indicators",
            "features/close_analysis_indicators.py",
        ),
        (
            "gribuki_trade.features.close_analysis_models",
            "features/close_analysis_models.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_paper_day_config",
            "services/ashare/ashare_paper_day_config.py",
        ),
        ("gribuki_trade.gui.napcat_process", "gui/napcat_process.py"),
        (
            "gribuki_trade.cli_commands.post_close_results",
            "cli_commands/post_close_results.py",
        ),
        (
            "gribuki_trade.cli_commands.binance_results",
            "cli_commands/binance_results.py",
        ),
        ("gribuki_trade.trading.oms_schema", "trading/oms_schema.py"),
        ("gribuki_trade.storage.live_record_models", "storage/live_record_models.py"),
        ("gribuki_trade.adapters.ashare.screening_factors", "adapters/ashare/screening_factors.py"),
        ("gribuki_trade.storage.live_record_schema", "storage/live_record_schema.py"),
        ("gribuki_trade.adapters.binance.request_builder", "adapters/binance/request_builder.py"),
        (
            "gribuki_trade.adapters.market_data.akshare_daily_stitch",
            "adapters/market_data/akshare_daily_stitch.py",
        ),
        (
            "gribuki_trade.adapters.market_data.akshare_daily_router",
            "adapters/market_data/akshare_daily_router.py",
        ),
        (
            "gribuki_trade.adapters.market_data.akshare_payload",
            "adapters/market_data/akshare_payload.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_intraday_llm_serialization",
            "services/ashare/ashare_intraday_llm_serialization.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_intraday_llm_policy",
            "services/ashare/ashare_intraday_llm_policy.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_intraday_llm_models",
            "services/ashare/ashare_intraday_llm_models.py",
        ),
        (
            "gribuki_trade.reporting.paper_day_llm_projection",
            "reporting/paper_day_llm_projection.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_intraday_policy",
            "services/ashare/ashare_intraday_policy.py",
        ),
        (
            "gribuki_trade.cli_commands.close_research_payloads",
            "cli_commands/close_research_payloads.py",
        ),
        (
            "gribuki_trade.adapters.binance.futures_order_params",
            "adapters/binance/futures_order_params.py",
        ),
        (
            "gribuki_trade.adapters.binance.futures_parsing",
            "adapters/binance/futures_parsing.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_paper_day_notifications",
            "services/ashare/ashare_paper_day_notifications.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_paper_day_schedule",
            "services/ashare/ashare_paper_day_schedule.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_paper_day_events",
            "services/ashare/ashare_paper_day_events.py",
        ),
        ("gribuki_trade.trading.futures_oms_schema", "trading/futures_oms_schema.py"),
        ("gribuki_trade.trading.futures_oms_policy", "trading/futures_oms_policy.py"),
        ("gribuki_trade.trading.oms_position_policy", "trading/oms_position_policy.py"),
        (
            "gribuki_trade.ingest.search_discovery_policy",
            "ingest/search_discovery_policy.py",
        ),
        ("gribuki_trade.services.adversarial_macro_policy", "services/adversarial_macro_policy.py"),
        (
            "gribuki_trade.cli_commands.live_sync_payloads",
            "cli_commands/live_sync_payloads.py",
        ),
        ("gribuki_trade.cli_commands.runtime", "cli_commands/runtime.py"),
        (
            "gribuki_trade.services.ashare.ashare_paper_day_llm_payloads",
            "services/ashare/ashare_paper_day_llm_payloads.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_paper_day_documents",
            "services/ashare/ashare_paper_day_documents.py",
        ),
        ("gribuki_trade.adapters.binance.errors", "adapters/binance/errors.py"),
        ("gribuki_trade.adapters.binance.rate_limit", "adapters/binance/rate_limit.py"),
        (
            "gribuki_trade.adapters.binance.user_stream_parsing",
            "adapters/binance/user_stream_parsing.py",
        ),
        (
            "gribuki_trade.storage.live_record_work_policy",
            "storage/live_record_work_policy.py",
        ),
        (
            "gribuki_trade.storage.live_record_protection_policy",
            "storage/live_record_protection_policy.py",
        ),
        ("gribuki_trade.storage.live_record_errors", "storage/live_record_errors.py"),
        ("gribuki_trade.storage.live_record_integrity", "storage/live_record_integrity.py"),
        (
            "gribuki_trade.services.binance.binance_execution_records",
            "services/binance/binance_execution_records.py",
        ),
        (
            "gribuki_trade.services.binance.binance_execution_models",
            "services/binance/binance_execution_models.py",
        ),
        (
            "gribuki_trade.reporting.paper_day_account_projection",
            "reporting/paper_day_account_projection.py",
        ),
        (
            "gribuki_trade.reporting.paper_day_projection_models",
            "reporting/paper_day_projection_models.py",
        ),
        (
            "gribuki_trade.reporting.paper_day_execution_projection",
            "reporting/paper_day_execution_projection.py",
        ),
        (
            "gribuki_trade.adapters.ashare.screening_payload",
            "adapters/ashare/screening_payload.py",
        ),
        (
            "gribuki_trade.services.adversarial_macro_boundaries",
            "services/adversarial_macro_boundaries.py",
        ),
        (
            "gribuki_trade.storage.paper_orders_codec",
            "storage/paper_orders_codec.py",
        ),
        (
            "gribuki_trade.cli_commands.handlers.napcat",
            "cli_commands/handlers/napcat.py",
        ),
        (
            "gribuki_trade.services.ashare.ashare_close_notification_splitting",
            "services/ashare/ashare_close_notification_splitting.py",
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
    for name in (
        "_ashare_snapshot",
        "_ashare_bars",
        "_ashare_daily",
        "_ashare_research_runs",
        "_ashare_source_health",
        "_ashare_watchlist",
        "_ashare_screening_run_json",
        "_ashare_intraday_run_json",
    ):
        assert callable(getattr(ashare, name))
    assert callable(binance._binance_live_status)


def test_binance_handler_modules_are_directly_importable() -> None:
    """处理器可在不预先导入 CLI facade 的情况下单独导航。"""

    for module_name in (
        "gribuki_trade.cli_commands.handlers.binance_live",
        "gribuki_trade.cli_commands.handlers.binance",
    ):
        result = subprocess.run(
            [sys.executable, "-c", f"import {module_name}"],
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr


def test_napcat_handler_is_directly_importable() -> None:
    """NapCat handler 可独立导入，避免 CLI facade 循环依赖。"""

    result = subprocess.run(
        [sys.executable, "-c", "import gribuki_trade.cli_commands.handlers.napcat"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    handler = importlib.import_module("gribuki_trade.cli_commands.handlers.napcat")
    for name in (
        "_napcat_configure",
        "_napcat_status",
        "_napcat_dispatch",
        "_napcat_send_test",
        "_napcat_send_artifact",
    ):
        assert callable(getattr(handler, name))


def test_paper_order_store_reexports_codec_models() -> None:
    """订单存储保留历史模型身份，同时纯 codec 可独立导航。"""

    facade = importlib.import_module("gribuki_trade.storage.paper_orders")
    codec = importlib.import_module("gribuki_trade.storage.paper_orders_codec")
    for name in (
        "PaperOrderEvent",
        "PaperOrderEventType",
        "PaperRunRecord",
        "PaperOrderStoreError",
        "PaperOrderStoreConflictError",
        "PaperOrderStoreLeaseError",
        "PaperOrderStoreIntegrityError",
    ):
        assert getattr(facade, name) is getattr(codec, name)


def test_close_notification_splitting_is_directly_importable() -> None:
    """盘后通知分段辅助可独立导入，且不依赖通知 facade 初始化。"""

    module = importlib.import_module(
        "gribuki_trade.services.ashare.ashare_close_notification_splitting"
    )
    for name in (
        "_split_contractual_instrument_report",
        "_split_plain_text_payload",
        "_dual_track_report_lines",
        "_split_notification_text",
    ):
        assert callable(getattr(module, name))

    facade = importlib.import_module(
        "gribuki_trade.services.ashare.ashare_close_notifications"
    )
    assert facade._split_notification_text is module._split_notification_text


def test_binance_execution_models_keep_facade_contract_identity() -> None:
    """Binance 执行协议与启动结果可独立导航，旧 facade 继续导出相同身份。"""

    models = importlib.import_module(
        "gribuki_trade.services.binance.binance_execution_models"
    )
    facade = importlib.import_module("gribuki_trade.services.binance.binance_execution")
    for name in (
        "BinanceSpotExecutionGateway",
        "BinanceStartupReconciliation",
        "BinanceTestnetOnlyError",
        "BinanceUserDataSource",
    ):
        assert getattr(facade, name) is getattr(models, name)


def test_paper_day_summary_models_keep_facade_type_identity() -> None:
    """摘要模型独立可导航，同时旧导入路径保留相同类型身份。"""

    summary = importlib.import_module("gribuki_trade.reporting.paper_day_summary")
    models = importlib.import_module("gribuki_trade.reporting.paper_day_summary_models")
    for name in (
        "PaperDayExecutiveProjection",
        "PaperDayPriceAcceptanceProjection",
        "PaperDayRiskPolicyProjection",
        "PaperDayWatchlistChange",
    ):
        assert getattr(summary, name) is getattr(models, name)


def test_cli_runtime_support_is_importable_before_the_facade() -> None:
    """运行时辅助可单独导航，且导入顺序不会触发 CLI 循环依赖。"""

    runtime = importlib.import_module("gribuki_trade.cli_commands.runtime")
    assert callable(runtime.apply_integration_runtime_defaults)
    assert callable(runtime.required_local_secret)
    facade = importlib.import_module("gribuki_trade.cli")
    assert facade._apply_integration_runtime_defaults is runtime._apply_integration_runtime_defaults
    assert facade._required_local_secret is runtime._required_local_secret


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


def test_model_extractions_keep_facade_type_identity() -> None:
    """跨领域模型拆分后，旧 facade 与新模块必须导出相同类型对象。"""

    boundaries = (
        (
            "gribuki_trade.strategy_lab.ashare_evaluator",
            "gribuki_trade.strategy_lab.ashare_evaluator_models",
            ("PITStrategyScore", "CompletedAShareDailyBar", "AShareDailyEvaluatorConfig"),
        ),
        (
            "gribuki_trade.strategy_lab.experiments",
            "gribuki_trade.strategy_lab.experiment_models",
            ("DataManifest", "StrategyWeights", "PerformanceMetrics", "StrategyExperiment"),
        ),
        (
            "gribuki_trade.features.cross_market_relations",
            "gribuki_trade.features.cross_market_models",
            ("TargetCloseObservation", "CrossMarketFactorSeries", "CrossMarketRelationsReport"),
        ),
        (
            "gribuki_trade.services.exit_plan_lifecycle",
            "gribuki_trade.services.exit_plan_lifecycle_models",
            ("ExitPlanEventStore", "ExitBarrierObservation", "ExitPlanLifecycleError"),
        ),
        (
            "gribuki_trade.services.binance.binance_shadow",
            "gribuki_trade.services.binance.binance_shadow_models",
            (
                "BinanceShadowConfig",
                "ShadowEnvironmentWatermark",
                "ShadowAdapterSnapshot",
                "BinanceShadowStatistics",
            ),
        ),
        (
            "gribuki_trade.services.binance.binance_futures_unattended",
            "gribuki_trade.services.binance.binance_futures_unattended_models",
            ("FuturesStartupReconciliation",),
        ),
        (
            "gribuki_trade.services.ashare.ashare_paper_day",
            "gribuki_trade.services.ashare.ashare_paper_day_models",
            ("PaperDayResult",),
        ),
    )
    for facade_name, model_name, names in boundaries:
        facade = importlib.import_module(facade_name)
        model = importlib.import_module(model_name)
        for name in names:
            assert getattr(facade, name) is getattr(model, name)
