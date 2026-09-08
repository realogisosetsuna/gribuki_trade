"""A-share PAPER 日运行配置与风险策略清单。

本模块只包含冻结的调度/负载配置和纯策略清单投影；它不访问存储、网络、
调度器或券商。运行器通过历史 facade 导入这些对象，以保持兼容。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, time, timedelta
from decimal import Decimal

from gribuki_trade.domain.paper_day import PaperDayRunManifest
from gribuki_trade.features.ashare_surveillance import IntradayCandidateClass
from gribuki_trade.ports.market_data import FreshnessStatus
from gribuki_trade.services.ashare.ashare_intraday_paper import IntradayPaperRiskConfig

_ENTRY_POLICY_SCHEMA = "ashare-paper-entry-policy@2"
PAPER_RISK_POLICY_CHANGE_CONFIRMATION = "PAPER_RISK_POLICY_CHANGE"
_TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID = (
    "AKShare/Tencent stock_zh_a_spot_tx + Tencent qt.gtimg.cn bulk quote"
)


@dataclass(frozen=True, slots=True)
class ASharePaperDayConfig:
    """一个完整交易时段的冻结日程、风险和负载限制。"""

    initial_cash: Decimal = Decimal("200000")
    preopen_screen_time: time = time(8, 15)
    latest_preopen_start: time = time(9, 20)
    market_open: time = time(9, 30)
    morning_end: time = time(11, 30)
    afternoon_start: time = time(13, 0)
    market_close: time = time(15, 0)
    finalization_time: time = time(15, 5)
    boundary_bar_publication_grace: timedelta = timedelta(seconds=15)
    first_intraday_scan_delay: timedelta = timedelta(minutes=5)
    surveillance_interval: timedelta = timedelta(minutes=15)
    primary_monitor_interval: timedelta = timedelta(minutes=1)
    secondary_monitor_interval: timedelta = timedelta(minutes=5)
    health_notification_interval: timedelta = timedelta(minutes=30)
    minute_history_lookback: timedelta = timedelta(days=5)
    anomaly_ttl: timedelta = timedelta(minutes=20)
    maximum_watchlist_size: int = 30
    initial_core_size: int = 10
    dynamic_candidate_size: int = 20
    market_data_concurrency: int = 4
    fallback_price_tolerance: Decimal = Decimal("0.015")
    scheduler_tick_seconds: float = 1.0
    lease_for: timedelta = timedelta(minutes=2)
    lease_renew_interval: timedelta = timedelta(seconds=30)
    intraday_llm_enabled: bool = False
    intraday_llm_required_for_buy: bool = False
    exit_plan_enabled: bool = True
    exit_plan_holding_weekdays: int = 5
    exit_plan_trading_sessions: tuple[date, ...] = ()
    exit_plan_calendar_sha256: str | None = None
    strategy_version: str = "ashare-paper-day@1"

    def __post_init__(self) -> None:
        if not self.initial_cash.is_finite() or self.initial_cash <= 0:
            raise ValueError("initial_cash must be finite and positive")
        ordered = (
            self.preopen_screen_time,
            self.latest_preopen_start,
            self.market_open,
            self.morning_end,
            self.afternoon_start,
            self.market_close,
            self.finalization_time,
        )
        if tuple(sorted(ordered)) != ordered or len(set(ordered)) != len(ordered):
            raise ValueError("PAPER-day schedule times must be unique and ordered")
        for name in (
            "first_intraday_scan_delay",
            "surveillance_interval",
            "primary_monitor_interval",
            "secondary_monitor_interval",
            "health_notification_interval",
            "boundary_bar_publication_grace",
            "minute_history_lookback",
            "anomaly_ttl",
            "lease_for",
            "lease_renew_interval",
        ):
            if getattr(self, name) <= timedelta(0):
                raise ValueError(f"{name} must be positive")
        if self.lease_renew_interval >= self.lease_for:
            raise ValueError("lease_renew_interval must be shorter than lease_for")
        for name in (
            "maximum_watchlist_size",
            "initial_core_size",
            "dynamic_candidate_size",
            "market_data_concurrency",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.initial_core_size > self.maximum_watchlist_size:
            raise ValueError("initial_core_size exceeds maximum_watchlist_size")
        if self.dynamic_candidate_size > self.maximum_watchlist_size:
            raise ValueError("dynamic_candidate_size exceeds maximum_watchlist_size")
        if self.market_data_concurrency > 16:
            raise ValueError("market_data_concurrency must not exceed 16")
        if not self.fallback_price_tolerance.is_finite() or not (
            Decimal("0") < self.fallback_price_tolerance <= Decimal("0.10")
        ):
            raise ValueError("fallback_price_tolerance must be in (0, 0.10]")
        if not 0.1 <= self.scheduler_tick_seconds <= 10:
            raise ValueError("scheduler_tick_seconds must be in [0.1, 10]")
        if not self.strategy_version.strip():
            raise ValueError("strategy_version must not be empty")
        if not isinstance(self.intraday_llm_enabled, bool) or not isinstance(
            self.intraday_llm_required_for_buy, bool
        ):
            raise TypeError("intraday LLM flags must be bool")
        if not isinstance(self.exit_plan_enabled, bool):
            raise TypeError("exit_plan_enabled must be bool")
        if not self.exit_plan_enabled:
            raise ValueError(
                "exit_plan_enabled cannot be disabled: QUICK protection is mandatory before orders"
            )
        if (
            isinstance(self.exit_plan_holding_weekdays, bool)
            or not isinstance(self.exit_plan_holding_weekdays, int)
            or self.exit_plan_holding_weekdays < 1
        ):
            raise ValueError("exit_plan_holding_weekdays must be a positive integer")
        if tuple(sorted(set(self.exit_plan_trading_sessions))) != (
            self.exit_plan_trading_sessions
        ):
            raise ValueError("exit_plan_trading_sessions must be unique and ordered")
        if self.exit_plan_trading_sessions and (
            self.exit_plan_calendar_sha256 is None
            or len(self.exit_plan_calendar_sha256) != 64
        ):
            raise ValueError("verified exit-plan sessions require a calendar SHA-256")
        if self.exit_plan_calendar_sha256 is not None and len(
            self.exit_plan_calendar_sha256
        ) != 64:
            raise ValueError("exit_plan_calendar_sha256 must be a SHA-256 digest")

    def audit_document(self) -> dict[str, object]:
        """返回纳入运行清单且可安全编码为 JSON 的配置。"""

        return {
            "initial_cash": str(self.initial_cash),
            "preopen_screen_time": self.preopen_screen_time.isoformat(),
            "latest_preopen_start": self.latest_preopen_start.isoformat(),
            "market_open": self.market_open.isoformat(),
            "morning_end": self.morning_end.isoformat(),
            "afternoon_start": self.afternoon_start.isoformat(),
            "market_close": self.market_close.isoformat(),
            "finalization_time": self.finalization_time.isoformat(),
            "first_intraday_scan_delay_seconds": int(
                self.first_intraday_scan_delay.total_seconds()
            ),
            "surveillance_interval_seconds": int(self.surveillance_interval.total_seconds()),
            "primary_monitor_interval_seconds": int(self.primary_monitor_interval.total_seconds()),
            "secondary_monitor_interval_seconds": int(
                self.secondary_monitor_interval.total_seconds()
            ),
            "health_notification_interval_seconds": int(
                self.health_notification_interval.total_seconds()
            ),
            "boundary_bar_publication_grace_seconds": int(
                self.boundary_bar_publication_grace.total_seconds()
            ),
            "minute_history_lookback_seconds": int(self.minute_history_lookback.total_seconds()),
            "anomaly_ttl_seconds": int(self.anomaly_ttl.total_seconds()),
            "maximum_watchlist_size": self.maximum_watchlist_size,
            "initial_core_size": self.initial_core_size,
            "dynamic_candidate_size": self.dynamic_candidate_size,
            "market_data_concurrency": self.market_data_concurrency,
            "fallback_price_tolerance": str(self.fallback_price_tolerance),
            "intraday_llm_enabled": self.intraday_llm_enabled,
            "intraday_llm_required_for_buy": self.intraday_llm_required_for_buy,
            "exit_plan_enabled": self.exit_plan_enabled,
            "exit_plan_holding_weekdays": self.exit_plan_holding_weekdays,
            "exit_plan_trading_sessions": [
                item.isoformat() for item in self.exit_plan_trading_sessions
            ],
            "exit_plan_calendar_sha256": self.exit_plan_calendar_sha256,
            "exit_plan_calendar_mode": (
                "VERIFIED_TRADING_SESSIONS"
                if self.exit_plan_trading_sessions
                else "LEGACY_WEEKDAY_FALLBACK"
            ),
            "exit_plan_policy": "QUICK_BEFORE_ORDER_DEEP_AFTER_FILL_BARRIER_AFTER_FILL",
            "strategy_version": self.strategy_version,
            "execution_mode": "PAPER_ONLY_NO_BROKER",
            "sell_policy": "RECORD_AND_NOTIFY_NO_SELL_ORDER",
            "match_policy": "NEXT_FULLY_POST_SIGNAL_1M_INTERVAL_IOC",
        }



def _entry_policy_document(config: ASharePaperDayConfig) -> dict[str, object]:
    """每次重启都会记录的稳定、非敏感授权策略。"""

    return {
        "candidate_class_required": IntradayCandidateClass.MOMENTUM_EXPANSION.value,
        "complete_whole_market_scan_required": True,
        "degraded_scan_authority": False,
        "degraded_sina_minute_requires_exact_composite_source": (
            _TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID
        ),
        "fallback_price_tolerance": str(config.fallback_price_tolerance),
        "minute_freshness_required": FreshnessStatus.CURRENT.value,
        "schema": _ENTRY_POLICY_SCHEMA,
    }


def _runner_config_document(
    config: ASharePaperDayConfig,
    risk_config: IntradayPaperRiskConfig,
) -> dict[str, object]:
    """把调度与数据策略、执行风险策略绑定为一个哈希。"""

    return {
        "paper_day": config.audit_document(),
        "intraday_risk": risk_config.audit_document(),
    }


def _risk_policy_manifest_binding(
    manifest: PaperDayRunManifest,
    risk_policy: Mapping[str, object],
) -> str:
    """说明策略是由清单绑定，还是来自旧运行时升级。"""

    return (
        "MATCHES_MANIFEST"
        if manifest.config.get("intraday_risk_policy") == dict(risk_policy)
        else "RUNTIME_APPEND_ONLY_LEGACY_MANIFEST"
    )






__all__ = [
    "ASharePaperDayConfig",
    "PAPER_RISK_POLICY_CHANGE_CONFIRMATION",
]
