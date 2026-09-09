"""PAPER 日摘要的不可变投影模型。

这些数据类只描述旁路摘要的类型结构，不读取文件、不访问 SQLite，也不执行
投影计算；解析与渲染分别留在摘要 facade 和专门的 codec/渲染模块。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from .paper_day_llm_projection import PaperDayLLMProjection
from .paper_day_projection_models import (
    PaperDayNotificationProjection,
    PaperDayPositionProjection,
    PaperDaySidecarEvent,
)


@dataclass(frozen=True, slots=True)
class PaperDayWatchlistChange:
    sequence: int
    known_at: datetime
    added: tuple[str, ...]
    removed: tuple[str, ...]
    resulting_count: int | None


@dataclass(frozen=True, slots=True)
class PaperDaySourceTransition:
    sequence: int
    known_at: datetime
    component: str
    previous_state: str | None
    state: str
    error_code: str | None


@dataclass(frozen=True, slots=True)
class PaperDayPriceAcceptanceProjection:
    """旁路事件中保留的一条不可变买卖价格区间。"""

    status: str
    side: str | None
    board: str | None
    acceptable_lower: Decimal | None
    acceptable_upper: Decimal | None
    exchange_lower: Decimal | None
    exchange_upper: Decimal | None
    invalidation_boundary: Decimal | None
    limit_price: Decimal | None
    reference_price: Decimal | None
    price_tick: Decimal | None
    policy_version: str | None
    price_cage_status: str | None
    real_broker_submission_allowed: bool | None


@dataclass(frozen=True, slots=True)
class PaperDayQuantityRuleProjection:
    """运行器保留的板块特定限价单数量约束。"""

    board: str | None
    minimum_buy_quantity: int | None
    buy_increment: int | None
    maximum_limit_order_quantity: int | None
    paper_partial_fill_increment: int | None
    minimum_regular_sell_quantity: int | None
    sell_increment: int | None
    sell_residual_policy: str | None
    policy_version: str | None


@dataclass(frozen=True, slots=True)
class PaperDayBuyExecutionProjection:
    """一笔已提交模拟订单采用的买入区间与数量规则。"""

    sequence: int
    known_at: datetime
    symbol: str
    order_id: str | None
    quantity: int | None
    price_acceptance: PaperDayPriceAcceptanceProjection
    quantity_rule: PaperDayQuantityRuleProjection | None


@dataclass(frozen=True, slots=True)
class PaperDaySellExecutionProjection:
    """仅记录的卖出区间、T+1 阻断项与未来数量计划。"""

    sequence: int
    known_at: datetime
    symbol: str
    price_acceptance: PaperDayPriceAcceptanceProjection
    quantity_rule: PaperDayQuantityRuleProjection | None
    available_to_sell: int | None
    quantity_plan_status: str | None
    future_limit_order_sequence: tuple[int, ...]
    current_blockers: tuple[str, ...]
    future_non_execution_conditions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PaperDayRiskPolicyProjection:
    """可由携带策略的旁路事件证明的最新风险边界。"""

    sequence: int
    known_at: datetime
    source_event_type: str
    maximum_positions: int | None
    position_count_limit_enabled: bool | None
    cash_reserve_fraction: Decimal | None
    maximum_gross_fraction: Decimal | None
    maximum_symbol_fraction: Decimal | None
    risk_per_trade_fraction: Decimal | None
    price_acceptance_policy_version: str | None
    order_quantity_policy_version: str | None
    order_quantity_rules: tuple[PaperDayQuantityRuleProjection, ...]
    unsupported_quantity_boards: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PaperDayRiskPolicyChangeProjection:
    """一次经操作员授权、仅追加的盘中风险策略迁移。"""

    sequence: int
    known_at: datetime
    operator_authorized: bool | None
    reason_codes: tuple[str, ...]
    old_policy_sha256: str | None
    new_policy_sha256: str | None
    old_maximum_positions: int | None
    new_maximum_positions: int | None
    new_position_count_limit_enabled: bool | None
    validated_fill_count: int | None
    validated_position_count: int | None


@dataclass(frozen=True, slots=True)
class PaperDayStableReasonProjection:
    """一条已观测的稳定拒绝或阻断代码及其中文解释。"""

    category: str
    code: str
    count: int
    explanation: str


@dataclass(frozen=True, slots=True)
class PaperDayExecutiveProjection:
    """无需打开 SQLite 即可计算的强类型确定性摘要。"""

    session_root: Path
    status_path: Path
    event_log_path: Path
    stdout_path: Path | None
    original_report_path: Path | None
    session_date: date
    run_id: str
    lifecycle: str
    coverage: str
    first_known_at: datetime
    last_known_at: datetime
    market_opened_at: datetime | None
    market_closed_at: datetime | None
    status_event_count: int | None
    sidecar_event_count: int
    phases_seen: tuple[str, ...]
    preopen_outcome: str
    preopen_candidate_count: int | None
    watchlist_initial_count: int | None
    watchlist_current_count: int | None
    watchlist_peak_count: int | None
    watchlist_update_count: int
    watchlist_added_total: int
    watchlist_removed_total: int
    watchlist_changes: tuple[PaperDayWatchlistChange, ...]
    scan_count: int
    scan_status_counts: tuple[tuple[str, int], ...]
    scan_universe_min: int | None
    scan_universe_max: int | None
    scan_universe_last: int | None
    monitor_evaluations: int
    monitor_invalid: int
    monitored_symbols: int
    monitor_interval_counts: tuple[tuple[str, int], ...]
    technical_decision_counts: tuple[tuple[str, int], ...]
    buy_signal_count: int
    buy_approved_count: int
    buy_rejected_count: int
    buy_reject_reasons: tuple[tuple[str, int], ...]
    risk_policy_changes: tuple[PaperDayRiskPolicyChangeProjection, ...]
    current_risk_policy: PaperDayRiskPolicyProjection | None
    buy_execution_acceptances: tuple[PaperDayBuyExecutionProjection, ...]
    sell_execution_acceptances: tuple[PaperDaySellExecutionProjection, ...]
    stable_rejection_reasons: tuple[PaperDayStableReasonProjection, ...]
    sell_signal_count: int
    sell_not_submitted_count: int
    orders_submitted: int
    order_matches: int
    orders_filled: int
    orders_partially_filled: int
    orders_expired: int
    expiry_reason_counts: tuple[tuple[str, int], ...]
    fills_applied: int
    filled_shares: int
    fill_notional: Decimal | None
    commission: Decimal | None
    transfer_fee: Decimal | None
    stamp_tax: Decimal | None
    starting_cash: Decimal | None
    final_cash: Decimal | None
    final_equity: Decimal | None
    gross_exposure: Decimal | None
    realized_pnl: Decimal | None
    unrealized_pnl: Decimal | None
    total_mark_to_market_pnl: Decimal | None
    positions: tuple[PaperDayPositionProjection, ...]
    notifications: PaperDayNotificationProjection
    llm: PaperDayLLMProjection
    source_transitions: tuple[PaperDaySourceTransition, ...]
    source_degradation_count: int
    source_recovery_count: int
    events: tuple[PaperDaySidecarEvent, ...]
    warnings: tuple[str, ...]


