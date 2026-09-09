"""A 股 PAPER 日风险策略迁移与执行策略文档的纯边界。

本模块只读取不可变事件、运行清单和内存中的策略对象，负责判断风险策略
迁移是否被精确授权，以及把价格/数量约束转换为审计文档。SQLite 事务、
事件追加、调度和运行器状态仍由 :mod:`ashare_paper_day` facade 负责。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

from gribuki_trade.domain.paper_day import PaperDayEvent, PaperDayRunManifest
from gribuki_trade.services.ashare.ashare_intraday_paper import (
    IntradayOrderQuantityRule,
    IntradayPaperOrder,
    IntradayPaperRiskConfig,
    IntradayPriceAcceptance,
    IntradaySellQuantityPlan,
    IntradaySellQuantityStatus,
)
from gribuki_trade.services.ashare.ashare_paper_day_config import (
    PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
    ASharePaperDayConfig,
)
from gribuki_trade.services.ashare.ashare_paper_day_serialization import (
    _document_sha256,
)

_REMOVE_POSITION_COUNT_CAP = "REMOVE_POSITION_COUNT_CAP"
_ADD_PRICE_ACCEPTANCE_BOUNDS = "ADD_PRICE_ACCEPTANCE_BOUNDS"
_ADD_BOARD_QUANTITY_RULES = "ADD_BOARD_QUANTITY_RULES"


class PaperDayRiskPolicyChangeError(RuntimeError):
    """运行时风险策略迁移在修改任何日志前失败。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"PAPER day risk-policy change unavailable ({code})")


@dataclass(frozen=True, slots=True)
class _RiskPolicyMigration:
    """等待追加日志、且已完整校验的旧策略到新策略迁移。"""

    previous_policy: Mapping[str, object]
    new_policy: Mapping[str, object]
    reason_codes: tuple[str, ...]
    policy_diff: tuple[Mapping[str, object], ...]
    baseline_source: str
    source_event: PaperDayEvent | None


def requested_risk_policy_migration(
    *,
    events: tuple[PaperDayEvent, ...],
    manifest: PaperDayRunManifest,
    new_policy: Mapping[str, object],
    confirmation: str | None,
) -> _RiskPolicyMigration | None:
    """验证精确且已显式授权的运行时策略变更。"""

    baseline = _latest_risk_policy_baseline(events, manifest)
    if baseline is None:
        reconstructed = _reconstruct_legacy_cli_risk_policy(manifest)
        if reconstructed is not None:
            baseline = (None, "LEGACY_CLI_DEFAULTS_RECONSTRUCTED", reconstructed)
        else:
            # 不得把缺少可审计策略基线的留存运行视为通配状态。
            # 显式确认也不能让未知旧策略变得可以安全重解释。
            raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_BASELINE_MISSING")
    source_event, baseline_source, previous_policy = baseline
    if previous_policy == dict(new_policy):
        if confirmation is not None:
            if matching_risk_policy_migration_exists(events, new_policy):
                return None
            raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_NOT_REQUIRED")
        return None
    if confirmation != PAPER_RISK_POLICY_CHANGE_CONFIRMATION:
        raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_CONFIRMATION_REQUIRED")
    reason_codes = allowed_risk_policy_change_reasons(previous_policy, new_policy)
    if reason_codes is None:
        raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_NOT_ALLOWED")
    return _RiskPolicyMigration(
        previous_policy=previous_policy,
        new_policy=dict(new_policy),
        reason_codes=reason_codes,
        policy_diff=risk_policy_diff(previous_policy, new_policy),
        baseline_source=baseline_source,
        source_event=source_event,
    )


def _latest_risk_policy_baseline(
    events: tuple[PaperDayEvent, ...],
    manifest: PaperDayRunManifest,
) -> tuple[PaperDayEvent | None, str, dict[str, object]] | None:
    for event in reversed(events):
        value = event.payload.get("risk_policy")
        if isinstance(value, dict):
            return event, "JOURNAL_EVENT", dict(value)
    value = manifest.config.get("intraday_risk_policy")
    if isinstance(value, dict):
        return None, "RUN_MANIFEST", dict(value)
    return None


def _reconstruct_legacy_cli_risk_policy(
    manifest: PaperDayRunManifest,
) -> dict[str, object] | None:
    """仅重建旧版 CLI 中固定且不可配置的策略。"""

    config = manifest.config
    runner_config = ASharePaperDayConfig(initial_cash=manifest.initial_cash).audit_document()
    runtime_keys = {
        "calendar_provider",
        "calendar_verified",
        "latest_completed_session",
        "notification_channel",
        "notification_preflight_policy",
        "notification_preflight_required",
        "notification_target_kind",
    }
    latest_completed = config.get("latest_completed_session")
    target_kind = config.get("notification_target_kind")
    if (
        set(config) != set(runner_config) | runtime_keys
        or any(config.get(key) != value for key, value in runner_config.items())
        or config.get("calendar_provider") != "BaoStock"
        or config.get("calendar_verified") is not True
        or config.get("notification_channel") != "onebot"
        or config.get("notification_preflight_policy") != "GET_STATUS_GOOD_AND_ONLINE"
        or config.get("notification_preflight_required") is not True
        or target_kind not in {"private", "group"}
        or not isinstance(latest_completed, str)
    ):
        return None
    try:
        latest_completed_date = date.fromisoformat(latest_completed)
    except ValueError:
        return None
    if latest_completed_date >= manifest.session_date:
        return None
    return {
        "initial_equity": str(manifest.initial_cash),
        "cash_reserve_fraction": "0.20",
        "maximum_gross_fraction": "0.80",
        "maximum_positions": 5,
        "maximum_symbol_fraction": "0.20",
        "risk_per_trade_fraction": "0.0075",
        "minimum_stop_fraction": "0.015",
        "stock_lot_size": 100,
        "volume_participation_rate": "0.01",
        "buy_limit_markup": "0.001",
        "stock_slippage_rate": "0.0005",
        "etf_slippage_rate": "0.0002",
        "stock_price_quantum": "0.01",
        "etf_price_quantum": "0.001",
        "latest_entry_time": "14:55:00",
        "execution_policy": "NEXT_FULLY_POST_SIGNAL_1M_INTERVAL_IOC",
    }


def matching_risk_policy_migration_exists(
    events: tuple[PaperDayEvent, ...],
    new_policy: Mapping[str, object],
) -> bool:
    """检查同一策略迁移是否已经追加到事件链，保证重试幂等。"""

    expected_sha256 = _document_sha256(new_policy)
    expected_reasons = (
        _REMOVE_POSITION_COUNT_CAP,
        _ADD_PRICE_ACCEPTANCE_BOUNDS,
        _ADD_BOARD_QUANTITY_RULES,
    )
    for event in reversed(events):
        if event.event_type != "OPERATOR_RISK_POLICY_CHANGED":
            continue
        payload = event.payload
        reasons = payload.get("reason_codes")
        previous_policy = payload.get("old_risk_policy")
        retained_new_policy = payload.get("new_risk_policy")
        return (
            payload.get("operator_authorized") is True
            and payload.get("authorization_code") == PAPER_RISK_POLICY_CHANGE_CONFIRMATION
            and payload.get("new_risk_policy_sha256") == expected_sha256
            and isinstance(previous_policy, dict)
            and payload.get("old_risk_policy_sha256") == _document_sha256(previous_policy)
            and retained_new_policy == dict(new_policy)
            and payload.get("risk_policy") == dict(new_policy)
            and payload.get("risk_policy_sha256") == expected_sha256
            and isinstance(reasons, list)
            and all(isinstance(item, str) for item in reasons)
            and tuple(reasons) == expected_reasons
            and allowed_risk_policy_change_reasons(previous_policy, new_policy)
            == expected_reasons
        )
    return False


def allowed_risk_policy_change_reasons(
    previous_policy: Mapping[str, object],
    new_policy: Mapping[str, object],
) -> tuple[str, ...] | None:
    """仅允许今日已复核的三项变更，拒绝其他所有差异。"""

    missing = object()
    previous = dict(previous_policy)
    current = dict(new_policy)

    previous_maximum = previous.pop("maximum_positions", missing)
    current_maximum = current.pop("maximum_positions", missing)
    previous_count_enabled = previous.pop("position_count_limit_enabled", missing)
    current_count_enabled = current.pop("position_count_limit_enabled", missing)
    if not (
        previous_maximum == 5
        and current_maximum is None
        and (previous_count_enabled is missing or previous_count_enabled is True)
        and current_count_enabled is False
    ):
        return None

    previous_sell_markdown = previous.pop("sell_limit_markdown", missing)
    current_sell_markdown = current.pop("sell_limit_markdown", missing)
    previous_price_policy = previous.pop("price_acceptance_policy", missing)
    current_price_policy = current.pop("price_acceptance_policy", missing)
    expected_price_policy = IntradayPaperRiskConfig().audit_document().get(
        "price_acceptance_policy"
    )
    if not (
        (previous_sell_markdown is missing or previous_sell_markdown == "0.001")
        and current_sell_markdown == "0.001"
        and previous_price_policy is missing
        and isinstance(current_price_policy, dict)
        and current_price_policy == expected_price_policy
    ):
        return None
    previous_quantity_policy = previous.pop("order_quantity_policy", missing)
    current_quantity_policy = current.pop("order_quantity_policy", missing)
    expected_quantity_policy = IntradayPaperRiskConfig().audit_document().get(
        "order_quantity_policy"
    )
    if not (
        previous_quantity_policy is missing
        and isinstance(current_quantity_policy, dict)
        and current_quantity_policy == expected_quantity_policy
    ):
        return None
    if previous != current:
        return None
    return (
        _REMOVE_POSITION_COUNT_CAP,
        _ADD_PRICE_ACCEPTANCE_BOUNDS,
        _ADD_BOARD_QUANTITY_RULES,
    )


def risk_policy_diff(
    previous_policy: Mapping[str, object],
    new_policy: Mapping[str, object],
) -> tuple[Mapping[str, object], ...]:
    """按字段排序生成可审计的策略差异。"""

    changes: list[Mapping[str, object]] = []
    for field in sorted(set(previous_policy) | set(new_policy)):
        old_present = field in previous_policy
        new_present = field in new_policy
        old_value = previous_policy.get(field)
        new_value = new_policy.get(field)
        if old_present == new_present and old_value == new_value:
            continue
        changes.append(
            {
                "field": field,
                "new_present": new_present,
                "new_value": new_value,
                "old_present": old_present,
                "old_value": old_value,
            }
        )
    return tuple(changes)


def _incomplete_fill_ids(events: tuple[PaperDayEvent, ...]) -> tuple[str, ...]:
    applied = {
        item.correlation_id
        for item in events
        if item.event_type == "FILL_APPLIED" and item.correlation_id is not None
    }
    started = {
        item.correlation_id
        for item in events
        if item.event_type == "FILL_STARTED" and item.correlation_id is not None
    }
    terminal_match_fills: set[str] = set()
    for event in events:
        if event.event_type != "ORDER_MATCH_EVALUATED":
            continue
        fill = event.payload.get("fill")
        fill_id = fill.get("fill_id") if isinstance(fill, dict) else None
        if isinstance(fill_id, str):
            terminal_match_fills.add(fill_id)
    return tuple(sorted((started | terminal_match_fills) - applied))


def validate_risk_policy_migration_state(
    *,
    pending: Mapping[str, IntradayPaperOrder],
    events: tuple[PaperDayEvent, ...],
) -> None:
    """拒绝重解释任何未结委托或未完成的成交 saga。"""

    if pending:
        raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_PENDING_ORDER")
    if _incomplete_fill_ids(events):
        raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_INCOMPLETE_FILL")


def price_acceptance_document(
    value: IntradayPriceAcceptance | None,
) -> dict[str, object] | None:
    """把价格笼子转换成稳定审计文档。"""

    if value is None:
        return None
    return {
        "acceptable_lower_inclusive": value.acceptable_lower,
        "acceptable_upper_inclusive": value.acceptable_upper,
        "board": value.board.value,
        "exchange_lower": value.exchange_lower,
        "exchange_upper": value.exchange_upper,
        "exchange_band_regime": "CONSERVATIVE_STANDARD_NON_ST_BOARD_BAND",
        "continuous_auction_price_cage_status": "NOT_L1_VERIFIED_PAPER_MINUTE_BAR_ONLY",
        "real_broker_submission_allowed": False,
        "invalidation_boundary_exclusive": value.invalidation_price,
        "limit_price": value.limit_price,
        "policy_version": value.policy_version,
        "price_tick": value.price_tick,
        "reference_price": value.reference_price,
        "side": value.side.value,
    }


def quantity_rule_document(
    value: IntradayOrderQuantityRule | None,
) -> dict[str, object] | None:
    """把板块申报数量规则转换成稳定审计文档。"""

    if value is None:
        return None
    return {
        "board": value.board.value,
        "buy_increment": value.buy_increment,
        "limit_order_type": "LIMIT",
        "maximum_limit_order_quantity": value.maximum_limit_order_quantity,
        "minimum_buy_quantity": value.minimum_buy_quantity,
        "minimum_regular_sell_quantity": value.minimum_regular_sell_quantity,
        "paper_partial_fill_increment": value.paper_partial_fill_increment,
        "sell_increment": value.sell_increment,
        "sell_residual_policy": value.sell_residual_policy,
        "policy_version": value.policy_version,
    }


def sell_quantity_plan_document(
    value: IntradaySellQuantityPlan | None,
) -> dict[str, object] | None:
    """把卖出数量计划转换成可重放文档。"""

    if value is None:
        return None
    order_sequence = [*value.regular_order_quantities]
    if value.residual_sell_all_quantity > 0:
        order_sequence.append(value.residual_sell_all_quantity)
    return {
        "available_to_sell": value.available_to_sell,
        "future_limit_order_sequence": order_sequence,
        "quantity_rule": quantity_rule_document(value.rule),
        "regular_order_quantities": list(value.regular_order_quantities),
        "residual_component_quantity": value.residual_component_quantity,
        "residual_must_be_sold_all_once": value.residual_component_quantity > 0,
        "residual_sell_all_quantity": value.residual_sell_all_quantity,
        "status": value.status.value,
    }


def sell_quantity_plan_text(value: IntradaySellQuantityPlan | None) -> str:
    """生成面向操作员的中文卖出数量解释。"""

    if value is None:
        return "卖出数量规则：板块不受当前 PAPER 执行模型支持，未来不得创建卖单。"
    rule = value.rule
    if value.status is IntradaySellQuantityStatus.NO_SELLABLE_QUANTITY:
        return "卖出数量规则：当前无 T+1 可卖数量；今日买入部分不得当日卖出。"
    sequence = "、".join(
        str(item)
        for item in (
            *value.regular_order_quantities,
            *((value.residual_sell_all_quantity,) if value.residual_sell_all_quantity > 0 else ()),
        )
    )
    rule_text = (
        f"卖出数量规则：常规限价卖单最少{rule.minimum_regular_sell_quantity}股，"
        f"其后按{rule.sell_increment}股递增，单笔最多"
        f"{rule.maximum_limit_order_quantity}股；未来申报序列为{sequence}股。"
    )
    if value.residual_component_quantity == 0:
        return rule_text
    return (
        f"{rule_text} 末笔{value.residual_sell_all_quantity}股包含"
        f"{value.residual_component_quantity}股余股，必须作为届时全部余额一次性卖出，"
        "不得拆成多笔余股申报。"
    )


__all__ = [
    "PAPER_RISK_POLICY_CHANGE_CONFIRMATION",
    "PaperDayRiskPolicyChangeError",
    "_RiskPolicyMigration",
    "_ADD_BOARD_QUANTITY_RULES",
    "_ADD_PRICE_ACCEPTANCE_BOUNDS",
    "_REMOVE_POSITION_COUNT_CAP",
    "_incomplete_fill_ids",
    "_latest_risk_policy_baseline",
    "_reconstruct_legacy_cli_risk_policy",
    "allowed_risk_policy_change_reasons",
    "matching_risk_policy_migration_exists",
    "price_acceptance_document",
    "quantity_rule_document",
    "requested_risk_policy_migration",
    "risk_policy_diff",
    "sell_quantity_plan_document",
    "sell_quantity_plan_text",
    "validate_risk_policy_migration_state",
]
