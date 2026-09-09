"""PAPER 日报摘要的纯事件解释与执行投影。

本模块把 sidecar 事件载荷转换为不可变摘要模型，不读取文件或 SQLite。
摘要门面负责加载伴随文件、组装投影和写入报告。
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from decimal import Decimal

from .paper_day_projection_models import PaperDaySidecarEvent
from .paper_day_sidecar_codec import (
    counter_items as _sidecar_counter_items,
)
from .paper_day_sidecar_codec import (
    int_tuple as _sidecar_int_tuple,
)
from .paper_day_sidecar_codec import (
    string_tuple as _sidecar_string_tuple,
)
from .paper_day_summary_models import (
    PaperDayBuyExecutionProjection,
    PaperDayPriceAcceptanceProjection,
    PaperDayQuantityRuleProjection,
    PaperDayRiskPolicyChangeProjection,
    PaperDayRiskPolicyProjection,
    PaperDaySellExecutionProjection,
    PaperDaySourceTransition,
    PaperDayStableReasonProjection,
    PaperDayWatchlistChange,
)


def _optional_string(value: object) -> str | None:
    from .paper_day_codec import optional_string

    return optional_string(value)


def _optional_bool(value: object) -> bool | None:
    from .paper_day_codec import optional_bool

    return optional_bool(value)


def _optional_int(value: object) -> int | None:
    from .paper_day_codec import optional_int

    return optional_int(value)


def _optional_decimal(value: object) -> Decimal | None:
    from .paper_day_codec import optional_decimal

    return optional_decimal(value)


def _optional_object(value: object) -> dict[str, object] | None:
    from .paper_day_codec import optional_object

    return optional_object(value)


def _list_length(value: object) -> int | None:
    from .paper_day_codec import list_length

    return list_length(value)


_STABLE_REASON_EXPLANATIONS: Mapping[str, str] = {
    "UNSPECIFIED_REJECTION": "sidecar 记录了拒绝，但没有提供更具体的稳定原因码",
    "NO_CURRENT_SESSION_ANOMALY": (
        "最近一次全市场扫描未将该标的列为当日异动候选，只有分钟技术形态而缺少横截面确认"
    ),
    "ANOMALY_NOT_MOMENTUM_EXPANSION": "标的虽在异动名单中，但尚未达到动量扩张级别",
    "NO_CURRENT_SESSION_SCAN": "本交易日尚无可用的全市场扫描结果",
    "CURRENT_SESSION_SCAN_NOT_COMPLETE": "最近一次全市场扫描不完整或处于降级状态",
    "CURRENT_SESSION_ANOMALY_EXPIRED": "最近一次异动确认已超过有效期",
    "SYMBOL_ALREADY_ENTERED_TODAY": "该标的今日已经模拟买入，策略不重复加仓",
    "SYMBOL_ORDER_ALREADY_PENDING": "该标的已有待撮合 PAPER 委托",
    "PENDING_CAPACITY_RESERVED": "另一笔待撮合委托已占用保守资金容量",
    "SYMBOL_ALREADY_HELD": "账户已经持有该标的，今日策略禁止重复加仓",
    "MINUTE_DATA_STALE": "最新完整分钟线已经过期",
    "UNAPPROVED_DEGRADED_MINUTE_PROVIDER": "分钟行情来自未获准的降级数据源",
    "DEGRADED_SOURCE_NOT_INDEPENDENTLY_CORROBORATED": (
        "降级分钟行情没有得到独立全市场快照交叉确认"
    ),
    "CORROBORATION_PRICE_MISSING": "交叉确认快照缺少有效价格",
    "CROSS_SOURCE_PRICE_DIVERGENCE": "分钟行情与全市场快照价格偏差超过容许范围",
    "MINUTE_FRESHNESS_NOT_CURRENT": "分钟行情的新鲜度状态不是当前可用",
    "SIGNAL_NOT_ENTER_CANDIDATE": "技术结论不是可入场候选",
    "SIGNAL_PRICE_MISSING": "技术信号缺少有效参考价格",
    "INVALIDATION_PRICE_MISSING": "技术信号缺少有效失效价格",
    "INVALIDATION_NOT_BELOW_ENTRY": "失效价格没有低于拟入场价格",
    "SIGNAL_NOT_COMPLETED": "信号所用分钟线尚未完整收盘",
    "SIGNAL_SESSION_MISMATCH": "信号不属于当前交易日",
    "ACCOUNT_SESSION_MISMATCH": "PAPER 账户交易日未与信号对齐",
    "ENTRY_CUTOFF_PASSED": "已超过当日允许创建买入委托的最晚时间",
    "BOARD_MISSING": "缺少可验证的上市板块信息",
    "BOARD_UNSUPPORTED": "当前盘中执行模型不支持该板块",
    "BOARD_SYMBOL_MISMATCH": "证券代码与上市板块不一致",
    "PREVIOUS_CLOSE_MISSING": "缺少可验证的上一交易日收盘价",
    "PRICE_OUTSIDE_DAILY_BAND": "信号价格超出保守日价格带",
    "LIMIT_OUTSIDE_DAILY_BAND": "拟定限价超出保守日价格带",
    "MAX_POSITIONS_REACHED": "显式启用的持仓数量熔断器已达到上限",
    "GROSS_LIMIT_REACHED": "组合总敞口已达到策略上限",
    "SYMBOL_LIMIT_REACHED": "单一标的资金敞口已达到策略上限",
    "CASH_RESERVE_BINDING": "创建委托后会侵占最低现金储备",
    "BELOW_ROUND_LOT": "风险和资金预算计算出的数量不足一手",
    "BELOW_BOARD_MINIMUM_BUY": "风险、资金、费用和板块规则测算后不足最低买入数量",
    "SYMBOL_MISMATCH": "撮合行情标的与委托标的不一致",
    "BAR_NOT_CLOSED": "用于撮合的分钟线尚未完整收盘",
    "BAR_NOT_ONE_MINUTE": "撮合证据不是策略要求的 1 分钟线",
    "BAR_NOT_STRICTLY_AFTER_SIGNAL": "撮合分钟并未严格晚于信号分钟",
    "BAR_STARTED_BEFORE_ORDER": "撮合分钟早于委托激活时间",
    "SESSION_MISMATCH": "撮合分钟与委托不属于同一交易日",
    "ORDER_EXPIRED": "委托已经超过当日最晚入场时间",
    "BAR_NOT_YET_OBSERVABLE": "撮合分钟在决策时尚不可知，不能用于成交",
    "BAR_OHLC_INVALID": "撮合分钟的 OHLC 关系无效",
    "BAR_VOLUME_ZERO": "撮合分钟没有可验证成交量",
    "BAR_OUTSIDE_DAILY_BAND": "分钟行情超出保守日价格带",
    "LOCKED_LIMIT_UP_QUEUE_UNMODELED": "涨停封死，分钟线无法证明排队买单能够成交",
    "SIGNAL_INVALIDATED_BEFORE_FILL": "撮合分钟触及或跌破失效位，买入逻辑已失效",
    "LIMIT_NOT_TOUCHED": "撮合分钟没有触及买入限价",
    "VOLUME_CAPACITY_BELOW_LOT": "成交量参与上限折算后不足板块规定的 PAPER 撮合单位",
    "FILL_OUTSIDE_ACCEPTABLE_RANGE": "拟成交价不在冻结的策略可接受区间内",
    "ORDER_QUANTITY_OUTSIDE_BOARD_RULES": "委托数量不符合板块最低数量、递增单位或单笔上限",
    "MORNING_SESSION_ENDED": "上午交易时段结束，未撮合委托在午休前撤销",
    "ORDER_EXPIRED_AT_RECESS": "午休边界到达，未成交委托已撤销",
    "ORDER_EXPIRED_AT_CLOSE": "收盘边界到达，未成交委托已撤销",
    "REFERENCE_OR_PREVIOUS_CLOSE_MISSING": "缺少卖出参考价或昨收，无法冻结价格区间",
    "PRICE_CORRIDOR_INVALID": "卖出价格区间或价格带元数据无效",
    "NO_SELL_ORDER_BY_DAY_TEST_POLICY": "本次全天测试明确只记录卖出信号，不创建卖单",
    "T1_SELLABLE_QUANTITY_ZERO": "今日买入股份受 T+1 限制，当前可卖数量为零",
    "SELL_QUANTITY_RULE_UNAVAILABLE": "板块数量规则不可用，未来不得创建卖单",
    "NEXT_EXECUTION_PRICE_BELOW_SELL_LIMIT": "下一执行价低于最低接受卖价时不得假设成交",
    "NEXT_BAR_HIGH_BELOW_SELL_LIMIT": "下一分钟最高价仍低于卖出限价时不得假设成交",
    "LOCKED_LIMIT_DOWN_QUEUE_UNMODELED": "跌停封死时分钟线无法证明卖单排队能够成交",
    "INSUFFICIENT_VERIFIED_VOLUME": "可验证成交量不足，不能模拟足额卖出",
    "SELL_QUANTITY_NOT_BOARD_VALID": "拟卖数量不符合对应板块申报规则",
    "PRICE_BAND_OR_REFERENCE_METADATA_INVALID": "日价格带或参考价格元数据无效",
    "REMOVE_POSITION_COUNT_CAP": "取消原固定 5 个持仓的计数硬上限",
    "ADD_PRICE_ACCEPTANCE_BOUNDS": "为买入和卖出增加可审计的价格接受区间与破位不成交规则",
    "ADD_BOARD_QUANTITY_RULES": "增加主板、创业板和科创板的分板块申报数量规则",
    "NOT_L1_VERIFIED_PAPER_MINUTE_BAR_ONLY": "仅有分钟 K 证据，未验证实时盘口价格笼子",
    "LLM_PREOPEN_CONTEXT_UNAVAILABLE": "盘前宏观基线不可用，强制复核模式下买入失败关闭",
    "LLM_PREOPEN_CONTEXT_MODEL_MISMATCH": "盘前宏观基线的响应模型与冻结请求模型不一致",
    "LLM_EVIDENCE_SNAPSHOT_UNAVAILABLE": "原始证据快照不可按清单精确重放，禁止静默换用新快照",
    "LLM_REVIEW_NOT_READY": "候选复核尚未完成；买入路径不会等待网络或模型",
    "LLM_REVIEW_NOT_JOURNALED": "模型结果已返回但尚未先写入审计日志，因此不可用于交易",
    "LLM_REVIEW_EXPIRED": "候选复核已超过冻结的有效期",
    "LLM_REVIEW_INPUT_MISMATCH": "复核结果与当前候选、扫描版本或证据作用域不一致",
    "LLM_REVIEW_KNOWN_AFTER_SIGNAL": "复核结果在技术信号之后才可知，不允许回看使用",
    "LLM_MODEL_IDENTITY_MISMATCH": "请求模型、响应模型或提示词合约身份不一致",
    "LLM_REVIEW_FAILED": "模型复核失败并以稳定失败码留痕",
    "LLM_REVIEW_ABSTAINED": "模型明确选择弃权，强制复核模式下不得买入",
    "LLM_EVIDENCE_INVALID": "复核证据覆盖不足、无引用或包含未知引用",
    "LLM_NEGATIVE_VETO": "宏观复核达到负面否决阈值，只能否决技术入场",
    "LLM_COMBINED_SCORE_BELOW_ENTRY_THRESHOLD": "技术分与宏观分融合后低于 0.70 入场阈值",
}


def _stable_reason_explanation(code: str) -> str:
    return _STABLE_REASON_EXPLANATIONS.get(
        code,
        "该稳定码暂无内置中文释义，请以对应不可变事件 payload 为准",
    )


def _watchlist_changes(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDayWatchlistChange, ...]:
    result = []
    for event in events:
        if event.event_type != "WATCHLIST_UPDATED":
            continue
        added = _string_tuple(event.payload.get("added"))
        removed = _string_tuple(event.payload.get("removed"))
        resulting_count = _list_length(event.payload.get("watchlist"))
        result.append(
            PaperDayWatchlistChange(
                sequence=event.sequence,
                known_at=event.known_at,
                added=added,
                removed=removed,
                resulting_count=resulting_count,
            )
        )
    return tuple(result)


def _string_tuple(value: object) -> tuple[str, ...]:
    return _sidecar_string_tuple(value)


def _counter_items(counter: Counter[str]) -> tuple[tuple[str, int], ...]:
    return _sidecar_counter_items(counter)


def _price_acceptance(
    value: object,
    *,
    status: str,
) -> PaperDayPriceAcceptanceProjection:
    document = _optional_object(value) or {}
    return PaperDayPriceAcceptanceProjection(
        status=status,
        side=_optional_string(document.get("side")),
        board=_optional_string(document.get("board")),
        acceptable_lower=_optional_decimal(document.get("acceptable_lower_inclusive")),
        acceptable_upper=_optional_decimal(document.get("acceptable_upper_inclusive")),
        exchange_lower=_optional_decimal(document.get("exchange_lower")),
        exchange_upper=_optional_decimal(document.get("exchange_upper")),
        invalidation_boundary=_optional_decimal(document.get("invalidation_boundary_exclusive")),
        limit_price=_optional_decimal(document.get("limit_price")),
        reference_price=_optional_decimal(document.get("reference_price")),
        price_tick=_optional_decimal(document.get("price_tick")),
        policy_version=_optional_string(document.get("policy_version")),
        price_cage_status=_optional_string(document.get("continuous_auction_price_cage_status")),
        real_broker_submission_allowed=_optional_bool(
            document.get("real_broker_submission_allowed")
        ),
    )


def _quantity_rule(
    value: object,
    *,
    board_override: str | None = None,
) -> PaperDayQuantityRuleProjection | None:
    document = _optional_object(value)
    if document is None:
        return None
    return PaperDayQuantityRuleProjection(
        board=board_override or _optional_string(document.get("board")),
        minimum_buy_quantity=_optional_int(document.get("minimum_buy_quantity")),
        buy_increment=_optional_int(document.get("buy_increment")),
        maximum_limit_order_quantity=_optional_int(document.get("maximum_limit_order_quantity")),
        paper_partial_fill_increment=_optional_int(document.get("paper_partial_fill_increment")),
        minimum_regular_sell_quantity=_optional_int(document.get("minimum_regular_sell_quantity")),
        sell_increment=_optional_int(document.get("sell_increment")),
        sell_residual_policy=_optional_string(document.get("sell_residual_policy")),
        policy_version=_optional_string(document.get("policy_version")),
    )


def _risk_policy_changes(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDayRiskPolicyChangeProjection, ...]:
    result: list[PaperDayRiskPolicyChangeProjection] = []
    for event in events:
        if event.event_type != "OPERATOR_RISK_POLICY_CHANGED":
            continue
        previous = _optional_object(event.payload.get("old_risk_policy")) or {}
        current = _optional_object(event.payload.get("new_risk_policy")) or {}
        result.append(
            PaperDayRiskPolicyChangeProjection(
                sequence=event.sequence,
                known_at=event.known_at,
                operator_authorized=_optional_bool(event.payload.get("operator_authorized")),
                reason_codes=_string_tuple(event.payload.get("reason_codes")),
                old_policy_sha256=_optional_string(event.payload.get("old_risk_policy_sha256")),
                new_policy_sha256=_optional_string(event.payload.get("new_risk_policy_sha256")),
                old_maximum_positions=_optional_int(previous.get("maximum_positions")),
                new_maximum_positions=_optional_int(current.get("maximum_positions")),
                new_position_count_limit_enabled=_optional_bool(
                    current.get("position_count_limit_enabled")
                ),
                validated_fill_count=_optional_int(event.payload.get("validated_fill_count")),
                validated_position_count=_optional_int(
                    event.payload.get("validated_position_count")
                ),
            )
        )
    return tuple(result)


def _current_risk_policy(
    events: Iterable[PaperDaySidecarEvent],
) -> PaperDayRiskPolicyProjection | None:
    selected: tuple[PaperDaySidecarEvent, dict[str, object]] | None = None
    policy_event_types = {
        "DAY_STARTED",
        "OPERATOR_RISK_POLICY_CHANGED",
        "RUNNER_RECOVERY_AFTER_ABORT",
        "RUNNER_RESUMED",
    }
    for event in events:
        if event.event_type not in policy_event_types:
            continue
        policy = _optional_object(event.payload.get("risk_policy"))
        if policy is None and event.event_type == "OPERATOR_RISK_POLICY_CHANGED":
            policy = _optional_object(event.payload.get("new_risk_policy"))
        if policy is not None:
            selected = (event, policy)
    if selected is None:
        return None
    event, policy = selected
    price_policy = _optional_object(policy.get("price_acceptance_policy")) or {}
    quantity_policy = _optional_object(policy.get("order_quantity_policy")) or {}
    quantity_rules = tuple(
        rule
        for board, value in quantity_policy.items()
        if board != "version"
        if (rule := _quantity_rule(value, board_override=board)) is not None
    )
    unsupported_quantity_boards = tuple(
        board
        for board, value in quantity_policy.items()
        if board != "version" and isinstance(value, str)
    )
    return PaperDayRiskPolicyProjection(
        sequence=event.sequence,
        known_at=event.known_at,
        source_event_type=event.event_type,
        maximum_positions=_optional_int(policy.get("maximum_positions")),
        position_count_limit_enabled=_optional_bool(policy.get("position_count_limit_enabled")),
        cash_reserve_fraction=_optional_decimal(policy.get("cash_reserve_fraction")),
        maximum_gross_fraction=_optional_decimal(policy.get("maximum_gross_fraction")),
        maximum_symbol_fraction=_optional_decimal(policy.get("maximum_symbol_fraction")),
        risk_per_trade_fraction=_optional_decimal(policy.get("risk_per_trade_fraction")),
        price_acceptance_policy_version=_optional_string(price_policy.get("version")),
        order_quantity_policy_version=_optional_string(quantity_policy.get("version")),
        order_quantity_rules=quantity_rules,
        unsupported_quantity_boards=unsupported_quantity_boards,
    )


def _buy_execution_acceptances(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDayBuyExecutionProjection, ...]:
    result: list[PaperDayBuyExecutionProjection] = []
    for event in events:
        if event.event_type != "ORDER_SUBMITTED":
            continue
        acceptance_document = _optional_object(event.payload.get("price_acceptance"))
        if acceptance_document is None:
            continue
        order = _optional_object(event.payload.get("order")) or {}
        symbol = event.symbol or _optional_string(order.get("symbol"))
        if symbol is None:
            continue
        result.append(
            PaperDayBuyExecutionProjection(
                sequence=event.sequence,
                known_at=event.known_at,
                symbol=symbol,
                order_id=_optional_string(order.get("order_id")) or event.correlation_id,
                quantity=_optional_int(order.get("quantity")),
                price_acceptance=_price_acceptance(
                    acceptance_document,
                    status="AVAILABLE",
                ),
                quantity_rule=_quantity_rule(event.payload.get("quantity_rule")),
            )
        )
    return tuple(result)


def _sell_execution_acceptances(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDaySellExecutionProjection, ...]:
    result: list[PaperDaySellExecutionProjection] = []
    for event in events:
        if event.event_type != "SELL_PRICE_ACCEPTANCE_EVALUATED":
            continue
        symbol = event.symbol
        if symbol is None:
            continue
        plan = _optional_object(event.payload.get("sell_quantity_plan")) or {}
        status = _optional_string(event.payload.get("price_acceptance_status")) or "UNKNOWN"
        result.append(
            PaperDaySellExecutionProjection(
                sequence=event.sequence,
                known_at=event.known_at,
                symbol=symbol,
                price_acceptance=_price_acceptance(
                    event.payload.get("price_acceptance"),
                    status=status,
                ),
                quantity_rule=_quantity_rule(event.payload.get("quantity_rule")),
                available_to_sell=_optional_int(plan.get("available_to_sell")),
                quantity_plan_status=_optional_string(plan.get("status")),
                future_limit_order_sequence=_int_tuple(plan.get("future_limit_order_sequence")),
                current_blockers=_string_tuple(event.payload.get("current_execution_blockers")),
                future_non_execution_conditions=_string_tuple(
                    event.payload.get("future_non_execution_conditions")
                ),
            )
        )
    return tuple(result)


def _int_tuple(value: object) -> tuple[int, ...]:
    return _sidecar_int_tuple(value)


def _stable_rejection_reasons(
    *,
    buy_reject_reasons: Counter[str],
    expiry_reasons: Counter[str],
    sell_acceptances: tuple[PaperDaySellExecutionProjection, ...],
) -> tuple[PaperDayStableReasonProjection, ...]:
    counts: Counter[tuple[str, str]] = Counter()
    for code, count in buy_reject_reasons.items():
        counts[("买入门控/风控", code)] += count
    for code, count in expiry_reasons.items():
        counts[("PAPER 撮合/到期", code)] += count
    for acceptance in sell_acceptances:
        if acceptance.price_acceptance.status not in {"AVAILABLE", "UNKNOWN"}:
            counts[("卖出价格评估", acceptance.price_acceptance.status)] += 1
        for code in acceptance.current_blockers:
            counts[("卖出当前阻断", code)] += 1
    return tuple(
        PaperDayStableReasonProjection(
            category=category,
            code=code,
            count=count,
            explanation=_stable_reason_explanation(code),
        )
        for (category, code), count in sorted(
            counts.items(),
            key=lambda item: (item[0][0], -item[1], item[0][1]),
        )
    )


def _source_transitions(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDaySourceTransition, ...]:
    result = []
    for event in events:
        if event.event_type != "SOURCE_STATE_CHANGED":
            continue
        component = _optional_string(event.payload.get("component"))
        state = _optional_string(event.payload.get("state"))
        if component is None or state is None:
            continue
        result.append(
            PaperDaySourceTransition(
                sequence=event.sequence,
                known_at=event.known_at,
                component=component,
                previous_state=_optional_string(event.payload.get("previous_state")),
                state=state,
                error_code=_optional_string(event.payload.get("error_code")),
            )
        )
    return tuple(result)


def _source_recovery_count(transitions: tuple[PaperDaySourceTransition, ...]) -> int:
    latest: dict[str, str] = {}
    recoveries = 0
    for item in transitions:
        previous = latest.get(item.component, item.previous_state)
        if item.state == "HEALTHY" and previous not in (None, "HEALTHY"):
            recoveries += 1
        latest[item.component] = item.state
    return recoveries
