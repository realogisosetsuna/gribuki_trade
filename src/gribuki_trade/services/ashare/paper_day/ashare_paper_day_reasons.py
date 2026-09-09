"""A 股 PAPER-day 入场拒绝和单分钟撮合原因的纯展示映射。

本模块只保存稳定原因码到中文说明的映射，以及不带副作用的展示函数。
运行器、账本、事件日志和通知生命周期仍由 ``ashare_paper_day`` 负责。
"""

from __future__ import annotations

from collections.abc import Mapping

_ENTRY_REJECTION_EXPLANATIONS: Mapping[str, str] = {
    "NO_CURRENT_SESSION_ANOMALY": (
        "最近一次全市场扫描没有把该标的列为当日异动候选；只有单股分钟技术形态，缺少横截面确认"
    ),
    "ANOMALY_NOT_MOMENTUM_EXPANSION": ("标的虽在异动名单中，但尚未达到动量扩张级别"),
    "NO_CURRENT_SESSION_SCAN": "尚无本交易日全市场扫描结果",
    "CURRENT_SESSION_SCAN_NOT_COMPLETE": "最近一次全市场扫描数据不完整或处于降级状态",
    "CURRENT_SESSION_ANOMALY_EXPIRED": "最近一次异动确认已经超过有效期",
    "SYMBOL_ALREADY_ENTERED_TODAY": "该标的今日已经模拟买入，不重复加仓",
    "SYMBOL_ORDER_ALREADY_PENDING": "该标的已有待撮合 PAPER 委托",
    "PENDING_CAPACITY_RESERVED": "已有另一笔待撮合委托占用保守资金容量",
    "SYMBOL_ALREADY_HELD": "账户已经持有该标的，今日策略禁止重复加仓",
    "MINUTE_DATA_STALE": "最新完成分钟线已经过期",
    "UNAPPROVED_DEGRADED_MINUTE_PROVIDER": "分钟行情来自未获准的降级数据源",
    "DEGRADED_SOURCE_NOT_INDEPENDENTLY_CORROBORATED": (
        "降级分钟行情没有得到独立全市场快照交叉确认"
    ),
    "CORROBORATION_PRICE_MISSING": "交叉确认快照缺少有效价格",
    "CROSS_SOURCE_PRICE_DIVERGENCE": "分钟行情与全市场快照价格偏差超过容许范围",
    "MINUTE_FRESHNESS_NOT_CURRENT": "分钟行情的新鲜度状态不是当前可用",
    "SIGNAL_NOT_ENTER_CANDIDATE": "技术结论不是可入场候选",
    "SIGNAL_PRICE_MISSING": "技术信号缺少有效参考价格",
    "INVALIDATION_PRICE_MISSING": "技术信号缺少失效参考价格",
    "INVALIDATION_NOT_BELOW_ENTRY": "失效参考价格没有低于拟入场价格",
    "SIGNAL_NOT_COMPLETED": "信号所用分钟线尚未完整收盘",
    "SIGNAL_SESSION_MISMATCH": "信号不属于当前交易日",
    "ACCOUNT_SESSION_MISMATCH": "PAPER 账户交易日尚未对齐",
    "ENTRY_CUTOFF_PASSED": "已超过当日允许新建买入委托的最晚时间",
    "BOARD_MISSING": "缺少可验证的上市板块信息",
    "BOARD_UNSUPPORTED": "当前盘中执行模型不支持该上市板块",
    "BOARD_SYMBOL_MISMATCH": "证券代码与上市板块不一致",
    "PREVIOUS_CLOSE_MISSING": "缺少可验证的上一交易日收盘价",
    "PRICE_OUTSIDE_DAILY_BAND": "信号价格超出当日价格限制范围",
    "LIMIT_OUTSIDE_DAILY_BAND": "拟定限价超出当日价格限制范围",
    "PROTECTIVE_STOP_INVALID": "成交前保护止损无效",
    "QUICK_EXIT_PLAN_UNAVAILABLE": "无法用当前时点数据形成成交前 QUICK 退出计划",
    "QUICK_EXIT_CONDITION_ALREADY_MET": "买入前已经触及 QUICK 退出条件，拒绝建立新仓",
    "MAX_POSITIONS_REACHED": "显式启用的持仓数量熔断器已达到上限",
    "GROSS_LIMIT_REACHED": "组合总敞口已达到上限",
    "SYMBOL_LIMIT_REACHED": "单一标的资金上限已达到",
    "CASH_RESERVE_BINDING": "执行后会侵占最低现金储备",
    "BELOW_ROUND_LOT": "按风险与资金约束计算的数量不足一手",
    "BELOW_BOARD_MINIMUM_BUY": ("按风险、资金、费用与板块申报规则测算后不足最低买入数量"),
    "LLM_PREOPEN_CONTEXT_UNAVAILABLE": "盘前宏观基线或原证据快照不可用",
    "LLM_REVIEW_NOT_READY": "候选复核尚未完成；买入路径不会等待模型或网络",
    "LLM_REVIEW_NOT_JOURNALED": "模型结果尚未先写入不可变日志，不能用于交易",
    "LLM_REVIEW_EXPIRED": "候选复核已超过冻结有效期",
    "LLM_REVIEW_INPUT_MISMATCH": "复核结果与当前候选或扫描证据作用域不一致",
    "LLM_REVIEW_KNOWN_AFTER_SIGNAL": "复核结果晚于技术信号可知，禁止回看使用",
    "LLM_MODEL_IDENTITY_MISMATCH": "请求模型、响应模型或提示词合约身份不一致",
    "LLM_REVIEW_FAILED": "候选模型复核失败",
    "LLM_REVIEW_ABSTAINED": "候选模型复核明确弃权",
    "LLM_EVIDENCE_INVALID": "复核证据覆盖不足或引用无效",
    "LLM_NEGATIVE_VETO": "宏观复核达到负面否决阈值",
    "LLM_COMBINED_SCORE_BELOW_ENTRY_THRESHOLD": "技术分与宏观分融合后低于 0.70 入场阈值",
}


def _entry_rejection_display(reason: str | None) -> str:
    """将入场拒绝原因码转换成稳定的中文审计说明。"""

    if reason is None:
        return "入场门未通过，具体原因不可用"
    return _ENTRY_REJECTION_EXPLANATIONS.get(reason, "入场门未通过")


_MATCH_REASON_EXPLANATIONS: Mapping[str, str] = {
    "EXIT_CONDITION_MET_BEFORE_FILL": "撮合分钟已触及预登记退出条件，无法证明先买入后退出",
    "EXIT_PLAN_MISSING_BEFORE_FILL": "待撮合委托缺少可验证 QUICK 退出计划，已安全终止",
    "SIGNAL_INVALIDATED_BEFORE_FILL": "撮合分钟触及或跌破技术失效位，买入逻辑已失效",
    "LIMIT_NOT_TOUCHED": "撮合分钟没有触及买入限价",
    "BAR_OUTSIDE_DAILY_BAND": "分钟行情超出已验证的交易所日价格带",
    "LOCKED_LIMIT_UP_QUEUE_UNMODELED": "涨停板封死，分钟线无法证明买单排队能够成交",
    "VOLUME_CAPACITY_BELOW_LOT": "按成交量参与上限计算后低于该板块的 PAPER 撮合单位",
    "BAR_VOLUME_ZERO": "撮合分钟没有可验证成交量",
    "ORDER_EXPIRED": "委托已超过当日最晚入场时间",
    "FILL_OUTSIDE_ACCEPTABLE_RANGE": "拟成交价不在冻结的策略可接受区间内",
    "ORDER_QUANTITY_OUTSIDE_BOARD_RULES": ("委托数量不符合对应板块的最低数量、递增单位或单笔上限"),
}


def _match_reason_display(reason: str) -> str:
    """将单分钟撮合拒绝原因码转换成稳定的中文审计说明。"""

    return _MATCH_REASON_EXPLANATIONS.get(reason, "未满足单分钟 IOC 成交条件")


__all__ = [
    "_ENTRY_REJECTION_EXPLANATIONS",
    "_MATCH_REASON_EXPLANATIONS",
    "_entry_rejection_display",
    "_match_reason_display",
]
