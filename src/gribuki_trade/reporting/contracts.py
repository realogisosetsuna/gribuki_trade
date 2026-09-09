"""面向用户的稳定报告分类、交付方式与结构校验。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum


class ReportKind(StrEnum):
    INTRADAY_ALERT = "INTRADAY_ALERT"
    EXECUTION_RECEIPT = "EXECUTION_RECEIPT"
    POSITION_REVIEW = "POSITION_REVIEW"
    DAILY_REVIEW = "DAILY_REVIEW"
    INSTRUMENT_RESEARCH = "INSTRUMENT_RESEARCH"
    SYSTEM_HEALTH = "SYSTEM_HEALTH"


class ReportDelivery(StrEnum):
    SHORT_TEXT = "SHORT_TEXT"
    MARKDOWN_FILE = "MARKDOWN_FILE"
    # 调用场景可以在低延迟短文本与可归档 Markdown 之间选择其一；名称刻意
    # 不使用 AND，避免把“支持两种格式”误写成“每次必须同时发送两份”。
    SHORT_TEXT_OR_MARKDOWN = "SHORT_TEXT_OR_MARKDOWN"
    SHORT_TEXT_AND_MARKDOWN = "SHORT_TEXT_AND_MARKDOWN"


@dataclass(frozen=True, slots=True)
class ReportContract:
    kind: ReportKind
    chinese_name: str
    delivery: ReportDelivery
    required_sections: tuple[str, ...]
    purpose: str

    def __post_init__(self) -> None:
        if not self.chinese_name.strip() or not self.purpose.strip():
            raise ValueError("report contract labels must not be empty")
        if not self.required_sections or any(
            not section.strip() for section in self.required_sections
        ):
            raise ValueError("report contract must define non-empty sections")
        if len(set(self.required_sections)) != len(self.required_sections):
            raise ValueError("report contract sections must be unique")


REPORT_CONTRACTS: dict[ReportKind, ReportContract] = {
    ReportKind.INTRADAY_ALERT: ReportContract(
        kind=ReportKind.INTRADAY_ALERT,
        chinese_name="盘中交易告警",
        delivery=ReportDelivery.SHORT_TEXT,
        required_sections=("发生了什么", "执行结果", "关键价格", "证据时点"),
        purpose="在数秒内说明信号、门禁、委托或数据故障，不承载长篇研究。",
    ),
    ReportKind.EXECUTION_RECEIPT: ReportContract(
        kind=ReportKind.EXECUTION_RECEIPT,
        chinese_name="成交与账户回执",
        delivery=ReportDelivery.SHORT_TEXT_OR_MARKDOWN,
        required_sections=("成交事实", "费用与资金", "持仓变化", "后续保护计划"),
        purpose="确认 PAPER 或实盘观察账本发生的不可变变化。",
    ),
    ReportKind.POSITION_REVIEW: ReportContract(
        kind=ReportKind.POSITION_REVIEW,
        chinese_name="持仓持续复核",
        delivery=ReportDelivery.MARKDOWN_FILE,
        required_sections=("当前结论", "保护计划", "证据与反证", "下一复核条件"),
        purpose="展示止盈、止损、时间门和深度复核版本，不生成券商委托。",
    ),
    ReportKind.DAILY_REVIEW: ReportContract(
        kind=ReportKind.DAILY_REVIEW,
        chinese_name="盘后日报与次日知识基线",
        delivery=ReportDelivery.SHORT_TEXT_AND_MARKDOWN,
        required_sections=("执行摘要", "市场复盘", "操作复盘", "持仓深研", "次日基线"),
        purpose="在交易日结束后形成可复用、可追溯的下一交易日输入。",
    ),
    ReportKind.INSTRUMENT_RESEARCH: ReportContract(
        kind=ReportKind.INSTRUMENT_RESEARCH,
        chinese_name="标的深度研究",
        delivery=ReportDelivery.SHORT_TEXT_OR_MARKDOWN,
        required_sections=("结论", "技术结构", "基本面与宏观", "对抗观点", "失效条件"),
        purpose="用冻结证据和明确反证条件解释单一标的，不直接授权交易。",
    ),
    ReportKind.SYSTEM_HEALTH: ReportContract(
        kind=ReportKind.SYSTEM_HEALTH,
        chinese_name="系统健康与数据质量",
        delivery=ReportDelivery.SHORT_TEXT_OR_MARKDOWN,
        required_sections=("总体状态", "数据源", "模型与通知", "缺口与恢复动作"),
        purpose="汇总数据、LLM、NapCat 和持久化边界的健康状态。",
    ),
}

_GENERAL_LABELS = {
    "ABSTAIN": "证据不足，暂不形成方向结论",
    "ACTIVE": "持续关注",
    "ADVERSARIAL": "结构化对抗分析器",
    "AFTERNOON": "下午交易时段",
    "ALLOW": "复核通过",
    "AVAILABLE": "可用",
    "BASELINE": "原单分析器",
    "BOUND": "已绑定",
    "BOOTSTRAP": "启动准备阶段",
    "CANCELLED": "已取消",
    "COMPLETE": "完整完成",
    "COMPLETED": "已完成",
    "CLOSE_RESEARCH_FAILED": "收盘深度研究未完成",
    "CLOSING": "收盘阶段",
    "DEGRADED": "降级可用",
    "DISABLED": "未启用",
    "ENTER": "满足研究入场条件",
    "ENTER_CANDIDATE": "技术面入场候选",
    "FAILED": "失败",
    "FIVE_MINUTES": "5分钟",
    "FROZEN": "已冻结",
    "FULL_SESSION": "完整交易时段",
    "HEALTHY": "正常",
    "IN_PROGRESS": "进行中",
    "LUNCH": "午间休市阶段",
    "MISSED": "已错过",
    "MORNING": "上午交易时段",
    "NOT_APPLICABLE": "不适用",
    "NOT_RECORDED": "未记录",
    "NOT_STARTED": "尚未开始",
    "ONE_MINUTE": "1分钟",
    "OPEN_AUCTION": "开盘阶段",
    "PARTIAL": "部分完成",
    "PARTIAL_SESSION": "部分交易时段",
    "PENDING": "等待中",
    "POST_CLOSE": "盘后阶段",
    "PUBLISH": "形成有证据支持的宏观观点",
    "PREOPEN": "盘前阶段",
    "PAPER_READ_ONLY": "只读 PAPER 复盘",
    "LOCAL_ARTIFACT_ONLY": "仅生成本地报告文件",
    "REDUCE": "减仓/退出风险信号",
    "REJECT": "复核拒绝",
    "RECOVERED": "已恢复",
    "RESTORED": "已从持久记录恢复",
    "SCHEDULED": "已调度",
    "SENT": "已发送",
    "SHORT_1_TO_5_DAYS": "短线（1至5个交易日）",
    "SKIPPED": "已跳过",
    "SSE_MAIN": "沪市主板",
    "SZSE_MAIN": "深市主板",
    "SWING_1_TO_8_WEEKS": "波段（1至8周）",
    "CHINEXT": "创业板",
    "STAR": "科创板",
    "BSE": "北交所",
    "THIRTY_MINUTES": "30分钟",
    "TERMINAL": "终止阶段",
    "FIFTEEN_MINUTES": "15分钟",
    "UNKNOWN": "状态未知",
    "VETO": "否决入场",
    "WATCH": "继续观察",
}


def report_contract(kind: ReportKind) -> ReportContract:
    return REPORT_CONTRACTS[ReportKind(kind)]


def humanize_internal_code(code: str | None) -> str:
    """把稳定码翻译成人话，同时保留未知码的精确审计定位。"""

    if code is None or not code.strip():
        return "无"
    normalized = code.strip().upper()
    direct = _GENERAL_LABELS.get(normalized)
    if direct is not None:
        return direct
    # PAPER 日报维护当前最完整的执行原因目录；延迟导入可避免报告模块循环依赖。
    try:
        from gribuki_trade.reporting.paper_day.paper_day_summary import (  # noqa: PLC0415
            _STABLE_REASON_EXPLANATIONS,
        )
    except ImportError:  # pragma: no cover - 安装包完整性不变量
        return f"尚未分类的状态（审计码：{normalized}）"
    return _STABLE_REASON_EXPLANATIONS.get(
        normalized,
        f"尚未分类的状态（审计码：{normalized}）",
    )


def humanize_codes(codes: tuple[str, ...]) -> str:
    if not codes:
        return "无"
    return "；".join(dict.fromkeys(humanize_internal_code(code) for code in codes))


def render_stable_markdown_report(
    kind: ReportKind,
    *,
    title: str,
    sections: Mapping[str, str],
) -> str:
    """按稳定顺序生成 Markdown 报告，缺节、空节或错序一律拒绝。"""

    contract = report_contract(kind)
    normalized_title = title.strip()
    if not normalized_title:
        raise ValueError("report title must not be empty")
    if contract.delivery is ReportDelivery.SHORT_TEXT:
        raise ValueError("short-text report kinds do not use Markdown templates")
    normalized_sections = {
        str(name).strip(): str(content).strip() for name, content in sections.items()
    }
    if any(not name or not content for name, content in normalized_sections.items()):
        raise ValueError("report section names and content must not be empty")
    missing = tuple(
        name for name in contract.required_sections if name not in normalized_sections
    )
    if missing:
        raise ValueError(f"missing required report sections: {', '.join(missing)}")

    optional = tuple(
        sorted(name for name in normalized_sections if name not in contract.required_sections)
    )
    ordered = (*contract.required_sections, *optional)
    lines = [
        f"# {normalized_title}",
        "",
        f"> 报告类型：{contract.chinese_name}",
        f"> 用途：{contract.purpose}",
    ]
    for name in ordered:
        lines.extend(("", f"## {name}", "", normalized_sections[name]))
    rendered = "\n".join(lines).rstrip() + "\n"
    validate_markdown_report_contract(kind, rendered)
    return rendered


def validate_markdown_report_contract(kind: ReportKind, markdown: str) -> None:
    """校验定制报告也完整遵守对应契约，而不强迫所有报告使用同一排版。

    部分生产报告包含大量专用表格，直接交给通用 renderer 会丢失可读性。它们可继续
    自定义排版，但必须调用本函数，证明所有必需二级章节按契约顺序出现且内容非空。
    """

    contract = report_contract(kind)
    if contract.delivery is ReportDelivery.SHORT_TEXT:
        raise ValueError("short-text report kinds do not use Markdown contracts")
    if not isinstance(markdown, str) or not markdown.strip().startswith("# "):
        raise ValueError("Markdown report must start with a level-one title")

    positions: list[int] = []
    for section in contract.required_sections:
        marker = f"\n## {section}\n"
        position = markdown.find(marker)
        if position < 0:
            raise ValueError(f"missing required report section: {section}")
        positions.append(position)
        content_start = position + len(marker)
        next_heading = markdown.find("\n## ", content_start)
        content_end = len(markdown) if next_heading < 0 else next_heading
        if not markdown[content_start:content_end].strip():
            raise ValueError(f"report section must not be empty: {section}")
    if positions != sorted(positions):
        raise ValueError("required report sections are out of contract order")


def render_stable_text_report(
    kind: ReportKind,
    *,
    title: str,
    sections: Mapping[str, str],
) -> str:
    """生成适合 QQ 的稳定短文本，同时保留与 Markdown 相同的语义骨架。

    ``〔章节〕`` 是刻意选择的纯文本边界：QQ 中容易扫描，导出 Markdown 时也能
    无歧义地提升为二级标题。仅允许契约声明可通过短文本交付的报告类型使用。
    """

    contract = report_contract(kind)
    if contract.delivery is ReportDelivery.MARKDOWN_FILE:
        raise ValueError("Markdown-only report kinds do not use text templates")
    normalized_title = title.strip().removeprefix("【").removesuffix("】").strip()
    if not normalized_title:
        raise ValueError("report title must not be empty")
    normalized_sections = {
        str(name).strip(): str(content).strip() for name, content in sections.items()
    }
    if any(not name or not content for name, content in normalized_sections.items()):
        raise ValueError("report section names and content must not be empty")
    missing = tuple(
        name for name in contract.required_sections if name not in normalized_sections
    )
    if missing:
        raise ValueError(f"missing required report sections: {', '.join(missing)}")

    optional = tuple(
        sorted(name for name in normalized_sections if name not in contract.required_sections)
    )
    ordered = (*contract.required_sections, *optional)
    lines = [f"【{normalized_title}】", f"报告类型：{contract.chinese_name}"]
    for name in ordered:
        lines.extend(("", f"〔{name}〕", normalized_sections[name]))
    rendered = "\n".join(lines).rstrip()
    validate_text_report_contract(kind, rendered)
    return rendered


def validate_text_report_contract(kind: ReportKind, text: str) -> None:
    """验证一条可交付短文本完整实现对应报告契约。"""

    contract = report_contract(kind)
    if contract.delivery is ReportDelivery.MARKDOWN_FILE:
        raise ValueError("Markdown-only report kinds do not use text contracts")
    if not isinstance(text, str) or not text.strip().startswith("【"):
        raise ValueError("text report must start with a Chinese bracket title")
    if f"\n报告类型：{contract.chinese_name}\n" not in f"\n{text.strip()}\n":
        raise ValueError("text report has no matching report-type declaration")

    positions: list[int] = []
    for section in contract.required_sections:
        marker = f"\n〔{section}〕\n"
        position = text.find(marker)
        if position < 0:
            raise ValueError(f"missing required report section: {section}")
        positions.append(position)
        content_start = position + len(marker)
        next_heading = text.find("\n〔", content_start)
        content_end = len(text) if next_heading < 0 else next_heading
        if not text[content_start:content_end].strip():
            raise ValueError(f"report section must not be empty: {section}")
    if positions != sorted(positions):
        raise ValueError("required report sections are out of contract order")


def stable_markdown_template(kind: ReportKind, *, title: str) -> str:
    """返回可供界面预览和报告实现者检查的可见骨架。"""

    contract = report_contract(kind)
    return render_stable_markdown_report(
        kind,
        title=title,
        sections={
            name: "<!-- 请填入经验证、与本节相关的内容 -->"
            for name in contract.required_sections
        },
    )
