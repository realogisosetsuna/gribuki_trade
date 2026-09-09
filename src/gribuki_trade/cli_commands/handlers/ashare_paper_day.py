"""A 股 PAPER-day 命令编排处理器。

运行时依赖通过 CLI facade 延迟解析，保留历史 monkeypatch 与嵌入入口。"""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from gribuki_trade.ports.market_data import AsyncTradingCalendar

class _LazyCliFacade:
    def __getattr__(self, name: str) -> Any:
        from gribuki_trade import cli
        return getattr(cli, name)

_cli: Any = _LazyCliFacade()

__all__ = [
    "_PaperDayCLIError",
    "_ashare_paper_day",
    "_load_ashare_paper_day_calendar_window",
    "_run_ashare_paper_day",
    "_verify_ashare_paper_day_calendar",
]

class _PaperDayCLIError(RuntimeError):
    """消息中不含供应商数据的稳定生产边界错误。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"A-share PAPER day unavailable ({code})")
async def _ashare_paper_day(
    action: str,
    runtime_dir: str,
    session_date: date | None,
    account_id: str,
    initial_cash: Decimal,
    target_kind_value: str | None,
    target_id: str | None,
    base_url: str,
    confirmation: str | None,
    recover_after_abort: bool = False,
    maximum_positions: int | None = None,
    risk_policy_change_confirmation: str | None = None,
    intraday_llm_enabled: bool = True,
    intraday_llm_review_top_n: int = 6,
    intraday_llm_review_ttl_minutes: int = 20,
    intraday_llm_max_calls: int | None = None,
    intraday_llm_events_db: str = "runtime/news/events.sqlite3",
    intraday_llm_provider: str | None = "deepseek",
    intraday_llm_model: str | None = None,
    report_artifact_recovery_action: str | None = None,
    report_artifact_recovery_confirmation: str | None = None,
    report_artifact_provider_identifier: str | None = None,
    *,
    now: datetime | None = None,
    calendar_provider: AsyncTradingCalendar | None = None,
) -> dict[str, object]:
    """运行或检查一个隔离的单进程 A 股 PAPER 会话。

    ``status`` 与 ``report`` 刻意只检查伴随文件；``summary`` 读取相同伴随文件，并以
    原子方式写入增强 Markdown 投影。这些操作都不会打开实时日志、发件箱或账本数据库，
    从而在受共享 WAL 重置竞态影响的 SQLite 运行时上保持其安全。
    """

    if action not in {"run", "status", "report", "summary"}:
        raise ValueError("unsupported A-share PAPER-day action")
    if not isinstance(recover_after_abort, bool):
        raise TypeError("recover_after_abort must be bool")
    if maximum_positions is not None and (
        isinstance(maximum_positions, bool)
        or not isinstance(maximum_positions, int)
        or maximum_positions <= 0
    ):
        raise ValueError("maximum_positions must be a positive integer or None")
    if risk_policy_change_confirmation not in {None, "PAPER_RISK_POLICY_CHANGE"}:
        raise ValueError("unsupported risk-policy change confirmation")
    artifact_recovery_values = (
        report_artifact_recovery_action,
        report_artifact_recovery_confirmation,
        report_artifact_provider_identifier,
    )
    if action != "run" and any(value is not None for value in artifact_recovery_values):
        raise ValueError("report-artifact recovery is only available for run")
    if report_artifact_recovery_action not in {
        None,
        "MARK_SENT_AFTER_PROVIDER_VERIFICATION",
        "RESEND_AFTER_PROVIDER_NON_RECEIPT_VERIFICATION",
    }:
        raise ValueError("unsupported report-artifact recovery action")
    if report_artifact_recovery_action is None:
        if any(value is not None for value in artifact_recovery_values[1:]):
            raise ValueError("report-artifact recovery options require an action")
    elif report_artifact_recovery_confirmation != "PAPER_REPORT_ARTIFACT_RECOVERY":
        raise ValueError("report-artifact recovery requires explicit confirmation")
    elif (
        report_artifact_recovery_action == "MARK_SENT_AFTER_PROVIDER_VERIFICATION"
        and not report_artifact_provider_identifier
    ):
        raise ValueError("mark-sent recovery requires a provider file ID")
    elif (
        report_artifact_recovery_action == "RESEND_AFTER_PROVIDER_NON_RECEIPT_VERIFICATION"
        and report_artifact_provider_identifier is not None
    ):
        raise ValueError("resend recovery does not accept a provider file ID")
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None or resolved_now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    local_today = resolved_now.astimezone(ZoneInfo("Asia/Shanghai")).date()
    resolved_session = session_date or local_today
    session_root = Path(runtime_dir).resolve() / resolved_session.isoformat()

    if action == "status":
        return _cli._ashare_paper_day_status(session_root, resolved_session)  # type: ignore[no-any-return]
    if action == "report":
        return _cli._ashare_paper_day_report(session_root, resolved_session)  # type: ignore[no-any-return]
    if action == "summary":
        return _cli._ashare_paper_day_summary(session_root, resolved_session)  # type: ignore[no-any-return]

    if not isinstance(intraday_llm_enabled, bool):
        raise TypeError("intraday_llm_enabled must be bool")
    for name, value in (
        ("intraday_llm_review_top_n", intraday_llm_review_top_n),
        ("intraday_llm_review_ttl_minutes", intraday_llm_review_ttl_minutes),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if intraday_llm_max_calls is not None and (
        isinstance(intraday_llm_max_calls, bool)
        or not isinstance(intraday_llm_max_calls, int)
        or intraday_llm_max_calls <= 0
    ):
        raise ValueError("intraday_llm_max_calls must be a positive integer or None")
    if not isinstance(intraday_llm_events_db, str) or not intraday_llm_events_db.strip():
        raise ValueError("intraday_llm_events_db must not be empty")
    intraday_llm_provider = (intraday_llm_provider or "deepseek").strip().casefold()
    if intraday_llm_provider not in {"deepseek", "openai"}:
        raise ValueError("intraday_llm_provider must be deepseek or openai")
    default_model = (
        _cli.DEFAULT_DEEPSEEK_MODEL if intraday_llm_provider == "deepseek" else "gpt-5.6"
    )
    intraday_llm_model = _cli.validate_runtime_model_id(intraday_llm_model or default_model)

    if confirmation != "PAPER_DAY":
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            action,
            resolved_session,
            session_root,
            "PAPER_DAY_CONFIRMATION_REQUIRED",
        )
    if target_kind_value is None or target_id is None or not target_id.strip():
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            action,
            resolved_session,
            session_root,
            "NOTIFICATION_TARGET_REQUIRED",
        )
    if resolved_session != local_today:
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            action,
            resolved_session,
            session_root,
            "SESSION_DATE_NOT_TODAY",
        )
    if not account_id.strip():
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            action,
            resolved_session,
            session_root,
            "ACCOUNT_ID_INVALID",
        )

    try:
        (
            latest_completed,
            future_trading_sessions,
            calendar_revision_sha256,
        ) = await _cli._load_ashare_paper_day_calendar_window(
            resolved_session,
            now=resolved_now,
            calendar_provider=calendar_provider,
        )
    except _PaperDayCLIError as error:
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            action,
            resolved_session,
            session_root,
            error.code,
        )

    try:
        token = _cli._required_local_secret(_cli.NAPCAT_ACCESS_TOKEN_SECRET)
    except (RuntimeError, _cli.SecretProviderError):
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            action,
            resolved_session,
            session_root,
            "NAPCAT_TOKEN_NOT_CONFIGURED",
        )

    llm_api_key: str | None = None
    if intraday_llm_enabled:
        try:
            llm_api_key = _cli._required_local_secret(
                _cli.DEEPSEEK_API_KEY_SECRET
                if intraday_llm_provider == "deepseek"
                else _cli.OPENAI_API_KEY_SECRET
            )
        except (RuntimeError, _cli.SecretProviderError):
            return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
                action,
                resolved_session,
                session_root,
                "INTRADAY_LLM_API_KEY_NOT_CONFIGURED",
            )

    try:
        return await _cli._run_ashare_paper_day(  # type: ignore[no-any-return]
            session_root=session_root,
            session_date=resolved_session,
            latest_completed_session=latest_completed,
            future_trading_sessions=future_trading_sessions,
            calendar_revision_sha256=calendar_revision_sha256,
            account_id=account_id,
            initial_cash=initial_cash,
            target_kind_value=target_kind_value,
            target_id=target_id.strip(),
            base_url=base_url,
            access_token=token,
            started_at=resolved_now,
            recover_after_abort=recover_after_abort,
            maximum_positions=maximum_positions,
            risk_policy_change_confirmation=risk_policy_change_confirmation,
            intraday_llm_enabled=intraday_llm_enabled,
            intraday_llm_review_top_n=intraday_llm_review_top_n,
            intraday_llm_review_ttl_minutes=(intraday_llm_review_ttl_minutes),
            intraday_llm_max_calls=intraday_llm_max_calls,
            intraday_llm_events_db=intraday_llm_events_db,
            intraday_llm_provider=intraday_llm_provider,
            intraday_llm_model=intraday_llm_model,
            intraday_llm_api_key=llm_api_key,
            report_artifact_recovery_action=report_artifact_recovery_action,
            report_artifact_recovery_confirmation=(
                report_artifact_recovery_confirmation
            ),
            report_artifact_provider_identifier=(
                report_artifact_provider_identifier
            ),
        )
    except asyncio.CancelledError:
        raise
    except _PaperDayCLIError as error:
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            action,
            resolved_session,
            session_root,
            error.code,
        )
    except Exception:
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            action,
            resolved_session,
            session_root,
            "PAPER_DAY_RUN_FAILED",
        )


async def _verify_ashare_paper_day_calendar(
    session_date: date,
    *,
    now: datetime,
    calendar_provider: AsyncTradingCalendar | None = None,
) -> date:
    """从完整真实日历中返回相邻的上一交易日。"""

    latest_completed, _, _ = await _cli._load_ashare_paper_day_calendar_window(
        session_date,
        now=now,
        calendar_provider=calendar_provider,
    )
    return cast(date, latest_completed)


async def _load_ashare_paper_day_calendar_window(
    session_date: date,
    *,
    now: datetime,
    calendar_provider: AsyncTradingCalendar | None = None,
) -> tuple[date, tuple[date, ...], str]:
    """冻结前一交易日及未来退出时间门所需的真实 A 股交易日历。"""

    from gribuki_trade.adapters.baostock import BaoStockDailyAdapter

    provider = calendar_provider or BaoStockDailyAdapter(
        max_attempts=1,
        timeout_seconds=20.0,
    )
    start = session_date - timedelta(days=45)
    end = session_date + timedelta(days=45)
    try:
        supplied = tuple(await provider.fetch_trade_calendar_async(start, end))
    except asyncio.CancelledError:
        raise
    except Exception:
        raise _PaperDayCLIError("TRADING_CALENDAR_UNAVAILABLE") from None
    expected_dates = tuple(
        start + timedelta(days=offset) for offset in range((end - start).days + 1)
    )
    supplied_dates = tuple(item.calendar_date for item in supplied)
    if supplied_dates != expected_dates or any(
        type(item.is_trading_day) is not bool for item in supplied
    ):
        raise _PaperDayCLIError("TRADING_CALENDAR_INVALID")
    trading_dates = tuple(item.calendar_date for item in supplied if item.is_trading_day)
    if session_date not in trading_dates:
        raise _PaperDayCLIError("TODAY_NOT_TRADING_SESSION")
    prior = tuple(item for item in trading_dates if item < session_date)
    if not prior:
        raise _PaperDayCLIError("PREVIOUS_TRADING_SESSION_MISSING")
    future = tuple(item for item in trading_dates if item > session_date)
    if len(future) < 5:
        raise _PaperDayCLIError("FUTURE_TRADING_SESSIONS_MISSING")
    local_now = now.astimezone(ZoneInfo("Asia/Shanghai"))
    if local_now.date() != session_date:
        raise _PaperDayCLIError("SESSION_DATE_NOT_TODAY")
    calendar_revision_sha256 = hashlib.sha256(
        json.dumps(
            [
                {
                    "date": item.calendar_date.isoformat(),
                    "is_trading_day": item.is_trading_day,
                }
                for item in supplied
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return prior[-1], future, calendar_revision_sha256


async def _run_ashare_paper_day(
    *,
    session_root: Path,
    session_date: date,
    latest_completed_session: date,
    future_trading_sessions: tuple[date, ...] = (),
    calendar_revision_sha256: str | None = None,
    account_id: str,
    initial_cash: Decimal,
    target_kind_value: str,
    target_id: str,
    base_url: str,
    access_token: str,
    started_at: datetime,
    recover_after_abort: bool = False,
    maximum_positions: int | None = None,
    risk_policy_change_confirmation: str | None = None,
    intraday_llm_enabled: bool = True,
    intraday_llm_review_top_n: int = 6,
    intraday_llm_review_ttl_minutes: int = 20,
    intraday_llm_max_calls: int | None = None,
    intraday_llm_events_db: str = "runtime/news/events.sqlite3",
    intraday_llm_provider: str = "deepseek",
    intraday_llm_model: str | None = None,
    intraday_llm_api_key: str | None = None,
    report_artifact_recovery_action: str | None = None,
    report_artifact_recovery_confirmation: str | None = None,
    report_artifact_provider_identifier: str | None = None,
) -> dict[str, object]:
    """冻结 LLM 证据、管理其共享客户端，再进入日内运行时。"""

    import httpx

    from gribuki_trade.adapters.llm import (
        DeepSeekChatMacroAnalyzer,
        OpenAIResponsesMacroAnalyzer,
    )
    from gribuki_trade.ports.llm_analyzer import MacroAnalyzer
    from gribuki_trade.security.config import SecretValue
    from gribuki_trade.services.ashare_intraday_llm import (
        IntradayLLMConfig,
    )
    from gribuki_trade.services.llm_production import (
        OwnedProductionDualTrackAnalyzer,
        PaperDayDualTrackDeepExitAssessmentProvider,
        ProductionLLMProfile,
        build_production_dual_track_analyzer,
        recommended_intraday_review_timeout,
    )
    from gribuki_trade.services.macro_research import MacroResearchService

    if not isinstance(intraday_llm_enabled, bool):
        raise TypeError("intraday_llm_enabled must be bool")
    for name, value in (
        ("intraday_llm_review_top_n", intraday_llm_review_top_n),
        ("intraday_llm_review_ttl_minutes", intraday_llm_review_ttl_minutes),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if intraday_llm_max_calls is not None and (
        isinstance(intraday_llm_max_calls, bool)
        or not isinstance(intraday_llm_max_calls, int)
        or intraday_llm_max_calls <= 0
    ):
        raise ValueError("intraday_llm_max_calls must be a positive integer or None")
    if not isinstance(intraday_llm_events_db, str) or not (intraday_llm_events_db.strip()):
        raise ValueError("intraday_llm_events_db must not be empty")
    intraday_llm_provider = intraday_llm_provider.strip().casefold()
    if intraday_llm_provider not in {"deepseek", "openai"}:
        raise ValueError("intraday_llm_provider must be deepseek or openai")
    intraday_llm_model = _cli.validate_runtime_model_id(
        intraday_llm_model or _cli.DEFAULT_DEEPSEEK_MODEL
    )
    if started_at.tzinfo is None or started_at.utcoffset() is None:
        raise ValueError("started_at must be timezone-aware")

    events_path = Path(intraday_llm_events_db).resolve()
    if not intraday_llm_enabled:
        return await _cli._run_ashare_paper_day_owned(  # type: ignore[no-any-return]
            session_root=session_root,
            session_date=session_date,
            latest_completed_session=latest_completed_session,
            future_trading_sessions=future_trading_sessions,
            calendar_revision_sha256=calendar_revision_sha256,
            account_id=account_id,
            initial_cash=initial_cash,
            target_kind_value=target_kind_value,
            target_id=target_id,
            base_url=base_url,
            access_token=access_token,
            started_at=started_at,
            recover_after_abort=recover_after_abort,
            maximum_positions=maximum_positions,
            risk_policy_change_confirmation=risk_policy_change_confirmation,
            intraday_research=None,
            intraday_llm_config=None,
            intraday_events_path=events_path,
            deep_exit_assessment_provider=None,
            report_artifact_recovery_action=report_artifact_recovery_action,
            report_artifact_recovery_confirmation=(
                report_artifact_recovery_confirmation
            ),
            report_artifact_provider_identifier=(
                report_artifact_provider_identifier
            ),
        )

    if not isinstance(intraday_llm_api_key, str) or not intraday_llm_api_key.strip():
        raise _PaperDayCLIError("INTRADAY_LLM_API_KEY_NOT_CONFIGURED")

    shared_client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=4, max_keepalive_connections=4),
        timeout=httpx.Timeout(8.0),
    )
    owned_dual: OwnedProductionDualTrackAnalyzer | None = None
    try:
        baseline: MacroAnalyzer
        if intraday_llm_provider == "deepseek":
            baseline = DeepSeekChatMacroAnalyzer.for_intraday(
                SecretValue(intraday_llm_api_key),
                model=intraday_llm_model,
                client=shared_client,
            )
        else:
            baseline = OpenAIResponsesMacroAnalyzer(
                SecretValue(intraday_llm_api_key),
                model=intraday_llm_model,
                timeout_seconds=15.0,
                client=shared_client,
            )
        owned_dual = build_production_dual_track_analyzer(
            baseline,
            audit_path=session_root / "llm-adversarial.sqlite3",
            profile=ProductionLLMProfile.INTRADAY,
            maximum_calls_per_session=intraday_llm_max_calls,
        )
        research = MacroResearchService(owned_dual)
        deep_exit_assessment_provider = PaperDayDualTrackDeepExitAssessmentProvider(owned_dual)
        llm_config = IntradayLLMConfig(
            enabled=True,
            required_for_buy=True,
            review_top_n=intraday_llm_review_top_n,
            review_ttl=timedelta(minutes=intraday_llm_review_ttl_minutes),
            per_review_timeout=recommended_intraday_review_timeout(),
            maximum_reviews_per_session=intraday_llm_max_calls,
        )
        return await _cli._run_ashare_paper_day_owned(  # type: ignore[no-any-return]
            session_root=session_root,
            session_date=session_date,
            latest_completed_session=latest_completed_session,
            future_trading_sessions=future_trading_sessions,
            calendar_revision_sha256=calendar_revision_sha256,
            account_id=account_id,
            initial_cash=initial_cash,
            target_kind_value=target_kind_value,
            target_id=target_id,
            base_url=base_url,
            access_token=access_token,
            started_at=started_at,
            recover_after_abort=recover_after_abort,
            maximum_positions=maximum_positions,
            risk_policy_change_confirmation=risk_policy_change_confirmation,
            intraday_research=research,
            intraday_llm_config=llm_config,
            intraday_events_path=events_path,
            deep_exit_assessment_provider=deep_exit_assessment_provider,
            report_artifact_recovery_action=report_artifact_recovery_action,
            report_artifact_recovery_confirmation=(
                report_artifact_recovery_confirmation
            ),
            report_artifact_provider_identifier=(
                report_artifact_provider_identifier
            ),
        )
    finally:
        # 所拥有运行时会在返回前关闭/取消其协调器；之后才可拆除会话级传输池。
        if owned_dual is not None:
            owned_dual.close()
        await shared_client.aclose()
