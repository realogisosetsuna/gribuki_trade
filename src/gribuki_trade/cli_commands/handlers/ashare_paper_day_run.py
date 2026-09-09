"""A 股 PAPER-day 运行资源与会话执行处理器。

该模块保留运行器、账本和 outbox 的原有事务边界；CLI 依赖通过 facade 延迟解析。"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from gribuki_trade.services.ashare_intraday_llm import IntradayLLMConfig
    from gribuki_trade.services.ashare_paper_day import (
        PaperDayDeepExitAssessmentProvider,
        PaperDayIntradayLLMPlanFactory,
    )
    from gribuki_trade.services.macro_research import MacroResearchService

class _LazyCliFacade:
    def __getattr__(self, name: str) -> Any:
        from gribuki_trade import cli
        return getattr(cli, name)

_cli: Any = _LazyCliFacade()

__all__ = ["_run_ashare_paper_day_owned", "_paper_day_resume_config_compatible"]

async def _run_ashare_paper_day_owned(
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
    recover_after_abort: bool,
    maximum_positions: int | None,
    risk_policy_change_confirmation: str | None,
    intraday_research: MacroResearchService | None,
    intraday_llm_config: IntradayLLMConfig | None,
    intraday_events_path: Path,
    deep_exit_assessment_provider: PaperDayDeepExitAssessmentProvider | None,
    report_artifact_recovery_action: str | None = None,
    report_artifact_recovery_confirmation: str | None = None,
    report_artifact_provider_identifier: str | None = None,
) -> dict[str, object]:
    """构造并管理阻塞式日内循环的每项可变依赖。"""

    from gribuki_trade.adapters.akshare import AKShareMarketDataAdapter
    from gribuki_trade.adapters.ashare_preopen_screening import (
        AKSharePreopenScreeningAdapter,
    )
    from gribuki_trade.adapters.ashare_surveillance import (
        AKShareAShareSurveillanceAdapter,
    )
    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.domain.paper_day import (
        PaperDayRunManifest,
        paper_day_target_hash,
    )
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.runtime import (
        PaperAccountChainError,
        SystemAwakeGuard,
        prepare_paper_day_ledger,
    )
    from gribuki_trade.services.ashare_intraday_llm_plans import (
        FrozenPITIntradayLLMPlanFactory,
        ReplayGuardedIntradayLLMCoordinator,
        build_preopen_context,
        load_frozen_pit_event_snapshot,
        replay_frozen_pit_event_snapshot,
    )
    from gribuki_trade.services.ashare_intraday_paper import IntradayPaperRiskConfig
    from gribuki_trade.services.ashare_paper import ASharePaperTradingService
    from gribuki_trade.services.ashare_paper_day import (
        ASharePaperDayConfig,
        ASharePaperDayRunner,
        PaperDayAbortRecoveryRequiredError,
        PaperDayEventPublisher,
        PaperDayRiskPolicyChangeError,
        intraday_llm_evidence_manifest_document,
        intraday_llm_manifest_document,
    )
    from gribuki_trade.services.ashare_preopen_screening import (
        ASharePreopenScreeningService,
    )
    from gribuki_trade.services.ashare_surveillance import (
        AShareIntradaySurveillanceService,
    )
    from gribuki_trade.services.notification_dispatch import (
        NotificationDispatchService,
    )
    from gribuki_trade.storage.outbox import SQLiteOutbox
    from gribuki_trade.storage.paper_day import (
        PaperDayStoreLeaseError,
        SQLitePaperDayStore,
    )
    from gribuki_trade.storage.paper_ledger import SQLitePaperLedger

    target_kind = NotificationTargetKind(target_kind_value)
    session_root.mkdir(parents=True, exist_ok=True)
    report_dir = (session_root / "reports").resolve()
    report_dir.mkdir(parents=True, exist_ok=True)
    status_path = session_root / "status.json"
    llm_enabled = intraday_llm_config is not None
    if llm_enabled != (intraday_research is not None):
        raise ValueError("intraday LLM runtime dependencies must be supplied together")
    config = ASharePaperDayConfig(
        initial_cash=initial_cash,
        intraday_llm_enabled=llm_enabled,
        intraday_llm_required_for_buy=llm_enabled,
        exit_plan_trading_sessions=future_trading_sessions,
        exit_plan_calendar_sha256=calendar_revision_sha256,
    )
    risk_config = IntradayPaperRiskConfig(
        initial_equity=initial_cash,
        maximum_positions=maximum_positions,
    )
    base_manifest_config = {
        **config.audit_document(),
        "calendar_provider": "BaoStock",
        "calendar_verified": True,
        "latest_completed_session": latest_completed_session.isoformat(),
        "notification_channel": "onebot",
        "notification_preflight_policy": "GET_STATUS_GOOD_AND_ONLINE",
        "notification_preflight_required": True,
        "notification_target_kind": target_kind.value,
        "intraday_risk_policy": risk_config.audit_document(),
    }
    target_hash = paper_day_target_hash(
        channel="onebot",
        target_kind=target_kind.value,
        target_id=target_id,
    )
    owner_id = f"paper-day-{uuid4().hex}"
    day_store_path = session_root / "journal.sqlite3"
    outbox_path = session_root / "outbox.sqlite3"
    ledger_path = session_root / "ledger.sqlite3"
    ledger_lineage: dict[str, object] | None = None

    notifier_config = OneBotConfig(
        access_token=access_token,
        base_url=base_url,
        private_target_ids=(
            frozenset({target_id}) if target_kind is NotificationTargetKind.PRIVATE else frozenset()
        ),
        group_target_ids=(
            frozenset({target_id}) if target_kind is NotificationTargetKind.GROUP else frozenset()
        ),
        artifact_root=report_dir,
    )

    # 将主机的正常显示策略与所有外部凭据都留在领域代码之外。进程级防护会在每条退出路径
    # 恢复 Windows 睡眠策略，并且绝不请求保持显示器唤醒。
    with SystemAwakeGuard():
        async with OneBotNotifier(notifier_config) as notifier:
            try:
                notification_status = await notifier.get_status()
            except asyncio.CancelledError:
                raise
            except Exception:
                raise _cli._PaperDayCLIError("PAPER_DAY_NOTIFICATION_PREFLIGHT_FAILED") from None
            if not isinstance(notification_status, Mapping) or not (
                notification_status.get("good") is True
                and notification_status.get("online") is True
            ):
                raise _cli._PaperDayCLIError("PAPER_DAY_NOTIFICATION_PREFLIGHT_FAILED")
            try:
                prepared_ledger = prepare_paper_day_ledger(
                    session_root,
                    session_date=session_date,
                    account_id=account_id,
                )
            except PaperAccountChainError:
                raise _cli._PaperDayCLIError("PAPER_ACCOUNT_CONTINUITY_INVALID") from None
            ledger_path = prepared_ledger.ledger_path
            ledger_lineage = prepared_ledger.audit_document()
            base_manifest_config["paper_account_continuity"] = ledger_lineage
            with SQLitePaperDayStore(day_store_path) as day_store:
                scoped = tuple(
                    item
                    for item in day_store.list_runs()
                    if item.session_date == session_date and item.account_id == account_id
                )
                if len(scoped) > 1:
                    raise _cli._PaperDayCLIError("PAPER_DAY_RUN_SCOPE_CONFLICT")
                retained = scoped[0] if scoped else None
                if retained is not None and retained.target_hash != target_hash:
                    raise _cli._PaperDayCLIError("NOTIFICATION_TARGET_CONFLICT")

                if llm_enabled:
                    assert intraday_research is not None
                    assert intraday_llm_config is not None
                    retained_evidence_binding = (
                        None
                        if retained is None
                        else retained.config.get("intraday_llm_evidence_snapshot")
                    )
                    if retained is None:
                        evidence = load_frozen_pit_event_snapshot(
                            intraday_events_path,
                            as_of=started_at,
                        )
                        retained_factory_audit = None
                    elif isinstance(retained_evidence_binding, dict):
                        nested_audit = retained_evidence_binding.get("audit_document")
                        retained_factory_audit = (
                            cast(dict[str, object], nested_audit)
                            if isinstance(nested_audit, dict)
                            else cast(
                                dict[str, object],
                                retained_evidence_binding,
                            )
                        )
                        evidence = replay_frozen_pit_event_snapshot(
                            intraday_events_path,
                            retained_audit=retained_factory_audit,
                            fallback_as_of=started_at,
                        )
                    else:
                        raise _cli._PaperDayCLIError("PAPER_DAY_RUNTIME_POLICY_CONFLICT")
                    intraday_llm = ReplayGuardedIntradayLLMCoordinator(
                        intraday_research,
                        config=intraday_llm_config,
                        journal_restore_allowed=evidence.available,
                    )
                    intraday_plan_factory = FrozenPITIntradayLLMPlanFactory(
                        intraday_research,
                        evidence,
                        retained_audit=retained_factory_audit,
                    )
                    manifest_evidence_audit = intraday_llm_evidence_manifest_document(
                        intraday_plan_factory
                    )
                else:
                    path_sha256 = hashlib.sha256(
                        str(intraday_events_path.resolve()).encode("utf-8")
                    ).hexdigest()
                    retained_disabled_evidence = (
                        None
                        if retained is None
                        else retained.config.get("intraday_llm_evidence_snapshot")
                    )
                    manifest_evidence_audit = (
                        dict(retained_disabled_evidence)
                        if isinstance(retained_disabled_evidence, dict)
                        else {
                            "as_of": started_at.astimezone(UTC),
                            "database_path_sha256": path_sha256,
                            "event_count": 0,
                            "failure_code": "INTRADAY_LLM_OPERATOR_DISABLED",
                            "operator_opt_out": True,
                            "source": "OPERATOR_CLI",
                            "status": "DISABLED",
                        }
                    )
                    evidence = None
                    intraday_llm = None
                    intraday_plan_factory = None

                manifest_config = {
                    **base_manifest_config,
                    "intraday_llm_policy": intraday_llm_manifest_document(intraday_llm),
                    "intraday_llm_evidence_snapshot": manifest_evidence_audit,
                }
                proposed = PaperDayRunManifest.create(
                    session_date=session_date,
                    account_id=account_id,
                    config=manifest_config,
                    created_at=started_at,
                    target_hash=target_hash,
                    initial_cash=initial_cash,
                )
                created_new_run = retained is None
                if retained is None:
                    day_store.create_run(proposed)
                    manifest = proposed
                else:
                    if (
                        retained.initial_cash != initial_cash
                        or not _cli._paper_day_resume_config_compatible(
                            retained=retained.config,
                            proposed=proposed.config,
                            intraday_llm_enabled=llm_enabled,
                        )
                    ):
                        raise _cli._PaperDayCLIError("PAPER_DAY_RUNTIME_POLICY_CONFLICT")
                    manifest = retained

                llm_preopen_context = None
                retained_event_types = {
                    item.event_type for item in day_store.events(manifest.run_id)
                }
                preopen_already_decided = bool(
                    retained_event_types
                    & {
                        "LLM_PREOPEN_CONTEXT_FROZEN",
                        "LLM_PREOPEN_CONTEXT_FAILED",
                    }
                )
                if (
                    intraday_llm is not None
                    and intraday_plan_factory is not None
                    and intraday_research is not None
                    and created_new_run
                    and not preopen_already_decided
                    and evidence is not None
                    and evidence.available
                ):
                    market_open_at = datetime.combine(
                        session_date,
                        config.market_open,
                        tzinfo=ZoneInfo("Asia/Shanghai"),
                    ).astimezone(UTC)
                    if started_at.astimezone(UTC) < market_open_at:
                        try:
                            preopen_plan = intraday_plan_factory.prepare_preopen(
                                session_date=session_date
                            )
                            if preopen_plan.eligible_for_analysis:
                                preopen_run = await intraday_research.execute(preopen_plan)
                                known_at = max(
                                    datetime.now(UTC),
                                    started_at.astimezone(UTC),
                                )
                                if known_at < market_open_at:
                                    valid_until = datetime.combine(
                                        session_date,
                                        config.finalization_time,
                                        tzinfo=ZoneInfo("Asia/Shanghai"),
                                    ).astimezone(UTC)
                                    llm_preopen_context = build_preopen_context(
                                        preopen_plan,
                                        preopen_run,
                                        session_date=session_date,
                                        known_at=known_at,
                                        valid_until=valid_until,
                                    )
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            # 供应商与证据细节保持脱敏。运行器会记录缺失上下文，并且只关闭
                            # 新买入，绝不关闭监控或卖出。
                            llm_preopen_context = None
                with (
                    SQLiteOutbox(outbox_path) as outbox,
                    SQLitePaperLedger(ledger_path) as ledger,
                ):
                    paper = ASharePaperTradingService(ledger)
                    dispatcher = NotificationDispatchService(
                        outbox,
                        {notifier.channel: notifier},
                        target_kind=target_kind,
                        target_id=target_id,
                    )
                    publisher = PaperDayEventPublisher(
                        manifest=manifest,
                        store=day_store,
                        outbox=outbox,
                        dispatcher=dispatcher,
                        target_kind=target_kind,
                        target_id=target_id,
                        owner_id=owner_id,
                        clock=lambda: datetime.now(UTC),
                        status_path=status_path,
                    )
                    preopen = ASharePreopenScreeningService(
                        AKSharePreopenScreeningAdapter(
                            latest_completed_session,
                            timeout_seconds=35.0,
                            history_timeout_seconds=18.0,
                            history_concurrency=4,
                        )
                    )
                    surveillance = AShareIntradaySurveillanceService(
                        AKShareAShareSurveillanceAdapter(
                            timeout_seconds=35.0,
                        )
                    )
                    market_data = AKShareMarketDataAdapter(
                        timeout_seconds=10.0,
                        max_attempts=1,
                        intraday_stale_after_seconds=180.0,
                    )
                    runner = ASharePaperDayRunner(
                        manifest=manifest,
                        latest_completed_session=latest_completed_session,
                        owner_id=owner_id,
                        store=day_store,
                        publisher=publisher,
                        preopen_screening=preopen,
                        surveillance=surveillance,
                        market_data=market_data,
                        paper=paper,
                        outbox=outbox,
                        report_dir=report_dir,
                        config=config,
                        risk_config=risk_config,
                        intraday_llm=intraday_llm,
                        intraday_llm_plan_factory=cast(
                            "PaperDayIntradayLLMPlanFactory | None",
                            intraday_plan_factory,
                        ),
                        llm_preopen_context=llm_preopen_context,
                        deep_exit_assessment_provider=deep_exit_assessment_provider,
                        artifact_notifier=notifier,
                        artifact_target_kind=target_kind,
                        artifact_target_id=target_id,
                        report_artifact_recovery_action=(
                            report_artifact_recovery_action
                        ),
                        report_artifact_recovery_confirmation=(
                            report_artifact_recovery_confirmation
                        ),
                        report_artifact_provider_identifier=(
                            report_artifact_provider_identifier
                        ),
                        preopen_recovery_path=(session_root / "preopen-recovery-seed.json"),
                        recover_after_abort=recover_after_abort,
                        risk_policy_change_confirmation=(risk_policy_change_confirmation),
                    )
                    try:
                        result = await runner.run()
                    except PaperDayAbortRecoveryRequiredError:
                        raise _cli._PaperDayCLIError(
                            "DAY_ABORTED_OPERATOR_RECOVERY_REQUIRED"
                        ) from None
                    except PaperDayRiskPolicyChangeError as error:
                        raise _cli._PaperDayCLIError(error.code) from None
                    except PaperDayStoreLeaseError:
                        raise _cli._PaperDayCLIError("PAPER_DAY_WRITER_LEASE_UNAVAILABLE") from None
                    finally:
                        if intraday_llm is not None:
                            await intraday_llm.close()
    delivery_projection = _cli._paper_day_delivery_projection(result)
    return {
        **delivery_projection,
        "action": "run",
        "run_id": result.run_id,
        "session_date": result.session_date.isoformat(),
        "completed": result.completed,
        "event_count": result.event_count,
        "account": _cli._paper_snapshot_json(result.final_snapshot),
        "report_path": str(result.report_path),
        "runtime_dir": str(session_root),
        "execution_mode": "PAPER_ONLY_NO_BROKER",
        "system_awake": "PROCESS_SCOPED_SYSTEM_ONLY",
        "intraday_llm": {
            "enabled": llm_enabled,
            "required_for_buy": llm_enabled,
            "runtime_evidence_failure_code": (None if evidence is None else evidence.failure_code),
            "runtime_evidence_status": ("DISABLED" if evidence is None else evidence.status),
        },
        "paper_account_continuity": ledger_lineage,
    }


def _paper_day_resume_config_compatible(
    *,
    retained: Mapping[str, object],
    proposed: Mapping[str, object],
    intraday_llm_enabled: bool,
) -> bool:
    """比较不可变运行时策略，同时委托风险迁移。"""

    from gribuki_trade.services.ashare_paper_day import (
        intraday_llm_manifest_compatible,
    )

    old = dict(retained)
    new = dict(proposed)
    old.pop("intraday_risk_policy", None)
    new.pop("intraday_risk_policy", None)
    old_policy = old.pop("intraday_llm_policy", None)
    new_policy = new.pop("intraday_llm_policy", None)
    old_evidence = old.pop("intraday_llm_evidence_snapshot", None)
    new_evidence = new.pop("intraday_llm_evidence_snapshot", None)
    old_continuity = old.pop("paper_account_continuity", None)
    new_continuity = new.pop("paper_account_continuity", None)
    if old_continuity is None:
        # 引入跨交易日血缘前创建的运行已经拥有日期本地账本。它们只能通过由同一不可变
        # 数据库合成的显式旧版来源恢复。
        if not (
            isinstance(new_continuity, dict)
            and new_continuity.get("origin")
            in {
                "LEGACY_SESSION_LOCAL",
                "LEGACY_EMPTY_SESSION_LEDGER",
                "NEW_ACCOUNT",
            }
        ):
            return False
    elif old_continuity != new_continuity:
        return False
    if intraday_llm_enabled:
        if (
            not isinstance(new_policy, Mapping)
            or not intraday_llm_manifest_compatible(old_policy, new_policy)
            or old_evidence != new_evidence
        ):
            return False
    else:
        if old_policy is not None and (
            not isinstance(new_policy, Mapping)
            or not intraday_llm_manifest_compatible(old_policy, new_policy)
        ):
            return False
        if old_evidence is not None and old_evidence != new_evidence:
            return False
        # LLM 门控出现前创建的运行隐式将两个值都设为 false。显式选择退出时可以恢复这些
        # 不可变日志；运行器会在恢复检查后追加可审计的运维禁用事件，且不重写旧版清单。
        for key in (
            "intraday_llm_enabled",
            "intraday_llm_required_for_buy",
        ):
            if key not in old and new.get(key) is False:
                new.pop(key, None)
    return old == new
