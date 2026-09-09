"""对抗宏观分析的渐进发布 feature flag adapter。"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING

from gribuki_trade.analysis.schemas import MacroAnalysis, MacroAnalysisRequest
from gribuki_trade.ports.llm_analyzer import (
    AnalyzerAuditIdentity,
    AuditableMacroAnalyzer,
    MacroAnalyzer,
)

if TYPE_CHECKING:
    from gribuki_trade.services.macro.adversarial_macro import AdversarialMacroAnalyzer

from gribuki_trade.services.macro.adversarial_macro_models import (
    AdversarialFeatureMode,
    AdversarialMacroRun,
    AdversarialShadowRecord,
    ShadowObserver,
)


class FeatureFlaggedAdversarialMacroAnalyzer:
    """用于渐进发布、默认关闭的 BASELINE/SHADOW/ENFORCE 入口。

    SHADOW 并行启动两个分支，但只等待 baseline；对抗任务通过完成回调异步
    送入 observer，因此慢 provider 不会增加 baseline 的返回延迟。
    """

    def __init__(
        self,
        baseline: MacroAnalyzer,
        adversarial: AdversarialMacroAnalyzer,
        *,
        mode: AdversarialFeatureMode = AdversarialFeatureMode.BASELINE,
        baseline_identity: AnalyzerAuditIdentity | None = None,
        shadow_observer: ShadowObserver | None = None,
    ) -> None:
        discovered = (
            baseline.audit_identity if isinstance(baseline, AuditableMacroAnalyzer) else None
        )
        if (
            baseline_identity is not None
            and discovered is not None
            and baseline_identity != discovered
        ):
            raise ValueError("explicit baseline identity does not match baseline analyzer")
        resolved_identity = baseline_identity or discovered
        if resolved_identity is None:
            raise ValueError("feature-flagged analysis requires an auditable baseline")
        self._baseline_identity = resolved_identity
        self._baseline = baseline
        self._adversarial = adversarial
        self._mode = mode
        self._shadow_observer = shadow_observer
        self._shadow_tasks: set[asyncio.Task[AdversarialMacroRun]] = set()

    @property
    def mode(self) -> AdversarialFeatureMode:
        return self._mode

    @property
    def audit_identity(self) -> AnalyzerAuditIdentity:
        if self._mode is AdversarialFeatureMode.ENFORCE:
            return self._adversarial.audit_identity
        return self._baseline_identity

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        if self._mode is AdversarialFeatureMode.BASELINE:
            return await self._baseline.analyze(request)
        if self._mode is AdversarialFeatureMode.ENFORCE:
            return await self._adversarial.analyze(request)

        shadow_task = asyncio.create_task(
            self._adversarial.analyze_case(request),
            name=f"adversarial-shadow:{request.analysis_id[-12:]}",
        )
        try:
            baseline_analysis = await self._baseline.analyze(request)
        except BaseException:
            shadow_task.cancel()
            await asyncio.gather(shadow_task, return_exceptions=True)
            raise
        # 给纯内存/零延迟 provider 几次调度机会，以便测试与本地观察立即完成；
        # 每次只让出事件循环，不等待任何实际 I/O 或计时器。
        for _ in range(4):
            if shadow_task.done():
                break
            await asyncio.sleep(0)
        if shadow_task.done():
            self._publish_shadow(shadow_task, baseline_analysis)
        else:
            self._shadow_tasks.add(shadow_task)
            shadow_task.add_done_callback(
                lambda task: self._publish_shadow(task, baseline_analysis)
            )
        return baseline_analysis

    def _publish_shadow(
        self,
        task: asyncio.Task[AdversarialMacroRun],
        baseline_analysis: MacroAnalysis,
    ) -> None:
        self._shadow_tasks.discard(task)
        if task.cancelled():
            return
        with suppress(Exception):
            shadow_run = task.result()
            if self._shadow_observer is not None:
                self._shadow_observer(
                    AdversarialShadowRecord(
                        analysis_id=baseline_analysis.analysis_id,
                        baseline_analysis=baseline_analysis,
                        adversarial_analysis=shadow_run.analysis,
                        adversarial_failure_code=shadow_run.failure_code,
                        baseline_identity_sha256=(self._baseline_identity.manifest_sha256),
                        adversarial_identity_sha256=(shadow_run.audit_identity.manifest_sha256),
                    )
                )
