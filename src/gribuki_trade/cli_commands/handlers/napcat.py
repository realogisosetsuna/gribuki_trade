"""NapCat/OneBot 运维命令处理器。

本模块集中管理 NapCat 配置、健康检查、发件箱派发和报告附件发送。
CLI facade 通过延迟代理读取运行时依赖，保留历史 monkeypatch 入口并避免
处理器被单独导入时与 ``gribuki_trade.cli`` 形成循环依赖。
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


class _LazyCliFacade:
    """延迟解析 CLI facade，保持嵌入调用方和测试替换点兼容。"""

    def __getattr__(self, name: str) -> Any:
        from gribuki_trade import cli

        return getattr(cli, name)


_cli: Any = _LazyCliFacade()


def _post_close_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _napcat_configure(
    runtime_dir: str,
    onebot_port: int,
    webui_port: int,
    force: bool,
) -> dict[str, object]:
    from gribuki_trade.napcat_setup import configure_portable_napcat_runtime

    result = configure_portable_napcat_runtime(
        Path(runtime_dir).resolve(),
        _cli.KeyringSecretProvider(),
        onebot_port=onebot_port,
        webui_port=webui_port,
        force=force,
    )
    return {
        "configured": True,
        "onebot_base_url": f"http://127.0.0.1:{result.onebot_port}",
        "onebot_config": str(result.onebot_config_path),
        "runtime_dir": str(result.runtime_dir),
        "tokens_stored_in_keyring": True,
        "webui_config": str(result.webui_config_path),
        "webui_url": f"http://127.0.0.1:{result.webui_port}",
    }


async def _napcat_status(base_url: str) -> dict[str, object]:
    from gribuki_trade.adapters.notifiers import (
        OneBotConfig,
        OneBotError,
        OneBotNotifier,
    )

    try:
        token = _cli._required_local_secret(_cli.NAPCAT_ACCESS_TOKEN_SECRET)
    except (RuntimeError, _cli.SecretProviderError):
        return {
            "app_name": "unknown",
            "base_url": base_url,
            "error_code": "LOCAL_SECRET_UNAVAILABLE",
            "good": False,
            "next_action": (
                ".\\.venv\\Scripts\\python.exe -m gribuki_trade "
                "secret-set napcat.onebot.access_token"
            ),
            "online": False,
            "protocol_version": "unknown",
            "retryable": False,
        }
    try:
        async with OneBotNotifier(OneBotConfig(access_token=token, base_url=base_url)) as notifier:
            status = await notifier.get_status()
            version = await notifier.get_version_info()
    except OneBotError as error:
        if error.code == "transport_error":
            next_action = "在 GUI 的‘集成管理’页显式启动 NapCat 并完成 QQ 登录"
        elif error.code == "authentication_rejected":
            next_action = (
                ".\\.venv\\Scripts\\python.exe -m gribuki_trade "
                "secret-set napcat.onebot.access_token"
            )
        else:
            next_action = "在 GUI 的‘集成管理’页检查 NapCat 状态和本机 WebUI"
        return {
            "app_name": "unknown",
            "base_url": base_url,
            "error_code": error.code,
            "good": False,
            "next_action": next_action,
            "online": False,
            "protocol_version": "unknown",
            "retryable": error.retryable,
        }
    return {
        "app_name": str(version.get("app_name", "unknown")),
        "base_url": base_url,
        "error_code": None,
        "good": bool(status.get("good", False)),
        "next_action": None,
        "online": bool(status.get("online", False)),
        "protocol_version": str(version.get("protocol_version", "unknown")),
        "retryable": False,
    }


async def _napcat_dispatch(
    base_url: str,
    target_kind_value: str,
    target_id: str,
    outbox_path: str,
    cycles: int,
    poll_interval: float,
) -> dict[str, object]:
    """运行有限且仅出站的 OneBot 发件箱工作器。"""

    if cycles < 1:
        raise ValueError("cycles must be positive")
    if not 0 <= poll_interval < float("inf"):
        raise ValueError("poll_interval must be non-negative and finite")

    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.services import NotificationDispatchService
    from gribuki_trade.storage import SQLiteOutbox

    target_kind = NotificationTargetKind(target_kind_value)
    token = _cli._required_local_secret(_cli.NAPCAT_ACCESS_TOKEN_SECRET)
    allowlist = frozenset({target_id})
    config = OneBotConfig(
        access_token=token,
        base_url=base_url,
        private_target_ids=(
            allowlist if target_kind is NotificationTargetKind.PRIVATE else frozenset()
        ),
        group_target_ids=(
            allowlist if target_kind is NotificationTargetKind.GROUP else frozenset()
        ),
    )
    path = Path(outbox_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with SQLiteOutbox(path) as outbox:
        async with OneBotNotifier(config) as notifier:
            service = NotificationDispatchService(
                outbox,
                {notifier.channel: notifier},
                target_kind=target_kind,
                target_id=target_id,
            )
            statistics = await service.poll(
                max_cycles=cycles,
                poll_interval=poll_interval,
            )
    return {
        "base_url": base_url,
        "claimed": statistics.claimed,
        "cycles_completed": statistics.cycles_completed,
        "dead": statistics.dead,
        "expired": statistics.expired,
        "outbox_path": str(path),
        "poll_interval": poll_interval,
        "reached_cycle_limit": statistics.reached_cycle_limit,
        "retry_scheduled": statistics.retry_scheduled,
        "sent": statistics.sent,
        "stop_requested": statistics.stop_requested,
        "target_id": target_id,
        "target_kind": target_kind.value,
    }


async def _napcat_send_test(
    base_url: str,
    target_kind_value: str,
    target_id: str,
) -> dict[str, object]:
    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.ports.notifier import (
        NotificationTargetKind,
        OutboundNotification,
    )
    from gribuki_trade.reporting.contracts import (
        ReportKind,
        render_stable_text_report,
    )

    target_kind = NotificationTargetKind(target_kind_value)
    token = _cli._required_local_secret(_cli.NAPCAT_ACCESS_TOKEN_SECRET)
    allowlist = frozenset({target_id})
    config = OneBotConfig(
        access_token=token,
        base_url=base_url,
        private_target_ids=(
            allowlist if target_kind is NotificationTargetKind.PRIVATE else frozenset()
        ),
        group_target_ids=(
            allowlist if target_kind is NotificationTargetKind.GROUP else frozenset()
        ),
    )
    created_at = datetime.now(UTC)
    target_label = "私聊" if target_kind is NotificationTargetKind.PRIVATE else "群聊"
    health_text = render_stable_text_report(
        ReportKind.SYSTEM_HEALTH,
        title="Gribuki Trade｜NapCat 通知链路测试",
        sections={
            "总体状态": "正在执行一次显式、只读的通知链路测试；不包含交易指令。",
            "数据源": "本测试不读取行情、新闻或账户数据。",
            "模型与通知": (
                f"仅验证本地 NapCat/OneBot 到{target_label}目标的出站消息；不调用 LLM。"
            ),
            "缺口与恢复动作": (
                f"测试发起时点：{created_at.isoformat(timespec='seconds')}；"
                "若未收到，请在 GUI 集成管理页检查 NapCat 与 QQ 登录状态。"
            ),
        },
    )
    notification = OutboundNotification(
        idempotency_key=f"manual-health-test:{target_kind.value}:{int(created_at.timestamp())}",
        channel="onebot",
        target_kind=target_kind,
        target_id=target_id,
        text=health_text,
        created_at=created_at,
        expires_at=created_at + timedelta(minutes=5),
    )
    async with OneBotNotifier(config) as notifier:
        receipt = await notifier.send(notification)
    return {
        "channel": receipt.channel,
        "delivered": True,
        "provider_message_id": receipt.provider_message_id,
        "target_kind": target_kind.value,
    }


async def _napcat_send_artifact(
    base_url: str,
    target_kind_value: str,
    target_id: str,
    artifact_kind: str,
    report_kind_value: str,
    artifact_root: str,
    artifact: str,
    receipt_database: str,
) -> dict[str, object]:
    """经报告契约和持久交付边界显式发送一份 Markdown 报告。"""

    from gribuki_trade.adapters.notifiers import OneBotConfig, OneBotNotifier
    from gribuki_trade.ports.notifier import NotificationTargetKind
    from gribuki_trade.reporting.contracts import (
        ReportKind,
        validate_markdown_report_contract,
    )
    from gribuki_trade.storage.execution.report_artifact_outbox import (
        ReportArtifactOutboxError,
        ReportArtifactStatus,
        SQLiteReportArtifactOutbox,
    )

    target_kind = NotificationTargetKind(target_kind_value)
    if artifact_kind != "file":
        raise ValueError("napcat-send-artifact only accepts Markdown report files")
    report_kind = ReportKind(report_kind_value)
    root, resolved = _validated_markdown_report_artifact(artifact_root, artifact)
    report_bytes = resolved.read_bytes()
    report_text = report_bytes.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    validate_markdown_report_contract(report_kind, report_text)
    artifact_sha256 = hashlib.sha256(report_bytes).hexdigest()
    key_material = "\0".join(
        (
            "napcat-report-artifact@1",
            report_kind.value,
            target_kind.value,
            target_id,
            artifact_sha256,
        )
    )
    idempotency_key = "report-artifact:" + hashlib.sha256(key_material.encode("utf-8")).hexdigest()
    receipt_path = Path(receipt_database).expanduser().resolve()
    created_at = datetime.now(UTC)
    claimed = False

    try:
        with SQLiteReportArtifactOutbox(receipt_path) as outbox:
            delivery = outbox.enqueue(
                idempotency_key=idempotency_key,
                report_kind=report_kind.value,
                target_kind=target_kind,
                target_id=target_id,
                artifact_name=resolved.name,
                artifact_sha256=artifact_sha256,
                created_at=created_at,
            )
            if delivery.status is ReportArtifactStatus.SENT:
                return _report_artifact_delivery_result(
                    delivery.provider_identifier,
                    artifact_sha256=artifact_sha256,
                    artifact_kind=artifact_kind,
                    report_kind=report_kind.value,
                    target_kind=target_kind.value,
                    already_sent=True,
                )

            token = _cli._required_local_secret(_cli.NAPCAT_ACCESS_TOKEN_SECRET)
            allowlist = frozenset({target_id})
            config = OneBotConfig(
                access_token=token,
                base_url=base_url,
                private_target_ids=(
                    allowlist if target_kind is NotificationTargetKind.PRIVATE else frozenset()
                ),
                group_target_ids=(
                    allowlist if target_kind is NotificationTargetKind.GROUP else frozenset()
                ),
                artifact_root=root,
            )
            async with OneBotNotifier(config) as notifier:
                delivery = outbox.claim(idempotency_key, claimed_at=datetime.now(UTC))
                if delivery.status is ReportArtifactStatus.SENT:
                    return _report_artifact_delivery_result(
                        delivery.provider_identifier,
                        artifact_sha256=artifact_sha256,
                        artifact_kind=artifact_kind,
                        report_kind=report_kind.value,
                        target_kind=target_kind.value,
                        already_sent=True,
                    )
                claimed = True
                if _cli._post_close_sha256(resolved) != artifact_sha256:
                    outbox.mark_ambiguous(idempotency_key)
                    raise ReportArtifactOutboxError("REPORT_ARTIFACT_CHANGED")
                try:
                    file_receipt = (
                        await notifier.upload_private_file(target_id, str(resolved))
                        if target_kind is NotificationTargetKind.PRIVATE
                        else await notifier.upload_group_file(target_id, str(resolved))
                    )
                    provider_identifier = file_receipt.provider_file_id
                    if provider_identifier is None:
                        raise ReportArtifactOutboxError("REPORT_ARTIFACT_PROVIDER_RECEIPT_MISSING")
                    delivery = outbox.mark_sent(
                        idempotency_key,
                        provider_identifier=provider_identifier,
                        sent_at=datetime.now(UTC),
                    )
                except BaseException:
                    with suppress(Exception):
                        outbox.mark_ambiguous(idempotency_key)
                    raise
    except asyncio.CancelledError:
        raise
    except ReportArtifactOutboxError as error:
        return _report_artifact_failure_result(
            error.code,
            artifact_sha256=artifact_sha256,
            artifact_kind=artifact_kind,
            report_kind=report_kind.value,
            target_kind=target_kind.value,
        )
    except Exception:
        return _report_artifact_failure_result(
            (
                "REPORT_ARTIFACT_DELIVERY_AMBIGUOUS"
                if claimed
                else "REPORT_ARTIFACT_DELIVERY_FAILED"
            ),
            artifact_sha256=artifact_sha256,
            artifact_kind=artifact_kind,
            report_kind=report_kind.value,
            target_kind=target_kind.value,
        )

    return _report_artifact_delivery_result(
        delivery.provider_identifier,
        artifact_sha256=artifact_sha256,
        artifact_kind=artifact_kind,
        report_kind=report_kind.value,
        target_kind=target_kind.value,
        already_sent=False,
    )


def _validated_markdown_report_artifact(
    artifact_root: str,
    artifact: str,
) -> tuple[Path, Path]:
    root_input = Path(artifact_root).expanduser()
    if root_input.is_symlink():
        raise ValueError("artifact_root must not be a symbolic link")
    try:
        root = root_input.resolve(strict=True)
    except (OSError, RuntimeError):
        raise ValueError("artifact_root must be an existing directory") from None
    if not root.is_dir():
        raise ValueError("artifact_root must be an existing directory")
    supplied = Path(artifact).expanduser()
    candidate = supplied if supplied.is_absolute() else root / supplied
    lexical = Path(os.path.abspath(candidate))
    try:
        relative = lexical.relative_to(root)
    except ValueError:
        raise ValueError("artifact must remain inside artifact_root") from None
    current = root
    try:
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise ValueError("artifact path must not contain symbolic links")
        resolved = lexical.resolve(strict=True)
        resolved.relative_to(root)
    except ValueError:
        raise
    except (OSError, RuntimeError):
        raise ValueError("artifact must be an existing regular file") from None
    if not resolved.is_file() or resolved.suffix.casefold() not in {".md", ".markdown"}:
        raise ValueError("artifact must be an existing Markdown file")
    return root, resolved


def _report_artifact_delivery_result(
    provider_identifier: str | None,
    *,
    artifact_sha256: str,
    artifact_kind: str,
    report_kind: str,
    target_kind: str,
    already_sent: bool,
) -> dict[str, object]:
    return {
        "already_sent": already_sent,
        "artifact_kind": artifact_kind,
        "artifact_sha256": artifact_sha256,
        "channel": "onebot",
        "delivered": True,
        "ok": True,
        "provider_identifier": provider_identifier,
        "report_kind": report_kind,
        "target_kind": target_kind,
    }


def _report_artifact_failure_result(
    error_code: str,
    *,
    artifact_sha256: str,
    artifact_kind: str,
    report_kind: str,
    target_kind: str,
) -> dict[str, object]:
    return {
        "artifact_kind": artifact_kind,
        "artifact_sha256": artifact_sha256,
        "channel": "onebot",
        "delivered": False,
        "error_code": error_code,
        "ok": False,
        "report_kind": report_kind,
        "target_kind": target_kind,
    }
