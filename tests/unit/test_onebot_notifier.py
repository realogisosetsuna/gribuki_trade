import asyncio
import json
from pathlib import Path

import httpx
import pytest

from gribuki_trade.adapters.notifiers import (
    OneBotConfig,
    OneBotError,
    OneBotNotifier,
    OneBotTargetNotAllowedError,
)
from gribuki_trade.ports.notifier import NotificationTargetKind, OutboundNotification


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com:3000",
        "http://192.168.1.20:3000",
        "http://127.0.0.1:3000/onebot",
        "ftp://127.0.0.1:3000",
        "http://user:password@127.0.0.1:3000",
    ],
)
def test_config_rejects_every_non_loopback_or_ambiguous_endpoint(url: str) -> None:
    with pytest.raises(ValueError):
        OneBotConfig(access_token="secret", base_url=url)


def test_config_normalizes_allowlists_and_redacts_token() -> None:
    config = OneBotConfig(
        access_token="very-secret",
        base_url="http://[::1]:3000",
        private_target_ids=frozenset({"00123"}),
        group_target_ids=frozenset({456}),
    )

    assert config.private_target_ids == frozenset({"123"})
    assert config.group_target_ids == frozenset({"456"})
    assert "very-secret" not in repr(config)


def test_config_redacts_artifact_root(tmp_path: Path) -> None:
    config = OneBotConfig(access_token="secret", artifact_root=tmp_path)

    assert config.artifact_root == tmp_path.resolve()
    assert str(tmp_path) not in repr(config)


def test_send_private_uses_token_allowlist_and_text_segment() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        document = json.loads(request.content)
        assert request.url.path == "/send_private_msg"
        assert request.headers["Authorization"] == "Bearer placeholder-token"
        assert document == {
            "user_id": 12345,
            "message": [
                {"type": "text", "data": {"text": "[CQ:image,file=do-not-run]"}}
            ],
        }
        return httpx.Response(
            200,
            json={"status": "ok", "retcode": 0, "data": {"message_id": 99}},
        )

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        notifier = OneBotNotifier(
            OneBotConfig(
                access_token="placeholder-token",
                private_target_ids=frozenset({"12345"}),
            ),
            client,
        )
        receipt = await notifier.send(
            OutboundNotification(
                idempotency_key="signal:1",
                channel="onebot",
                target_kind=NotificationTargetKind.PRIVATE,
                target_id="12345",
                text="[CQ:image,file=do-not-run]",
            )
        )
        assert receipt.provider_message_id == "99"
        assert receipt.channel == "onebot"
        await notifier.aclose()
        await client.aclose()

    asyncio.run(scenario())
    assert len(requests) == 1


def test_allowlist_rejection_happens_before_network_io() -> None:
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        notifier = OneBotNotifier(
            OneBotConfig(access_token="secret", group_target_ids=frozenset({"10"})),
            client,
        )
        with pytest.raises(OneBotTargetNotAllowedError) as caught:
            await notifier.send_group("11", "alert")
        assert caught.value.retryable is False
        assert caught.value.code == "target_not_allowed"
        await client.aclose()

    asyncio.run(scenario())
    assert calls == 0


def test_local_images_use_array_segments_for_private_and_group(tmp_path: Path) -> None:
    image_path = tmp_path / "report.png"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\nminimal-test-payload")
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer placeholder-token"
        document = json.loads(request.content)
        expected_id = "user_id" if request.url.path == "/send_private_msg" else "group_id"
        expected_value = 123 if expected_id == "user_id" else 456
        assert document == {
            expected_id: expected_value,
            "message": [
                {
                    "type": "image",
                    "data": {"file": str(image_path.resolve())},
                }
            ],
        }
        return httpx.Response(
            200,
            json={"status": "ok", "retcode": 0, "data": {"message_id": expected_value}},
        )

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        notifier = OneBotNotifier(
            OneBotConfig(
                access_token="placeholder-token",
                private_target_ids=frozenset({123}),
                group_target_ids=frozenset({456}),
                artifact_root=tmp_path,
            ),
            client,
        )
        private_receipt = await notifier.send_private_image(123, image_path)
        group_receipt = await notifier.send_group_image(456, "report.png")
        assert private_receipt.provider_message_id == "123"
        assert group_receipt.provider_message_id == "456"
        assert str(image_path) not in repr(private_receipt)
        await client.aclose()

    asyncio.run(scenario())
    assert [request.url.path for request in requests] == [
        "/send_private_msg",
        "/send_group_msg",
    ]


def test_local_files_use_napcat_upload_actions_for_private_and_group(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "收盘报告.md"
    report_path.write_text("# A股收盘报告\n\n可读内容", encoding="utf-8")
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer placeholder-token"
        document = json.loads(request.content)
        expected_id = "user_id" if request.url.path == "/upload_private_file" else "group_id"
        expected_value = "123" if expected_id == "user_id" else "456"
        assert document == {
            expected_id: expected_value,
            "file": str(report_path.resolve()),
            "name": report_path.name,
        }
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "retcode": 0,
                "data": {"file_id": f"file-{expected_value}"},
            },
        )

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        notifier = OneBotNotifier(
            OneBotConfig(
                access_token="placeholder-token",
                private_target_ids=frozenset({123}),
                group_target_ids=frozenset({456}),
                artifact_root=tmp_path,
            ),
            client,
        )
        private_receipt = await notifier.upload_private_file(123, report_path)
        group_receipt = await notifier.upload_group_file(456, report_path.name)
        assert private_receipt.provider_file_id == "file-123"
        assert group_receipt.provider_file_id == "file-456"
        assert str(report_path) not in repr(private_receipt)
        await client.aclose()

    asyncio.run(scenario())
    assert [request.url.path for request in requests] == [
        "/upload_private_file",
        "/upload_group_file",
    ]


def test_artifact_target_rejection_happens_before_filesystem_or_network(
    tmp_path: Path,
) -> None:
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        notifier = OneBotNotifier(
            OneBotConfig(
                access_token="secret",
                private_target_ids=frozenset({10}),
                artifact_root=tmp_path,
            ),
            client,
        )
        with pytest.raises(OneBotTargetNotAllowedError):
            await notifier.send_private_image(11, "does-not-exist.png")
        await client.aclose()

    asyncio.run(scenario())
    assert calls == 0


@pytest.mark.parametrize(
    "untrusted_path",
    [
        "https://example.com/report.png",
        "file:///tmp/report.png",
        "data:image/png;base64,AAAA",
        r"\\server\share\report.png",
    ],
)
def test_remote_and_unc_artifacts_are_rejected_before_io(
    tmp_path: Path,
    untrusted_path: str,
) -> None:
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        notifier = OneBotNotifier(
            OneBotConfig(
                access_token="secret",
                private_target_ids=frozenset({10}),
                artifact_root=tmp_path,
            ),
            client,
        )
        with pytest.raises(OneBotError) as caught:
            await notifier.send_private_image(10, untrusted_path)
        assert caught.value.code == "artifact_path_not_allowed"
        assert untrusted_path not in str(caught.value)
        await client.aclose()

    asyncio.run(scenario())
    assert calls == 0


def test_artifact_cannot_escape_configured_root(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    outside = tmp_path / "outside.md"
    outside.write_text("outside", encoding="utf-8")

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: None))
        notifier = OneBotNotifier(
            OneBotConfig(
                access_token="secret",
                private_target_ids=frozenset({10}),
                artifact_root=root,
            ),
            client,
        )
        with pytest.raises(OneBotError) as caught:
            await notifier.upload_private_file(10, "../outside.md")
        assert caught.value.code == "artifact_path_not_allowed"
        assert str(outside) not in str(caught.value)
        await client.aclose()

    asyncio.run(scenario())


def test_symbolic_link_artifact_is_rejected(tmp_path: Path) -> None:
    real = tmp_path / "real.md"
    real.write_text("report", encoding="utf-8")
    linked = tmp_path / "linked.md"
    try:
        linked.symlink_to(real)
    except OSError:
        pytest.skip("symbolic links are unavailable for this test account")

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: None))
        notifier = OneBotNotifier(
            OneBotConfig(
                access_token="secret",
                private_target_ids=frozenset({10}),
                artifact_root=tmp_path,
            ),
            client,
        )
        with pytest.raises(OneBotError) as caught:
            await notifier.upload_private_file(10, linked)
        assert caught.value.code == "artifact_path_not_allowed"
        assert str(linked) not in str(caught.value)
        await client.aclose()

    asyncio.run(scenario())


def test_artifact_size_type_and_mime_are_enforced(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized.md"
    oversized.write_text("12345", encoding="utf-8")
    forbidden = tmp_path / "program.exe"
    forbidden.write_text("x", encoding="utf-8")
    disguised = tmp_path / "disguised.png"
    disguised.write_text("not an image", encoding="utf-8")

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: None))
        notifier = OneBotNotifier(
            OneBotConfig(
                access_token="secret",
                private_target_ids=frozenset({10}),
                artifact_root=tmp_path,
                max_file_bytes=4,
            ),
            client,
        )
        with pytest.raises(OneBotError) as too_large:
            await notifier.upload_private_file(10, oversized)
        assert too_large.value.code == "artifact_too_large"

        with pytest.raises(OneBotError) as bad_type:
            await notifier.upload_private_file(10, forbidden)
        assert bad_type.value.code == "artifact_type_not_allowed"

        image_notifier = OneBotNotifier(
            OneBotConfig(
                access_token="secret",
                private_target_ids=frozenset({10}),
                artifact_root=tmp_path,
            ),
            client,
        )
        with pytest.raises(OneBotError) as illegal_type:
            await image_notifier.send_private_image(10, forbidden)
        assert illegal_type.value.code == "artifact_type_not_allowed"
        with pytest.raises(OneBotError) as mime_mismatch:
            await image_notifier.send_private_image(10, disguised)
        assert mime_mismatch.value.code == "artifact_mime_mismatch"
        await client.aclose()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status_code", "response_document", "code", "retryable"),
    [
        (503, None, "server_error", True),
        (
            200,
            {"status": "failed", "retcode": 100, "data": None},
            "remote_rejected",
            False,
        ),
    ],
)
def test_artifact_http_and_remote_failures_are_sanitized(
    tmp_path: Path,
    status_code: int,
    response_document: dict[str, object] | None,
    code: str,
    retryable: bool,
) -> None:
    report = tmp_path / "report.md"
    report.write_text("# report", encoding="utf-8")

    async def handler(_request: httpx.Request) -> httpx.Response:
        if response_document is None:
            return httpx.Response(status_code, text=f"sensitive:{report}")
        return httpx.Response(status_code, json=response_document)

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        notifier = OneBotNotifier(
            OneBotConfig(
                access_token="secret",
                private_target_ids=frozenset({10}),
                artifact_root=tmp_path,
            ),
            client,
        )
        with pytest.raises(OneBotError) as caught:
            await notifier.upload_private_file(10, report)
        assert caught.value.code == code
        assert caught.value.retryable is retryable
        assert str(report) not in str(caught.value)
        await client.aclose()

    asyncio.run(scenario())


def test_read_only_health_endpoints_are_supported() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/get_status":
            data = {"online": True, "good": True}
        else:
            assert request.url.path == "/get_version_info"
            data = {"app_name": "NapCat.OneBot", "protocol_version": "v11"}
        return httpx.Response(200, json={"status": "ok", "retcode": 0, "data": data})

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        notifier = OneBotNotifier(OneBotConfig(access_token="secret"), client)
        assert (await notifier.get_status())["online"] is True
        assert (await notifier.get_version_info())["protocol_version"] == "v11"
        await client.aclose()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status_code", "code", "retryable"),
    [
        (401, "authentication_rejected", False),
        (404, "endpoint_not_found", False),
        (429, "temporarily_unavailable", True),
        (503, "server_error", True),
    ],
)
def test_http_failures_have_stable_classifications(
    status_code: int, code: str, retryable: bool
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, text="must not appear in the error")

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        notifier = OneBotNotifier(
            OneBotConfig(access_token="secret", private_target_ids=frozenset({1})),
            client,
        )
        with pytest.raises(OneBotError) as caught:
            await notifier.send_private(1, "alert")
        assert caught.value.code == code
        assert caught.value.retryable is retryable
        assert "must not appear" not in str(caught.value)
        await client.aclose()

    asyncio.run(scenario())
