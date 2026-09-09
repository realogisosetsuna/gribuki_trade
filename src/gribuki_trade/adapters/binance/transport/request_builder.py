"""Binance REST 请求编码的纯函数。

网关负责传输、重试和响应处理；本模块只把结构化参数转换成 Binance 所需的
签名 URL、请求体和请求头，因此认证行为可以在无网络环境下独立测试。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlencode

from ..models import BinanceCredentials
from ..spot.parsing import parameter_text, sign_hmac_sha256


class _ParameterText(Protocol):
    def __call__(self, value: object) -> str: ...


class _Signer(Protocol):
    def __call__(self, secret_key: str, payload: str) -> str: ...


@dataclass(frozen=True, slots=True)
class EncodedBinanceRequest:
    """一条 Binance REST 请求的线路组成部分。

    单独返回 ``signature`` 是为了让网关在错误信息中进行脱敏。公开请求的签名为空，
    凭据本身不会通过可变请求对象的 repr 泄露。
    """

    url: str
    headers: dict[str, str]
    body: bytes | None
    signature: str = ""


def encode_request(
    *,
    method: str,
    base_url: str,
    path: str,
    params: tuple[tuple[str, object], ...] = (),
    credentials: BinanceCredentials | None = None,
    recv_window_ms: int | None = None,
    timestamp_ms: int | None = None,
    parameter_encoder: _ParameterText = parameter_text,
    signer: _Signer = sign_hmac_sha256,
) -> EncodedBinanceRequest:
    """编码公开或签名的 Binance 查询/表单参数。

    签名请求会先追加 ``recvWindow`` 和 ``timestamp``，再计算 HMAC-SHA256，符合
    Binance 的规范参数顺序。调用方提供时间戳和凭据，因此本函数没有时钟、环境或网络依赖。
    """

    normalized_method = method.upper()
    if normalized_method not in {"GET", "POST", "PUT", "DELETE"}:
        raise ValueError(f"unsupported Binance HTTP method: {method!r}")
    if credentials is not None and (recv_window_ms is None or timestamp_ms is None):
        raise ValueError("signed Binance requests require recv_window_ms and timestamp_ms")
    if credentials is None and (recv_window_ms is not None or timestamp_ms is not None):
        raise ValueError("recv_window_ms/timestamp_ms require credentials")

    encoded_params = list(params)
    if credentials is not None:
        assert recv_window_ms is not None and timestamp_ms is not None
        encoded_params.extend(
            (("recvWindow", recv_window_ms), ("timestamp", timestamp_ms))
        )
    payload = urlencode(
        [(key, parameter_encoder(value)) for key, value in encoded_params],
        encoding="utf-8",
        safe="",
    )
    headers = {"Accept": "application/json"}
    signature = ""
    if credentials is not None:
        signature = signer(credentials.secret_key, payload)
        payload = f"{payload}&signature={signature}" if payload else f"signature={signature}"
        headers["X-MBX-APIKEY"] = credentials.api_key

    url = f"{base_url}{path}"
    body: bytes | None = None
    if normalized_method in {"POST", "PUT", "DELETE"}:
        body = payload.encode("utf-8") if payload else None
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif payload:
        url = f"{url}?{payload}"
    return EncodedBinanceRequest(url=url, headers=headers, body=body, signature=signature)
