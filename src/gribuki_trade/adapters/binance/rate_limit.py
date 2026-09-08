"""Binance 限流响应头的纯解析与累积。"""

from __future__ import annotations

from collections.abc import Mapping

from .models import BinanceRateLimitUsage


def parse_rate_limit_usage(
    headers: Mapping[str, str],
    *,
    prior: BinanceRateLimitUsage | None = None,
) -> BinanceRateLimitUsage:
    """解析响应头并保留本次未出现的历史计数器。

    Binance 不保证每个响应都返回所有计数器；缺失字段沿用上一观测值，
    ``Retry-After`` 则只表示当前响应的退避提示。
    """

    normalized = {str(key).lower(): str(value) for key, value in headers.items()}

    def read(name: str) -> int | None:
        raw = normalized.get(name.lower())
        if raw is None:
            return None
        try:
            value = int(raw)
        except ValueError:
            return None
        return value if value >= 0 else None

    observed = BinanceRateLimitUsage(
        used_weight_1m=read("x-mbx-used-weight-1m"),
        order_count_10s=read("x-mbx-order-count-10s"),
        order_count_1d=read("x-mbx-order-count-1d"),
        retry_after_seconds=read("retry-after"),
    )
    previous = prior or BinanceRateLimitUsage()
    return BinanceRateLimitUsage(
        used_weight_1m=(
            observed.used_weight_1m
            if observed.used_weight_1m is not None
            else previous.used_weight_1m
        ),
        order_count_10s=(
            observed.order_count_10s
            if observed.order_count_10s is not None
            else previous.order_count_10s
        ),
        order_count_1d=(
            observed.order_count_1d
            if observed.order_count_1d is not None
            else previous.order_count_1d
        ),
        retry_after_seconds=observed.retry_after_seconds,
    )


__all__ = ["parse_rate_limit_usage"]
