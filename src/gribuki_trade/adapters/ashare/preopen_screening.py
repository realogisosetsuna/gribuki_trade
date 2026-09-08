"""以已验证上一交易日为锚点的 A 股盘前筛选输入。

常规 :mod:`ashare_screening` 适配器刻意只在当前交易日收盘后接受实时全市场
端点。本模块不会放宽这一不变量，而是为已经通过独立交易日历验证最近完成
交易日的操作员提供单独且明确降级的路径。

下一交易日开盘前，公开报价页面通常显示前收盘价和上一交易日累计成交额，
但不会发布权威载荷时间戳或交易所交易日身份。因此本适配器：

* 使用注入且已验证的完成交易日标记批次；
* 将 ``available_at`` 记录为实际采集完成时间，绝不回填为前收盘时刻；
* 只返回沪深证券并拒绝北京证券代码；
* 始终把股票池和因子批次都标记为 ``DEGRADED``；
* 将历史因子查询严格限制在已验证交易日内。

返回的批次适用于盘前研究候选列表，但不是历史重放来源，也不得被表述为在
上一交易日收盘时已经观测到的数据。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, time
from functools import partial
from typing import Any
from zoneinfo import ZoneInfo

from gribuki_trade.adapters.ashare.screening import (
    _UNIVERSE_SOURCES,
    AKSHARE_HISTORY_SOURCE_ID,
    AKShareAShareScreeningAdapter,
    AKShareScreeningCoverageError,
    AKShareScreeningPointInTimeError,
    AKShareScreeningSourcesExhaustedError,
    _call_async,
    _factor_batch_source_id,
    _factor_revision,
    _import_akshare,
    _parse_universe_rows,
    _provider_records,
    _universe_revision,
)
from gribuki_trade.ports.ashare_screening import (
    AShareFactorSnapshot,
    AShareUniverseSnapshot,
    ScreeningHistoryPolicy,
    ScreeningSourceQuality,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
PREOPEN_CUTOFF = time(9, 25)
PREOPEN_UNIVERSE_SOURCE_SUFFIX = "preopen-verified-previous-session"
PREOPEN_FACTOR_SOURCE_ID = (
    f"{AKSHARE_HISTORY_SOURCE_ID}/{PREOPEN_UNIVERSE_SOURCE_SUFFIX}"
)


class AKSharePreopenScreeningAdapter:
    """``AsyncAShareScreeningData`` 的降级盘前实现。

    ``verified_latest_completed_session`` 是信任边界。调用方应从独立验证的
    交易所日历取得它（本项目使用 BaoStock 的闭区间交易日历），而不是根据
    工作日推断。

    该实现刻意组合常规筛选适配器，以获取上市元数据并逐代码计算因子。其盘后
    实时边界保持不变；只有本适配器拥有盘前重标记策略及其强制警告。
    """

    def __init__(
        self,
        verified_latest_completed_session: date,
        client: Any | None = None,
        *,
        timeout_seconds: float = 35.0,
        history_timeout_seconds: float = 18.0,
        minimum_universe_count: int = 4500,
        minimum_history_sessions: int = 201,
        max_factor_symbols: int = 300,
        history_concurrency: int = 4,
        history_calendar_days: int = 430,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not isinstance(verified_latest_completed_session, date):
            raise TypeError("verified_latest_completed_session must be a date")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        if minimum_universe_count < 1:
            raise ValueError("minimum_universe_count must be positive")
        if max_factor_symbols < 1:
            raise ValueError("max_factor_symbols must be positive")
        if history_concurrency < 1 or history_concurrency > 16:
            raise ValueError("history_concurrency must be between 1 and 16")

        self._verified_session = verified_latest_completed_session
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._minimum_universe_count = minimum_universe_count
        self._max_factor_symbols = max_factor_symbols
        self._history_concurrency = history_concurrency
        self._now = now
        # 复用经过加固的元数据/历史解析和公司行动防护，刻意不调用其公共盘后
        # 方法。
        self._underlying = AKShareAShareScreeningAdapter(
            client,
            timeout_seconds=timeout_seconds,
            history_timeout_seconds=history_timeout_seconds,
            minimum_universe_count=minimum_universe_count,
            minimum_history_sessions=minimum_history_sessions,
            max_factor_symbols=max_factor_symbols,
            history_concurrency=history_concurrency,
            history_calendar_days=history_calendar_days,
            now=now,
        )

    @property
    def verified_latest_completed_session(self) -> date:
        """每个返回批次所锚定的、经日历验证的交易日。"""

        return self._verified_session

    async def fetch_universe_snapshot(
        self,
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareUniverseSnapshot:
        """观测一份当前网页快照，并进行保守标记。

        ``available_at`` 是本次获取的完成时间，不是 ``known_at``，也不是前一
        收盘时刻。因此，应用额外服务门禁的调用方必须把决策时间设在本方法
        返回之后。
        """

        started_at = self._validate_request(as_of=as_of, known_at=known_at)
        client = self._client or _import_akshare()
        metadata, metadata_warnings = await self._underlying._fetch_listing_metadata(  # noqa: SLF001
            client,
            self._verified_session,
        )
        failures: list[str] = []
        for spec in _UNIVERSE_SOURCES:
            try:
                rows = await _call_async(
                    partial(_provider_records, client, spec.operation),
                    timeout_seconds=self._timeout_seconds,
                    operation=spec.operation,
                )
                parsed = _parse_universe_rows(
                    rows,
                    spec=spec,
                    as_of=self._verified_session,
                    metadata=metadata,
                    minimum_count=self._minimum_universe_count,
                )
                records = tuple(
                    item for item in parsed if item.symbol.endswith((".SH", ".SZ"))
                )
                if len(records) < self._minimum_universe_count:
                    raise AKShareScreeningCoverageError(
                        "pre-open SH/SZ universe is below the configured minimum"
                    )
                completed_at = self._validated_completion(
                    known_at,
                    not_before=started_at,
                )
                base_revision = _universe_revision(
                    spec.source_id,
                    self._verified_session,
                    records,
                )
                warnings = (
                    "PREOPEN_CURRENT_WEB_SNAPSHOT_RELABELED_TO_VERIFIED_PREVIOUS_SESSION",
                    "PREOPEN_OBSERVATION_ONLY_NOT_AN_EXCHANGE_CLOSE_SNAPSHOT",
                    "AVAILABLE_AT_IS_ACTUAL_PREOPEN_COLLECTION_TIME_NOT_PRIOR_CLOSE",
                    "SH_SZ_ONLY_BSE_EXCLUDED",
                    "CURRENT_NAME_AND_STATUS_FIELDS_MAY_INCLUDE_OVERNIGHT_CHANGES",
                    (
                        "VERIFIED_LATEST_COMPLETED_SESSION:"
                        f"{self._verified_session.isoformat()}"
                    ),
                    *metadata_warnings,
                    *(f"PRIOR_SOURCE_FAILED:{item}" for item in failures),
                )
                return AShareUniverseSnapshot(
                    as_of=self._verified_session,
                    available_at=completed_at,
                    observed_at=completed_at,
                    source_id=(
                        f"{spec.source_id}/{PREOPEN_UNIVERSE_SOURCE_SUFFIX}"
                    ),
                    source_revision=_preopen_revision(
                        kind="universe",
                        source_id=spec.source_id,
                        verified_session=self._verified_session,
                        available_at=completed_at,
                        underlying_revision=base_revision,
                    ),
                    records=records,
                    quality=ScreeningSourceQuality.DEGRADED,
                    warnings=tuple(dict.fromkeys(warnings)),
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except AKShareScreeningPointInTimeError:
                raise
            except Exception as exc:
                failures.append(f"{spec.operation}:{type(exc).__name__}")
        raise AKShareScreeningSourcesExhaustedError(tuple(failures))

    async def fetch_factor_snapshot(
        self,
        symbols: Sequence[str],
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareFactorSnapshot:
        """严格截至已验证交易日计算不复权因子。"""

        started_at = self._validate_request(as_of=as_of, known_at=known_at)
        canonical = tuple(_canonical_sh_sz_symbol(item) for item in symbols)
        if not canonical:
            raise ValueError("at least one SH/SZ factor symbol is required")
        if len(canonical) != len(set(canonical)):
            raise ValueError("factor symbols must be unique")
        if len(canonical) > self._max_factor_symbols:
            raise ValueError(
                f"factor request exceeds adapter budget {self._max_factor_symbols}"
            )
        client = self._client or _import_akshare()
        semaphore = asyncio.Semaphore(self._history_concurrency)

        async def one(symbol: str) -> Any:
            async with semaphore:
        # _factor_record 会发送 end_date=as_of；在计算任何因子前，其解析器会
        # 拒绝晚于 as_of 的提供者记录。
                return await self._underlying._factor_record(  # noqa: SLF001
                    client,
                    symbol,
                    self._verified_session,
                )

        records = tuple(await asyncio.gather(*(one(symbol) for symbol in canonical)))
        completed_at = self._validated_completion(
            known_at,
            not_before=started_at,
        )
        history_source_id = _factor_batch_source_id(records)
        base_revision = _factor_revision(
            self._verified_session,
            records,
            source_id=history_source_id,
        )
        degraded_records = sum(bool(item.warnings) for item in records)
        sina_fallbacks = sum(
            any(
                warning.startswith("HISTORY_SOURCE_FALLBACK:AKShare/Sina ")
                for warning in item.warnings
            )
            for item in records
        )
        warnings = [
            "PREOPEN_FACTORS_STRICTLY_CUTOFF_AT_VERIFIED_PREVIOUS_SESSION",
            "PREOPEN_BATCH_ALWAYS_DEGRADED_PENDING_SESSION_OPEN_REVALIDATION",
            "UNADJUSTED_HISTORY_WITH_CORPORATE_ACTION_GUARD",
            "SH_SZ_ONLY_BSE_REJECTED",
            (
                "VERIFIED_LATEST_COMPLETED_SESSION:"
                f"{self._verified_session.isoformat()}"
            ),
            f"SYMBOL_FACTOR_DEGRADATIONS:{degraded_records}",
        ]
        if sina_fallbacks:
            warnings.extend(
                (
                    f"SINA_HISTORY_FALLBACK_SYMBOLS:{sina_fallbacks}/{len(records)}",
                    "SINA_FALLBACK_IS_UNADJUSTED_DAILY_HISTORY",
                    "SINA_PREVIOUS_CLOSE_DERIVED_FROM_ADJACENT_RAW_CLOSES",
                    "SINA_CORPORATE_ACTION_GUARD_HAS_NO_INDEPENDENT_REFERENCE_CLOSE",
                )
            )
        preopen_factor_source_id = (
            f"{history_source_id}/{PREOPEN_UNIVERSE_SOURCE_SUFFIX}"
        )
        return AShareFactorSnapshot(
            as_of=self._verified_session,
            available_at=completed_at,
            observed_at=completed_at,
            source_id=preopen_factor_source_id,
            source_revision=_preopen_revision(
                kind="factors",
                source_id=history_source_id,
                verified_session=self._verified_session,
                available_at=completed_at,
                underlying_revision=base_revision,
            ),
            feature_version="ashare-screening-raw-factors@1/preopen",
            history_policy=(
                ScreeningHistoryPolicy.UNADJUSTED_WITH_CORPORATE_ACTION_GUARD
            ),
            records=records,
            quality=ScreeningSourceQuality.DEGRADED,
            warnings=tuple(warnings),
        )

    def _validate_request(self, *, as_of: date, known_at: datetime) -> datetime:
        if known_at.tzinfo is None or known_at.utcoffset() is None:
            raise ValueError("known_at must be timezone-aware")
        if as_of != self._verified_session:
            raise AKShareScreeningPointInTimeError(
                "pre-open as_of must equal verified_latest_completed_session"
            )
        observed = _aware_now(self._now)
        local_known = known_at.astimezone(SHANGHAI)
        local_observed = observed.astimezone(SHANGHAI)
        if local_known.date() != local_observed.date():
            raise AKShareScreeningPointInTimeError(
                "pre-open live snapshot supports only the current Shanghai date"
            )
        if self._verified_session >= local_known.date():
            raise AKShareScreeningPointInTimeError(
                "verified session must precede the current pre-open date"
            )
        if local_known.timetz().replace(tzinfo=None) > PREOPEN_CUTOFF:
            raise AKShareScreeningPointInTimeError(
                "pre-open screening is unavailable after 09:25 Asia/Shanghai"
            )
        if local_observed.timetz().replace(tzinfo=None) > PREOPEN_CUTOFF:
            raise AKShareScreeningPointInTimeError(
                "collector is outside the pre-open window"
            )
        if known_at.astimezone(UTC) > observed:
            raise AKShareScreeningPointInTimeError("known_at is in the future")
        return observed

    def _validated_completion(
        self,
        known_at: datetime,
        *,
        not_before: datetime,
    ) -> datetime:
        completed = _aware_now(self._now)
        local_completed = completed.astimezone(SHANGHAI)
        local_known = known_at.astimezone(SHANGHAI)
        if local_completed.date() != local_known.date():
            raise AKShareScreeningPointInTimeError(
                "collector date changed during pre-open fetch"
            )
        if local_completed.timetz().replace(tzinfo=None) > PREOPEN_CUTOFF:
            raise AKShareScreeningPointInTimeError(
                "pre-open fetch completed after 09:25 Asia/Shanghai"
            )
        if completed < not_before:
            raise AKShareScreeningPointInTimeError(
                "collector clock moved backwards during pre-open fetch"
            )
        return completed


def _canonical_sh_sz_symbol(value: str) -> str:
    normalized = value.strip().upper()
    raw_code = normalized.split(".", maxsplit=1)[0]
    if raw_code.startswith(("4", "8", "92")):
        raise ValueError("pre-open factor screening supports SH/SZ only")
    if len(normalized) == 6 and normalized.isdigit():
        suffix = "SH" if normalized.startswith(("5", "6", "9")) else "SZ"
        normalized = f"{normalized}.{suffix}"
    if len(normalized) != 9 or normalized[6] != ".":
        raise ValueError("symbol must look like 600000.SH or 000001.SZ")
    code, exchange = normalized.split(".", maxsplit=1)
    if not code.isdigit() or exchange not in {"SH", "SZ"}:
        raise ValueError("pre-open factor screening supports SH/SZ only")
    expected = "SH" if code.startswith(("5", "6", "9")) else "SZ"
    if exchange != expected:
        raise ValueError("symbol exchange conflicts with its code family")
    return normalized


def _preopen_revision(
    *,
    kind: str,
    source_id: str,
    verified_session: date,
    available_at: datetime,
    underlying_revision: str,
) -> str:
    document: Mapping[str, object] = {
        "available_at": available_at.astimezone(UTC).isoformat(),
        "kind": kind,
        "policy": PREOPEN_UNIVERSE_SOURCE_SUFFIX,
        "source_id": source_id,
        "underlying_revision": underlying_revision,
        "verified_latest_completed_session": verified_session.isoformat(),
    }
    encoded = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _aware_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now must return a timezone-aware datetime")
    return value.astimezone(UTC)
