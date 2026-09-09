from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from gribuki_trade.services.research.research_watch import (
    ResearchSymbolStatus,
    ResearchWatchAlreadyRunningError,
    ResearchWatchService,
    ResearchWatchServiceError,
)

NOW = datetime(2026, 8, 13, 6, 0, tzinfo=UTC)


class AdvancingClock:
    def __init__(self) -> None:
        self.value = NOW

    def __call__(self) -> datetime:
        current = self.value
        self.value += timedelta(milliseconds=1)
        return current


def test_multiple_symbols_run_in_each_cycle_with_aggregate_statistics() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        sleeps: list[float] = []

        async def run_once(symbol: str) -> str:
            calls.append(symbol)
            return f"research:{symbol}:{calls.count(symbol)}"

        async def sleep(seconds: float) -> None:
            sleeps.append(seconds)

        service = ResearchWatchService(
            ["600000", "000001.sz"],
            run_once,
            clock=AdvancingClock(),
            sleep=sleep,
        )

        statistics = await service.run(max_cycles=2, interval_seconds=15)

        assert service.symbols == ("600000.SH", "000001.SZ")
        assert calls == ["600000.SH", "000001.SZ", "600000.SH", "000001.SZ"]
        assert sleeps == [15]
        assert statistics.cycles_completed == 2
        assert statistics.symbols_attempted == 4
        assert statistics.succeeded == 4
        assert statistics.failed == 0
        assert statistics.stop_requested is False
        assert statistics.reached_cycle_limit is True
        assert [cycle.cycle_number for cycle in statistics.cycles] == [1, 2]
        assert [cycle.succeeded for cycle in statistics.cycles] == [2, 2]
        assert statistics.cycles[0].symbol_runs[0].result == "research:600000.SH:1"
        assert service.is_running is False

    asyncio.run(scenario())


def test_symbol_failure_is_sanitized_and_does_not_block_peer_symbols() -> None:
    async def scenario() -> None:
        calls: list[str] = []

        async def run_once(symbol: str) -> str:
            calls.append(symbol)
            if symbol == "600000.SH":
                raise RuntimeError("token=top-secret https://provider.invalid/private?account=123")
            return f"ok:{symbol}"

        service = ResearchWatchService(
            ["600000.SH", "000001.SZ", "300750.SZ"],
            run_once,
            clock=AdvancingClock(),
        )

        statistics = await service.run(max_cycles=1, interval_seconds=0)

        assert calls == ["600000.SH", "000001.SZ", "300750.SZ"]
        assert statistics.succeeded == 2
        assert statistics.failed == 1
        failed = statistics.cycles[0].symbol_runs[0]
        assert failed.status is ResearchSymbolStatus.FAILED
        assert failed.result is None
        assert failed.error_code == "research_run_failed"
        rendered = repr(statistics)
        assert "top-secret" not in rendered
        assert "provider.invalid" not in rendered
        assert "account=123" not in rendered

    asyncio.run(scenario())


def test_stop_interrupts_injected_wait_without_starting_another_cycle() -> None:
    async def scenario() -> None:
        wait_started = asyncio.Event()
        sleep_cancelled = asyncio.Event()
        calls: list[str] = []

        async def run_once(symbol: str) -> str:
            calls.append(symbol)
            return symbol

        async def blocking_sleep(_seconds: float) -> None:
            wait_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                sleep_cancelled.set()
                raise

        service = ResearchWatchService(
            ["600000.SH", "000001.SZ"],
            run_once,
            clock=AdvancingClock(),
            sleep=blocking_sleep,
        )
        task = asyncio.create_task(service.run(max_cycles=10, interval_seconds=3_600))
        await asyncio.wait_for(wait_started.wait(), timeout=1)

        service.request_stop()
        statistics = await asyncio.wait_for(task, timeout=1)

        assert sleep_cancelled.is_set()
        assert calls == ["600000.SH", "000001.SZ"]
        assert statistics.cycles_completed == 1
        assert statistics.stop_requested is True
        assert statistics.reached_cycle_limit is False

    asyncio.run(scenario())


def test_stop_during_symbol_run_prevents_starting_later_symbols() -> None:
    async def scenario() -> None:
        holder: dict[str, ResearchWatchService[str]] = {}
        calls: list[str] = []

        async def run_once(symbol: str) -> str:
            calls.append(symbol)
            holder["service"].request_stop()
            return symbol

        service = ResearchWatchService(["600000.SH", "000001.SZ"], run_once, clock=AdvancingClock())
        holder["service"] = service

        statistics = await service.run(max_cycles=5)

        assert calls == ["600000.SH"]
        assert statistics.symbols_attempted == 1
        assert statistics.succeeded == 1
        assert statistics.cycles_completed == 1
        assert statistics.stop_requested is True

    asyncio.run(scenario())


def test_same_instance_rejects_concurrent_start_and_can_run_again_later() -> None:
    async def scenario() -> None:
        entered = asyncio.Event()
        release = asyncio.Event()

        async def run_once(symbol: str) -> str:
            entered.set()
            await release.wait()
            return symbol

        service = ResearchWatchService(["600000.SH"], run_once, clock=AdvancingClock())
        first = asyncio.create_task(service.run(max_cycles=1))
        await asyncio.wait_for(entered.wait(), timeout=1)
        assert service.is_running is True

        with pytest.raises(ResearchWatchAlreadyRunningError) as caught:
            await service.run(max_cycles=1)
        assert caught.value.__context__ is None

        release.set()
        first_result = await asyncio.wait_for(first, timeout=1)
        assert first_result.succeeded == 1
        assert service.is_running is False

        second_result = await service.run(max_cycles=1)
        assert second_result.succeeded == 1

    asyncio.run(scenario())


def test_stopped_instance_requires_explicit_reset_before_later_work() -> None:
    async def scenario() -> None:
        calls = 0

        async def run_once(_symbol: str) -> None:
            nonlocal calls
            calls += 1

        service = ResearchWatchService(["600000.SH"], run_once, clock=AdvancingClock())
        service.request_stop()

        stopped = await service.run(max_cycles=2)
        assert stopped.cycles_completed == 0
        assert stopped.stop_requested is True
        assert calls == 0

        service.reset_stop()
        resumed = await service.run(max_cycles=1)
        assert resumed.succeeded == 1
        assert calls == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("max_cycles", "interval_seconds"),
    [(0, 1.0), (-1, 1.0), (1, -0.1)],
)
def test_run_rejects_unbounded_or_invalid_configuration(
    max_cycles: int, interval_seconds: float
) -> None:
    async def run_once(symbol: str) -> str:
        return symbol

    service = ResearchWatchService(["600000.SH"], run_once)
    with pytest.raises(ValueError):
        asyncio.run(
            service.run(
                max_cycles=max_cycles,
                interval_seconds=interval_seconds,
            )
        )


def test_none_is_not_an_implicit_unbounded_cycle_count() -> None:
    async def run_once(symbol: str) -> str:
        return symbol

    service = ResearchWatchService(["600000.SH"], run_once)
    with pytest.raises(ValueError, match="max_cycles"):
        asyncio.run(service.run(max_cycles=None))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "symbols",
    [[], [""], ["not-a-symbol"], ["600000", "600000.SH"]],
)
def test_constructor_rejects_empty_invalid_or_duplicate_symbols(
    symbols: list[str],
) -> None:
    async def run_once(symbol: str) -> str:
        return symbol

    with pytest.raises(ValueError):
        ResearchWatchService(symbols, run_once)


def test_clock_and_wait_failures_expose_only_stable_codes() -> None:
    async def run_once(symbol: str) -> str:
        return symbol

    def exploding_clock() -> datetime:
        raise RuntimeError("clock token=clock-secret")

    clock_service = ResearchWatchService(["600000.SH"], run_once, clock=exploding_clock)
    with pytest.raises(ResearchWatchServiceError) as clock_error:
        asyncio.run(clock_service.run(max_cycles=1))
    assert clock_error.value.code == "clock_failed"
    assert "clock-secret" not in repr(clock_error.value)
    assert clock_error.value.__context__ is None

    async def exploding_sleep(_seconds: float) -> None:
        raise RuntimeError("sleep token=sleep-secret")

    wait_service = ResearchWatchService(
        ["600000.SH"],
        run_once,
        clock=AdvancingClock(),
        sleep=exploding_sleep,
    )
    with pytest.raises(ResearchWatchServiceError) as wait_error:
        asyncio.run(wait_service.run(max_cycles=2, interval_seconds=1))
    assert wait_error.value.code == "wait_failed"
    assert "sleep-secret" not in repr(wait_error.value)
    assert wait_error.value.__context__ is None
