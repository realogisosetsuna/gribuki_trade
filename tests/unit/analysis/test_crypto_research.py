from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.adapters.binance import BinanceKlineArchive, Kline
from gribuki_trade.backtest import CryptoFeeConfig
from gribuki_trade.services.research.crypto_research import (
    CryptoResearchError,
    CryptoResearchRequest,
    CryptoResearchService,
    binance_klines_to_crypto_bars,
    format_crypto_backtest_summary,
)
from gribuki_trade.strategy import CryptoTrendConfig


def kline(index: int, close: str) -> Kline:
    open_time_ms = index * 60_000
    price = Decimal(close)
    return Kline(
        open_time_ms=open_time_ms,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("100"),
        close_time_ms=open_time_ms + 59_999,
        quote_volume=price * Decimal("100"),
        trade_count=10,
        taker_buy_base_volume=Decimal("50"),
        taker_buy_quote_volume=price * Decimal("50"),
    )


def request(**changes: object) -> CryptoResearchRequest:
    values: dict[str, object] = {
        "environment": "TESTNET",
        "symbol": "btcusdt",
        "interval": "1m",
        "base_asset": "btc",
        "quote_asset": "usdt",
        "initial_quote_balance": Decimal("1000"),
        "trend": CryptoTrendConfig(
            fast_window=2,
            slow_window=3,
            minimum_history=3,
            target_position_fraction=Decimal("0.5"),
            quantity_step=Decimal("0.01"),
            minimum_order_quantity=Decimal("0.01"),
        ),
        "fees": CryptoFeeConfig(
            maker_rate=Decimal("0"),
            taker_rate=Decimal("0"),
        ),
        "market_slippage_rate": Decimal("0"),
        "max_bar_volume_fraction": Decimal("1"),
    }
    values.update(changes)
    return CryptoResearchRequest(**values)  # type: ignore[arg-type]


def archive_path(tmp_path: Path) -> Path:
    return tmp_path / "binance-history.sqlite3"


def test_kline_conversion_marks_bar_available_at_next_boundary() -> None:
    converted = binance_klines_to_crypto_bars("btcusdt", (kline(0, "10"),))

    assert converted[0].symbol == "BTCUSDT"
    assert converted[0].open_time == datetime(1970, 1, 1, tzinfo=UTC)
    assert converted[0].close_time == datetime(1970, 1, 1, 0, 1, tzinfo=UTC)
    assert converted[0].available_at == converted[0].close_time
    assert converted[0].complete


def test_service_loads_archive_runs_baseline_and_builds_exact_summary(
    tmp_path: Path,
) -> None:
    bars = tuple(kline(index, str(10 + index)) for index in range(5))
    with BinanceKlineArchive(archive_path(tmp_path)) as archive:
        archive.append("TESTNET", "BTCUSDT", "1m", bars)

        run = CryptoResearchService(archive).run(request())

    assert run.dataset.row_count == 5
    assert run.integrity.complete
    assert run.summary.bar_count == 5
    assert run.summary.dataset_sha256 == run.dataset.sha256
    assert run.summary.order_count >= 1
    assert run.summary.fill_count >= 1
    assert run.report.final_equity_quote == run.summary.final_equity_quote
    serialized = run.summary.as_dict()
    assert serialized["environment"] == "TESTNET"
    assert serialized["initial_equity_quote"] == "1000"
    rendered = format_crypto_backtest_summary(run.summary)
    assert run.dataset.sha256 in rendered
    assert "BTCUSDT 1m" in rendered


def test_service_rejects_a_gap_in_selected_replay(tmp_path: Path) -> None:
    with BinanceKlineArchive(archive_path(tmp_path)) as archive:
        archive.append(
            "TESTNET",
            "BTCUSDT",
            "1m",
            (kline(0, "10"), kline(2, "12")),
        )

        with pytest.raises(CryptoResearchError, match="not contiguous"):
            CryptoResearchService(archive).run(request())


def test_service_rejects_an_empty_selected_range(tmp_path: Path) -> None:
    with BinanceKlineArchive(archive_path(tmp_path)) as archive:
        archive.append("TESTNET", "BTCUSDT", "1m", (kline(0, "10"),))

        with pytest.raises(CryptoResearchError, match="contains no bars"):
            CryptoResearchService(archive).run(request(start_time_ms=60_000))


def test_request_normalizes_identity_and_requires_decimal_balances() -> None:
    selected = request()

    assert selected.symbol == "BTCUSDT"
    assert selected.base_asset == "BTC"
    assert selected.quote_asset == "USDT"
    with pytest.raises(TypeError, match="must be a Decimal"):
        request(initial_quote_balance=1000)

