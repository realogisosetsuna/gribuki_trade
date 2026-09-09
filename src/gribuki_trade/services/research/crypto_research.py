"""用于可审计 Binance 现货基线回测的应用服务。"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

from gribuki_trade.adapters.binance import (
    KLINE_INTERVALS,
    BinanceEnvironment,
    BinanceKlineArchive,
    Kline,
    KlineDataset,
    KlineIntegrityReport,
    normalize_symbol,
)
from gribuki_trade.backtest import (
    CryptoBacktestConfig,
    CryptoBacktestEngine,
    CryptoBacktestReport,
    CryptoBar,
    CryptoFeeConfig,
)
from gribuki_trade.strategy.crypto_trend import (
    CryptoTrendConfig,
    MovingAverageCryptoTrendStrategy,
)


class CryptoResearchError(RuntimeError):
    """归档数据无法按请求的假设回放。"""


@dataclass(frozen=True, slots=True)
class CryptoResearchRequest:
    """一次可复现回放的输入与声明假设。"""

    environment: BinanceEnvironment | str
    symbol: str
    interval: str
    base_asset: str
    quote_asset: str
    initial_quote_balance: Decimal
    initial_base_balance: Decimal = Decimal("0")
    trend: CryptoTrendConfig = field(default_factory=CryptoTrendConfig)
    fees: CryptoFeeConfig = field(default_factory=CryptoFeeConfig)
    market_slippage_rate: Decimal = Decimal("0.0005")
    max_bar_volume_fraction: Decimal = Decimal("0.01")
    start_time_ms: int | None = None
    end_time_ms: int | None = None
    require_contiguous_bars: bool = True

    def __post_init__(self) -> None:
        try:
            environment = (
                self.environment
                if isinstance(self.environment, BinanceEnvironment)
                else BinanceEnvironment(str(self.environment).upper())
            )
        except ValueError:
            raise ValueError(
                f"unsupported Binance environment: {self.environment!r}"
            ) from None
        symbol = normalize_symbol(self.symbol)
        base_asset = self.base_asset.strip().upper()
        quote_asset = self.quote_asset.strip().upper()
        if not base_asset or not quote_asset or base_asset == quote_asset:
            raise ValueError("base_asset and quote_asset must be distinct and non-empty")
        if self.interval not in KLINE_INTERVALS:
            raise ValueError(f"unsupported Binance kline interval: {self.interval!r}")
        for name, value in (
            ("initial_quote_balance", self.initial_quote_balance),
            ("initial_base_balance", self.initial_base_balance),
            ("market_slippage_rate", self.market_slippage_rate),
            ("max_bar_volume_fraction", self.max_bar_volume_fraction),
        ):
            _require_decimal(name, value)
        if self.initial_quote_balance < 0 or self.initial_base_balance < 0:
            raise ValueError("initial spot balances must be non-negative")
        if self.initial_quote_balance == 0 and self.initial_base_balance == 0:
            raise ValueError("at least one initial balance must be positive")
        if not isinstance(self.trend, CryptoTrendConfig):
            raise TypeError("trend must be CryptoTrendConfig")
        if not isinstance(self.fees, CryptoFeeConfig):
            raise TypeError("fees must be CryptoFeeConfig")
        if not Decimal("0") <= self.market_slippage_rate < Decimal("1"):
            raise ValueError("market_slippage_rate must be in [0, 1)")
        if not Decimal("0") < self.max_bar_volume_fraction <= Decimal("1"):
            raise ValueError("max_bar_volume_fraction must be in (0, 1]")
        _validate_optional_time(self.start_time_ms, "start_time_ms")
        _validate_optional_time(self.end_time_ms, "end_time_ms")
        if (
            self.start_time_ms is not None
            and self.end_time_ms is not None
            and self.end_time_ms < self.start_time_ms
        ):
            raise ValueError("end_time_ms must not precede start_time_ms")
        if not isinstance(self.require_contiguous_bars, bool):
            raise TypeError("require_contiguous_bars must be a bool")
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "base_asset", base_asset)
        object.__setattr__(self, "quote_asset", quote_asset)


@dataclass(frozen=True, slots=True)
class CryptoBacktestSummary:
    """完整回放报告中便于序列化的小型子集。"""

    environment: BinanceEnvironment
    symbol: str
    interval: str
    dataset_sha256: str
    bar_count: int
    first_open_time: datetime
    last_available_time: datetime
    initial_equity_quote: Decimal
    final_equity_quote: Decimal
    net_profit_quote: Decimal
    total_return: Decimal
    max_drawdown: Decimal
    total_fees_quote: Decimal
    turnover_quote: Decimal
    order_count: int
    fill_count: int
    pending_order_count: int

    def as_dict(self) -> dict[str, str | int]:
        """返回可直接输出为 JSON 的值。"""

        return {
            "environment": self.environment.value,
            "symbol": self.symbol,
            "interval": self.interval,
            "dataset_sha256": self.dataset_sha256,
            "bar_count": self.bar_count,
            "first_open_time": self.first_open_time.isoformat(),
            "last_available_time": self.last_available_time.isoformat(),
            "initial_equity_quote": format(self.initial_equity_quote, "f"),
            "final_equity_quote": format(self.final_equity_quote, "f"),
            "net_profit_quote": format(self.net_profit_quote, "f"),
            "total_return": format(self.total_return, "f"),
            "max_drawdown": format(self.max_drawdown, "f"),
            "total_fees_quote": format(self.total_fees_quote, "f"),
            "turnover_quote": format(self.turnover_quote, "f"),
            "order_count": self.order_count,
            "fill_count": self.fill_count,
            "pending_order_count": self.pending_order_count,
        }


@dataclass(frozen=True, slots=True)
class CryptoResearchRun:
    """一次回放的数据来源、转换后行情柱与完整结果。"""

    request: CryptoResearchRequest
    dataset: KlineDataset
    integrity: KlineIntegrityReport
    bars: tuple[CryptoBar, ...]
    report: CryptoBacktestReport
    summary: CryptoBacktestSummary


class CryptoResearchService:
    """加载不可变 Binance K 线并执行基线策略。"""

    def __init__(self, archive: BinanceKlineArchive) -> None:
        self._archive = archive

    def run(self, request: CryptoResearchRequest) -> CryptoResearchRun:
        environment = cast(BinanceEnvironment, request.environment)
        dataset = self._archive.dataset(
            environment,
            request.symbol,
            request.interval,
        )
        integrity = self._archive.integrity(
            environment,
            request.symbol,
            request.interval,
        )
        klines = self._archive.load(
            environment,
            request.symbol,
            request.interval,
            start_time_ms=request.start_time_ms,
            end_time_ms=request.end_time_ms,
        )
        if not klines:
            raise CryptoResearchError("archive contains no bars for the requested replay")
        if request.require_contiguous_bars:
            _require_contiguous(klines)
        bars = binance_klines_to_crypto_bars(request.symbol, klines)
        strategy = MovingAverageCryptoTrendStrategy(
            symbol=request.symbol,
            base_asset=request.base_asset,
            quote_asset=request.quote_asset,
            config=request.trend,
        )
        engine = CryptoBacktestEngine(
            CryptoBacktestConfig(
                symbol=request.symbol,
                base_asset=request.base_asset,
                quote_asset=request.quote_asset,
                fees=request.fees,
                market_slippage_rate=request.market_slippage_rate,
                max_bar_volume_fraction=request.max_bar_volume_fraction,
            )
        )
        report = engine.run(
            bars,
            strategy,
            initial_balances={
                request.base_asset: request.initial_base_balance,
                request.quote_asset: request.initial_quote_balance,
            },
        )
        summary = CryptoBacktestSummary(
            environment=environment,
            symbol=request.symbol,
            interval=request.interval,
            dataset_sha256=dataset.sha256,
            bar_count=len(bars),
            first_open_time=bars[0].open_time,
            last_available_time=bars[-1].available_at,
            initial_equity_quote=report.initial_equity_quote,
            final_equity_quote=report.final_equity_quote,
            net_profit_quote=report.net_profit_quote,
            total_return=report.total_return,
            max_drawdown=report.max_drawdown,
            total_fees_quote=report.total_fees_quote,
            turnover_quote=report.turnover_quote,
            order_count=len(report.orders),
            fill_count=report.trade_count,
            pending_order_count=len(report.pending_order_ids),
        )
        return CryptoResearchRun(
            request=request,
            dataset=dataset,
            integrity=integrity,
            bars=bars,
            report=report,
            summary=summary,
        )


def binance_klines_to_crypto_bars(
    symbol: str,
    klines: Sequence[Kline],
) -> tuple[CryptoBar, ...]:
    """将 Binance 含端点的收盘时间戳转换为 K 线可用时间。"""

    normalized_symbol = normalize_symbol(symbol)
    converted: list[CryptoBar] = []
    for kline in klines:
        open_time = _datetime_from_milliseconds(kline.open_time_ms)
        # Binance closeTime 是最后一个包含在内的毫秒。已完成行情柱在一毫秒后可观测，
        # 通常正好位于下一边界。
        available_at = _datetime_from_milliseconds(kline.close_time_ms + 1)
        converted.append(
            CryptoBar(
                symbol=normalized_symbol,
                open_time=open_time,
                close_time=available_at,
                available_at=available_at,
                open=kline.open,
                high=kline.high,
                low=kline.low,
                close=kline.close,
                volume=kline.volume,
                complete=True,
            )
        )
    opens = [bar.open_time for bar in converted]
    if opens != sorted(opens) or len(opens) != len(set(opens)):
        raise CryptoResearchError("Binance klines must be strictly ordered and unique")
    return tuple(converted)


def format_crypto_backtest_summary(summary: CryptoBacktestSummary) -> str:
    """渲染面向运维人员且不隐藏精确值的紧凑报告。"""

    return "\n".join(
        (
            f"Binance {summary.environment.value} {summary.symbol} {summary.interval}",
            f"dataset_sha256={summary.dataset_sha256}",
            f"bars={summary.bar_count} orders={summary.order_count} "
            f"fills={summary.fill_count} pending={summary.pending_order_count}",
            f"equity={format(summary.initial_equity_quote, 'f')} -> "
            f"{format(summary.final_equity_quote, 'f')}",
            f"net_profit={format(summary.net_profit_quote, 'f')} "
            f"return={format(summary.total_return, 'f')} "
            f"max_drawdown={format(summary.max_drawdown, 'f')}",
            f"fees={format(summary.total_fees_quote, 'f')} "
            f"turnover={format(summary.turnover_quote, 'f')}",
        )
    )


def _require_contiguous(klines: Sequence[Kline]) -> None:
    for previous, current in zip(klines, klines[1:], strict=False):
        if previous.close_time_ms + 1 != current.open_time_ms:
            raise CryptoResearchError(
                "replay bars are not contiguous: expected "
                f"{previous.close_time_ms + 1}, got {current.open_time_ms}"
            )


def _datetime_from_milliseconds(value: int) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CryptoResearchError("kline timestamp must be a non-negative integer")
    try:
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)
    except OverflowError as error:
        raise CryptoResearchError("kline timestamp is outside datetime range") from error


def _validate_optional_time(value: int | None, name: str) -> None:
    if value is not None and (
        isinstance(value, bool) or not isinstance(value, int) or value < 0
    ):
        raise ValueError(f"{name} must be a non-negative integer or None")


def _require_decimal(name: str, value: object) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
