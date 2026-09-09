"""A 股日线策略评估器的不可变输入、配置与结果模型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import StrEnum
from zoneinfo import ZoneInfo

from gribuki_trade.backtest.costs import InstrumentType
from gribuki_trade.strategy_lab.experiments import PerformanceMetrics

_MARKET_TZ = ZoneInfo("Asia/Shanghai")
_SCORE_BOUND = Decimal("1")


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > 160 or any(ord(char) < 32 for char in normalized):
        raise ValueError(f"{name} must be a non-empty printable identifier")
    return normalized


def _finite_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise TypeError(f"{name} must be a finite Decimal")
    return value


def _positive_decimal(value: object, name: str) -> Decimal:
    resolved = _finite_decimal(value, name)
    if resolved <= 0:
        raise ValueError(f"{name} must be positive")
    return resolved


def _aware_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


class AShareExecutionPolicy(StrEnum):
    """支持的日线撮合策略。"""

    NEXT_OPEN = "NEXT_OPEN"
    CONSERVATIVE_OPEN_LIMIT = "CONSERVATIVE_OPEN_LIMIT"


class AShareEvaluationAction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    STAY_CASH = "STAY_CASH"
    NO_FILL_SUSPENDED = "NO_FILL_SUSPENDED"
    NO_FILL_PRICE_LIMIT_UNKNOWN = "NO_FILL_PRICE_LIMIT_UNKNOWN"
    NO_FILL_LIMIT_LOCKED = "NO_FILL_LIMIT_LOCKED"
    NO_FILL_ORDER_LIMIT = "NO_FILL_ORDER_LIMIT"
    NO_FILL_VOLUME = "NO_FILL_VOLUME"
    NO_FILL_CASH = "NO_FILL_CASH"
    NO_FILL_T_PLUS_ONE = "NO_FILL_T_PLUS_ONE"


@dataclass(frozen=True, slots=True)
class PITStrategyScore:
    """一个分数，以及证明其何时可知的血缘信息。"""

    family_id: str
    value: Decimal
    known_at: datetime
    source_id: str
    source_revision: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "family_id", _identifier(self.family_id, "family_id"))
        object.__setattr__(self, "value", _finite_decimal(self.value, "value"))
        if not -_SCORE_BOUND <= self.value <= _SCORE_BOUND:
            raise ValueError("score value must be in [-1, 1]")
        object.__setattr__(self, "known_at", _aware_utc(self.known_at, "known_at"))
        object.__setattr__(self, "source_id", _identifier(self.source_id, "source_id"))
        object.__setattr__(
            self,
            "source_revision",
            _identifier(self.source_revision, "source_revision"),
        )


@dataclass(frozen=True, slots=True)
class CompletedAShareDailyBar:
    """带明确执行门槛的下一交易日已完成、不复权 K 线。

    涨跌停价格为可选项，因为冻结数据源中某些标的或交易日确实没有可用
    区间。任一边界缺失时，执行按闭锁原则失败；评估器绝不推测板块或
    ST 标的特有的百分比。
    """

    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_shares: int
    suspended: bool
    lower_price_limit: Decimal | None
    upper_price_limit: Decimal | None
    completed_at: datetime
    source_id: str
    source_revision: str

    def __post_init__(self) -> None:
        if not isinstance(self.session_date, date) or isinstance(self.session_date, datetime):
            raise TypeError("session_date must be a date")
        for field_name in ("open", "high", "low", "close"):
            value = _positive_decimal(getattr(self, field_name), field_name)
            object.__setattr__(self, field_name, value)
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("OHLC values are inconsistent")
        if self.low > self.high:
            raise ValueError("bar low must not exceed high")
        if isinstance(self.volume_shares, bool) or not isinstance(self.volume_shares, int):
            raise TypeError("volume_shares must be an integer")
        if self.volume_shares < 0:
            raise ValueError("volume_shares must be non-negative")
        if not isinstance(self.suspended, bool):
            raise TypeError("suspended must be a bool")
        if (self.lower_price_limit is None) != (self.upper_price_limit is None):
            raise ValueError("lower and upper price limits must be supplied together")
        if self.lower_price_limit is not None and self.upper_price_limit is not None:
            lower = _positive_decimal(self.lower_price_limit, "lower_price_limit")
            upper = _positive_decimal(self.upper_price_limit, "upper_price_limit")
            if lower >= upper:
                raise ValueError("lower_price_limit must be below upper_price_limit")
            if self.low < lower or self.high > upper:
                raise ValueError("OHLC values must remain inside the explicit price band")
            object.__setattr__(self, "lower_price_limit", lower)
            object.__setattr__(self, "upper_price_limit", upper)
        completed_at = _aware_utc(self.completed_at, "completed_at")
        local_completion = completed_at.astimezone(_MARKET_TZ)
        if local_completion < datetime.combine(
            self.session_date,
            time(15, 0),
            tzinfo=_MARKET_TZ,
        ):
            raise ValueError("completed_at must not precede the session close")
        object.__setattr__(self, "completed_at", completed_at)
        object.__setattr__(self, "source_id", _identifier(self.source_id, "source_id"))
        object.__setattr__(
            self,
            "source_revision",
            _identifier(self.source_revision, "source_revision"),
        )


@dataclass(frozen=True, slots=True)
class AShareDailyStrategyObservation:
    """一个时点信号，以及严格晚于它的执行和计价交易日。"""

    observation_id: str
    symbol: str
    instrument_type: InstrumentType
    signal_as_of: datetime
    technical_scores: tuple[PITStrategyScore, ...]
    macro_score: PITStrategyScore
    execution_bar: CompletedAShareDailyBar
    buy_limit_price: Decimal | None = None
    sell_limit_price: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "observation_id",
            _identifier(self.observation_id, "observation_id"),
        )
        symbol = self.symbol.strip().upper()
        if len(symbol) != 9 or symbol[6:] not in {".SH", ".SZ", ".BJ"}:
            raise ValueError("symbol must use the 000001.SZ/600000.SH/920000.BJ form")
        if not symbol[:6].isdigit():
            raise ValueError("symbol must begin with six digits")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "instrument_type", InstrumentType(self.instrument_type))
        signal_as_of = _aware_utc(self.signal_as_of, "signal_as_of")
        object.__setattr__(self, "signal_as_of", signal_as_of)
        technical = tuple(sorted(self.technical_scores, key=lambda item: item.family_id))
        if not technical or len({score.family_id for score in technical}) != len(technical):
            raise ValueError("technical_scores must be non-empty and have unique families")
        if any(score.family_id == "macro" for score in technical):
            raise ValueError("macro is reserved for macro_score")
        if self.macro_score.family_id != "macro":
            raise ValueError("macro_score.family_id must be 'macro'")
        for score in (*technical, self.macro_score):
            if score.known_at > signal_as_of:
                raise ValueError("score known_at must not exceed signal_as_of")
        signal_date = signal_as_of.astimezone(_MARKET_TZ).date()
        if self.execution_bar.session_date <= signal_date:
            raise ValueError("execution bar must be from a later session than the signal")
        object.__setattr__(self, "technical_scores", technical)
        for field_name in ("buy_limit_price", "sell_limit_price"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _positive_decimal(value, field_name))

    @property
    def signal_date(self) -> date:
        return self.signal_as_of.astimezone(_MARKET_TZ).date()


@dataclass(frozen=True, slots=True)
class AShareDailyEvaluatorConfig:
    strategy_version: str = "ashare-daily-pit-evaluator@1"
    label_id: str = "next-session-open-mark-close@1"
    starting_cash_cny: Decimal = Decimal("1000000")
    enter_threshold: Decimal = Decimal("0.15")
    exit_threshold: Decimal = Decimal("0")
    max_volume_participation: Decimal = Decimal("0.01")
    transfer_fee_bps: Decimal = Decimal("0")
    annual_sessions: int = 252
    execution_policy: AShareExecutionPolicy = AShareExecutionPolicy.NEXT_OPEN

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "strategy_version",
            _identifier(self.strategy_version, "strategy_version"),
        )
        object.__setattr__(self, "label_id", _identifier(self.label_id, "label_id"))
        starting_cash = _positive_decimal(self.starting_cash_cny, "starting_cash_cny")
        enter = _finite_decimal(self.enter_threshold, "enter_threshold")
        exit_ = _finite_decimal(self.exit_threshold, "exit_threshold")
        participation = _finite_decimal(
            self.max_volume_participation,
            "max_volume_participation",
        )
        transfer = _finite_decimal(self.transfer_fee_bps, "transfer_fee_bps")
        if not -_SCORE_BOUND <= exit_ < enter <= _SCORE_BOUND:
            raise ValueError("thresholds must satisfy -1 <= exit < enter <= 1")
        if not Decimal("0") < participation <= Decimal("1"):
            raise ValueError("max_volume_participation must be in (0, 1]")
        if transfer < 0:
            raise ValueError("transfer_fee_bps must be non-negative")
        if isinstance(self.annual_sessions, bool) or self.annual_sessions < 1:
            raise ValueError("annual_sessions must be a positive integer")
        object.__setattr__(self, "starting_cash_cny", starting_cash)
        object.__setattr__(self, "enter_threshold", enter)
        object.__setattr__(self, "exit_threshold", exit_)
        object.__setattr__(self, "max_volume_participation", participation)
        object.__setattr__(self, "transfer_fee_bps", transfer)
        object.__setattr__(
            self,
            "execution_policy",
            AShareExecutionPolicy(self.execution_policy),
        )

    @property
    def manifest_parameters(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            sorted(
                (
                    ("annual_sessions", str(self.annual_sessions)),
                    ("enter_threshold", str(self.enter_threshold)),
                    ("execution_policy", self.execution_policy.value),
                    ("exit_threshold", str(self.exit_threshold)),
                    ("max_volume_participation", str(self.max_volume_participation)),
                    ("starting_cash_cny", str(self.starting_cash_cny)),
                    ("transfer_fee_bps", str(self.transfer_fee_bps)),
                )
            )
        )


@dataclass(frozen=True, slots=True)
class AShareEvaluationEvent:
    observation_id: str
    session_date: date
    action: AShareEvaluationAction
    fused_score: Decimal
    quantity: int = 0
    fill_price: Decimal | None = None
    cash_after: Decimal | None = None


@dataclass(frozen=True, slots=True)
class AShareEvaluationResult:
    """一个隔离评估切片的指标与审计轨迹。

    ``PerformanceMetrics.family_contributions`` 包含加权决策分数贡献均值，
    因此有意不将其标注为盈亏归因。
    """

    metrics: PerformanceMetrics
    events: tuple[AShareEvaluationEvent, ...]
    data_manifest_sha256: str
    strategy_manifest_sha256: str
