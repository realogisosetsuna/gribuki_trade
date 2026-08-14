"""退出策略的确定性离线评估、滚动验证与试验登记。

本模块只重放已经冻结的历史样本，不连接行情、券商或生产参数。评估按 A 股
T+1、停牌和一字跌停不可卖等约束执行；日线同时触及止损和止盈时固定采用更
保守的止损优先语义。候选策略只能依据滚动验证集排序，最终留出集在候选编号
确定后才会读取，登记结果也不会自动晋升为生产配置。
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import StrEnum
from itertools import chain
from typing import Protocol
from zoneinfo import ZoneInfo

from gribuki_trade.strategy_lab.exit_policies import (
    ExitPolicyParameters,
    ExitPolicySearchSpace,
    generate_exit_policy_candidates,
)
from gribuki_trade.strategy_lab.experiments import (
    WalkForwardConfig,
    WalkForwardPlan,
    build_walk_forward_plan,
)

_MARKET_TZ = ZoneInfo("Asia/Shanghai")
_BPS = Decimal("10000")
_ONE = Decimal("1")
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ExitPolicyBarrier(StrEnum):
    """离线重放中能够触发卖出请求的退出原因。"""

    STOP_LOSS = "STOP_LOSS"
    TRAILING_STOP = "TRAILING_STOP"
    TAKE_PROFIT = "TAKE_PROFIT"
    TIME = "TIME"
    TERMINAL_MARK = "TERMINAL_MARK"


class ExitPolicyOutcomeStatus(StrEnum):
    """样本最终是实际模拟成交，还是仅在数据末端估值。"""

    FILLED = "FILLED"
    MARKED_OPEN = "MARKED_OPEN"


class ExitPolicyTrialStatus(StrEnum):
    """预注册候选在试验登记表中的终态。"""

    EVALUATED = "EVALUATED"
    REJECTED_INVALID_PLAN = "REJECTED_INVALID_PLAN"


class ExitPolicyObjective(StrEnum):
    """只能使用验证集计算的候选排序目标。"""

    NET_RETURN = "NET_RETURN"
    RETURN_TO_DRAWDOWN = "RETURN_TO_DRAWDOWN"
    HIT_RATE = "HIT_RATE"


class ExitPolicyInvalidPlanError(ValueError):
    """参数在某个冻结样本上无法形成合法正价格退出计划。"""


@dataclass(frozen=True, slots=True)
class ExitEvaluationBar:
    """一根已完成、未复权且带有当日真实涨跌停边界的日线。"""

    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_shares: int
    suspended: bool
    lower_price_limit: Decimal
    upper_price_limit: Decimal
    completed_at: datetime
    source_id: str
    source_revision: str

    def __post_init__(self) -> None:
        if not isinstance(self.session_date, date) or isinstance(self.session_date, datetime):
            raise TypeError("session_date must be a date")
        for name in (
            "open",
            "high",
            "low",
            "close",
            "lower_price_limit",
            "upper_price_limit",
        ):
            object.__setattr__(self, name, _positive_decimal(getattr(self, name), name))
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("OHLC values are inconsistent")
        if self.low > self.high:
            raise ValueError("bar low must not exceed high")
        if self.lower_price_limit >= self.upper_price_limit:
            raise ValueError("lower_price_limit must be below upper_price_limit")
        if self.low < self.lower_price_limit or self.high > self.upper_price_limit:
            raise ValueError("OHLC values must remain inside the explicit price band")
        if isinstance(self.volume_shares, bool) or not isinstance(self.volume_shares, int):
            raise TypeError("volume_shares must be an integer")
        if self.volume_shares < 0:
            raise ValueError("volume_shares must be non-negative")
        if not isinstance(self.suspended, bool):
            raise TypeError("suspended must be a bool")
        completed_at = _aware_utc(self.completed_at, "completed_at")
        market_close = datetime.combine(
            self.session_date,
            time(15, 0),
            tzinfo=_MARKET_TZ,
        )
        if completed_at.astimezone(_MARKET_TZ) < market_close:
            raise ValueError("completed_at must not precede the session close")
        object.__setattr__(self, "completed_at", completed_at)
        object.__setattr__(self, "source_id", _identifier(self.source_id, "source_id"))
        object.__setattr__(
            self,
            "source_revision",
            _identifier(self.source_revision, "source_revision"),
        )

    @property
    def lower_limit_locked(self) -> bool:
        """只有全天封死在跌停价时，日线证据才足以判定无法卖出。"""

        return (
            not self.suspended
            and self.volume_shares >= 0
            and self.open == self.high == self.low == self.close == self.lower_price_limit
        )


@dataclass(frozen=True, slots=True)
class ExitPolicyEpisode:
    """一笔已经成交的买入及其随后可用于退出重放的完整日线窗口。"""

    episode_id: str
    symbol: str
    entry_at: datetime
    entry_session_date: date
    entry_price: Decimal
    quantity: int
    atr_at_entry: Decimal
    structure_low_at_entry: Decimal
    features_known_at: datetime
    future_bars: tuple[ExitEvaluationBar, ...]
    source_revisions: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "episode_id", _identifier(self.episode_id, "episode_id"))
        symbol = self.symbol.strip().upper()
        if (
            len(symbol) != 9
            or not symbol[:6].isdigit()
            or symbol[6:] not in {".SH", ".SZ", ".BJ"}
        ):
            raise ValueError("symbol must use canonical 600000.SH form")
        object.__setattr__(self, "symbol", symbol)
        entry_at = _aware_utc(self.entry_at, "entry_at")
        known_at = _aware_utc(self.features_known_at, "features_known_at")
        if known_at > entry_at:
            raise ValueError("features_known_at must not exceed entry_at")
        if not isinstance(self.entry_session_date, date) or isinstance(
            self.entry_session_date,
            datetime,
        ):
            raise TypeError("entry_session_date must be a date")
        if entry_at.astimezone(_MARKET_TZ).date() != self.entry_session_date:
            raise ValueError("entry_session_date must match entry_at in Asia/Shanghai")
        object.__setattr__(self, "entry_at", entry_at)
        object.__setattr__(self, "features_known_at", known_at)
        for name in ("entry_price", "atr_at_entry", "structure_low_at_entry"):
            object.__setattr__(self, name, _positive_decimal(getattr(self, name), name))
        if isinstance(self.quantity, bool) or not isinstance(self.quantity, int):
            raise TypeError("quantity must be an integer")
        if self.quantity < 1:
            raise ValueError("quantity must be positive")
        bars = tuple(self.future_bars)
        if not bars:
            raise ValueError("future_bars must not be empty")
        dates = tuple(bar.session_date for bar in bars)
        if dates != tuple(sorted(dates)) or len(set(dates)) != len(dates):
            raise ValueError("future_bars must have strictly increasing sessions")
        if any(bar.session_date <= self.entry_session_date for bar in bars):
            raise ValueError("future_bars must begin after the entry session for T+1")
        if any(bar.completed_at <= entry_at for bar in bars):
            raise ValueError("future bars must complete after entry_at")
        revisions = tuple(sorted(self.source_revisions))
        if not revisions or len({name for name, _ in revisions}) != len(revisions):
            raise ValueError("source_revisions must be non-empty and uniquely named")
        for source_id, revision in revisions:
            _identifier(source_id, "source ID")
            _identifier(revision, "source revision")
        object.__setattr__(self, "future_bars", bars)
        object.__setattr__(self, "source_revisions", revisions)


@dataclass(frozen=True, slots=True)
class ExitPolicyDataset:
    """按内容哈希冻结的退出策略样本集。"""

    dataset_id: str
    schema_version: str
    episodes: tuple[ExitPolicyEpisode, ...]
    frozen_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_id", _identifier(self.dataset_id, "dataset_id"))
        object.__setattr__(
            self,
            "schema_version",
            _identifier(self.schema_version, "schema_version"),
        )
        episodes = tuple(self.episodes)
        if not episodes:
            raise ValueError("episodes must not be empty")
        ordered = tuple(
            sorted(episodes, key=lambda item: (item.entry_session_date, item.episode_id))
        )
        if episodes != ordered:
            raise ValueError("episodes must be ordered by entry session and episode_id")
        if len({item.episode_id for item in episodes}) != len(episodes):
            raise ValueError("episode_id values must be unique")
        frozen_at = _aware_utc(self.frozen_at, "frozen_at")
        latest_evidence = max(
            chain(
                (episode.entry_at for episode in episodes),
                (bar.completed_at for episode in episodes for bar in episode.future_bars),
            )
        )
        if frozen_at < latest_evidence:
            raise ValueError("frozen_at must not precede dataset evidence")
        object.__setattr__(self, "episodes", episodes)
        object.__setattr__(self, "frozen_at", frozen_at)

    @property
    def entry_sessions(self) -> tuple[date, ...]:
        return tuple(sorted({episode.entry_session_date for episode in self.episodes}))

    @property
    def content_sha256(self) -> str:
        return _sha256_document(_dataset_document(self))

    @property
    def source_revisions(self) -> tuple[tuple[str, str], ...]:
        revisions = {
            revision
            for episode in self.episodes
            for revision in (
                *episode.source_revisions,
                *((bar.source_id, bar.source_revision) for bar in episode.future_bars),
            )
        }
        return tuple(sorted(revisions))


@dataclass(frozen=True, slots=True)
class ExitPolicyCostModel:
    """以基点表示的双边费用、卖出税费和保守卖出滑点。"""

    commission_bps: Decimal
    tax_bps: Decimal
    transfer_fee_bps: Decimal
    sell_slippage_bps: Decimal
    minimum_commission_cny: Decimal = Decimal("5")

    def __post_init__(self) -> None:
        for name in (
            "commission_bps",
            "tax_bps",
            "transfer_fee_bps",
            "sell_slippage_bps",
            "minimum_commission_cny",
        ):
            object.__setattr__(self, name, _nonnegative_decimal(getattr(self, name), name))
        if self.sell_slippage_bps >= _BPS:
            raise ValueError("sell_slippage_bps must be below 10000")

    @property
    def fingerprint(self) -> str:
        return _sha256_document(_cost_document(self))


@dataclass(frozen=True, slots=True)
class ExitPolicyOutcome:
    """单个样本的退出结果、净收益和可复核执行证据。"""

    episode_id: str
    entry_session_date: date
    parameter_fingerprint: str
    initial_stop_price: Decimal
    final_stop_price: Decimal
    take_profit_price: Decimal
    barrier: ExitPolicyBarrier
    barrier_observed_on: date
    status: ExitPolicyOutcomeStatus
    execution_on: date | None
    raw_execution_price: Decimal | None
    net_execution_price: Decimal
    holding_sessions: int
    both_price_barriers_touched: bool
    blocked_sessions: int
    net_pnl_cny: Decimal
    net_return: Decimal
    warning_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "episode_id", _identifier(self.episode_id, "episode_id"))
        if not isinstance(self.entry_session_date, date) or isinstance(
            self.entry_session_date,
            datetime,
        ):
            raise TypeError("entry_session_date must be a date")
        if _SHA256.fullmatch(self.parameter_fingerprint) is None:
            raise ValueError("parameter_fingerprint must be a lowercase SHA-256 digest")
        for name in (
            "initial_stop_price",
            "final_stop_price",
            "take_profit_price",
            "net_execution_price",
        ):
            object.__setattr__(self, name, _positive_decimal(getattr(self, name), name))
        object.__setattr__(self, "barrier", ExitPolicyBarrier(self.barrier))
        object.__setattr__(self, "status", ExitPolicyOutcomeStatus(self.status))
        if self.status is ExitPolicyOutcomeStatus.FILLED:
            if self.execution_on is None or self.raw_execution_price is None:
                raise ValueError("FILLED requires execution date and raw price")
        elif self.execution_on is not None or self.raw_execution_price is not None:
            raise ValueError("MARKED_OPEN must not claim an execution")
        if self.raw_execution_price is not None:
            object.__setattr__(
                self,
                "raw_execution_price",
                _positive_decimal(self.raw_execution_price, "raw_execution_price"),
            )
        for name in ("holding_sessions", "blocked_sessions"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        object.__setattr__(self, "net_pnl_cny", _finite_decimal(self.net_pnl_cny, "net_pnl_cny"))
        object.__setattr__(self, "net_return", _finite_decimal(self.net_return, "net_return"))
        warnings = tuple(_identifier(code, "warning code") for code in self.warning_codes)
        if len(warnings) != len(set(warnings)):
            raise ValueError("warning_codes must be unique")
        object.__setattr__(self, "warning_codes", warnings)


@dataclass(frozen=True, slots=True)
class ExitPolicyMetrics:
    """逐笔等资本复投收益、保守逐笔回撤、已成交胜率和盈亏比。"""

    episode_count: int
    completed_count: int
    marked_open_count: int
    winning_count: int
    losing_count: int
    net_return: Decimal
    maximum_drawdown: Decimal
    average_return: Decimal
    hit_rate: Decimal
    profit_factor: Decimal | None
    gross_profit_cny: Decimal
    gross_loss_cny: Decimal
    blocked_session_count: int

    def __post_init__(self) -> None:
        for name in (
            "episode_count",
            "completed_count",
            "marked_open_count",
            "winning_count",
            "losing_count",
            "blocked_session_count",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.episode_count < 1:
            raise ValueError("episode_count must be positive")
        if self.completed_count + self.marked_open_count != self.episode_count:
            raise ValueError("completed and marked-open counts must cover all episodes")
        for name in (
            "net_return",
            "maximum_drawdown",
            "average_return",
            "hit_rate",
            "gross_profit_cny",
            "gross_loss_cny",
        ):
            object.__setattr__(self, name, _finite_decimal(getattr(self, name), name))
        if self.maximum_drawdown < 0:
            raise ValueError("maximum_drawdown must be non-negative")
        if not Decimal("0") <= self.hit_rate <= _ONE:
            raise ValueError("hit_rate must be in [0, 1]")
        if self.gross_profit_cny < 0 or self.gross_loss_cny < 0:
            raise ValueError("gross profit and loss must be non-negative")
        if self.profit_factor is not None:
            object.__setattr__(
                self,
                "profit_factor",
                _nonnegative_decimal(self.profit_factor, "profit_factor"),
            )


@dataclass(frozen=True, slots=True)
class ExitPolicyEvaluation:
    """一个参数候选在一组样本上的指标和逐笔结果。"""

    parameters: ExitPolicyParameters
    observation_indices: tuple[int, ...]
    metrics: ExitPolicyMetrics
    outcomes: tuple[ExitPolicyOutcome, ...]

    def __post_init__(self) -> None:
        if not self.observation_indices:
            raise ValueError("observation_indices must not be empty")
        if self.observation_indices != tuple(sorted(self.observation_indices)) or len(
            set(self.observation_indices)
        ) != len(self.observation_indices):
            raise ValueError("observation_indices must be strictly increasing and unique")
        if len(self.outcomes) != len(self.observation_indices):
            raise ValueError("outcomes must cover every observation index")
        if self.metrics.episode_count != len(self.outcomes):
            raise ValueError("metrics must cover every outcome")
        fingerprint = self.parameters.fingerprint
        if any(outcome.parameter_fingerprint != fingerprint for outcome in self.outcomes):
            raise ValueError("outcome parameter fingerprints must match the evaluation")


class ExitPolicyEvaluatorProtocol(Protocol):
    """允许试验编排器注入可记录调用的纯离线评估器。"""

    @property
    def dataset(self) -> ExitPolicyDataset: ...

    def evaluate(
        self,
        parameters: ExitPolicyParameters,
        observation_indices: tuple[int, ...],
        costs: ExitPolicyCostModel,
        *,
        label_horizon_sessions: int,
    ) -> ExitPolicyEvaluation: ...


class ExitPolicyEvaluator:
    """按冻结日线确定性重放每一个退出候选。"""

    def __init__(self, dataset: ExitPolicyDataset) -> None:
        self._dataset = dataset

    @property
    def dataset(self) -> ExitPolicyDataset:
        return self._dataset

    def evaluate(
        self,
        parameters: ExitPolicyParameters,
        observation_indices: tuple[int, ...],
        costs: ExitPolicyCostModel,
        *,
        label_horizon_sessions: int,
    ) -> ExitPolicyEvaluation:
        indices = _validated_indices(observation_indices, len(self._dataset.episodes))
        if (
            isinstance(label_horizon_sessions, bool)
            or not isinstance(label_horizon_sessions, int)
            or label_horizon_sessions < parameters.maximum_holding_sessions
        ):
            raise ValueError("label horizon must cover maximum_holding_sessions")
        if any(
            len(self._dataset.episodes[index].future_bars) < label_horizon_sessions
            for index in indices
        ):
            raise ValueError("episode future-bar coverage is shorter than label horizon")
        outcomes = tuple(
            _evaluate_episode(
                self._dataset.episodes[index],
                parameters,
                costs,
                label_horizon_sessions,
            )
            for index in indices
        )
        return ExitPolicyEvaluation(
            parameters=parameters,
            observation_indices=indices,
            metrics=_metrics(outcomes),
            outcomes=outcomes,
        )


@dataclass(frozen=True, slots=True)
class ExitPolicyWalkForwardFold:
    """按入场交易日分组后的一个验证折。"""

    fold_id: str
    train_sessions: tuple[date, ...]
    purge_sessions: tuple[date, ...]
    validation_sessions: tuple[date, ...]
    embargo_sessions: tuple[date, ...]
    validation_episode_indices: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class ExitPolicyWalkForwardPlan:
    """保留交易日边界和样本索引映射的退出策略滚动计划。"""

    dataset_sha256: str
    session_plan: WalkForwardPlan
    folds: tuple[ExitPolicyWalkForwardFold, ...]
    holdout_sessions: tuple[date, ...]
    holdout_episode_indices: tuple[int, ...]
    minimum_validation_episodes: int
    minimum_holdout_episodes: int

    def __post_init__(self) -> None:
        if _SHA256.fullmatch(self.dataset_sha256) is None:
            raise ValueError("dataset_sha256 must be a lowercase SHA-256 digest")

    @property
    def plan_sha256(self) -> str:
        return _sha256_document(_walk_forward_document(self))


def build_exit_policy_walk_forward_plan(
    dataset: ExitPolicyDataset,
    config: WalkForwardConfig,
    *,
    minimum_validation_episodes: int,
    minimum_holdout_episodes: int,
) -> ExitPolicyWalkForwardPlan:
    """先按交易日切分，再映射到样本，避免同日标的跨集合泄漏。"""

    _positive_integer(minimum_validation_episodes, "minimum_validation_episodes")
    _positive_integer(minimum_holdout_episodes, "minimum_holdout_episodes")
    session_plan = build_walk_forward_plan(dataset.entry_sessions, config)
    indices_by_session: dict[date, list[int]] = {}
    for index, episode in enumerate(dataset.episodes):
        indices_by_session.setdefault(episode.entry_session_date, []).append(index)

    folds = tuple(
        ExitPolicyWalkForwardFold(
            fold_id=fold.fold_id,
            train_sessions=_sessions_at(dataset.entry_sessions, fold.train_indices),
            purge_sessions=_sessions_at(dataset.entry_sessions, fold.purge_indices),
            validation_sessions=_sessions_at(
                dataset.entry_sessions,
                fold.validation_indices,
            ),
            embargo_sessions=_sessions_at(dataset.entry_sessions, fold.embargo_indices),
            validation_episode_indices=_episode_indices_for_sessions(
                indices_by_session,
                _sessions_at(dataset.entry_sessions, fold.validation_indices),
            ),
        )
        for fold in session_plan.folds
    )
    for fold in folds:
        if len(fold.validation_episode_indices) < minimum_validation_episodes:
            raise ValueError(
                f"{fold.fold_id} has fewer than minimum_validation_episodes"
            )
    holdout_sessions = _sessions_at(dataset.entry_sessions, session_plan.test_indices)
    holdout_indices = _episode_indices_for_sessions(indices_by_session, holdout_sessions)
    if len(holdout_indices) < minimum_holdout_episodes:
        raise ValueError("holdout has fewer than minimum_holdout_episodes")
    return ExitPolicyWalkForwardPlan(
        dataset_sha256=dataset.content_sha256,
        session_plan=session_plan,
        folds=folds,
        holdout_sessions=holdout_sessions,
        holdout_episode_indices=holdout_indices,
        minimum_validation_episodes=minimum_validation_episodes,
        minimum_holdout_episodes=minimum_holdout_episodes,
    )


@dataclass(frozen=True, slots=True)
class ExitPolicyFoldResult:
    """一个候选在单个验证折上的指标。"""

    fold_id: str
    evaluation: ExitPolicyEvaluation

    def __post_init__(self) -> None:
        object.__setattr__(self, "fold_id", _identifier(self.fold_id, "fold_id"))


@dataclass(frozen=True, slots=True)
class ExitPolicyTrial:
    """一个预注册参数候选的完整验证记录。"""

    trial_id: str
    parameters: ExitPolicyParameters
    status: ExitPolicyTrialStatus
    fold_results: tuple[ExitPolicyFoldResult, ...]
    validation_metrics: ExitPolicyMetrics | None
    validation_score: Decimal | None
    rejection_code: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "trial_id", _identifier(self.trial_id, "trial_id"))
        object.__setattr__(self, "status", ExitPolicyTrialStatus(self.status))
        if self.status is ExitPolicyTrialStatus.EVALUATED:
            if (
                not self.fold_results
                or self.validation_metrics is None
                or self.validation_score is None
                or self.rejection_code is not None
            ):
                raise ValueError("evaluated trials require metrics and no rejection")
            object.__setattr__(
                self,
                "validation_score",
                _finite_decimal(self.validation_score, "validation_score"),
            )
        elif (
            self.fold_results
            or self.validation_metrics is not None
            or self.validation_score is not None
            or self.rejection_code is None
        ):
            raise ValueError("rejected trials require only a rejection code")
        if self.rejection_code is not None:
            object.__setattr__(
                self,
                "rejection_code",
                _identifier(self.rejection_code, "rejection_code"),
            )


@dataclass(frozen=True, slots=True)
class ExitPolicyTrialRegistry:
    """可哈希、可序列化且永不自动写入生产配置的试验登记。"""

    registry_id: str
    experiment_version: str
    dataset_sha256: str
    search_space_sha256: str
    walk_forward_sha256: str
    cost_model_sha256: str
    objective: ExitPolicyObjective
    trials: tuple[ExitPolicyTrial, ...]
    selected_trial_id: str
    baseline_trial_id: str
    selected_holdout: ExitPolicyEvaluation
    baseline_holdout: ExitPolicyEvaluation
    created_at: datetime
    research_only: bool = True
    promotion_authorized: bool = False

    def __post_init__(self) -> None:
        for name in (
            "registry_id",
            "experiment_version",
            "selected_trial_id",
            "baseline_trial_id",
        ):
            object.__setattr__(self, name, _identifier(getattr(self, name), name))
        for name in (
            "dataset_sha256",
            "search_space_sha256",
            "walk_forward_sha256",
            "cost_model_sha256",
        ):
            if _SHA256.fullmatch(getattr(self, name)) is None:
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")
        object.__setattr__(self, "objective", ExitPolicyObjective(self.objective))
        trials = tuple(sorted(self.trials, key=lambda item: item.trial_id))
        if not trials or len({trial.trial_id for trial in trials}) != len(trials):
            raise ValueError("trials must be non-empty and uniquely identified")
        trial_ids = {trial.trial_id for trial in trials}
        if self.selected_trial_id not in trial_ids or self.baseline_trial_id not in trial_ids:
            raise ValueError("selected and baseline trial IDs must be registered")
        selected = next(trial for trial in trials if trial.trial_id == self.selected_trial_id)
        baseline = next(trial for trial in trials if trial.trial_id == self.baseline_trial_id)
        if (
            selected.status is not ExitPolicyTrialStatus.EVALUATED
            or baseline.status is not ExitPolicyTrialStatus.EVALUATED
        ):
            raise ValueError("selected and baseline trials must be evaluated")
        if self.selected_holdout.parameters.fingerprint != selected.parameters.fingerprint:
            raise ValueError("selected holdout must match the selected trial")
        if self.baseline_holdout.parameters.fingerprint != baseline.parameters.fingerprint:
            raise ValueError("baseline holdout must match the baseline trial")
        if (
            self.selected_holdout.observation_indices
            != self.baseline_holdout.observation_indices
        ):
            raise ValueError("selected and baseline holdouts must use identical episodes")
        if self.research_only is not True or self.promotion_authorized is not False:
            raise ValueError("exit-policy registries are research-only and non-promoting")
        object.__setattr__(self, "trials", trials)
        object.__setattr__(self, "created_at", _aware_utc(self.created_at, "created_at"))

    @property
    def registry_sha256(self) -> str:
        return _sha256_document(_registry_document(self))


def run_exit_policy_walk_forward_experiment(
    *,
    evaluator: ExitPolicyEvaluatorProtocol,
    search_space: ExitPolicySearchSpace,
    baseline: ExitPolicyParameters,
    walk_forward: ExitPolicyWalkForwardPlan,
    costs: ExitPolicyCostModel,
    objective: ExitPolicyObjective,
    created_at: datetime,
    experiment_version: str = "exit-policy-walk-forward@1",
) -> ExitPolicyTrialRegistry:
    """登记全部候选，只按验证折选参，然后评估选中项和基线留出集。"""

    experiment_version = _identifier(experiment_version, "experiment_version")
    objective = ExitPolicyObjective(objective)
    created_at = _aware_utc(created_at, "created_at")
    if created_at < evaluator.dataset.frozen_at:
        raise ValueError("created_at must not precede dataset.frozen_at")
    if evaluator.dataset.content_sha256 != walk_forward.dataset_sha256:
        raise ValueError("walk-forward plan belongs to a different frozen dataset")
    if evaluator.dataset.entry_sessions != walk_forward.session_plan.timestamps:
        raise ValueError("walk-forward session lineage does not match the evaluator")
    expected_plan = build_exit_policy_walk_forward_plan(
        evaluator.dataset,
        walk_forward.session_plan.config,
        minimum_validation_episodes=walk_forward.minimum_validation_episodes,
        minimum_holdout_episodes=walk_forward.minimum_holdout_episodes,
    )
    if walk_forward != expected_plan:
        raise ValueError("walk-forward plan does not match its deterministic rebuild")
    candidates = generate_exit_policy_candidates(search_space)
    candidate_by_fingerprint = {candidate.fingerprint: candidate for candidate in candidates}
    if baseline.fingerprint not in candidate_by_fingerprint:
        raise ValueError("baseline must be a registered search-space candidate")
    if max(candidate.maximum_holding_sessions for candidate in candidates) > (
        walk_forward.session_plan.config.label_horizon_sessions
    ):
        raise ValueError("walk-forward label horizon must cover every candidate")

    trials: list[ExitPolicyTrial] = []
    for candidate in candidates:
        trial_id = f"exit-{candidate.fingerprint[:24]}"
        try:
            fold_results = tuple(
                ExitPolicyFoldResult(
                    fold_id=fold.fold_id,
                    evaluation=evaluator.evaluate(
                        candidate,
                        fold.validation_episode_indices,
                        costs,
                        label_horizon_sessions=(
                            walk_forward.session_plan.config.label_horizon_sessions
                        ),
                    ),
                )
                for fold in walk_forward.folds
            )
            validation = _combine_evaluations(candidate, fold_results)
            trials.append(
                ExitPolicyTrial(
                    trial_id=trial_id,
                    parameters=candidate,
                    status=ExitPolicyTrialStatus.EVALUATED,
                    fold_results=fold_results,
                    validation_metrics=validation.metrics,
                    validation_score=_objective_score(validation.metrics, objective),
                    rejection_code=None,
                )
            )
        except ExitPolicyInvalidPlanError:
            trials.append(
                ExitPolicyTrial(
                    trial_id=trial_id,
                    parameters=candidate,
                    status=ExitPolicyTrialStatus.REJECTED_INVALID_PLAN,
                    fold_results=(),
                    validation_metrics=None,
                    validation_score=None,
                    rejection_code="INVALID_NONPOSITIVE_STOP",
                )
            )

    evaluated = tuple(
        trial for trial in trials if trial.status is ExitPolicyTrialStatus.EVALUATED
    )
    if not evaluated:
        raise ValueError("no valid exit-policy candidate remains after evaluation")
    selected = min(
        evaluated,
        key=lambda trial: (-_required_score(trial), trial.trial_id),
    )
    baseline_trial = next(
        trial for trial in trials if trial.parameters.fingerprint == baseline.fingerprint
    )
    if baseline_trial.status is not ExitPolicyTrialStatus.EVALUATED:
        raise ValueError("baseline candidate is invalid for the frozen dataset")

    horizon = walk_forward.session_plan.config.label_horizon_sessions
    selected_holdout = evaluator.evaluate(
        selected.parameters,
        walk_forward.holdout_episode_indices,
        costs,
        label_horizon_sessions=horizon,
    )
    baseline_holdout = (
        selected_holdout
        if selected.trial_id == baseline_trial.trial_id
        else evaluator.evaluate(
            baseline_trial.parameters,
            walk_forward.holdout_episode_indices,
            costs,
            label_horizon_sessions=horizon,
        )
    )
    registry_seed = {
        "cost": costs.fingerprint,
        "dataset": evaluator.dataset.content_sha256,
        "experiment": experiment_version,
        "objective": objective.value,
        "plan": walk_forward.plan_sha256,
        "search": search_space.manifest_sha256,
    }
    return ExitPolicyTrialRegistry(
        registry_id=f"exit-registry-{_sha256_document(registry_seed)[:24]}",
        experiment_version=experiment_version,
        dataset_sha256=evaluator.dataset.content_sha256,
        search_space_sha256=search_space.manifest_sha256,
        walk_forward_sha256=walk_forward.plan_sha256,
        cost_model_sha256=costs.fingerprint,
        objective=objective,
        trials=tuple(trials),
        selected_trial_id=selected.trial_id,
        baseline_trial_id=baseline_trial.trial_id,
        selected_holdout=selected_holdout,
        baseline_holdout=baseline_holdout,
        created_at=created_at,
    )


def exit_policy_registry_to_json(registry: ExitPolicyTrialRegistry) -> str:
    """以稳定字段顺序输出完整试验登记，便于文件归档和摘要复核。"""

    return json.dumps(
        _registry_document(registry),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _evaluate_episode(
    episode: ExitPolicyEpisode,
    parameters: ExitPolicyParameters,
    costs: ExitPolicyCostModel,
    label_horizon_sessions: int,
) -> ExitPolicyOutcome:
    atr_stop = episode.entry_price - parameters.atr_stop_multiple * episode.atr_at_entry
    structure_stop = (
        episode.structure_low_at_entry
        - parameters.structure_buffer_atr * episode.atr_at_entry
    )
    initial_stop = min(atr_stop, structure_stop)
    if initial_stop <= 0 or initial_stop >= episode.entry_price:
        raise ExitPolicyInvalidPlanError("candidate creates a non-positive or invalid stop")
    risk = episode.entry_price - initial_stop
    take_profit = episode.entry_price + parameters.reward_to_risk * risk
    current_stop = initial_stop
    pending_barrier: ExitPolicyBarrier | None = None
    pending_observed_on: date | None = None
    pending_both = False
    blocked_sessions = 0
    warnings: list[str] = []
    bars = episode.future_bars[:label_horizon_sessions]

    for held, bar in enumerate(bars, start=1):
        barrier = pending_barrier
        barrier_observed_on = pending_observed_on
        both_touched = pending_both
        raw_price: Decimal | None = None
        if barrier is not None:
            raw_price = bar.open
        else:
            stop_touched = bar.low <= current_stop
            target_touched = bar.high >= take_profit
            both_touched = stop_touched and target_touched
            if stop_touched:
                barrier = (
                    ExitPolicyBarrier.TRAILING_STOP
                    if current_stop > initial_stop
                    else ExitPolicyBarrier.STOP_LOSS
                )
                raw_price = min(bar.open, current_stop)
                barrier_observed_on = bar.session_date
                if both_touched:
                    warnings.append("OHLC_ORDER_UNKNOWN_STOP_FIRST")
            elif target_touched:
                barrier = ExitPolicyBarrier.TAKE_PROFIT
                raw_price = max(bar.open, take_profit)
                barrier_observed_on = bar.session_date
            elif held >= parameters.maximum_holding_sessions:
                barrier = ExitPolicyBarrier.TIME
                raw_price = bar.close
                barrier_observed_on = bar.session_date
                warnings.append("DAILY_TIME_EXIT_AT_CLOSE")

        if barrier is not None and raw_price is not None:
            if barrier_observed_on is None:
                raise AssertionError("exit barrier is missing its observation date")
            if bar.suspended:
                pending_barrier = barrier
                pending_observed_on = barrier_observed_on
                pending_both = both_touched
                blocked_sessions += 1
                warnings.append("EXIT_BLOCKED_SUSPENDED")
            elif bar.lower_limit_locked:
                pending_barrier = barrier
                pending_observed_on = barrier_observed_on
                pending_both = both_touched
                blocked_sessions += 1
                warnings.append("EXIT_BLOCKED_LOWER_LIMIT_LOCKED")
            else:
                return _filled_outcome(
                    episode,
                    parameters,
                    costs,
                    initial_stop=initial_stop,
                    final_stop=current_stop,
                    take_profit=take_profit,
                    barrier=barrier,
                    barrier_date=barrier_observed_on,
                    raw_price=raw_price,
                    bar=bar,
                    holding_sessions=held,
                    both_touched=both_touched,
                    blocked_sessions=blocked_sessions,
                    warnings=warnings,
                )

        if pending_barrier is None and parameters.trailing_atr_multiple is not None:
            trailing_candidate = (
                bar.high - parameters.trailing_atr_multiple * episode.atr_at_entry
            )
            current_stop = max(current_stop, min(trailing_candidate, take_profit))

    terminal_bar = bars[-1]
    terminal_price = _net_sell_price(terminal_bar.close, terminal_bar, costs)
    pnl, trade_return = _net_trade_result(episode, terminal_price, costs)
    terminal_warnings = tuple(
        sorted(
            {
                *warnings,
                "TERMINAL_POSITION_MARKED_NOT_FILLED",
                *("EXIT_PENDING_AT_TERMINAL" for _ in (0,) if pending_barrier is not None),
            }
        )
    )
    return ExitPolicyOutcome(
        episode_id=episode.episode_id,
        entry_session_date=episode.entry_session_date,
        parameter_fingerprint=parameters.fingerprint,
        initial_stop_price=initial_stop,
        final_stop_price=current_stop,
        take_profit_price=take_profit,
        barrier=pending_barrier or ExitPolicyBarrier.TERMINAL_MARK,
        barrier_observed_on=pending_observed_on or terminal_bar.session_date,
        status=ExitPolicyOutcomeStatus.MARKED_OPEN,
        execution_on=None,
        raw_execution_price=None,
        net_execution_price=terminal_price,
        holding_sessions=len(bars),
        both_price_barriers_touched=pending_both,
        blocked_sessions=blocked_sessions,
        net_pnl_cny=pnl,
        net_return=trade_return,
        warning_codes=terminal_warnings,
    )


def _filled_outcome(
    episode: ExitPolicyEpisode,
    parameters: ExitPolicyParameters,
    costs: ExitPolicyCostModel,
    *,
    initial_stop: Decimal,
    final_stop: Decimal,
    take_profit: Decimal,
    barrier: ExitPolicyBarrier,
    barrier_date: date,
    raw_price: Decimal,
    bar: ExitEvaluationBar,
    holding_sessions: int,
    both_touched: bool,
    blocked_sessions: int,
    warnings: Sequence[str],
) -> ExitPolicyOutcome:
    net_price = _net_sell_price(raw_price, bar, costs)
    pnl, trade_return = _net_trade_result(episode, net_price, costs)
    return ExitPolicyOutcome(
        episode_id=episode.episode_id,
        entry_session_date=episode.entry_session_date,
        parameter_fingerprint=parameters.fingerprint,
        initial_stop_price=initial_stop,
        final_stop_price=final_stop,
        take_profit_price=take_profit,
        barrier=barrier,
        barrier_observed_on=barrier_date,
        status=ExitPolicyOutcomeStatus.FILLED,
        execution_on=bar.session_date,
        raw_execution_price=raw_price,
        net_execution_price=net_price,
        holding_sessions=holding_sessions,
        both_price_barriers_touched=both_touched,
        blocked_sessions=blocked_sessions,
        net_pnl_cny=pnl,
        net_return=trade_return,
        warning_codes=tuple(sorted(set(warnings))),
    )


def _net_sell_price(
    raw_price: Decimal,
    bar: ExitEvaluationBar,
    costs: ExitPolicyCostModel,
) -> Decimal:
    slipped = raw_price * (_ONE - costs.sell_slippage_bps / _BPS)
    return max(bar.lower_price_limit, min(slipped, bar.upper_price_limit))


def _net_trade_result(
    episode: ExitPolicyEpisode,
    net_sell_price: Decimal,
    costs: ExitPolicyCostModel,
) -> tuple[Decimal, Decimal]:
    entry_notional = episode.entry_price * episode.quantity
    exit_notional = net_sell_price * episode.quantity
    buy_variable_bps = costs.commission_bps + costs.transfer_fee_bps
    buy_fee = max(
        costs.minimum_commission_cny,
        entry_notional * buy_variable_bps / _BPS,
    )
    sell_fee = max(
        costs.minimum_commission_cny,
        exit_notional * (costs.commission_bps + costs.transfer_fee_bps) / _BPS,
    ) + exit_notional * costs.tax_bps / _BPS
    capital = entry_notional + buy_fee
    pnl = exit_notional - sell_fee - capital
    return pnl, pnl / capital


def _metrics(outcomes: tuple[ExitPolicyOutcome, ...]) -> ExitPolicyMetrics:
    if not outcomes:
        raise ValueError("outcomes must not be empty")
    equity = _ONE
    peak = _ONE
    maximum_drawdown = Decimal("0")
    # 同一入场日采用亏损优先的保守顺序，避免样本编号偶然改变逐笔回撤。
    return_path = tuple(
        sorted(
            outcomes,
            key=lambda outcome: (
                outcome.entry_session_date,
                outcome.net_return,
                outcome.episode_id,
            ),
        )
    )
    for outcome in return_path:
        equity *= _ONE + outcome.net_return
        peak = max(peak, equity)
        if peak > 0:
            maximum_drawdown = max(maximum_drawdown, (peak - equity) / peak)
    completed = tuple(
        outcome for outcome in outcomes if outcome.status is ExitPolicyOutcomeStatus.FILLED
    )
    wins = tuple(outcome for outcome in completed if outcome.net_pnl_cny > 0)
    losses = tuple(outcome for outcome in completed if outcome.net_pnl_cny < 0)
    gross_profit = sum((outcome.net_pnl_cny for outcome in wins), Decimal("0"))
    gross_loss = -sum((outcome.net_pnl_cny for outcome in losses), Decimal("0"))
    hit_rate = (
        Decimal(len(wins)) / Decimal(len(completed)) if completed else Decimal("0")
    )
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None
    return ExitPolicyMetrics(
        episode_count=len(outcomes),
        completed_count=len(completed),
        marked_open_count=len(outcomes) - len(completed),
        winning_count=len(wins),
        losing_count=len(losses),
        net_return=equity - _ONE,
        maximum_drawdown=maximum_drawdown,
        average_return=(
            sum((outcome.net_return for outcome in outcomes), Decimal("0"))
            / Decimal(len(outcomes))
        ),
        hit_rate=hit_rate,
        profit_factor=profit_factor,
        gross_profit_cny=gross_profit,
        gross_loss_cny=gross_loss,
        blocked_session_count=sum(outcome.blocked_sessions for outcome in outcomes),
    )


def _combine_evaluations(
    parameters: ExitPolicyParameters,
    fold_results: tuple[ExitPolicyFoldResult, ...],
) -> ExitPolicyEvaluation:
    outcomes = tuple(
        outcome for fold in fold_results for outcome in fold.evaluation.outcomes
    )
    indices = tuple(
        index
        for fold in fold_results
        for index in fold.evaluation.observation_indices
    )
    if len(indices) != len(set(indices)):
        raise ValueError("walk-forward validation episodes must not overlap")
    return ExitPolicyEvaluation(
        parameters=parameters,
        observation_indices=indices,
        metrics=_metrics(outcomes),
        outcomes=outcomes,
    )


def _objective_score(
    metrics: ExitPolicyMetrics,
    objective: ExitPolicyObjective,
) -> Decimal:
    if objective is ExitPolicyObjective.NET_RETURN:
        return metrics.net_return
    if objective is ExitPolicyObjective.HIT_RATE:
        return metrics.hit_rate
    if metrics.maximum_drawdown == 0:
        return metrics.net_return
    return metrics.net_return / metrics.maximum_drawdown


def _required_score(trial: ExitPolicyTrial) -> Decimal:
    if trial.validation_score is None:
        raise AssertionError("evaluated trial is missing validation_score")
    return trial.validation_score


def _validated_indices(indices: tuple[int, ...], size: int) -> tuple[int, ...]:
    if not indices:
        raise ValueError("observation_indices must not be empty")
    if indices != tuple(sorted(indices)) or len(set(indices)) != len(indices):
        raise ValueError("observation_indices must be strictly increasing and unique")
    if indices[0] < 0 or indices[-1] >= size:
        raise IndexError("observation index is outside the frozen dataset")
    return indices


def _sessions_at(sessions: tuple[date, ...], indices: tuple[int, ...]) -> tuple[date, ...]:
    return tuple(sessions[index] for index in indices)


def _episode_indices_for_sessions(
    indices_by_session: dict[date, list[int]],
    sessions: tuple[date, ...],
) -> tuple[int, ...]:
    return tuple(
        index for session in sessions for index in indices_by_session.get(session, ())
    )


def _dataset_document(dataset: ExitPolicyDataset) -> dict[str, object]:
    return {
        "dataset_id": dataset.dataset_id,
        "episodes": [_episode_document(episode) for episode in dataset.episodes],
        "frozen_at": dataset.frozen_at.isoformat(),
        "schema_version": dataset.schema_version,
    }


def _episode_document(episode: ExitPolicyEpisode) -> dict[str, object]:
    return {
        "atr_at_entry": str(episode.atr_at_entry),
        "entry_at": episode.entry_at.isoformat(),
        "entry_price": str(episode.entry_price),
        "entry_session_date": episode.entry_session_date.isoformat(),
        "episode_id": episode.episode_id,
        "features_known_at": episode.features_known_at.isoformat(),
        "future_bars": [_bar_document(bar) for bar in episode.future_bars],
        "quantity": episode.quantity,
        "source_revisions": [list(item) for item in episode.source_revisions],
        "structure_low_at_entry": str(episode.structure_low_at_entry),
        "symbol": episode.symbol,
    }


def _bar_document(bar: ExitEvaluationBar) -> dict[str, object]:
    return {
        "close": str(bar.close),
        "completed_at": bar.completed_at.isoformat(),
        "high": str(bar.high),
        "low": str(bar.low),
        "lower_price_limit": str(bar.lower_price_limit),
        "open": str(bar.open),
        "session_date": bar.session_date.isoformat(),
        "source_id": bar.source_id,
        "source_revision": bar.source_revision,
        "suspended": bar.suspended,
        "upper_price_limit": str(bar.upper_price_limit),
        "volume_shares": bar.volume_shares,
    }


def _cost_document(costs: ExitPolicyCostModel) -> dict[str, object]:
    return {
        "commission_bps": str(costs.commission_bps),
        "minimum_commission_cny": str(costs.minimum_commission_cny),
        "sell_slippage_bps": str(costs.sell_slippage_bps),
        "tax_bps": str(costs.tax_bps),
        "transfer_fee_bps": str(costs.transfer_fee_bps),
    }


def _parameters_document(parameters: ExitPolicyParameters) -> dict[str, object]:
    return {
        "atr_stop_multiple": str(parameters.atr_stop_multiple),
        "maximum_holding_sessions": parameters.maximum_holding_sessions,
        "policy_version": parameters.policy_version,
        "reward_to_risk": str(parameters.reward_to_risk),
        "structure_buffer_atr": str(parameters.structure_buffer_atr),
        "trailing_atr_multiple": (
            None
            if parameters.trailing_atr_multiple is None
            else str(parameters.trailing_atr_multiple)
        ),
    }


def _walk_forward_document(plan: ExitPolicyWalkForwardPlan) -> dict[str, object]:
    config = plan.session_plan.config
    return {
        "dataset_sha256": plan.dataset_sha256,
        "config": {
            "embargo_size": config.embargo_size,
            "initial_train_size": config.initial_train_size,
            "label_horizon_sessions": config.label_horizon_sessions,
            "minimum_folds": config.minimum_folds,
            "purge_size": config.purge_size,
            "step_size": config.step_size,
            "test_size": config.test_size,
            "validation_size": config.validation_size,
        },
        "folds": [
            {
                "embargo_sessions": [value.isoformat() for value in fold.embargo_sessions],
                "fold_id": fold.fold_id,
                "purge_sessions": [value.isoformat() for value in fold.purge_sessions],
                "train_sessions": [value.isoformat() for value in fold.train_sessions],
                "validation_episode_indices": list(fold.validation_episode_indices),
                "validation_sessions": [
                    value.isoformat() for value in fold.validation_sessions
                ],
            }
            for fold in plan.folds
        ],
        "holdout_episode_indices": list(plan.holdout_episode_indices),
        "holdout_sessions": [value.isoformat() for value in plan.holdout_sessions],
        "minimum_holdout_episodes": plan.minimum_holdout_episodes,
        "minimum_validation_episodes": plan.minimum_validation_episodes,
    }


def _metrics_document(metrics: ExitPolicyMetrics) -> dict[str, object]:
    return {
        "average_return": str(metrics.average_return),
        "blocked_session_count": metrics.blocked_session_count,
        "completed_count": metrics.completed_count,
        "episode_count": metrics.episode_count,
        "gross_loss_cny": str(metrics.gross_loss_cny),
        "gross_profit_cny": str(metrics.gross_profit_cny),
        "hit_rate": str(metrics.hit_rate),
        "losing_count": metrics.losing_count,
        "marked_open_count": metrics.marked_open_count,
        "maximum_drawdown": str(metrics.maximum_drawdown),
        "net_return": str(metrics.net_return),
        "profit_factor": (
            None if metrics.profit_factor is None else str(metrics.profit_factor)
        ),
        "winning_count": metrics.winning_count,
    }


def _outcome_document(outcome: ExitPolicyOutcome) -> dict[str, object]:
    return {
        "barrier": outcome.barrier.value,
        "barrier_observed_on": outcome.barrier_observed_on.isoformat(),
        "blocked_sessions": outcome.blocked_sessions,
        "both_price_barriers_touched": outcome.both_price_barriers_touched,
        "episode_id": outcome.episode_id,
        "entry_session_date": outcome.entry_session_date.isoformat(),
        "execution_on": (
            None if outcome.execution_on is None else outcome.execution_on.isoformat()
        ),
        "final_stop_price": str(outcome.final_stop_price),
        "holding_sessions": outcome.holding_sessions,
        "initial_stop_price": str(outcome.initial_stop_price),
        "net_execution_price": str(outcome.net_execution_price),
        "net_pnl_cny": str(outcome.net_pnl_cny),
        "net_return": str(outcome.net_return),
        "parameter_fingerprint": outcome.parameter_fingerprint,
        "raw_execution_price": (
            None
            if outcome.raw_execution_price is None
            else str(outcome.raw_execution_price)
        ),
        "status": outcome.status.value,
        "take_profit_price": str(outcome.take_profit_price),
        "warning_codes": list(outcome.warning_codes),
    }


def _evaluation_document(evaluation: ExitPolicyEvaluation) -> dict[str, object]:
    return {
        "metrics": _metrics_document(evaluation.metrics),
        "observation_indices": list(evaluation.observation_indices),
        "outcomes": [_outcome_document(outcome) for outcome in evaluation.outcomes],
        "parameter_fingerprint": evaluation.parameters.fingerprint,
    }


def _registry_document(registry: ExitPolicyTrialRegistry) -> dict[str, object]:
    return {
        "baseline_holdout": _evaluation_document(registry.baseline_holdout),
        "baseline_trial_id": registry.baseline_trial_id,
        "cost_model_sha256": registry.cost_model_sha256,
        "created_at": registry.created_at.isoformat(),
        "dataset_sha256": registry.dataset_sha256,
        "experiment_version": registry.experiment_version,
        "objective": registry.objective.value,
        "promotion_authorized": registry.promotion_authorized,
        "registry_id": registry.registry_id,
        "research_only": registry.research_only,
        "search_space_sha256": registry.search_space_sha256,
        "selected_holdout": _evaluation_document(registry.selected_holdout),
        "selected_trial_id": registry.selected_trial_id,
        "trials": [
            {
                "folds": [
                    {
                        "evaluation": _evaluation_document(fold.evaluation),
                        "fold_id": fold.fold_id,
                    }
                    for fold in trial.fold_results
                ],
                "parameter_fingerprint": trial.parameters.fingerprint,
                "parameters": _parameters_document(trial.parameters),
                "rejection_code": trial.rejection_code,
                "status": trial.status.value,
                "trial_id": trial.trial_id,
                "validation_metrics": (
                    None
                    if trial.validation_metrics is None
                    else _metrics_document(trial.validation_metrics)
                ),
                "validation_score": (
                    None
                    if trial.validation_score is None
                    else str(trial.validation_score)
                ),
            }
            for trial in registry.trials
        ],
        "walk_forward_sha256": registry.walk_forward_sha256,
    }


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if _SAFE_IDENTIFIER.fullmatch(normalized) is None:
        raise ValueError(f"{name} must be a safe non-empty identifier")
    return normalized


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _finite_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _positive_decimal(value: object, name: str) -> Decimal:
    value = _finite_decimal(value, name)
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _nonnegative_decimal(value: object, name: str) -> Decimal:
    value = _finite_decimal(value, name)
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _aware_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _sha256_document(document: object) -> str:
    encoded = json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "ExitEvaluationBar",
    "ExitPolicyBarrier",
    "ExitPolicyCostModel",
    "ExitPolicyDataset",
    "ExitPolicyEpisode",
    "ExitPolicyEvaluation",
    "ExitPolicyEvaluator",
    "ExitPolicyEvaluatorProtocol",
    "ExitPolicyFoldResult",
    "ExitPolicyInvalidPlanError",
    "ExitPolicyMetrics",
    "ExitPolicyObjective",
    "ExitPolicyOutcome",
    "ExitPolicyOutcomeStatus",
    "ExitPolicyTrial",
    "ExitPolicyTrialRegistry",
    "ExitPolicyTrialStatus",
    "ExitPolicyWalkForwardFold",
    "ExitPolicyWalkForwardPlan",
    "build_exit_policy_walk_forward_plan",
    "exit_policy_registry_to_json",
    "run_exit_policy_walk_forward_experiment",
]
