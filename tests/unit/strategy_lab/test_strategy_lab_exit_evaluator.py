from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.strategy_lab.exit_evaluator import (
    ExitEvaluationBar,
    ExitPolicyBarrier,
    ExitPolicyCostModel,
    ExitPolicyDataset,
    ExitPolicyEpisode,
    ExitPolicyEvaluator,
    ExitPolicyObjective,
    ExitPolicyOutcomeStatus,
    ExitPolicyTrialStatus,
    build_exit_policy_walk_forward_plan,
    exit_policy_registry_to_json,
    run_exit_policy_walk_forward_experiment,
)
from gribuki_trade.strategy_lab.exit_policies import (
    ExitPolicyParameters,
    ExitPolicySearchSpace,
)
from gribuki_trade.strategy_lab.experiments import WalkForwardConfig


def _completed_at(session_date: date) -> datetime:
    return datetime.combine(session_date, datetime.min.time(), tzinfo=UTC) + timedelta(
        hours=7
    )


def _bar(
    session_date: date,
    *,
    open_: str = "10.00",
    high: str = "10.50",
    low: str = "9.50",
    close: str = "10.10",
    lower: str = "8.00",
    upper: str = "12.00",
    suspended: bool = False,
    volume: int = 100_000,
) -> ExitEvaluationBar:
    return ExitEvaluationBar(
        session_date=session_date,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume_shares=volume,
        suspended=suspended,
        lower_price_limit=Decimal(lower),
        upper_price_limit=Decimal(upper),
        completed_at=_completed_at(session_date),
        source_id="daily-bars",
        source_revision=f"r-{session_date.isoformat()}",
    )


def _episode(
    episode_id: str,
    entry_session: date,
    *,
    bars: tuple[ExitEvaluationBar, ...] | None = None,
    entry_price: str = "10.00",
    atr: str = "1.00",
    structure_low: str = "9.50",
) -> ExitPolicyEpisode:
    entry_at = datetime.combine(
        entry_session,
        datetime.min.time(),
        tzinfo=UTC,
    ) + timedelta(hours=2)
    default_bars = (
        _bar(entry_session + timedelta(days=1)),
        _bar(entry_session + timedelta(days=2)),
        _bar(entry_session + timedelta(days=3)),
    )
    return ExitPolicyEpisode(
        episode_id=episode_id,
        symbol="600000.SH",
        entry_at=entry_at,
        entry_session_date=entry_session,
        entry_price=Decimal(entry_price),
        quantity=1_000,
        atr_at_entry=Decimal(atr),
        structure_low_at_entry=Decimal(structure_low),
        features_known_at=entry_at - timedelta(minutes=1),
        future_bars=default_bars if bars is None else bars,
        source_revisions=(("entry-fills", "fills-r1"),),
    )


def _dataset(episodes: tuple[ExitPolicyEpisode, ...]) -> ExitPolicyDataset:
    latest = max(bar.completed_at for episode in episodes for bar in episode.future_bars)
    return ExitPolicyDataset(
        dataset_id="exit-dataset@test",
        schema_version="exit-episodes@1",
        episodes=episodes,
        frozen_at=latest + timedelta(days=1),
    )


def _parameters(**changes: object) -> ExitPolicyParameters:
    values: dict[str, object] = {
        "policy_version": "exit-policy@test",
        "atr_stop_multiple": Decimal("1.0"),
        "structure_buffer_atr": Decimal("0.1"),
        "reward_to_risk": Decimal("1.0"),
        "maximum_holding_sessions": 2,
        "trailing_atr_multiple": None,
    }
    values.update(changes)
    return ExitPolicyParameters(**values)  # type: ignore[arg-type]


def _costs(**changes: object) -> ExitPolicyCostModel:
    values: dict[str, object] = {
        "commission_bps": Decimal("0"),
        "tax_bps": Decimal("0"),
        "transfer_fee_bps": Decimal("0"),
        "sell_slippage_bps": Decimal("0"),
        "minimum_commission_cny": Decimal("0"),
    }
    values.update(changes)
    return ExitPolicyCostModel(**values)  # type: ignore[arg-type]


def _search_space() -> ExitPolicySearchSpace:
    return ExitPolicySearchSpace(
        search_space_id="exit-grid@test",
        policy_version="exit-policy@test",
        atr_stop_multiples=(Decimal("1.0"), Decimal("20.0")),
        structure_buffer_atr_values=(Decimal("0.1"),),
        reward_to_risk_values=(Decimal("1.0"),),
        maximum_holding_sessions_values=(2,),
        trailing_atr_multiples=(None,),
        max_candidates=2,
    )


def _weekdays(start: date, count: int) -> tuple[date, ...]:
    values: list[date] = []
    cursor = start
    while len(values) < count:
        if cursor.weekday() < 5:
            values.append(cursor)
        cursor += timedelta(days=1)
    return tuple(values)


def _walk_forward_dataset(*, episodes_per_session: int = 1) -> ExitPolicyDataset:
    episodes: list[ExitPolicyEpisode] = []
    for session_index, entry_session in enumerate(_weekdays(date(2026, 6, 1), 16)):
        future = _weekdays(entry_session + timedelta(days=1), 3)
        bars = (
            _bar(future[0], high="11.20", low="9.60", close="10.80"),
            _bar(future[1], high="11.30", low="9.80", close="11.00"),
            _bar(future[2], high="11.10", low="9.70", close="10.70"),
        )
        for duplicate in range(episodes_per_session):
            episodes.append(
                _episode(
                    f"episode-{session_index:02d}-{duplicate}",
                    entry_session,
                    bars=bars,
                )
            )
    return _dataset(tuple(episodes))


def _walk_config() -> WalkForwardConfig:
    return WalkForwardConfig(
        initial_train_size=4,
        validation_size=2,
        test_size=2,
        step_size=2,
        purge_size=2,
        embargo_size=2,
        label_horizon_sessions=2,
        minimum_folds=2,
    )


def test_episode_enforces_t_plus_one_at_the_dataset_boundary() -> None:
    entry_session = date(2026, 8, 13)

    with pytest.raises(ValueError, match=r"after the entry session for T\+1"):
        _episode(
            "same-session",
            entry_session,
            bars=(_bar(entry_session),),
        )


def test_same_bar_stop_and_target_uses_conservative_stop_first() -> None:
    entry_session = date(2026, 8, 13)
    episode = _episode(
        "ambiguous-ohlc",
        entry_session,
        bars=(
            _bar(
                entry_session + timedelta(days=1),
                open_="10.00",
                high="11.20",
                low="8.80",
                close="10.50",
            ),
            _bar(entry_session + timedelta(days=2)),
        ),
    )

    result = ExitPolicyEvaluator(_dataset((episode,))).evaluate(
        _parameters(),
        (0,),
        _costs(),
        label_horizon_sessions=2,
    )

    outcome = result.outcomes[0]
    assert outcome.barrier is ExitPolicyBarrier.STOP_LOSS
    assert outcome.both_price_barriers_touched is True
    assert outcome.raw_execution_price == Decimal("9.00")
    assert outcome.net_return == Decimal("-0.1")
    assert "OHLC_ORDER_UNKNOWN_STOP_FIRST" in outcome.warning_codes


def test_lower_limit_lock_blocks_sell_then_next_session_open_fills() -> None:
    entry_session = date(2026, 8, 12)
    first = entry_session + timedelta(days=1)
    second = entry_session + timedelta(days=2)
    episode = _episode(
        "lower-limit-retry",
        entry_session,
        bars=(
            _bar(
                first,
                open_="8.00",
                high="8.00",
                low="8.00",
                close="8.00",
                lower="8.00",
                upper="12.00",
                volume=0,
            ),
            _bar(
                second,
                open_="8.30",
                high="8.60",
                low="8.10",
                close="8.40",
            ),
        ),
    )

    outcome = ExitPolicyEvaluator(_dataset((episode,))).evaluate(
        _parameters(),
        (0,),
        _costs(),
        label_horizon_sessions=2,
    ).outcomes[0]

    assert outcome.status is ExitPolicyOutcomeStatus.FILLED
    assert outcome.barrier is ExitPolicyBarrier.STOP_LOSS
    assert outcome.barrier_observed_on == first
    assert outcome.execution_on == second
    assert outcome.raw_execution_price == Decimal("8.30")
    assert outcome.blocked_sessions == 1
    assert "EXIT_BLOCKED_LOWER_LIMIT_LOCKED" in outcome.warning_codes


def test_trailing_stop_uses_only_the_previous_completed_bar() -> None:
    entry_session = date(2026, 8, 11)
    episode = _episode(
        "trailing-stop",
        entry_session,
        bars=(
            _bar(
                entry_session + timedelta(days=1),
                open_="10.00",
                high="10.80",
                low="9.50",
                close="10.60",
            ),
            _bar(
                entry_session + timedelta(days=2),
                open_="10.60",
                high="10.70",
                low="10.20",
                close="10.40",
            ),
        ),
    )

    outcome = ExitPolicyEvaluator(_dataset((episode,))).evaluate(
        _parameters(trailing_atr_multiple=Decimal("0.5")),
        (0,),
        _costs(),
        label_horizon_sessions=2,
    ).outcomes[0]

    assert outcome.barrier is ExitPolicyBarrier.TRAILING_STOP
    assert outcome.barrier_observed_on == entry_session + timedelta(days=2)
    assert outcome.final_stop_price == Decimal("10.30")
    assert outcome.raw_execution_price == Decimal("10.30")


def test_metrics_report_compounded_return_drawdown_hit_rate_and_profit_factor() -> None:
    first_session = date(2026, 8, 10)
    second_session = date(2026, 8, 11)
    winner = _episode(
        "winner",
        first_session,
        bars=(
            _bar(
                first_session + timedelta(days=1),
                high="11.20",
                low="9.50",
                close="11.00",
            ),
            _bar(first_session + timedelta(days=2)),
        ),
    )
    loser = _episode(
        "loser",
        second_session,
        bars=(
            _bar(
                second_session + timedelta(days=1),
                high="10.50",
                low="8.90",
                close="9.00",
            ),
            _bar(second_session + timedelta(days=2)),
        ),
    )

    metrics = ExitPolicyEvaluator(_dataset((winner, loser))).evaluate(
        _parameters(),
        (0, 1),
        _costs(),
        label_horizon_sessions=2,
    ).metrics

    assert metrics.net_return == Decimal("-0.01")
    assert metrics.maximum_drawdown == Decimal("0.1")
    assert metrics.average_return == Decimal("0.0")
    assert metrics.hit_rate == Decimal("0.5")
    assert metrics.profit_factor == Decimal("1")
    assert metrics.gross_profit_cny == metrics.gross_loss_cny == Decimal("1000")


def test_fees_slippage_and_terminal_marks_are_explicit_in_metrics() -> None:
    entry_session = date(2026, 8, 10)
    bars = (
        _bar(
            entry_session + timedelta(days=1),
            open_="10.00",
            high="10.40",
            low="9.60",
            close="10.20",
        ),
        _bar(
            entry_session + timedelta(days=2),
            open_="10.20",
            high="10.40",
            low="9.80",
            close="10.20",
            suspended=True,
            volume=0,
        ),
    )
    evaluator = ExitPolicyEvaluator(
        _dataset((_episode("terminal", entry_session, bars=bars),))
    )
    outcome = evaluator.evaluate(
        _parameters(maximum_holding_sessions=2),
        (0,),
        _costs(
            commission_bps=Decimal("3"),
            tax_bps=Decimal("5"),
            sell_slippage_bps=Decimal("10"),
            minimum_commission_cny=Decimal("5"),
        ),
        label_horizon_sessions=2,
    )

    assert outcome.outcomes[0].status is ExitPolicyOutcomeStatus.MARKED_OPEN
    assert outcome.outcomes[0].barrier is ExitPolicyBarrier.TIME
    assert outcome.outcomes[0].net_execution_price == Decimal("10.18980")
    assert outcome.metrics.completed_count == 0
    assert outcome.metrics.marked_open_count == 1
    assert outcome.metrics.profit_factor is None
    assert "TERMINAL_POSITION_MARKED_NOT_FILLED" in outcome.outcomes[0].warning_codes
    assert "EXIT_PENDING_AT_TERMINAL" in outcome.outcomes[0].warning_codes


def test_evaluator_rejects_right_censored_future_bar_windows() -> None:
    entry_session = date(2026, 8, 10)
    episode = _episode(
        "right-censored",
        entry_session,
        bars=(_bar(entry_session + timedelta(days=1)),),
    )

    with pytest.raises(ValueError, match="shorter than label horizon"):
        ExitPolicyEvaluator(_dataset((episode,))).evaluate(
            _parameters(),
            (0,),
            _costs(),
            label_horizon_sessions=2,
        )


def test_walk_forward_splits_by_session_and_keeps_same_day_episodes_together() -> None:
    dataset = _walk_forward_dataset(episodes_per_session=2)
    plan = build_exit_policy_walk_forward_plan(
        dataset,
        _walk_config(),
        minimum_validation_episodes=4,
        minimum_holdout_episodes=4,
    )

    assert len(plan.folds) >= 2
    assert len(plan.holdout_episode_indices) == 4
    for fold in plan.folds:
        assert len(fold.validation_episode_indices) == 4
        observed_sessions = {
            dataset.episodes[index].entry_session_date
            for index in fold.validation_episode_indices
        }
        assert observed_sessions == set(fold.validation_sessions)
        assert not observed_sessions.intersection(plan.holdout_sessions)
    assert len(plan.plan_sha256) == 64


def test_walk_forward_rejects_insufficient_episode_counts() -> None:
    dataset = _walk_forward_dataset()

    with pytest.raises(ValueError, match="minimum_validation_episodes"):
        build_exit_policy_walk_forward_plan(
            dataset,
            _walk_config(),
            minimum_validation_episodes=3,
            minimum_holdout_episodes=1,
        )


class _RecordingEvaluator:
    def __init__(self, delegate: ExitPolicyEvaluator) -> None:
        self._delegate = delegate
        self.calls: list[tuple[str, tuple[int, ...]]] = []

    @property
    def dataset(self) -> ExitPolicyDataset:
        return self._delegate.dataset

    def evaluate(
        self,
        parameters: ExitPolicyParameters,
        observation_indices: tuple[int, ...],
        costs: ExitPolicyCostModel,
        *,
        label_horizon_sessions: int,
    ):
        self.calls.append((parameters.fingerprint, observation_indices))
        return self._delegate.evaluate(
            parameters,
            observation_indices,
            costs,
            label_horizon_sessions=label_horizon_sessions,
        )


def test_experiment_registers_every_trial_and_opens_holdout_only_after_selection() -> None:
    dataset = _walk_forward_dataset()
    plan = build_exit_policy_walk_forward_plan(
        dataset,
        _walk_config(),
        minimum_validation_episodes=2,
        minimum_holdout_episodes=2,
    )
    recording = _RecordingEvaluator(ExitPolicyEvaluator(dataset))
    baseline = _parameters()
    created_at = datetime(2026, 8, 14, 10, 0, tzinfo=UTC)

    registry = run_exit_policy_walk_forward_experiment(
        evaluator=recording,
        search_space=_search_space(),
        baseline=baseline,
        walk_forward=plan,
        costs=_costs(),
        objective=ExitPolicyObjective.NET_RETURN,
        created_at=created_at,
    )

    assert len(registry.trials) == _search_space().candidate_count == 2
    statuses = {trial.status for trial in registry.trials}
    assert statuses == {
        ExitPolicyTrialStatus.EVALUATED,
        ExitPolicyTrialStatus.REJECTED_INVALID_PLAN,
    }
    holdout = plan.holdout_episode_indices
    holdout_calls = [call for call in recording.calls if call[1] == holdout]
    assert len(holdout_calls) == 1
    first_holdout_position = recording.calls.index(holdout_calls[0])
    assert all(indices != holdout for _, indices in recording.calls[:first_holdout_position])
    assert registry.selected_trial_id == registry.baseline_trial_id
    assert registry.research_only is True
    assert registry.promotion_authorized is False
    assert len(registry.registry_sha256) == 64

    document = exit_policy_registry_to_json(registry)
    assert '"promotion_authorized":false' in document
    assert '"REJECTED_INVALID_PLAN"' in document
    assert '"atr_stop_multiple":"1.0"' in document
    assert registry.registry_sha256 == replace(registry).registry_sha256


def test_experiment_rejects_walk_forward_plan_from_another_dataset() -> None:
    dataset = _walk_forward_dataset()
    plan = build_exit_policy_walk_forward_plan(
        dataset,
        _walk_config(),
        minimum_validation_episodes=2,
        minimum_holdout_episodes=2,
    )
    changed = replace(
        dataset,
        episodes=(
            replace(dataset.episodes[0], quantity=2_000),
            *dataset.episodes[1:],
        ),
    )

    with pytest.raises(ValueError, match="different frozen dataset"):
        run_exit_policy_walk_forward_experiment(
            evaluator=ExitPolicyEvaluator(changed),
            search_space=_search_space(),
            baseline=_parameters(),
            walk_forward=plan,
            costs=_costs(),
            objective=ExitPolicyObjective.NET_RETURN,
            created_at=datetime(2026, 8, 14, 10, 0, tzinfo=UTC),
        )


def test_dataset_digest_changes_when_frozen_market_evidence_changes() -> None:
    entry_session = date(2026, 8, 10)
    first = _dataset((_episode("digest", entry_session),))
    changed_episode = replace(
        first.episodes[0],
        future_bars=(
            replace(first.episodes[0].future_bars[0], close=Decimal("10.20")),
            *first.episodes[0].future_bars[1:],
        ),
    )
    second = _dataset((changed_episode,))

    assert first.content_sha256 != second.content_sha256
