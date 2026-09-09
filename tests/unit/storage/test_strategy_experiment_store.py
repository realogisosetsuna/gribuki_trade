from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.storage.strategy_experiments import (
    SQLiteStrategyExperimentStore,
    StrategyExperimentCollisionError,
)
from gribuki_trade.strategy_lab.experiments import (
    CostScenario,
    DataManifest,
    PerformanceMetrics,
    StrategyExperiment,
    StrategyManifest,
    StrategyWeights,
    WalkForwardConfig,
    WeightConstraints,
    build_walk_forward_plan,
    run_walk_forward_experiment,
)


class _Evaluator:
    def evaluate(
        self,
        weights: StrategyWeights,
        observation_indices: tuple[int, ...],
        cost_scenario: CostScenario,
    ) -> PerformanceMetrics:
        value = dict(weights.technical)["trend"] - cost_scenario.slippage_bps / Decimal("10000")
        return PerformanceMetrics(
            net_return=value,
            annualized_return=value,
            annualized_volatility=Decimal("0.2"),
            sharpe=value / Decimal("0.2"),
            max_drawdown=Decimal("0.1"),
            turnover=Decimal("1"),
            trade_count=len(observation_indices),
            hit_rate=Decimal("0.5"),
            family_contributions=(("trend", value),),
        )


def _build_experiment() -> StrategyExperiment:
    timestamps = tuple(date(2026, 1, 1) + timedelta(days=index) for index in range(40))
    plan = build_walk_forward_plan(
        timestamps,
        WalkForwardConfig(
            initial_train_size=10,
            validation_size=5,
            test_size=5,
            step_size=5,
            purge_size=2,
            embargo_size=2,
            label_horizon_sessions=2,
            minimum_folds=2,
        ),
    )
    baseline = StrategyWeights(
        technical=(("trend", Decimal("0.7")),),
        macro=Decimal("0.3"),
    )
    return run_walk_forward_experiment(
        data_manifest=DataManifest(
            dataset_id="dataset",
            content_sha256="b" * 64,
            schema_version="bars@1",
            observation_count=len(timestamps),
            observed_start=timestamps[0],
            observed_end=timestamps[-1],
            frozen_at=datetime(2026, 2, 10, tzinfo=UTC),
            feature_ids=("close",),
            label_id="forward-return",
        ),
        strategy_manifest=StrategyManifest(
            strategy_version="strategy@1",
            code_revision="deadbeef",
            parameters=(("horizon", "5"),),
        ),
        plan=plan,
        constraints=WeightConstraints(
            technical_family_ids=("trend",),
            max_technical_family_weight=Decimal("0.8"),
            max_macro_weight=Decimal("0.4"),
        ),
        candidates=(baseline,),
        baseline=baseline,
        cost_scenarios=(
            CostScenario(
                "base",
                commission_bps=Decimal("2"),
                slippage_bps=Decimal("3"),
                tax_bps=Decimal("5"),
            ),
        ),
        evaluator=_Evaluator(),
        created_at=datetime(2026, 2, 11, tzinfo=UTC),
    )


def test_store_round_trip_idempotence_listing_and_collision(tmp_path: Path) -> None:
    database = tmp_path.joinpath("experiments.sqlite3")
    experiment = _build_experiment()
    with SQLiteStrategyExperimentStore(database) as store:
        assert store.append(experiment) is True
        assert store.append(experiment) is False
        stored = store.get(experiment.experiment_id)
        assert stored is not None
        assert stored.trial_count == experiment.trial_count
        assert stored.data_manifest_sha256 == experiment.data_manifest.manifest_sha256
        assert stored.payload_document()["research_only"] is True
        assert store.list_experiments(strategy_version="strategy@1") == (stored,)

        changed = replace(experiment, warnings=(*experiment.warnings, "CHANGED"))
        with pytest.raises(StrategyExperimentCollisionError):
            store.append(changed)


def test_store_rejects_use_after_close(tmp_path: Path) -> None:
    database = tmp_path.joinpath("experiments.sqlite3")
    store = SQLiteStrategyExperimentStore(database)
    store.close()
    with pytest.raises(RuntimeError, match="closed"):
        store.get("strategy-exp-any")
