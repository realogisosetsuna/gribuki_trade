"""冻结退出策略实验入口的端到端离线测试。"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pytest

from gribuki_trade.strategy_lab.exit_experiment_io import (
    load_exit_policy_dataset,
    load_frozen_exit_policy_experiment,
    run_frozen_exit_policy_experiment,
)


def test_frozen_dataset_spec_walk_forward_and_atomic_artifact(tmp_path: Path) -> None:
    dataset_path = tmp_path / "dataset.json"
    specification_path = tmp_path / "specification.json"
    destination = tmp_path / "results" / "registry.json"
    frozen_at = _write_dataset(dataset_path)
    dataset, dataset_file_sha256 = load_exit_policy_dataset(dataset_path)
    _write_specification(specification_path, dataset.content_sha256)

    frozen = load_frozen_exit_policy_experiment(dataset_path, specification_path)
    assert frozen.dataset_file_sha256 == dataset_file_sha256
    assert frozen.dataset.content_sha256 == dataset.content_sha256
    assert frozen.search_space.candidate_count == 8

    artifact = run_frozen_exit_policy_experiment(
        dataset_path,
        specification_path,
        destination,
        created_at=frozen_at + timedelta(days=1),
    )
    assert artifact.destination == destination.resolve()
    assert artifact.registry.research_only is True
    assert artifact.registry.promotion_authorized is False
    assert artifact.registry.selected_trial_id
    assert len(artifact.registry.trials) == 8
    document = json.loads(destination.read_text(encoding="utf-8"))
    assert document["artifact_schema_version"] == "exit-policy-experiment-artifact@1"
    assert document["dataset_file_sha256"] == dataset_file_sha256
    assert document["dataset_content_sha256"] == dataset.content_sha256
    assert document["registry_sha256"] == artifact.registry.registry_sha256
    assert document["research_only"] is True
    assert document["promotion_authorized"] is False
    assert document["execution_authority"] is False

    with pytest.raises(FileExistsError, match="already exists"):
        run_frozen_exit_policy_experiment(
            dataset_path,
            specification_path,
            destination,
            created_at=frozen_at + timedelta(days=2),
        )


def test_dataset_content_change_is_rejected_by_pre_registered_spec(tmp_path: Path) -> None:
    dataset_path = tmp_path / "dataset.json"
    specification_path = tmp_path / "specification.json"
    _write_dataset(dataset_path)
    dataset, _ = load_exit_policy_dataset(dataset_path)
    _write_specification(specification_path, dataset.content_sha256)

    document = json.loads(dataset_path.read_text(encoding="utf-8"))
    document["episodes"][0]["entry_price"] = "10.01"
    dataset_path.write_text(
        json.dumps(document, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="content hash"):
        load_frozen_exit_policy_experiment(dataset_path, specification_path)


def test_duplicate_json_key_and_symlink_inputs_fail_closed(tmp_path: Path) -> None:
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"dataset_id":"a","dataset_id":"b"}', encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate JSON key"):
        load_exit_policy_dataset(duplicate)

    target = tmp_path / "target.json"
    _write_dataset(target)
    linked = tmp_path / "linked.json"
    try:
        linked.symlink_to(target)
    except OSError:
        pytest.skip("当前测试账户没有创建符号链接的权限")
    with pytest.raises(ValueError, match="non-symlink"):
        load_exit_policy_dataset(linked)


def _write_dataset(path: Path) -> datetime:
    sessions = _business_sessions(date(2026, 1, 5), 20)
    episodes: list[dict[str, object]] = []
    latest = datetime.min.replace(tzinfo=UTC)
    for index, entry_session in enumerate(sessions[:16]):
        entry_price = 10 + index / 100
        entry_at = datetime.combine(entry_session, time(6, 0), tzinfo=UTC)
        bars: list[dict[str, object]] = []
        for future_session in sessions[index + 1 : index + 3]:
            completed = datetime.combine(future_session, time(7, 1), tzinfo=UTC)
            latest = max(latest, completed)
            bars.append(
                {
                    "close": f"{entry_price + 0.20:.2f}",
                    "completed_at": completed.isoformat(),
                    "high": f"{entry_price + 0.40:.2f}",
                    "low": f"{entry_price - 0.10:.2f}",
                    "lower_price_limit": "8.00",
                    "open": f"{entry_price + 0.10:.2f}",
                    "session_date": future_session.isoformat(),
                    "source_id": "fixture.daily",
                    "source_revision": f"rev-{future_session.isoformat()}",
                    "suspended": False,
                    "upper_price_limit": "13.00",
                    "volume_shares": 1_000_000,
                }
            )
        episodes.append(
            {
                "atr_at_entry": "0.30",
                "entry_at": entry_at.isoformat(),
                "entry_price": f"{entry_price:.2f}",
                "entry_session_date": entry_session.isoformat(),
                "episode_id": f"episode-{index:03d}",
                "features_known_at": (entry_at - timedelta(minutes=1)).isoformat(),
                "future_bars": bars,
                "quantity": 100,
                "source_revisions": [["fixture.daily", "rev-frozen-1"]],
                "structure_low_at_entry": f"{entry_price - 0.70:.2f}",
                "symbol": "600000.SH",
            }
        )
    frozen_at = latest + timedelta(seconds=1)
    path.write_text(
        json.dumps(
            {
                "dataset_id": "fixture-exit-dataset",
                "episodes": episodes,
                "frozen_at": frozen_at.isoformat(),
                "schema_version": "exit-policy-dataset@1",
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return frozen_at


def _write_specification(path: Path, dataset_content_sha256: str) -> None:
    path.write_text(
        json.dumps(
            {
                "baseline": {
                    "atr_stop_multiple": "1.5",
                    "maximum_holding_sessions": 2,
                    "policy_version": "fixture-exit-policy@1",
                    "reward_to_risk": "1.5",
                    "structure_buffer_atr": "0.25",
                    "trailing_atr_multiple": None,
                },
                "cost_model": {
                    "commission_bps": "2.5",
                    "minimum_commission_cny": "5",
                    "sell_slippage_bps": "5",
                    "tax_bps": "5",
                    "transfer_fee_bps": "0.1",
                },
                "dataset_content_sha256": dataset_content_sha256,
                "experiment_version": "fixture-exit-experiment@1",
                "minimum_holdout_episodes": 2,
                "minimum_validation_episodes": 2,
                "objective": "RETURN_TO_DRAWDOWN",
                "schema_version": "exit-policy-experiment-spec@1",
                "search_space": {
                    "atr_stop_multiples": ["1.5", "2.0"],
                    "max_candidates": 20,
                    "maximum_holding_sessions_values": [2],
                    "policy_version": "fixture-exit-policy@1",
                    "reward_to_risk_values": ["1.5", "2.0"],
                    "search_space_id": "fixture-exit-search",
                    "structure_buffer_atr_values": ["0.25"],
                    "trailing_atr_multiples": [None, "1.5"],
                },
                "walk_forward": {
                    "embargo_size": 2,
                    "initial_train_size": 2,
                    "label_horizon_sessions": 2,
                    "minimum_folds": 2,
                    "purge_size": 2,
                    "step_size": 2,
                    "test_size": 2,
                    "validation_size": 2,
                },
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _business_sessions(start: date, count: int) -> tuple[date, ...]:
    sessions: list[date] = []
    current = start
    while len(sessions) < count:
        if current.weekday() < 5:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)
