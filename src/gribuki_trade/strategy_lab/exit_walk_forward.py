"""退出策略滚动验证的纯计划边界。

本模块只负责把已经冻结的数据集按交易日映射到训练、验证、清洗、隔离和
留出区间。它不执行退出模拟，也不读取文件或修改研究登记，因此可以独立
测试计划哈希、样本索引和最小样本门槛。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from gribuki_trade.strategy_lab.experiments import (
    WalkForwardConfig,
    WalkForwardPlan,
    build_walk_forward_plan,
)

if TYPE_CHECKING:
    from gribuki_trade.strategy_lab.exit_evaluator import ExitPolicyDataset


_SHA256 = re.compile(r"^[0-9a-f]{64}$")


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
        """由稳定的序列化边界计算计划身份。"""

        from gribuki_trade.strategy_lab.exit_serialization import (
            sha256_document,
            walk_forward_document,
        )

        return sha256_document(walk_forward_document(self))


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


def _sessions_at(sessions: tuple[date, ...], indices: tuple[int, ...]) -> tuple[date, ...]:
    """按确定性索引把滚动计划边界还原为交易日。"""

    return tuple(sessions[index] for index in indices)


def _episode_indices_for_sessions(
    indices_by_session: dict[date, list[int]],
    sessions: tuple[date, ...],
) -> tuple[int, ...]:
    """保留数据集原顺序，将交易日映射为逐笔 episode 索引。"""

    return tuple(
        index for session in sessions for index in indices_by_session.get(session, ())
    )


def _positive_integer(value: object, name: str) -> int:
    """校验滚动计划中的最小样本门槛。"""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


__all__ = [
    "ExitPolicyWalkForwardFold",
    "ExitPolicyWalkForwardPlan",
    "build_exit_policy_walk_forward_plan",
]
