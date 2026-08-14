"""冻结退出策略数据集的严格加载、实验编排与原子归档入口。

本模块把离线 evaluator 变成可重复运行的研究工作流，但仍不会修改任何生产参数。
输入必须是两个已经落盘的 JSON 文件：一份逐笔历史样本集和一份预注册实验规格；
两者的文件哈希、内容哈希、walk-forward 计划及完整 trial registry 一并写入结果。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import cast

from gribuki_trade.strategy_lab.exit_evaluator import (
    ExitEvaluationBar,
    ExitPolicyCostModel,
    ExitPolicyDataset,
    ExitPolicyEpisode,
    ExitPolicyEvaluator,
    ExitPolicyObjective,
    ExitPolicyTrialRegistry,
    build_exit_policy_walk_forward_plan,
    exit_policy_registry_to_json,
    run_exit_policy_walk_forward_experiment,
)
from gribuki_trade.strategy_lab.exit_policies import (
    ExitPolicyParameters,
    ExitPolicySearchSpace,
)
from gribuki_trade.strategy_lab.experiments import WalkForwardConfig

_DATASET_SCHEMA = "exit-policy-dataset@1"
_SPEC_SCHEMA = "exit-policy-experiment-spec@1"
_ARTIFACT_SCHEMA = "exit-policy-experiment-artifact@1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_INPUT_BYTES = 128 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class FrozenExitPolicyExperiment:
    """从两份冻结文件解析出的完整且不可变实验输入。"""

    dataset: ExitPolicyDataset
    search_space: ExitPolicySearchSpace
    baseline: ExitPolicyParameters
    costs: ExitPolicyCostModel
    walk_forward: WalkForwardConfig
    minimum_validation_episodes: int
    minimum_holdout_episodes: int
    objective: ExitPolicyObjective
    experiment_version: str
    dataset_file_sha256: str
    specification_file_sha256: str


@dataclass(frozen=True, slots=True)
class ExitPolicyExperimentArtifact:
    """一次只研究、不自动晋升的实验结果及其落盘身份。"""

    destination: Path
    registry: ExitPolicyTrialRegistry
    artifact_sha256: str
    dataset_file_sha256: str
    specification_file_sha256: str


def load_exit_policy_dataset(path: Path) -> tuple[ExitPolicyDataset, str]:
    """加载一份严格冻结的样本集，同时返回原始文件 SHA-256。"""

    document, file_sha256 = _read_json_file(path)
    return _dataset_from_document(document), file_sha256


def load_frozen_exit_policy_experiment(
    dataset_path: Path,
    specification_path: Path,
) -> FrozenExitPolicyExperiment:
    """严格解析冻结样本和预注册规格；任何未知字段或哈希错配均拒绝。"""

    dataset, dataset_file_sha256 = load_exit_policy_dataset(dataset_path)
    specification, specification_file_sha256 = _read_json_file(specification_path)
    _require_exact_keys(
        specification,
        {
            "baseline",
            "cost_model",
            "dataset_content_sha256",
            "experiment_version",
            "minimum_holdout_episodes",
            "minimum_validation_episodes",
            "objective",
            "schema_version",
            "search_space",
            "walk_forward",
        },
        "experiment specification",
    )
    if _text(specification, "schema_version") != _SPEC_SCHEMA:
        raise ValueError("unsupported exit-policy experiment specification schema")
    expected_dataset = _sha256(specification, "dataset_content_sha256")
    if dataset.content_sha256 != expected_dataset:
        raise ValueError("frozen dataset content hash does not match the specification")
    search_space = _search_space(_mapping(specification, "search_space"))
    baseline = _parameters(_mapping(specification, "baseline"))
    if baseline.policy_version != search_space.policy_version:
        raise ValueError("baseline policy version does not match the search space")
    costs = _cost_model(_mapping(specification, "cost_model"))
    walk_forward = _walk_forward(_mapping(specification, "walk_forward"))
    minimum_validation = _positive_int(
        specification,
        "minimum_validation_episodes",
    )
    minimum_holdout = _positive_int(specification, "minimum_holdout_episodes")
    objective = ExitPolicyObjective(_text(specification, "objective"))
    experiment_version = _text(specification, "experiment_version")
    return FrozenExitPolicyExperiment(
        dataset=dataset,
        search_space=search_space,
        baseline=baseline,
        costs=costs,
        walk_forward=walk_forward,
        minimum_validation_episodes=minimum_validation,
        minimum_holdout_episodes=minimum_holdout,
        objective=objective,
        experiment_version=experiment_version,
        dataset_file_sha256=dataset_file_sha256,
        specification_file_sha256=specification_file_sha256,
    )


def run_frozen_exit_policy_experiment(
    dataset_path: Path,
    specification_path: Path,
    destination: Path,
    *,
    created_at: datetime | None = None,
    overwrite: bool = False,
) -> ExitPolicyExperimentArtifact:
    """执行完整 walk-forward/holdout 流程，并把登记表原子写入指定文件。"""

    frozen = load_frozen_exit_policy_experiment(dataset_path, specification_path)
    plan = build_exit_policy_walk_forward_plan(
        frozen.dataset,
        frozen.walk_forward,
        minimum_validation_episodes=frozen.minimum_validation_episodes,
        minimum_holdout_episodes=frozen.minimum_holdout_episodes,
    )
    moment = created_at or datetime.now(UTC)
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("created_at must be timezone-aware")
    registry = run_exit_policy_walk_forward_experiment(
        evaluator=ExitPolicyEvaluator(frozen.dataset),
        search_space=frozen.search_space,
        baseline=frozen.baseline,
        walk_forward=plan,
        costs=frozen.costs,
        objective=frozen.objective,
        created_at=moment.astimezone(UTC),
        experiment_version=frozen.experiment_version,
    )
    registry_document = cast(
        dict[str, object],
        json.loads(exit_policy_registry_to_json(registry)),
    )
    document: dict[str, object] = {
        "artifact_schema_version": _ARTIFACT_SCHEMA,
        "dataset_content_sha256": frozen.dataset.content_sha256,
        "dataset_file_sha256": frozen.dataset_file_sha256,
        "execution_authority": False,
        "promotion_authorized": False,
        "registry": registry_document,
        "registry_sha256": registry.registry_sha256,
        "research_only": True,
        "specification_file_sha256": frozen.specification_file_sha256,
        "walk_forward_sha256": plan.plan_sha256,
    }
    encoded = _canonical_json(document).encode("utf-8") + b"\n"
    resolved = destination.resolve()
    if destination.is_symlink():
        raise ValueError("exit-policy artifact destination must not be a symlink")
    if resolved.exists() and not overwrite:
        raise FileExistsError("exit-policy artifact already exists")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(resolved, encoded)
    return ExitPolicyExperimentArtifact(
        destination=resolved,
        registry=registry,
        artifact_sha256=hashlib.sha256(encoded).hexdigest(),
        dataset_file_sha256=frozen.dataset_file_sha256,
        specification_file_sha256=frozen.specification_file_sha256,
    )


def _dataset_from_document(document: Mapping[str, object]) -> ExitPolicyDataset:
    _require_exact_keys(
        document,
        {"dataset_id", "episodes", "frozen_at", "schema_version"},
        "exit-policy dataset",
    )
    if _text(document, "schema_version") != _DATASET_SCHEMA:
        raise ValueError("unsupported exit-policy dataset schema")
    episodes = tuple(_episode(item) for item in _object_sequence(document, "episodes"))
    return ExitPolicyDataset(
        dataset_id=_text(document, "dataset_id"),
        schema_version=_DATASET_SCHEMA,
        episodes=episodes,
        frozen_at=_datetime(document, "frozen_at"),
    )


def _episode(document: Mapping[str, object]) -> ExitPolicyEpisode:
    _require_exact_keys(
        document,
        {
            "atr_at_entry",
            "entry_at",
            "entry_price",
            "entry_session_date",
            "episode_id",
            "features_known_at",
            "future_bars",
            "quantity",
            "source_revisions",
            "structure_low_at_entry",
            "symbol",
        },
        "exit-policy episode",
    )
    return ExitPolicyEpisode(
        episode_id=_text(document, "episode_id"),
        symbol=_text(document, "symbol"),
        entry_at=_datetime(document, "entry_at"),
        entry_session_date=_date(document, "entry_session_date"),
        entry_price=_decimal(document, "entry_price"),
        quantity=_positive_int(document, "quantity"),
        atr_at_entry=_decimal(document, "atr_at_entry"),
        structure_low_at_entry=_decimal(document, "structure_low_at_entry"),
        features_known_at=_datetime(document, "features_known_at"),
        future_bars=tuple(_bar(item) for item in _object_sequence(document, "future_bars")),
        source_revisions=_pairs(document, "source_revisions"),
    )


def _bar(document: Mapping[str, object]) -> ExitEvaluationBar:
    _require_exact_keys(
        document,
        {
            "close",
            "completed_at",
            "high",
            "low",
            "lower_price_limit",
            "open",
            "session_date",
            "source_id",
            "source_revision",
            "suspended",
            "upper_price_limit",
            "volume_shares",
        },
        "exit-policy evaluation bar",
    )
    suspended = document.get("suspended")
    if not isinstance(suspended, bool):
        raise TypeError("suspended must be a boolean")
    volume = document.get("volume_shares")
    if isinstance(volume, bool) or not isinstance(volume, int):
        raise TypeError("volume_shares must be an integer")
    return ExitEvaluationBar(
        session_date=_date(document, "session_date"),
        open=_decimal(document, "open"),
        high=_decimal(document, "high"),
        low=_decimal(document, "low"),
        close=_decimal(document, "close"),
        volume_shares=volume,
        suspended=suspended,
        lower_price_limit=_decimal(document, "lower_price_limit"),
        upper_price_limit=_decimal(document, "upper_price_limit"),
        completed_at=_datetime(document, "completed_at"),
        source_id=_text(document, "source_id"),
        source_revision=_text(document, "source_revision"),
    )


def _search_space(document: Mapping[str, object]) -> ExitPolicySearchSpace:
    _require_exact_keys(
        document,
        {
            "atr_stop_multiples",
            "max_candidates",
            "maximum_holding_sessions_values",
            "policy_version",
            "reward_to_risk_values",
            "search_space_id",
            "structure_buffer_atr_values",
            "trailing_atr_multiples",
        },
        "exit-policy search space",
    )
    trailing: list[Decimal | None] = []
    for item in _sequence(document, "trailing_atr_multiples"):
        trailing.append(None if item is None else _decimal_value(item, "trailing ATR"))
    return ExitPolicySearchSpace(
        search_space_id=_text(document, "search_space_id"),
        policy_version=_text(document, "policy_version"),
        atr_stop_multiples=_decimals(document, "atr_stop_multiples"),
        structure_buffer_atr_values=_decimals(
            document,
            "structure_buffer_atr_values",
        ),
        reward_to_risk_values=_decimals(document, "reward_to_risk_values"),
        maximum_holding_sessions_values=_positive_ints(
            document,
            "maximum_holding_sessions_values",
        ),
        trailing_atr_multiples=tuple(trailing),
        max_candidates=_positive_int(document, "max_candidates"),
    )


def _parameters(document: Mapping[str, object]) -> ExitPolicyParameters:
    _require_exact_keys(
        document,
        {
            "atr_stop_multiple",
            "maximum_holding_sessions",
            "policy_version",
            "reward_to_risk",
            "structure_buffer_atr",
            "trailing_atr_multiple",
        },
        "exit-policy baseline",
    )
    trailing = document.get("trailing_atr_multiple")
    return ExitPolicyParameters(
        policy_version=_text(document, "policy_version"),
        atr_stop_multiple=_decimal(document, "atr_stop_multiple"),
        structure_buffer_atr=_decimal(document, "structure_buffer_atr"),
        reward_to_risk=_decimal(document, "reward_to_risk"),
        maximum_holding_sessions=_positive_int(
            document,
            "maximum_holding_sessions",
        ),
        trailing_atr_multiple=(
            None if trailing is None else _decimal_value(trailing, "trailing_atr_multiple")
        ),
    )


def _cost_model(document: Mapping[str, object]) -> ExitPolicyCostModel:
    _require_exact_keys(
        document,
        {
            "commission_bps",
            "minimum_commission_cny",
            "sell_slippage_bps",
            "tax_bps",
            "transfer_fee_bps",
        },
        "exit-policy cost model",
    )
    return ExitPolicyCostModel(
        commission_bps=_decimal(document, "commission_bps"),
        tax_bps=_decimal(document, "tax_bps"),
        transfer_fee_bps=_decimal(document, "transfer_fee_bps"),
        sell_slippage_bps=_decimal(document, "sell_slippage_bps"),
        minimum_commission_cny=_decimal(document, "minimum_commission_cny"),
    )


def _walk_forward(document: Mapping[str, object]) -> WalkForwardConfig:
    names = {
        "embargo_size",
        "initial_train_size",
        "label_horizon_sessions",
        "minimum_folds",
        "purge_size",
        "step_size",
        "test_size",
        "validation_size",
    }
    _require_exact_keys(document, names, "exit-policy walk-forward config")
    return WalkForwardConfig(**{name: _positive_int(document, name) for name in names})


def _read_json_file(path: Path) -> tuple[dict[str, object], str]:
    source = path.resolve(strict=True)
    if path.is_symlink() or not source.is_file():
        raise ValueError("frozen exit-policy input must be a regular non-symlink file")
    size = source.stat().st_size
    if size < 2 or size > _MAX_INPUT_BYTES:
        raise ValueError("frozen exit-policy input size is outside the accepted range")
    raw = source.read_bytes()
    try:
        parsed = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except UnicodeDecodeError as error:
        raise ValueError("frozen exit-policy input must be UTF-8") from error
    except json.JSONDecodeError as error:
        raise ValueError("frozen exit-policy input must contain valid JSON") from error
    if not isinstance(parsed, dict) or any(not isinstance(key, str) for key in parsed):
        raise TypeError("frozen exit-policy input root must be an object")
    return cast(dict[str, object], parsed), hashlib.sha256(raw).hexdigest()


def _unique_object(pairs: Sequence[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"duplicate JSON key: {key}")
        document[key] = value
    return document


def _require_exact_keys(
    document: Mapping[str, object],
    expected: set[str],
    label: str,
) -> None:
    actual = set(document)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        raise ValueError(f"{label} fields mismatch; missing={missing}; unknown={unknown}")


def _mapping(document: Mapping[str, object], name: str) -> dict[str, object]:
    value = document.get(name)
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise TypeError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _sequence(document: Mapping[str, object], name: str) -> list[object]:
    value = document.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return cast(list[object], value)


def _object_sequence(
    document: Mapping[str, object],
    name: str,
) -> tuple[dict[str, object], ...]:
    result: list[dict[str, object]] = []
    for value in _sequence(document, name):
        if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
            raise TypeError(f"{name} must contain objects")
        result.append(cast(dict[str, object], value))
    return tuple(result)


def _text(document: Mapping[str, object], name: str) -> str:
    value = document.get(name)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{name} must be a non-empty string")
    return value.strip()


def _sha256(document: Mapping[str, object], name: str) -> str:
    value = _text(document, name)
    if _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _decimal(document: Mapping[str, object], name: str) -> Decimal:
    if name not in document:
        raise KeyError(name)
    return _decimal_value(document[name], name)


def _decimal_value(value: object, name: str) -> Decimal:
    if value is None or isinstance(value, (bool, float)):
        raise TypeError(f"{name} must be a JSON string or integer decimal")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{name} must be a decimal") from None
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _positive_int(document: Mapping[str, object], name: str) -> int:
    value = document.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise TypeError(f"{name} must be a positive integer")
    return value


def _positive_ints(document: Mapping[str, object], name: str) -> tuple[int, ...]:
    values = _sequence(document, name)
    if not values:
        raise ValueError(f"{name} must not be empty")
    return tuple(
        _positive_int({name: value}, name)
        for value in values
    )


def _decimals(document: Mapping[str, object], name: str) -> tuple[Decimal, ...]:
    values = _sequence(document, name)
    if not values:
        raise ValueError(f"{name} must not be empty")
    return tuple(_decimal_value(value, name) for value in values)


def _date(document: Mapping[str, object], name: str) -> date:
    try:
        return date.fromisoformat(_text(document, name))
    except ValueError:
        raise ValueError(f"{name} must be an ISO date") from None


def _datetime(document: Mapping[str, object], name: str) -> datetime:
    try:
        result = datetime.fromisoformat(_text(document, name))
    except ValueError:
        raise ValueError(f"{name} must be an ISO datetime") from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return result.astimezone(UTC)


def _pairs(document: Mapping[str, object], name: str) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = []
    for item in _sequence(document, name):
        if (
            not isinstance(item, list)
            or len(item) != 2
            or any(not isinstance(value, str) or not value.strip() for value in item)
        ):
            raise TypeError(f"{name} must contain two-string arrays")
        pairs.append((item[0].strip(), item[1].strip()))
    return tuple(pairs)


def _canonical_json(document: Mapping[str, object]) -> str:
    return json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _atomic_write(destination: Path, payload: bytes) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()
