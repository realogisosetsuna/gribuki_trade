from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    _iso_datetime,
    _positive_integer,
)


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 experiments 命令族。"""

    factor_discover = commands.add_parser(
        "strategy-factor-discover",
        help="expand the bounded research-only factor grammar without market data",
    )
    factor_discover.add_argument(
        "--max-trials",
        type=_positive_integer,
        default=100,
        help="hard expansion budget; the command fails instead of truncating",
    )
    factor_discover.add_argument(
        "--output",
        help="optional JSON destination written atomically; stdout is always retained",
    )
    exit_evaluate = commands.add_parser(
        "strategy-exit-evaluate",
        help="对冻结 A 股退出样本运行仅研究的 walk-forward/holdout 实验",
    )
    exit_evaluate.add_argument("--dataset", required=True, help="冻结样本 JSON 文件")
    exit_evaluate.add_argument(
        "--specification",
        required=True,
        help="预注册搜索空间、成本和 walk-forward 规格 JSON 文件",
    )
    exit_evaluate.add_argument(
        "--output",
        required=True,
        help="完整不可变 trial registry 的原子输出路径",
    )
    exit_evaluate.add_argument(
        "--created-at",
        type=_iso_datetime,
        help="可选的可重放实验时点；必须含时区",
    )
    exit_evaluate.add_argument(
        "--overwrite",
        action="store_true",
        help="显式允许替换同一路径的研究产物；绝不修改生产策略",
    )
    exit_evaluate.add_argument(
        "--confirm",
        choices=("RESEARCH_ONLY",),
        help="写入试验登记前必须精确确认 RESEARCH_ONLY",
    )
