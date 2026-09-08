from __future__ import annotations

import argparse

from gribuki_trade.cli_commands.parser_support import (
    _iso_datetime,
    _non_negative_float,
    _positive_float,
    _positive_integer,
)


def register(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """注册 live_sync 命令族。"""

    live_sync = commands.add_parser(
        "live-sync",
        help="接收实盘成交、检查账本，或执行一次应用级保护分析与行情跟踪",
    )
    live_sync.add_argument("action", choices=("ingest", "status", "cycle"))
    live_sync.add_argument(
        "--ledger-db",
        default="runtime/live/observed-live.sqlite3",
        help="与 PAPER 完全隔离的实盘观察 SQLite 账本",
    )
    live_sync.add_argument(
        "--allowed-sender",
        action="append",
        help="ingest 必填；可重复指定允许同步成交的 QQ 号",
    )
    live_sync.add_argument(
        "--event-json",
        help="ingest 必填；OneBot v11 私聊事件 JSON 文件，使用 - 从标准输入读取",
    )
    live_sync.add_argument("--account", help="status 必填；要检查的实盘观察账户")
    live_sync.add_argument(
        "--received-at",
        type=_iso_datetime,
        help="可选接收时点；仅用于可重放导入，必须含时区",
    )
    live_sync.add_argument(
        "--exit-plan-db",
        default="runtime/live/exit-plans.sqlite3",
        help="cycle 使用的独立退出计划哈希账本",
    )
    live_sync.add_argument(
        "--outbox-path",
        default="runtime/live/outbox.sqlite3",
        help="cycle 使用的单目标 NapCat 出站队列",
    )
    live_sync.add_argument("--target-kind", choices=("private", "group"))
    live_sync.add_argument("--target-id")
    live_sync.add_argument(
        "--base-url",
        default=None,
        help="OneBot 地址；cycle 省略时读取 GUI 共享配置",
    )
    live_sync.add_argument(
        "--llm-provider",
        choices=("deepseek", "openai"),
        default=None,
        help="cycle 双轨分析 provider；省略时读取 GUI 共享配置",
    )
    live_sync.add_argument(
        "--llm-model",
        default=None,
        help="cycle 双轨分析模型；省略时读取 GUI 共享配置",
    )
    live_sync.add_argument("--work-limit", type=_positive_integer, default=20)
    live_sync.add_argument(
        "--quick-timeout-seconds",
        type=_positive_float,
        default=45.0,
        help="ingest 确认 BUY 后建立 QUICK 及单次跟踪各自允许的有限秒数",
    )
    live_sync.add_argument(
        "--deep-timeout-seconds",
        type=_positive_float,
        default=600.0,
        help="单轮等待 DEEP 的有限秒数；超时后持久任务自动释放以便下轮重试",
    )
    live_sync.add_argument(
        "--tracking-pump-interval",
        type=_positive_float,
        default=30.0,
        help="等待 DEEP 时继续观察完整 K 线的间隔秒数",
    )
    live_sync.add_argument(
        "--tracking-pump-limit",
        type=_positive_integer,
        default=20,
        help="等待 DEEP 时最多追加的持仓观察轮数",
    )
    live_sync.add_argument(
        "--dispatch-cycles",
        type=_positive_integer,
        default=3,
        help="cycle 最多执行的 NapCat outbox 派发轮数",
    )
    live_sync.add_argument(
        "--dispatch-poll-interval",
        type=_non_negative_float,
        default=0.5,
    )
    live_sync.add_argument(
        "--confirm",
        choices=("LIVE_SYNC_CYCLE",),
        help="cycle 必须显式确认；该动作只分析、跟踪和提醒，绝不下单",
    )
