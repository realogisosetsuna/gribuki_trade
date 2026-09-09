"""A-share 命令族共享的无副作用 argparse 片段。

这些函数只负责把稳定的参数契约注册到调用方提供的 parser，不读取配置、
访问网络或创建服务对象。这样各个 A-share 命令族可以复用同一组新闻源和
通知目标约束，避免命令帮助文本与 choices 在不同模块之间逐渐漂移。
"""

from __future__ import annotations

import argparse

GLOBAL_NEWS_FEEDS = (
    "global_eastmoney",
    "global_cailianpress",
    "global_sina",
    "global_10jqka",
)
"""可用于跨市场新闻采集的稳定全局 feed 标识。"""


ALL_NEWS_FEEDS = ("individual_eastmoney", *GLOBAL_NEWS_FEEDS)
"""单股新闻命令可接受的 feed 标识，包括个股 Eastmoney feed。"""


def add_optional_notify_target_arguments(parser: argparse.ArgumentParser) -> None:
    """注册可选的 OneBot 通知目标参数。

    close-research 的单次、批量和 watch 命令都使用同样的目标字段；目标
    仍然由上层命令在执行阶段校验，这里只保留 argparse 层的类型 choices。
    """

    parser.add_argument("--notify-target-kind", choices=("private", "group"))
    parser.add_argument("--notify-target-id")


__all__ = [
    "ALL_NEWS_FEEDS",
    "GLOBAL_NEWS_FEEDS",
    "add_optional_notify_target_arguments",
]
