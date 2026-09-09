"""PAPER 日摘要伴随文件的纯值编解码工具。

这里放置不依赖报告投影数据类的 JSON/序列化边界：最终运行结果的读取、
以及 sidecar 数组中常见的字符串和整数投影。模块不访问数据库、网络或运行器，
因此可以独立测试并供其他报告格式复用。
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import cast


def string_tuple(value: object) -> tuple[str, ...]:
    """保留 JSON 数组中的字符串元素，并以稳定顺序返回。"""

    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def int_tuple(value: object) -> tuple[int, ...]:
    """保留 JSON 数组中的整数元素，拒绝布尔值。"""

    if not isinstance(value, list):
        return ()
    return tuple(
        item for item in value if isinstance(item, int) and not isinstance(item, bool)
    )


def counter_items(counter: Counter[str]) -> tuple[tuple[str, int], ...]:
    """把计数器编码成报告可直接消费的确定性序列。"""

    return tuple(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def read_final_result(
    root: Path, session_date: date, run_id: str
) -> dict[str, object] | None:
    """从最终 JSON 或 stdout sidecar 中读取与本次运行匹配的结果。

    stdout 可能包含日志和多行 JSON，因此从文件末尾向前尝试每一行；只有
    ``ok/action/run_id/session_date`` 全部匹配时才返回，避免误采纳其他运行结果。
    """

    candidates = (
        root / "final-result.json",
        root / "result.json",
        root / "runner.stdout.log",
    )
    for path in candidates:
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        documents = [text.strip(), *(line.strip() for line in reversed(text.splitlines()))]
        for document in documents:
            if not document:
                continue
            try:
                parsed = json.loads(document)
            except json.JSONDecodeError:
                continue
            if not isinstance(parsed, dict):
                continue
            value = cast(dict[str, object], parsed)
            if (
                value.get("ok") is True
                and value.get("action") == "run"
                and value.get("run_id") == run_id
                and value.get("session_date") == session_date.isoformat()
            ):
                return value
    return None


__all__ = ["counter_items", "int_tuple", "read_final_result", "string_tuple"]
