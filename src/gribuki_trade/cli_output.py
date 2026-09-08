"""命令行输出层使用的纯格式化与原子 JSON 写入函数。"""

from __future__ import annotations

import json
import os
import tempfile
from decimal import Decimal
from pathlib import Path


def decimal_text(value: Decimal | None) -> str | None:
    """把 Decimal 转成稳定的人员可读文本。"""

    return None if value is None else format(value, "f")


def three_decimal_text(value: Decimal | None) -> str | None:
    """以三位小数输出研究和运维文档中的 Decimal。"""

    return None if value is None else format(value.quantize(Decimal("0.001")), "f")


def atomic_write_json(path: Path, payload: object) -> None:
    """在同一文件系统中原子替换 JSON 文件。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            delete=False,
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        ) as stream:
            temporary = Path(stream.name)
            json.dump(
                payload,
                stream,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


_decimal_text = decimal_text
_three_decimal_text = three_decimal_text
_atomic_write_cli_json = atomic_write_json
