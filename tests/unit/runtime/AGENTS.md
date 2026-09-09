# Runtime 测试地图

本目录覆盖 PAPER/SHADOW/LIVE guard、临时目录、系统唤醒和 PAPER 连续性
manifest。实现位于 `src/gribuki_trade/runtime/`；任何 LIVE 规则变更都必须
保持 fail-closed，并补充对应 guard 回归测试。

运行 `python -m pytest --temp-dir runtime/runtime-tests -q tests/unit/runtime`。
