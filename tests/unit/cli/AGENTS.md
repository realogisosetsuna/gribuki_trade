# CLI 测试地图

本目录覆盖命令注册、参数解析、handler 路由和结果投影。实现入口位于
`src/gribuki_trade/cli.py` 与 `src/gribuki_trade/cli_commands/`；测试不应
直接引入 broker SDK，交易安全边界由 service/runtime 契约覆盖。

运行 `python -m pytest --temp-dir runtime/cli-tests -q tests/unit/cli`。
