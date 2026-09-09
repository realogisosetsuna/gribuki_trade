# Reporting 测试地图

本目录覆盖 sidecar 投影、PAPER-day 报告、artifact 和报告契约。实现位于
`src/gribuki_trade/reporting/`；纯投影不得读取 SQLite、网络或 broker，固定
夹具统一从 `tests/fixtures/` 读取。

运行 `python -m pytest --temp-dir runtime/reporting-tests -q tests/unit/reporting`。
