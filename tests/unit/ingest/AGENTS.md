# Ingest 测试地图

本目录覆盖新闻、搜索和官方文档采集、解析、标准化与去重契约。实现位于
`src/gribuki_trade/ingest/`；测试只使用固定 payload 或本地 fake transport，
并验证来源、时间和降级信息。

运行 `python -m pytest --temp-dir runtime/ingest-tests -q tests/unit/ingest`。
