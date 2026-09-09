# Schwab adapter 测试地图

本目录覆盖 Schwab client、OAuth、transport 和 runtime 适配契约。实现位于
`src/gribuki_trade/adapters/schwab/`；使用 offline transport，生产 OAuth
和 broker 可用性不由单元测试宣称。

运行 `python -m pytest --temp-dir runtime/schwab-tests -q tests/unit/adapters/schwab`。
