# Notifier adapter 测试地图

本目录覆盖 OneBot/NapCat 消息 adapter 的 payload、鉴权和错误映射。实现位于
`src/gribuki_trade/adapters/notifiers/`；测试使用 fake HTTP，不发送真实消息。

运行 `python -m pytest --temp-dir runtime/notifier-tests -q tests/unit/adapters/notifiers`。
