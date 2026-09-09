# Services 测试地图

本目录按业务流程分组：`candidate/`、`recommendation/`、`research/`、
`macro/`、`deepseek/`、`live/`、`news/` 和 `notifications/`。实现位于
`src/gribuki_trade/services/`；测试通过 ports 和 fake adapter 验证编排、
失败策略、research_only 及 LIVE 保护，不直接依赖供应商 SDK。

运行 `python -m pytest --temp-dir runtime/service-tests -q tests/unit/services`。
