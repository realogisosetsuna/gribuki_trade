# 持久化测试地图

本文件细化根 `AGENTS.md` 的测试定位规则；交易安全和持久化约束不变。
先读 [`ARCHITECTURE.md`](../../../ARCHITECTURE.md) 和
[`data-lineage.md`](../../../docs/architecture/data-lineage.md)。

- 本目录覆盖 SQLite store、append-only 事件、租约、outbox、恢复和纯持久化策略。
- 对应实现位于 `src/gribuki_trade/storage/`，PAPER 连续性位于 `runtime/`。
- 新增 store 契约测试放在本目录；服务流程测试仍放在对应业务领域。
- 使用临时 SQLite 文件验证事务、幂等和重启行为，不读取 `runtime/` 中的真实状态。

运行 `python -m pytest --temp-dir runtime/storage-tests -q tests/unit/storage`。
