# 源码布局与定位规则

本文件描述当前 `src/gribuki_trade` 的目录职责和新增代码的归属规则。
历史兼容 facade 仍保留在旧路径，但新的实现应放在下面列出的职责目录中。

## 目录树

```text
gribuki_trade/
├── domain/       领域对象、事件和状态不变量
├── ports/        外部边界协议，不包含具体供应商实现
├── adapters/     API、文件和模拟器适配器
│   ├── binance/  Binance REST/WebSocket 协议与 wire parsing
│   ├── ashare/   A 股供应商、SSE 官方数据和纯 payload parsing
│   ├── market_data/
│   ├── macro/
│   ├── llm/
│   ├── schwab/
│   ├── notifiers/
│   └── simulated/
├── ingest/       新闻、搜索和证据采集
├── pipeline/     标准化、去重和事件流水线
├── features/     纯技术指标、筛选和横截面计算
├── strategy/     策略决策语言和组合规则
├── backtest/     离线回测和费用模型
├── strategy_lab/ 研究实验、walk-forward 和 factor DSL
├── services/     应用流程、恢复、审批和失败策略
│   ├── binance/  Binance 执行、SHADOW 和无人值守流程
│   └── ashare/   A 股研究、PAPER 和盘后流程
├── trading/      Broker-neutral OMS、仓位和订单状态转换
├── storage/      SQLite store、事件日志、lease 和 outbox
├── runtime/      PAPER/SHADOW/LIVE guard、连续性和临时目录
├── security/     secrets、keyring 和运行配置
├── reporting/    sidecar 投影、报告和 artifact 合约
├── gui/          Qt 页面和 presentation wiring
└── cli_commands/命令注册、参数解析和按命令族划分的 handler
```

测试代码采用相同的领域分组，避免 `tests/unit/` 继续堆叠所有模块的平铺文件：

```text
tests/
├── fixtures/             固定的供应商 payload、报告和行情样本
└── unit/
    ├── ashare/           A 股研究、PAPER、盘前和盘后
    ├── binance/          Spot、USDⓈ-M Futures、流和执行
    └── trading/          OMS、订单和仓位策略
```

其他测试领域按同一规则迁移到 `tests/unit/<领域>/`。测试文件保持
`test_<module>.py` 命名，pytest 仍从 `tests/` 递归收集，因此不会改变 CI 命令。
完整的测试目录说明见 [`tests/README.md`](../../tests/README.md)。

## 新代码放置规则

| 需求 | 归属 | 不应放入 |
|---|---|---|
| Binance/SSE/AKShare/Schwab 的请求、响应或连接生命周期 | `adapters/<provider>/` | `services/`、`strategy/`、`gui/` |
| 响应字段校验、Decimal 转换、URL/参数构造 | 与适配器并列的 `*_parsing.py`、`*_params.py` 或 `*_payload.py` | REST client 的事务方法 |
| 多个适配器组成的业务流程、重试和恢复 | `services/<domain>/` | provider adapter、CLI parser |
| SQLite DDL、租约、幂等 append、单调状态转换 | `storage/` 或 `trading/` | 纯 projection、GUI、strategy |
| 不含 I/O 的规则、模型和结果投影 | `features/`、`policy/`、对应服务旁的 `*_models.py`/`*_policy.py` | durable store、broker SDK |
| CLI 参数注册和命令族处理 | `cli_commands/parsers/`、`cli_commands/handlers/` | 大型 `cli.py` 新增实现 |
| sidecar、JSON、Markdown 和消息格式化 | `reporting/` 或 `cli_commands/*_payloads.py` | storage transaction、网络 adapter |

## Facade 规则

顶层历史模块（例如 `cli.py`、`adapters/ashare_derivatives.py`）是兼容入口，
用于保留旧 import、嵌入调用和测试 monkeypatch。拆分时应让 facade 重新导出
同一个对象身份，并在 `tests/unit/test_module_layout.py` 中加入路径和 identity
断言。新调用方应直接依赖职责目录中的实现模块。

纯模块不得导入 SQLite 连接、broker client、Qt widget 或网络 transport。凡是
会改变订单、账本、outbox 或恢复状态的代码，都必须留在对应 service、trading
或 storage 的事务边界内。

## 人工定位路径

遇到一个新问题时，先按外部边界、业务流程、持久化边界和展示边界搜索：

```bash
rg -n "class |def |async def" src/gribuki_trade/<area>
rg -n "from gribuki_trade\.adapters|from gribuki_trade\.storage" src/gribuki_trade
python -m pytest --temp-dir runtime/layout -q tests/unit/test_module_layout.py
```

完整的 facade 到实现映射见 [`module-map.md`](module-map.md)，重构顺序和已完成
切片见 [`modularization-roadmap.md`](modularization-roadmap.md)。
