# 源码布局与定位规则

本文件描述当前 `src/gribuki_trade` 的目录职责和新增代码的归属规则。
实现模块按职责目录归档；旧的根级适配器和服务文件已移除，调用方应使用下面列出的规范路径。

## 目录树

```text
gribuki_trade/
├── domain/       领域对象、事件和状态不变量
├── ports/        外部边界协议，不包含具体供应商实现
├── adapters/     API、文件和模拟器适配器
│   ├── binance/  Binance REST/WebSocket 协议与 wire parsing
│   │   ├── spot/       Spot REST、订单参数和解析
│   │   ├── futures/    USDⓈ-M Futures REST、订单参数和用户流
│   │   ├── market_data/深度、历史行情和快照恢复
│   │   ├── transport/  签名、限频、HTTP 和错误映射
│   │   └── auth/       凭证和环境配置
│   ├── ashare/   A 股适配器（market、screening、profile 子包）
│   │   ├── market/      breadth、context、derivatives、surveillance
│   │   ├── screening/   screening、factor、payload、preopen
│   │   └── profile/     instrument profile
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
│       ├── paper_day/  PAPER 日账本、恢复、报告和事件
│       ├── intraday/   盘中 PAPER、数量策略和 LLM
│       ├── close/      收盘分析和盘后流程
│       ├── evidence/   breadth、context、derivatives 证据
│       └── research/   筛选、盘前、surveillance 和研究
│   └── live/     实盘观察、保护输入、订单记录和恢复编排
│   └── macro/    宏观证据选择、研究和对抗分析
│   └── exit/     退出计划生命周期和保护状态
│   ├── research/ 研究候选、跨市场证据和推荐编排
│   ├── communications/ 新闻采集和通知投递
│   └── llm/      生产 LLM 双轨编排
├── trading/      Broker-neutral OMS、仓位和订单状态转换
│   ├── core/     通用订单/仓位状态与 SQLite OMS
│   ├── futures/  USDⓈ-M Futures OMS、保护计划和恢复策略
│   └── spot/     Spot 订单列表状态
├── storage/      SQLite store、事件日志、lease 和 outbox
│   ├── live_records/  实盘命令、记录和保护状态
│   ├── paper/        PAPER 账本、订单和日级状态
│   ├── research/     研究、事件、行情证据和原始文档
│   └── execution/    退出计划、outbox、审计和实验
├── runtime/      PAPER/SHADOW/LIVE guard、连续性和临时目录
├── security/     secrets、keyring 和运行配置
├── reporting/    sidecar 投影、报告和 artifact 合约
│   ├── paper_day/  PAPER 日报告、sidecar 编解码和投影
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
    ├── trading/          OMS、订单和仓位策略
    ├── storage/          SQLite store、事件、outbox、租约和恢复
    ├── adapters/         按 market_data、ashare、macro、simulated 分组的适配器契约
    ├── cli/              CLI 参数、handler 和结果投影
    ├── gui/              Qt、integration gateway 和 NapCat 生命周期
    ├── runtime/          guard、临时目录和 PAPER 连续性
    ├── reporting/        sidecar、artifact 和报告契约
    ├── strategy_lab/     研究实验、factor DSL、walk-forward 和退出评估
    ├── ingest/           新闻、搜索、官方文档采集、解析和去重
    ├── services/         按 candidate、research、macro、live 等流程分组
    ├── analysis/         close、cross-market、crypto、exit、technical 和 cost 纯计算
    ├── adapters/schwab/  Schwab OAuth、client、transport 和 runtime 契约
    ├── adapters/notifiers/ OneBot/NapCat 通知 adapter 契约
    └── meta/             仓库布局、agent-readiness 和源码语言约束
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

## 模块入口规则

`adapters/` 与 `services/` 根目录只保留包初始化文件，不再放置供应商或业务
实现；历史根级别模块已经删除。新调用方直接导入职责目录，例如 `adapters.binance.spot.order_params`、
`adapters.market_data.akshare` 和 `services.live.live_trade_orchestration`。包初始化
文件不主动导入全部平台，避免导入一个纯模型时触发网络、密钥或重量级依赖。

纯模块不得导入 SQLite 连接、broker client、Qt widget 或网络 transport。凡是
会改变订单、账本、outbox 或恢复状态的代码，都必须留在对应 service、trading
或 storage 的事务边界内。

## 人工定位路径

遇到一个新问题时，先按外部边界、业务流程、持久化边界和展示边界搜索：

```bash
rg -n "class |def |async def" src/gribuki_trade/<area>
rg -n "from gribuki_trade\.adapters|from gribuki_trade\.storage" src/gribuki_trade
python -m pytest --temp-dir runtime/layout -q tests/unit/meta/test_module_layout.py
```

当前规范路径和职责映射见 [`module-map.md`](module-map.md)，重构记录见
[`modularization-roadmap.md`](modularization-roadmap.md)。

测试目录与源码边界保持同构：持久化不变量放在
`tests/unit/storage/`；行情和供应商协议放在
`tests/unit/adapters/<领域>/`，其中领域目录最多再增加一层。测试搬迁只改变
路径，不改变导入、fixture 内容或 pytest 收集规则；`tests/README.md` 提供各组
的定位命令和最近的测试地图。

实盘服务实现统一位于 `services/live/`。新的实盘观察、保护和记录代码应直接放入
`services/live/`。

宏观研究与对抗分析实现统一位于 `services/macro/`。新的宏观证据选择、分析策略
和研究编排应直接放入 `services/macro/`。

退出计划生命周期实现统一位于 `services/exit/`。

研究候选、跨市场证据和推荐服务统一位于 `services/research/`；新闻采集
与通知投递位于 `services/communications/`；生产 LLM 编排位于
`services/llm/`。
