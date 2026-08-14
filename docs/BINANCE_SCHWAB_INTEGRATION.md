# Binance 与 Charles Schwab 接入边界

更新日期：2026-08-15

本文按“底层适配器能力、已经接入的执行服务、尚未形成的用户工作流”区分现状。Testnet、PAPER、
SHADOW 和离线 transport 只能验证工程行为，不代表真实成交质量、监管许可或策略收益。

## Binance Spot

### 底层 gateway

Spot gateway 已实现：

- 公共 REST：连通性、服务器时间、`exchangeInfo`、ticker、订单簿和 K 线；
- 签名 REST：账户、限价下单、查询、撤单、open/all orders、trades、fees 与
  `/api/v3/order/test`；
- 公共 WebSocket 与私有 User Data Stream；
- HMAC-SHA-256、服务器时钟偏差校准、`Decimal` 价格/数量以及常用 symbol filter 校验；
- 对 `-1007`、写请求 5xx、transport/protocol 不确定性统一进入 `UNKNOWN`，不盲目重发；
- Testnet/LIVE 使用不同 URL 和凭据名，绝不跨环境回退。

底层 LIVE gateway 只有调用者显式传入 `allow_live=True` 才能构造。这只是低层防误用开关，
不表示仓库已经提供生产 LIVE 执行入口。

### 已编排的 Testnet 执行

`BinanceSpotTestnetExecutionService` 是当前唯一 durable 远端虚拟执行闭环，并在构造时硬限制
Spot Testnet：

- SQLite WAL/FULL OMS 把订单与 SUBMIT/CANCEL command outbox 先落盘再越过进程边界；
- 启动时把遗留 `IN_FLIGHT` 隔离为 `UNKNOWN`，再读取 account、openOrders、allOrders、
  myTrades 与必要的精确 getOrder；
- 未解释订单或命令没有完成 REST authoritative reconciliation 前，不派发新的待处理命令；
- 私有流重连 epoch 变化后先做 REST 补账，无法解释时终止；
- 状态、fill、余额和持仓更新幂等，迟到事件不能回退终态；
- 账户与 symbol 白名单在服务边界强制执行。

用户侧 CLI 仅包含 Testnet 变更入口：

~~~powershell
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-status --symbol BTCUSDT
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-order-test --symbol BTCUSDT --notional 20
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-oms-cycle --symbol BTCUSDT --notional 20 --database runtime/binance/testnet-oms.sqlite3 --confirm TESTNET
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-oms-fill --symbol BTCUSDT --notional 20 --database runtime/binance/testnet-oms-fill.sqlite3 --confirm TESTNET_FILL
~~~

前两条分别只读或使用 Binance `order/test`；后两条会进入 Testnet 撮合或虚拟成交。历史本机验证
记录见 [Binance 模拟交易验收快照](BINANCE_SIMULATION_STATUS.md)。

### 历史研究与本地 SHADOW

`binance-history-sync` 只归档已闭合 K 线，冲突修订失败关闭并生成数据集 SHA-256；
`binance-backtest` 使用 point-in-time、下一 bar 才成交的单交易对 Spot 模型；
`binance-shadow-run` 用公开行情驱动本地 PAPER/OMS，水印固定为 `NO_REMOTE_ORDERS`。
SHADOW 选择 `--environment LIVE` 只代表生产公共行情，不会打开远端订单提交。

### Futures

USDⓈ-M/COIN-M 当前只有 Demo public status、签名 account/position 与 `order/test` 验证能力，
没有 place-order 方法或 durable Futures OMS。Spot Testnet key 不会复用为 Futures Demo key；
不支持的 Margin/Portfolio Margin 阶段不会回退到 LIVE。

### 仍未完成

- 没有面向用户的 Binance 生产 LIVE CLI、durable service 或运维 runbook；
- `LiveTradingGuard` / `GuardedBrokerAdapter` 是可复用安全构件，但当前没有成为所有 gateway
  调用的不可绕过外壳；
- Testnet/SHADOW 仍缺长期 soak、系统化故障注入、完整速率与订单计数阻断、组合级风险、
  kill switch 和 GUI 生产接线；
- 本地 PAPER/回测成交模型不复制真实队列、深度、延迟和市场冲击。

因此，准确结论是“默认拒绝且无生产入口”，不是“底层代码技术上绝对无法访问 LIVE”。

## Charles Schwab

### 已实现的隔离适配层

- OAuth 2.0 Authorization Code：一次性 state、callback URI 精确匹配、code exchange、按
  `expires_in` 刷新与并发刷新合并；
- app credential 与完整 token JSON 通过 OS keyring/SecretProvider 保存；
- Market Data REST：quotes、price history、option chain、market hours；
- Trader REST：账户 hash、账户/持仓、订单列表与单笔订单、place/cancel；
- 简单 LIMIT 股票/期权订单；数量必须为整数，期权必须显式开平仓 instruction；
- 只对明确 401 做一次 refresh+重放；429 保留 `Retry-After`；transport/5xx 写入不确定性不自动重发；
- 生产 OAuth/REST 只有显式 `allow_live=True` 才能构造，错误与日志不暴露 token、账户路径或响应正文。

`SchwabBroker` 的 client-order-id 幂等只在该适配器进程内生效，不等于 durable OMS。

### 尚未形成的用户工作流

- 没有 Schwab 用户侧 CLI；
- 没有 streaming 行情/账户活动；
- 没有 durable OMS/outbox、启动对账器或执行服务；
- 没有真实 Developer App、OAuth、行情 entitlement、限频与账户权限联调；
- 没有把 `LiveTradingGuard` 强制装配到所有 Schwab 调用。

底层 adapter 可以由 Python 调用者在明确解锁后组装生产请求，但这不构成已验收、可运维的交易通路。

账户获批后，只应通过无回显本机输入写入：

~~~text
schwab.client_id
schwab.client_secret
schwab.oauth.token
~~~

网页登录、2FA 和账户选择由用户直接在 Schwab 页面完成；项目不接收券商密码或验证码。

## 运行模式与安全含义

`TradingMode` 与 `LiveTradingGuard` 定义了可复用策略：

- `PAPER`：禁止接触真实 broker；
- `SHADOW`：只允许连接、查询和订阅，拒绝 submit/cancel/replace；
- `LIVE`：要求当前进程内精确确认短语、账户白名单和交易所白名单。

但该 guard 目前不是所有 Binance/Schwab adapter 的全局强制外壳。现有安全性来自具体入口的组合：
Binance durable service 硬限制 Testnet、SHADOW 不含私有 gateway、Schwab 没有用户侧执行入口，
以及底层生产 endpoint 的 `allow_live=True` 显式门。未来若新增 LIVE 编排，必须把 guard、持久
OMS、启动/持续对账、组合风险、kill switch、最小权限凭据和人工发布一起接入，而不能只依赖
`allow_live`。
