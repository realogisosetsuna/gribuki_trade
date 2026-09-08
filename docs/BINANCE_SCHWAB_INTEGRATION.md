# Binance 与 Charles Schwab 接入边界

更新日期：2026-09-09

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

底层 LIVE gateway 只有调用者显式传入 `allow_live=True` 才能构造。用户侧 LIVE 入口还必须
通过 `LiveTradingGuard` 的进程内确认、账户白名单和交易所白名单。

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

### Spot LIVE 用户入口

LIVE 命令默认不执行；每次调用都必须显式传入 `ENABLE LIVE TRADING`，并由
`LiveTradingGuard` 在当前进程内校验 Binance 账户白名单。建议先运行只读状态和
`/api/v3/order/test` 校验：

~~~bash
./.venv/Scripts/python.exe -m gribuki_trade binance-live-status \
  --symbol BTCUSDT --confirm "ENABLE LIVE TRADING"
./.venv/Scripts/python.exe -m gribuki_trade binance-live-order-test \
  --symbol BTCUSDT --notional 20 --confirm "ENABLE LIVE TRADING"
~~~

需要真实提交或撤单时使用 durable SQLite OMS；`submit` 会进入真实撮合，`cancel` 只
接受该 OMS 已记录的 `client_order_id`：

~~~bash
./.venv/Scripts/python.exe -m gribuki_trade binance-live-order submit \
  --symbol BTCUSDT --notional 20 --database runtime/binance/live-oms.sqlite3 \
  --confirm "ENABLE LIVE TRADING"
./.venv/Scripts/python.exe -m gribuki_trade binance-live-order cancel \
  --client-order-id <client-order-id> --database runtime/binance/live-oms.sqlite3 \
  --confirm "ENABLE LIVE TRADING"
~~~

LIVE 凭据只从 `binance.live.api_key` 和 `binance.live.secret_key` 加载；命令不会回退
到 Testnet 凭据。状态和 order-test 不创建真实订单，离线测试也不会调用 LIVE 网络。
Spot LIVE 提交结果会返回 `reason` 和 `broker_error_code`；例如余额不足时可以直接看到
交易所拒绝原因，不能把 `order-test` 的 accepted 当成资金已经足够。

详细余额使用独立的只读命令；默认展示非零资产，`--include-zero` 展示全部返回资产，
`--asset USDT` 可筛选指定币种。Spot 和 USDⓈ-M 是分别查询的钱包，并非币安全部账户资产。

API 白名单使用当前代理的公网出口地址时，可在同一个 Git Bash 环境快速查询：

~~~bash
bash ./scripts/current_ip.sh
bash ./scripts/current_ip.sh --verbose
~~~

脚本只输出公网 IPv4/IPv6，不会把本机局域网地址或虚拟网卡地址误当成白名单地址。
如果交易进程和 Git Bash 使用不同的 `HTTPS_PROXY`/`ALL_PROXY`，应在交易进程相同的网络环境中查询。

~~~bash
./.venv/Scripts/python.exe -m gribuki_trade binance-live-balance \
  --asset USDT --confirm "ENABLE LIVE TRADING"
./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-balance \
  --confirm "ENABLE LIVE TRADING"
~~~

Spot 的 `free` 为可用余额、`locked` 为冻结余额；合约的 `walletBalance` 为钱包余额，
`availableBalance` 为可用余额，`unrealizedProfit` 为未实现盈亏。合约账户汇总的单位取决于
单资产或多资产模式，应结合逐币种余额解读，不将不同币种直接相加。

前两条分别只读或使用 Binance `order/test`；后两条会进入 Testnet 撮合或虚拟成交。历史本机验证
记录见 [Binance 模拟交易验收快照](BINANCE_SIMULATION_STATUS.md)。

### 历史研究与本地 SHADOW

`binance-history-sync` 只归档已闭合 K 线，冲突修订失败关闭并生成数据集 SHA-256；
`binance-backtest` 使用 point-in-time、下一 bar 才成交的单交易对 Spot 模型；
`binance-shadow-run` 用公开行情驱动本地 PAPER/OMS，水印固定为 `NO_REMOTE_ORDERS`。
SHADOW 选择 `--environment LIVE` 只代表生产公共行情，不会打开远端订单提交。

### USDⓈ-M Futures LIVE 用户入口

Futures REST 客户端现在支持签名账户、持仓、挂单、历史成交查询、订单提交、单笔撤单和撤销
全部挂单；`BinanceFuturesExecutionService` 在 LIVE 下强制 `allow_live=True` 与
`LiveTradingGuard`，并提供启动对账。Spot Testnet key 不会复用为 Futures Demo key；不支持的
Margin/Portfolio Margin 阶段不会回退到 LIVE。

先运行只读状态和 `order/test`：

~~~bash
./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-status \
  --symbol BTCUSDT --confirm "ENABLE LIVE TRADING"
./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-order-test \
  --symbol BTCUSDT --side BUY --quantity 0.001 --confirm "ENABLE LIVE TRADING"
~~~

状态结果中的 `position_mode` 会是 `ONE_WAY` 或 `HEDGE`。单向持仓使用
`--position-side BOTH`（省略时客户端也会安全地补成 `BOTH`）；双向持仓必须显式传
`--position-side LONG` 或 `--position-side SHORT`，系统不会根据 BUY/SELL 猜测，也不会
自动修改账户持仓模式。`time_sync_rtt_ms` 是同步请求耗时；`clock_offset_ms` 若持续达到
数万毫秒，先同步本机系统时钟再进行交易。

真实 Futures 提交/撤单必须显式指定 `submit` 或 `cancel`，并继续通过相同的 LIVE 守卫：

~~~bash
./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-order submit \
  --symbol BTCUSDT --side BUY --position-side BOTH --order-type LIMIT --quantity 0.001 --price 50000 \
  --confirm "ENABLE LIVE TRADING"
./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-order cancel \
  --symbol BTCUSDT --client-order-id <client-order-id> \
  --confirm "ENABLE LIVE TRADING"
~~~

`order-test` 不进入撮合；`submit` 会创建真实订单。无人值守合约运行入口使用独立的 Futures
SQLite OMS。它启动时先同步时间并对账余额、Hedge 仓位、普通订单和 Algo 订单，再连接
`/private` 用户流；断线换代会重复对账，网络不确定的提交会持久化为 `UNKNOWN`，不会盲目重试。
私有流不健康时，新的订单变化会被拒绝：

~~~bash
./.venv/Scripts/python.exe -m gribuki_trade binance-live-futures-stream \
  --symbol BTCUSDT --database runtime/binance/live-futures-oms.sqlite3 \
  --confirm "ENABLE LIVE TRADING"
~~~

该命令只监听和对账，不提交新订单；`--max-events N` 可用于受控验收后自动退出。策略层应通过
`BinanceFuturesUnattendedExecutionService.submit_order` 和 `submit_algo_order` 发单，把客户端
订单号/Algo 客户端编号作为幂等键。Algo 订单的 `algoId` 与触发后的实际 `orderId` 会分别保存并
建立父子关联。动态止盈止损使用新的保护计划 revision，撤销或超时后必须先查询 Algo 状态再继续。

### 仍未完成

- Spot LIVE 与 USDⓈ-M Futures LIVE 均已提供受保护的用户 CLI；两者不能复用交易端点，均只从
  `binance.live.api_key`/`binance.live.secret_key` 读取凭据。
- LiveTradingGuard / GuardedBrokerAdapter 是可复用安全构件；Spot LIVE CLI 的每次
  连接、查询、提交和撤单都在调用网关前进行进程内确认、账户白名单和交易所白名单校验；
  kill switch 和 GUI 生产接线；
- 本地 PAPER/回测成交模型不复制真实队列、深度、延迟和市场冲击。

### Spot 与 USDⓈ-M Futures API 对照及策略接口

以下接口对应 Binance 官方文档的 Core Spot Trade 与 USDⓈ-M Futures Trade
REST API。两类产品必须使用各自的基址、签名请求和订单参数，不能把现货的
`trailingDelta` 传给合约，也不能把合约的 `callbackRate` 传给现货。

| 能力 | Spot | USDⓈ-M Futures |
|---|---|---|
| REST 基址 | `https://api.binance.com/api/v3` | `https://fapi.binance.com/fapi/v1` |
| 下单 | `POST /order` | `POST /order` |
| 测试单 | `POST /order/test` | `POST /order/test` |
| 查询/撤单 | `GET/DELETE /order` | `GET/DELETE /order` |
| 保护组合 | `orderList/oco`、`orderList/oto`、`orderList/otoco` | 条件单或 `algoOrder` |
| 移动止盈止损 | `trailingDelta`，整数 BIPS | `callbackRate`，百分比；普通单 0.1–5，Algo 0.1–10 |
| 杠杆/保证金 | 不适用 | `leverage`、`marginType`、持仓模式、多资产模式 |
| 持仓方向 | 现货资产余额 | 单向 `BOTH` 或 Hedge 的 `LONG/SHORT` |
| 低延迟状态 | Spot User Data Stream 的订单/余额事件 | Futures User Data Stream 的订单/账户/持仓事件 |

现货策略通过 `BinanceSpotGateway` 的结构化接口调用：
`submit_spot_order` 支持原生止损、止盈和 trailingDelta；
`submit_oco`、`submit_oto`、`submit_otoco` 创建条件订单列表；
`cancel_replace` 用交易所的 cancel-replace 更新动态保护单；
`cancel_order_list` 和 `cancel_all_open_orders` 用于撤销保护组合或标的全部挂单。
请求对象是 `BinanceSpotOrderLeg`、`BinanceSpotOcoRequest`、
`BinanceSpotOtoRequest` 和 `BinanceSpotOtocoRequest`，策略不需要拼接原始
URL 或签名参数。

合约策略通过 `BinanceFuturesExecutionService` 调用：

- `set_leverage(symbol, leverage)` 设置单个合约杠杆。API 接受 1–125，实际可用上限仍受风险档位和名义价值限制；返回值中的 `maxNotionalValue` 必须保存并用于风险判断。
- `set_margin_type(symbol, "ISOLATED"|"CROSSED")` 设置逐合约保证金模式。
- `set_position_mode(True|False)` 设置全账户单向/双向持仓；有持仓或挂单时 Binance 可能拒绝切换。
- `set_multi_assets_mode(True|False)` 设置 USDⓈ-M 多资产保证金模式。
- `submit_protection_order(BinanceFuturesProtectionOrder(...))` 统一表达止损、止盈和移动止损。
- `submit_algo_order`、`submit_algo_trailing_stop`、`get_algo_order`、`open_algo_orders`、`cancel_algo_order` 和 `cancel_all_algo_orders` 对应当前官方 `/fapi/v1/algoOrder` 生命周期。
- `replace_protection_order` 执行撤销旧保护单后创建新保护单，并返回两次结果供对账；条件单没有被假设为可原地修改。

在当前账户为 Hedge Mode 时，开多使用 `BUY + LONG`，保护多仓使用
`SELL + LONG`；开空使用 `SELL + SHORT`，保护空仓使用 `BUY + SHORT`。
Hedge Mode 下不能发送 `reduceOnly`，`closePosition=true` 不能和
`quantity` 同时发送；移动止损必须提供数量且不能使用 `closePosition`。

官方接口参考：

- [Spot Trade REST API](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/trade)
- [USDⓈ-M Futures Trade REST API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)
- [Spot User Data Stream](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-api/user-data-stream)
- [USDⓈ-M Futures User Data Streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/user-data-streams)

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
