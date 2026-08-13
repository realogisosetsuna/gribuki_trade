# Binance Spot 与 Charles Schwab 接入说明

本文记录截至 2026-08-13 已落地的代码、真实验证结果和仍未完成的生产门槛。任何 Testnet/PAPER 结果都不代表真实成交质量或策略收益。

## Binance Spot

### 已实现

- REST 公共行情：连通性、服务器时间、`exchangeInfo`、ticker、订单簿、K 线。
- HMAC-SHA-256 签名账户接口：账户、限价委托、查询、撤单、`/api/v3/order/test`。
- Testnet 与 Live 使用不同 URL、不同本机凭据名；不存在环境间凭据回退。
- Live 构造必须显式 `allow_live=True`，上层另有 PAPER/SHADOW/LIVE、账户白名单、交易所白名单和本机确认短语。
- `Decimal` 精确数量、价格和本地 `PRICE_FILTER`、`LOT_SIZE`、`MIN_NOTIONAL/NOTIONAL` 检查。
- 本机时钟偏差以服务器请求往返中点校准；遇到 `-1021` 只校准并重试一次。
- `-1007`、写请求 5xx 或网络中断进入 `UNKNOWN`，同一客户端订单号不会盲目重发。
- 公共 WebSocket 支持 `bookTicker`、`trade`、`kline`、组合流、心跳、有限重连及序列回退检测。
- 用户订单/余额事件通过当前 WebSocket API 的 User Data Stream 接入，不使用旧式页面逆向或非公开接口。

官方依据：[Spot REST](https://developers.binance.com/en/docs/products/spot/rest-api)、[Testnet WebSocket Streams](https://developers.binance.com/en/docs/products/spot/testnet/web-socket-streams)、[User Data Stream](https://developers.binance.com/en/docs/products/spot/testnet/user-data-stream)。

### 本机凭据名

凭据存放在 Windows Credential Manager/macOS Keychain 的 `gribuki-trade` 服务下，源代码只包含名称：

- `binance.testnet.api_key`
- `binance.testnet.secret_key`
- `binance.live.api_key`
- `binance.live.secret_key`

Testnet Key 已在本机凭据库中可用。正式环境凭据未配置，也不会自动复用 Testnet Key。

### 已完成的真实 Testnet 验证

- 公开 REST ping、服务器时间、签名账户查询通过。
- 发现本机时钟约偏离服务器 54 秒，自动校准后签名查询通过。
- `/api/v3/order/test` 接受约 20 USDT 的 BTCUSDT 虚拟订单参数，未进入撮合。
- 一笔约 20 USDT 的 Testnet BTCUSDT 虚拟限价单完成 `ACCEPTED → 查询 → CANCELED`，没有残留挂单。
- 公共 WebSocket 已真实收到并解析 BTCUSDT `bookTicker` 与 `trade`。
- 私有 WebSocket API 已完成签名订阅；同一虚拟委托收到 `NEW/ACCEPTED → CANCELED/CANCELED`，与 REST 和本地状态一致。

可重复执行：

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-status --symbol BTCUSDT
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-order-test --symbol BTCUSDT --notional 20
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-cycle --symbol BTCUSDT --notional 20 --confirm TESTNET
```

最后一条会向 Testnet 撮合引擎发送虚拟委托；若委托仍可撤，会立即撤单。它永远使用 Testnet 环境和 Testnet 凭据名。

### 正式环境前仍需完成

- SQLite WAL 订单事件库、外发前 durable outbox、启动与持续对账。
- 把 User Data Stream 的执行回报接入 OMS，并覆盖断线期间 REST 补偿查询。
- 完整解析动态限频、订单计数以及更多 symbol filters；目前不能仅凭已有四类 filter 放行所有订单类型。
- 资金冻结、成交账本、费用、仓位、最大亏损、行情陈旧、报单速率和 kill switch。
- Testnet 至少连续运行与故障演练；Live 先只读，再用独立最小权限 Key 和极小金额人工确认。

## Charles Schwab

### 已实现

- OAuth 2.0 Authorization Code：一次性 `state`、redirect URI 精确匹配、code 交换、依据 `expires_in` 刷新。
- Access/refresh token 可通过 OS 凭据库存储，不写 JSON 明文文件。
- 生产 `httpx` 异步连接池和可注入离线 transport。
- Market Data REST：quotes、price history、option chain、market hours。
- Trader REST：账户号到 hash 映射、账户/持仓、订单列表/单笔订单、下单和撤单。
- 简单股票限价单；股票/期权数量必须为整数，期权必须显式给出开平仓 instruction。
- 401 只刷新并重试一次；429 保留 `Retry-After`；写请求超时/5xx 不自动重发并进入 `UNKNOWN`。
- Production OAuth 和 REST 默认拒绝，只有显式 `allow_live=True` 才能构造。

### 账户开通后需要配置

Schwab Developer Portal 的 App 达到 `Ready to Use` 后，在本机凭据库写入：

- `schwab.client_id`
- `schwab.client_secret`

OAuth token 会写入 `schwab.oauth.token`。还需要从 Portal 确认并逐字符提供 callback URI；网页登录、2FA、账户选择由用户直接在 Schwab 页面完成，程序不接收券商密码或验证码。

App 凭据到手后用无回显提示写入本机凭据库：

```powershell
gribuki-trade secret-set schwab.client_id
gribuki-trade secret-set schwab.client_secret
gribuki-trade secret-status
```

### 尚未做真实验证的部分

- 尚无 App Key/Secret，因此没有向 Schwab 生产域名发送任何请求。
- Schwab 没有已确认可供 Trader API 使用的公开 sandbox；开发期用本地 PaperBroker 和离线 transport。
- 获批后的具体 OAuth token 条款、限频、行情 entitlement、streamer 字段和账户权限必须按当日 Portal 文档与真实响应复核。
- Streaming 行情/账户活动、订单 preview、复杂期权单和真实对账尚未开放。

## 运行模式

- `PAPER`：只允许本地 PaperBroker，不访问真实券商。
- `SHADOW`：真实接口只读/订阅，提交、撤单和改单在调用适配器之前被拒绝。
- `LIVE`：必须同时满足本机无回显确认、账户白名单和交易所白名单；解锁只在当前进程内有效。

策略与 GUI 不直接导入券商 SDK。后续的唯一调用方向为：策略信号 → 组合/执行计划 → 风控 → 持久化 OMS → 受守卫保护的 BrokerAdapter。
