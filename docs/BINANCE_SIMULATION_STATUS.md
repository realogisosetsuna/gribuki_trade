# Binance 模拟交易状态与验收边界

更新日期：2026-08-13

## 当前结论

目前已分别跑通三条可重复的 Binance 研究/模拟链路：

1. Binance Global Spot Testnet 的真实远端虚拟下单、成交、手续费、余额与持久化 OMS 对账。
2. Binance 公开生产行情驱动、本地永不远端下单的 PAPER/SHADOW 运行与重启恢复。
3. 闭合历史 K 线归档、点时策略和事件驱动回测。

这意味着“功能闭环”已成立，但还不能称为“无人值守模拟盘已完成生产验收”。长期 soak、故障注入、成交模型真实性、完整风险指标和 GUI 实时接线仍需继续完成。任何 Testnet、Demo、PAPER 或回测结果都不代表未来收益。

## 已验证能力

### Spot Testnet 远端虚拟交易

- HMAC 账户、时钟、权限、交易规则和费率读取。
- 公共行情 WebSocket 与私有 User Data Stream。
- SQLite WAL/FULL OMS：提交和撤单命令先落盘再外发。
- 不确定发送进入 `UNKNOWN`，按 `clientOrderId` 对账，不盲目重发。
- 启动读取 account、openOrders、allOrders、myTrades；存在未解释订单时禁止发送新命令。
- 命令按账户和 symbol 白名单隔离；重启后仍能按 `symbol + clientOrderId` 撤单。
- 私有流重新订阅后自动执行 REST 补账。
- OMS 的成交数量单调，已 `FILLED` 不会被迟到的撤单/过期事件覆盖。

2026-08-13 的真实 Testnet 虚拟成交结果：

- `BTCUSDT`，约 20.32 USDT，数量 0.00032000 BTC。
- 私有流事件为 `NEW → TRADE`，最终状态 `FILLED`。
- 1 笔 fill 已持久化；submit 命令状态 `SENT`。
- REST 对账发现 1 笔成交、0 个活动订单、0 个未解释订单。
- BTC 与 USDT 的 Testnet 虚拟余额变化与成交一致。

此前还验证了 `NEW → CANCELED`、撤单请求结果暂不确定后由私有流和 REST 收敛，以及重启后撤销旧活动订单。

### 历史数据与回测

- 公共生产接口分页下载；以 Binance 服务器时钟判断 K 线是否真正闭合。
- SQLite 增量不可变归档、重复幂等、冲突修订拒绝、缺口检查和 SHA-256 指纹。
- Decimal 现货账本、手续费、滑点、下一 Bar 才允许成交、Bar 成交量上限与部分成交。
- 策略只能读取当时已闭合、已可用的数据。

已归档 BTCUSDT 5 分钟 K 线 25,919 根，约 90 天，0 缺口。数据集指纹：

```text
1caa17548401ac7abc8094bca0b3b9f424ac8ba784931372e5986d65791c9821
```

均线工程基准以 10,000 USDT、60% 目标仓位、2% 再平衡带、0.1% 费率和 0.05% 滑点回放，结果约为 -56.11%，最大回撤约 56.12%，750 笔 fill，费用约 2,927.53 USDT。它是用于暴露高换手成本的负面对照，不是交易推荐。

### 实时 PAPER/SHADOW

- 可使用 Binance Testnet 或公开生产行情；水印始终包含 `NO_REMOTE_ORDERS`。
- 本地 PAPER 订单、free/locked 余额、手续费、成交和 OMS 状态均持久化。
- 只处理闭合 K 线，拒绝重复冲突、Bar 缺口和陈旧行情。
- 实测消费 931 个公开行情事件和 2 根闭合 1 分钟 K 线，生成 2 个本地信号并完成 1 次本地成交。
- 使用同一 SQLite 重启后，恢复了 1 个活动本地订单，并在新盘口到达后完成成交。

有界运行在 Bar 边界结束时可能保留新产生的本地活动订单及锁定资金；这是“保存并在下次恢复”的策略，不代表已平仓。CLI 会输出 `open_paper_order_count`。

当前 PaperBroker 在最优一档把可成交剩余量一次填完，不模拟队列位置、盘口深度、网络延迟或真实市场冲击，因此只能用于管线与状态恢复验证，不能用于评价短线成交质量。

## 各产品模拟环境边界

| 产品 | 官方非生产环境 | 当前状态 |
|---|---|---|
| Spot | Spot Testnet、Demo | Testnet 完整成交闭环；Demo 环境模型已定义，尚未接入完整 Spot gateway |
| USDⓈ-M Futures | Demo Trading | 公共端点已真实连通；鉴权账户、持仓与 `/order/test` 代码已完成，等待独立 Demo Key |
| COIN-M Futures | Demo Trading | 公共端点已真实连通；鉴权账户、持仓与 `/order/test` 代码已完成，等待独立 Demo Key |
| Margin | 没有公开的等价 Spot Testnet/Demo | 明确拒绝，不回退到 Live |
| Portfolio Margin | 没有公开的等价非生产 `/papi` | 明确拒绝，不回退到 Live |

Spot Testnet Key 不能复用于 Futures Demo。官方边界参考：[Spot Testnet](https://github.com/binance/binance-spot-api-docs/blob/master/testnet/general-info.md)、[Spot Demo](https://github.com/binance/binance-spot-api-docs/blob/master/demo-mode/general-info.md)、[USDⓈ-M Futures](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info)、[COIN-M Futures](https://developers.binance.com/docs/derivatives/coin-margined-futures/general-info)、[Portfolio Margin](https://developers.binance.com/docs/derivatives/portfolio-margin/general-info)。

## 可重复命令

```powershell
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-status --symbol BTCUSDT
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-oms-cycle --symbol BTCUSDT --notional 20 --database runtime/binance/testnet-oms.sqlite3 --confirm TESTNET
.\.venv\Scripts\python.exe -m gribuki_trade binance-testnet-oms-fill --symbol BTCUSDT --notional 20 --database runtime/binance/testnet-oms-fill.sqlite3 --confirm TESTNET_FILL

.\.venv\Scripts\python.exe -m gribuki_trade binance-history-sync --symbol BTCUSDT --interval 5m --days 90 --environment LIVE --database runtime/binance/market.sqlite3
.\.venv\Scripts\python.exe -m gribuki_trade binance-backtest --symbol BTCUSDT --interval 5m --environment LIVE --database runtime/binance/market.sqlite3 --initial-quote 10000 --fast-window 12 --slow-window 48 --target-position 0.60 --rebalance-band 0.02 --maker-fee 0.001 --taker-fee 0.001 --slippage 0.0005
.\.venv\Scripts\python.exe -m gribuki_trade binance-shadow-run --symbol BTCUSDT --interval 1m --environment LIVE --closed-bars 3 --database runtime/binance/shadow-oms.sqlite3

.\.venv\Scripts\python.exe -m gribuki_trade binance-futures-demo-status --product USDS_FUTURES
.\.venv\Scripts\python.exe -m gribuki_trade binance-futures-demo-status --product COIN_FUTURES
```

## 距离模拟盘工程验收仍缺什么

1. 把 Shadow/Testnet runner 做成长驻 supervisor，先连续运行 7 天，再扩到 20–30 天。
2. 注入断网、429/418、5xx、私有流断线、进程强退与 Win11 重启；恢复后不得重复订单。
3. 为 Testnet 构造并验证部分成交；逐笔核对余额、手续费资产、订单和成交。
4. 增加心跳、资源、重连、REST 权重、订单计数、陈旧行情和未决状态监控。
5. 完成单笔/单币/总敞口、日损、总回撤、最大活动订单、动作速率和 kill switch 的端到端故障测试。
6. GUI 移除 Binance 演示数据，接入真实 PAPER/Testnet 连接、资金、持仓、订单、成交、费用、风险和对账状态。
7. 将 Paper 成交器升级为使用盘口数量、延迟和参与率的保守模型；tick/深度回放另做高保真版本。
8. Futures 在提供独立 Demo Key 后继续实现真正的 Demo 委托、撤单、用户流、保证金、杠杆、资金费率、标记价格和强平风险。

## 当前需要用户提供什么

继续 Spot Testnet、公开行情 Shadow 和历史回测不需要新增信息，也不需要稳定公网 IPv4、QQ 配置、Live Key 或真实资金。

若要继续两套 Futures Demo 鉴权验证，需要分别在 Binance Futures Demo 创建密钥，并仅在本机凭据库录入：

```text
binance.usds_futures.demo.api_key
binance.usds_futures.demo.secret_key
binance.coin_futures.demo.api_key
binance.coin_futures.demo.secret_key
```

没有固定公网 IPv4 不阻塞当前阶段。未来若进入正式交易，固定出口 IP 仍是强烈建议；不应通过扩大 API 权限来替代网络白名单。
