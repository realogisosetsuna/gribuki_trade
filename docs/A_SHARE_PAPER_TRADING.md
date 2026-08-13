# A 股 PAPER 账本与保守日线撮合

本模块为本地、无券商、无网络副作用的研究系统。持久账本解决“已经确定的一笔成交如何记账、重放和审计”；保守日线撮合器只执行明确的模拟假设；持久 wrapper 再以 append-only 事件和恢复 saga 保存委托及 bar-run。任何一层都不声称还原盘口排队或真实成交。

## 模块边界

- `domain/paper_trading.py`：成交、费用、持仓、账户快照和账本事件值对象。
- `ports/paper_ledger.py`：服务所依赖的最小持久化协议。
- `storage/paper_ledger.py`：SQLite WAL、append-only、逐账户哈希链实现。
- `services/ashare_paper.py`：开户、成交录入、交易日 rollover、状态重放。
- `domain/paper_orders.py`：六态限价委托、显式价格区间、撮合 bar 和结果值对象。
- `services/ashare_paper_matching.py`：FIFO、成交量参与率和 next-bar 保守撮合。
- `storage/paper_orders.py`：append-only 委托/run 事件、hash chain、run identity 和 writer lease。
- `services/ashare_paper_recovery.py`：跨订单库与资金账本的可恢复 saga。
- `backtest/costs.py`：模拟盘与回测共用的费用计算器；现已包含可配置过户费。

服务没有导入行情或券商适配器。人工成交和模拟成交都使用 `ASharePaperFill`，仅用 `source=MANUAL/SIMULATED` 区分来源。人工账单允许录入实际费用覆盖值；模拟成交必须使用配置费率。

## 第二阶段撮合能力

- 委托状态为 `PENDING/PARTIALLY_FILLED/FILLED/CANCELLED/REJECTED/EXPIRED`；
- 只处理决策日之后、调用方明确提供的未复权完整日线；
- 缺 OHLC、零成交量、停牌、缺价格区间或 bar 超出区间均不成交；
- 买入为 100 股整手，卖出允许清理零股尾仓；
- 限价必须被 bar 触及，成交价使用不突破限价的保守方向滑点；
- 默认最多使用 bar 成交股数的 1%，按委托时间 FIFO 分配，可跨 bar 部分成交；
- 确定性 `fill_id` 写入上述持久账本，重复 bar 精确幂等，修订冲突失败关闭；
- 部分成交按同一订单累计最低佣金，避免每个 bar 重复收取一遍最低佣金。

纯撮合器仍是确定性内存引擎；生产入口应使用第三阶段持久 wrapper。它可恢复未完成提交、订单状态和 bar-run，并依赖资金账本的确定性 `fill_id` 防止跨库崩溃后重复扣款。当前尚无撮合 CLI/GUI，调用方必须显式 `recover()` 后再使用。

## 已实现不变量

1. 资金不得为负，失败成交不会追加事件。
2. 持仓满足 `quantity = available_to_sell + today_buy`。
3. 买入计入 `today_buy`，同一交易日不可卖；显式 rollover 到下一个已确认交易日后才变为 `available_to_sell`。
4. 卖出数量不得超过 `available_to_sell`，因此不会裸卖或超卖。
5. `fill_id` 是账户内成交幂等键：同内容重放不重复记账，不同内容复用同一编号会失败。
6. 每笔成交保存最终成交额、佣金、过户费、印花税、现金变化和已实现盈亏，后续修改默认费率不会改变历史结果。
7. SQLite 使用 WAL 和 `synchronous=FULL`；数据库触发器禁止 UPDATE/DELETE。
8. 账户快照只由事件重放生成。进程重启、历史审计和日常查询走同一投影代码。
9. 每个账户的事件带前序哈希；读取时验证 payload 摘要和哈希链。
10. rollover 只接受更晚日期。服务不把自然日误当交易日，调用方必须提供经交易所日历确认的交易日。

## 费用假设

默认个人佣金是假设值 `0.03%`、每笔最低 5 元，必须按实际券商合同修改。股票过户费默认按成交额 `0.001%` 双向，股票卖出印花税默认 `0.05%`；ETF 默认不收过户费和印花税。所有费率均可配置，人工账单也可逐笔覆盖实际费用。

参考依据：

- 中国结算自 2022-04-29 将股票交易过户费统一调整为成交金额 `0.01‰` 双向收取：<https://www.xinhuanet.com/2022-04/28/c_1128605983.htm>
- 财政部、税务总局公告自 2023-08-28 将证券交易印花税减半征收：<https://fgk.chinatax.gov.cn/zcfgk/c102416/c5211343/content.html>

这些是默认建模依据，不替代券商实际交割单；费率发生变化时应更新配置，不应回写历史事件。

## 使用示例

CLI 已提供显式、有限动作入口；它只维护账本，不会连接行情或自动撮合：

```powershell
python -m gribuki_trade ashare-paper open --account personal-paper `
  --initial-cash 100000 --session-date 2026-08-14

python -m gribuki_trade ashare-paper fill --account personal-paper `
  --symbol 600000.SH --side BUY --quantity 100 --price 10.00 `
  --instrument STOCK --source MANUAL --session-date 2026-08-14

python -m gribuki_trade ashare-paper snapshot --account personal-paper
```

`rollover` 必须传入已经由交易日历确认的下一交易日；`fills` 用于读取不可变成交历史。模拟成交和人工成交使用同一个账本契约，但 CLI 不会把推荐自动转换为成交。

Python API 示例：

```python
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from gribuki_trade.domain import (
    ASharePaperFill,
    PaperFillSource,
    PaperInstrumentType,
    Side,
)
from gribuki_trade.services import ASharePaperTradingService
from gribuki_trade.storage import SQLitePaperLedger

shanghai = ZoneInfo("Asia/Shanghai")

with SQLitePaperLedger("runtime/research/ashare-paper.sqlite3") as ledger:
    service = ASharePaperTradingService(ledger)
    service.open_account(
        "personal-paper",
        initial_cash=Decimal("100000"),
        session_date=date(2026, 8, 14),
        opened_at=datetime(2026, 8, 14, 8, 30, tzinfo=shanghai),
    )
    receipt = service.record_fill(
        ASharePaperFill(
            account_id="personal-paper",
            fill_id="manual-20260814-001",
            symbol="600000.SH",
            side=Side.BUY,
            quantity=100,
            price=Decimal("10.00"),
            instrument_type=PaperInstrumentType.STOCK,
            trading_date=date(2026, 8, 14),
            executed_at=datetime(2026, 8, 14, 10, 0, tzinfo=shanghai),
            source=PaperFillSource.MANUAL,
        )
    )
```

## 持久订单与崩溃恢复

第三阶段新增 `SQLitePaperOrderStore` 与
`DurableASharePaperOrderMatcher`。它们不改变保守日线撮合规则，只把订单状态和
bar-run 运行协议持久化：

1. 提交先写 `ORDER_SUBMISSION_STARTED`，验证/预留后写
   `ORDER_STATE_APPLIED`；未完成提交会阻止 bar run，恢复时可重建。
2. 撮合前写 `RUN_STARTED`，其中保留完整原始 bar、价格限制、来源版本、撮合
   配置以及所有订单的运行前快照。
3. 确定性 `fill_id` 先通过资金账本幂等记账，再写
   `ORDER_FILL_APPLIED`，最后写订单状态和 `RUN_COMPLETED`。
4. 若进程在两个 SQLite 数据库之间崩溃，重启扫描 incomplete run 并从
   `RUN_STARTED` 重算；账本已存在的同一 `fill_id` 不会重复扣款。
5. SQLite `BEGIN IMMEDIATE`、全局单个未完成 run、run lease 与全局 durable-wrapper
   writer lease 防止并发撮合和并发预算预留。另一实例须待全局 lease 到期后接管，
   并先执行完整恢复；未完成 run 存在时也不能提交或变更订单。
6. 同一 `symbol / trade_date / config` 只能绑定一个 bar/source revision 和运行前
   订单快照。精确重放返回 `applied_new=False` 和容量摘要，但 `outcomes=()`：历史
   receipt 含账户投影，系统不会伪造 receipt；完整结果仍可从事件与账本审计。

订单事件和运行身份有禁止 `UPDATE/DELETE` 的触发器及 SHA-256 hash chain。lease
表是唯一可变表，只承担单写者协调，不承载业务事实。订单库和资金账本不是同一
数据库，因此这里明确采用 saga，不宣称跨库 ACID。

运行库门槛：本次本机 `sqlite3.sqlite_version` 为 3.50.4，属于 SQLite 官方 2026 年披露的
WAL-reset 缺陷可能影响范围。writer lease 只能处理业务并发，不能修复 checkpointer/writer
竞争。升级至 3.50.7、3.44.6 或 >=3.51.3 前，同一订单库或资金账本路径必须保持单进程、
单 Store 实例；不得把 durable wrapper 放入多 worker 部署。官方说明：
<https://www.sqlite.org/wal.html#walresetbug>。

## 明确未实现

- durable wrapper 尚未接入撮合 CLI/GUI；调用方必须显式执行 `recover()`。
- 自动推导历史涨跌停规则、停复牌日历、集合竞价、盘口队列或盘中路径；价格区间必须由调用方明确提供。
- 盘口冲击、真实排队位置和逐笔成交模拟；日线模型只能给出保守假设。
- T+0 ETF/债券/跨境品种的差异化交收；第一阶段账本按 T+1 管理支持的股票和 ETF。
- 分红、送股、配股、拆并股等公司行动。
- 现金存取、融资融券、多币种、冻结资金和未成交委托预占。
- GUI 账户页、账单导入和持仓图表。CLI 已实现，但仍坚持“明确动作、无自动撮合”的边界。

在上述撮合与公司行动数据没有可靠来源前，系统应继续拒绝伪造“接近实盘”的成交结果。
