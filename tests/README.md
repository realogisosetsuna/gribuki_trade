# 测试目录布局

`tests/unit/` 按被测边界分组，文件名仍保留 `test_<module>.py` 形式。这样可以
先按领域定位测试，再按模块名查找具体契约；pytest 仍然递归收集整个 `tests/`
目录，因此现有的全量命令和 CI 配置无需改变。

## 单元测试分组

| 目录 | 覆盖范围 |
|---|---|
| `ashare/` | A 股研究、筛选、PAPER、盘前、盘后和日内流程 |
| `binance/` | Binance Spot、USDⓈ-M Futures、监控、User Data Stream 和执行 |
| `trading/` | Broker-neutral OMS、订单、仓位策略和 Futures OMS |
| `storage/` | SQLite store、事件、outbox、租约、恢复和持久化策略 |
| `adapters/market_data/` | AKShare、Baostock、归档行情、跨市场行情和纯 payload 解码 |
| `adapters/ashare/` | A 股供应商接口及 instrument profile 适配契约 |
| `adapters/macro/` | CBOE VIX 和官方利率数据适配契约 |
| `adapters/simulated/` | 手工 ticket、PAPER broker 和 PAPER account 适配契约 |

其余测试组会按同一规则逐步迁移到 `tests/unit/<领域>/`。迁移只改变文件路径，
不会改变模块导入或 pytest 节点中的测试函数名。

## 常用命令

```bash
# 全量测试，pytest 会递归发现所有分组目录
python -m pytest --temp-dir runtime/tmp -q

# 只运行一个领域
python -m pytest --temp-dir runtime/tmp tests/unit/binance -q
python -m pytest --temp-dir runtime/tmp tests/unit/ashare -q
python -m pytest --temp-dir runtime/tmp tests/unit/trading -q
python -m pytest --temp-dir runtime/tmp tests/unit/storage -q
python -m pytest --temp-dir runtime/tmp tests/unit/adapters/market_data -q

# 按模块名筛选
python -m pytest --temp-dir runtime/tmp tests/unit/binance/test_binance_futures_parsing.py -q
```

测试夹具统一放在 `tests/fixtures/`，跨测试的 pytest 配置保持在仓库根目录的
`conftest.py` 或 `tests/conftest.py`。

适配器测试在 `adapters/` 下最多再按一个供应商领域分层，不为单个测试文件
继续增加目录。已有 `ashare/`、`binance/` 领域测试仍保留完整业务流程覆盖；
测试归属按主要被测边界判断，不能只因它使用了 SQLite 或模拟 adapter 就搬入
`storage/` 或 `adapters/`。恢复和持久化不变量的专门测试归入 `storage/`。
