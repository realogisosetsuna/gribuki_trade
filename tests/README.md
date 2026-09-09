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

# 按模块名筛选
python -m pytest --temp-dir runtime/tmp tests/unit/binance/test_binance_futures_parsing.py -q
```

测试夹具统一放在 `tests/fixtures/`，跨测试的 pytest 配置保持在仓库根目录的
`conftest.py` 或 `tests/conftest.py`。
