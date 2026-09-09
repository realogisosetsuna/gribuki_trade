# 适配器测试地图

本文件细化根 `AGENTS.md` 的测试定位规则；PAPER/SHADOW/LIVE 边界不变。
先读 [`ARCHITECTURE.md`](../../../ARCHITECTURE.md) 和
[`data-lineage.md`](../../../docs/architecture/data-lineage.md)。

- `market_data/`：AKShare、Baostock、归档和跨市场行情协议。
- `ashare/`：A 股供应商和 instrument profile 协议。
- `macro/`：CBOE VIX、官方利率协议。
- `simulated/`：手工 ticket、PAPER broker 和 account 适配器。
- 夹具从 `tests/fixtures/` 读取；测试只使用固定 payload 或本地 transport。
- 不在供应商领域下继续为单个模块建立目录。

运行 `python -m pytest --temp-dir runtime/adapter-tests -q tests/unit/adapters`。
