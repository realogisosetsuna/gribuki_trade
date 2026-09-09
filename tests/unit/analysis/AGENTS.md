# Analysis 测试地图

本目录覆盖 close/cross-market/crypto/exit/technical/cost 纯计算契约。
实现位于 `src/gribuki_trade/analysis/`、`features/` 和 `backtest/`；测试不应
访问网络、SQLite 或 broker。

运行 `python -m pytest --temp-dir runtime/analysis-tests -q tests/unit/analysis`。
