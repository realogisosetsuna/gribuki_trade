# GUI 测试地图

本目录覆盖 Qt smoke、integration gateway、NapCat 生命周期和界面配置。
实现位于 `src/gribuki_trade/gui/`；测试使用本地临时目录和 fake transport，
不连接真实 broker 或外部通知服务。

运行 `python -m pytest --temp-dir runtime/gui-tests -q tests/unit/gui`。
