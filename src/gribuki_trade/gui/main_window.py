"""The PAPER-only desktop workstation shell.

The widgets deliberately use local demonstration data.  No broker adapter is
imported here and no control can submit a real order.
"""

from __future__ import annotations

from datetime import datetime
from math import sin

import pyqtgraph as pg
from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QCloseEvent, QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMainWindow,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QSplitter,
    QStatusBar,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

_STYLE = """
QMainWindow, QWidget {
    background: #0b1220;
    color: #dbe7f5;
    font-family: "Microsoft YaHei UI", "PingFang SC", sans-serif;
    font-size: 13px;
}
QFrame#topBar {
    background: #101b2d;
    border-bottom: 1px solid #24344e;
}
QLabel#brand { font-size: 19px; font-weight: 700; color: #f7fbff; }
QLabel#subtitle { color: #7890ad; }
QLabel#modeBadge {
    background: #153d35;
    color: #64e0ae;
    border: 1px solid #28725f;
    border-radius: 7px;
    padding: 7px 14px;
    font-size: 14px;
    font-weight: 700;
}
QLabel#offlineBadge {
    background: #202c3f;
    color: #9db0c8;
    border: 1px solid #35465f;
    border-radius: 7px;
    padding: 7px 11px;
}
QFrame#card {
    background: #111c2e;
    border: 1px solid #233450;
    border-radius: 9px;
}
QLabel#cardTitle { color: #8196af; font-size: 12px; }
QLabel#cardValue { color: #f4f8fd; font-size: 23px; font-weight: 700; }
QLabel#positive { color: #ef6a6a; font-weight: 650; }
QLabel#negative { color: #54c99c; font-weight: 650; }
QTabWidget::pane { border: 0; top: -1px; background: #0b1220; }
QTabBar::tab {
    background: #0b1220;
    color: #8396ae;
    padding: 12px 18px;
    border-bottom: 2px solid transparent;
}
QTabBar::tab:selected { color: #e8f2ff; border-bottom-color: #4d91ff; }
QTabBar::tab:hover { color: #c9d9ed; background: #101a2b; }
QGroupBox {
    background: #111c2e;
    border: 1px solid #233450;
    border-radius: 9px;
    margin-top: 12px;
    padding: 13px 10px 10px 10px;
    font-weight: 650;
}
QGroupBox::title { subcontrol-origin: margin; left: 13px; padding: 0 5px; }
QTableWidget {
    background: #111c2e;
    alternate-background-color: #0e1828;
    border: 1px solid #233450;
    border-radius: 7px;
    gridline-color: #1c2b42;
    selection-background-color: #244f80;
}
QHeaderView::section {
    background: #162338;
    color: #98abc1;
    padding: 8px;
    border: 0;
    border-right: 1px solid #253650;
    font-weight: 600;
}
QPushButton {
    background: #2767bd;
    color: white;
    border: 0;
    border-radius: 6px;
    padding: 8px 15px;
    font-weight: 600;
}
QPushButton:hover { background: #3479d2; }
QPushButton:pressed { background: #1f559d; }
QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {
    background: #0c1626;
    color: #e0ebf8;
    border: 1px solid #2a3d59;
    border-radius: 5px;
    padding: 6px;
}
QPlainTextEdit {
    background: #08101c;
    color: #a9bdd4;
    border: 1px solid #233450;
    border-radius: 7px;
    font-family: Consolas, "SF Mono", monospace;
}
QProgressBar {
    background: #0c1626;
    border: 1px solid #283a55;
    border-radius: 5px;
    text-align: center;
    color: #dce8f7;
}
QProgressBar::chunk { background: #3477c9; border-radius: 4px; }
QStatusBar { background: #0c1524; color: #8094ad; }
QScrollBar:vertical { background: #0c1524; width: 10px; }
QScrollBar::handle:vertical { background: #2c405b; border-radius: 5px; min-height: 24px; }
"""


class MetricCard(QFrame):
    """Compact overview metric."""

    def __init__(self, title: str, value: str, detail: str, tone: str = "neutral") -> None:
        super().__init__()
        self.setObjectName("card")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        title_label = QLabel(title)
        title_label.setObjectName("cardTitle")
        value_label = QLabel(value)
        value_label.setObjectName("cardValue")
        detail_label = QLabel(detail)
        if tone in {"positive", "negative"}:
            detail_label.setObjectName(tone)
        else:
            detail_label.setStyleSheet("color: #7f94ad")
        layout.addWidget(title_label)
        layout.addWidget(value_label)
        layout.addWidget(detail_label)


class TradingMainWindow(QMainWindow):
    """PAPER-only main window used by the desktop prototype."""

    message_requested = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self.setObjectName("tradingMainWindow")
        self.setWindowTitle("Gribuki Trade · PAPER 工作台")
        self.resize(1360, 850)
        self.setMinimumSize(1040, 680)
        self.setStyleSheet(_STYLE)

        self._clock_label = QLabel()
        self._tabs = QTabWidget()
        self._tabs.setObjectName("workspaceTabs")
        self._tabs.setDocumentMode(True)

        root = QWidget()
        root_layout = QVBoxLayout(root)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)
        root_layout.addWidget(self._build_top_bar())
        root_layout.addWidget(self._tabs, 1)
        self.setCentralWidget(root)

        self._tabs.addTab(self._build_overview_tab(), "总览")
        self._tabs.addTab(self._build_chart_tab(), "K线")
        self._tabs.addTab(self._build_research_tab(), "资讯 / 建议")
        self._tabs.addTab(self._build_strategy_tab(), "策略参数")
        self._tabs.addTab(self._build_backtest_tab(), "回测报告")
        self._tabs.addTab(self._build_orders_tab(), "订单 / 成交")
        self._tabs.addTab(self._build_risk_tab(), "风控 / 日志")

        status = QStatusBar()
        status.showMessage("PAPER 环境 · 本地演示数据 · 未连接任何券商")
        self.setStatusBar(status)
        self.message_requested.connect(status.showMessage)

        self._clock_timer = QTimer(self)
        self._clock_timer.setInterval(1_000)
        self._clock_timer.timeout.connect(self._update_clock)
        self._clock_timer.start()
        self._update_clock()

    @property
    def workspace_tabs(self) -> QTabWidget:
        """Expose the workspace tabs for UI tests and automation."""

        return self._tabs

    def _build_top_bar(self) -> QWidget:
        bar = QFrame()
        bar.setObjectName("topBar")
        layout = QHBoxLayout(bar)
        layout.setContentsMargins(22, 13, 22, 13)

        identity = QVBoxLayout()
        identity.setSpacing(1)
        brand = QLabel("GRIBUKI TRADE")
        brand.setObjectName("brand")
        subtitle = QLabel("A股自动化交易工作台 · 桌面原型")
        subtitle.setObjectName("subtitle")
        identity.addWidget(brand)
        identity.addWidget(subtitle)
        layout.addLayout(identity)
        layout.addStretch()

        self._clock_label.setStyleSheet("color: #8da1b8; padding-right: 8px")
        layout.addWidget(self._clock_label)
        offline = QLabel("●  数据源离线")
        offline.setObjectName("offlineBadge")
        layout.addWidget(offline)
        paper = QLabel("PAPER · 纸面交易")
        paper.setObjectName("modeBadge")
        paper.setAccessibleName("PAPER mode")
        layout.addWidget(paper)
        return bar

    def _build_overview_tab(self) -> QWidget:
        page, layout = self._scroll_page()

        note = QLabel("演示组合 · 以下资金、收益与持仓均为占位数据，不代表真实账户")
        note.setStyleSheet(
            "background:#18253a; color:#9fb5ce; border:1px solid #2a405f;"
            "border-radius:6px; padding:9px 12px;"
        )
        layout.addWidget(note)

        cards = QGridLayout()
        cards.setHorizontalSpacing(12)
        cards.setVerticalSpacing(12)
        cards.addWidget(MetricCard("总资产", "¥ 1,000,000.00", "PAPER 初始资金"), 0, 0)
        cards.addWidget(MetricCard("今日盈亏", "+ ¥ 2,860.40", "+0.29%", "positive"), 0, 1)
        cards.addWidget(MetricCard("可用资金", "¥ 612,430.00", "资金占用 38.76%"), 0, 2)
        cards.addWidget(MetricCard("最大回撤", "- 2.18%", "策略组合近30日", "negative"), 0, 3)
        layout.addLayout(cards)

        lower = QSplitter(Qt.Orientation.Horizontal)
        positions = QGroupBox("模拟持仓")
        positions_layout = QVBoxLayout(positions)
        positions_table = self._table(
            ["代码", "名称", "持仓", "可卖", "成本", "最新", "浮盈亏"],
            [
                ["510300.SH", "沪深300ETF", "12,000", "12,000", "3.842", "3.881", "+468.00"],
                ["600036.SH", "招商银行", "2,000", "2,000", "34.80", "35.22", "+840.00"],
                ["159920.SZ", "恒生ETF", "8,000", "8,000", "1.184", "1.176", "-64.00"],
            ],
        )
        positions_layout.addWidget(positions_table)
        lower.addWidget(positions)

        strategies = QGroupBox("策略运行状态")
        strategies_layout = QVBoxLayout(strategies)
        strategy_table = self._table(
            ["策略", "周期", "模式", "状态", "下一动作"],
            [
                ["周度趋势", "1周", "PAPER", "观察中", "周五 14:45 再平衡"],
                ["ETF 5分钟VWAP回归", "5分钟", "PAPER", "已暂停", "等待行情连接"],
            ],
        )
        strategies_layout.addWidget(strategy_table)
        lower.addWidget(strategies)
        lower.setSizes([760, 500])
        layout.addWidget(lower, 1)
        return page

    def _build_chart_tab(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(18, 15, 18, 18)

        controls = QHBoxLayout()
        symbol = QComboBox()
        symbol.addItems(["510300.SH  沪深300ETF", "600036.SH  招商银行", "159920.SZ  恒生ETF"])
        period = QComboBox()
        period.addItems(["1分钟", "5分钟", "日线"])
        controls.addWidget(QLabel("证券"))
        controls.addWidget(symbol)
        controls.addSpacing(12)
        controls.addWidget(QLabel("周期"))
        controls.addWidget(period)
        controls.addStretch()
        data_badge = QLabel("演示行情 · 非实时")
        data_badge.setStyleSheet(
            "color:#e8b967; border:1px solid #715d36; padding:5px 9px;"
            "border-radius:5px"
        )
        controls.addWidget(data_badge)
        layout.addLayout(controls)

        plot = pg.PlotWidget(background="#0b1220")
        plot.setObjectName("marketChart")
        plot.showGrid(x=True, y=True, alpha=0.16)
        plot.setLabel("left", "价格")
        plot.setLabel("bottom", "演示序列")
        plot.getAxis("left").setTextPen(pg.mkPen("#8fa5bf"))
        plot.getAxis("bottom").setTextPen(pg.mkPen("#8fa5bf"))
        plot.addLegend(offset=(12, 12))

        x = list(range(120))
        close = [3.82 + index * 0.00045 + 0.025 * sin(index / 7.0) for index in x]
        moving_average = []
        for index in x:
            start = max(0, index - 9)
            window = close[start : index + 1]
            moving_average.append(sum(window) / len(window))
        plot.plot(x, close, pen=pg.mkPen("#63a4ff", width=2), name="收盘价")
        plot.plot(x, moving_average, pen=pg.mkPen("#f2bd62", width=1.5), name="MA10")
        layout.addWidget(plot, 1)

        footer = QLabel(
            "图表使用本地生成的占位序列。接入行情后，"
            "主图将显示K线、成交量、买卖标记和指标叠加。"
        )
        footer.setStyleSheet("color:#788da7")
        layout.addWidget(footer)
        return page

    def _build_strategy_tab(self) -> QWidget:
        page, layout = self._scroll_page()
        columns = QHBoxLayout()

        selector = QGroupBox("策略清单")
        selector_layout = QVBoxLayout(selector)
        selector_table = self._table(
            ["名称", "类别", "运行环境", "状态"],
            [
                ["周度趋势", "波段", "PAPER", "观察中"],
                ["ETF 5分钟VWAP回归", "日内", "PAPER", "暂停"],
            ],
        )
        selector_layout.addWidget(selector_table)
        columns.addWidget(selector, 3)

        parameters = QGroupBox("周度趋势 · 参数")
        form = QFormLayout(parameters)
        universe = QComboBox()
        universe.addItems(["沪深300成分股", "自定义白名单", "高流动性ETF"])
        lookback = QSpinBox()
        lookback.setRange(5, 250)
        lookback.setValue(120)
        skip_days = QSpinBox()
        skip_days.setRange(0, 20)
        skip_days.setValue(5)
        positions = QSpinBox()
        positions.setRange(1, 50)
        positions.setValue(10)
        max_weight = QDoubleSpinBox()
        max_weight.setRange(1, 100)
        max_weight.setValue(12)
        max_weight.setSuffix(" %")
        rebalance = QComboBox()
        rebalance.addItems(["每周首个交易日 10:00", "每周五 14:45", "每月末 14:45"])
        paper_enabled = QCheckBox("允许在 PAPER 环境产生模拟订单")
        paper_enabled.setChecked(False)
        save = QPushButton("应用到 PAPER")
        save.clicked.connect(
            lambda: self.message_requested.emit("参数已应用到 PAPER 配置（演示，不会产生真实订单）")
        )
        form.addRow("股票池", universe)
        form.addRow("动量回看", lookback)
        form.addRow("跳过最近交易日", skip_days)
        form.addRow("最大持仓数", positions)
        form.addRow("单票权重上限", max_weight)
        form.addRow("再平衡", rebalance)
        form.addRow("", paper_enabled)
        form.addRow("", save)
        columns.addWidget(parameters, 2)
        layout.addLayout(columns)

        guard = QLabel(
            "安全边界：当前构建仅有 PAPER 模式。策略参数不能连接券商，也没有任何实盘开关。"
        )
        guard.setWordWrap(True)
        guard.setStyleSheet(
            "background:#153d35; color:#7de0bb; border:1px solid #28725f;"
            "border-radius:7px; padding:12px;"
        )
        layout.addWidget(guard)
        layout.addStretch()
        return page

    def _build_research_tab(self) -> QWidget:
        page, layout = self._scroll_page()

        notice = QLabel(
            "研究辅助 · 只读展示 · 当前页面不会自动下单；任何建议都必须经过人工核验。"
        )
        notice.setObjectName("researchNotice")
        notice.setWordWrap(True)
        notice.setStyleSheet(
            "background:#18253a; color:#9fb5ce; border:1px solid #2a405f;"
            "border-radius:6px; padding:9px 12px;"
        )
        layout.addWidget(notice)

        upper = QSplitter(Qt.Orientation.Horizontal)

        sources = QGroupBox("来源状态")
        sources_layout = QVBoxLayout(sources)
        source_table = self._table(
            ["来源", "用途", "状态", "最后更新"],
            [
                ["AKShare", "盘中快照 / 分时", "尚未运行", "—"],
                ["BaoStock", "历史日线 / EOD", "尚未运行", "—"],
                ["公开资讯源", "新闻 / 公告", "尚未运行", "—"],
            ],
        )
        source_table.setObjectName("researchSourceStatus")
        sources_layout.addWidget(source_table)
        upper.addWidget(sources)

        evidence = QGroupBox("最新证据")
        evidence_layout = QVBoxLayout(evidence)
        evidence_table = self._table(
            ["时间", "来源", "标题 / 摘要", "验证状态"],
            [["—", "尚未运行", "尚未运行", "尚未运行"]],
        )
        evidence_table.setObjectName("researchEvidenceTable")
        evidence_layout.addWidget(evidence_table)
        upper.addWidget(evidence)
        upper.setSizes([560, 700])
        layout.addWidget(upper)

        recommendations = QGroupBox("技术面 / 宏观面建议")
        recommendations_layout = QVBoxLayout(recommendations)
        recommendation_table = self._table(
            ["分析层", "状态", "结论", "置信度", "边界"],
            [
                ["技术面", "尚未运行", "尚未运行", "尚未运行", "不构成交易指令"],
                ["宏观面", "尚未运行", "尚未运行", "尚未运行", "不构成交易指令"],
            ],
        )
        recommendation_table.setObjectName("researchRecommendationTable")
        recommendations_layout.addWidget(recommendation_table)
        layout.addWidget(recommendations)

        notification = QGroupBox("NapCatQQ 通知")
        notification_layout = QHBoxLayout(notification)
        napcat_status = QLabel("未配置 · 未连接 OneBot · 未发送任何消息")
        napcat_status.setObjectName("napcatStatus")
        napcat_status.setStyleSheet("color:#e8b967")
        notification_layout.addWidget(napcat_status)
        notification_layout.addStretch()
        layout.addWidget(notification)
        layout.addStretch()
        return page

    def _build_backtest_tab(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(18, 15, 18, 18)

        header = QHBoxLayout()
        header.addWidget(QLabel("周度趋势 · 演示回测"))
        header.addStretch()
        header.addWidget(QLabel("2023-01-03 — 2026-07-31 · 基准：沪深300"))
        layout.addLayout(header)

        metrics = QGridLayout()
        metrics.addWidget(MetricCard("累计收益", "+21.46%", "基准 +15.02%", "positive"), 0, 0)
        metrics.addWidget(MetricCard("年化收益", "+6.03%", "演示结果", "positive"), 0, 1)
        metrics.addWidget(MetricCard("最大回撤", "-9.84%", "2024-02", "negative"), 0, 2)
        metrics.addWidget(MetricCard("夏普比率", "0.81", "无风险利率 2%"), 0, 3)
        layout.addLayout(metrics)

        chart = pg.PlotWidget(background="#0b1220")
        chart.setObjectName("backtestChart")
        chart.showGrid(x=True, y=True, alpha=0.16)
        chart.addLegend(offset=(12, 12))
        x = list(range(160))
        equity = [100 + index * 0.13 + 4.2 * sin(index / 13.0) for index in x]
        benchmark = [100 + index * 0.09 + 3.6 * sin(index / 15.0) for index in x]
        chart.plot(x, equity, pen=pg.mkPen("#63a4ff", width=2), name="策略净值")
        chart.plot(x, benchmark, pen=pg.mkPen("#7f91a8", width=1.5), name="沪深300")
        layout.addWidget(chart, 1)

        warning = QLabel(
            "占位报告：尚未接入真实数据、费用、滑点、停牌和涨跌停撮合，"
            "不可据此作投资决策。"
        )
        warning.setStyleSheet("color:#e8b967")
        layout.addWidget(warning)
        return page

    def _build_orders_tab(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(18, 15, 18, 18)

        banner = QLabel("PAPER 订单账本 · 只读演示 · 无实盘委托入口")
        banner.setObjectName("modeBadge")
        banner.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(banner)

        splitter = QSplitter(Qt.Orientation.Vertical)
        orders = QGroupBox("模拟委托")
        orders_layout = QVBoxLayout(orders)
        orders_layout.addWidget(
            self._table(
                ["时间", "本地订单号", "策略", "代码", "方向", "数量", "限价", "状态"],
                [
                    [
                        "10:05:12", "PAPER-0003", "ETF日内动量", "159920.SZ",
                        "买入", "2,000", "1.176", "全部成交",
                    ],
                    [
                        "09:47:26", "PAPER-0002", "周度趋势", "600036.SH",
                        "买入", "1,000", "35.18", "已撤销",
                    ],
                    [
                        "09:35:04", "PAPER-0001", "周度趋势", "510300.SH",
                        "买入", "3,000", "3.878", "全部成交",
                    ],
                ],
            )
        )
        splitter.addWidget(orders)

        fills = QGroupBox("模拟成交")
        fills_layout = QVBoxLayout(fills)
        fills_layout.addWidget(
            self._table(
                ["时间", "成交编号", "订单号", "代码", "方向", "数量", "价格", "费用"],
                [
                    [
                        "10:05:13", "FILL-0002", "PAPER-0003", "159920.SZ",
                        "买入", "2,000", "1.176", "5.00",
                    ],
                    [
                        "09:35:05", "FILL-0001", "PAPER-0001", "510300.SH",
                        "买入", "3,000", "3.878", "5.00",
                    ],
                ],
            )
        )
        splitter.addWidget(fills)
        splitter.setSizes([390, 300])
        layout.addWidget(splitter, 1)
        return page

    def _build_risk_tab(self) -> QWidget:
        page = QWidget()
        layout = QHBoxLayout(page)
        layout.setContentsMargins(18, 15, 18, 18)

        left = QVBoxLayout()
        summary = QGroupBox("PAPER 风控状态")
        summary_layout = QVBoxLayout(summary)
        state = QLabel("●  正常 · 未发现风险事件")
        state.setObjectName("positive")
        summary_layout.addWidget(state)
        for name, value, maximum in [
            ("总仓位", 39, 80),
            ("单票最大权重", 15, 20),
            ("当日模拟订单", 3, 100),
            ("当日回撤", 9, 100),
        ]:
            row = QHBoxLayout()
            row.addWidget(QLabel(name))
            progress = QProgressBar()
            progress.setMaximum(maximum)
            progress.setValue(value)
            progress.setFormat(f"{value} / {maximum}")
            row.addWidget(progress, 1)
            summary_layout.addLayout(row)
        left.addWidget(summary)

        limits = QGroupBox("限制配置（演示）")
        limits_layout = QVBoxLayout(limits)
        limits_layout.addWidget(
            self._table(
                ["规则", "限制", "当前", "状态"],
                [
                    ["最大总仓位", "80%", "38.76%", "通过"],
                    ["单票最大仓位", "20%", "14.09%", "通过"],
                    ["单笔委托金额", "¥50,000", "¥11,634", "通过"],
                    ["行情新鲜度", "≤ 3秒", "离线", "阻止新单"],
                ],
            )
        )
        left.addWidget(limits, 1)
        layout.addLayout(left, 3)

        log_group = QGroupBox("运行日志")
        log_layout = QVBoxLayout(log_group)
        log = QPlainTextEdit()
        log.setReadOnly(True)
        log.setPlainText(
            "09:30:00 INFO  PAPER 引擎已启动\n"
            "09:30:00 INFO  已加载 A 股交易日历（演示）\n"
            "09:30:01 WARN  未配置实时行情，策略保持暂停\n"
            "09:35:04 INFO  PAPER-0001 通过事前风控\n"
            "09:35:05 INFO  PAPER-0001 模拟成交 3000 @ 3.878\n"
            "10:05:13 INFO  PAPER-0003 模拟成交 2000 @ 1.176\n"
            "--:--:-- INFO  当前构建不含券商连接和实盘功能"
        )
        log_layout.addWidget(log)
        clear = QPushButton("清空界面日志")
        clear.clicked.connect(log.clear)
        log_layout.addWidget(clear, 0, Qt.AlignmentFlag.AlignRight)
        layout.addWidget(log_group, 4)
        return page

    def _scroll_page(self) -> tuple[QScrollArea, QVBoxLayout]:
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setContentsMargins(18, 15, 18, 18)
        layout.setSpacing(14)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(content)
        return scroll, layout

    @staticmethod
    def _table(headers: list[str], rows: list[list[str]]) -> QTableWidget:
        table = QTableWidget(len(rows), len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setAlternatingRowColors(True)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        for row_index, row in enumerate(rows):
            for column_index, value in enumerate(row):
                item = QTableWidgetItem(value)
                if value.startswith("+"):
                    item.setForeground(QColor("#ef6a6a"))
                elif value.startswith("-"):
                    item.setForeground(QColor("#54c99c"))
                table.setItem(row_index, column_index, item)
        table.resizeRowsToContents()
        table.setMinimumHeight(150)
        return table

    def _update_clock(self) -> None:
        now = datetime.now().astimezone()
        self._clock_label.setText(now.strftime("%Y-%m-%d  %H:%M:%S  %Z"))

    def closeEvent(self, event: QCloseEvent) -> None:  # noqa: N802
        self._clock_timer.stop()
        super().closeEvent(event)


def preview() -> int:
    """Run this module directly while developing the UI."""

    app = QApplication.instance() or QApplication([])
    window = TradingMainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(preview())
