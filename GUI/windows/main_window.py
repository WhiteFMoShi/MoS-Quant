from __future__ import annotations

import html
import json
import re
import sys
import threading
import time
from collections import OrderedDict
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd
from PySide6.QtCore import QDate, QEventLoop, QLocale, QPoint, Qt, QThread, QSignalBlocker, QTimer, Signal, QStringListModel
from PySide6.QtGui import QBrush, QColor, QFont, QIcon, QKeySequence, QShortcut, QTextCharFormat
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCalendarWidget,
    QCompleter,
    QCheckBox,
    QDateEdit,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QListView,
    QLineEdit,
    QMenu,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QProgressDialog,
    QHeaderView,
    QScrollArea,
    QStackedWidget,
    QTabWidget,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from core.data_service import DataService, FetchRequest, FetchResponse
from core.cache_paths import cache_root
from core.parquet_compat import has_parquet_engine
from GUI.config.column_labels import to_chinese_headers
from GUI.config.datasets import DATASET_OPTIONS, DatasetOption
from GUI.services.symbol_service import SymbolData, SymbolService
from GUI.styles.dark_theme import MAIN_WINDOW_STYLE, apply_dark_palette
from GUI.widgets.flow_pie import FlowPieWidget
from GUI.widgets.market_chart import MarketChartWidget
from GUI.widgets.styled_combo_box import StyledComboBox
from GUI.workers.fetch_worker import FetchWorker
from GUI.workers.preload_worker import PreloadWorker
from core.unified_data_module import UnifiedDataModule


class MainWindow(QMainWindow):
    symbol_data_loaded = Signal(object, object)
    sector_data_loaded = Signal(str, object, object)
    market_extension_loaded = Signal(str, object)
    watch_spot_loaded = Signal(object, object)
    trade_dates_loaded = Signal(object, object)

    DATASETS = DATASET_OPTIONS
    NAV_WIDTH = 68
    LEFT_PANEL_WIDTH = 248
    PAGE_WATCH = 0
    PAGE_MARKET = 1
    PAGE_DATA = 2
    PAGE_HELP = 3
    RIGHT_PAGE_WATCH = 0
    RIGHT_PAGE_ANALYSIS = 1
    RIGHT_PAGE_MARKET = 2
    RIGHT_PAGE_HELP = 3

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("MoS Quant")
        self.resize(1320, 860)
        self._thread: QThread | None = None
        self._worker: FetchWorker | None = None
        self._last_fetch_request: FetchRequest | None = None
        self._symbol_candidates: list[str] = []
        self._symbol_records: list[tuple[str, str]] = []
        self._symbol_name_to_code: dict[str, str] = {}
        self._symbol_code_to_name: dict[str, str] = {}
        self._symbol_completer_ready = False
        self._symbol_completer_loading = False
        self._cache_root = cache_root()
        self._project_root = Path(__file__).resolve().parents[2]
        self._symbol_service = SymbolService(self._cache_root / "symbols")
        self._watchlist_file = self._cache_root / "watchlist.json"
        self._watchlist: list[str] = []
        self._active_fetch_scene = "data"
        self._market_symbol = "000001"
        self._market_initialized = False
        self._market_fetch_pending = False
        self._trade_dates: set[str] = set()
        self._trade_dates_loading = False
        self._trade_dates_prepared = False
        self._trade_dates_retry_scheduled = False
        self._trade_dates_retry_count = 0
        self._board_change_cache_date = ""
        self._board_change_cache: dict[str, float] = {}
        self._stock_sector_cache: dict[str, str] = {}
        self._stock_sector_spot_cache_date = ""
        self._stock_sector_spot_cache: dict[str, str] = {}
        self._sector_loading_codes: set[str] = set()
        self._market_ext_cache: dict[str, dict[str, object]] = {}
        self._market_ext_loading_codes: set[str] = set()
        self._concept_change_cache_date = ""
        self._concept_change_cache: dict[str, float] = {}
        self._help_left_md_path = self._project_root / "docs" / "help_bug_feedback.md"
        self._help_right_md_path = self._project_root / "docs" / "help_usage.md"
        self._market_mem_cache: OrderedDict[tuple[str, str, str, str, str], FetchResponse] = OrderedDict()
        self._market_mem_cache_limit = 10
        self._watch_spot_cache: dict[str, dict[str, object]] = {}
        self._watch_spot_cache_ts = 0.0
        self._watch_spot_loading = False
        self._watch_spot_pending = False
        self._ui_ready = False
        watch_cache_dir = self._cache_root / "watch"
        watch_cache_dir.mkdir(parents=True, exist_ok=True)
        self._watch_spot_snapshot_file = watch_cache_dir / "spot_snapshot.json"
        self._watch_manual_width_cols: set[int] = set()
        self._watch_resizing_columns = False
        self._watch_spot_last_notify_ts = 0.0
        self._last_progress_log_text = ""
        self._last_progress_log_ts = 0.0
        self._load_watch_spot_snapshot()
        market_cache_dir = self._cache_root / "market"
        market_cache_dir.mkdir(parents=True, exist_ok=True)
        self._trade_dates_parquet = market_cache_dir / "trade_dates.parquet"
        self._trade_dates_pickle = market_cache_dir / "trade_dates.pkl"
        self.symbol_data_loaded.connect(self._on_symbol_data_loaded)
        self.sector_data_loaded.connect(self._on_sector_data_loaded)
        self.market_extension_loaded.connect(self._on_market_extension_loaded)
        self.watch_spot_loaded.connect(self._on_watch_spot_loaded)
        self.trade_dates_loaded.connect(self._on_trade_dates_loaded)
        self._build_ui()
        self._apply_style()
        self._bind_shortcuts()
        # Prepare trade calendar early so minute checkpoints and date widgets behave consistently,
        # even if user never enters the data page.
        if (not self._trade_dates_prepared) and (not self._trade_dates_loading):
            self._prepare_trade_dates()

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        main_layout = QHBoxLayout(root)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(8)

        self.left_shell = QFrame()
        self.left_shell.setObjectName("leftShell")
        self.left_shell.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        left_shell_layout = QHBoxLayout(self.left_shell)
        left_shell_layout.setContentsMargins(0, 0, 0, 0)
        left_shell_layout.setSpacing(8)
        self._left_shell_spacing = left_shell_layout.spacing()
        self.left_shell.setFixedWidth(self.NAV_WIDTH + self.LEFT_PANEL_WIDTH + self._left_shell_spacing)

        self.nav_rail = QFrame()
        self.nav_rail.setObjectName("navRail")
        self.nav_rail.setFixedWidth(self.NAV_WIDTH)
        nav_layout = QVBoxLayout(self.nav_rail)
        nav_layout.setContentsMargins(10, 12, 10, 12)
        nav_layout.setSpacing(8)

        self.nav_watch_btn = QPushButton("自选")
        self.nav_watch_btn.setObjectName("navButton")
        self.nav_watch_btn.setCheckable(True)
        self.nav_watch_btn.clicked.connect(lambda: self._switch_left_page(self.PAGE_WATCH))

        self.nav_market_btn = QPushButton("行情")
        self.nav_market_btn.setObjectName("navButton")
        self.nav_market_btn.setCheckable(True)
        self.nav_market_btn.clicked.connect(lambda: self._switch_left_page(self.PAGE_MARKET))

        self.nav_data_btn = QPushButton("分析")
        self.nav_data_btn.setObjectName("navButton")
        self.nav_data_btn.setCheckable(True)
        self.nav_data_btn.setChecked(True)
        self.nav_data_btn.clicked.connect(lambda: self._switch_left_page(self.PAGE_DATA))

        self.nav_help_btn = QPushButton("帮助")
        self.nav_help_btn.setObjectName("navButton")
        self.nav_help_btn.setCheckable(True)
        self.nav_help_btn.clicked.connect(lambda: self._switch_left_page(self.PAGE_HELP))

        nav_layout.addWidget(self.nav_watch_btn)
        nav_layout.addWidget(self.nav_market_btn)
        nav_layout.addWidget(self.nav_data_btn)
        nav_layout.addWidget(self.nav_help_btn)
        nav_layout.addStretch(1)

        self.left_pages = QStackedWidget()
        self.left_pages.setObjectName("leftPages")

        watch_page = QFrame()
        watch_page.setObjectName("controlPanel")
        watch_page.setFixedWidth(self.LEFT_PANEL_WIDTH)
        watch_page.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        watch_layout = QVBoxLayout(watch_page)
        watch_layout.setContentsMargins(14, 12, 14, 12)
        watch_layout.setSpacing(8)

        watch_title = QLabel("自选")
        watch_title.setObjectName("panelTitle")
        watch_desc = QLabel("维护你的关注股票列表")
        watch_desc.setObjectName("panelDesc")
        watch_layout.addWidget(watch_title)
        watch_layout.addWidget(watch_desc)
        watch_layout.addStretch(1)

        market_control_page = QFrame()
        market_control_page.setObjectName("controlPanel")
        market_control_page.setFixedWidth(self.LEFT_PANEL_WIDTH)
        market_control_page.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        market_layout = QVBoxLayout(market_control_page)
        market_layout.setContentsMargins(14, 12, 14, 12)
        market_layout.setSpacing(8)

        market_title = QLabel("行情")
        market_title.setObjectName("panelTitle")
        market_desc = QLabel("历史走势浏览，支持股票与指数")
        market_desc.setObjectName("panelDesc")
        market_layout.addWidget(market_title)
        market_layout.addWidget(market_desc)

        market_form = QGridLayout()
        market_form.setHorizontalSpacing(10)
        market_form.setVerticalSpacing(8)
        market_form.setColumnMinimumWidth(0, 56)
        market_form.setColumnStretch(1, 1)

        self.market_dataset_label = QLabel("类型")
        self.market_dataset_label.setObjectName("fieldLabel")
        self.market_dataset_combo = StyledComboBox()
        self.market_dataset_combo.setObjectName("marketDatasetCombo")
        self.market_dataset_combo.setMinimumHeight(34)
        self.market_dataset_combo.addItem("指数日线", "index_daily")
        self.market_dataset_combo.addItem("A股日线", "stock_daily")

        self.market_cycle_label = QLabel("周期")
        self.market_cycle_label.setObjectName("fieldLabel")
        self.market_cycle_combo = StyledComboBox()
        self.market_cycle_combo.setObjectName("marketCycleCombo")
        self.market_cycle_combo.setMinimumHeight(34)
        self.market_cycle_combo.addItem("分时", "1")
        self.market_cycle_combo.addItem("5分", "5")
        self.market_cycle_combo.addItem("15分", "15")
        self.market_cycle_combo.addItem("30分", "30")
        self.market_cycle_combo.addItem("60分", "60")
        self.market_cycle_combo.addItem("日线", "daily")
        self.market_cycle_combo.addItem("周线", "weekly")
        self.market_cycle_combo.addItem("月线", "monthly")
        self.market_cycle_combo.setCurrentIndex(5)

        self.market_range_label = QLabel("范围")
        self.market_range_label.setObjectName("fieldLabel")
        self.market_range_combo = StyledComboBox()
        self.market_range_combo.setObjectName("marketRangeCombo")
        self.market_range_combo.setMinimumHeight(34)
        self.market_range_combo.addItem("近5日", 5)
        self.market_range_combo.addItem("近20日", 20)
        self.market_range_combo.addItem("近60日", 60)
        self.market_range_combo.addItem("近半年", 180)
        self.market_range_combo.addItem("近1年", 365)
        self.market_range_combo.addItem("全部历史", 0)
        self.market_range_combo.setCurrentIndex(2)
        self.market_range_label.setVisible(False)
        self.market_range_combo.setVisible(False)

        market_form.addWidget(self.market_dataset_label, 0, 0)
        market_form.addWidget(self.market_dataset_combo, 0, 1)
        market_form.addWidget(self.market_cycle_label, 1, 0)
        market_form.addWidget(self.market_cycle_combo, 1, 1)
        market_layout.addLayout(market_form)

        indicator_row = QGridLayout()
        indicator_row.setHorizontalSpacing(8)
        indicator_row.setVerticalSpacing(6)
        self.indicator_macd_check = QCheckBox("MACD")
        self.indicator_macd_check.setObjectName("refreshCacheCheck")
        self.indicator_macd_check.setChecked(True)
        self.indicator_kdj_check = QCheckBox("KDJ")
        self.indicator_kdj_check.setObjectName("refreshCacheCheck")
        self.indicator_kdj_check.setChecked(False)
        self.indicator_rsi_check = QCheckBox("RSI")
        self.indicator_rsi_check.setObjectName("refreshCacheCheck")
        self.indicator_rsi_check.setChecked(False)
        self.indicator_boll_check = QCheckBox("BOLL")
        self.indicator_boll_check.setObjectName("refreshCacheCheck")
        self.indicator_boll_check.setChecked(False)
        indicator_row.addWidget(self.indicator_macd_check, 0, 0)
        indicator_row.addWidget(self.indicator_kdj_check, 0, 1)
        indicator_row.addWidget(self.indicator_rsi_check, 1, 0)
        indicator_row.addWidget(self.indicator_boll_check, 1, 1)
        market_layout.addLayout(indicator_row)

        self.market_refresh_button = QPushButton("刷新行情")
        self.market_refresh_button.setObjectName("fetchButton")
        self.market_refresh_button.setMinimumHeight(34)
        self.market_refresh_button.setCursor(Qt.PointingHandCursor)
        self.market_refresh_button.clicked.connect(self._on_market_refresh_clicked)
        market_layout.addWidget(self.market_refresh_button)
        market_layout.addStretch(1)

        panel = QFrame()
        panel.setObjectName("controlPanel")
        panel.setFixedWidth(self.LEFT_PANEL_WIDTH)
        panel.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        panel_layout = QVBoxLayout(panel)
        panel_layout.setContentsMargins(14, 12, 14, 12)
        panel_layout.setSpacing(8)

        title = QLabel("分析")
        title.setObjectName("panelTitle")
        desc = QLabel("通过 AKShare 获取行情与市场数据并进行分析")
        desc.setObjectName("panelDesc")

        form = QGridLayout()
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(8)
        form.setColumnMinimumWidth(0, 56)
        form.setColumnStretch(1, 1)

        self.dataset_combo = StyledComboBox()
        self.dataset_combo.setObjectName("datasetCombo")
        self.dataset_combo.setMinimumWidth(112)
        self.dataset_combo.setMaximumWidth(126)
        self.dataset_combo.setMinimumContentsLength(4)
        self.dataset_combo.setSizeAdjustPolicy(
            StyledComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        for option in self.DATASETS:
            self.dataset_combo.addItem(option.label, option)
        self.dataset_combo.setMaxVisibleItems(8)
        combo_view = QListView()
        combo_view.setObjectName("datasetComboPopup")
        combo_view.setFrameShape(QFrame.NoFrame)
        combo_view.setSpacing(0)
        combo_view.setUniformItemSizes(True)
        combo_view.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.dataset_combo.setView(combo_view)

        self.symbol_input = QLineEdit("000001")
        self.symbol_input.setPlaceholderText("")
        self.symbol_input.setVisible(False)
        self.start_date_edit = QDateEdit(QDate.currentDate().addMonths(-3))
        self.start_date_edit.setObjectName("开始日期")
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDisplayFormat("yyyy-MM-dd")

        self.end_date_edit = QDateEdit(QDate.currentDate())
        self.end_date_edit.setObjectName("结束日期")
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDisplayFormat("yyyy-MM-dd")

        self.single_date_edit = QDateEdit(self._latest_weekday(QDate.currentDate()))
        self.single_date_edit.setObjectName("单日日期")
        self.single_date_edit.setCalendarPopup(True)
        self.single_date_edit.setDisplayFormat("yyyy-MM-dd")

        for widget in (
            self.dataset_combo,
            self.symbol_input,
            self.start_date_edit,
            self.end_date_edit,
            self.single_date_edit,
        ):
            widget.setMinimumHeight(34)

        self.dataset_label = QLabel("数据集")
        self.dataset_label.setObjectName("datasetInlineLabel")
        self.dataset_label.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        self.start_date_label = QLabel("开始日期")
        self.end_date_label = QLabel("结束日期")
        self.single_date_label = QLabel("单日日期")
        for label in (
            self.start_date_label,
            self.end_date_label,
            self.single_date_label,
        ):
            label.setObjectName("fieldLabel")
            label.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)

        self.refresh_cache_check = QCheckBox("刷新缓存")
        self.refresh_cache_check.setObjectName("refreshCacheCheck")
        self.refresh_cache_check.setChecked(False)
        self.full_history_check = QCheckBox("获取历史数据（忽略开始/结束日期）")
        self.full_history_check.setObjectName("refreshCacheCheck")
        self.full_history_check.setChecked(False)

        self.fetch_button = QPushButton("开始分析")
        self.fetch_button.setObjectName("fetchButton")
        self.fetch_button.setMinimumHeight(34)
        self.fetch_button.setMinimumWidth(128)
        self.fetch_button.setCursor(Qt.PointingHandCursor)
        self.fetch_button.clicked.connect(self._on_fetch_clicked)
        self.fetch_button.setVisible(False)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(6)
        header_row.addWidget(title, 0, Qt.AlignLeft | Qt.AlignVCenter)
        header_row.addStretch(1)
        self.dataset_label.setVisible(False)
        header_row.addWidget(self.dataset_combo, 0, Qt.AlignRight | Qt.AlignVCenter)
        panel_layout.addLayout(header_row)
        panel_layout.addWidget(desc)

        form.addWidget(self.start_date_label, 0, 0)
        form.addWidget(self.start_date_edit, 0, 1)
        form.addWidget(self.end_date_label, 1, 0)
        form.addWidget(self.end_date_edit, 1, 1)
        form.addWidget(self.single_date_label, 2, 0)
        form.addWidget(self.single_date_edit, 2, 1)
        form.addWidget(self.full_history_check, 3, 1, alignment=Qt.AlignLeft)
        form.addWidget(self.refresh_cache_check, 4, 1, alignment=Qt.AlignLeft)
        panel_layout.addLayout(form)

        self.quick_symbol_input = QLineEdit()
        self.quick_symbol_input.setObjectName("quickSymbolInput")
        self.quick_symbol_input.setFixedWidth(196)
        self.quick_symbol_input.setMinimumHeight(34)
        self.quick_symbol_input.setPlaceholderText("输入代码/名称后回车")
        self.quick_symbol_input.setVisible(False)
        self.quick_symbol_input.returnPressed.connect(self._on_quick_symbol_return)
        self.quick_symbol_input.textEdited.connect(self._on_quick_symbol_text_edited)

        self._symbol_model = QStringListModel(self)
        self._symbol_completer = QCompleter(self._symbol_model, self)
        self._symbol_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._symbol_completer.setFilterMode(Qt.MatchContains)
        self._symbol_completer.setCompletionMode(QCompleter.PopupCompletion)
        completer_popup = QListView()
        completer_popup.setObjectName("quickSymbolCompleterPopup")
        completer_popup.setUniformItemSizes(True)
        self._symbol_completer.setPopup(completer_popup)
        self.quick_symbol_input.setCompleter(self._symbol_completer)

        watch_right = QFrame()
        watch_right.setObjectName("resultPanel")
        watch_right_layout = QVBoxLayout(watch_right)
        watch_right_layout.setContentsMargins(16, 16, 16, 16)
        watch_right_layout.setSpacing(10)

        watch_right_title = QLabel("自选列表")
        watch_right_title.setObjectName("resultTitle")
        watch_right_layout.addWidget(watch_right_title)

        watch_card = QFrame()
        watch_card.setObjectName("watchCard")
        watch_card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        watch_card_layout = QVBoxLayout(watch_card)
        watch_card_layout.setContentsMargins(8, 8, 8, 8)
        watch_card_layout.setSpacing(0)
        self.watch_list = QTableWidget()
        self.watch_list.setObjectName("watchListTable")
        self.watch_list.setColumnCount(9)
        self.watch_list.setHorizontalHeaderLabels(
            ["序号", "证券代码", "证券名称", "类型", "现价", "涨幅%", "涨跌", "涨速%", "换手%"]
        )
        self.watch_list.setAlternatingRowColors(True)
        self.watch_list.setSortingEnabled(False)
        self.watch_list.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.watch_list.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.watch_list.setSelectionMode(QAbstractItemView.SingleSelection)
        self.watch_list.verticalHeader().setVisible(False)
        watch_header = self.watch_list.horizontalHeader()
        watch_header.setStretchLastSection(False)
        watch_header.setSectionsMovable(False)
        watch_header.setDefaultAlignment(Qt.AlignCenter)
        watch_header.setSectionResizeMode(QHeaderView.Interactive)
        watch_header.sectionResized.connect(self._on_watch_header_resized)
        self.watch_list.cellDoubleClicked.connect(self._on_watch_item_activated)
        self.watch_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.watch_list.customContextMenuRequested.connect(self._on_watch_list_context_menu)
        watch_card_layout.addWidget(self.watch_list, 1)
        watch_right_layout.addWidget(watch_card, 1)

        analysis_right = QFrame()
        analysis_right.setObjectName("resultPanel")
        analysis_right_layout = QVBoxLayout(analysis_right)
        analysis_right_layout.setContentsMargins(16, 16, 16, 16)
        analysis_right_layout.setSpacing(12)

        self.result_title = QLabel("分析结果")
        self.result_title.setObjectName("resultTitle")
        analysis_right_layout.addWidget(self.result_title)

        self.table = QTableWidget()
        self.table.setObjectName("analysisResultTable")
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(False)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setStretchLastSection(True)
        analysis_right_layout.addWidget(self.table, 1)

        market_right = QFrame()
        market_right.setObjectName("resultPanel")
        market_right_layout = QVBoxLayout(market_right)
        market_right_layout.setContentsMargins(16, 16, 16, 16)
        market_right_layout.setSpacing(8)

        market_body = QHBoxLayout()
        market_body.setSpacing(12)
        market_left_col = QVBoxLayout()
        market_left_col.setSpacing(10)

        self.market_chart = MarketChartWidget()
        market_left_col.addWidget(self.market_chart, 1)
        market_body.addLayout(market_left_col, 3)

        market_side = QFrame()
        market_side.setObjectName("marketSideCard")
        market_side.setMinimumWidth(246)
        market_side.setMaximumWidth(338)
        market_side_layout = QVBoxLayout(market_side)
        market_side_layout.setContentsMargins(0, 0, 0, 0)
        market_side_layout.setSpacing(0)
        market_side_fixed = QWidget()
        market_side_fixed.setObjectName("marketSideFixed")
        fixed_layout = QVBoxLayout(market_side_fixed)
        fixed_layout.setContentsMargins(12, 12, 12, 10)
        fixed_layout.setSpacing(6)
        market_side_layout.addWidget(market_side_fixed, 0)

        self.market_name_label = QLabel("上证指数")
        self.market_name_label.setObjectName("marketName")
        self.market_symbol_label = QLabel("000001")
        self.market_symbol_label.setObjectName("marketSymbol")
        self.market_sector_label = QLabel("")
        self.market_sector_label.setObjectName("marketSector")
        self.market_sector_label.setVisible(False)
        self.market_price_label = QLabel("--")
        self.market_price_label.setObjectName("marketPrice")
        self.market_change_label = QLabel("--")
        self.market_change_label.setObjectName("marketDelta")
        self.market_meta_title = QLabel("000001  --")
        self.market_meta_title.setObjectName("panelDesc")
        identity_row = QHBoxLayout()
        identity_row.setContentsMargins(0, 0, 0, 0)
        identity_row.setSpacing(8)
        identity_row.addWidget(self.market_name_label, 0, Qt.AlignLeft | Qt.AlignVCenter)
        identity_row.addWidget(self.market_symbol_label, 0, Qt.AlignLeft | Qt.AlignVCenter)
        identity_row.addWidget(self.market_sector_label, 0, Qt.AlignLeft | Qt.AlignVCenter)
        identity_row.addStretch(1)
        fixed_layout.addLayout(identity_row)
        fixed_layout.addWidget(self.market_price_label)
        fixed_layout.addWidget(self.market_change_label)
        fixed_layout.addWidget(self.market_meta_title)

        market_side_scroll = QScrollArea()
        market_side_scroll.setObjectName("marketSideScroll")
        market_side_scroll.setWidgetResizable(True)
        market_side_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        market_side_scroll.setFrameShape(QFrame.NoFrame)
        market_side_content = QWidget()
        market_side_content.setObjectName("marketSideContent")
        side_content_layout = QVBoxLayout(market_side_content)
        side_content_layout.setContentsMargins(12, 8, 12, 12)
        side_content_layout.setSpacing(8)
        market_side_scroll.setWidget(market_side_content)
        market_side_layout.addWidget(market_side_scroll, 1)

        stat_grid = QGridLayout()
        stat_grid.setHorizontalSpacing(8)
        stat_grid.setVerticalSpacing(6)
        self._market_stats: dict[str, QLabel] = {}
        for row, key in enumerate(
            (
                "昨收",
                "开盘",
                "最高",
                "最低",
                "收盘",
                "涨跌",
                "涨跌幅",
                "成交量",
                "成交额",
                "5日均量",
                "20日均量",
                "近5日涨跌",
                "近20日涨跌",
                "年内涨跌",
                "20日波动",
                "区间涨跌",
                "区间振幅",
                "区间最高",
                "区间最低",
                "交易天数",
            )
        ):
            name = QLabel(key)
            name.setObjectName("fieldLabel")
            value = QLabel("--")
            value.setObjectName("marketStatValue")
            stat_grid.addWidget(name, row, 0)
            stat_grid.addWidget(value, row, 1)
            self._market_stats[key] = value
        side_content_layout.addLayout(stat_grid)

        self.market_detail_tabs = QTabWidget()
        self.market_detail_tabs.setObjectName("marketDetailTabs")
        flow_tab = QWidget()
        flow_layout = QGridLayout(flow_tab)
        flow_layout.setContentsMargins(4, 4, 4, 4)
        flow_layout.setHorizontalSpacing(6)
        flow_layout.setVerticalSpacing(6)
        self.flow_pie = FlowPieWidget()
        self.flow_pie.setObjectName("flowPie")
        self.flow_pie.setMaximumHeight(104)
        flow_layout.addWidget(self.flow_pie, 0, 0, 1, 2)
        self._flow_labels: dict[str, QLabel] = {}
        flow_fields = ("主力净流入", "主力净占比", "超大单净流入", "大单净流入", "中单净流入", "小单净流入")
        for row, key in enumerate(flow_fields, start=1):
            n = QLabel(key)
            n.setObjectName("fieldLabel")
            v = QLabel("--")
            v.setObjectName("marketStatValue")
            flow_layout.addWidget(n, row, 0)
            flow_layout.addWidget(v, row, 1)
            self._flow_labels[key] = v

        self.market_detail_tabs.addTab(flow_tab, "资金流向")
        related_tab = QWidget()
        related_tab.setObjectName("relatedBoardTab")
        related_layout = QGridLayout(related_tab)
        related_layout.setContentsMargins(6, 6, 6, 6)
        related_layout.setHorizontalSpacing(8)
        related_layout.setVerticalSpacing(10)
        related_layout.setColumnStretch(1, 1)
        self._related_board_labels: dict[str, QLabel] = {}
        for row, key in enumerate(("行业板块", "地区板块", "概念板块", "风格板块")):
            name = QLabel(key)
            name.setObjectName("fieldLabel")
            value = QLabel("--")
            value.setObjectName("boardGroupValue")
            value.setWordWrap(True)
            value.setTextFormat(Qt.RichText)
            related_layout.addWidget(name, row, 0, Qt.AlignTop)
            related_layout.addWidget(value, row, 1, Qt.AlignTop)
            self._related_board_labels[key] = value
        self.market_detail_tabs.addTab(related_tab, "关联板块")
        side_content_layout.addWidget(self.market_detail_tabs, 1)
        side_content_layout.addStretch(1)
        market_body.addWidget(market_side, 1)
        market_right_layout.addLayout(market_body, 1)

        self.right_pages = QStackedWidget()
        self.right_pages.setObjectName("rightPages")
        self.right_pages.addWidget(watch_right)
        self.right_pages.addWidget(analysis_right)
        self.right_pages.addWidget(market_right)
        help_right = QFrame()
        help_right.setObjectName("resultPanel")
        help_right_layout = QVBoxLayout(help_right)
        help_right_layout.setContentsMargins(16, 16, 16, 16)
        help_right_layout.setSpacing(12)
        help_right_title = QLabel("帮助")
        help_right_title.setObjectName("resultTitle")
        help_right_layout.addWidget(help_right_title)
        self.help_right_doc = QTextEdit()
        self.help_right_doc.setReadOnly(True)
        self.help_right_doc.setObjectName("logBox")
        help_right_layout.addWidget(self.help_right_doc, 1)
        self.right_pages.addWidget(help_right)

        self.log_label = QLabel("运行日志")
        self.log_label.setObjectName("fieldLabel")
        self.log_box = QTextEdit()
        self.log_box.setObjectName("logBox")
        self.log_box.setReadOnly(True)
        self.log_box.setLineWrapMode(QTextEdit.NoWrap)
        self.log_box.setFixedHeight(140)
        panel_layout.addStretch(1)
        panel_layout.addWidget(self.log_label)
        panel_layout.addWidget(self.log_box)

        help_page = QFrame()
        help_page.setObjectName("controlPanel")
        help_page.setFixedWidth(self.LEFT_PANEL_WIDTH)
        help_page.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        help_layout = QVBoxLayout(help_page)
        help_layout.setContentsMargins(14, 12, 14, 12)
        help_layout.setSpacing(8)
        self.help_left_doc = QTextEdit()
        self.help_left_doc.setReadOnly(True)
        self.help_left_doc.setObjectName("logBox")
        help_layout.addWidget(self.help_left_doc, 1)

        self.left_pages.addWidget(watch_page)
        self.left_pages.addWidget(market_control_page)
        self.left_pages.addWidget(panel)
        self.left_pages.addWidget(help_page)
        left_shell_layout.addWidget(self.nav_rail)
        left_shell_layout.addWidget(self.left_pages)

        main_layout.addWidget(self.left_shell)
        main_layout.addWidget(self.right_pages, 1)

        self.floating_pod = QFrame(root)
        self.floating_pod.setObjectName("floatingPod")
        pod_layout = QHBoxLayout(self.floating_pod)
        pod_layout.setContentsMargins(10, 8, 10, 10)
        pod_layout.setSpacing(8)
        pod_layout.addWidget(self.quick_symbol_input)

        self.progress_block = QFrame()
        self.progress_block.setObjectName("progressPod")
        progress_layout = QVBoxLayout(self.progress_block)
        progress_layout.setContentsMargins(0, 0, 0, 0)
        progress_layout.setSpacing(3)

        progress_row = QHBoxLayout()
        progress_row.setContentsMargins(0, 0, 0, 0)
        progress_row.setSpacing(6)

        self.progress_status = QLabel("下载中")
        self.progress_status.setObjectName("floatingStatus")
        self.progress_status.setVisible(True)
        self.progress_status.setMinimumWidth(176)
        self.progress_percent = QLabel("0%")
        self.progress_percent.setObjectName("floatingPercent")
        progress_row.addWidget(self.progress_status, 1)
        progress_row.addWidget(self.progress_percent, 0, Qt.AlignRight | Qt.AlignVCenter)
        progress_layout.addLayout(progress_row)

        self.progress_bar = QProgressBar()
        self.progress_bar.setObjectName("fetchProgressBar")
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(12)
        self.progress_bar.setFixedWidth(268)
        progress_layout.addWidget(self.progress_bar)

        self.progress_block.setVisible(False)
        pod_layout.addWidget(self.progress_block)
        self.floating_pod.setVisible(False)
        self.floating_pod.raise_()

        self.dataset_combo.currentIndexChanged.connect(self._refresh_dataset_hint)
        self.full_history_check.toggled.connect(lambda _: self._refresh_dataset_hint())
        self.market_dataset_combo.currentIndexChanged.connect(lambda _: self._on_market_controls_changed())
        self.market_cycle_combo.currentIndexChanged.connect(lambda _: self._on_market_controls_changed())
        self.indicator_macd_check.toggled.connect(lambda _: self._on_indicator_controls_changed())
        self.indicator_kdj_check.toggled.connect(lambda _: self._on_indicator_controls_changed())
        self.indicator_rsi_check.toggled.connect(lambda _: self._on_indicator_controls_changed())
        self.indicator_boll_check.toggled.connect(lambda _: self._on_indicator_controls_changed())
        self._refresh_dataset_hint()
        self._on_indicator_controls_changed()
        self._sync_market_chart_mode()
        self._init_market_detail_placeholders()
        self._ensure_symbol_lookup_ready()
        self._load_watchlist()
        self._refresh_watchlist_view()
        self._reload_help_docs()
        self._switch_left_page(self.PAGE_WATCH)
        self._refresh_market_identity()
        self._tune_date_calendars()
        self._position_floating_pod()
        self._ui_ready = True
        QTimer.singleShot(1200, self._run_startup_deferred_tasks)

    def _run_startup_deferred_tasks(self) -> None:
        if self._thread is not None:
            return
        if self.left_pages.currentIndex() == self.PAGE_WATCH and self._watch_spot_pending:
            self._ensure_watch_spot_loaded()

    def _bind_shortcuts(self) -> None:
        self._enter_shortcut = QShortcut(QKeySequence(Qt.Key_Return), self)
        self._enter_shortcut.activated.connect(self._on_enter_pressed)
        self._numpad_enter_shortcut = QShortcut(QKeySequence(Qt.Key_Enter), self)
        self._numpad_enter_shortcut.activated.connect(self._on_enter_pressed)
        self._esc_shortcut = QShortcut(QKeySequence(Qt.Key_Escape), self)
        self._esc_shortcut.activated.connect(self._on_escape_pressed)

    def _apply_style(self) -> None:
        app = QApplication.instance()
        if app is not None:
            apply_dark_palette(app)
        self.setStyleSheet(MAIN_WINDOW_STYLE)

    def _reload_help_docs(self) -> None:
        self._ensure_help_markdown_files()
        self._load_markdown_to_text_edit(self._help_left_md_path, self.help_left_doc)
        self._load_markdown_to_text_edit(self._help_right_md_path, self.help_right_doc)

    def _ensure_help_markdown_files(self) -> None:
        docs_dir = self._project_root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)

        if not self._help_left_md_path.exists():
            self._help_left_md_path.write_text(
                "# Bug 反馈\n\n"
                "请在此记录问题，建议包含：\n"
                "- 软件版本\n"
                "- 操作步骤\n"
                "- 实际结果\n"
                "- 期望结果\n"
                "- 截图或日志\n\n"
                "示例：\n\n"
                "## 问题 1\n"
                "- 页面：行情\n"
                "- 现象：切换周线时卡顿\n"
                "- 复现步骤：...\n",
                encoding="utf-8",
            )

        if not self._help_right_md_path.exists():
            self._help_right_md_path.write_text(
                "# MoS Quant 使用说明\n\n"
                "## 1. 自选页面\n"
                "- Enter 弹出右下角输入框\n"
                "- 输入代码后回车加入自选\n\n"
                "## 2. 行情页面\n"
                "- 支持分时/5分/15分/30分/60分/日/周/月\n"
                "- 滚轮缩放，鼠标拖拽或方向键平移\n\n"
                "## 3. 分析页面\n"
                "- 支持数据抓取、本地缓存与结果分析\n"
                "- 可选择历史模式\n",
                encoding="utf-8",
            )

    def _load_markdown_to_text_edit(self, path: Path, target: QTextEdit) -> None:
        try:
            content = path.read_text(encoding="utf-8")
        except Exception as exc:
            target.setPlainText(f"文档读取失败: {path}\n{exc}")
            return
        if not content.strip():
            target.setPlainText(f"文档为空: {path}")
            return
        target.setHtml(self._markdown_to_html_preserve_blank_lines(content))

    @classmethod
    def _markdown_to_html_preserve_blank_lines(cls, markdown_text: str) -> str:
        def _inline(text: str) -> str:
            placeholders: list[str] = []

            def _protect_html_tag(match: re.Match[str]) -> str:
                placeholders.append(match.group(0))
                return f"@@HTML_TAG_{len(placeholders) - 1}@@"

            protected = re.sub(r"</?[^>]+?>", _protect_html_tag, text)
            escaped = html.escape(protected)
            escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
            escaped = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", escaped)
            escaped = re.sub(r"\*([^*]+)\*", r"<i>\1</i>", escaped)
            for i, tag in enumerate(placeholders):
                escaped = escaped.replace(f"@@HTML_TAG_{i}@@", tag)
            return escaped

        blocks: list[str] = []
        for raw_line in str(markdown_text).splitlines():
            line = raw_line.rstrip("\n\r")
            stripped = line.strip()
            if stripped == "":
                blocks.append("<div style='height:0.9em;'></div>")
                continue

            if stripped.startswith("### "):
                blocks.append(f"<div style='font-size:16px;font-weight:700;margin:3px 0;'>{_inline(stripped[4:])}</div>")
                continue
            if stripped.startswith("## "):
                blocks.append(f"<div style='font-size:18px;font-weight:700;margin:4px 0;'>{_inline(stripped[3:])}</div>")
                continue
            if stripped.startswith("# "):
                blocks.append(f"<div style='font-size:22px;font-weight:800;margin:6px 0;'>{_inline(stripped[2:])}</div>")
                continue

            bullet = re.match(r"^[-*]\s+(.+)$", stripped)
            if bullet:
                blocks.append(f"<div style='margin-left:1.2em;'>• {_inline(bullet.group(1))}</div>")
                continue

            ordered = re.match(r"^(\\d+)\\.\\s+(.+)$", stripped)
            if ordered:
                blocks.append(f"<div style='margin-left:0.2em;'>{ordered.group(1)}. {_inline(ordered.group(2))}</div>")
                continue

            blocks.append(f"<div>{_inline(line)}</div>")

        return (
            "<div style='white-space:pre-wrap; line-height:1.45; font-size:14px;'>"
            + "".join(blocks)
            + "</div>"
        )

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._position_floating_pod()
        if hasattr(self, "watch_list") and self.right_pages.currentIndex() == self.RIGHT_PAGE_WATCH:
            self._autosize_watch_columns()

    def _refresh_dataset_hint(self) -> None:
        option = self._current_dataset_option()
        requires_symbol = self._requires_symbol(option)
        can_use_history = requires_symbol
        if not can_use_history and self.full_history_check.isChecked():
            self.full_history_check.setChecked(False)
        self.full_history_check.setEnabled(can_use_history)
        use_history = can_use_history and self.full_history_check.isChecked()

        self.symbol_input.setEnabled(requires_symbol)
        self.start_date_edit.setEnabled(requires_symbol and not use_history)
        self.end_date_edit.setEnabled(requires_symbol and not use_history)
        self.single_date_edit.setEnabled(not requires_symbol)
        self._set_visual_state(self.symbol_input, requires_symbol)
        self._set_visual_state(self.start_date_edit, requires_symbol and not use_history)
        self._set_visual_state(self.end_date_edit, requires_symbol and not use_history)
        self._set_visual_state(self.single_date_edit, not requires_symbol)
        self._set_label_state(self.start_date_label, requires_symbol and not use_history)
        self._set_label_state(self.end_date_label, requires_symbol and not use_history)
        self._set_label_state(self.single_date_label, not requires_symbol)
        self._set_visual_state(self.full_history_check, can_use_history)

        if not requires_symbol:
            self._hide_quick_symbol_input()

    def _on_market_controls_changed(self) -> None:
        self._sync_market_chart_mode()
        self._refresh_market_identity()
        if self.left_pages.currentIndex() == self.PAGE_MARKET and self._market_initialized:
            if self._thread is not None:
                if not self._market_fetch_pending:
                    self._append_log("行情参数已变更，当前任务结束后自动刷新")
                self._market_fetch_pending = True
                return
            self._trigger_market_fetch()

    def _on_indicator_controls_changed(self) -> None:
        self.market_chart.set_indicator_visibility(
            macd=self.indicator_macd_check.isChecked(),
            kdj=self.indicator_kdj_check.isChecked(),
            rsi=self.indicator_rsi_check.isChecked(),
            boll=self.indicator_boll_check.isChecked(),
        )

    def _sync_market_chart_mode(self) -> None:
        dataset = self.market_dataset_combo.currentData()
        dataset_value = dataset if isinstance(dataset, str) else "index_daily"
        period = self.market_cycle_combo.currentData()
        period_value = period if isinstance(period, str) else "daily"
        line_mode = dataset_value != "stock_daily" or period_value in {"1", "5", "15", "30", "60"}
        self.market_chart.set_main_series_mode("line" if line_mode else "candle")

    def _init_market_detail_placeholders(self) -> None:
        if hasattr(self, "flow_pie"):
            self.flow_pie.set_flow_values({})
        for label in self._flow_labels.values():
            label.setText("--")
            label.setStyleSheet("")
        if hasattr(self, "_related_board_labels"):
            for label in self._related_board_labels.values():
                label.setText("--")
                label.setToolTip("")
                label.setStyleSheet("")

    def _current_dataset_option(self) -> DatasetOption:
        option = self.dataset_combo.currentData()
        return option if option is not None else self.DATASETS[0]

    @staticmethod
    def _requires_symbol(option: DatasetOption) -> bool:
        return option.requires_symbol

    @staticmethod
    def _latest_weekday(base_date: QDate) -> QDate:
        d = base_date.addDays(-1)
        while d.dayOfWeek() > 5:
            d = d.addDays(-1)
        return d

    @staticmethod
    def _latest_completed_trade_day(base_date: QDate) -> QDate:
        d = base_date
        if d.dayOfWeek() > 5:
            while d.dayOfWeek() > 5:
                d = d.addDays(-1)
            return d
        return d

    def _tune_date_calendars(self) -> None:
        day_fmt = QTextCharFormat()
        day_fmt.setForeground(QColor("#dfe3ed"))
        header_fmt = QTextCharFormat()
        header_fmt.setForeground(QColor("#98a2b4"))

        weekdays = (
            Qt.Monday,
            Qt.Tuesday,
            Qt.Wednesday,
            Qt.Thursday,
            Qt.Friday,
            Qt.Saturday,
            Qt.Sunday,
        )
        for date_edit in (self.start_date_edit, self.end_date_edit, self.single_date_edit):
            calendar = date_edit.calendarWidget()
            calendar.setLocale(QLocale(QLocale.Chinese, QLocale.China))
            calendar.setFirstDayOfWeek(Qt.Monday)
            calendar.setHorizontalHeaderFormat(QCalendarWidget.SingleLetterDayNames)
            calendar.setHeaderTextFormat(header_fmt)
            for day in weekdays:
                calendar.setWeekdayTextFormat(day, day_fmt)
            calendar.currentPageChanged.connect(
                lambda year, month, cal=calendar: self._mark_calendar_page(cal, year, month)
            )
            date_edit.dateChanged.connect(
                lambda qdate, title=date_edit.objectName() or "日期": self._on_date_changed(qdate, title)
            )

    def _prepare_trade_dates(self) -> None:
        self._trade_dates_prepared = True
        if self._load_trade_dates_from_cache():
            self._refresh_all_calendar_marks()
            self._append_trade_dates_status_log()
            # Cache-first for responsiveness, then refresh calendar source in background.
            self._schedule_trade_dates_retry(delay_seconds=2)
            return
        if self._trade_dates_loading:
            return
        self._trade_dates_loading = True
        threading.Thread(target=self._load_trade_dates_async, daemon=True).start()

    def _load_trade_dates_async(self) -> None:
        # Delegate calendar fetch/validation to the core DataService so the entire app
        # uses the same calendar quality rules and cache files.
        errors: list[str] = []
        try:
            DataService._refresh_trade_dates_cache_worker()
            dates = sorted(DataService._load_trade_dates())
        except Exception as exc:
            errors.append(str(exc))
            dates = []
        self.trade_dates_loaded.emit(dates, errors)

    def _on_trade_dates_loaded(self, dates_obj: object, errors_obj: object) -> None:
        self._trade_dates_loading = False
        errors: list[str] = []
        if isinstance(errors_obj, list):
            errors = [str(item) for item in errors_obj if str(item).strip()]

        dates: set[str] = set()
        if isinstance(dates_obj, (list, tuple, set)):
            dates = {str(item) for item in dates_obj if str(item).strip()}
        if not dates:
            fallback_dates = self._build_weekday_trade_dates()
            if fallback_dates:
                self._trade_dates = fallback_dates
                self._refresh_all_calendar_marks()
            self._trade_dates_prepared = True
            self._trade_dates_retry_count += 1
            self._schedule_trade_dates_retry()
            if errors:
                self._append_log("交易日历主源波动，已回退为工作日规则（假期附近可能影响最新性判断），后台将自动重试")
            return

        self._trade_dates_retry_count = 0
        self._trade_dates_retry_scheduled = False
        self._trade_dates_prepared = True
        self._trade_dates = dates
        self._persist_trade_dates_cache(self._trade_dates)
        self._append_trade_dates_status_log()
        if errors:
            self._append_log("交易日历主源波动，已自动切换备用源")
        self._refresh_all_calendar_marks()

    def _append_trade_dates_status_log(self) -> None:
        try:
            meta_path = self._cache_root / "market" / "trade_dates.meta.json"
            if meta_path.exists():
                payload = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    src = str(payload.get("source", "--"))
                    mx = str(payload.get("ts_max", "--"))
                    rows = str(payload.get("rows", "--"))
                    self._append_log(f"交易日历已加载: source={src}, max={mx}, rows={rows}")
                    return
        except Exception:
            pass
        try:
            if self._trade_dates:
                mx = max(self._trade_dates)
                self._append_log(f"交易日历已加载: max={mx}, rows={len(self._trade_dates)}")
        except Exception:
            return

    def _schedule_trade_dates_retry(self, delay_seconds: int | None = None) -> None:
        if self._trade_dates_retry_scheduled or self._trade_dates_loading:
            return
        if delay_seconds is None:
            # 60s, 120s, 240s, 480s, then cap at 900s
            delay_seconds = min(900, int(60 * (2 ** min(self._trade_dates_retry_count, 3))))
        self._trade_dates_retry_scheduled = True
        QTimer.singleShot(max(10, int(delay_seconds)) * 1000, self._retry_trade_dates_fetch)

    def _retry_trade_dates_fetch(self) -> None:
        self._trade_dates_retry_scheduled = False
        if self._trade_dates_loading:
            return
        self._trade_dates_loading = True
        threading.Thread(target=self._load_trade_dates_async, daemon=True).start()

    def _load_trade_dates_remote(self, errors: list[str]) -> set[str]:
        # Kept for backward-compatibility; core DataService is the source of truth now.
        def call_with_timeout(loader: object, timeout_seconds: float = 12.0) -> pd.DataFrame:
            payload: dict[str, object] = {}
            error: dict[str, Exception] = {}

            def runner() -> None:
                try:
                    payload["df"] = loader()
                except Exception as exc:
                    error["exc"] = exc

            t = threading.Thread(target=runner, daemon=True)
            t.start()
            t.join(timeout=max(0.5, float(timeout_seconds)))
            if t.is_alive():
                raise TimeoutError("timeout")
            if "exc" in error:
                raise error["exc"]
            df = payload.get("df")
            if not isinstance(df, pd.DataFrame):
                return pd.DataFrame()
            return df

        loaders = (
            ("tencent_index_daily_calendar", lambda: ak.stock_zh_index_daily_tx(symbol="sh000001")),
        )
        for source_name, loader in loaders:
            try:
                df = call_with_timeout(loader, timeout_seconds=12.0)
                dates = self._extract_trade_dates(df)
                if dates and (not self._is_synthetic_trade_dates(dates)):
                    return dates
                errors.append(f"{source_name}:invalid_calendar" if dates else f"{source_name}:empty")
            except Exception as exc:
                errors.append(f"{source_name}:{exc}")
        return set()

    @staticmethod
    def _parse_trade_date_values(values: pd.Series) -> pd.Series:
        index = values.index
        parsers = (
            lambda: pd.to_datetime(values, errors="coerce"),
            lambda: pd.to_datetime(values, errors="coerce", format="ISO8601", utc=True),
            lambda: pd.to_datetime(values, errors="coerce", format="mixed", utc=True),
        )
        for parser in parsers:
            try:
                parsed = parser()
                ts = parsed if isinstance(parsed, pd.Series) else pd.Series(parsed, index=index)
                try:
                    if hasattr(ts, "dt") and ts.dt.tz is not None:
                        ts = ts.dt.tz_convert(None)
                except Exception:
                    try:
                        ts = ts.dt.tz_localize(None)
                    except Exception:
                        pass
                if ts.notna().sum() > 0:
                    return ts
            except Exception:
                continue
        return pd.Series(pd.NaT, index=index)

    @classmethod
    def _extract_trade_dates(cls, df: pd.DataFrame | None) -> set[str]:
        if df is None or df.empty:
            return set()
        col: str | None = None
        for candidate in ("trade_date", "date", "日期", "datetime", "时间", "day"):
            if candidate in df.columns:
                col = candidate
                break
        if col is None:
            col = df.columns[0]
        ts = cls._parse_trade_date_values(df[col]).dropna()
        if ts.empty:
            return set()
        return set(ts.dt.strftime("%Y-%m-%d").tolist())

    def _persist_trade_dates_cache(self, dates: set[str]) -> None:
        if not dates:
            return
        cache_df = pd.DataFrame({"trade_date": pd.Series(sorted(dates), dtype="object")})
        # Always write pickle first so environments without parquet engines can still load the cache.
        try:
            cache_df.to_pickle(self._trade_dates_pickle)
        except Exception:
            pass
        if has_parquet_engine():
            try:
                cache_df.to_parquet(self._trade_dates_parquet, index=False)
            except Exception:
                # Prevent unreadable stale parquet from shadowing valid pickle cache.
                try:
                    if self._trade_dates_parquet.exists():
                        self._trade_dates_parquet.unlink()
                except Exception:
                    pass

    @staticmethod
    def _build_weekday_trade_dates() -> set[str]:
        start = QDate(1990, 1, 1)
        end = QDate.currentDate().addYears(2)
        out: set[str] = set()
        cursor = QDate(start)
        while cursor <= end:
            if cursor.dayOfWeek() < 6:
                out.add(cursor.toString("yyyy-MM-dd"))
            cursor = cursor.addDays(1)
        return out

    def _load_trade_dates_from_cache(self) -> bool:
        if self._trade_dates_pickle.exists():
            candidates = [self._trade_dates_pickle]
        else:
            candidates = []
        if self._trade_dates_parquet.exists() and has_parquet_engine():
            candidates.append(self._trade_dates_parquet)
        if not candidates:
            return False

        for path in candidates:
            try:
                df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_pickle(path)
                if df is None or df.empty:
                    continue
                dates = self._extract_trade_dates(df)
                if not dates:
                    continue
                if self._is_synthetic_trade_dates(dates):
                    continue
                self._trade_dates = dates
                if self._trade_dates:
                    return True
            except Exception:
                continue
        return False

    @staticmethod
    def _is_synthetic_trade_dates(dates: set[str]) -> bool:
        if not dates:
            return True
        try:
            ts = pd.to_datetime(sorted(dates), errors="coerce")
            ts = ts[pd.notna(ts)]
            if len(ts) == 0:
                return True
            mn = pd.Timestamp(ts.min()).normalize()
            mx = pd.Timestamp(ts.max()).normalize()
            now = pd.Timestamp.now().normalize()
            # Synthetic weekday calendars typically start at 1990-01-01 and extend far into the future.
            if mn < pd.Timestamp("1990-12-01"):
                return True
            if mx > (now + pd.Timedelta(days=370)):
                return True
            # Stale or sparse calendars will break minute freshness checks around holidays.
            if mx < (now - pd.Timedelta(days=20)):
                return True
            last_year = int(now.year) - 1
            if last_year >= 1991:
                y_start = pd.Timestamp(f"{last_year}-01-01")
                y_end = pd.Timestamp(f"{last_year}-12-31")
                if int(((ts >= y_start) & (ts <= y_end)).sum()) < 180:
                    return True
            if int(now.month) >= 2:
                y_start = pd.Timestamp(f"{now.year}-01-01")
                if int(((ts >= y_start) & (ts <= now)).sum()) < 10:
                    return True
            return False
        except Exception:
            return True

    def _refresh_all_calendar_marks(self) -> None:
        for date_edit in (self.start_date_edit, self.end_date_edit, self.single_date_edit):
            cal = date_edit.calendarWidget()
            self._mark_calendar_page(cal, cal.yearShown(), cal.monthShown())

    def _mark_calendar_page(self, calendar: QCalendarWidget, year: int, month: int) -> None:
        if not self._trade_dates:
            return
        first = QDate(year, month, 1)
        if not first.isValid():
            return
        for day in range(1, first.daysInMonth() + 1):
            current = QDate(year, month, day)
            fmt = QTextCharFormat()
            key = current.toString("yyyy-MM-dd")
            if current.dayOfWeek() >= 6:
                fmt.setForeground(QColor("#8b92a3"))
            elif key not in self._trade_dates:
                fmt.setForeground(QColor("#ff8d8d"))
                fmt.setFontWeight(QFont.DemiBold)
                fmt.setToolTip("交易所休市")
            else:
                fmt.setForeground(QColor("#dfe3ed"))
            calendar.setDateTextFormat(current, fmt)

    def _on_date_changed(self, qdate: QDate, title: str) -> None:
        if not self._trade_dates:
            return
        key = qdate.toString("yyyy-MM-dd")
        if qdate.dayOfWeek() < 6 and key not in self._trade_dates:
            self._append_log(f"{title}: {key} 为休市日，可能无数据")

    def _position_floating_pod(self) -> None:
        if not hasattr(self, "floating_pod"):
            return
        margin = 18
        pod_size = self.floating_pod.sizeHint()
        self.floating_pod.resize(pod_size)
        root_rect = self.centralWidget().rect()
        x = max(margin, root_rect.width() - pod_size.width() - margin)
        y = max(margin, root_rect.height() - pod_size.height() - margin - 2)
        self.floating_pod.move(x, y)
        self.floating_pod.raise_()

    def _update_floating_pod_visibility(self) -> None:
        visible = (not self.quick_symbol_input.isHidden()) or (not self.progress_block.isHidden())
        self.floating_pod.setVisible(visible)
        if visible:
            self._position_floating_pod()

    def _hide_quick_symbol_input(self) -> None:
        if self.quick_symbol_input.isHidden():
            return
        self.quick_symbol_input.clear()
        self.quick_symbol_input.setVisible(False)
        self._symbol_completer.popup().hide()
        self._update_floating_pod_visibility()

    def _on_escape_pressed(self) -> None:
        if not self.quick_symbol_input.isHidden():
            self._hide_quick_symbol_input()

    def _ensure_symbol_lookup_ready(self) -> None:
        if self._symbol_records:
            return
        cached_data = self._symbol_service.load_cache()
        if cached_data is not None:
            self._set_symbol_data(cached_data)

    def _switch_left_page(self, index: int) -> None:
        self.left_pages.setCurrentIndex(index)
        self.nav_watch_btn.setChecked(index == self.PAGE_WATCH)
        self.nav_market_btn.setChecked(index == self.PAGE_MARKET)
        self.nav_data_btn.setChecked(index == self.PAGE_DATA)
        self.nav_help_btn.setChecked(index == self.PAGE_HELP)
        self._set_left_aux_visible(index != self.PAGE_WATCH)
        if index == self.PAGE_WATCH:
            self.right_pages.setCurrentIndex(self.RIGHT_PAGE_WATCH)
            self._ensure_watch_spot_loaded()
        elif index == self.PAGE_MARKET:
            self.right_pages.setCurrentIndex(self.RIGHT_PAGE_MARKET)
        elif index == self.PAGE_HELP:
            self.right_pages.setCurrentIndex(self.RIGHT_PAGE_HELP)
            self._reload_help_docs()
        else:
            self.right_pages.setCurrentIndex(self.RIGHT_PAGE_ANALYSIS)
            if (not self._trade_dates_prepared) and (not self._trade_dates_loading):
                self._prepare_trade_dates()
        if not self.quick_symbol_input.isHidden():
            self._hide_quick_symbol_input()
        if index == self.PAGE_MARKET and not self._market_initialized:
            self._market_initialized = True
            self._trigger_market_fetch()

    def _set_left_aux_visible(self, visible: bool) -> None:
        self.left_pages.setVisible(visible)
        if visible:
            width = self.NAV_WIDTH + self.LEFT_PANEL_WIDTH + self._left_shell_spacing
        else:
            width = self.NAV_WIDTH
        self.left_shell.setFixedWidth(width)

    def _load_watchlist(self) -> None:
        if not self._watchlist_file.exists():
            self._watchlist = []
            return
        try:
            payload = json.loads(self._watchlist_file.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                self._watchlist = [str(v).strip() for v in payload if str(v).strip()]
            else:
                self._watchlist = []
        except Exception:
            self._watchlist = []

    def _save_watchlist(self) -> None:
        try:
            self._watchlist_file.write_text(
                json.dumps(self._watchlist, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            self._append_log(f"自选列表保存失败: {exc}")

    def _refresh_watchlist_view(self) -> None:
        self.watch_list.setRowCount(len(self._watchlist))
        for row, symbol in enumerate(self._watchlist, start=1):
            name = self._symbol_code_to_name.get(symbol, "")
            self.watch_list.setItem(row - 1, 0, self._make_watch_item(str(row), align=Qt.AlignCenter))
            self.watch_list.setItem(row - 1, 1, self._make_watch_item(symbol))
            self.watch_list.setItem(row - 1, 2, self._make_watch_item(name or "--"))
            self.watch_list.setItem(row - 1, 3, self._make_watch_item(self._infer_watch_symbol_type(symbol), align=Qt.AlignCenter))
            for col in (4, 5, 6, 7, 8):
                self.watch_list.setItem(row - 1, col, self._make_watch_item("--", align=Qt.AlignRight | Qt.AlignVCenter))
        self._autosize_watch_columns()
        if self.left_pages.currentIndex() == self.PAGE_WATCH:
            self._ensure_watch_spot_loaded()
        else:
            self._watch_spot_pending = bool(self._watchlist)

    def _ensure_watch_spot_loaded(self, *, force: bool = False) -> None:
        if not self._watchlist:
            self._watch_spot_pending = False
            return
        if (not force) and (not self._ui_ready):
            self._watch_spot_pending = True
            return
        if self._watch_spot_loading:
            return
        if self._thread is not None:
            self._watch_spot_pending = True
            return
        if (not force) and self.left_pages.currentIndex() != self.PAGE_WATCH:
            self._watch_spot_pending = True
            return
        self._watch_spot_pending = False
        cache_covers_all = bool(self._watch_spot_cache) and all(
            code in self._watch_spot_cache for code in self._watchlist
        )
        cache_missing_key_metrics = any(
            (code not in self._watch_spot_cache)
            or (self._watch_spot_cache.get(code, {}).get("speed") is None)
            or (self._watch_spot_cache.get(code, {}).get("turnover") is None)
            for code in self._watchlist
        )
        if self._watch_spot_cache:
            self._apply_watch_spot_data(self._watch_spot_cache)
            if (
                not force
                and cache_covers_all
                and (not cache_missing_key_metrics)
                and (time.time() - self._watch_spot_cache_ts) <= 90
            ):
                return
        if not force and (not cache_missing_key_metrics) and (time.time() - self._watch_spot_cache_ts) <= 15:
            return
        now = time.time()
        if (now - self._watch_spot_last_notify_ts) >= 5:
            self._watch_spot_last_notify_ts = now
            self._append_log(f"自选行情后台更新中: {len(self._watchlist)} 只")
        self._watch_spot_loading = True
        threading.Thread(target=self._load_watch_spot_async, daemon=True).start()

    def _load_watch_spot_async(self) -> None:
        data: dict[str, dict[str, object]] | None = None
        error: str | None = None
        issues: list[str] = []
        watch_codes = set(self._watchlist)
        # Fast path first: per-symbol fallback usually returns quickly for a small watchlist.
        data = self._load_watch_spot_fallback_by_symbol(list(watch_codes))

        df, spot_err = self._run_with_timeout(lambda: ak.stock_zh_a_spot_em(), timeout_seconds=8.0)
        if spot_err is not None:
            issues.append(f"spot_em:{spot_err}")
        if isinstance(df, pd.DataFrame):
            try:
                code_col = self._pick_column(df, ("代码", "证券代码", "symbol"))
                if code_col is None:
                    raise RuntimeError("缺少代码列")
                name_col = self._pick_column(df, ("名称", "证券名称", "name"))
                price_col = self._pick_column(df, ("最新价", "现价", "price", "last"))
                pct_col = self._pick_column(df, ("涨跌幅", "pct_chg", "change_percent"))
                chg_col = self._pick_column(df, ("涨跌额", "涨跌", "change", "change_amount"))
                speed_col = self._pick_column(df, ("涨速", "涨速%", "speed", "change_speed", "change_speed_pct"))
                turn_col = self._pick_column(df, ("换手率", "换手", "换手率(%)", "turnover", "turnover_rate", "turnover_ratio"))
                if data is None:
                    data = {}
                for _, row in df.iterrows():
                    code = str(row.get(code_col, "")).strip().replace(".0", "")
                    if code.isdigit():
                        code = code.zfill(6)
                    if code not in watch_codes:
                        continue
                    existing = data.get(code, {})
                    name_val = str(row.get(name_col, "")).strip() if name_col else str(existing.get("name", "") or "")
                    price_val = self._to_float(row.get(price_col)) if price_col else None
                    pct_val = self._to_float(row.get(pct_col)) if pct_col else None
                    chg_val = self._to_float(row.get(chg_col)) if chg_col else None
                    speed_val = self._to_float(row.get(speed_col)) if speed_col else None
                    turnover_val = self._to_float(row.get(turn_col)) if turn_col else None
                    data[code] = {
                        "name": name_val or str(existing.get("name", "") or ""),
                        "price": price_val if price_val is not None else existing.get("price"),
                        "pct": pct_val if pct_val is not None else existing.get("pct"),
                        "chg": chg_val if chg_val is not None else existing.get("chg"),
                        "speed": speed_val if speed_val is not None else existing.get("speed"),
                        "turnover": turnover_val if turnover_val is not None else existing.get("turnover"),
                    }
            except Exception as exc:
                issues.append(f"spot_em_parse:{exc}")
        else:
            # Fallback: sina full market spot; does not include turnover/speed, but can refresh price/pct/chg.
            sina_df, sina_err = self._run_with_timeout(lambda: ak.stock_zh_a_spot(), timeout_seconds=8.0)
            if sina_err is not None:
                issues.append(f"spot_sina:{sina_err}")
            elif isinstance(sina_df, pd.DataFrame):
                try:
                    code_col = self._pick_column(sina_df, ("代码", "symbol"))
                    name_col = self._pick_column(sina_df, ("名称", "name"))
                    price_col = self._pick_column(sina_df, ("最新价", "price", "last"))
                    pct_col = self._pick_column(sina_df, ("涨跌幅", "pct_chg", "change_percent"))
                    chg_col = self._pick_column(sina_df, ("涨跌额", "涨跌", "change", "change_amount"))
                    if code_col is not None:
                        if data is None:
                            data = {}
                        for _, row in sina_df.iterrows():
                            code = str(row.get(code_col, "")).strip().replace(".0", "")
                            if code.isdigit():
                                code = code.zfill(6)
                            if code not in watch_codes:
                                continue
                            payload = data.get(code, {})
                            payload.update(
                                {
                                    "name": str(row.get(name_col, "")).strip() if name_col else payload.get("name", ""),
                                    "price": self._to_float(row.get(price_col)) if price_col else payload.get("price"),
                                    "pct": self._to_float(row.get(pct_col)) if pct_col else payload.get("pct"),
                                    "chg": self._to_float(row.get(chg_col)) if chg_col else payload.get("chg"),
                                }
                            )
                            data[code] = payload
                except Exception as exc:
                    issues.append(f"spot_sina_parse:{exc}")

        missing = [c for c in self._watchlist if not data or c not in data]
        if missing:
            fallback = self._load_watch_spot_fallback_by_symbol(missing)
            if fallback:
                if data is None:
                    data = {}
                data.update(fallback)
        metric_missing = [
            c
            for c in self._watchlist
            if (data is None)
            or (c not in data)
            or (data.get(c, {}).get("turnover") is None)
            or (data.get(c, {}).get("speed") is None)
        ]
        if metric_missing:
            metric_patch = self._load_watch_spot_fallback_by_symbol(metric_missing)
            if metric_patch:
                if data is None:
                    data = {}
                for code, patch in metric_patch.items():
                    base = data.get(code, {})
                    for key in ("name", "price", "pct", "chg", "speed", "turnover"):
                        if (base.get(key) is None) and (patch.get(key) is not None):
                            base[key] = patch.get(key)
                    data[code] = base

        if not data:
            if issues:
                error = " | ".join(issues)
            else:
                error = "自选行情数据为空"
        self.watch_spot_loaded.emit(data, error)

    @staticmethod
    def _run_with_timeout(callable_obj, timeout_seconds: float) -> tuple[object | None, str | None]:
        result: dict[str, object] = {}
        error: dict[str, str] = {}

        def _runner() -> None:
            try:
                result["value"] = callable_obj()
            except Exception as exc:
                error["message"] = str(exc)

        worker = threading.Thread(target=_runner, daemon=True)
        worker.start()
        worker.join(timeout=max(0.1, float(timeout_seconds)))
        if worker.is_alive():
            return None, "timeout"
        if "message" in error:
            return None, error["message"]
        return result.get("value"), None

    def _on_watch_spot_loaded(self, data: object, error: object) -> None:
        self._watch_spot_loading = False
        if error:
            self._append_log(f"自选行情更新失败: {error}")
            return
        spot = data if isinstance(data, dict) else {}
        prev_spot = dict(self._watch_spot_cache)
        prev_ts = float(self._watch_spot_cache_ts or 0.0)
        now_ts = time.time()
        # If realtime source misses speed, estimate from last cached price delta per minute.
        if prev_spot and prev_ts > 0:
            dt = max(1.0, now_ts - prev_ts)
            for code, payload in spot.items():
                if payload.get("turnover") is None:
                    payload["turnover"] = prev_spot.get(code, {}).get("turnover")
                if payload.get("speed") is not None:
                    continue
                cur = self._to_float(payload.get("price"))
                prev = self._to_float(prev_spot.get(code, {}).get("price"))
                if cur is None or prev in (None, 0):
                    continue
                payload["speed"] = (cur - float(prev)) / float(prev) * 100 * 60.0 / dt
        self._watch_spot_cache = spot
        self._watch_spot_cache_ts = now_ts
        self._save_watch_spot_snapshot(spot, self._watch_spot_cache_ts)
        self._apply_watch_spot_data(spot)
        miss_speed = sum(1 for code in self._watchlist if spot.get(code, {}).get("speed") is None)
        miss_turn = sum(1 for code in self._watchlist if spot.get(code, {}).get("turnover") is None)
        self._append_log(
            f"自选行情已更新: {len(self._watchlist)} 只 | 缺涨速 {miss_speed} | 缺换手 {miss_turn}"
        )

    def _apply_watch_spot_data(self, spot: dict[str, dict[str, object]]) -> None:
        for row, symbol in enumerate(self._watchlist):
            payload = spot.get(symbol, {})
            name = str(payload.get("name", "") or self._symbol_code_to_name.get(symbol, "") or "--")
            self.watch_list.setItem(row, 2, self._make_watch_item(name))
            self.watch_list.setItem(row, 3, self._make_watch_item(self._infer_watch_symbol_type(symbol), align=Qt.AlignCenter))
            self.watch_list.setItem(row, 4, self._make_watch_item(self._format_watch_value(payload.get("price")), align=Qt.AlignRight | Qt.AlignVCenter))
            self.watch_list.setItem(row, 5, self._make_watch_item(self._format_watch_value(payload.get("pct"), signed=True), align=Qt.AlignRight | Qt.AlignVCenter, color=self._watch_signed_color(payload.get("pct"))))
            self.watch_list.setItem(row, 6, self._make_watch_item(self._format_watch_value(payload.get("chg"), signed=True), align=Qt.AlignRight | Qt.AlignVCenter, color=self._watch_signed_color(payload.get("chg"))))
            self.watch_list.setItem(row, 7, self._make_watch_item(self._format_watch_value(payload.get("speed"), signed=True), align=Qt.AlignRight | Qt.AlignVCenter, color=self._watch_signed_color(payload.get("speed"))))
            self.watch_list.setItem(row, 8, self._make_watch_item(self._format_watch_value(payload.get("turnover")), align=Qt.AlignRight | Qt.AlignVCenter))
        self._autosize_watch_columns()

    def _load_watch_spot_fallback_by_symbol(self, symbols: list[str]) -> dict[str, dict[str, object]]:
        out: dict[str, dict[str, object]] = {}
        end_date = QDate.currentDate().toString("yyyyMMdd")
        start_date = QDate.currentDate().addDays(-40).toString("yyyyMMdd")
        for code in symbols:
            payload = self._fetch_single_watch_fallback(code, start_date, end_date)
            if payload is not None:
                out[code] = payload
        return out

    def _fetch_single_watch_fallback(self, code: str, start_date: str, end_date: str) -> dict[str, object] | None:
        quote = self._normalize_a_quote_symbol(code)
        turnover_val: float | None = None
        speed_val: float | None = None

        # Try to get turnover from recent daily bars (eastmoney often carries 换手率).
        try:
            df_turn = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            turn_col = self._pick_column(df_turn, ("换手率", "换手", "换手率(%)", "turnover", "turnover_rate", "turnover_ratio"))
            if turn_col is not None and not df_turn.empty:
                turnover_val = self._to_float(df_turn.iloc[-1][turn_col])
        except Exception:
            pass

        # Approximate speed by latest minute-to-minute change when available.
        try:
            end_qdate = QDate.fromString(end_date, "yyyyMMdd")
            if not end_qdate.isValid():
                end_qdate = QDate.currentDate()
            start_qdate = end_qdate.addDays(-1)
            min_df = ak.stock_zh_a_hist_min_em(
                symbol=code,
                start_date=f"{start_qdate.toString('yyyy-MM-dd')} 09:30:00",
                end_date=f"{end_qdate.toString('yyyy-MM-dd')} 15:00:00",
                period="1",
                adjust="",
            )
            if min_df is not None and len(min_df.index) >= 2:
                close_col = self._pick_column(min_df, ("收盘", "最新价", "close", "price"))
                if close_col is not None:
                    close_vals = pd.to_numeric(min_df[close_col], errors="coerce").dropna()
                    if len(close_vals.index) >= 2 and float(close_vals.iloc[-2]) != 0:
                        speed_val = (float(close_vals.iloc[-1]) - float(close_vals.iloc[-2])) / float(close_vals.iloc[-2]) * 100
        except Exception:
            pass
        if speed_val is None:
            try:
                tick_df = ak.stock_zh_a_tick_tx_js(symbol=quote)
                price_col = self._pick_column(tick_df, ("成交价格", "price"))
                if price_col is not None and tick_df is not None and len(tick_df.index) >= 2:
                    prices = pd.to_numeric(tick_df[price_col], errors="coerce").dropna()
                    if len(prices.index) >= 2 and float(prices.iloc[-2]) != 0:
                        speed_val = (float(prices.iloc[-1]) - float(prices.iloc[-2])) / float(prices.iloc[-2]) * 100
            except Exception:
                pass

        loaders = (
            ("sina_daily", lambda: self._fetch_watch_sina_daily_safe(symbol=quote, start_date=start_date, end_date=end_date, adjust="qfq")),
            ("eastmoney_daily", lambda: ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")),
            ("tencent_daily", lambda: ak.stock_zh_a_hist_tx(symbol=quote, start_date=start_date, end_date=end_date, adjust="qfq")),
        )
        best_payload: dict[str, object] | None = None
        for source_name, loader in loaders:
            try:
                df = loader()
                if df is None or df.empty:
                    continue
                close_col = self._pick_column(df, ("收盘", "close"))
                pct_col = self._pick_column(df, ("涨跌幅", "pct_chg", "change_percent"))
                chg_col = self._pick_column(df, ("涨跌额", "涨跌", "change", "change_amount"))
                turn_col = self._pick_column(df, ("换手率", "换手", "换手率(%)", "turnover", "turnover_rate", "turnover_ratio"))
                if close_col is None:
                    continue
                close_series = pd.to_numeric(df[close_col], errors="coerce").dropna()
                if close_series.empty:
                    continue
                last_close = float(close_series.iloc[-1])
                prev_close = float(close_series.iloc[-2]) if len(close_series.index) >= 2 else None
                pct_val = self._to_float(df.iloc[-1][pct_col]) if pct_col else None
                chg_val = self._to_float(df.iloc[-1][chg_col]) if chg_col else None
                if chg_val is None and prev_close not in (None, 0):
                    chg_val = last_close - float(prev_close)
                if pct_val is None and prev_close not in (None, 0):
                    pct_val = (last_close - float(prev_close)) / float(prev_close) * 100
                payload = {
                    "name": self._symbol_code_to_name.get(code, ""),
                    "price": last_close,
                    "pct": pct_val,
                    "chg": chg_val,
                    "speed": speed_val,
                    "turnover": self._normalize_turnover_pct(df.iloc[-1][turn_col], source=source_name) if turn_col else turnover_val,
                }
                if payload.get("turnover") is not None:
                    return payload
                if best_payload is None:
                    best_payload = payload
            except Exception:
                continue
        return best_payload

    def _fetch_watch_sina_daily_safe(
        self,
        *,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        start_dash = self._watch_compact_to_dash_date(start_date)
        end_dash = self._watch_compact_to_dash_date(end_date)
        try:
            df = ak.stock_zh_a_daily(
                symbol=symbol,
                start_date=start_dash,
                end_date=end_dash,
                adjust=adjust,
            )
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df
        except Exception:
            pass
        df_all = ak.stock_zh_a_daily(symbol=symbol, adjust=adjust)
        if not isinstance(df_all, pd.DataFrame) or df_all.empty:
            return pd.DataFrame()
        filtered = self._filter_watch_daily_by_compact_date(df_all, start_date=start_date, end_date=end_date)
        if not filtered.empty:
            return filtered
        return df_all

    @staticmethod
    def _watch_compact_to_dash_date(value: str) -> str:
        text = str(value or "").strip()
        try:
            return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            return text

    @staticmethod
    def _filter_watch_daily_by_compact_date(df: pd.DataFrame, *, start_date: str, end_date: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        date_col = None
        for candidate in ("date", "日期"):
            if candidate in df.columns:
                date_col = candidate
                break
        if date_col is None:
            return pd.DataFrame()
        series = pd.to_datetime(df[date_col], errors="coerce")
        start = pd.to_datetime(start_date, format="%Y%m%d", errors="coerce")
        end = pd.to_datetime(end_date, format="%Y%m%d", errors="coerce")
        if pd.isna(start) or pd.isna(end):
            return pd.DataFrame()
        mask = series.between(start, end)
        return df.loc[mask].reset_index(drop=True)

    def _normalize_turnover_pct(self, value: object, *, source: str = "") -> float | None:
        parsed = self._to_float(value)
        if parsed is None:
            return None
        if source == "sina_daily":
            return float(parsed) * 100.0
        return float(parsed)

    @staticmethod
    def _normalize_a_quote_symbol(code: str) -> str:
        raw = str(code or "").strip().lower().replace("sh", "").replace("sz", "").replace("bj", "")
        if raw.startswith(("6", "5", "9")):
            return f"sh{raw}"
        if raw.startswith(("8", "4")):
            return f"bj{raw}"
        return f"sz{raw}"

    @staticmethod
    def _is_etf_code(code: str) -> bool:
        raw = str(code or "").strip().lower().replace("sh", "").replace("sz", "").replace("bj", "")
        if not (raw.isdigit() and len(raw) == 6):
            return False
        return raw.startswith(
            (
                "510",
                "511",
                "512",
                "513",
                "515",
                "516",
                "518",
                "588",
                "159",
                "563",
                "561",
                "513",
                "501",
            )
        )

    def _infer_watch_symbol_type(self, symbol: str) -> str:
        raw = str(symbol or "").strip().lower()
        if not raw:
            return "--"
        # Preserve explicit index prefixes when user types sh/sz index codes.
        code = raw.replace("sh", "").replace("sz", "").replace("bj", "")
        if not (code.isdigit() and len(code) == 6):
            return "--"

        if self._is_etf_code(code):
            return "ETF"

        if raw.startswith(("sh", "sz")) and code in UnifiedDataModule.KNOWN_INDEX_CODES:
            return "指数"

        if code.startswith(("399", "980")) or code in UnifiedDataModule.KNOWN_INDEX_CODES:
            # Prefer A股 when code exists in the current stock list (e.g. 000001).
            if code not in self._symbol_code_to_name:
                return "指数"

        if code.startswith(("8", "4")):
            return "北交所"

        return "A股"

    def _load_watch_spot_snapshot(self) -> None:
        path = self._watch_spot_snapshot_file
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            data = payload.get("data")
            ts = payload.get("ts")
            if isinstance(data, dict):
                self._watch_spot_cache = data
            if isinstance(ts, (int, float)):
                self._watch_spot_cache_ts = float(ts)
        except Exception:
            self._watch_spot_cache = {}
            self._watch_spot_cache_ts = 0.0

    def _save_watch_spot_snapshot(self, data: dict[str, dict[str, object]], ts: float) -> None:
        try:
            payload = {"ts": float(ts), "data": data}
            self._watch_spot_snapshot_file.write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            return

    def _autosize_watch_columns(self) -> None:
        header = self.watch_list.horizontalHeader()
        min_widths = {0: 52, 1: 92, 2: 132, 3: 66, 4: 86, 5: 84, 6: 84, 7: 84, 8: 84}
        self._watch_resizing_columns = True
        try:
            for col in range(self.watch_list.columnCount()):
                if col in self._watch_manual_width_cols:
                    continue
                header.setSectionResizeMode(col, QHeaderView.ResizeToContents)
                self.watch_list.resizeColumnToContents(col)
                width = max(self.watch_list.columnWidth(col), min_widths.get(col, 72))
                self.watch_list.setColumnWidth(col, width)
                header.setSectionResizeMode(col, QHeaderView.Interactive)
            max_widths = {2: 186}
            for col, max_width in max_widths.items():
                if col in self._watch_manual_width_cols:
                    continue
                if self.watch_list.columnWidth(col) > max_width:
                    self.watch_list.setColumnWidth(col, max_width)
        finally:
            self._watch_resizing_columns = False

    def _on_watch_header_resized(self, logical_index: int, _old_size: int, new_size: int) -> None:
        if self._watch_resizing_columns:
            return
        if new_size > 0:
            self._watch_manual_width_cols.add(int(logical_index))

    @staticmethod
    def _format_watch_value(value: object, *, signed: bool = False) -> str:
        if value is None:
            return "--"
        try:
            num = float(value)
            if signed:
                sign = "+" if num > 0 else ""
                return f"{sign}{num:.2f}"
            return f"{num:.2f}"
        except Exception:
            return "--"

    @staticmethod
    def _watch_signed_color(value: object) -> str | None:
        try:
            v = float(value)
        except Exception:
            return None
        if v > 0:
            return "#ff4d4f"
        if v < 0:
            return "#47c16f"
        return "#d8deea"

    @staticmethod
    def _make_watch_item(
        text: str,
        *,
        align: Qt.AlignmentFlag = Qt.AlignLeft | Qt.AlignVCenter,
        color: str | None = None,
    ) -> QTableWidgetItem:
        item = QTableWidgetItem(text)
        item.setTextAlignment(int(align))
        if color:
            item.setForeground(QBrush(QColor(color)))
        return item

    def _add_watch_symbol_value(self, raw: str) -> tuple[bool, str]:
        self._ensure_symbol_lookup_ready()
        symbol = SymbolService.resolve_code(raw, self._symbol_name_to_code).strip()
        if not symbol:
            return False, ""
        if symbol in self._watchlist:
            self._append_log(f"自选已存在: {symbol}")
            return False, symbol
        self._watchlist.append(symbol)
        self._save_watchlist()
        self._refresh_watchlist_view()
        return True, symbol

    def _remove_selected_watch_symbol(self) -> None:
        row = self.watch_list.currentRow()
        if row < 0:
            return
        code_item = self.watch_list.item(row, 1)
        if code_item is None:
            return
        symbol = str(code_item.text()).strip()
        self._watchlist = [s for s in self._watchlist if s != symbol]
        self._save_watchlist()
        self._refresh_watchlist_view()
        self._append_log(f"已移除自选: {symbol}")

    def _on_watch_item_activated(self, row: int, _column: int) -> None:
        code_item = self.watch_list.item(row, 1)
        if code_item is None:
            return
        symbol = str(code_item.text()).strip()
        self._activate_watch_symbol(symbol)

    def _on_watch_list_context_menu(self, pos: QPoint) -> None:
        item = self.watch_list.itemAt(pos)
        if item is None:
            return
        self.watch_list.selectRow(item.row())
        menu = QMenu(self)
        delete_action = menu.addAction("删除自选")
        selected = menu.exec(self.watch_list.viewport().mapToGlobal(pos))
        if selected == delete_action:
            self._remove_selected_watch_symbol()

    def _activate_watch_symbol(self, symbol: str) -> None:
        if not symbol:
            return
        was_market_initialized = self._market_initialized
        self._auto_select_market_dataset_for_symbol(symbol)
        self._market_symbol = symbol
        self.symbol_input.setText(symbol)
        self._refresh_market_identity()
        self._switch_left_page(self.PAGE_MARKET)
        if was_market_initialized:
            self._trigger_market_fetch()
        self._append_log(f"已从自选载入代码并切换到行情: {symbol}")

    def _set_visual_state(self, widget: QWidget, active: bool) -> None:
        widget.setProperty("inactive", "false" if active else "true")
        widget.style().unpolish(widget)
        widget.style().polish(widget)

    def _auto_select_market_dataset_for_symbol(self, symbol: str) -> None:
        self._ensure_symbol_lookup_ready()
        current = self.market_dataset_combo.currentData()
        current_value = current if isinstance(current, str) else "index_daily"
        target_value = UnifiedDataModule.resolve_market_dataset(
            symbol,
            stock_codes=self._symbol_code_to_name.keys(),
            fallback=current_value,
        )
        if target_value == current_value:
            return
        idx = self.market_dataset_combo.findData(target_value)
        if idx < 0:
            return
        blocker = QSignalBlocker(self.market_dataset_combo)
        self.market_dataset_combo.setCurrentIndex(idx)
        del blocker
        self._sync_market_chart_mode()

    def _set_label_state(self, label: QLabel, active: bool) -> None:
        label.setProperty("muted", "false" if active else "true")
        label.style().unpolish(label)
        label.style().polish(label)

    def _on_enter_pressed(self) -> None:
        if self._thread is not None:
            return
        page_index = self.left_pages.currentIndex()
        if page_index == self.PAGE_HELP:
            return
        if page_index == self.PAGE_WATCH:
            if not self.quick_symbol_input.isVisible():
                self._show_quick_symbol_input()
                return
            if self.quick_symbol_input.text().strip():
                self._on_quick_symbol_return()
                return
            self.quick_symbol_input.setFocus()
            self.quick_symbol_input.selectAll()
            return

        if page_index == self.PAGE_MARKET:
            if not self.quick_symbol_input.isVisible():
                self._show_quick_symbol_input()
                return
            if self.quick_symbol_input.text().strip():
                self._on_quick_symbol_return()
                return
            self.quick_symbol_input.setFocus()
            self.quick_symbol_input.selectAll()
            return

        option = self._current_dataset_option()
        if self._requires_symbol(option):
            if not self.quick_symbol_input.isVisible():
                self._show_quick_symbol_input()
                return
            if self.quick_symbol_input.text().strip():
                self._on_quick_symbol_return()
                return
            self.quick_symbol_input.setFocus()
            self.quick_symbol_input.selectAll()
            return
        self._trigger_fetch()

    def _show_quick_symbol_input(self) -> None:
        page_index = self.left_pages.currentIndex()
        if page_index == self.PAGE_WATCH:
            self.quick_symbol_input.setPlaceholderText("输入代码/名称后回车加入自选")
        elif page_index == self.PAGE_MARKET:
            self.quick_symbol_input.setPlaceholderText("输入代码/名称后回车查看行情")
            if not self.quick_symbol_input.text().strip():
                self.quick_symbol_input.setText(self._market_symbol)
        else:
            self.quick_symbol_input.setPlaceholderText("输入代码/名称后回车")
            if not self.quick_symbol_input.text().strip():
                self.quick_symbol_input.setText(self.symbol_input.text())
        self._ensure_symbol_completer_ready()
        self.quick_symbol_input.setVisible(True)
        self._update_floating_pod_visibility()
        self.quick_symbol_input.setFocus()
        self.quick_symbol_input.selectAll()

    def _on_quick_symbol_text_edited(self, text: str) -> None:
        query = text.strip()
        if not query:
            self._symbol_completer.popup().hide()
            return

        suggestions = SymbolService.build_suggestions(query, self._symbol_records)
        if not suggestions:
            suggestions = self._symbol_candidates[:20]
        if suggestions:
            self._symbol_model.setStringList(suggestions)
        self._symbol_completer.setCompletionPrefix(query)
        self._symbol_completer.complete()

    def _on_quick_symbol_return(self) -> None:
        page_index = self.left_pages.currentIndex()
        if page_index == self.PAGE_HELP:
            self._hide_quick_symbol_input()
            return
        if page_index == self.PAGE_WATCH:
            raw = self.quick_symbol_input.text().strip()
            if not raw:
                self.quick_symbol_input.setPlaceholderText("请输入股票代码或名称")
                return
            added, symbol = self._add_watch_symbol_value(raw)
            if added:
                self._hide_quick_symbol_input()
                self._append_log(f"已加入自选: {symbol}")
            return

        if page_index == self.PAGE_MARKET:
            code = SymbolService.resolve_code(self.quick_symbol_input.text(), self._symbol_name_to_code)
            if not code:
                self.quick_symbol_input.setPlaceholderText("请输入股票或指数代码")
                return
            self._auto_select_market_dataset_for_symbol(code)
            self._market_symbol = code
            self.symbol_input.setText(code)
            self._refresh_market_identity()
            self._hide_quick_symbol_input()
            self._trigger_market_fetch()
            return

        option = self._current_dataset_option()
        if not self._requires_symbol(option):
            self._hide_quick_symbol_input()
            self._trigger_fetch()
            return

        code = SymbolService.resolve_code(self.quick_symbol_input.text(), self._symbol_name_to_code)
        if not code:
            self.quick_symbol_input.setPlaceholderText("请输入股票或指数代码")
            return
        self.symbol_input.setText(code)
        self._hide_quick_symbol_input()
        self._trigger_fetch()

    def _trigger_fetch(self) -> None:
        if self.fetch_button.isEnabled() and self._thread is None:
            self._on_fetch_clicked()

    def _trigger_market_fetch(self) -> None:
        if self.market_refresh_button.isEnabled() and self._thread is None:
            self._on_market_refresh_clicked()

    def _ensure_symbol_completer_ready(self) -> None:
        if self._symbol_completer_ready or self._symbol_completer_loading:
            return

        if not self.refresh_cache_check.isChecked():
            cached_data = self._symbol_service.load_cache()
            if cached_data is not None:
                self._set_symbol_data(cached_data)
                self._symbol_completer_ready = True
                self._append_log(f"股票列表已从本地缓存加载，共 {len(self._symbol_candidates)} 条")
                return

        self._symbol_completer_loading = True
        self._append_log("正在加载股票列表推荐...")
        threading.Thread(target=self._load_symbol_data_async, daemon=True).start()

    def _load_symbol_data_async(self) -> None:
        data: SymbolData | None = None
        error: str | None = None
        try:
            data = self._symbol_service.fetch_remote()
        except Exception as exc:
            error = str(exc)
        self.symbol_data_loaded.emit(data, error)

    def _on_symbol_data_loaded(
        self,
        data: object,
        error: object,
    ) -> None:
        self._symbol_completer_loading = False
        if error:
            self._append_log(f"股票列表加载失败，将仅支持手动代码输入: {error}")
            return
        symbol_data = data if isinstance(data, SymbolData) else None
        if symbol_data is None:
            self._append_log("股票列表加载失败，将仅支持手动代码输入: 数据为空")
            return
        self._set_symbol_data(symbol_data)
        self._symbol_completer_ready = True
        try:
            self._symbol_service.save_cache(symbol_data)
        except Exception as exc:
            self._append_log(f"写入股票列表缓存失败: {exc}")
        self._append_log(f"股票列表已加载，共 {len(self._symbol_candidates)} 条")

    def _set_symbol_data(self, symbol_data: SymbolData) -> None:
        self._symbol_records = list(symbol_data.records)
        self._symbol_candidates = list(symbol_data.candidates)
        self._symbol_name_to_code = dict(symbol_data.name_to_code)
        self._symbol_code_to_name = {code: name for code, name in self._symbol_records if name}
        self._symbol_model.setStringList(self._symbol_candidates[:20])
        self._refresh_watchlist_view()

    def _on_fetch_clicked(self) -> None:
        option = self._current_dataset_option()
        use_history = self._requires_symbol(option) and self.full_history_check.isChecked()
        if use_history:
            start_date = "1990-01-01"
            end_date = QDate.currentDate().toString("yyyy-MM-dd")
        else:
            start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
            end_date = self.end_date_edit.date().toString("yyyy-MM-dd")

        request = FetchRequest(
            dataset=option.value,
            symbol=self.symbol_input.text(),
            start_date=start_date,
            end_date=end_date,
            single_date=self.single_date_edit.date().toString("yyyy-MM-dd"),
            force_refresh=self.refresh_cache_check.isChecked(),
            period="daily",
        )
        self._active_fetch_scene = "data"
        self._set_fetch_buttons_enabled(False)
        self._set_fetch_progress(active=True)
        mode = "强制刷新" if request.force_refresh else "优先缓存"
        if use_history:
            self._append_log("已启用历史模式：忽略开始/结束日期控件")
        self._append_log(f"开始抓取: {option.label} ({mode})")
        self._start_worker(request)

    def _on_market_refresh_clicked(self) -> None:
        if self._thread is not None:
            self._market_fetch_pending = True
            return
        request = self._build_market_request()
        if self._try_render_market_from_memory_cache(request):
            return
        # Always route refresh through the data service so cache freshness rules apply.
        self._market_fetch_pending = False
        self._active_fetch_scene = "market"
        self._set_fetch_buttons_enabled(False)
        self._set_fetch_progress(active=True)
        mode = "强制刷新" if request.force_refresh else "优先缓存"
        dataset_name = "指数" if request.dataset == "index_daily" else "A股"
        self._append_log(
            f"开始抓取: 行情 {dataset_name}{self._period_label_text(request.period)} {self._market_symbol} ({mode})"
        )
        self._append_log(
            f"请求参数: dataset={request.dataset}, symbol={request.symbol}, period={request.period}, "
            f"start={request.start_date}, end={request.end_date}"
        )
        self._start_worker(request)

    def _market_request_cache_key(self, request: FetchRequest) -> tuple[str, str, str, str, str]:
        return (
            str(request.dataset),
            str(request.symbol).strip().lower(),
            str(request.period),
            str(request.start_date),
            str(request.end_date),
        )

    def _try_render_market_from_memory_cache(self, request: FetchRequest) -> bool:
        if request.force_refresh:
            return False
        key = self._market_request_cache_key(request)
        cached = self._market_mem_cache.get(key)
        if cached is None:
            return False
        if not self._is_market_response_fresh(cached, request):
            self._append_log("内存缓存末端落后，自动执行网络增量更新")
            try:
                del self._market_mem_cache[key]
            except KeyError:
                pass
            return False
        self._market_mem_cache.move_to_end(key, last=True)
        self._active_fetch_scene = "market"
        self._render_market_response(cached)
        self._append_log(
            f"抓取完成: {cached.title}，共 {len(cached.dataframe.index)} 行 | 来源: 内存缓存"
        )
        if not self.quick_symbol_input.isHidden():
            self._hide_quick_symbol_input()
        return True

    def _build_market_request(self) -> FetchRequest:
        period = self.market_cycle_combo.currentData()
        period_value = period if isinstance(period, str) else "daily"
        today = QDate.currentDate()
        if period_value in {"1", "5", "15", "30", "60"}:
            # Align with data service minute checkpoint so we don't constantly treat the latest intraday
            # payload as stale (e.g. end fixed at 15:00 while checkpoint is 10:12).
            checkpoint = DataService._minute_checkpoint_for_period(period_value)
            end_ts = pd.to_datetime(checkpoint, format="%Y%m%d%H%M", errors="coerce")
            if pd.isna(end_ts):
                end_ts = pd.Timestamp.now().floor("min")
            end_text = pd.Timestamp(end_ts).strftime("%Y-%m-%d %H:%M:%S")
            start_ts = (pd.Timestamp(end_ts).normalize() - pd.Timedelta(days=self._minute_window_days(period_value))).replace(
                hour=9,
                minute=30,
                second=0,
            )
            start_text = pd.Timestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S")
        else:
            checkpoint = DataService._daily_checkpoint()
            start_text = "1990-01-01"
            end_text = DataService._as_dash_date(checkpoint)

        dataset = self.market_dataset_combo.currentData()
        dataset_value = dataset if isinstance(dataset, str) else "index_daily"
        return FetchRequest(
            dataset=dataset_value,
            symbol=self._market_symbol,
            start_date=start_text,
            end_date=end_text,
            single_date=today.toString("yyyy-MM-dd"),
            force_refresh=self.refresh_cache_check.isChecked(),
            period=period_value,
        )

    def _set_fetch_buttons_enabled(self, enabled: bool) -> None:
        self.fetch_button.setEnabled(enabled)
        self.market_refresh_button.setEnabled(enabled)

    def _start_worker(self, request: FetchRequest) -> None:
        self._last_fetch_request = request
        self._thread = QThread(self)
        self._worker = FetchWorker(request)
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.progress.connect(self._on_fetch_progress)
        self._worker.success.connect(self._on_fetch_success)
        self._worker.error.connect(self._on_fetch_error)
        self._worker.finished.connect(self._thread.quit)
        self._worker.finished.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.finished.connect(self._on_worker_finished)
        self._thread.start()

    def _on_fetch_success(self, response: FetchResponse) -> None:
        if self._active_fetch_scene == "market":
            self._render_market_response(response)
            if self._last_fetch_request is not None:
                key = self._market_request_cache_key(self._last_fetch_request)
                self._market_mem_cache[key] = response
                self._market_mem_cache.move_to_end(key, last=True)
                while len(self._market_mem_cache) > int(self._market_mem_cache_limit):
                    self._market_mem_cache.popitem(last=False)
        else:
            self.result_title.setText(response.title)
            self._render_dataframe(response.dataframe)
        source = "本地缓存" if response.from_cache else "网络更新"
        self._append_log(
            f"抓取完成: {response.title}，共 {len(response.dataframe.index)} 行 | 来源: {source}"
        )
        cache_path = Path(response.cache_path)
        cache_dir_display: str | Path = cache_path.parent
        try:
            cache_dir_display = cache_path.parent.relative_to(self._cache_root.parent)
        except ValueError:
            pass
        self._append_log(f"缓存文件: {cache_path.name}")
        self._append_log(f"缓存目录: {cache_dir_display}")
        if not self.quick_symbol_input.isHidden():
            self._hide_quick_symbol_input()
        if self.refresh_cache_check.isChecked():
            self.refresh_cache_check.setChecked(False)

    def _on_fetch_error(self, message: str) -> None:
        friendly = self._humanize_fetch_error(message)
        self._append_log(f"抓取失败:\n{friendly}")
        if self.refresh_cache_check.isChecked():
            self.refresh_cache_check.setChecked(False)
            self._append_log("已自动取消强制刷新，后续将优先使用缓存/增量更新。")
        title = "行情获取失败" if self._active_fetch_scene == "market" else "分析获取失败"
        QMessageBox.critical(self, title, friendly)

    def _on_worker_finished(self) -> None:
        self._set_fetch_buttons_enabled(True)
        self._set_fetch_progress(active=False)
        self._thread = None
        self._worker = None
        self._last_fetch_request = None
        if (
            self._market_fetch_pending
            and self.left_pages.currentIndex() == self.PAGE_MARKET
            and self._market_initialized
        ):
            self._market_fetch_pending = False
            self._on_market_refresh_clicked()
            return
        if self._watch_spot_pending and self.left_pages.currentIndex() == self.PAGE_WATCH:
            self._ensure_watch_spot_loaded()

    def _on_fetch_progress(self, percent: int, message: str) -> None:
        if self.progress_block.isHidden():
            return
        now = time.time()
        value = max(0, min(100, int(percent)))
        raw_message = (message or "下载中").strip()
        self.progress_bar.setValue(value)
        self.progress_status.setText(self._compact_progress_text(raw_message))
        self.progress_status.setToolTip(raw_message)
        self.progress_percent.setText(f"{value}%")
        if (
            raw_message != self._last_progress_log_text
            and (now - self._last_progress_log_ts) >= 1.5
            and value < 100
        ):
            self._last_progress_log_text = raw_message
            self._last_progress_log_ts = now
            self._append_log(f"下载进度 {value}%: {raw_message}")

    @staticmethod
    def _compact_progress_text(message: str) -> str:
        text = re.sub(r"\s+", " ", str(message or "")).strip()
        return text or "下载中"

    @staticmethod
    def _humanize_fetch_error(message: str) -> str:
        raw = (message or "").strip()
        if not raw:
            return "数据请求失败，请稍后重试。"
        if "Traceback" in raw:
            return "程序内部异常，建议重启后重试；若重复出现请提供日志。"

        text = raw
        text = text.replace("eastmoney:", "东财源: ")
        text = text.replace("sina:", "新浪源: ")
        text = text.replace("tencent:", "腾讯源: ")
        text = text.replace("tx_min:", "腾讯分钟源: ")
        text = text.replace("tx_window:", "腾讯窗口源: ")
        text = text.replace("新浪源: date", "新浪源: 时间字段解析异常（上游格式波动）")
        text = text.replace("index_min:", "指数分钟源: ")
        text = text.replace("RemoteDisconnected", "远端连接中断")
        text = text.replace("Connection aborted.", "连接被中断")
        text = text.replace("Read timed out", "请求超时")
        text = text.replace("sparse_result", "该时间段返回为空（可能休市）")
        text = text.replace("left_gap", "返回数据覆盖不足（窗口型分钟源限制）")
        text = text.replace("low_density", "返回序列异常稀疏（上游数据不完整）")
        text = text.replace("stale_tail", "返回数据末端滞后（未到最新交易日）")
        text = text.replace("all_sources_stale", "所有可用源返回旧数据（未到最新交易日）")
        text = text.replace(
            "Value based partial slicing on non-monotonic DatetimeIndexes with non-existing keys is not allowed",
            "时间索引异常（上游源数据有缺陷）",
        )
        text = re.sub(r"Remote end closed connection without response", "远端未返回响应", text)
        text = re.sub(r"You might want to try:.*?(?=\s*\|\s*|$)", "建议稍后重试。", text)
        text = re.sub(
            r"time data\s+['\"][^'\"]+['\"]\s+doesn?t match format\s+['\"][^'\"]+['\"]",
            "时间字段格式异常（上游源格式波动）",
            text,
        )
        text = text.replace("doesnt match format", "时间字段格式异常（上游源格式波动）")
        text = re.sub(r"[()']+", "", text)
        text = re.sub(r"\s+,", ",", text)
        text = re.sub(r"\s*\|\s*", "\n- ", text)
        text = re.sub(r"\s{2,}", " ", text).strip()

        hints: list[str] = []
        if any(k in raw for k in ("RemoteDisconnected", "timed out", "Connection aborted")):
            hints.append("网络波动或上游源站不稳定")
        if "sparse_result" in raw:
            hints.append("所选区间可能是休市日或无交易数据")
        if "left_gap" in raw:
            hints.append("分钟源只返回最近窗口，系统会尝试切换可按区间下载的源")
        if "stale_tail" in raw:
            hints.append("上游源返回较旧数据，系统已尝试切换其它源")
        if "all_sources_stale" in raw:
            hints.append("多个源都返回旧数据，建议稍后重试或切换周期验证")
        if "index_min" in raw:
            hints.append("分钟级指数接口不稳定，可先切换到日线验证")
        if "doesnt match format" in raw:
            hints.append("新浪源返回时间格式波动，系统会自动切换其它源")
        if "non-monotonic DatetimeIndexes" in raw:
            hints.append("腾讯源时间索引异常，建议稍后重试")

        if hints:
            hint_lines = "\n".join(f"• {h}" for h in hints)
            return f"{text}\n\n可能原因:\n{hint_lines}"
        return text

    def _render_market_response(self, response: FetchResponse) -> None:
        df = response.dataframe
        date_col = self._pick_column(df, ("日期", "date", "trade_date", "datetime"))
        if date_col is not None and df is not None and not df.empty:
            ts = self._parse_trade_date_values(df[date_col])
            ts_clean = ts.dropna()
            if not ts_clean.empty:
                safe = df.copy()
                safe["_market_dt_"] = ts
                safe = safe.dropna(subset=["_market_dt_"]).sort_values("_market_dt_")
                safe = safe.drop(columns=["_market_dt_"]).reset_index(drop=True)
                if not safe.empty:
                    df = safe
                    date_col = self._pick_column(df, ("日期", "date", "trade_date", "datetime"))
        self._sync_market_chart_mode()
        self._refresh_market_identity()
        if date_col is not None and not df.empty:
            ts = self._parse_trade_date_values(df[date_col]).dropna()
            if not ts.empty:
                start_date = pd.Timestamp(ts.min()).strftime("%Y.%m.%d")
                end_date = pd.Timestamp(ts.max()).strftime("%Y.%m.%d")
            else:
                start_date = self._fmt_meta_date(df.iloc[0][date_col])
                end_date = self._fmt_meta_date(df.iloc[-1][date_col])
            self.market_meta_title.setText(self._fmt_meta_range(start_date, end_date))
        else:
            self.market_meta_title.setText("--")
        self.market_chart.set_dataframe(df)
        self._update_market_summary(df)
        self._update_market_extensions(df)
        self._refresh_market_sector_badge()

    def _is_market_response_fresh(self, response: FetchResponse, request: FetchRequest) -> bool:
        period = DataService._normalize_period(str(request.period or "daily"))
        minute_mode = period in {"1", "5", "15", "30", "60"}
        end_value = str(request.end_date or "").strip()
        if minute_mode:
            if len(end_value) <= 10:
                end_value = f"{end_value} 15:00:00"
        else:
            parsed = pd.to_datetime(end_value, errors="coerce")
            if pd.isna(parsed):
                return False
            end_value = pd.Timestamp(parsed).strftime("%Y%m%d")
        try:
            return DataService._is_result_fresh_for_request_end(
                response.dataframe,
                end_value=end_value,
                period=period,
                minute_mode=minute_mode,
            )
        except Exception:
            return False

    def _update_market_summary(self, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        date_col = self._pick_column(df, ("日期", "date", "trade_date", "datetime"))
        open_col = self._pick_column(df, ("开盘", "open", "open_price"))
        high_col = self._pick_column(df, ("最高", "high", "high_price"))
        low_col = self._pick_column(df, ("最低", "low", "low_price"))
        close_col = self._pick_column(df, ("收盘", "收盘价", "close", "latest"))
        volume_col = self._pick_column(df, ("成交量", "volume", "vol"))
        amount_col = self._pick_column(df, ("成交额", "amount", "turnover"))
        volume_from_amount = False
        if volume_col is None and amount_col is not None:
            amount_probe = pd.to_numeric(df[amount_col], errors="coerce")
            if self._looks_like_volume_series(amount_probe):
                volume_col = amount_col
                amount_col = None
                volume_from_amount = True

        tail = df.tail(1).iloc[0]
        prev_close = None
        if close_col is not None and len(df.index) > 1:
            prev_close = self._to_float(df.tail(2).iloc[0][close_col])

        current_close = self._to_float(tail[close_col]) if close_col else None
        current_open = self._to_float(tail[open_col]) if open_col else None
        current_high = self._to_float(tail[high_col]) if high_col else None
        current_low = self._to_float(tail[low_col]) if low_col else None
        current_volume = self._to_float(tail[volume_col]) if volume_col else None
        current_amount = self._to_float(tail[amount_col]) if amount_col else None
        if (
            current_amount is None
            and volume_from_amount
            and current_volume is not None
            and current_close not in (None, 0)
        ):
            current_amount = float(current_volume) * float(current_close) * 100.0

        delta = None
        pct = None
        if current_close is not None and prev_close not in (None, 0):
            delta = current_close - float(prev_close)
            pct = delta / float(prev_close) * 100

        self.market_price_label.setText("--" if current_close is None else f"{current_close:.2f}")
        if delta is None or pct is None:
            self.market_change_label.setText("--")
            self.market_change_label.setStyleSheet("color:#c8cedc;")
            self.market_price_label.setStyleSheet("color:#c8cedc;")
        else:
            sign = "+" if delta >= 0 else ""
            self.market_change_label.setText(f"{sign}{delta:.2f}  {sign}{pct:.2f}%")
            color = self._signed_color(delta)
            self.market_change_label.setStyleSheet(f"color:{color};")
            self.market_price_label.setStyleSheet(f"color:{color};")

        closes = pd.to_numeric(df[close_col], errors="coerce").dropna() if close_col else pd.Series(dtype=float)
        closes = closes[closes > 0]
        highs = pd.to_numeric(df[high_col], errors="coerce").dropna() if high_col else pd.Series(dtype=float)
        highs = highs[highs > 0]
        lows = pd.to_numeric(df[low_col], errors="coerce").dropna() if low_col else pd.Series(dtype=float)
        lows = lows[lows > 0]
        interval_bars = max(40, min(240, int(getattr(self.market_chart, "_default_visible_bars", 60) or 60)))
        interval_df = df.tail(interval_bars)
        interval_closes = (
            pd.to_numeric(interval_df[close_col], errors="coerce").dropna()
            if close_col
            else pd.Series(dtype=float)
        )
        interval_closes = interval_closes[interval_closes > 0]
        interval_highs = (
            pd.to_numeric(interval_df[high_col], errors="coerce").dropna()
            if high_col
            else pd.Series(dtype=float)
        )
        interval_highs = interval_highs[interval_highs > 0]
        interval_lows = (
            pd.to_numeric(interval_df[low_col], errors="coerce").dropna()
            if low_col
            else pd.Series(dtype=float)
        )
        interval_lows = interval_lows[interval_lows > 0]
        volumes = pd.to_numeric(df[volume_col], errors="coerce").dropna() if volume_col else pd.Series(dtype=float)
        volumes = volumes[volumes >= 0]
        close_series = pd.to_numeric(df[close_col], errors="coerce") if close_col else pd.Series(dtype=float)
        close_series = close_series.where(close_series > 0)
        period_delta = None
        period_high = None
        period_low = None
        period_amp = None
        pct_5 = None
        pct_20 = None
        pct_ytd = None
        vol_20 = None
        avg_vol_5 = None
        avg_vol_20 = None
        trade_days = len(closes.index) if not closes.empty else len(df.index)

        if not closes.empty:
            first_close = float(closes.iloc[0])
            last_close = float(closes.iloc[-1])
            period_delta = last_close - first_close
            if len(closes.index) >= 6:
                base_5 = float(closes.iloc[-6])
                if base_5 != 0:
                    pct_5 = (last_close - base_5) / base_5 * 100
            if len(closes.index) >= 21:
                base_20 = float(closes.iloc[-21])
                if base_20 != 0:
                    pct_20 = (last_close - base_20) / base_20 * 100
        if not interval_highs.empty:
            period_high = float(interval_highs.max())
        if not interval_lows.empty:
            period_low = float(interval_lows.min())
        if period_high is not None and period_low not in (None, 0):
            period_amp = (period_high - float(period_low)) / float(period_low) * 100
        if len(interval_closes.index) >= 2:
            period_delta = float(interval_closes.iloc[-1] - interval_closes.iloc[0])
        if not volumes.empty:
            avg_vol_5 = float(volumes.tail(5).mean())
            avg_vol_20 = float(volumes.tail(20).mean())
        amounts = pd.to_numeric(df[amount_col], errors="coerce").dropna() if amount_col else pd.Series(dtype=float)
        amounts = amounts[amounts >= 0]
        avg_amount_20 = float(amounts.tail(20).mean()) if not amounts.empty else None
        if avg_amount_20 is None and volume_from_amount and avg_vol_20 is not None and current_close not in (None, 0):
            avg_amount_20 = float(avg_vol_20) * float(current_close) * 100.0
        volume_bias = None
        if current_volume is not None and avg_vol_20 not in (None, 0):
            volume_bias = float(current_volume) - float(avg_vol_20)
        amount_bias = None
        if current_amount is not None and avg_amount_20 not in (None, 0):
            amount_bias = float(current_amount) - float(avg_amount_20)
        avg_vol_bias = None
        if avg_vol_5 is not None and avg_vol_20 not in (None, 0):
            avg_vol_bias = float(avg_vol_5) - float(avg_vol_20)
        if close_series is not None and not close_series.empty:
            returns = close_series.pct_change().dropna()
            if len(returns.index) >= 20:
                vol_20 = float(returns.tail(20).std() * 100)
        if date_col is not None and close_col is not None:
            dates = pd.to_datetime(df[date_col], errors="coerce")
            if dates.notna().any() and not close_series.empty:
                last_date = dates.dropna().iloc[-1]
                year_mask = dates.dt.year == int(last_date.year)
                year_closes = close_series[year_mask].dropna()
                if len(year_closes.index) >= 2:
                    start_year = float(year_closes.iloc[0])
                    end_year = float(year_closes.iloc[-1])
                    if start_year != 0:
                        pct_ytd = (end_year - start_year) / start_year * 100

        values = {
            "昨收": self._fmt_price(prev_close),
            "开盘": self._fmt_price(current_open),
            "最高": self._fmt_price(current_high),
            "最低": self._fmt_price(current_low),
            "收盘": self._fmt_price(current_close),
            "涨跌": self._fmt_signed_price(delta),
            "涨跌幅": self._fmt_percent(pct),
            "成交量": self._fmt_large_num(current_volume),
            "成交额": self._fmt_large_num(current_amount),
            "5日均量": self._fmt_large_num(avg_vol_5),
            "20日均量": self._fmt_large_num(avg_vol_20),
            "近5日涨跌": self._fmt_percent(pct_5),
            "近20日涨跌": self._fmt_percent(pct_20),
            "年内涨跌": self._fmt_percent(pct_ytd),
            "20日波动": self._fmt_percent(vol_20),
            "区间涨跌": self._fmt_signed_price(period_delta),
            "区间振幅": self._fmt_percent(period_amp),
            "区间最高": self._fmt_price(period_high),
            "区间最低": self._fmt_price(period_low),
            "交易天数": str(trade_days),
        }
        signed_fields = {
            "涨跌": delta,
            "涨跌幅": pct,
            "近5日涨跌": pct_5,
            "近20日涨跌": pct_20,
            "年内涨跌": pct_ytd,
            "区间涨跌": period_delta,
            "区间振幅": period_amp,
            "20日波动": vol_20,
            "成交量": volume_bias,
            "成交额": amount_bias,
            "5日均量": avg_vol_bias,
        }
        relative_price_fields = {
            "开盘": current_open,
            "最高": current_high,
            "最低": current_low,
            "收盘": current_close,
        }
        for key, value in values.items():
            label = self._market_stats.get(key)
            if label is not None:
                label.setText(value)
                signed_value = signed_fields.get(key)
                if signed_value is None and key in relative_price_fields and prev_close not in (None, 0):
                    current_value = relative_price_fields.get(key)
                    if current_value is not None:
                        signed_value = float(current_value) - float(prev_close)
                if signed_value is None:
                    label.setStyleSheet("")
                else:
                    label.setStyleSheet(f"color:{self._signed_color(signed_value)};")

    def _update_market_extensions(self, df: pd.DataFrame | None = None) -> None:
        dataset = self.market_dataset_combo.currentData()
        if dataset != "stock_daily":
            self._update_market_index_panels(df if isinstance(df, pd.DataFrame) else pd.DataFrame())
            return

        code = self._market_symbol.strip().lower().replace("sh", "").replace("sz", "").replace("bj", "")
        if not code:
            self._init_market_detail_placeholders()
            return
        payload = self._get_market_ext_cache(code)
        if payload is not None:
            self._apply_market_extension_payload(payload)
            self._append_log("扩展面板: 命中当日缓存")
            return
        self._append_log("扩展面板: 拉取当日快照")
        self._request_market_extensions_async(code)

    def _get_market_ext_cache(self, code: str) -> dict[str, object] | None:
        cached = self._market_ext_cache.get(code)
        if not cached:
            return None
        day_key = str(cached.get("day_key", ""))
        if day_key != self._market_ext_day_key():
            return None
        payload = cached.get("payload")
        return payload if isinstance(payload, dict) else None

    def _request_market_extensions_async(self, code: str) -> None:
        if not code or code in self._market_ext_loading_codes:
            return
        self._market_ext_loading_codes.add(code)
        threading.Thread(
            target=self._load_market_extensions_async,
            args=(code,),
            daemon=True,
        ).start()

    def _load_market_extensions_async(self, code: str) -> None:
        errors: list[str] = []
        flow_values, flow_error = self._fetch_market_flow_values(code)
        if flow_error:
            errors.append(flow_error)
        related_boards, board_error = self._fetch_related_board_groups(code)
        if board_error:
            errors.append(board_error)
        payload = {
            "flow_values": flow_values,
            "related_boards": related_boards,
            "errors": errors,
        }
        self.market_extension_loaded.emit(code, payload)

    def _on_market_extension_loaded(self, code: str, payload_obj: object) -> None:
        self._market_ext_loading_codes.discard(code)
        payload = payload_obj if isinstance(payload_obj, dict) else {}
        self._market_ext_cache[code] = {
            "ts": time.monotonic(),
            "day_key": self._market_ext_day_key(),
            "payload": payload,
        }
        for error in payload.get("errors", []):
            text = str(error).strip()
            if text:
                self._append_log(text)
        current_code = self._normalize_stock_code(self._market_symbol)
        dataset = self.market_dataset_combo.currentData()
        if dataset == "stock_daily" and current_code == code:
            self._apply_market_extension_payload(payload)

    @staticmethod
    def _market_ext_day_key() -> str:
        return QDate.currentDate().toString("yyyy-MM-dd")

    def _apply_market_extension_payload(self, payload: dict[str, object]) -> None:
        flow_values_obj = payload.get("flow_values", {})
        flow_values = flow_values_obj if isinstance(flow_values_obj, dict) else {}
        if hasattr(self, "flow_pie"):
            self.flow_pie.set_flow_values(flow_values)

        for key, label in self._flow_labels.items():
            label.setText("--")
            label.setStyleSheet("")
            item = flow_values.get(key)
            if not isinstance(item, dict):
                continue
            text = str(item.get("text", "--"))
            label.setText(text)
            value = item.get("value")
            try:
                value_num = float(value) if value is not None else None
            except Exception:
                value_num = None
            if value_num is not None:
                label.setStyleSheet(f"color:{self._signed_color(value_num)};")

        related_obj = payload.get("related_boards", {})
        related_boards = related_obj if isinstance(related_obj, dict) else {}
        self._apply_related_board_groups(related_boards)

    def _fetch_market_flow_values(self, symbol: str) -> tuple[dict[str, dict[str, object]], str | None]:
        market = self._market_prefix(symbol)
        flow_values: dict[str, dict[str, object]] = {}
        try:
            df = ak.stock_individual_fund_flow(stock=symbol, market=market)
            if df is None or df.empty:
                return flow_values, None
            row = df.tail(1).iloc[0]
            mapping = {
                "主力净流入": "主力净流入-净额",
                "主力净占比": "主力净流入-净占比",
                "超大单净流入": "超大单净流入-净额",
                "大单净流入": "大单净流入-净额",
                "中单净流入": "中单净流入-净额",
                "小单净流入": "小单净流入-净额",
            }
            for key, col in mapping.items():
                if col not in df.columns:
                    continue
                value = self._to_float(row[col])
                if key.endswith("占比"):
                    text = self._fmt_percent(value)
                else:
                    text = self._fmt_signed_amount(value)
                flow_values[key] = {"text": text, "value": value}
            return flow_values, None
        except Exception as exc:
            return flow_values, f"资金流向更新失败: {exc}"

    def _apply_related_board_groups(self, groups: dict[str, object]) -> None:
        if not hasattr(self, "_related_board_labels"):
            return
        for key, label in self._related_board_labels.items():
            label.setText("--")
            label.setToolTip("")
            label.setStyleSheet("")
            items_obj = groups.get(key, []) if isinstance(groups, dict) else []
            items = items_obj if isinstance(items_obj, list) else []
            html_parts: list[str] = []
            tips: list[str] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "")).strip()
                if not name:
                    continue
                full_name = str(item.get("full_name", name)).strip() or name
                pct_raw = item.get("pct")
                pct_value = self._to_float(pct_raw) if pct_raw is not None else None
                if pct_value is None:
                    html_parts.append(f"<span style='color:#d7dcea;'>{html.escape(name)}</span>")
                    tips.append(full_name)
                else:
                    pct_text = self._fmt_percent(pct_value)
                    color = self._signed_color(pct_value)
                    html_parts.append(
                        f"<span style='color:#52a2ff;'>{html.escape(name)}</span>"
                        f"<span style='color:{color};'>({pct_text})</span>"
                    )
                    tips.append(f"{full_name} {pct_text}")
            if html_parts:
                label.setText("  ".join(html_parts))
                label.setToolTip("\n".join(tips))
            else:
                label.setText("--")

    def _fetch_related_board_groups(self, symbol: str) -> tuple[dict[str, list[dict[str, object]]], str | None]:
        groups: dict[str, list[dict[str, object]]] = {
            "行业板块": [],
            "地区板块": [],
            "概念板块": [],
            "风格板块": [],
        }
        code = self._normalize_stock_code(symbol)
        if not code:
            return groups, None

        errors: list[str] = []
        profile = self._fetch_stock_profile_row(code)
        if profile is None:
            errors.append("公司资料接口不可用")

        industry = ""
        if profile:
            for k in ("所属行业", "行业"):
                value = str(profile.get(k, "")).strip()
                if value and value not in {"--", "nan", "None"}:
                    industry = value
                    break
        if not industry:
            industry = self._stock_sector_cache.get(code, "")
        if not industry:
            industry = self._query_stock_sector_name(code)

        business_text = ""
        if profile:
            business_text = f"{profile.get('主营业务', '')} {profile.get('经营范围', '')}".strip()
        extra_business = self._fetch_stock_business_text(code)
        if extra_business:
            business_text = f"{business_text} {extra_business}".strip()

        if industry:
            board_map = self._load_board_change_map()
            pct = self._lookup_board_change(industry, board_map)
            groups["行业板块"].append(
                {
                    "name": self._compact_sector_name(industry, max_len=10),
                    "full_name": industry,
                    "pct": pct,
                }
            )

        region = ""
        if profile:
            region = self._extract_region_keyword(str(profile.get("注册地址", "")).strip())
            if not region:
                region = self._extract_region_keyword(str(profile.get("办公地址", "")).strip())
        if region:
            groups["地区板块"].append({"name": f"{region}板块", "full_name": f"{region}板块", "pct": None})

        groups["风格板块"].append({"name": self._infer_market_style(code), "full_name": self._infer_market_style(code), "pct": None})

        concept_map = self._load_concept_change_map()
        concept_items: list[dict[str, object]] = []
        seen_concepts: set[str] = set()

        def _append_concept(name: str, pct_value: float | None) -> None:
            clean = str(name or "").strip()
            if not clean or clean in seen_concepts:
                return
            seen_concepts.add(clean)
            concept_items.append({"name": clean, "full_name": clean, "pct": pct_value})

        direct_concepts = self._query_stock_concepts_from_individual_info(code)
        for concept_name in direct_concepts:
            concept_pct = self._lookup_board_change(concept_name, concept_map) if concept_map else None
            _append_concept(concept_name, concept_pct)

        inferred_concepts = self._infer_concepts_from_text(f"{industry} {business_text}".strip())
        for concept_name in inferred_concepts:
            concept_pct = self._lookup_board_change(concept_name, concept_map) if concept_map else None
            _append_concept(concept_name, concept_pct)

        if concept_map and len(concept_items) < 4:
            company_name = str((profile or {}).get("公司名称", "")).strip()
            concept_matches = self._match_related_concepts(
                concept_map=concept_map,
                industry=industry,
                company_name=company_name,
                business_text=business_text,
                limit=6,
            )
            for concept_name, pct in concept_matches:
                _append_concept(concept_name, pct)
                if len(concept_items) >= 4:
                    break

        groups["概念板块"] = concept_items[:4]

        has_any = any(bool(v) for v in groups.values())
        if not has_any and errors:
            return groups, f"关联板块更新失败: {'; '.join(errors)}"
        return groups, None

    @staticmethod
    def _extract_region_keyword(address: str) -> str:
        text = str(address or "").strip()
        if not text:
            return ""
        direct_cities = ("北京", "上海", "天津", "重庆")
        for city in direct_cities:
            if city in text:
                return city
        regions = ("内蒙古", "广西", "西藏", "宁夏", "新疆", "香港", "澳门")
        for region in regions:
            if region in text:
                return region
        m = re.search(r"([\u4e00-\u9fa5]{2,3})(?:省|市|自治区|特别行政区)", text)
        if m:
            return m.group(1)
        return ""

    def _fetch_stock_profile_row(self, code: str) -> dict[str, object] | None:
        for _ in range(2):
            try:
                df = ak.stock_profile_cninfo(symbol=code)
                if df is None or df.empty:
                    continue
                row = df.tail(1).iloc[0]
                return {str(col): row[col] for col in df.columns}
            except Exception:
                time.sleep(0.2)
                continue
        return None

    def _fetch_stock_business_text(self, code: str) -> str:
        for _ in range(2):
            try:
                df = ak.stock_zyjs_ths(symbol=code)
                if df is None or df.empty:
                    continue
                row = df.tail(1).iloc[0]
                parts: list[str] = []
                for key in ("主营业务", "产品类型", "产品名称", "经营范围"):
                    value = str(row.get(key, "")).strip()
                    if value and value not in {"--", "nan", "None"}:
                        parts.append(value)
                text = " ".join(parts).strip()
                if text:
                    return text
            except Exception:
                time.sleep(0.2)
                continue
        return ""

    def _query_stock_concepts_from_individual_info(self, code: str) -> list[str]:
        concepts: list[str] = []
        for _ in range(2):
            try:
                df = ak.stock_individual_info_em(symbol=code)
                if df is None or df.empty:
                    continue
                item_col = self._pick_column(df, ("item", "项目", "指标"))
                value_col = self._pick_column(df, ("value", "值", "内容"))
                if item_col and value_col:
                    for _, row in df.iterrows():
                        item_text = str(row[item_col]).strip()
                        if not item_text:
                            continue
                        if ("概念" in item_text) or ("题材" in item_text):
                            value_text = str(row[value_col]).strip()
                            concepts.extend(self._split_multi_tags(value_text))
                break
            except Exception:
                time.sleep(0.2)
                continue
        dedup: list[str] = []
        seen: set[str] = set()
        for concept in concepts:
            name = concept.strip()
            if not name or name in {"--", "nan", "None"}:
                continue
            if len(name) > 24:
                continue
            if name in seen:
                continue
            seen.add(name)
            dedup.append(name)
            if len(dedup) >= 6:
                break
        return dedup

    @staticmethod
    def _split_multi_tags(text: str) -> list[str]:
        raw = str(text or "").strip()
        if not raw:
            return []
        parts = re.split(r"[、,，;；/|\\s]+", raw)
        out: list[str] = []
        for part in parts:
            name = part.strip()
            if not name:
                continue
            out.append(name)
        return out

    @staticmethod
    def _infer_concepts_from_text(text: str) -> list[str]:
        corpus = str(text or "")
        mapping = (
            ("黄金", "黄金概念"),
            ("白银", "白银概念"),
            ("有色", "有色金属"),
            ("铜", "铜缆高速连接"),
            ("镍", "镍金属"),
            ("锂", "锂电池"),
            ("稀土", "稀土永磁"),
            ("并购", "并购重组"),
            ("重组", "并购重组"),
            ("金融", "参股金融"),
            ("周期", "周期股"),
        )
        out: list[str] = []
        seen: set[str] = set()
        for kw, concept in mapping:
            if kw in corpus and concept not in seen:
                seen.add(concept)
                out.append(concept)
            if len(out) >= 4:
                break
        return out

    def _load_concept_change_map(self) -> dict[str, float]:
        today = QDate.currentDate().toString("yyyy-MM-dd")
        if self._concept_change_cache_date == today and self._concept_change_cache:
            return self._concept_change_cache

        concept_changes: dict[str, float] = {}
        try:
            df = ak.stock_board_concept_name_em()
            if df is not None and not df.empty:
                name_col = self._pick_column(df, ("板块名称", "名称"))
                pct_col = self._pick_column(df, ("涨跌幅", "涨跌幅%"))
                if name_col and pct_col:
                    for _, row in df.iterrows():
                        name = str(row[name_col]).strip()
                        pct = self._to_float(row[pct_col])
                        if name and pct is not None:
                            concept_changes[name] = pct
        except Exception:
            concept_changes = {}

        self._concept_change_cache_date = today
        self._concept_change_cache = concept_changes
        return concept_changes

    @staticmethod
    def _match_related_concepts(
        *,
        concept_map: dict[str, float],
        industry: str,
        company_name: str,
        business_text: str,
        limit: int = 4,
    ) -> list[tuple[str, float]]:
        if not concept_map:
            return []
        corpus = f"{industry} {company_name} {business_text}".strip()
        if not corpus:
            return []

        seeds = (
            "黄金",
            "白银",
            "有色",
            "铜",
            "镍",
            "锂",
            "稀土",
            "能源",
            "储能",
            "军工",
            "半导体",
            "人工智能",
            "并购重组",
            "周期",
            "金融",
        )
        keywords = {w for w in seeds if w in corpus}
        for token in re.findall(r"[\u4e00-\u9fa5]{2,6}", industry):
            if token not in {"有限", "公司", "股份", "资源"}:
                keywords.add(token)

        ranked = sorted(concept_map.items(), key=lambda item: abs(float(item[1])), reverse=True)
        matched: list[tuple[str, float]] = []
        seen: set[str] = set()
        for name, pct in ranked:
            clean = str(name).strip()
            if not clean or clean in seen:
                continue
            if clean in corpus:
                matched.append((clean, float(pct)))
                seen.add(clean)
            else:
                for kw in keywords:
                    if kw and kw in clean:
                        matched.append((clean, float(pct)))
                        seen.add(clean)
                        break
            if len(matched) >= limit:
                break
        return matched

    @staticmethod
    def _infer_market_style(code: str) -> str:
        code_text = str(code or "").strip()
        if code_text.startswith(("688", "689")):
            return "科创板"
        if code_text.startswith(("300", "301")):
            return "创业板"
        if code_text.startswith(("8", "4")):
            return "北交所"
        if code_text.startswith(("6", "5", "9")):
            return "沪市主板"
        return "深市主板"

    def _update_market_index_panels(self, df: pd.DataFrame) -> None:
        if hasattr(self, "flow_pie"):
            self.flow_pie.set_flow_values({})
        if df is None or df.empty:
            self._init_market_detail_placeholders()
            for label in self._flow_labels.values():
                label.setText("--")
            return

        close_col = self._pick_column(df, ("收盘", "收盘价", "close", "latest"))
        volume_col = self._pick_column(df, ("成交量", "volume", "vol"))
        closes = pd.to_numeric(df[close_col], errors="coerce").dropna() if close_col else pd.Series(dtype=float)
        closes = closes[closes > 0]
        volumes = pd.to_numeric(df[volume_col], errors="coerce").dropna() if volume_col else pd.Series(dtype=float)
        volumes = volumes[volumes >= 0]
        returns = closes.pct_change().dropna()
        pct_5 = None
        pct_20 = None
        if len(closes.index) >= 6:
            base_5 = float(closes.iloc[-6])
            pct_5 = ((float(closes.iloc[-1]) - base_5) / base_5 * 100) if base_5 != 0 else None
        if len(closes.index) >= 21:
            base_20 = float(closes.iloc[-21])
            pct_20 = ((float(closes.iloc[-1]) - base_20) / base_20 * 100) if base_20 != 0 else None

        values = {
            "主力净流入": "指数模式",
            "主力净占比": self._fmt_percent(pct_5),
            "超大单净流入": self._fmt_percent(pct_20),
            "大单净流入": self._fmt_percent(float(returns.tail(20).std() * 100) if len(returns.index) >= 20 else None),
            "中单净流入": self._fmt_large_num(float(volumes.tail(5).mean()) if not volumes.empty else None),
            "小单净流入": self._fmt_large_num(float(volumes.tail(20).mean()) if not volumes.empty else None),
        }
        for key, label in self._flow_labels.items():
            label.setStyleSheet("")
            label.setText(values.get(key, "--"))
        self._apply_related_board_groups(
            {
                "行业板块": [{"name": "指数模式", "pct": None}],
                "地区板块": [],
                "概念板块": [],
                "风格板块": [{"name": "宽基指数", "pct": None}],
            }
        )

    @staticmethod
    def _signed_color(value: float) -> str:
        if value > 0:
            return "#ef5350"
        if value < 0:
            return "#26a69a"
        return "#c8cedc"

    @classmethod
    def _signed_color_for_text(cls, text: str) -> str | None:
        t = text.strip()
        if not t or t == "--":
            return None
        normalized = t.replace("＋", "+").replace("－", "-").replace("−", "-")
        if normalized.startswith("+"):
            return cls._signed_color(1.0)
        if normalized.startswith("-"):
            return cls._signed_color(-1.0)
        if "买盘" in normalized or "主动买" in normalized or normalized == "买":
            return cls._signed_color(1.0)
        if "卖盘" in normalized or "主动卖" in normalized or normalized == "卖":
            return cls._signed_color(-1.0)
        if normalized.endswith("%"):
            try:
                value = float(normalized[:-1].replace(",", ""))
                return cls._signed_color(value)
            except Exception:
                return None
        return None

    @staticmethod
    def _fmt_dynamic(value: object) -> str:
        try:
            f = float(str(value).replace(",", ""))
            if abs(f) >= 1000:
                return f"{f:.0f}"
            if abs(f) >= 100:
                return f"{f:.2f}"
            return f"{f:.3f}"
        except Exception:
            return "--"

    @staticmethod
    def _fmt_volume_lot(value: object) -> str:
        try:
            f = float(str(value).replace(",", ""))
            if abs(f) >= 100000000:
                return f"{f / 100000000:.2f}亿"
            if abs(f) >= 10000:
                return f"{f / 10000:.2f}万"
            return f"{f:.0f}"
        except Exception:
            return "--"

    @staticmethod
    def _fmt_signed_amount(value: float | None) -> str:
        if value is None:
            return "--"
        sign = "+" if value >= 0 else ""
        abs_val = abs(value)
        if abs_val >= 100000000:
            return f"{sign}{value / 100000000:.2f}亿"
        if abs_val >= 10000:
            return f"{sign}{value / 10000:.2f}万"
        return f"{sign}{value:.0f}"

    @staticmethod
    def _looks_like_volume_series(series: pd.Series) -> bool:
        clean = pd.to_numeric(series, errors="coerce").dropna()
        if clean.empty:
            return False
        med = abs(float(clean.median()))
        p95 = abs(float(clean.quantile(0.95)))
        # Daily amount(成交额) is usually much larger than volume(手).
        return med <= 2e7 and p95 <= 2e8

    @staticmethod
    def _normalize_stock_symbol_for_tick(symbol: str) -> str:
        raw = symbol.strip().lower()
        if raw.startswith(("sh", "sz", "bj")):
            return raw
        if raw.startswith(("6", "5", "9")):
            return f"sh{raw}"
        if raw.startswith(("8", "4")):
            return f"bj{raw}"
        return f"sz{raw}"

    @staticmethod
    def _market_prefix(symbol: str) -> str:
        raw = symbol.strip().lower()
        if raw.startswith(("sh", "6", "5", "9")):
            return "sh"
        if raw.startswith(("bj", "8", "4")):
            return "bj"
        return "sz"

    @staticmethod
    def _period_label_text(period: str) -> str:
        mapping = {
            "1": "分时",
            "5": "5分",
            "15": "15分",
            "30": "30分",
            "60": "60分",
            "daily": "日线",
            "weekly": "周线",
            "monthly": "月线",
        }
        return mapping.get(period, "日线")

    @staticmethod
    def _minute_window_days(period: str) -> int:
        mapping = {
            "1": 10,
            "5": 45,
            "15": 120,
            "30": 240,
            "60": 365,
        }
        return int(mapping.get(period, 90))

    def _refresh_market_identity(self) -> None:
        dataset = self.market_dataset_combo.currentData()
        dataset_value = dataset if isinstance(dataset, str) else "index_daily"
        name = self._resolve_market_name(self._market_symbol, dataset_value)
        self.market_name_label.setText(name)
        self.market_symbol_label.setText(self._market_symbol)
        if dataset_value == "stock_daily":
            self.market_sector_label.setVisible(True)
            self.market_sector_label.setText("板块 --")
            self.market_sector_label.setStyleSheet("color:#98a2b4;")
        else:
            self.market_sector_label.setVisible(False)
            self.market_sector_label.setText("")
            self.market_sector_label.setStyleSheet("")

    def _refresh_market_sector_badge(self) -> None:
        dataset = self.market_dataset_combo.currentData()
        dataset_value = dataset if isinstance(dataset, str) else "index_daily"
        if dataset_value != "stock_daily":
            self.market_sector_label.setVisible(False)
            self.market_sector_label.setText("")
            self.market_sector_label.setStyleSheet("")
            return

        code = self._normalize_stock_code(self._market_symbol)
        if not code:
            self.market_sector_label.setVisible(True)
            self.market_sector_label.setText("板块 --")
            self.market_sector_label.setStyleSheet("color:#98a2b4;")
            return

        sector_name = self._stock_sector_cache.get(code)
        if sector_name is None:
            self.market_sector_label.setVisible(True)
            self.market_sector_label.setText("板块 加载中…")
            self.market_sector_label.setStyleSheet("color:#9aa3b7;")
            self._request_sector_snapshot_async(code)
            return
        if not sector_name:
            self.market_sector_label.setVisible(True)
            self.market_sector_label.setText("板块 --")
            self.market_sector_label.setStyleSheet("color:#98a2b4;")
            return

        board_changes = (
            self._board_change_cache
            if self._board_change_cache_date == QDate.currentDate().toString("yyyy-MM-dd")
            else {}
        )
        sector_pct = self._lookup_board_change(sector_name, board_changes)
        sector_display = self._compact_sector_name(sector_name)
        self.market_sector_label.setToolTip(sector_name)
        if sector_pct is None:
            self.market_sector_label.setText(sector_display)
            self.market_sector_label.setStyleSheet("color:#b8c0d0;")
        else:
            self.market_sector_label.setText(f"{sector_display} {self._fmt_percent(sector_pct)}")
            self.market_sector_label.setStyleSheet(f"color:{self._signed_color(sector_pct)};")
        self.market_sector_label.setVisible(True)

    def _request_sector_snapshot_async(self, code: str) -> None:
        if not code or code in self._sector_loading_codes:
            return
        self._sector_loading_codes.add(code)
        threading.Thread(target=self._load_sector_snapshot_async, args=(code,), daemon=True).start()

    def _load_sector_snapshot_async(self, code: str) -> None:
        sector_name = self._query_stock_sector_name(code)
        self.sector_data_loaded.emit(code, sector_name, {})
        if not sector_name:
            return
        today = QDate.currentDate().toString("yyyy-MM-dd")
        board_map: dict[str, float] = {}
        board_map = (
            self._board_change_cache
            if self._board_change_cache_date == today and self._board_change_cache
            else self._fetch_board_change_map_remote()
        )
        if board_map:
            self.sector_data_loaded.emit(code, sector_name, board_map)

    def _on_sector_data_loaded(self, code: str, sector_name_obj: object, board_map_obj: object) -> None:
        self._sector_loading_codes.discard(code)
        sector_name = str(sector_name_obj).strip() if sector_name_obj is not None else ""
        self._stock_sector_cache[code] = sector_name

        board_map = board_map_obj if isinstance(board_map_obj, dict) else {}
        if board_map:
            self._board_change_cache_date = QDate.currentDate().toString("yyyy-MM-dd")
            self._board_change_cache = dict(board_map)

        if self._normalize_stock_code(self._market_symbol) == code:
            self._refresh_market_sector_badge()

    def _query_stock_sector_name(self, code: str) -> str:
        for _ in range(2):
            try:
                df = ak.stock_individual_info_em(symbol=code)
                if df is None or df.empty:
                    continue
                item_col = self._pick_column(df, ("item", "项目", "指标"))
                value_col = self._pick_column(df, ("value", "值", "内容"))
                if item_col and value_col:
                    for _, row in df.iterrows():
                        item = str(row[item_col]).strip()
                        if "行业" in item:
                            value = str(row[value_col]).strip()
                            if value not in {"", "--", "nan", "None"}:
                                return value
            except Exception:
                continue
            time.sleep(0.18)
        cninfo_name = self._query_stock_sector_name_from_cninfo(code)
        if cninfo_name:
            return cninfo_name
        return self._query_stock_sector_name_from_spot(code)

    def _query_stock_sector_name_from_spot(self, code: str) -> str:
        today = QDate.currentDate().toString("yyyy-MM-dd")
        if self._stock_sector_spot_cache_date != today or not self._stock_sector_spot_cache:
            try:
                df = ak.stock_zh_a_spot_em()
                mapping: dict[str, str] = {}
                if df is not None and not df.empty:
                    code_col = self._pick_column(df, ("代码", "code", "symbol"))
                    industry_col = self._pick_column(df, ("所处行业", "所属行业", "行业", "板块"))
                    if code_col and industry_col:
                        for _, row in df.iterrows():
                            c = str(row[code_col]).strip()
                            v = str(row[industry_col]).strip()
                            if c and v and v not in {"--", "nan", "None"}:
                                mapping[c] = v
                self._stock_sector_spot_cache = mapping
                self._stock_sector_spot_cache_date = today
            except Exception:
                return ""
        return self._stock_sector_spot_cache.get(code, "")

    def _query_stock_sector_name_from_cninfo(self, code: str) -> str:
        end_date = QDate.currentDate().toString("yyyyMMdd")
        for _ in range(2):
            try:
                df = ak.stock_industry_change_cninfo(
                    symbol=code,
                    start_date="20180101",
                    end_date=end_date,
                )
                if df is None or df.empty:
                    continue
                date_col = self._pick_column(df, ("变更日期", "日期", "date"))
                if date_col:
                    sorted_df = df.copy()
                    sorted_df["_ts"] = pd.to_datetime(sorted_df[date_col], errors="coerce")
                    sorted_df = sorted_df.sort_values("_ts")
                    row = sorted_df.iloc[-1]
                else:
                    row = df.tail(1).iloc[0]
                for key in ("行业中类", "行业大类", "行业次类", "行业门类", "所属行业", "行业"):
                    if key not in df.columns:
                        continue
                    value = str(row[key]).strip()
                    if value and value not in {"--", "nan", "None"}:
                        return value
            except Exception:
                pass
            time.sleep(0.2)
        return ""

    def _load_board_change_map(self) -> dict[str, float]:
        today = QDate.currentDate().toString("yyyy-MM-dd")
        if self._board_change_cache_date == today and self._board_change_cache:
            return self._board_change_cache

        board_changes: dict[str, float] = {}
        try:
            df = ak.stock_board_industry_name_em()
            if df is not None and not df.empty:
                name_col = self._pick_column(df, ("板块名称", "名称"))
                pct_col = self._pick_column(df, ("涨跌幅", "涨跌幅%"))
                if name_col and pct_col:
                    for _, row in df.iterrows():
                        board_name = str(row[name_col]).strip()
                        pct_value = self._to_float(row[pct_col])
                        if board_name and pct_value is not None:
                            board_changes[board_name] = pct_value
        except Exception:
            board_changes = {}

        self._board_change_cache_date = today
        self._board_change_cache = board_changes
        return board_changes

    def _fetch_board_change_map_remote(self) -> dict[str, float]:
        board_changes: dict[str, float] = {}
        try:
            df = ak.stock_board_industry_name_em()
            if df is None or df.empty:
                return board_changes
            name_col = self._pick_column(df, ("板块名称", "名称"))
            pct_col = self._pick_column(df, ("涨跌幅", "涨跌幅%"))
            if not name_col or not pct_col:
                return board_changes
            for _, row in df.iterrows():
                board_name = str(row[name_col]).strip()
                pct_value = self._to_float(row[pct_col])
                if board_name and pct_value is not None:
                    board_changes[board_name] = pct_value
        except Exception:
            return {}
        return board_changes

    @staticmethod
    def _lookup_board_change(sector_name: str, board_changes: dict[str, float]) -> float | None:
        if not sector_name or not board_changes:
            return None
        direct = board_changes.get(sector_name)
        if direct is not None:
            return direct

        normalized = sector_name.replace(" ", "")
        for name, value in board_changes.items():
            clean = name.replace(" ", "")
            if clean == normalized:
                return value
        for name, value in board_changes.items():
            clean = name.replace(" ", "")
            if normalized in clean or clean in normalized:
                return value
        return None

    @staticmethod
    def _compact_sector_name(name: str, max_len: int = 8) -> str:
        text = str(name or "").strip()
        if not text:
            return ""
        text = re.sub(r"[、，,；;·\s]+", "", text)
        text = re.split(r"[（(/|]", text)[0]
        for old, new in (
            ("矿采选", ""),
            ("和器材", ""),
            ("和精制茶", ""),
            ("与汽车零部件", ""),
            ("货币金融服务", "金融"),
            ("资本市场服务", "资本市场"),
        ):
            text = text.replace(old, new)
        for suffix in ("制造业", "采选业", "服务业", "批发业", "零售业", "行业", "业"):
            if text.endswith(suffix):
                text = text[: -len(suffix)]
                break
        text = text.strip("-_ ")
        if not text:
            return "板块"
        if len(text) > max_len:
            return f"{text[:max_len]}…"
        return text

    @staticmethod
    def _normalize_stock_code(symbol: str) -> str:
        code = symbol.strip().lower().replace("sh", "").replace("sz", "").replace("bj", "")
        return code if code.isdigit() and len(code) == 6 else ""

    def _resolve_market_name(self, symbol: str, dataset: str) -> str:
        code = symbol.strip().lower().replace("sh", "").replace("sz", "").replace("bj", "")
        stock_name = self._symbol_code_to_name.get(code) or self._symbol_code_to_name.get(symbol, "")
        if dataset == "stock_daily":
            return stock_name or "个股"
        index_names = {
            "000001": "上证指数",
            "000016": "上证50",
            "000300": "沪深300",
            "000688": "科创50",
            "000852": "中证1000",
            "000905": "中证500",
            "399001": "深证成指",
            "399006": "创业板指",
            "399005": "中小100",
        }
        return index_names.get(code, stock_name or "指数")

    @staticmethod
    def _pick_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
        direct = {str(col): col for col in df.columns}
        lower = {str(col).lower(): col for col in df.columns}
        for name in candidates:
            if name in direct:
                return str(direct[name])
        for name in candidates:
            matched = lower.get(name.lower())
            if matched is not None:
                return str(matched)
        return None

    @staticmethod
    def _to_float(value: object) -> float | None:
        if value is None:
            return None
        try:
            if isinstance(value, (int, float)) and not pd.isna(value):
                return float(value)
        except Exception:
            pass
        try:
            text = str(value).strip()
            if not text or text.lower() in {"nan", "none", "null", "--", "-", "n/a"}:
                return None
            text = text.replace(",", "").replace("%", "")
            scale = 1.0
            if text.endswith("亿"):
                scale = 1e8
                text = text[:-1]
            elif text.endswith("万"):
                scale = 1e4
                text = text[:-1]
            elif text.endswith("千"):
                scale = 1e3
                text = text[:-1]
            return float(text) * scale
        except Exception:
            return None

    @staticmethod
    def _fmt_meta_date(value: object) -> str:
        text = str(value)
        parsed = pd.to_datetime(text, errors="coerce")
        if pd.isna(parsed):
            return text
        return parsed.strftime("%Y.%m.%d")

    @staticmethod
    def _fmt_meta_range(start_text: str, end_text: str) -> str:
        s = (start_text or "--").strip()
        e = (end_text or "--").strip()
        return f"区间 {s} · {e}"

    @staticmethod
    def _fmt_price(value: float | None) -> str:
        if value is None:
            return "--"
        return f"{value:.2f}"

    @staticmethod
    def _fmt_signed_price(value: float | None) -> str:
        if value is None:
            return "--"
        sign = "+" if value >= 0 else ""
        return f"{sign}{value:.2f}"

    @staticmethod
    def _fmt_percent(value: float | None) -> str:
        if value is None:
            return "--"
        sign = "+" if value >= 0 else ""
        return f"{sign}{value:.2f}%"

    @staticmethod
    def _fmt_large_num(value: float | None) -> str:
        if value is None:
            return "--"
        abs_val = abs(value)
        if abs_val >= 100000000:
            return f"{value / 100000000:.2f}亿"
        if abs_val >= 10000:
            return f"{value / 10000:.2f}万"
        return f"{value:.0f}"

    def _append_log(self, message: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        level = self._infer_log_level(message)
        label = {
            "success": "成功",
            "error": "错误",
            "warning": "提示",
            "info": "进行中",
            "muted": "缓存",
        }[level]
        palette = {
            "success": ("#1e2d24", "#2fa361", "#b6f2cf"),
            "error": ("#332024", "#d86174", "#ffd4dc"),
            "warning": ("#3a2e20", "#d39a4b", "#ffdfb0"),
            "info": ("#1f2a3d", "#5b9bff", "#cfe2ff"),
            "muted": ("#262a34", "#727c90", "#d3d8e5"),
        }
        bg, border, text = palette[level]
        safe_message = html.escape(message).replace("\n", "<br>")
        item_html = (
            f"<div style='margin:4px 0; padding:6px 8px; border-radius:8px; "
            f"background:{bg}; color:{text}; text-align:left;'>"
            f"<span style='font-size:11px; opacity:0.78;'>[{ts}]</span> "
            f"<span style='font-size:11px; margin-left:4px; padding:1px 5px; "
            f"border-radius:6px; background:{border}; color:#ffffff;'>{label}</span> "
            f"<span>{safe_message}</span>"
            f"</div>"
        )
        self.log_box.append(item_html)
        self.log_box.verticalScrollBar().setValue(self.log_box.verticalScrollBar().maximum())
        try:
            self.log_box.horizontalScrollBar().setValue(self.log_box.horizontalScrollBar().minimum())
        except Exception:
            pass

    def _append_runtime_env_log(self) -> None:
        try:
            python = sys.executable
        except Exception:
            python = "--"
        try:
            ak_version = str(getattr(ak, "__version__", "--"))
        except Exception:
            ak_version = "--"
        try:
            import core.data_service as data_service_module

            ds_path = str(Path(str(getattr(data_service_module, "__file__", "--"))).resolve())
        except Exception:
            ds_path = "--"
        self._append_log(f"运行环境: python={python} | akshare={ak_version}")
        self._append_log(f"代码位置: core.data_service={ds_path}")
        try:
            max_day = DataService._TRADE_DATES_MAX_DAY
            rows = len(DataService._TRADE_DATES or []) if DataService._TRADE_DATES is not None else 0
            if max_day is not None:
                self._append_log(f"核心交易日历状态: max_day={pd.Timestamp(max_day).strftime('%Y-%m-%d')} rows={rows}")
            else:
                self._append_log(f"核心交易日历状态: max_day=-- rows={rows}")
        except Exception:
            pass
        try:
            market_dir = self._cache_root / "market"
            pkl = market_dir / "trade_dates.pkl"
            pq = market_dir / "trade_dates.parquet"
            meta = market_dir / "trade_dates.meta.json"
            self._append_log(
                f"交易日历缓存: pkl={'Y' if pkl.exists() else 'N'} parquet={'Y' if pq.exists() else 'N'} meta={'Y' if meta.exists() else 'N'}"
            )
            if meta.exists():
                payload = json.loads(meta.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    src = str(payload.get("source", "--"))
                    mx = str(payload.get("ts_max", "--"))
                    rows = str(payload.get("rows", "--"))
                    self._append_log(f"交易日历缓存meta: source={src}, max={mx}, rows={rows}")
        except Exception:
            pass

    @staticmethod
    def _infer_log_level(message: str) -> str:
        if "抓取失败" in message or "失败" in message or "异常" in message:
            return "error"
        if "抓取完成" in message or "已加载" in message or "已更新" in message:
            return "success"
        if "开始抓取" in message or "正在" in message or "下载进度" in message or "更新中" in message:
            return "info"
        if "缓存文件" in message or "缓存目录" in message:
            return "muted"
        return "warning"

    def _set_fetch_progress(self, active: bool) -> None:
        self.progress_block.setVisible(active)
        if active:
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(3)
            self.progress_status.setText("准备下载")
            self.progress_percent.setText("3%")
            self._last_progress_log_text = ""
            self._last_progress_log_ts = 0.0
        else:
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(0)
            self.progress_status.setText("")
            self.progress_percent.setText("0%")
        self._update_floating_pod_visibility()

    def _render_dataframe(self, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            self.table.clear()
            self.table.setRowCount(0)
            self.table.setColumnCount(0)
            self._append_log("返回为空数据")
            return

        safe_df = self._enrich_dataframe_for_display(df).fillna("")
        max_rows = min(500, len(safe_df.index))
        safe_df = safe_df.head(max_rows)

        columns = [str(col) for col in safe_df.columns]
        display_headers = to_chinese_headers(columns)
        self.table.clear()
        self.table.setColumnCount(len(columns))
        self.table.setHorizontalHeaderLabels(display_headers)
        self.table.setRowCount(len(safe_df.index))

        for row_idx, row in enumerate(safe_df.itertuples(index=False)):
            for col_idx, value in enumerate(row):
                self.table.setItem(row_idx, col_idx, QTableWidgetItem(str(value)))

        self.table.resizeColumnsToContents()
        self._append_log(f"表格已更新，展示前 {max_rows} 行")

    def _enrich_dataframe_for_display(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        close_col = self._pick_column(out, ("收盘", "收盘价", "close", "latest"))
        if close_col is not None:
            closes = pd.to_numeric(out[close_col], errors="coerce")
            out["5日均价"] = closes.rolling(window=5, min_periods=5).mean().round(4)
            out["10日均价"] = closes.rolling(window=10, min_periods=10).mean().round(4)
            out["20日均价"] = closes.rolling(window=20, min_periods=20).mean().round(4)

        volume_col = self._pick_column(out, ("成交量", "volume", "vol"))
        amount_col = self._pick_column(out, ("成交额", "amount", "turnover"))
        volumes: pd.Series | None = None
        if volume_col is not None:
            volumes = pd.to_numeric(out[volume_col], errors="coerce")
        elif amount_col is not None:
            amount_series = pd.to_numeric(out[amount_col], errors="coerce")
            if self._looks_like_volume_series(amount_series):
                volumes = amount_series
                out["成交量(推断)"] = amount_series
        if volumes is not None:
            out["5日均量"] = volumes.rolling(window=5, min_periods=5).mean().round(2)
            out["20日均量"] = volumes.rolling(window=20, min_periods=20).mean().round(2)
        return out


def _resolve_app_icon_path() -> Path | None:
    project_root = Path(__file__).resolve().parents[2]
    candidates = (
        project_root / "img" / "icon" / "icon_512.png",
        project_root / "img" / "icon" / "icon.png",
        project_root / "img" / "icon" / "icon.jpeg",
        project_root / "imgs" / "icon.png",
        project_root / "imgs" / "icon.jpeg",
    )
    for path in candidates:
        if path.exists():
            return path
    return None


def run() -> None:
    app = QApplication([])
    icon = None
    icon_path = _resolve_app_icon_path()
    if icon_path is not None:
        qicon = QIcon(str(icon_path))
        if not qicon.isNull():
            icon = qicon
            app.setWindowIcon(qicon)
    app.setStyle("Fusion")
    apply_dark_palette(app)
    preload_error: str | None = None
    preload_thread = QThread()
    preload_worker = PreloadWorker(cache_root())
    preload_worker.moveToThread(preload_thread)
    preload_dialog = QProgressDialog("正在初始化…", None, 0, 100)
    preload_dialog.setWindowTitle("MoS Quant 初始化")
    preload_dialog.setWindowModality(Qt.ApplicationModal)
    preload_dialog.setCancelButton(None)
    preload_dialog.setAutoClose(False)
    preload_dialog.setMinimumDuration(0)
    preload_dialog.setValue(0)

    def _on_preload_progress(percent: int, message: str) -> None:
        preload_dialog.setValue(max(0, min(100, int(percent))))
        preload_dialog.setLabelText(str(message or "正在初始化…"))

    def _on_preload_error(message: str) -> None:
        nonlocal preload_error
        preload_error = str(message or "").strip() or "初始化失败"

    preload_worker.progress.connect(_on_preload_progress)
    preload_worker.error.connect(_on_preload_error)
    preload_thread.started.connect(preload_worker.run)

    loop = QEventLoop()
    preload_worker.finished.connect(loop.quit)
    preload_worker.error.connect(loop.quit)
    preload_worker.finished.connect(preload_thread.quit)
    preload_worker.finished.connect(preload_worker.deleteLater)
    preload_thread.finished.connect(preload_thread.deleteLater)

    preload_dialog.show()
    preload_thread.start()
    loop.exec()
    preload_thread.wait(5000)
    preload_dialog.close()

    if preload_error and "trade_dates:" in preload_error:
        QMessageBox.critical(None, "初始化失败", f"关键资源加载失败，已中止启动:\n{preload_error}")
        return

    window = MainWindow()
    if icon is not None:
        window.setWindowIcon(icon)
    window.show()
    QTimer.singleShot(350, window._append_runtime_env_log)
    if preload_error:
        QTimer.singleShot(
            450,
            lambda: window._append_log(f"初始化阶段部分资源加载失败，将使用缓存/后续后台重试: {preload_error}"),
        )
    app.exec()


if __name__ == "__main__":
    run()
