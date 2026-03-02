from __future__ import annotations

from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication

MAIN_WINDOW_STYLE = """
QMainWindow {
    background-color: #111214;
}
#controlPanel {
    background-color: #181a1f;
    border: 1px solid #2a2d35;
    border-radius: 16px;
}
#navRail {
    background-color: #16181d;
    border: 1px solid #2b2f38;
    border-radius: 16px;
}
#resultPanel {
    background-color: #181a1f;
    border: 1px solid #2a2d35;
    border-radius: 16px;
}
QLabel, QLineEdit, QComboBox, QDateEdit, QPushButton, QTextEdit, QTableWidget {
    font-family: "PingFang SC", "Avenir Next", "Segoe UI", sans-serif;
    font-size: 13px;
    color: #e6e8ee;
}
#panelTitle {
    color: #f2f3f7;
    font-size: 22px;
    font-weight: 700;
}
#panelDesc {
    color: #9aa0ad;
    font-size: 13px;
}
#watchHint {
    color: #aab2c2;
    font-size: 12px;
}
#datasetInlineLabel {
    color: #bcc2cf;
    font-size: 12px;
    font-weight: 600;
}
#controlPanel QLabel {
    color: #d7dbe5;
}
QLabel#fieldLabel {
    color: #ced3de;
    font-size: 13px;
    font-weight: 600;
}
QPushButton#navButton {
    text-align: left;
    border: 1px solid #303541;
    border-radius: 10px;
    background-color: #20242d;
    color: #cbd1de;
    font-size: 12px;
    font-weight: 600;
    padding: 8px 10px;
}
QPushButton#navButton:hover {
    background-color: #272c37;
}
QPushButton#navButton:checked {
    border: 1px solid #4f8dfd;
    background-color: #2a3651;
    color: #e9f1ff;
}
QFrame#watchCard {
    border: 1px solid #303645;
    border-radius: 12px;
    background-color: #1b202b;
}
QTableWidget#watchListTable {
    border: none;
    border-radius: 10px;
    background-color: #182238;
    alternate-background-color: #1b2741;
    gridline-color: #293850;
    selection-background-color: #2f63a8;
    selection-color: #ffffff;
}
QTableWidget#watchListTable QHeaderView::section {
    background-color: #1a263c;
    color: #9eabc1;
    border: none;
    border-bottom: 1px solid #2a3953;
    border-right: 1px solid #273854;
    padding: 6px 8px;
    font-size: 12px;
    font-weight: 600;
}
QTableWidget#watchListTable QHeaderView {
    background-color: #1a263c;
}
QTableWidget#watchListTable QTableCornerButton::section {
    background-color: #1a263c;
    border: none;
    border-right: 1px solid #273854;
    border-bottom: 1px solid #2a3953;
}
QFrame#marketSideCard {
    background-color: #1c2029;
    border: 1px solid #303645;
    border-radius: 12px;
}
QScrollArea#marketSideScroll {
    border: none;
    background: transparent;
}
QWidget#marketSideContent {
    background: transparent;
}
QWidget#marketSideFixed {
    background-color: #1c2029;
    border-bottom: 1px solid #2d3443;
    border-top-left-radius: 12px;
    border-top-right-radius: 12px;
}
QLabel#marketSymbol {
    color: #d7dcea;
    font-size: 14px;
    font-weight: 500;
}
QLabel#marketName {
    color: #eef2fb;
    font-size: 18px;
    font-weight: 600;
}
QLabel#marketSector {
    color: #9da8bc;
    font-size: 12px;
    font-weight: 600;
}
QLabel#marketPrice {
    color: #f3f6ff;
    font-size: 30px;
    font-weight: 700;
}
QLabel#marketDelta {
    color: #c8cedc;
    font-size: 15px;
    font-weight: 600;
}
QLabel#marketStatValue {
    color: #e7ebf5;
    font-size: 13px;
    font-weight: 600;
}
QLabel#boardGroupValue {
    color: #d7dcea;
    font-size: 13px;
    font-weight: 600;
    padding-top: 1px;
}
QTabWidget#marketDetailTabs::pane {
    border: 1px solid #303748;
    border-radius: 10px;
    background-color: #191d26;
    top: -1px;
}
QWidget#flowPie {
    background-color: #171c26;
    border: 1px solid #2f3748;
    border-radius: 10px;
}
QTabBar::tab {
    background-color: #232833;
    color: #cfd6e5;
    border: 1px solid #313949;
    border-bottom: none;
    border-top-left-radius: 8px;
    border-top-right-radius: 8px;
    min-width: 72px;
    padding: 4px 8px;
    margin-right: 2px;
    font-size: 12px;
}
QTabBar::tab:selected {
    background-color: #2d3651;
    color: #eaf1ff;
    border-color: #4569a3;
}
QTableWidget#marketMiniTable {
    border: none;
    border-radius: 0px;
    background-color: #191d26;
    alternate-background-color: #1e2330;
    gridline-color: #2a2f3a;
    selection-background-color: #365f9e;
    selection-color: #ffffff;
    font-size: 12px;
}
QTableWidget#marketMiniTable QHeaderView::section {
    background-color: #202633;
    color: #c7cfdf;
    border: none;
    border-bottom: 1px solid #313746;
    padding: 4px 5px;
    font-weight: 600;
}
QCheckBox#refreshCacheCheck {
    color: #c3c8d4;
    spacing: 6px;
    font-size: 12px;
}
QCheckBox[inactive="true"] {
    color: #7f8798;
}
QCheckBox#refreshCacheCheck::indicator {
    width: 14px;
    height: 14px;
    border: 1px solid #4d5361;
    border-radius: 4px;
    background: #232631;
}
QCheckBox#refreshCacheCheck::indicator:checked {
    border: 1px solid #4f8dfd;
    background: #4f8dfd;
}
QLineEdit, QComboBox, QDateEdit {
    border: 1px solid #354055;
    border-radius: 14px;
    padding: 8px 12px;
    background-color: #202733;
    color: #eef2fa;
    font-size: 13px;
}
QFrame#floatingPod {
    background-color: rgba(21, 24, 31, 238);
    border: 1px solid #3a455a;
    border-radius: 16px;
}
QFrame#progressPod {
    background: transparent;
}
QLabel#floatingStatus {
    color: #d7deed;
    font-size: 12px;
    font-weight: 600;
}
QLabel#floatingPercent {
    color: #dce6fb;
    font-size: 12px;
    font-weight: 700;
}
QLineEdit#quickSymbolInput {
    border: 1px solid #5b81d6;
    border-radius: 12px;
    padding: 7px 11px;
    background-color: #1f2632;
    color: #f2f4fb;
    font-size: 13px;
}
QLineEdit#quickSymbolInput:focus {
    border: 1px solid #7ca8ff;
}
QListView#quickSymbolCompleterPopup {
    background-color: #1d2027;
    border: 1px solid #353b47;
    border-radius: 10px;
    padding: 4px;
    color: #e6e9f2;
    outline: 0px;
}
QListView#quickSymbolCompleterPopup::item {
    min-height: 28px;
    padding: 6px 10px;
    border-radius: 8px;
}
QListView#quickSymbolCompleterPopup::item:hover {
    background-color: #2e3543;
}
QListView#quickSymbolCompleterPopup::item:selected {
    background-color: #4f8dfd;
    color: #ffffff;
}
QLineEdit::placeholder {
    color: #7f8798;
}
QLineEdit:focus, QComboBox:focus, QDateEdit:focus {
    border: 1px solid #6f95e8;
    background-color: #242d3b;
}
QLineEdit:disabled, QComboBox:disabled, QDateEdit:disabled {
    color: #7e8596;
    background-color: #252b36;
    border: 1px solid #323745;
}
QLineEdit[inactive="true"], QDateEdit[inactive="true"] {
    color: #7e8596;
    background-color: #262933;
    border: 1px solid #323745;
}
QLabel[muted="true"] {
    color: #7f8798;
}
QComboBox {
    combobox-popup: 1;
    padding-right: 22px;
}
QComboBox::drop-down {
    subcontrol-origin: padding;
    subcontrol-position: center right;
    width: 0px;
    border: none;
    background-color: transparent;
}
QComboBox::down-arrow {
    image: none;
    width: 0px;
    height: 0px;
}
QDateEdit {
    padding-right: 26px;
}
QDateEdit::drop-down {
    subcontrol-origin: padding;
    subcontrol-position: top right;
    width: 22px;
    border: none;
    border-top-right-radius: 14px;
    border-bottom-right-radius: 14px;
    border-left: 1px solid #34405a;
    background-color: rgba(255, 255, 255, 0.01);
}
QDateEdit::drop-down:hover {
    background-color: rgba(255, 255, 255, 0.06);
}
QComboBox QAbstractItemView#datasetComboPopup {
    background-color: #1d2027;
    color: #eceff7;
    border: 1px solid #353b47;
    border-radius: 10px;
    outline: 0px;
    margin: 0px;
    padding: 4px;
}
QComboBox QAbstractItemView#datasetComboPopup::item {
    min-height: 30px;
    padding: 6px 10px;
    margin: 1px 0px;
    border-radius: 8px;
}
QComboBox QAbstractItemView#datasetComboPopup::item:hover {
    background-color: #2f3644;
}
QComboBox QAbstractItemView#datasetComboPopup::item:selected {
    background-color: #4f8dfd;
    color: #ffffff;
}
QPushButton#fetchButton {
    border: none;
    border-radius: 10px;
    background-color: #4f8dfd;
    color: #ffffff;
    font-size: 14px;
    font-weight: 600;
    padding: 8px 16px;
}
QPushButton#fetchButton:hover {
    background-color: #66a2ff;
}
QPushButton#fetchButton:pressed {
    background-color: #3f78db;
}
QPushButton#fetchButton:disabled {
    background-color: #4b5160;
    color: #aab0be;
}
QTextEdit {
    background-color: #1f232b;
    color: #e8ebf4;
    border-radius: 10px;
    border: 1px solid #2f3441;
    padding: 8px;
}
QProgressBar#fetchProgressBar {
    background-color: #2b3240;
    border: 1px solid #3f4960;
    border-radius: 6px;
    min-height: 12px;
}
QProgressBar#fetchProgressBar::chunk {
    background-color: #7da5ff;
    border-radius: 6px;
}
#resultTitle {
    color: #f2f4f9;
    font-size: 17px;
    font-weight: 600;
}
QCalendarWidget QWidget {
    alternate-background-color: #1d2128;
    background: #1b1e25;
    color: #e7eaf2;
}
QCalendarWidget QWidget#qt_calendar_navigationbar {
    background-color: #1f232c;
    border-bottom: 1px solid #313645;
}
QCalendarWidget QToolButton {
    color: #dce0ea;
    background-color: transparent;
    border: none;
    font-size: 13px;
    font-weight: 600;
    padding: 4px;
}
QCalendarWidget QToolButton:hover {
    background-color: #2a3040;
    border-radius: 6px;
}
QCalendarWidget QMenu {
    background-color: #1d2128;
    color: #e7eaf2;
    border: 1px solid #353b49;
}
QCalendarWidget QSpinBox {
    color: #e6e9f2;
    background: #222734;
    border: 1px solid #3a4050;
    border-radius: 6px;
    selection-background-color: #4f8dfd;
    selection-color: #ffffff;
}
QCalendarWidget QTableView {
    background-color: #1b1e25;
    color: #e7eaf2;
    selection-background-color: #355ea3;
    selection-color: #ffffff;
    outline: 0;
    border: 1px solid #343a49;
}
QCalendarWidget QHeaderView::section {
    background-color: #1f232c;
    color: #9fa8ba;
    border: none;
    padding: 4px;
    font-size: 13px;
}
QTableWidget {
    border: 1px solid #2f3440;
    border-radius: 10px;
    gridline-color: #2a2f3a;
    background-color: #1b1f26;
    alternate-background-color: #1f232c;
    selection-background-color: #365f9e;
    selection-color: #ffffff;
}
QTableWidget#watchResultTable, QTableWidget#analysisResultTable {
    border: 1px solid #2a3346;
    border-radius: 12px;
    gridline-color: #273248;
    background-color: #162136;
    alternate-background-color: #1a2740;
    selection-background-color: #2f5ea6;
    selection-color: #ffffff;
}
QTableWidget#watchResultTable QHeaderView::section, QTableWidget#analysisResultTable QHeaderView::section {
    background-color: #18253b;
    color: #9facc3;
    border: none;
    border-bottom: 1px solid #2a3650;
    border-right: 1px solid #25334f;
    padding: 6px 8px;
    font-weight: 600;
}
QTableWidget#analysisResultTable QHeaderView {
    background-color: #162136;
}
QTableWidget#analysisResultTable QTableCornerButton::section {
    background-color: #162136;
    border: none;
    border-bottom: 1px solid #2a3650;
}
QHeaderView::section {
    background-color: #1f232c;
    color: #c6ccda;
    border: none;
    border-bottom: 1px solid #313746;
    padding: 6px;
    font-weight: 600;
}
QScrollBar:vertical {
    background: transparent;
    width: 11px;
    margin: 2px;
}
QScrollBar::handle:vertical {
    background: #4b5160;
    border-radius: 5px;
    min-height: 30px;
}
QScrollBar::handle:vertical:hover {
    background: #5b6373;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}
QScrollBar:horizontal {
    background: transparent;
    height: 11px;
    margin: 2px;
}
QScrollBar::handle:horizontal {
    background: #4b5160;
    border-radius: 5px;
    min-width: 30px;
}
QScrollBar::handle:horizontal:hover {
    background: #5b6373;
}
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
    width: 0px;
}
"""


def apply_dark_palette(app: QApplication) -> None:
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor("#111214"))
    palette.setColor(QPalette.WindowText, QColor("#e6e8ee"))
    palette.setColor(QPalette.Base, QColor("#1b1f26"))
    palette.setColor(QPalette.AlternateBase, QColor("#1f232c"))
    palette.setColor(QPalette.ToolTipBase, QColor("#1f232c"))
    palette.setColor(QPalette.ToolTipText, QColor("#eef1f8"))
    palette.setColor(QPalette.Text, QColor("#e6e8ee"))
    palette.setColor(QPalette.Button, QColor("#232833"))
    palette.setColor(QPalette.ButtonText, QColor("#e8ebf4"))
    palette.setColor(QPalette.BrightText, QColor("#ffffff"))
    palette.setColor(QPalette.Highlight, QColor("#4f8dfd"))
    palette.setColor(QPalette.HighlightedText, QColor("#ffffff"))
    app.setPalette(palette)
