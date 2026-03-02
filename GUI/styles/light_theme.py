from __future__ import annotations

MAIN_WINDOW_STYLE = """
QMainWindow {
    background-color: #f5f5f7;
}
#controlPanel {
    background-color: #ffffff;
    border: 1px solid #e5e5ea;
    border-radius: 16px;
}
#resultPanel {
    background-color: #ffffff;
    border: 1px solid #e5e5ea;
    border-radius: 16px;
}
QLabel, QLineEdit, QComboBox, QDateEdit, QPushButton, QTextEdit, QTableWidget {
    font-family: "PingFang SC", "Avenir Next", "Segoe UI", sans-serif;
    font-size: 13px;
}
#panelTitle {
    color: #1d1d1f;
    font-size: 22px;
    font-weight: 700;
    letter-spacing: 0px;
}
#panelDesc {
    color: #6e6e73;
    font-size: 13px;
}
#datasetInlineLabel {
    color: #4a4a4f;
    font-size: 12px;
    font-weight: 600;
}
#hintLabel {
    color: #6e6e73;
    background-color: #f7f8fa;
    border: 1px solid #e5e5ea;
    border-radius: 10px;
    padding: 7px 9px;
    min-height: 16px;
}
#controlPanel QLabel {
    color: #3a3a3c;
}
QLabel#fieldLabel {
    color: #3a3a3c;
    font-size: 13px;
    font-weight: 600;
}
QCheckBox#refreshCacheCheck {
    color: #4a4a4f;
    spacing: 6px;
    font-size: 12px;
}
QCheckBox#refreshCacheCheck::indicator {
    width: 14px;
    height: 14px;
    border: 1px solid #c7c7cc;
    border-radius: 4px;
    background: #ffffff;
}
QCheckBox#refreshCacheCheck::indicator:checked {
    border: 1px solid #007aff;
    background: #007aff;
}
QLineEdit, QComboBox, QDateEdit {
    border: 1px solid #d2d2d7;
    border-radius: 10px;
    padding: 7px 10px;
    background-color: #fbfbfd;
    color: #1d1d1f;
    font-size: 13px;
}
QFrame#floatingPod {
    background-color: rgba(255, 255, 255, 245);
    border: 1px solid #d8dbe3;
    border-radius: 14px;
}
QLabel#floatingStatus {
    color: #4a4a4f;
    font-size: 12px;
    font-weight: 600;
}
QLineEdit#quickSymbolInput {
    border: 1px solid #0071e3;
    border-radius: 10px;
    padding: 7px 10px;
    background-color: #ffffff;
    color: #1d1d1f;
    font-size: 13px;
}
QLineEdit#quickSymbolInput:focus {
    border: 1px solid #0a84ff;
}
QListView#quickSymbolCompleterPopup {
    background-color: #ffffff;
    border: 1px solid #d2d2d7;
    border-radius: 10px;
    padding: 4px;
    color: #1d1d1f;
    outline: 0px;
}
QListView#quickSymbolCompleterPopup::item {
    min-height: 28px;
    padding: 6px 10px;
    border-radius: 8px;
}
QListView#quickSymbolCompleterPopup::item:hover {
    background-color: #eef6ff;
}
QListView#quickSymbolCompleterPopup::item:selected {
    background-color: #007aff;
    color: #ffffff;
}
QComboBox::item {
    color: #1d1d1f;
}
QLineEdit::placeholder {
    color: #8e8e93;
}
QLineEdit:focus, QComboBox:focus, QDateEdit:focus {
    border: 1px solid #0071e3;
    background-color: #ffffff;
}
QLineEdit:disabled, QComboBox:disabled, QDateEdit:disabled {
    color: #8e8e93;
    background-color: #f2f2f7;
    border: 1px solid #e5e5ea;
}
QLineEdit[inactive="true"], QDateEdit[inactive="true"] {
    color: #8e8e93;
    background-color: #f2f2f7;
    border: 1px solid #e5e5ea;
}
QLabel[muted="true"] {
    color: #8e8e93;
}
QComboBox {
    combobox-popup: 1;
    padding-right: 30px;
}
QComboBox::drop-down {
    subcontrol-origin: padding;
    subcontrol-position: top right;
    width: 28px;
    border-left: 1px solid #e5e5ea;
    border-top-right-radius: 10px;
    border-bottom-right-radius: 10px;
    background-color: #f2f2f7;
}
QComboBox::down-arrow {
    image: none;
    width: 0px;
    height: 0px;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 7px solid #6e6e73;
    margin-right: 8px;
}
QComboBox QAbstractItemView#datasetComboPopup {
    background-color: #ffffff;
    color: #1d1d1f;
    border: 1px solid #d2d2d7;
    border-radius: 10px;
    outline: 0px;
    margin: 0px;
    padding: 4px;
}
QComboBox QAbstractItemView#datasetComboPopup::viewport {
    background: transparent;
}
QComboBox QAbstractItemView#datasetComboPopup::item {
    min-height: 30px;
    padding: 6px 10px;
    margin: 1px 0px;
    border-radius: 8px;
    color: #1d1d1f;
}
QComboBox QAbstractItemView#datasetComboPopup::item:hover {
    background-color: #eaf3ff;
    color: #1d1d1f;
}
QComboBox QAbstractItemView#datasetComboPopup::item:selected {
    background-color: #007aff;
    color: #ffffff;
}
QDateEdit::drop-down {
    subcontrol-origin: padding;
    subcontrol-position: top right;
    width: 28px;
    border-left: 1px solid #e5e5ea;
    border-top-right-radius: 10px;
    border-bottom-right-radius: 10px;
    background-color: #f2f2f7;
}
QDateEdit::down-arrow {
    image: none;
    width: 0px;
    height: 0px;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 7px solid #6e6e73;
    margin-right: 8px;
}
QPushButton#fetchButton {
    border: none;
    border-radius: 10px;
    background-color: #007aff;
    color: #ffffff;
    font-size: 14px;
    font-weight: 600;
    padding: 8px 16px;
}
QPushButton#fetchButton:hover {
    background-color: #0a84ff;
}
QPushButton#fetchButton:pressed {
    background-color: #0067d8;
}
QPushButton#fetchButton:disabled {
    background-color: #c7c7cc;
    color: #f5f5f7;
}
QTextEdit {
    background-color: #f9f9fb;
    color: #1d1d1f;
    border-radius: 10px;
    border: 1px solid #e5e5ea;
    padding: 8px;
}
QProgressBar#fetchProgressBar {
    background-color: #ececf1;
    border: 1px solid #d9d9e0;
    border-radius: 4px;
}
QProgressBar#fetchProgressBar::chunk {
    background-color: #0a84ff;
    border-radius: 4px;
}
#resultTitle {
    color: #1d1d1f;
    font-size: 17px;
    font-weight: 600;
}
QCalendarWidget QWidget {
    alternate-background-color: #f7f7fa;
    background: #ffffff;
}
QCalendarWidget QWidget#qt_calendar_navigationbar {
    background-color: #f6f7fb;
    border-bottom: 1px solid #e3e6ef;
}
QCalendarWidget QToolButton {
    color: #1d1d1f;
    background-color: transparent;
    border: none;
    font-size: 13px;
    font-weight: 600;
    padding: 4px;
}
QCalendarWidget QToolButton:hover {
    background-color: #e9edf6;
    border-radius: 6px;
}
QCalendarWidget QMenu {
    background-color: #ffffff;
    color: #1d1d1f;
    border: 1px solid #d2d2d7;
}
QCalendarWidget QSpinBox {
    color: #1d1d1f;
    background: #ffffff;
    border: 1px solid #d2d7e3;
    border-radius: 6px;
    selection-background-color: #dce9ff;
    selection-color: #ffffff;
}
QCalendarWidget QTableView {
    background-color: #ffffff;
    color: #1d1d1f;
    selection-background-color: #dce9ff;
    selection-color: #1d1d1f;
    outline: 0;
    border: 1px solid #e2e5ee;
}
QCalendarWidget QTableView:disabled {
    color: #8e8e93;
}
QCalendarWidget QHeaderView::section {
    background-color: #f6f7fb;
    color: #6e6e73;
    border: none;
    padding: 4px;
    font-size: 13px;
}
QTableWidget {
    border: 1px solid #e5e5ea;
    border-radius: 10px;
    gridline-color: #eef0f2;
    background-color: #ffffff;
    alternate-background-color: #f8f9fb;
    selection-background-color: #dbeafe;
    selection-color: #1d1d1f;
}
QHeaderView::section {
    background-color: #f5f5f7;
    color: #4a4a4f;
    border: none;
    border-bottom: 1px solid #e5e5ea;
    padding: 6px;
    font-weight: 600;
}
"""
