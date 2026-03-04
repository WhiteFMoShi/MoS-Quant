from __future__ import annotations

import sys
import traceback
from pathlib import Path

try:
    from PySide6 import QtCore, QtGui, QtWidgets  # type: ignore
except Exception as exc:
    raise RuntimeError(
        "PySide6 is not installed or cannot be imported. "
        "Install dependencies (requirements.txt) and retry."
    ) from exc


def _repo_root() -> Path:
    # mos_quant/ui/qt_app.py -> <root>/mos_quant/ui/qt_app.py
    return Path(__file__).resolve().parents[2]


def _apply_dark_theme(app: QtWidgets.QApplication) -> None:
    app.setStyle("Fusion")

    # Prefer Apple-like typography when available, with sensible fallbacks.
    preferred_families = [
        "PingFang SC",
        "SF Pro Text",
        "SF Pro Display",
        "Helvetica Neue",
        "Segoe UI",
        "Inter",
    ]
    try:
        families = set(QtGui.QFontDatabase.families())
        for family in preferred_families:
            if family in families:
                app.setFont(QtGui.QFont(family, 12))
                break
    except Exception:
        pass

    palette = QtGui.QPalette()
    # Slightly lifted background to avoid harsh "pure black" bands between surfaces.
    palette.setColor(QtGui.QPalette.Window, QtGui.QColor("#0F0F10"))
    palette.setColor(QtGui.QPalette.WindowText, QtGui.QColor("#F2F2F7"))
    palette.setColor(QtGui.QPalette.Base, QtGui.QColor("#141416"))
    palette.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor("#1C1C1E"))
    palette.setColor(QtGui.QPalette.ToolTipBase, QtGui.QColor("#1C1C1E"))
    palette.setColor(QtGui.QPalette.ToolTipText, QtGui.QColor("#F2F2F7"))
    palette.setColor(QtGui.QPalette.Text, QtGui.QColor("#F2F2F7"))
    palette.setColor(QtGui.QPalette.Button, QtGui.QColor("#1C1C1E"))
    palette.setColor(QtGui.QPalette.ButtonText, QtGui.QColor("#F2F2F7"))
    palette.setColor(QtGui.QPalette.BrightText, QtGui.QColor("#FF453A"))
    palette.setColor(QtGui.QPalette.Highlight, QtGui.QColor("#0A84FF"))
    palette.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor("#FFFFFF"))
    app.setPalette(palette)

    # Apple-esque: compact card, subtle borders, blue accent.
    app.setStyleSheet(
        """
        QWidget { color: #F2F2F7; }
        QLabel { background: transparent; }

        QFrame#Card {
          background-color: #1C1C1E;
          border: 1px solid #2C2C2E;
          border-radius: 14px;
        }

        QLabel#H1 {
          font-size: 18px;
          font-weight: 650;
          color: #F2F2F7;
        }

        QLabel#Title {
          font-size: 16px;
          font-weight: 600;
          color: #F2F2F7;
        }

        QLabel#Subtitle {
          color: #8E8E93;
        }

        QLabel#Status {
          color: #F2F2F7;
        }

        QProgressBar {
          border: none;
          background: #2C2C2E;
          border-radius: 2px;
          height: 4px;
        }
        QProgressBar::chunk {
          background: #0A84FF;
          border-radius: 2px;
        }

        QPlainTextEdit#LogView {
          background-color: #141416;
          border: 1px solid #2C2C2E;
          border-radius: 10px;
          padding: 10px;
        }

        QFrame#QuickInput {
          background-color: rgba(28, 28, 30, 0.94);
          border: 1px solid rgba(58, 58, 60, 0.90);
          border-radius: 12px;
        }
        QFrame#QuickInput[active="true"] {
          border: 1px solid rgba(10, 132, 255, 0.85);
        }
        QLabel#QuickInputPrompt {
          color: #8E8E93;
          font-weight: 700;
          padding-left: 8px;
          padding-right: 6px;
        }
        QLineEdit#QuickInputEdit {
          background: transparent;
          border: none;
          padding: 7px 10px 7px 0px;
          color: #F2F2F7;
          font-size: 12px;
        }
        QLineEdit#QuickInputEdit::placeholder {
          color: #8E8E93;
        }

        QFrame#QuickInputSuggest {
          background-color: rgba(28, 28, 30, 0.96);
          border: 1px solid rgba(58, 58, 60, 0.92);
          border-radius: 12px;
        }
        QFrame#QuickInputSuggest[active="true"] {
          border: 1px solid rgba(10, 132, 255, 0.70);
        }
        QListWidget#QuickInputSuggestList {
          background: transparent;
          border: none;
          outline: none;
        }
        QListWidget#QuickInputSuggestList::item {
          padding: 5px 8px;
          border-radius: 10px;
          color: #F2F2F7;
          font-size: 12px;
        }
        QListWidget#QuickInputSuggestList::item:selected {
          background: rgba(10, 132, 255, 0.20);
        }
        QListWidget#QuickInputSuggestList::item:hover {
          background: rgba(242, 242, 247, 0.08);
        }

        QFrame#Sidebar {
          background-color: #151517;
          border-right: 1px solid #2C2C2E;
        }

        QLabel#Brand {
          font-size: 14px;
          font-weight: 700;
          color: #F2F2F7;
        }

        QListWidget#NavList {
          background: transparent;
          border: none;
          outline: none;
        }
        QListWidget#NavList::item {
          padding: 8px 6px;
          border-radius: 10px;
          color: #F2F2F7;
        }
        QListWidget#NavList::item:selected {
          background: rgba(10, 132, 255, 0.20);
          color: #F2F2F7;
        }
        QListWidget#NavList::item:hover {
          background: rgba(242, 242, 247, 0.08);
        }

        QFrame#Header {
          background-color: #1C1C1E;
          border: 1px solid #2C2C2E;
          border-radius: 14px;
        }

        QLabel#CardTitle {
          font-size: 13px;
          font-weight: 650;
          color: #F2F2F7;
        }
        QLabel#CardSubtitle {
          color: #8E8E93;
        }

        QLabel#InfoName {
          font-size: 13px;
          font-weight: 700;
          color: #F2F2F7;
        }
        QLabel#InfoIndustry {
          color: #8E8E93;
        }
        QLabel#CodePill {
          background-color: rgba(142, 142, 147, 0.14);
          border: 1px solid rgba(58, 58, 60, 0.90);
          border-radius: 9px;
          padding: 2px 6px;
          color: #F2F2F7;
          font-weight: 650;
          min-height: 18px;
        }

        QLabel#RangeLabel {
          color: #8E8E93;
        }

        QLabel#KeyLabel {
          color: #8E8E93;
        }
        QLabel#ValueLabel {
          color: #F2F2F7;
        }
        QLabel#SectionLabel {
          color: #8E8E93;
          font-size: 12px;
          font-weight: 650;
          padding-top: 6px;
        }

        QTableWidget#ProbeTable {
          background-color: #141416;
          border: 1px solid #2C2C2E;
          border-radius: 10px;
          gridline-color: transparent;
          selection-background-color: transparent;
        }
        QTableWidget#WatchTable {
          background-color: #141416;
          border: 1px solid #2C2C2E;
          border-radius: 12px;
          gridline-color: transparent;
          selection-background-color: transparent;
        }
        QTableWidget#WatchTable::item {
          padding: 8px 10px;
          border-bottom: 1px solid rgba(58, 58, 60, 0.45);
        }
        QTableWidget#WatchTable::item:selected {
          background: rgba(10, 132, 255, 0.18);
        }
        QHeaderView#WatchHeader::section {
          background-color: #1C1C1E;
          color: #8E8E93;
          border: none;
          padding: 8px 10px;
          font-weight: 650;
        }
        QHeaderView#WatchHeader::section:horizontal {
          border-right: 1px solid rgba(58, 58, 60, 0.45);
        }
        QHeaderView::section {
          background-color: #1C1C1E;
          color: #8E8E93;
          border: none;
          padding: 6px 8px;
        }

        QFrame#MarketToolbar {
          background-color: #151517;
          border: 1px solid #2C2C2E;
          border-radius: 12px;
        }
        QToolButton#TfButton {
          color: #A1A1A6;
          border: 1px solid transparent;
          border-radius: 10px;
          padding: 6px 10px;
          background: transparent;
        }
        QToolButton#TfButton:checked {
          color: #F2F2F7;
          background: rgba(242, 242, 247, 0.10);
          border: 1px solid rgba(58, 58, 60, 0.90);
        }
        QToolButton#TfButton:hover {
          color: #F2F2F7;
          background: rgba(242, 242, 247, 0.06);
        }
        QScrollArea#TfScroll {
          background: transparent;
        }
        QComboBox#MarketType {
          background-color: #141416;
          border: 1px solid #2C2C2E;
          border-radius: 10px;
          padding: 6px 10px;
          min-width: 140px;
        }
        QComboBox#MarketType::drop-down {
          border: none;
          width: 22px;
        }

        QComboBox#TfCombo {
          background-color: #141416;
          border: 1px solid #2C2C2E;
          border-radius: 10px;
          padding: 6px 10px;
          min-width: 120px;
        }
        QComboBox#TfCombo::drop-down {
          border: none;
          width: 22px;
        }

        QFrame#InfoTabBar {
          background: transparent;
        }
        QToolButton#InfoTabButton {
          background: #1C1C1E;
          border: 1px solid #2C2C2E;
          border-radius: 10px;
          padding: 6px 10px;
          color: #8E8E93;
        }
        QToolButton#InfoTabButton:checked {
          background: rgba(242, 242, 247, 0.10);
          color: #F2F2F7;
        }
        QToolButton#InfoTabButton:hover {
          background: rgba(242, 242, 247, 0.08);
          color: #F2F2F7;
        }

        QScrollArea#InfoScroll {
          background-color: #1C1C1E;
        }
        QScrollArea#InfoScroll QWidget#qt_scrollarea_viewport {
          background-color: #1C1C1E;
        }
        QStackedWidget#InfoPages {
          background: transparent;
        }

        QFrame#InfoDivider {
          border: none;
          background-color: #2C2C2E;
          max-height: 1px;
        }

        QLabel#PlaceholderHint {
          color: #8E8E93;
        }

        QToolButton {
          color: #0A84FF;
          background: transparent;
          border: none;
          padding: 6px 8px;
        }
        QToolButton:hover {
          background: rgba(10, 132, 255, 0.14);
          border-radius: 8px;
        }

        QPushButton {
          background-color: #2C2C2E;
          border: 1px solid #3A3A3C;
          border-radius: 10px;
          padding: 7px 14px;
        }
        QPushButton:hover { background-color: #3A3A3C; }
        QPushButton:pressed { background-color: #48484A; }

        QScrollBar:vertical {
          background: transparent;
          width: 10px;
          margin: 6px 2px 6px 2px;
        }
        QScrollBar::handle:vertical {
          background: #3A3A3C;
          border-radius: 5px;
          min-height: 24px;
        }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
          height: 0px;
        }
        QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
          background: transparent;
        }
        """
    )


def _set_app_icon(app: QtWidgets.QApplication) -> None:
    icon_path = _repo_root() / "img" / "icon.png"
    if icon_path.exists():
        app.setWindowIcon(QtGui.QIcon(str(icon_path)))


class LoaderWorker(QtCore.QObject):
    log = QtCore.Signal(str)
    failed = QtCore.Signal(str)
    succeeded = QtCore.Signal(object)
    finished = QtCore.Signal()

    @QtCore.Slot()
    def run(self) -> None:
        try:
            from mos_quant.core.loader import MoSQuantLoader

            loader = MoSQuantLoader()

            def cb(msg: str) -> None:
                self.log.emit(msg)

            ctx = loader.run(progress_cb=cb)
            self.succeeded.emit(ctx)
        except Exception as exc:
            tb = traceback.format_exc()
            self.failed.emit(f"{type(exc).__name__}: {exc}\n\n{tb}")
        finally:
            self.finished.emit()


class StartupWindow(QtWidgets.QWidget):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("MoS Quant")
        self.setMinimumSize(520, 320)
        self.resize(560, 340)

        self._status_raw = "正在准备…"

        # --- header ---
        icon = QtWidgets.QLabel()
        icon.setFixedSize(44, 44)
        icon_path = _repo_root() / "img" / "icon.png"
        if icon_path.exists():
            pm = QtGui.QPixmap(str(icon_path))
            icon.setPixmap(pm.scaled(44, 44, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))

        title = QtWidgets.QLabel("MoS Quant")
        title.setObjectName("Title")

        subtitle = QtWidgets.QLabel("启动加载")
        subtitle.setObjectName("Subtitle")

        title_col = QtWidgets.QVBoxLayout()
        title_col.setSpacing(2)
        title_col.setContentsMargins(0, 0, 0, 0)
        title_col.addWidget(title)
        title_col.addWidget(subtitle)

        header = QtWidgets.QHBoxLayout()
        header.setSpacing(12)
        header.addWidget(icon, 0, QtCore.Qt.AlignTop)
        header.addLayout(title_col, 1)

        self.status = QtWidgets.QLabel(self._status_raw)
        self.status.setObjectName("Status")
        self.status.setWordWrap(False)

        self.progress = QtWidgets.QProgressBar()
        self.progress.setRange(0, 0)  # indefinite
        self.progress.setTextVisible(False)
        self.progress.setFixedHeight(4)

        self.log_view = QtWidgets.QPlainTextEdit()
        self.log_view.setObjectName("LogView")
        self.log_view.setReadOnly(True)
        self.log_view.setLineWrapMode(QtWidgets.QPlainTextEdit.NoWrap)
        self.log_view.setMaximumBlockCount(2000)
        self.log_view.setFont(QtGui.QFont("Menlo", 11))
        self.log_view.setVisible(False)

        self.details_btn = QtWidgets.QToolButton()
        self.details_btn.setCheckable(True)
        self.details_btn.setChecked(False)
        self.details_btn.setText("详情")
        self.details_btn.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
        self.details_btn.setArrowType(QtCore.Qt.RightArrow)
        self.details_btn.toggled.connect(self._toggle_details)

        self.quit_btn = QtWidgets.QPushButton("取消")
        self.quit_btn.clicked.connect(QtWidgets.QApplication.quit)

        footer = QtWidgets.QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.addWidget(self.details_btn, 0, QtCore.Qt.AlignLeft)
        footer.addStretch(1)
        footer.addWidget(self.quit_btn, 0, QtCore.Qt.AlignRight)

        card = QtWidgets.QFrame()
        card.setObjectName("Card")
        card_layout = QtWidgets.QVBoxLayout()
        card_layout.setContentsMargins(18, 18, 18, 16)
        card_layout.setSpacing(12)
        card_layout.addLayout(header)
        card_layout.addWidget(self.status)
        card_layout.addWidget(self.progress)
        card_layout.addWidget(self.log_view, 1)
        card_layout.addLayout(footer)
        card.setLayout(card_layout)

        outer = QtWidgets.QVBoxLayout()
        outer.setContentsMargins(18, 18, 18, 18)
        outer.addWidget(card)
        self.setLayout(outer)

        self._thread: QtCore.QThread | None = None
        self._worker: LoaderWorker | None = None

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:  # noqa: N802
        super().resizeEvent(event)
        self._render_status()

    def start(self) -> None:
        self._thread = QtCore.QThread(self)
        self._worker = LoaderWorker()
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.log.connect(self._on_log)
        self._worker.failed.connect(self._on_failed)
        self._worker.succeeded.connect(self._on_succeeded)
        self._worker.finished.connect(self._thread.quit)
        self._worker.finished.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)

        self._thread.start()

    def _render_status(self) -> None:
        fm = self.status.fontMetrics()
        width = max(10, self.status.width())
        self.status.setText(fm.elidedText(self._status_raw, QtCore.Qt.ElideRight, width))

    @QtCore.Slot(bool)
    def _toggle_details(self, checked: bool) -> None:
        self.log_view.setVisible(checked)
        self.details_btn.setArrowType(QtCore.Qt.DownArrow if checked else QtCore.Qt.RightArrow)

    @QtCore.Slot(str)
    def _on_log(self, msg: str) -> None:
        progress_prefix = "[progress] "
        if msg.startswith(progress_prefix):
            payload = msg[len(progress_prefix) :].strip()
            pct_str, _, text = payload.partition(" ")
            try:
                pct = max(0, min(100, int(pct_str)))
            except Exception:
                pct = 0

            if self.progress.minimum() != 0 or self.progress.maximum() != 100:
                self.progress.setRange(0, 100)
                self.progress.setTextVisible(False)
            self.progress.setValue(pct)

            self._status_raw = text.strip() if text.strip() else f"{pct}%"
            self._render_status()
            return

        status_prefix = "[status] "
        if msg.startswith(status_prefix):
            self._status_raw = msg[len(status_prefix) :]
            self._render_status()
            return

        self._status_raw = msg
        self._render_status()
        self.log_view.appendPlainText(msg)

    @QtCore.Slot(str)
    def _on_failed(self, detail: str) -> None:
        self.progress.setRange(0, 1)
        self.progress.setValue(1)
        self._status_raw = "加载失败（快速失败）。"
        self._render_status()
        self.details_btn.setChecked(True)

        QtWidgets.QMessageBox.critical(self, "MoS Quant 启动错误", detail)
        QtWidgets.QApplication.quit()

    @QtCore.Slot(object)
    def _on_succeeded(self, ctx: object) -> None:
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        self._status_raw = "就绪，进入主界面…"
        self._render_status()

        from mos_quant.ui.main_window import MainWindow

        win = MainWindow(ctx)
        win.show()
        self.close()
        self._main_window = win  # keep ref


def main() -> int:
    app = QtWidgets.QApplication(sys.argv)
    _apply_dark_theme(app)
    _set_app_icon(app)
    w = StartupWindow()
    w.show()
    w.start()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
