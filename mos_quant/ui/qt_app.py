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
    palette.setColor(QtGui.QPalette.Window, QtGui.QColor("#0B0B0C"))
    palette.setColor(QtGui.QPalette.WindowText, QtGui.QColor("#F2F2F7"))
    palette.setColor(QtGui.QPalette.Base, QtGui.QColor("#121214"))
    palette.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor("#1C1C1E"))
    palette.setColor(QtGui.QPalette.ToolTipBase, QtGui.QColor("#1C1C1E"))
    palette.setColor(QtGui.QPalette.ToolTipText, QtGui.QColor("#F2F2F7"))
    palette.setColor(QtGui.QPalette.Text, QtGui.QColor("#F2F2F7"))
    palette.setColor(QtGui.QPalette.Button, QtGui.QColor("#2C2C2E"))
    palette.setColor(QtGui.QPalette.ButtonText, QtGui.QColor("#F2F2F7"))
    palette.setColor(QtGui.QPalette.BrightText, QtGui.QColor("#FF453A"))
    palette.setColor(QtGui.QPalette.Highlight, QtGui.QColor("#0A84FF"))
    palette.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor("#FFFFFF"))
    app.setPalette(palette)

    # Apple-esque: compact card, subtle borders, blue accent.
    app.setStyleSheet(
        """
        QWidget {
          background-color: #0B0B0C;
          color: #F2F2F7;
        }

        QFrame#Card {
          background-color: #1C1C1E;
          border: 1px solid #2C2C2E;
          border-radius: 14px;
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
          background-color: #121214;
          border: 1px solid #2C2C2E;
          border-radius: 10px;
          padding: 10px;
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
    icon_path = _repo_root() / "img" / "icon" / "icon.png"
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

        self._status_raw = "Preparing …"

        # --- header ---
        icon = QtWidgets.QLabel()
        icon.setFixedSize(44, 44)
        icon_path = _repo_root() / "img" / "icon" / "icon.png"
        if icon_path.exists():
            pm = QtGui.QPixmap(str(icon_path))
            icon.setPixmap(pm.scaled(44, 44, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))

        title = QtWidgets.QLabel("MoS Quant")
        title.setObjectName("Title")

        subtitle = QtWidgets.QLabel("Loader")
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
        self.details_btn.setText("Details")
        self.details_btn.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
        self.details_btn.setArrowType(QtCore.Qt.RightArrow)
        self.details_btn.toggled.connect(self._toggle_details)

        self.quit_btn = QtWidgets.QPushButton("Cancel")
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
        self._status_raw = msg
        self._render_status()
        self.log_view.appendPlainText(msg)

    @QtCore.Slot(str)
    def _on_failed(self, detail: str) -> None:
        self.progress.setRange(0, 1)
        self.progress.setValue(1)
        self._status_raw = "Loader failed (fail-fast)."
        self._render_status()
        self.details_btn.setChecked(True)

        QtWidgets.QMessageBox.critical(self, "MoS Quant Loader Error", detail)
        QtWidgets.QApplication.quit()

    @QtCore.Slot(object)
    def _on_succeeded(self, ctx: object) -> None:
        self.progress.setRange(0, 1)
        self.progress.setValue(1)
        self._status_raw = "Ready. Entering main program …"
        self._render_status()

        win = MainWindow(ctx)
        win.show()
        self.close()
        self._main_window = win  # keep ref


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, ctx: object) -> None:
        super().__init__()
        self.setWindowTitle("MoS Quant")
        self.resize(900, 560)

        # ctx is LoaderContext from mos_quant.core.loader
        default_url = getattr(ctx, "default_url", "")
        trade_calendar = getattr(ctx, "trade_calendar", None)

        info = QtWidgets.QLabel()
        info.setTextInteractionFlags(QtCore.Qt.TextSelectableByMouse)
        info.setFont(QtGui.QFont("Menlo", 12))

        if hasattr(trade_calendar, "__len__") and len(trade_calendar) > 0:
            first = trade_calendar["trade_date"].iloc[0]
            last = trade_calendar["trade_date"].iloc[-1]
            rows = len(trade_calendar)
            info.setText(
                "Default data source:\n"
                f"{default_url}\n\n"
                "Trade calendar cached locally.\n"
                f"Rows: {rows}\n"
                f"Range: {first} .. {last}\n"
            )
        else:
            info.setText(
                "Default data source:\n"
                f"{default_url}\n\n"
                "Trade calendar: (empty)\n"
            )

        central = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(info)
        central.setLayout(layout)
        self.setCentralWidget(central)


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
