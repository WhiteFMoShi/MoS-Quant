from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math
import random
import re
from typing import Iterable

from PySide6 import QtCore, QtGui, QtWidgets  # type: ignore

from mos_quant.core.stock_search import is_abbr_query, name_to_abbr, normalize_name


@dataclass(frozen=True)
class _CalendarSummary:
    rows: int
    first: str
    last: str


def _summarize_trade_calendar(trade_calendar: object) -> _CalendarSummary | None:
    if trade_calendar is None:
        return None
    if not hasattr(trade_calendar, "__len__") or len(trade_calendar) == 0:  # type: ignore[arg-type]
        return None
    try:
        first = trade_calendar["trade_date"].iloc[0]
        last = trade_calendar["trade_date"].iloc[-1]
        rows = int(len(trade_calendar))
        return _CalendarSummary(rows=rows, first=str(first), last=str(last))
    except Exception:
        return None


def _qt_key(name: str) -> int:
    if hasattr(QtCore.Qt, name):
        return int(getattr(QtCore.Qt, name))
    qt_key = getattr(QtCore.Qt, "Key", None)
    if qt_key is not None and hasattr(qt_key, name):
        return int(getattr(qt_key, name))
    raise AttributeError(f"Qt key not found: {name}")


class _Card(QtWidgets.QFrame):
    def __init__(self, title: str, *, subtitle: str = "") -> None:
        super().__init__()
        self.setObjectName("Card")

        self._title = QtWidgets.QLabel(title)
        self._title.setObjectName("CardTitle")

        self._subtitle = QtWidgets.QLabel(subtitle)
        self._subtitle.setObjectName("CardSubtitle")
        self._subtitle.setVisible(bool(subtitle))

        self._header = QtWidgets.QWidget()
        header_l = QtWidgets.QVBoxLayout()
        header_l.setContentsMargins(0, 0, 0, 0)
        header_l.setSpacing(2)
        header_l.addWidget(self._title)
        header_l.addWidget(self._subtitle)
        self._header.setLayout(header_l)
        self._header.setVisible(bool(title) or bool(subtitle))

        self._body = QtWidgets.QVBoxLayout()
        self._body.setContentsMargins(0, 0, 0, 0)
        self._body.setSpacing(10)

        top = QtWidgets.QVBoxLayout()
        top.setContentsMargins(16, 14, 16, 14)
        top.setSpacing(12)
        top.addWidget(self._header, 0)
        top.addLayout(self._body, 1)
        self.setLayout(top)

    def set_subtitle(self, text: str) -> None:
        self._subtitle.setText(text)
        self._subtitle.setVisible(bool(text))
        self._header.setVisible(bool(self._title.text()) or bool(text))

    @property
    def body(self) -> QtWidgets.QVBoxLayout:
        return self._body


class _KeyValueRow(QtWidgets.QWidget):
    def __init__(
        self,
        key: str,
        value: str,
        *,
        selectable: bool = True,
        compact: bool = False,
        value_width: int | None = None,
    ) -> None:
        super().__init__()
        self._key = _ElidedLabel(key)
        self._key.setObjectName("KeyLabel")
        self._key.setMinimumWidth(56)
        self._key.setMaximumWidth(72 if not compact else 16_777_215)

        self._value = _ElidedLabel(value)
        self._value.setObjectName("ValueLabel")
        self._value.setTextInteractionFlags(
            QtCore.Qt.TextSelectableByMouse if selectable else QtCore.Qt.NoTextInteraction
        )
        self._value.setAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
        if value_width is None:
            value_width = 88 if compact else 116
        self._value.setFixedWidth(int(value_width))

        lay = QtWidgets.QHBoxLayout()
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(8)
        if compact:
            self._key.setAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
            self._key.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
            lay.addWidget(self._key, 1)
            lay.addWidget(self._value, 0)
        else:
            lay.addWidget(self._key, 0)
            lay.addStretch(1)
            lay.addWidget(self._value, 0)
        self.setLayout(lay)

    def set_value(self, value: str) -> None:
        self._value.setText(value)

    def set_value_color(self, color: str | None) -> None:
        self._value.setStyleSheet("" if not color else f"color: {color};")


class _ElidedLabel(QtWidgets.QLabel):
    def __init__(self, text: str = "") -> None:
        super().__init__(text)
        self._raw = text
        self.setWordWrap(False)

    def setText(self, text: str) -> None:  # noqa: N802
        self._raw = text
        self._render()

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:  # noqa: N802
        super().resizeEvent(event)
        self._render()

    def _render(self) -> None:
        fm = self.fontMetrics()
        w = max(10, self.width())
        super().setText(fm.elidedText(self._raw, QtCore.Qt.ElideRight, w))


class _QuickInputOverlay(QtWidgets.QFrame):
    submitted = QtCore.Signal(str)
    active_changed = QtCore.Signal(bool)
    query_changed = QtCore.Signal(str)

    def __init__(self, parent: QtWidgets.QWidget) -> None:
        super().__init__(parent)
        self.setObjectName("QuickInput")
        self.setAttribute(QtCore.Qt.WA_StyledBackground, True)
        self.setVisible(False)

        self._prompt = QtWidgets.QLabel("›")
        self._prompt.setObjectName("QuickInputPrompt")

        self._edit = QtWidgets.QLineEdit()
        self._edit.setObjectName("QuickInputEdit")
        self._edit.setPlaceholderText("代码/名称")
        self._edit.returnPressed.connect(self._on_return_pressed)
        self._edit.textEdited.connect(self.query_changed.emit)
        self._edit.installEventFilter(self)

        lay = QtWidgets.QHBoxLayout()
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)
        lay.addWidget(self._prompt, 0)
        lay.addWidget(self._edit, 1)
        self.setLayout(lay)

        self.setFixedSize(150, 32)

        shadow = QtWidgets.QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(26)
        shadow.setOffset(0, 10)
        shadow.setColor(QtGui.QColor(0, 0, 0, 160))
        self.setGraphicsEffect(shadow)

        self._suggest: _QuickSuggestPopup | None = None

    def attach_suggest(self, popup: "_QuickSuggestPopup") -> None:
        self._suggest = popup

    def eventFilter(self, obj: QtCore.QObject, event: QtCore.QEvent) -> bool:  # noqa: N802
        if obj is self._edit and event.type() in (QtCore.QEvent.FocusIn, QtCore.QEvent.FocusOut):
            active = event.type() == QtCore.QEvent.FocusIn
            self._set_active(active)
            self.active_changed.emit(active)
        if obj is self._edit and event.type() == QtCore.QEvent.KeyPress:
            ev = event  # type: ignore[assignment]
            if isinstance(ev, QtGui.QKeyEvent) and self._suggest is not None and self._suggest.isVisible():
                key = ev.key()
                if key == _qt_key("Key_Down"):
                    self._suggest.move_selection(+1)
                    return True
                if key == _qt_key("Key_Up"):
                    self._suggest.move_selection(-1)
                    return True
                if key in {_qt_key("Key_Return"), _qt_key("Key_Enter"), _qt_key("Key_Tab")}:
                    picked = self._suggest.current_text()
                    if picked:
                        self._submit_text(picked)
                        return True
        return super().eventFilter(obj, event)

    def show_and_focus(self) -> None:
        self.setVisible(True)
        self.raise_()
        self._edit.setFocus(QtCore.Qt.ShortcutFocusReason)
        self._edit.selectAll()

    def hide_overlay(self) -> None:
        self.setVisible(False)
        self._edit.clear()
        self._set_active(False)

    def _set_active(self, active: bool) -> None:
        self.setProperty("active", active)
        self.style().unpolish(self)
        self.style().polish(self)
        self.update()

    def _submit_text(self, text: str) -> None:
        cleaned = text.strip()
        if not cleaned:
            return
        self.submitted.emit(cleaned)

    def set_text(self, text: str) -> None:
        self._edit.setText(text)
        self._edit.setCursorPosition(len(text))

    def text(self) -> str:
        return self._edit.text()

    @QtCore.Slot()
    def _on_return_pressed(self) -> None:
        text = self._edit.text().strip()
        if not text:
            return
        self._submit_text(text)


class _QuickSuggestPopup(QtWidgets.QFrame):
    activated = QtCore.Signal(str)

    def __init__(self, parent: QtWidgets.QWidget) -> None:
        super().__init__(parent)
        self.setObjectName("QuickInputSuggest")
        self.setAttribute(QtCore.Qt.WA_StyledBackground, True)
        self.setVisible(False)

        self._list = QtWidgets.QListWidget()
        self._list.setObjectName("QuickInputSuggestList")
        self._list.setUniformItemSizes(True)
        self._list.setSpacing(2)
        self._list.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self._list.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self._list.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self._list.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self._list.itemActivated.connect(lambda it: self.activated.emit(it.text()))
        self._list.itemClicked.connect(lambda it: self.activated.emit(it.text()))

        lay = QtWidgets.QVBoxLayout()
        lay.setContentsMargins(6, 6, 6, 6)
        lay.setSpacing(0)
        lay.addWidget(self._list, 1)
        self.setLayout(lay)

        shadow = QtWidgets.QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(18)
        shadow.setOffset(0, 8)
        shadow.setColor(QtGui.QColor(0, 0, 0, 140))
        self.setGraphicsEffect(shadow)

    def set_items(self, items: list[str]) -> None:
        self._list.clear()
        for text in items:
            it = QtWidgets.QListWidgetItem(text)
            it.setSizeHint(QtCore.QSize(0, 28))
            self._list.addItem(it)
        if items:
            self._list.setCurrentRow(0)

    def current_text(self) -> str:
        it = self._list.currentItem()
        return "" if it is None else it.text().strip()

    def move_selection(self, delta: int) -> None:
        n = self._list.count()
        if n <= 0:
            return
        cur = max(0, self._list.currentRow())
        nxt = max(0, min(n - 1, cur + int(delta)))
        self._list.setCurrentRow(nxt)
        self._list.scrollToItem(self._list.currentItem(), QtWidgets.QAbstractItemView.PositionAtCenter)

    def show_for_anchor(self, anchor: QtWidgets.QWidget) -> None:
        if not self._list.count():
            self.hide()
            return
        gap = 2
        w = anchor.width()
        item_h = 28
        visible_rows = min(5, self._list.count())
        h = 6 + visible_rows * item_h + 6
        self.resize(w, min(200, h))

        parent = self.parentWidget()
        if parent is None:
            return
        self.setProperty("active", bool(anchor.property("active")))
        self.style().unpolish(self)
        self.style().polish(self)
        ax, ay = anchor.pos().x(), anchor.pos().y()
        above_y = ay - self.height() - gap
        below_y = ay + anchor.height() + gap
        y = above_y if above_y >= 0 else below_y
        x = ax
        x = max(0, min(parent.width() - self.width(), x))
        y = max(0, min(parent.height() - self.height(), y))
        self.move(x, y)
        self.setVisible(True)
        self.raise_()


class _ProbeTable(QtWidgets.QTableWidget):
    def __init__(self) -> None:
        super().__init__(0, 4)
        self.setObjectName("ProbeTable")
        self.setHorizontalHeaderLabels(["URL", "成功", "次数", "延迟"])
        self.verticalHeader().setVisible(False)
        self.horizontalHeader().setStretchLastSection(True)
        self.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)
        self.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeToContents)
        self.horizontalHeader().setSectionResizeMode(2, QtWidgets.QHeaderView.ResizeToContents)
        self.horizontalHeader().setSectionResizeMode(3, QtWidgets.QHeaderView.ResizeToContents)
        self.setShowGrid(False)
        self.setAlternatingRowColors(True)
        self.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.setFocusPolicy(QtCore.Qt.NoFocus)

    def set_results(self, results: Iterable[object]) -> None:
        def safe_int(x: object, default: int = 0) -> int:
            try:
                return int(x)  # type: ignore[arg-type]
            except Exception:
                return default

        def safe_float(x: object, default: float = 0.0) -> float:
            try:
                return float(x)  # type: ignore[arg-type]
            except Exception:
                return default

        rows = list(results)
        self.setRowCount(len(rows))

        for r, item in enumerate(rows):
            target = getattr(item, "target", None)
            url = getattr(target, "url", "") if target is not None else ""
            success = safe_int(getattr(item, "success_count", 0))
            attempts = safe_int(getattr(item, "attempts", 0))
            latency = safe_float(getattr(item, "avg_latency_ms", 0.0))

            cells = [
                QtWidgets.QTableWidgetItem(str(url)),
                QtWidgets.QTableWidgetItem(str(success)),
                QtWidgets.QTableWidgetItem(str(attempts)),
                QtWidgets.QTableWidgetItem(f"{latency:.1f} ms"),
            ]
            for c, cell in enumerate(cells):
                cell.setFlags(QtCore.Qt.ItemIsEnabled)
                if c > 0:
                    cell.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                self.setItem(r, c, cell)


class DashboardPage(QtWidgets.QWidget):
    def __init__(self, ctx: object) -> None:
        super().__init__()

        default_url = str(getattr(ctx, "default_url", ""))
        trade_calendar = getattr(ctx, "trade_calendar", None)
        probe_results = getattr(ctx, "probe_results", []) or []

        cal = _summarize_trade_calendar(trade_calendar)

        source_card = _Card("默认数据源", subtitle="由网络探测自动选择")
        source_card.body.addWidget(_KeyValueRow("URL", default_url or "（空）"))
        source_card.body.addWidget(
            _KeyValueRow("保存至", "cache/watch/default_data_source.json", selectable=False)
        )
        source_card.body.addStretch(1)

        calendar_card = _Card("交易日历", subtitle="已本地缓存（新鲜度：天）")
        if cal is None:
            calendar_card.body.addWidget(_KeyValueRow("行数", "0"))
            calendar_card.body.addWidget(_KeyValueRow("范围", "（空）"))
        else:
            calendar_card.body.addWidget(_KeyValueRow("行数", str(cal.rows)))
            calendar_card.body.addWidget(_KeyValueRow("起始", cal.first))
            calendar_card.body.addWidget(_KeyValueRow("结束", cal.last))
        calendar_card.body.addStretch(1)

        probe_card = _Card("探测结果", subtitle="可达且延迟最低者优先")
        table = _ProbeTable()
        table.set_results(probe_results)
        probe_card.body.addWidget(table, 1)

        grid = QtWidgets.QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(14)
        grid.setVerticalSpacing(14)

        grid.addWidget(source_card, 0, 0)
        grid.addWidget(calendar_card, 0, 1)
        grid.addWidget(probe_card, 1, 0, 1, 2)
        grid.setRowStretch(1, 1)
        grid.setColumnStretch(0, 1)
        grid.setColumnStretch(1, 1)

        root = QtWidgets.QVBoxLayout()
        root.setContentsMargins(0, 0, 0, 0)
        root.addLayout(grid, 1)
        self.setLayout(root)


class _WatchTable(QtWidgets.QTableWidget):
    def __init__(self) -> None:
        super().__init__(0, 11)
        self.setObjectName("WatchTable")
        self.setShowGrid(False)
        self.setAlternatingRowColors(True)
        self.setCornerButtonEnabled(False)
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.setFocusPolicy(QtCore.Qt.NoFocus)

        headers = [
            "序号",
            "证券代码",
            "证券名称",
            "",
            "现价",
            "涨幅%",
            "涨跌",
            "涨速%",
            "换手%",
            "自选日",
            "自选价格",
        ]
        self.setHorizontalHeaderLabels(headers)
        self.verticalHeader().setVisible(False)

        hh = self.horizontalHeader()
        hh.setObjectName("WatchHeader")
        hh.setDefaultAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
        hh.setHighlightSections(False)
        hh.setSectionsClickable(True)
        hh.setSortIndicatorShown(True)

        # Column widths tuned for narrow + readable layout (like the reference screenshot).
        hh.setSectionResizeMode(0, QtWidgets.QHeaderView.Fixed)
        hh.setSectionResizeMode(1, QtWidgets.QHeaderView.Fixed)
        hh.setSectionResizeMode(2, QtWidgets.QHeaderView.Stretch)
        hh.setSectionResizeMode(3, QtWidgets.QHeaderView.Fixed)
        for c in (4, 5, 6, 7, 8, 9, 10):
            hh.setSectionResizeMode(c, QtWidgets.QHeaderView.Fixed)

        self.setColumnWidth(0, 52)
        self.setColumnWidth(1, 96)
        self.setColumnWidth(3, 36)
        self.setColumnWidth(4, 84)
        self.setColumnWidth(5, 82)
        self.setColumnWidth(6, 82)
        self.setColumnWidth(7, 82)
        self.setColumnWidth(8, 82)
        self.setColumnWidth(9, 106)
        self.setColumnWidth(10, 96)

        self._up_color = QtGui.QColor("#FF453A")  # CN convention: up=red
        self._down_color = QtGui.QColor("#30D158")  # down=green
        self._muted = QtGui.QColor("#8E8E93")

    def set_rows(self, rows: list[dict[str, object]]) -> None:
        self.setSortingEnabled(False)
        self.setRowCount(0)

        base_font = self.font()
        num_font = QtGui.QFont(base_font)
        num_font.setPointSize(max(10, base_font.pointSize()))
        num_font.setBold(True)

        def make_item(text: str, *, align: QtCore.Qt.Alignment | None = None) -> QtWidgets.QTableWidgetItem:
            it = QtWidgets.QTableWidgetItem(text)
            it.setFlags(QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsSelectable)
            if align is not None:
                it.setTextAlignment(align)
            return it

        def fmt_num(v: object, digits: int = 2, suffix: str = "") -> str:
            if v is None:
                return "--"
            try:
                return f"{float(v):.{digits}f}{suffix}"
            except Exception:
                return str(v)

        def apply_signed_color(item: QtWidgets.QTableWidgetItem, v: object) -> None:
            try:
                f = float(v)
            except Exception:
                item.setForeground(self._muted)
                return
            if f > 0:
                item.setForeground(self._up_color)
            elif f < 0:
                item.setForeground(self._down_color)
            else:
                item.setForeground(self._muted)

        def set_sort_number(item: QtWidgets.QTableWidgetItem, v: object) -> None:
            try:
                item.setData(QtCore.Qt.ItemDataRole.EditRole, float(v))
            except Exception:
                return

        for r, row in enumerate(rows):
            self.insertRow(r)
            self.setRowHeight(r, 42)

            seq = make_item(str(r + 1), align=QtCore.Qt.AlignCenter)
            seq.setForeground(self._muted)
            seq.setData(QtCore.Qt.ItemDataRole.EditRole, int(r + 1))
            self.setItem(r, 0, seq)

            code = make_item(str(row.get("code", "")), align=QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
            code.setFont(num_font)
            self.setItem(r, 1, code)

            name = make_item(str(row.get("name", "")), align=QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
            name.setFont(num_font)
            self.setItem(r, 2, name)

            tag = make_item(str(row.get("tag", "")), align=QtCore.Qt.AlignCenter)
            if str(row.get("tag", "")).strip():
                tag.setForeground(self._up_color)
                tag.setFont(num_font)
            else:
                tag.setForeground(self._muted)
            self.setItem(r, 3, tag)

            price_v = row.get("price")
            pct_v = row.get("pct")
            chg_v = row.get("chg")
            speed_v = row.get("speed")

            price = make_item(fmt_num(price_v), align=QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            price.setFont(num_font)
            if price_v is not None:
                set_sort_number(price, price_v)
            apply_signed_color(price, pct_v if pct_v is not None else chg_v)
            self.setItem(r, 4, price)

            pct = make_item(fmt_num(pct_v), align=QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            if pct_v is not None:
                set_sort_number(pct, pct_v)
            apply_signed_color(pct, pct_v)
            self.setItem(r, 5, pct)

            chg = make_item(fmt_num(chg_v), align=QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            if chg_v is not None:
                set_sort_number(chg, chg_v)
            apply_signed_color(chg, chg_v)
            self.setItem(r, 6, chg)

            speed = make_item(fmt_num(speed_v), align=QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            if speed_v is not None:
                set_sort_number(speed, speed_v)
            apply_signed_color(speed, speed_v)
            self.setItem(r, 7, speed)

            turnover_v = row.get("turnover")
            turnover = make_item(fmt_num(turnover_v), align=QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            turnover.setFont(num_font)
            if turnover_v is not None:
                set_sort_number(turnover, turnover_v)
            self.setItem(r, 8, turnover)

            watch_date = make_item(
                str(row.get("watch_date", "--")), align=QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter
            )
            watch_date.setForeground(self._muted)
            watch_date.setFont(num_font)
            try:
                watch_date.setData(QtCore.Qt.ItemDataRole.EditRole, int(str(row.get("watch_date", "") or "0")))
            except Exception:
                pass
            self.setItem(r, 9, watch_date)

            watch_price_v = row.get("watch_price")
            watch_price = make_item(fmt_num(watch_price_v), align=QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            watch_price.setFont(num_font)
            if watch_price_v is not None:
                set_sort_number(watch_price, watch_price_v)
            if price_v is not None and watch_price_v is not None:
                try:
                    diff = float(price_v) - float(watch_price_v)
                    apply_signed_color(watch_price, diff)
                except Exception:
                    watch_price.setForeground(self._muted)
            else:
                watch_price.setForeground(self._muted)
            self.setItem(r, 10, watch_price)

        self.setSortingEnabled(True)

def _try_import_pyqtgraph():
    try:
        import pyqtgraph as pg  # type: ignore

        return pg
    except Exception:
        return None


def _moving_avg(values: list[float], window: int) -> list[float]:
    if window <= 1:
        return list(values)
    out: list[float] = []
    buf: list[float] = []
    s = 0.0
    for v in values:
        buf.append(v)
        s += v
        if len(buf) > window:
            s -= buf.pop(0)
        out.append(s / len(buf))
    return out


def _ema(values: list[float], span: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (span + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1.0 - alpha) * out[-1])
    return out


class MarketPage(QtWidgets.QWidget):
    def __init__(self) -> None:
        super().__init__()

        self._pg = _try_import_pyqtgraph()
        self._rng = random.Random(42)
        self._tf_key_by_label: dict[str, str] = {}

        # ---- toolbar controls ----
        self.type_cb = QtWidgets.QComboBox()
        self.type_cb.setObjectName("MarketType")
        self.type_cb.addItems(["A股日线", "指数日线", "ETF日线", "A股分钟线"])

        self.refresh_btn = QtWidgets.QPushButton("刷新行情")
        self.refresh_btn.clicked.connect(self._on_refresh_clicked)

        self._tf_buttons: dict[str, QtWidgets.QToolButton] = {}
        self._tf_group = QtWidgets.QButtonGroup(self)
        self._tf_group.setExclusive(True)
        self._current_tf = "60分钟"

        tf_items = [
            ("分时", "intraday"),
            ("5日", "5d"),
            ("1分钟", "1m"),
            ("5分钟", "5m"),
            ("15分钟", "15m"),
            ("30分钟", "30m"),
            ("60分钟", "60m"),
            ("日线", "1d"),
            ("周线", "1w"),
            ("月线", "1mo"),
            ("季线", "1q"),
            ("年线", "1y"),
        ]

        for label, key in tf_items:
            self._tf_key_by_label[label] = key

        tf_bar = QtWidgets.QWidget()
        tf_bar.setObjectName("TfBar")
        tf_l = QtWidgets.QHBoxLayout()
        tf_l.setContentsMargins(0, 0, 0, 0)
        tf_l.setSpacing(4)
        tf_bar.setLayout(tf_l)

        for idx, (label, key) in enumerate(tf_items):
            btn = QtWidgets.QToolButton()
            btn.setObjectName("TfButton")
            btn.setText(label)
            btn.setCheckable(True)
            btn.setAutoRaise(True)
            btn.setToolButtonStyle(QtCore.Qt.ToolButtonTextOnly)
            self._tf_group.addButton(btn, idx)
            self._tf_buttons[label] = btn
            tf_l.addWidget(btn, 0)
            btn.clicked.connect(lambda _checked=False, k=key, l=label: self._on_timeframe_changed(l, k))

        self._tf_buttons[self._current_tf].setChecked(True)

        tf_scroll = QtWidgets.QScrollArea()
        tf_scroll.setObjectName("TfScroll")
        tf_scroll.setFrameShape(QtWidgets.QFrame.NoFrame)
        tf_scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        tf_scroll.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        tf_scroll.setWidgetResizable(True)
        tf_scroll.setWidget(tf_bar)
        tf_scroll.setFixedHeight(38)

        self._tf_combo = QtWidgets.QComboBox()
        self._tf_combo.setObjectName("TfCombo")
        self._tf_combo.addItems([label for label, _ in tf_items])
        self._tf_combo.setCurrentText(self._current_tf)
        self._tf_combo.currentTextChanged.connect(self._on_timeframe_combo_changed)

        self._tf_switch = QtWidgets.QStackedWidget()
        self._tf_switch.setObjectName("TfSwitch")
        self._tf_switch.addWidget(tf_scroll)
        self._tf_switch.addWidget(self._tf_combo)
        self._tf_switch.setCurrentIndex(0)
        self._tf_switch.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)

        toolbar = QtWidgets.QFrame()
        toolbar.setObjectName("MarketToolbar")
        self._toolbar = toolbar
        tb_l = QtWidgets.QHBoxLayout()
        tb_l.setContentsMargins(12, 8, 12, 8)
        tb_l.setSpacing(10)
        tb_l.addWidget(QtWidgets.QLabel("类型"), 0)
        tb_l.addWidget(self.type_cb, 0)
        tb_l.addSpacing(8)
        tb_l.addWidget(self._tf_switch, 1)
        tb_l.addWidget(self.refresh_btn, 0)
        toolbar.setLayout(tb_l)
        toolbar.installEventFilter(self)

        # ---- left chart panel ----
        chart = _Card("")
        self._chart_host = QtWidgets.QWidget()
        chart.body.addWidget(self._chart_host, 1)

        chart_host_l = QtWidgets.QVBoxLayout()
        chart_host_l.setContentsMargins(0, 0, 0, 0)
        chart_host_l.setSpacing(10)
        self._chart_host.setLayout(chart_host_l)
        chart_host_l.addWidget(toolbar, 0)

        self._price_plot = None
        self._vol_plot = None
        self._macd_plot = None
        self._series = {}

        if self._pg is None:
            placeholder = QtWidgets.QLabel("未安装 pyqtgraph，图表区域暂不可用。")
            placeholder.setObjectName("PlaceholderHint")
            placeholder.setAlignment(QtCore.Qt.AlignCenter)
            chart_host_l.addWidget(placeholder, 1)
        else:
            self._build_charts(chart_host_l)

        # ---- right info panel ----
        self._stock_name = _ElidedLabel("示例股票")
        self._stock_name.setObjectName("InfoName")

        self._stock_code = QtWidgets.QLabel("000000")
        self._stock_code.setObjectName("CodePill")
        self._stock_code.setAlignment(QtCore.Qt.AlignCenter)

        self._stock_industry = _ElidedLabel("行业示例")
        self._stock_industry.setObjectName("InfoIndustry")

        self._price = QtWidgets.QLabel("0.00")
        f = QtGui.QFont()
        f.setPointSize(28)
        f.setBold(True)
        self._price.setFont(f)

        self._change = QtWidgets.QLabel("+0.00  +0.00%")
        cf = QtGui.QFont()
        cf.setPointSize(12)
        cf.setBold(True)
        self._change.setFont(cf)

        self._range = QtWidgets.QLabel(f"{date.today()}  -  {date.today()}")
        self._range.setObjectName("RangeLabel")
        self._range.setWordWrap(True)

        info = _Card("")

        summary = QtWidgets.QWidget()
        summary_l = QtWidgets.QVBoxLayout()
        summary_l.setContentsMargins(2, 2, 2, 2)
        summary_l.setSpacing(6)
        summary.setLayout(summary_l)

        name_row = QtWidgets.QHBoxLayout()
        name_row.setContentsMargins(0, 0, 0, 0)
        name_row.setSpacing(8)
        name_row.addWidget(self._stock_name, 1)
        name_row.addWidget(self._stock_code, 0)
        summary_l.addLayout(name_row)
        summary_l.addWidget(self._stock_industry)
        summary_l.addWidget(self._price)
        summary_l.addWidget(self._change)
        summary_l.addWidget(self._range)
        info.body.addWidget(summary, 0)

        divider = QtWidgets.QFrame()
        divider.setObjectName("InfoDivider")
        divider.setFrameShape(QtWidgets.QFrame.HLine)
        divider.setFrameShadow(QtWidgets.QFrame.Plain)
        info.body.addWidget(divider, 0)

        metrics_scroll = QtWidgets.QScrollArea()
        metrics_scroll.setObjectName("InfoScroll")
        metrics_scroll.setWidgetResizable(True)
        metrics_scroll.setFrameShape(QtWidgets.QFrame.NoFrame)
        metrics_scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        metrics_scroll.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        # Ensure the scroll area blends with the card surface (avoid the darker Base color band).
        def _force_surface_bg(widget: QtWidgets.QWidget, color: str = "#1C1C1E") -> None:
            widget.setAutoFillBackground(True)
            pal = widget.palette()
            qcolor = QtGui.QColor(color)
            pal.setColor(QtGui.QPalette.Window, qcolor)
            pal.setColor(QtGui.QPalette.Base, qcolor)
            pal.setColor(QtGui.QPalette.AlternateBase, qcolor)
            widget.setPalette(pal)

        _force_surface_bg(metrics_scroll)
        _force_surface_bg(metrics_scroll.viewport())

        metrics_container = QtWidgets.QWidget()
        _force_surface_bg(metrics_container)
        metrics_l = QtWidgets.QVBoxLayout()
        metrics_l.setContentsMargins(0, 0, 0, 0)
        metrics_l.setSpacing(6)
        metrics_l.setAlignment(QtCore.Qt.AlignTop)
        metrics_container.setLayout(metrics_l)
        metrics_scroll.setWidget(metrics_container)

        self._stat_rows = {}
        keys = [
            "昨收",
            "开盘",
            "最高",
            "最低",
            "收盘",
            "涨跌",
            "涨跌幅",
            "成交量",
            "成交额",
            "5日涨跌",
            "20日涨跌",
            "年内涨跌",
            "20日波动",
        ]
        for key in keys:
            row = _KeyValueRow(key, "--", compact=True)
            self._stat_rows[key] = row
            metrics_l.addWidget(row)

        flow_keys = [
            "主力净流入",
            "超大单净流入",
            "大单净流入",
            "中单净流入",
            "小单净流入",
        ]
        flow_divider = QtWidgets.QFrame()
        flow_divider.setObjectName("InfoDivider")
        flow_divider.setFrameShape(QtWidgets.QFrame.HLine)
        flow_divider.setFrameShadow(QtWidgets.QFrame.Plain)
        metrics_l.addWidget(flow_divider)

        flow_title = QtWidgets.QLabel("资金流向")
        flow_title.setObjectName("SectionLabel")
        metrics_l.addWidget(flow_title)

        self._flow_rows = {}
        for key in flow_keys:
            # Flow labels are longer; shrink value column so the key is fully readable in narrow panel.
            row = _KeyValueRow(key, "--", compact=True, value_width=64)
            self._flow_rows[key] = row
            metrics_l.addWidget(row)

        info.body.addWidget(metrics_scroll, 1)

        # ---- layout (two panels) ----
        right_panel = QtWidgets.QWidget()
        right_l = QtWidgets.QVBoxLayout()
        right_l.setContentsMargins(0, 0, 0, 0)
        right_l.setSpacing(0)
        right_l.addWidget(info, 1)
        right_panel.setLayout(right_l)
        right_panel.setFixedWidth(190)

        root = QtWidgets.QVBoxLayout()
        root.setContentsMargins(0, 0, 0, 0)
        row = QtWidgets.QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(14)
        row.addWidget(chart, 1)
        row.addWidget(right_panel, 0)
        root.addLayout(row, 1)
        self.setLayout(root)

        self._on_refresh_clicked()

    def _on_timeframe_changed(self, label: str, key: str) -> None:
        _ = key  # reserved for future data fetch params
        self._current_tf = label
        if label in self._tf_buttons:
            self._tf_buttons[label].setChecked(True)
        if hasattr(self, "_tf_combo"):
            self._tf_combo.blockSignals(True)
            self._tf_combo.setCurrentText(label)
            self._tf_combo.blockSignals(False)
        # For demo data, we just refresh the series.
        self._on_refresh_clicked()

    def _on_timeframe_combo_changed(self, label: str) -> None:
        key = self._tf_key_by_label.get(label, "")
        self._on_timeframe_changed(label, key)

    def _update_tf_mode(self) -> None:
        # When the window gets narrow, switch the tab-bar to a dropdown to avoid ugly truncation.
        # Threshold chosen empirically for our toolbar contents.
        width = self._toolbar.width() if hasattr(self, "_toolbar") else 9999
        use_combo = width < 740
        want_index = 1 if use_combo else 0
        if hasattr(self, "_tf_switch") and self._tf_switch.currentIndex() != want_index:
            self._tf_switch.setCurrentIndex(want_index)

    def eventFilter(self, obj: QtCore.QObject, event: QtCore.QEvent) -> bool:  # noqa: N802
        if getattr(self, "_toolbar", None) is obj and event.type() == QtCore.QEvent.Resize:
            self._update_tf_mode()
        return super().eventFilter(obj, event)

    def _build_charts(self, parent_layout: QtWidgets.QVBoxLayout) -> None:
        pg = self._pg
        assert pg is not None

        pg.setConfigOptions(antialias=True, foreground="#A1A1A6")
        glw = pg.GraphicsLayoutWidget()
        glw.setBackground(None)
        glw.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)

        price = glw.addPlot(row=0, col=0)
        price.showGrid(x=True, y=True, alpha=0.12)
        price.setMenuEnabled(False)
        price.setMouseEnabled(x=True, y=False)
        price.hideButtons()

        vol = glw.addPlot(row=1, col=0)
        vol.showGrid(x=True, y=True, alpha=0.10)
        vol.setMenuEnabled(False)
        vol.setMouseEnabled(x=True, y=False)
        vol.hideButtons()
        vol.setMaximumHeight(140)

        macd = glw.addPlot(row=2, col=0)
        macd.showGrid(x=True, y=True, alpha=0.10)
        macd.setMenuEnabled(False)
        macd.setMouseEnabled(x=True, y=False)
        macd.hideButtons()
        macd.setMaximumHeight(160)

        self._price_plot = price
        self._vol_plot = vol
        self._macd_plot = macd
        parent_layout.addWidget(glw, 1)

    def _gen_dummy_series(self, n: int = 140) -> dict[str, list[float]]:
        closes: list[float] = []
        vols: list[float] = []
        base = 48.0 + self._rng.random() * 4.0
        drift = (self._rng.random() - 0.5) * 0.03

        v = base
        for i in range(n):
            noise = self._rng.gauss(0.0, 0.9) * (0.65 + 0.35 * math.sin(i / 9.0))
            v = max(1.0, v + drift + noise)
            closes.append(float(v))
            vols.append(float(150_000 + abs(self._rng.gauss(0.0, 1.0)) * 220_000))

        ma5 = _moving_avg(closes, 5)
        ma10 = _moving_avg(closes, 10)
        ma20 = _moving_avg(closes, 20)

        ema12 = _ema(closes, 12)
        ema26 = _ema(closes, 26)
        macd = [a - b for a, b in zip(ema12, ema26)]
        signal = _ema(macd, 9)
        hist = [m - s for m, s in zip(macd, signal)]

        start_day = date(2026, 1, 1)
        dates = [start_day.toordinal() + i for i in range(n)]
        return {
            "x": [float(i) for i in range(n)],
            "dates": dates,
            "close": closes,
            "volume": vols,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "macd": macd,
            "signal": signal,
            "hist": hist,
        }

    @QtCore.Slot()
    def _on_refresh_clicked(self) -> None:
        self._series = self._gen_dummy_series()
        self._update_charts()
        self._update_quote_panel()

    def _update_charts(self) -> None:
        if self._pg is None or self._price_plot is None:
            return

        pg = self._pg
        assert pg is not None

        x = self._series["x"]
        close = self._series["close"]
        ma5 = self._series["ma5"]
        ma10 = self._series["ma10"]
        ma20 = self._series["ma20"]

        self._price_plot.clear()
        self._price_plot.plot(x, close, pen=pg.mkPen("#F2F2F7", width=1.2), name="收盘")
        self._price_plot.plot(x, ma5, pen=pg.mkPen("#FFD60A", width=1.0), name="MA5")
        self._price_plot.plot(x, ma10, pen=pg.mkPen("#32ADE6", width=1.0), name="MA10")
        self._price_plot.plot(x, ma20, pen=pg.mkPen("#BF5AF2", width=1.0), name="MA20")

        vol = self._series["volume"]
        self._vol_plot.clear()
        bars = pg.BarGraphItem(x=x, height=vol, width=0.85, brush=pg.mkBrush("#2DD4BF"))
        self._vol_plot.addItem(bars)

        self._macd_plot.clear()
        hist = self._series["hist"]
        macd = self._series["macd"]
        signal = self._series["signal"]
        hist_brush = pg.mkBrush("#FF453A")
        hist_bars = pg.BarGraphItem(x=x, height=hist, width=0.85, brush=hist_brush)
        self._macd_plot.addItem(hist_bars)
        self._macd_plot.plot(x, macd, pen=pg.mkPen("#FFD60A", width=1.0))
        self._macd_plot.plot(x, signal, pen=pg.mkPen("#32ADE6", width=1.0))

    def _update_quote_panel(self) -> None:
        close = self._series.get("close", [])
        vol = self._series.get("volume", [])
        if len(close) < 2:
            return

        last = float(close[-1])
        prev = float(close[-2])
        chg = last - prev
        pct = (chg / prev) * 100.0 if prev != 0 else 0.0

        up_color = "#FF453A"
        down_color = "#30D158"
        tone = up_color if chg >= 0 else down_color

        self._price.setText(f"{last:.2f}")
        self._change.setText(f"{chg:+.2f}  {pct:+.2f}%")
        self._price.setStyleSheet(f"color: {tone};")
        self._change.setStyleSheet(f"color: {tone};")
        dates = self._series.get("dates", [])
        if isinstance(dates, list) and dates:
            start = date.fromordinal(int(dates[0]))
            end = date.fromordinal(int(dates[-1]))
            self._range.setText(f"{start.isoformat()}  —  {end.isoformat()}")
        else:
            self._range.setText("--")

        def set_neutral(key: str, text: str) -> None:
            row = self._stat_rows[key]
            row.set_value(text)
            row.set_value_color(None)

        def set_signed(key: str, value: float, fmt: str = "{:+.2f}") -> None:
            row = self._stat_rows[key]
            row.set_value(fmt.format(value))
            row.set_value_color(up_color if value >= 0 else down_color)

        set_neutral("昨收", f"{prev:.2f}")
        open_v = float(prev + chg * 0.25)
        set_neutral("开盘", f"{open_v:.2f}")
        hi = float(max(close[-20:]))
        lo = float(min(close[-20:]))
        set_neutral("最高", f"{hi:.2f}")
        set_neutral("最低", f"{lo:.2f}")
        set_neutral("收盘", f"{last:.2f}")
        set_signed("涨跌", chg)
        set_signed("涨跌幅", pct, fmt="{:+.2f}%")
        set_neutral("成交量", f"{(vol[-1] / 10_000):.2f} 万")
        set_neutral("成交额", "--")
        d5 = (close[-1] / close[-5] - 1) * 100
        d20 = (close[-1] / close[-20] - 1) * 100
        set_signed("5日涨跌", float(d5), fmt="{:+.2f}%")
        set_signed("20日涨跌", float(d20), fmt="{:+.2f}%")
        set_neutral("年内涨跌", "--")
        set_neutral("20日波动", "--")


class WatchlistPage(QtWidgets.QWidget):
    def __init__(self) -> None:
        super().__init__()

        card = _Card("")
        table = _WatchTable()
        self._table = table

        rows: list[dict[str, object]] = [
            {
                "code": "000603",
                "name": "盛达资源",
                "tag": "R",
                "price": 49.00,
                "pct": -3.75,
                "chg": -1.91,
                "speed": 0.14,
                "turnover": 5.13,
                "watch_date": "20260302",
                "watch_price": 56.57,
            },
            {
                "code": "600685",
                "name": "中船防务",
                "tag": "R",
                "price": 35.68,
                "pct": 1.51,
                "chg": 0.53,
                "speed": 0.03,
                "turnover": 2.92,
                "watch_date": "20260218",
                "watch_price": 35.99,
            },
            {
                "code": "000554",
                "name": "泰山石油",
                "tag": "R",
                "price": 11.62,
                "pct": 7.10,
                "chg": 0.77,
                "speed": 0.09,
                "turnover": 46.74,
                "watch_date": "20260218",
                "watch_price": 7.93,
            },
        ]
        table.set_rows(rows)
        table.sortItems(10, QtCore.Qt.SortOrder.DescendingOrder)

        card.body.addWidget(table, 1)

        root = QtWidgets.QVBoxLayout()
        root.setContentsMargins(0, 0, 0, 0)
        root.addWidget(card, 1)
        self.setLayout(root)


class PlaceholderPage(QtWidgets.QWidget):
    def __init__(self, title: str, description: str) -> None:
        super().__init__()
        card = _Card(title, subtitle=description)
        hint = QtWidgets.QLabel("Coming soon.")
        hint.setObjectName("PlaceholderHint")
        hint.setText("即将推出。")
        card.body.addWidget(hint)
        card.body.addStretch(1)

        outer = QtWidgets.QVBoxLayout()
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(card, 0)
        outer.addStretch(1)
        self.setLayout(outer)


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, ctx: object) -> None:
        super().__init__()
        self.setWindowTitle("MoS Quant")
        self.resize(1040, 680)
        self._quick_input_last_focus: QtWidgets.QWidget | None = None
        self._stock_index: list[tuple[str, str, str, str]] = []
        self._quick_input_debounce = QtCore.QTimer(self)
        self._quick_input_debounce.setSingleShot(True)
        self._quick_input_debounce.setInterval(0)
        self._quick_input_pending_query = ""
        self._quick_input_last_query = ""

        try:
            df = getattr(ctx, "a_stock_list", None)
            if df is not None and hasattr(df, "iterrows"):
                for _idx, row in df.iterrows():
                    code = str(row.get("code", "")).strip()
                    name = str(row.get("name", "")).strip()
                    if not code or not name:
                        continue
                    name_norm = normalize_name(name)
                    abbr = name_to_abbr(name)
                    self._stock_index.append((code, name, name_norm, abbr))
        except Exception:
            self._stock_index = []

        # ---- sidebar ----
        sidebar = QtWidgets.QFrame()
        sidebar.setObjectName("Sidebar")
        sidebar.setFixedWidth(172)

        brand = QtWidgets.QLabel("MoS Quant")
        brand.setObjectName("Brand")

        nav = QtWidgets.QListWidget()
        nav.setObjectName("NavList")
        nav.setUniformItemSizes(True)
        nav.setSpacing(2)

        items = [
            ("仪表盘", "系统状态概览"),
            ("行情", "报价、图表与指标"),
            ("自选", "自选股票与快捷操作"),
            ("数据", "拉取与管理数据集"),
            ("设置", "偏好设置"),
        ]
        for label, tooltip in items:
            it = QtWidgets.QListWidgetItem(label)
            it.setToolTip(tooltip)
            nav.addItem(it)
        nav.setCurrentRow(0)

        sidebar_lay = QtWidgets.QVBoxLayout()
        sidebar_lay.setContentsMargins(10, 18, 10, 14)
        sidebar_lay.setSpacing(12)
        sidebar_lay.addWidget(brand)
        sidebar_lay.addWidget(nav, 1)
        sidebar.setLayout(sidebar_lay)

        # ---- pages ----
        pages = QtWidgets.QStackedWidget()
        pages.setObjectName("Pages")
        pages.addWidget(DashboardPage(ctx))
        pages.addWidget(MarketPage())
        pages.addWidget(WatchlistPage())
        pages.addWidget(PlaceholderPage("数据", "拉取与管理数据集"))
        pages.addWidget(PlaceholderPage("设置", "偏好设置"))

        def on_nav_changed(row: int) -> None:
            pages.setCurrentIndex(max(0, row))

        nav.currentRowChanged.connect(on_nav_changed)

        content = QtWidgets.QWidget()
        content_l = QtWidgets.QVBoxLayout()
        content_l.setContentsMargins(18, 16, 18, 18)
        content_l.setSpacing(14)
        content_l.addWidget(pages, 1)
        content.setLayout(content_l)

        # ---- root ----
        root = QtWidgets.QWidget()
        root_l = QtWidgets.QHBoxLayout()
        root_l.setContentsMargins(0, 0, 0, 0)
        root_l.setSpacing(0)
        root_l.addWidget(sidebar, 0)
        root_l.addWidget(content, 1)
        root.setLayout(root_l)
        self.setCentralWidget(root)

        # ---- quick input overlay (Enter to show, Esc to hide) ----
        self._quick_input = _QuickInputOverlay(root)
        self._quick_suggest = _QuickSuggestPopup(root)
        self._quick_suggest.activated.connect(self._quick_input.submitted.emit)
        self._quick_input.attach_suggest(self._quick_suggest)
        self._quick_input.submitted.connect(self._on_quick_input_submitted)
        self._quick_input.active_changed.connect(self._on_quick_input_active_changed)
        self._quick_input.query_changed.connect(self._on_quick_input_query_changed)
        self._quick_input_debounce.timeout.connect(self._refresh_quick_input_suggestions)
        self._reposition_quick_input()

        self._show_quick_input_return = QtGui.QShortcut(
            QtGui.QKeySequence(_qt_key("Key_Return")),
            self,
        )
        self._show_quick_input_return.setContext(QtCore.Qt.ApplicationShortcut)
        self._show_quick_input_return.activated.connect(self._show_quick_input)

        self._show_quick_input_enter = QtGui.QShortcut(
            QtGui.QKeySequence(_qt_key("Key_Enter")),
            self,
        )
        self._show_quick_input_enter.setContext(QtCore.Qt.ApplicationShortcut)
        self._show_quick_input_enter.activated.connect(self._show_quick_input)

        self._hide_quick_input_esc = QtGui.QShortcut(
            QtGui.QKeySequence(_qt_key("Key_Escape")),
            self,
        )
        self._hide_quick_input_esc.setContext(QtCore.Qt.ApplicationShortcut)
        self._hide_quick_input_esc.activated.connect(self._hide_quick_input)

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:  # noqa: N802
        super().resizeEvent(event)
        self._reposition_quick_input()

    def showEvent(self, event: QtGui.QShowEvent) -> None:  # noqa: N802
        super().showEvent(event)
        self._reposition_quick_input()

    def _reposition_quick_input(self) -> None:
        host = self.centralWidget()
        if host is None or not hasattr(self, "_quick_input"):
            return
        margin = 18
        w = self._quick_input.width()
        h = self._quick_input.height()
        x = max(0, host.width() - w - margin)
        y = max(0, host.height() - h - margin)
        self._quick_input.move(x, y)
        if hasattr(self, "_quick_suggest") and self._quick_suggest.isVisible():
            self._quick_suggest.show_for_anchor(self._quick_input)

    @QtCore.Slot()
    def _show_quick_input(self) -> None:
        if self._quick_input.isVisible():
            self._quick_input.show_and_focus()
            return
        self._quick_input_last_focus = QtWidgets.QApplication.focusWidget()
        self._reposition_quick_input()
        self._quick_input.show_and_focus()
        self._quick_input_pending_query = self._quick_input.text()
        self._quick_input_debounce.start()

    @QtCore.Slot()
    def _hide_quick_input(self) -> None:
        if not self._quick_input.isVisible():
            return
        self._quick_input.hide_overlay()
        self._quick_suggest.hide()
        self._quick_input_last_query = ""
        self._show_quick_input_return.setEnabled(True)
        self._show_quick_input_enter.setEnabled(True)
        if self._quick_input_last_focus is not None:
            try:
                self._quick_input_last_focus.setFocus(QtCore.Qt.ShortcutFocusReason)
            except Exception:
                pass
        self._quick_input_last_focus = None

    @QtCore.Slot(bool)
    def _on_quick_input_active_changed(self, active: bool) -> None:
        self._show_quick_input_return.setEnabled(not active)
        self._show_quick_input_enter.setEnabled(not active)
        if not active:
            self._quick_suggest.hide()
            return
        self._quick_input_pending_query = self._quick_input.text()
        self._quick_input_debounce.start()

    @QtCore.Slot(str)
    def _on_quick_input_submitted(self, _text: str) -> None:
        self._hide_quick_input()

    @QtCore.Slot(str)
    def _on_quick_input_query_changed(self, text: str) -> None:
        self._quick_input_pending_query = text
        self._quick_input_debounce.start()

    def _refresh_quick_input_suggestions(self) -> None:
        query = self._quick_input_pending_query.strip()
        if query == self._quick_input_last_query:
            return
        self._quick_input_last_query = query
        if not query or not self._stock_index:
            self._quick_suggest.hide()
            return
        suggestions = self._match_stock_suggestions(query, limit=12)
        if not suggestions:
            self._quick_suggest.hide()
            return
        self._quick_suggest.set_items(suggestions)
        self._quick_suggest.show_for_anchor(self._quick_input)

    def _match_stock_suggestions(self, query: str, *, limit: int = 12) -> list[str]:
        q = query.strip()
        q_norm = normalize_name(q)
        is_digits = q_norm.isdigit()
        is_abbr = (not is_digits) and is_abbr_query(q_norm)
        limit = max(1, int(limit))

        # Avoid huge candidate sets for very short numeric inputs like "0" or "00".
        if is_digits and len(q_norm) < 2:
            return []

        buckets: dict[int, list[tuple[str, str]]] = {0: [], 1: [], 2: [], 3: [], 4: [], 5: [], 6: []}
        if is_digits:
            in_prefix_block = False
            for code, name, name_norm, abbr in self._stock_index:
                _ = (name_norm, abbr)
                if code == q_norm:
                    buckets[0].append((code, name))
                    break
                if code.startswith(q_norm):
                    in_prefix_block = True
                    if len(buckets[1]) < limit:
                        buckets[1].append((code, name))
                    continue
                if in_prefix_block:
                    # Since list is sorted by code, prefix matches are contiguous.
                    # If we already have enough, don't scan the entire universe.
                    if buckets[1]:
                        break
                if q_norm in code:
                    if len(buckets[4]) < limit:
                        buckets[4].append((code, name))
                elif q_norm in name_norm:
                    if len(buckets[6]) < limit:
                        buckets[6].append((code, name))
        elif is_abbr:
            for code, name, name_norm, abbr in self._stock_index:
                _ = name_norm
                if not abbr:
                    continue
                if abbr.startswith(q_norm):
                    if len(buckets[2]) < limit:
                        buckets[2].append((code, name))
                elif q_norm in abbr:
                    if len(buckets[3]) < limit:
                        buckets[3].append((code, name))
                elif q_norm in code:
                    if len(buckets[5]) < limit:
                        buckets[5].append((code, name))
        else:
            for code, name, name_norm, abbr in self._stock_index:
                _ = abbr
                if name_norm.startswith(q_norm):
                    if len(buckets[2]) < limit:
                        buckets[2].append((code, name))
                elif q_norm in name_norm:
                    if len(buckets[3]) < limit:
                        buckets[3].append((code, name))
                elif q_norm in code:
                    if len(buckets[5]) < limit:
                        buckets[5].append((code, name))

        out: list[str] = []
        for k in (0, 1, 2, 3, 4, 5, 6):
            for code, name in buckets[k]:
                out.append(f"{code}  {name}")
                if len(out) >= limit:
                    return out
        return out
