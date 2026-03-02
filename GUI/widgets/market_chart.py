from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pyqtgraph as pg
from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QKeySequence, QPainter, QPicture, QShortcut
from PySide6.QtWidgets import QLabel, QVBoxLayout, QWidget


pg.setConfigOptions(antialias=False)


def _fmt_cn_volume_value(value: float | None) -> str:
    if value is None or not math.isfinite(float(value)):
        return ""
    v = float(value)
    abs_v = abs(v)
    if abs_v >= 50000000:
        num = f"{v / 100000000:.2f}".rstrip("0").rstrip(".")
        return f"{num}亿"
    if abs_v >= 10000:
        num = f"{v / 10000:.2f}".rstrip("0").rstrip(".")
        return f"{num}万"
    return f"{v:.0f}"


class _RightAnchoredViewBox(pg.ViewBox):
    """
    Trading-style x-axis behavior:
    - wheel zoom changes visible interval length
    - pan (mouse drag / keyboard) changes right-edge time
    - panning never changes interval length
    """

    def __init__(self) -> None:
        super().__init__(enableMenu=False)
        self._data_count = 0
        self._min_visible = 20
        self._visible = 60
        self._right_edge = -0.5
        self._updating = False
        self._dragging = False
        self._drag_last_x = 0.0
        self._window_changed_cb = None
        self.setMouseEnabled(x=False, y=False)

    def set_window_changed_callback(self, callback) -> None:
        self._window_changed_cb = callback

    def set_data_count(self, count: int) -> None:
        prev_count = self._data_count
        self._data_count = max(0, int(count))
        if self._data_count <= 0:
            self.setLimits(xMin=-1.0, xMax=1.0, minXRange=1.0, maxXRange=2.0)
            return
        min_range = float(min(self._min_visible, max(1, self._data_count)))
        max_range = float(max(1, self._data_count))
        self.setLimits(
            xMin=-0.5,
            xMax=float(self._data_count - 0.5),
            minXRange=min_range,
            maxXRange=max_range,
        )
        self._visible = max(1, min(self._visible, self._data_count))
        if prev_count <= 0:
            self._right_edge = float(self._data_count - 0.5)
        else:
            self._right_edge = min(float(self._data_count - 0.5), max(self._visible - 0.5, self._right_edge))
        self._apply_window()

    def reset_to_latest(self, preferred_visible: int = 60) -> None:
        if self._data_count <= 0:
            return
        self._visible = max(self._min_visible, min(int(preferred_visible), self._data_count))
        self._right_edge = float(self._data_count - 0.5)
        self._apply_window()

    def wheelEvent(self, ev, axis=None):  # type: ignore[override]
        if self._data_count <= 1:
            ev.accept()
            return
        step = max(4, int(self._visible * 0.12))
        delta = ev.delta() if hasattr(ev, "delta") else 0
        if delta > 0:
            self._visible = max(self._min_visible, self._visible - step)
        else:
            self._visible = min(self._data_count, self._visible + step)
        self._apply_window()
        ev.accept()

    def mouseDragEvent(self, ev, axis=None):  # type: ignore[override]
        if ev.button() != Qt.LeftButton or self._data_count <= 1:
            ev.ignore()
            return
        if ev.isStart():
            self._dragging = True
            self._drag_last_x = float(ev.pos().x())
            ev.accept()
            return
        if ev.isFinish():
            self._dragging = False
            ev.accept()
            return
        if not self._dragging:
            ev.accept()
            return
        current_x = float(ev.pos().x())
        dx = current_x - self._drag_last_x
        self._drag_last_x = current_x
        px = max(20.0, float(self.width()))
        shift = int(round((-dx / px) * self._visible))
        if shift != 0:
            self.pan_by(shift)
        ev.accept()

    def pan_by(self, bars: int) -> None:
        if self._data_count <= 0 or bars == 0:
            return
        right_max = float(self._data_count - 0.5)
        right_min = float(self._visible - 0.5)
        self._right_edge = min(right_max, max(right_min, self._right_edge + bars))
        self._apply_window()

    @property
    def visible_count(self) -> int:
        return int(self._visible)

    def _apply_window(self) -> None:
        if self._data_count <= 0:
            return
        right = min(float(self._data_count - 0.5), max(self._visible - 0.5, self._right_edge))
        left = right - float(self._visible)
        if left < -0.5:
            left = -0.5
            right = left + float(self._visible)
        self._right_edge = right
        self._updating = True
        try:
            self.setXRange(left, right, padding=0.0)
        finally:
            self._updating = False
        if self._window_changed_cb is not None:
            try:
                self._window_changed_cb(left, right)
            except Exception:
                pass


class CandlestickItem(pg.GraphicsObject):
    """Fast candlestick item rendered into a cached QPicture."""

    def __init__(
        self,
        x: np.ndarray,
        open_: np.ndarray,
        high: np.ndarray,
        low: np.ndarray,
        close: np.ndarray,
        up_mask: np.ndarray | None = None,
        *,
        body_width: float = 0.72,
        up_color: str = "#ef5350",
        down_color: str = "#26a69a",
    ) -> None:
        super().__init__()
        self._x = np.asarray(x, dtype=float)
        self._high = np.asarray(high, dtype=float)
        self._low = np.asarray(low, dtype=float)
        self._picture = QPicture()
        self._bounds = None
        self._build_picture(x, open_, high, low, close, up_mask, body_width, up_color, down_color)

    def _build_picture(
        self,
        x: np.ndarray,
        open_: np.ndarray,
        high: np.ndarray,
        low: np.ndarray,
        close: np.ndarray,
        up_mask: np.ndarray | None,
        body_width: float,
        up_color: str,
        down_color: str,
    ) -> None:
        painter = QPainter(self._picture)
        try:
            for i in range(len(x)):
                xi = float(x[i])
                oi = float(open_[i])
                hi = float(high[i])
                li = float(low[i])
                ci = float(close[i])
                if not (math.isfinite(oi) and math.isfinite(hi) and math.isfinite(li) and math.isfinite(ci)):
                    continue

                up = bool(up_mask[i]) if up_mask is not None else (ci >= oi)
                color = QColor(up_color if up else down_color)
                painter.setPen(pg.mkPen(color, width=1))
                painter.drawLine(pg.QtCore.QPointF(xi, li), pg.QtCore.QPointF(xi, hi))

                top = min(oi, ci)
                bottom = max(oi, ci)
                h = max(bottom - top, 1e-6)
                painter.setBrush(pg.mkBrush(color))
                painter.drawRect(pg.QtCore.QRectF(xi - body_width / 2.0, top, body_width, h))
        finally:
            painter.end()
        self._bounds = self._picture.boundingRect()

    def paint(self, painter: QPainter, *_args) -> None:  # type: ignore[override]
        painter.drawPicture(0, 0, self._picture)

    def boundingRect(self):  # type: ignore[override]
        if self._bounds is None:
            return pg.QtCore.QRectF()
        return pg.QtCore.QRectF(self._bounds)

    def dataBounds(self, ax: int, frac: float = 1.0, orthoRange=None):  # type: ignore[override]
        if self._x.size == 0:
            return None
        if ax == 0:
            return (float(self._x[0] - 0.5), float(self._x[-1] + 0.5))
        if ax == 1:
            if orthoRange is not None:
                x0, x1 = orthoRange
                mask = (self._x >= x0) & (self._x <= x1)
                if np.any(mask):
                    lows = self._low[mask]
                    highs = self._high[mask]
                    lows = lows[np.isfinite(lows)]
                    highs = highs[np.isfinite(highs)]
                    if lows.size > 0 and highs.size > 0:
                        return (float(np.min(lows)), float(np.max(highs)))
            lows = self._low[np.isfinite(self._low)]
            highs = self._high[np.isfinite(self._high)]
            if lows.size > 0 and highs.size > 0:
                return (float(np.min(lows)), float(np.max(highs)))
            return None
        return None


class MarketChartWidget(QWidget):
    MAX_STORED_BARS = 30000

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("marketChart")
        self.setMinimumHeight(560)

        self._show_macd = True
        self._show_kdj = False
        self._show_rsi = False
        self._show_boll = False
        self._main_series_mode = "candle"

        self._safe_df = pd.DataFrame()
        self._default_visible_bars = 60
        self._x = np.array([], dtype=float)
        self._opens = np.array([], dtype=float)
        self._highs = np.array([], dtype=float)
        self._lows = np.array([], dtype=float)
        self._closes = np.array([], dtype=float)
        self._volumes = np.array([], dtype=float)
        self._ma5 = np.array([], dtype=float)
        self._ma10 = np.array([], dtype=float)
        self._ma20 = np.array([], dtype=float)
        self._boll_up = np.array([], dtype=float)
        self._boll_mid = np.array([], dtype=float)
        self._boll_low = np.array([], dtype=float)
        self._macd_dif = np.array([], dtype=float)
        self._macd_dea = np.array([], dtype=float)
        self._macd_hist = np.array([], dtype=float)
        self._kdj_k = np.array([], dtype=float)
        self._kdj_d = np.array([], dtype=float)
        self._kdj_j = np.array([], dtype=float)
        self._rsi14 = np.array([], dtype=float)
        self._price_plot: pg.PlotItem | None = None
        self._price_vb: _RightAnchoredViewBox | None = None
        self._price_info_item: pg.TextItem | None = None
        self._vol_plot: pg.PlotItem | None = None
        self._macd_plot: pg.PlotItem | None = None
        self._kdj_plot: pg.PlotItem | None = None
        self._rsi_plot: pg.PlotItem | None = None
        self._layout_signature: tuple[bool, bool, bool] | None = None
        self._date_axes: list[_DateAxis] = []

        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)
        self._layout.setSpacing(0)

        self._canvas = pg.GraphicsLayoutWidget()
        self._canvas.setBackground("#151821")
        self._layout.addWidget(self._canvas)

        self._empty_hint = QLabel("暂无行情数据")
        self._empty_hint.setAlignment(Qt.AlignCenter)
        self._empty_hint.setStyleSheet("color:#7f8798; font-size:13px;")
        self._layout.addWidget(self._empty_hint)
        self._empty_hint.hide()

        self.setFocusPolicy(Qt.StrongFocus)
        self._left_shortcut = QShortcut(QKeySequence(Qt.Key_Left), self)
        self._left_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self._left_shortcut.activated.connect(self._pan_left)
        self._right_shortcut = QShortcut(QKeySequence(Qt.Key_Right), self)
        self._right_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self._right_shortcut.activated.connect(self._pan_right)

        self._render_plots()

    def set_indicator_visibility(
        self,
        *,
        macd: bool,
        kdj: bool,
        rsi: bool,
        boll: bool,
    ) -> None:
        self._show_macd = macd
        self._show_kdj = kdj
        self._show_rsi = rsi
        self._show_boll = boll
        self._render_plots()

    def set_main_series_mode(self, mode: str) -> None:
        normalized = "line" if str(mode).lower() == "line" else "candle"
        if normalized == self._main_series_mode:
            return
        self._main_series_mode = normalized
        self._render_plots()

    def set_dataframe(self, df: pd.DataFrame) -> None:
        self._safe_df = self._normalize_dataframe(df)
        self._default_visible_bars = int(self._safe_df.attrs.get("default_visible_bars", 60))
        self._render_plots()

    def _render_plots(self) -> None:
        if self._safe_df.empty:
            self._canvas.clear()
            self._layout_signature = None
            self._date_axes = []
            self._price_plot = None
            self._price_vb = None
            self._price_info_item = None
            self._vol_plot = None
            self._macd_plot = None
            self._kdj_plot = None
            self._rsi_plot = None
            self._canvas.hide()
            self._empty_hint.show()
            return

        self._canvas.show()
        self._empty_hint.hide()

        x = np.arange(len(self._safe_df), dtype=float)
        self._x = x
        opens = self._safe_df["_open"].to_numpy(dtype=float)
        highs = self._safe_df["_high"].to_numpy(dtype=float)
        lows = self._safe_df["_low"].to_numpy(dtype=float)
        closes = self._safe_df["_close"].to_numpy(dtype=float)
        volumes = self._safe_df["_volume"].to_numpy(dtype=float)
        self._opens = opens
        self._highs = highs
        self._lows = lows
        self._closes = closes
        self._volumes = volumes
        dates = self._safe_df["_label_date"].tolist()
        self._ensure_plot_layout(dates)
        if self._price_plot is None or self._price_vb is None or self._vol_plot is None:
            return

        price_plot = self._price_plot
        price_vb = self._price_vb
        vol_plot = self._vol_plot
        for plot in (self._price_plot, self._vol_plot, self._macd_plot, self._kdj_plot, self._rsi_plot):
            if plot is not None:
                plot.clear()

        self._price_info_item = pg.TextItem(anchor=(0, 0))
        self._price_info_item.setZValue(20)
        price_plot.addItem(self._price_info_item, ignoreBounds=True)

        line_mode = self._main_series_mode == "line"
        up_mask = self._calc_price_up_mask(opens, closes)
        if line_mode:
            price_plot.plot(
                x,
                closes,
                pen=pg.mkPen("#dfe6f8", width=1.25),
                connect="finite",
            )
        else:
            candle_item = CandlestickItem(x, opens, highs, lows, closes, up_mask=up_mask)
            price_plot.addItem(candle_item)

        ma5 = self._safe_df["_ma5"].to_numpy(dtype=float)
        ma10 = self._safe_df["_ma10"].to_numpy(dtype=float)
        ma20 = self._safe_df["_ma20"].to_numpy(dtype=float)
        self._ma5 = ma5
        self._ma10 = ma10
        self._ma20 = ma20
        price_plot.plot(
            x,
            ma5,
            pen=pg.mkPen("#f7c948", width=1.2),
            connect="finite",
        )
        price_plot.plot(
            x,
            ma10,
            pen=pg.mkPen("#5aa9ff", width=1.2),
            connect="finite",
        )
        price_plot.plot(
            x,
            ma20,
            pen=pg.mkPen("#c084fc", width=1.2),
            connect="finite",
        )

        if self._show_boll:
            boll_up = self._safe_df["_boll_up"].to_numpy(dtype=float)
            boll_mid = self._safe_df["_boll_mid"].to_numpy(dtype=float)
            boll_low = self._safe_df["_boll_low"].to_numpy(dtype=float)
            self._boll_up = boll_up
            self._boll_mid = boll_mid
            self._boll_low = boll_low
            boll_up_curve = price_plot.plot(
                x,
                boll_up,
                pen=pg.mkPen("#7cd6ff", width=1.35),
                connect="finite",
            )
            price_plot.plot(
                x,
                boll_mid,
                pen=pg.mkPen("#ffb86b", width=1.25),
                connect="finite",
            )
            boll_low_curve = price_plot.plot(
                x,
                boll_low,
                pen=pg.mkPen("#5d8bff", width=1.35),
                connect="finite",
            )
            price_plot.addItem(
                pg.FillBetweenItem(
                    boll_up_curve,
                    boll_low_curve,
                    brush=pg.mkBrush(110, 160, 255, 26),
                )
            )
        else:
            self._boll_up = np.full_like(x, np.nan, dtype=float)
            self._boll_mid = np.full_like(x, np.nan, dtype=float)
            self._boll_low = np.full_like(x, np.nan, dtype=float)

        down_mask = ~up_mask

        if np.any(up_mask):
            vol_plot.addItem(
                pg.BarGraphItem(
                    x=x[up_mask],
                    height=volumes[up_mask],
                    y0=np.zeros(np.count_nonzero(up_mask)),
                    width=0.72,
                    brush=pg.mkBrush("#ef5350"),
                    pen=pg.mkPen("#ef5350", width=0),
                )
            )
        if np.any(down_mask):
            vol_plot.addItem(
                pg.BarGraphItem(
                    x=x[down_mask],
                    height=volumes[down_mask],
                    y0=np.zeros(np.count_nonzero(down_mask)),
                    width=0.72,
                    brush=pg.mkBrush("#26a69a"),
                    pen=pg.mkPen("#26a69a", width=0),
                )
            )

        indicator_plots = []
        if self._show_macd:
            macd_plot = self._macd_plot
            if macd_plot is None:
                return
            dif = self._safe_df["_macd_dif"].to_numpy(dtype=float)
            dea = self._safe_df["_macd_dea"].to_numpy(dtype=float)
            hist = self._safe_df["_macd_hist"].to_numpy(dtype=float)
            self._macd_dif = dif
            self._macd_dea = dea
            self._macd_hist = hist
            hist_up = hist >= 0
            hist_down = ~hist_up
            if np.any(hist_up):
                macd_plot.addItem(
                    pg.BarGraphItem(
                        x=x[hist_up],
                        height=hist[hist_up],
                        y0=np.zeros(np.count_nonzero(hist_up)),
                        width=0.72,
                        brush=pg.mkBrush("#ef5350"),
                        pen=pg.mkPen("#ef5350", width=0),
                    )
                )
            if np.any(hist_down):
                macd_plot.addItem(
                    pg.BarGraphItem(
                        x=x[hist_down],
                        height=hist[hist_down],
                        y0=np.zeros(np.count_nonzero(hist_down)),
                        width=0.72,
                        brush=pg.mkBrush("#26a69a"),
                        pen=pg.mkPen("#26a69a", width=0),
                    )
                )
            macd_plot.plot(
                x,
                dif,
                pen=pg.mkPen("#f7c948", width=1.1),
                connect="finite",
            )
            macd_plot.plot(
                x,
                dea,
                pen=pg.mkPen("#4bb3ff", width=1.1),
                connect="finite",
            )
            macd_plot.addLine(y=0.0, pen=pg.mkPen("#5e6472", width=1))
            indicator_plots.append(macd_plot)

        if self._show_kdj:
            kdj_plot = self._kdj_plot
            if kdj_plot is None:
                return
            k = self._safe_df["_kdj_k"].to_numpy(dtype=float)
            d = self._safe_df["_kdj_d"].to_numpy(dtype=float)
            j = self._safe_df["_kdj_j"].to_numpy(dtype=float)
            self._kdj_k = k
            self._kdj_d = d
            self._kdj_j = j
            kdj_plot.plot(
                x,
                k,
                pen=pg.mkPen("#ffd166", width=1.1),
                connect="finite",
            )
            kdj_plot.plot(
                x,
                d,
                pen=pg.mkPen("#4bb3ff", width=1.1),
                connect="finite",
            )
            kdj_plot.plot(
                x,
                j,
                pen=pg.mkPen("#c084fc", width=1.1),
                connect="finite",
            )
            kdj_plot.addLine(y=50.0, pen=pg.mkPen("#5e6472", width=1))
            indicator_plots.append(kdj_plot)

        if self._show_rsi:
            rsi_plot = self._rsi_plot
            if rsi_plot is None:
                return
            rsi = self._safe_df["_rsi14"].to_numpy(dtype=float)
            self._rsi14 = rsi
            rsi_plot.plot(
                x,
                rsi,
                pen=pg.mkPen("#f7c948", width=1.1),
                connect="finite",
            )
            rsi_plot.addLine(y=70.0, pen=pg.mkPen("#5e6472", width=1))
            rsi_plot.addLine(y=30.0, pen=pg.mkPen("#5e6472", width=1))
            indicator_plots.append(rsi_plot)
        else:
            self._rsi14 = np.full_like(x, np.nan, dtype=float)

        if not self._show_macd:
            self._macd_dif = np.full_like(x, np.nan, dtype=float)
            self._macd_dea = np.full_like(x, np.nan, dtype=float)
            self._macd_hist = np.full_like(x, np.nan, dtype=float)
        if not self._show_kdj:
            self._kdj_k = np.full_like(x, np.nan, dtype=float)
            self._kdj_d = np.full_like(x, np.nan, dtype=float)
            self._kdj_j = np.full_like(x, np.nan, dtype=float)

        # Only show x-axis on the last panel.
        last_plot = indicator_plots[-1] if indicator_plots else vol_plot
        last_plot.getAxis("bottom").setStyle(showValues=True)

        # Keep latest bar at the right edge, and zoom only by changing left edge.
        price_vb.set_data_count(len(x))
        price_vb.reset_to_latest(preferred_visible=self._default_visible_bars)

    def _ensure_plot_layout(self, dates: list[str]) -> None:
        signature = (self._show_macd, self._show_kdj, self._show_rsi)
        layout_ready = (
            self._layout_signature == signature
            and self._price_plot is not None
            and self._price_vb is not None
            and self._vol_plot is not None
            and (not self._show_macd or self._macd_plot is not None)
            and (not self._show_kdj or self._kdj_plot is not None)
            and (not self._show_rsi or self._rsi_plot is not None)
        )
        if layout_ready:
            for axis in self._date_axes:
                axis.set_labels(dates)
            return

        self._canvas.clear()
        self._layout_signature = signature
        self._date_axes = []
        self._price_plot = None
        self._price_vb = None
        self._price_info_item = None
        self._vol_plot = None
        self._macd_plot = None
        self._kdj_plot = None
        self._rsi_plot = None

        last_panel = "vol"
        if self._show_macd:
            last_panel = "macd"
        if self._show_kdj:
            last_panel = "kdj"
        if self._show_rsi:
            last_panel = "rsi"

        def _bottom_axis(panel: str):
            if panel == last_panel:
                axis = _DateAxis(dates)
                self._date_axes.append(axis)
                return axis
            return pg.AxisItem(orientation="bottom")

        price_axis = _bottom_axis("price")
        price_vb = _RightAnchoredViewBox()
        price_vb.set_window_changed_callback(self._on_window_changed)
        price_plot = self._canvas.addPlot(row=0, col=0, viewBox=price_vb, axisItems={"bottom": price_axis})
        self._setup_plot(price_plot, show_x=False, interactive=True)
        price_plot.setLabel("left", "价格", color="#9ca6bb")
        self._price_plot = price_plot
        self._price_vb = price_vb

        self._canvas.nextRow()
        vol_axis = _bottom_axis("vol")
        vol_left_axis = _VolumeAxis(orientation="left")
        vol_plot = self._canvas.addPlot(
            row=1,
            col=0,
            axisItems={"bottom": vol_axis, "left": vol_left_axis},
        )
        self._setup_plot(vol_plot, show_x=False, interactive=False)
        vol_plot.setMaximumHeight(120)
        vol_plot.setXLink(price_plot)
        vol_plot.setLabel("left", "成交量", color="#9ca6bb")
        self._vol_plot = vol_plot

        indicator_plots: list[pg.PlotItem] = []
        if self._show_macd:
            self._canvas.nextRow()
            macd_axis = _bottom_axis("macd")
            macd_plot = self._canvas.addPlot(row=2 + len(indicator_plots), col=0, axisItems={"bottom": macd_axis})
            self._setup_plot(macd_plot, show_x=False, interactive=False)
            macd_plot.setMaximumHeight(120)
            macd_plot.setXLink(price_plot)
            macd_plot.setLabel("left", "MACD", color="#9ca6bb")
            self._macd_plot = macd_plot
            indicator_plots.append(macd_plot)

        if self._show_kdj:
            self._canvas.nextRow()
            kdj_axis = _bottom_axis("kdj")
            kdj_plot = self._canvas.addPlot(row=2 + len(indicator_plots), col=0, axisItems={"bottom": kdj_axis})
            self._setup_plot(kdj_plot, show_x=False, interactive=False)
            kdj_plot.setMaximumHeight(120)
            kdj_plot.setXLink(price_plot)
            kdj_plot.setLabel("left", "KDJ", color="#9ca6bb")
            self._kdj_plot = kdj_plot
            indicator_plots.append(kdj_plot)

        if self._show_rsi:
            self._canvas.nextRow()
            rsi_axis = _bottom_axis("rsi")
            rsi_plot = self._canvas.addPlot(row=2 + len(indicator_plots), col=0, axisItems={"bottom": rsi_axis})
            self._setup_plot(rsi_plot, show_x=False, interactive=False)
            rsi_plot.setMaximumHeight(120)
            rsi_plot.setXLink(price_plot)
            rsi_plot.setLabel("left", "RSI", color="#9ca6bb")
            self._rsi_plot = rsi_plot
            indicator_plots.append(rsi_plot)

        last_plot = indicator_plots[-1] if indicator_plots else vol_plot
        last_plot.getAxis("bottom").setStyle(showValues=True)

    @staticmethod
    def _last_finite(values: np.ndarray) -> float | None:
        if values.size == 0:
            return None
        finite = values[np.isfinite(values)]
        if finite.size == 0:
            return None
        return float(finite[-1])

    @staticmethod
    def _fmt_num(value: float | None, digits: int = 2) -> str:
        if value is None:
            return "--"
        return f"{value:.{digits}f}"

    @staticmethod
    def _fmt_short_volume(value: float | None) -> str:
        if value is None:
            return "--"
        text = _fmt_cn_volume_value(value)
        return text if text else "--"

    @staticmethod
    def _calc_price_up_mask(opens: np.ndarray, closes: np.ndarray) -> np.ndarray:
        n = int(min(opens.size, closes.size))
        if n <= 0:
            return np.zeros(0, dtype=bool)
        o = np.asarray(opens[:n], dtype=float)
        c = np.asarray(closes[:n], dtype=float)
        up = c > o
        down = c < o
        flat = ~(up | down)
        if np.any(flat):
            prev_close = np.roll(c, 1)
            prev_close[0] = o[0]
            up_flat = c >= prev_close
            up = up | (flat & up_flat)
        return up

    @staticmethod
    def _setup_plot(plot: pg.PlotItem, *, show_x: bool, interactive: bool) -> None:
        plot.showGrid(x=True, y=True, alpha=0.14)
        plot.getAxis("left").setPen(pg.mkPen("#768097"))
        plot.getAxis("left").setTextPen(pg.mkPen("#9ca6bb"))
        plot.getAxis("bottom").setPen(pg.mkPen("#768097"))
        plot.getAxis("bottom").setTextPen(pg.mkPen("#9ca6bb"))
        plot.getAxis("bottom").setStyle(showValues=show_x)
        plot.getViewBox().setMouseEnabled(x=interactive, y=False)
        plot.setClipToView(True)
        plot.setDownsampling(mode="peak")
        plot.setMenuEnabled(False)
        plot.hideButtons()
        plot.enableAutoRange(x=False, y=False)

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
        self.setFocus(Qt.MouseFocusReason)
        super().mousePressEvent(event)

    @staticmethod
    def _visible_slice(left: float, right: float, length: int) -> slice:
        if length <= 0:
            return slice(0, 0)
        start = max(0, int(math.floor(left + 0.5)))
        end = min(length, int(math.ceil(right + 0.5)))
        if end <= start:
            end = min(length, start + 1)
        return slice(start, end)

    @staticmethod
    def _merge_finite(arrays: list[np.ndarray], extra: tuple[float, ...] = ()) -> np.ndarray:
        chunks: list[np.ndarray] = []
        for arr in arrays:
            if arr.size == 0:
                continue
            finite = arr[np.isfinite(arr)]
            if finite.size > 0:
                chunks.append(finite)
        if extra:
            chunks.append(np.asarray(extra, dtype=float))
        if not chunks:
            return np.array([], dtype=float)
        return np.concatenate(chunks)

    @classmethod
    def _apply_plot_y_range(
        cls,
        plot: pg.PlotItem | None,
        arrays: list[np.ndarray],
        *,
        extra: tuple[float, ...] = (),
        margin_ratio: float = 0.08,
    ) -> None:
        if plot is None:
            return
        merged = cls._merge_finite(arrays, extra=extra)
        if merged.size == 0:
            return
        y_min = float(np.min(merged))
        y_max = float(np.max(merged))
        span = max(1e-6, y_max - y_min)
        margin = max(span * margin_ratio, 1e-6)
        plot.setYRange(y_min - margin, y_max + margin, padding=0.0)

    def _on_window_changed(self, left: float, right: float) -> None:
        count = len(self._x)
        if count <= 0:
            return
        s = self._visible_slice(left, right, count)

        price_arrays = [
            self._closes[s],
            self._ma5[s],
            self._ma10[s],
            self._ma20[s],
            self._boll_up[s],
            self._boll_mid[s],
            self._boll_low[s],
        ]
        if self._main_series_mode != "line":
            price_arrays = [
                self._opens[s],
                self._highs[s],
                self._lows[s],
                *price_arrays,
            ]
        self._apply_plot_y_range(
            self._price_plot,
            price_arrays,
            margin_ratio=0.09,
        )
        self._apply_plot_y_range(
            self._vol_plot,
            [self._volumes[s]],
            extra=(0.0,),
            margin_ratio=0.12,
        )
        self._apply_plot_y_range(
            self._macd_plot,
            [self._macd_hist[s], self._macd_dif[s], self._macd_dea[s]],
            extra=(0.0,),
            margin_ratio=0.12,
        )
        self._apply_plot_y_range(
            self._kdj_plot,
            [self._kdj_k[s], self._kdj_d[s], self._kdj_j[s]],
            extra=(0.0, 100.0),
            margin_ratio=0.08,
        )
        self._apply_plot_y_range(
            self._rsi_plot,
            [self._rsi14[s]],
            extra=(0.0, 100.0),
            margin_ratio=0.08,
        )
        self._update_price_info_overlay(left, right)

    @staticmethod
    def _value_at(values: np.ndarray, idx: int) -> float | None:
        if idx < 0 or idx >= values.size:
            return None
        value = float(values[idx])
        if not math.isfinite(value):
            return None
        return value

    def _update_price_info_overlay(self, left: float, right: float) -> None:
        if self._price_plot is None or self._price_info_item is None:
            return
        if len(self._x) <= 0:
            self._price_info_item.setHtml("")
            return
        right_idx = max(0, min(len(self._x) - 1, int(math.floor(right))))
        ma5 = self._fmt_num(self._value_at(self._ma5, right_idx))
        ma10 = self._fmt_num(self._value_at(self._ma10, right_idx))
        ma20 = self._fmt_num(self._value_at(self._ma20, right_idx))
        main_label = "走势" if self._main_series_mode == "line" else "K线"
        text = (
            f"<span style='color:#d7dcea;'>{main_label}</span>"
            f"  <span style='color:#f7c948;'>MA5 {ma5}</span>"
            f"  <span style='color:#5aa9ff;'>MA10 {ma10}</span>"
            f"  <span style='color:#c084fc;'>MA20 {ma20}</span>"
        )
        if self._show_boll:
            boll_up = self._fmt_num(self._value_at(self._boll_up, right_idx))
            boll_mid = self._fmt_num(self._value_at(self._boll_mid, right_idx))
            boll_low = self._fmt_num(self._value_at(self._boll_low, right_idx))
            text += (
                f"  <span style='color:#7cd6ff;'>BOLL↑ {boll_up}</span>"
                f"  <span style='color:#ffb86b;'>BOLL中 {boll_mid}</span>"
                f"  <span style='color:#5d8bff;'>BOLL↓ {boll_low}</span>"
            )
        self._price_info_item.setHtml(f"<span style='font-size:11px;'>{text}</span>")
        x_range, y_range = self._price_plot.viewRange()
        x_left = float(x_range[0])
        y_top = float(y_range[1])
        y_bottom = float(y_range[0])
        y_span = max(1e-6, y_top - y_bottom)
        self._price_info_item.setPos(x_left + 0.7, y_top - y_span * 0.02)

    def _pan_left(self) -> None:
        if self._price_vb is None:
            return
        step = max(1, int(round(self._price_vb.visible_count * 0.14)))
        self._price_vb.pan_by(-step)

    def _pan_right(self) -> None:
        if self._price_vb is None:
            return
        step = max(1, int(round(self._price_vb.visible_count * 0.14)))
        self._price_vb.pan_by(step)

    @classmethod
    def _normalize_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        open_col = cls._pick_column(df, ("开盘", "open", "open_price"))
        high_col = cls._pick_column(df, ("最高", "high", "high_price"))
        low_col = cls._pick_column(df, ("最低", "low", "low_price"))
        close_col = cls._pick_column(df, ("收盘", "收盘价", "close", "latest"))
        vol_col = cls._pick_column(df, ("成交量", "volume", "vol"))
        amount_col = cls._pick_column(df, ("成交额", "amount", "turnover"))
        date_col = cls._pick_column(df, ("日期", "时间", "date", "datetime", "trade_date"))

        if close_col is None:
            return pd.DataFrame()

        safe = df.copy()
        safe["_close"] = cls._coerce_numeric(safe[close_col])
        safe["_open"] = cls._coerce_numeric(safe[open_col]) if open_col else safe["_close"]
        safe["_high"] = cls._coerce_numeric(safe[high_col]) if high_col else safe["_close"]
        safe["_low"] = cls._coerce_numeric(safe[low_col]) if low_col else safe["_close"]
        if vol_col:
            safe["_volume"] = cls._coerce_numeric(safe[vol_col]).fillna(0.0)
        elif amount_col:
            amount_series = cls._coerce_numeric(safe[amount_col])
            if cls._looks_like_volume_series(amount_series):
                safe["_volume"] = amount_series.fillna(0.0)
            else:
                denom = (safe["_close"].abs() * 100.0).replace(0.0, np.nan)
                est_volume = (amount_series / denom).replace([np.inf, -np.inf], np.nan)
                safe["_volume"] = est_volume.fillna(0.0)
        else:
            safe["_volume"] = 0.0

        safe[["_open", "_high", "_low", "_close", "_volume"]] = safe[[
            "_open",
            "_high",
            "_low",
            "_close",
            "_volume",
        ]].replace([np.inf, -np.inf], np.nan)
        safe = safe.dropna(subset=["_open", "_high", "_low", "_close"]).copy()
        if safe.empty:
            return pd.DataFrame()

        # Some minute feeds return open=0 for many rows; treat as missing and backfill from close.
        open_zero_mask = (safe["_open"] <= 0) & (safe["_close"] > 0)
        if open_zero_mask.any():
            safe.loc[open_zero_mask, "_open"] = safe.loc[open_zero_mask, "_close"]
        high_bad_mask = (safe["_high"] <= 0) & (safe["_close"] > 0)
        if high_bad_mask.any():
            safe.loc[high_bad_mask, "_high"] = safe.loc[high_bad_mask, "_close"]
        low_bad_mask = (safe["_low"] <= 0) & (safe["_close"] > 0)
        if low_bad_mask.any():
            safe.loc[low_bad_mask, "_low"] = safe.loc[low_bad_mask, "_close"]
        # Keep OHLC internally consistent after fallback.
        safe["_high"] = safe[["_high", "_open", "_close"]].max(axis=1)
        safe["_low"] = safe[["_low", "_open", "_close"]].min(axis=1)

        if date_col is None:
            safe["_label_date"] = [str(i + 1) for i in range(len(safe.index))]
            safe["_ts"] = pd.RangeIndex(start=0, stop=len(safe.index), step=1)
        else:
            safe["_label_date"] = safe[date_col].astype(str)
            ts = pd.to_datetime(safe[date_col], errors="coerce")
            if ts.notna().any():
                safe["_ts"] = ts
            else:
                safe["_ts"] = pd.RangeIndex(start=0, stop=len(safe.index), step=1)

        safe = safe.sort_values(by="_ts", kind="stable")
        safe = safe.drop_duplicates(subset=["_ts"], keep="last")
        safe = safe.tail(cls.MAX_STORED_BARS).reset_index(drop=True)
        safe = safe[(safe["_close"] > 0) & np.isfinite(safe["_close"])].copy()
        safe["_volume"] = safe["_volume"].fillna(0.0).clip(lower=0.0)
        if safe.empty:
            return pd.DataFrame()

        default_visible_bars = 60
        ts_series = safe["_ts"]
        if pd.api.types.is_datetime64_any_dtype(ts_series):
            max_ts = pd.to_datetime(ts_series, errors="coerce").max()
            if pd.notna(max_ts):
                cutoff = max_ts - pd.Timedelta(days=60)
                count = int((pd.to_datetime(ts_series, errors="coerce") >= cutoff).sum())
                if count > 0:
                    default_visible_bars = max(30, min(len(safe.index), count))
        safe.attrs["default_visible_bars"] = default_visible_bars

        close_series = safe["_close"]
        high_series = safe["_high"]
        low_series = safe["_low"]

        # Keep indicators continuous across the sorted series to align with common trading software.
        safe["_ma5"] = close_series.rolling(window=5, min_periods=5).mean()
        safe["_ma10"] = close_series.rolling(window=10, min_periods=10).mean()
        safe["_ma20"] = close_series.rolling(window=20, min_periods=20).mean()

        mid = close_series.rolling(window=20, min_periods=1).mean()
        std = close_series.rolling(window=20, min_periods=1).std(ddof=0)
        safe["_boll_mid"] = mid
        safe["_boll_up"] = mid + std * 2
        safe["_boll_low"] = mid - std * 2

        # MACD should be continuous on the full close series to match common trading software.
        ema12 = close_series.ewm(span=12, adjust=False).mean()
        ema26 = close_series.ewm(span=26, adjust=False).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9, adjust=False).mean()
        safe["_macd_dif"] = dif
        safe["_macd_dea"] = dea
        safe["_macd_hist"] = (dif - dea) * 2

        low_n = low_series.rolling(window=9, min_periods=1).min()
        high_n = high_series.rolling(window=9, min_periods=1).max()
        denom = (high_n - low_n).replace(0, np.nan)
        rsv = ((close_series - low_n) / denom * 100).fillna(50.0)
        k = rsv.ewm(com=2, adjust=False).mean()
        d = k.ewm(com=2, adjust=False).mean()
        safe["_kdj_k"] = k
        safe["_kdj_d"] = d
        safe["_kdj_j"] = 3 * k - 2 * d

        delta = close_series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        safe["_rsi14"] = (100 - (100 / (1 + rs))).fillna(50.0)

        return safe

    @staticmethod
    def _coerce_numeric(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")

    @staticmethod
    def _looks_like_volume_series(series: pd.Series) -> bool:
        clean = pd.to_numeric(series, errors="coerce").dropna()
        if clean.empty:
            return False
        med = abs(float(clean.median()))
        p95 = abs(float(clean.quantile(0.95)))
        return med <= 2e7 and p95 <= 2e8

    @staticmethod
    def _pick_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
        direct = {str(col): col for col in df.columns}
        lower = {str(col).lower(): col for col in df.columns}
        for name in candidates:
            if name in direct:
                return str(direct[name])
        for name in candidates:
            mapped = lower.get(name.lower())
            if mapped is not None:
                return str(mapped)
        return None


class _DateAxis(pg.AxisItem):
    def __init__(self, labels: list[str]) -> None:
        super().__init__(orientation="bottom")
        self._labels = labels

    def set_labels(self, labels: list[str]) -> None:
        self._labels = labels
        self.picture = None
        self.update()

    def tickStrings(self, values, scale, spacing):  # type: ignore[override]
        out: list[str] = []
        size = len(self._labels)
        for value in values:
            i = int(round(value))
            if 0 <= i < size:
                out.append(self._labels[i])
            else:
                out.append("")
        return out


class _VolumeAxis(pg.AxisItem):
    def tickStrings(self, values, scale, spacing):  # type: ignore[override]
        out: list[str] = []
        for value in values:
            try:
                out.append(_fmt_cn_volume_value(float(value)))
            except Exception:
                out.append("")
        return out
