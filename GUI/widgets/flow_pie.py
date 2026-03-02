from __future__ import annotations

from typing import Any

from PySide6.QtCore import QRectF, QSize, Qt
from PySide6.QtGui import QColor, QFontMetrics, QPainter
from PySide6.QtWidgets import QSizePolicy, QWidget


class FlowPieWidget(QWidget):
    """Donut chart + legend for fund-flow composition."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._segments: list[tuple[str, float, QColor]] = []
        self._rows: list[tuple[str, float, QColor]] = []
        self._total = 0.0
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setMinimumHeight(86)
        self.setMaximumHeight(110)

    def sizeHint(self) -> QSize:  # type: ignore[override]
        return QSize(180, 96)

    def minimumSizeHint(self) -> QSize:  # type: ignore[override]
        return QSize(140, 86)

    def set_flow_values(self, flow_values: dict[str, dict[str, Any]]) -> None:
        def pick_value(key: str) -> float:
            item = flow_values.get(key, {})
            if not isinstance(item, dict):
                return 0.0
            raw = item.get("value")
            try:
                return float(raw) if raw is not None else 0.0
            except Exception:
                return 0.0

        raw_values = [
            ("超大单", pick_value("超大单净流入")),
            ("大单", pick_value("大单净流入")),
            ("中单", pick_value("中单净流入")),
            ("小单", pick_value("小单净流入")),
        ]
        entries: list[tuple[str, float, QColor]] = []
        for idx, (name, value) in enumerate(raw_values):
            flow_name = f"{name}{'流入' if value >= 0 else '流出'}"
            amount = abs(value)
            if value >= 0:
                color = (QColor("#ff3f5c"), QColor("#ff6c84"), QColor("#d84864"), QColor("#ff8aa0"))[idx]
            else:
                color = (QColor("#18b57e"), QColor("#26c78f"), QColor("#1f9f73"), QColor("#177f5d"))[idx]
            entries.append((flow_name, amount, color))
        total = sum(value for _, value, _ in entries)
        self._segments = [(name, value, color) for name, value, color in entries if value > 0]
        if total > 0:
            self._rows = [(name, value / total * 100.0, color) for name, value, color in entries]
        else:
            self._rows = []
        self._total = total
        self.update()

    def paintEvent(self, event) -> None:  # type: ignore[override]
        del event
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing, True)

        rect = self.rect().adjusted(8, 8, -8, -8)
        if rect.width() <= 0 or rect.height() <= 0:
            return

        if not self._segments or self._total <= 0:
            painter.setPen(QColor("#8a93a6"))
            painter.drawText(rect, Qt.AlignCenter, "暂无资金结构")
            return

        pie_size = min(rect.height() - 10.0, 76.0, rect.width() * 0.34)
        pie_radius = pie_size * 0.5
        center_x = rect.left() + pie_radius + 6.0
        center_y = rect.center().y()
        pie_rect = QRectF(center_x - pie_radius, center_y - pie_radius, pie_size, pie_size)

        start_angle = 90 * 16
        for _, value, color in self._segments:
            span = int(round(-5760 * (value / self._total)))
            painter.setPen(Qt.NoPen)
            painter.setBrush(color)
            painter.drawPie(pie_rect, start_angle, span)
            start_angle += span

        hole = pie_rect.adjusted(pie_size * 0.28, pie_size * 0.28, -pie_size * 0.28, -pie_size * 0.28)
        painter.setBrush(QColor("#131924"))
        painter.setPen(QColor("#2b3345"))
        painter.drawEllipse(hole)

        legend_x = pie_rect.right() + 10.0
        legend_w = max(80.0, rect.right() - legend_x - 2.0)
        row_h = 18.0
        start_y = center_y - row_h * 2.0
        base_font = painter.font()
        base_font.setPixelSize(12)
        painter.setFont(base_font)
        metrics = QFontMetrics(base_font)
        for idx, (name, pct, color) in enumerate(self._rows):
            y = start_y + idx * row_h
            mark_rect = QRectF(legend_x, y + 3.0, 4.0, row_h - 6.0)
            muted = pct <= 0.001
            draw_color = QColor("#556077") if muted else color
            painter.setPen(Qt.NoPen)
            painter.setBrush(draw_color)
            painter.drawRect(mark_rect)

            pct_col_w = min(64.0, max(48.0, legend_w * 0.35))
            name_col_w = max(24.0, legend_w - 10.0 - pct_col_w)
            name_rect = QRectF(legend_x + 8.0, y, name_col_w, row_h)
            pct_rect = QRectF(name_rect.right() + 2.0, y, pct_col_w - 2.0, row_h)
            shown_name = metrics.elidedText(name, Qt.ElideRight, int(max(8.0, name_rect.width())))
            painter.setPen(QColor("#8e97ab") if muted else QColor("#cfd6e7"))
            painter.drawText(name_rect, Qt.AlignLeft | Qt.AlignVCenter, shown_name)
            painter.setPen(draw_color)
            painter.drawText(pct_rect, Qt.AlignRight | Qt.AlignVCenter, f"{pct:.1f}%")
