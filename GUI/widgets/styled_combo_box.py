from __future__ import annotations

from PySide6.QtCore import QPointF, Qt
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import QComboBox, QStyle, QStyleOptionComboBox, QStylePainter


class StyledComboBox(QComboBox):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setCursor(Qt.PointingHandCursor)

    def paintEvent(self, event) -> None:  # type: ignore[override]
        option = QStyleOptionComboBox()
        self.initStyleOption(option)
        # Keep frame + text painting, suppress native drop-down square.
        option.subControls = QStyle.SC_ComboBoxFrame | QStyle.SC_ComboBoxEditField

        painter = QStylePainter(self)
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.drawComplexControl(QStyle.CC_ComboBox, option)
        painter.drawControl(QStyle.CE_ComboBoxLabel, option)
        self._draw_chevron(painter, option.rect, enabled=self.isEnabled())

    @staticmethod
    def _draw_chevron(painter: QStylePainter, rect, *, enabled: bool) -> None:
        color = QColor("#aeb9cf" if enabled else "#7e8596")
        pen = QPen(color)
        pen.setWidthF(1.6)
        pen.setCapStyle(Qt.RoundCap)
        pen.setJoinStyle(Qt.RoundJoin)
        painter.setPen(pen)
        painter.setBrush(Qt.NoBrush)

        right = float(rect.right() - 13)
        center_y = float(rect.center().y() + 0.5)
        size = 4.2
        p1 = QPointF(right - size, center_y - 2.0)
        p2 = QPointF(right, center_y + 2.0)
        p3 = QPointF(right + size, center_y - 2.0)
        painter.drawLine(p1, p2)
        painter.drawLine(p2, p3)
