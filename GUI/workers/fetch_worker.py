from __future__ import annotations

import traceback

from PySide6.QtCore import QObject, Signal

from core.data_service import DataServiceError, FetchRequest
from core.unified_data_module import UnifiedDataModule


class FetchWorker(QObject):
    success = Signal(object)
    error = Signal(str)
    progress = Signal(int, str)
    finished = Signal()

    def __init__(self, request: FetchRequest):
        super().__init__()
        self._request = request

    def run(self) -> None:
        try:
            service = UnifiedDataModule.instance()
            response = service.fetch(self._request, progress_cb=self._emit_progress)
            self.success.emit(response)
        except DataServiceError as exc:
            self.error.emit(str(exc))
        except Exception:
            self.error.emit(traceback.format_exc())
        finally:
            self.finished.emit()

    def _emit_progress(self, percent: int, message: str) -> None:
        self.progress.emit(int(percent), str(message))
