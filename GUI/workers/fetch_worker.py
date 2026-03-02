from __future__ import annotations

import threading
import time
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
        self._progress_lock = threading.Lock()
        self._last_percent = 0
        self._last_message = "准备下载"
        self._last_progress_ts = 0.0
        self._heartbeat_stop = threading.Event()
        self._start_ts = 0.0

    def run(self) -> None:
        self._start_ts = time.monotonic()
        self._last_progress_ts = self._start_ts
        self._heartbeat_stop.clear()
        heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat.start()
        try:
            service = UnifiedDataModule.instance()
            response = service.fetch(self._request, progress_cb=self._emit_progress)
            self.success.emit(response)
        except DataServiceError as exc:
            self.error.emit(str(exc))
        except Exception:
            self.error.emit(traceback.format_exc())
        finally:
            self._heartbeat_stop.set()
            heartbeat.join(timeout=0.2)
            self.finished.emit()

    def _emit_progress(self, percent: int, message: str) -> None:
        now = time.monotonic()
        safe_percent = max(0, min(100, int(percent)))
        safe_message = str(message or "下载中")
        with self._progress_lock:
            self._last_percent = safe_percent
            self._last_message = safe_message
            self._last_progress_ts = now
        self.progress.emit(safe_percent, safe_message)

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(1.0):
            now = time.monotonic()
            with self._progress_lock:
                percent = self._last_percent
                message = self._last_message
                last_ts = self._last_progress_ts
            if percent >= 99:
                continue
            if (now - last_ts) < 1.0:
                continue
            elapsed = max(0, int(now - self._start_ts))
            self.progress.emit(percent, f"{message} · 已用{elapsed}s")
