from __future__ import annotations

import json
import time
from pathlib import Path

import akshare as ak
import pandas as pd
from PySide6.QtCore import QObject, Signal

from core.data_service import DataService
from GUI.services.symbol_service import SymbolService


class PreloadWorker(QObject):
    progress = Signal(int, str)
    finished = Signal(object)
    error = Signal(str)

    def __init__(self, cache_dir: Path) -> None:
        super().__init__()
        self._cache_dir = Path(cache_dir)

    def run(self) -> None:
        errors: list[str] = []
        warnings: list[str] = []
        result: dict[str, object] = {
            "symbols": False,
            "trade_dates": False,
            "watch_spot": False,
        }
        try:
            self.progress.emit(5, "初始化: 自选概要")
            self._update_watch_spot_snapshot()
            result["watch_spot"] = True
        except Exception as exc:
            # Best-effort only; main UI will refresh in background.
            warnings.append(f"watch_spot:{exc}")

        try:
            self.progress.emit(35, "初始化: 交易日历")
            self._ensure_trade_dates_ready()
            result["trade_dates"] = True
        except Exception as exc:
            errors.append(f"trade_dates:{exc}")

        try:
            self.progress.emit(65, "初始化: 股票列表")
            self._ensure_symbol_list_ready()
            result["symbols"] = True
        except Exception as exc:
            errors.append(f"symbols:{exc}")

        self.progress.emit(100, "初始化完成")
        if warnings:
            result["warnings"] = list(warnings)
        if errors:
            self.error.emit(" | ".join(errors))
        self.finished.emit(result)

    def _ensure_symbol_list_ready(self) -> None:
        service = SymbolService(self._cache_dir / "symbols")
        cached = service.load_cache()
        if cached is not None and cached.records:
            return
        fetched = service.fetch_remote()
        service.save_cache(fetched)

    @staticmethod
    def _first_col(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
        for key in candidates:
            if key in df.columns:
                return key
        return None

    def _update_watch_spot_snapshot(self) -> None:
        watchlist_path = self._cache_dir / "watchlist.json"
        if not watchlist_path.exists():
            return
        try:
            payload = json.loads(watchlist_path.read_text(encoding="utf-8"))
            if not isinstance(payload, list):
                return
            codes = [str(v).strip() for v in payload if str(v).strip()]
        except Exception:
            return
        if not codes:
            return

        df = self._call_with_timeout(lambda: ak.stock_zh_a_spot_em(), timeout_seconds=6.0)
        if df is None:
            df = self._call_with_timeout(lambda: ak.stock_zh_a_spot(), timeout_seconds=6.0)
        if df is None or df.empty:
            return

        code_col = self._first_col(df, ("代码", "证券代码", "symbol", "code"))
        name_col = self._first_col(df, ("名称", "证券名称", "name"))
        price_col = self._first_col(df, ("最新价", "现价", "price", "close"))
        pct_col = self._first_col(df, ("涨跌幅", "涨幅", "pct"))
        chg_col = self._first_col(df, ("涨跌额", "涨跌", "chg"))
        turn_col = self._first_col(df, ("换手率", "换手", "turnover", "turnover_rate"))

        if code_col is None:
            return
        safe = df.copy()
        safe[code_col] = safe[code_col].astype(str).str.strip().str.replace(".0", "", regex=False).str.zfill(6)
        safe = safe[safe[code_col].isin(set(codes))]
        if safe.empty:
            return

        out: dict[str, dict[str, object]] = {}
        for row in safe.itertuples(index=False):
            record = row._asdict() if hasattr(row, "_asdict") else dict(zip(safe.columns, row))
            code = str(record.get(code_col, "")).strip()
            if not code:
                continue
            name_val = str(record.get(name_col, "")) if name_col is not None else ""
            price_val = pd.to_numeric([record.get(price_col)], errors="coerce")[0] if price_col is not None else None
            pct_val = pd.to_numeric([record.get(pct_col)], errors="coerce")[0] if pct_col is not None else None
            chg_val = pd.to_numeric([record.get(chg_col)], errors="coerce")[0] if chg_col is not None else None
            turn_val = pd.to_numeric([record.get(turn_col)], errors="coerce")[0] if turn_col is not None else None
            out[code] = {
                "name": name_val.strip() if name_val else "",
                "price": None if pd.isna(price_val) else float(price_val),
                "pct": None if pd.isna(pct_val) else float(pct_val),
                "chg": None if pd.isna(chg_val) else float(chg_val),
                "speed": None,
                "turnover": None if pd.isna(turn_val) else float(turn_val),
            }

        if not out:
            return

        watch_cache_dir = self._cache_dir / "watch"
        watch_cache_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = watch_cache_dir / "spot_snapshot.json"
        try:
            snapshot_path.write_text(
                json.dumps({"ts": float(time.time()), "data": out}, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            return

    def _ensure_trade_dates_ready(self) -> None:
        if DataService._trade_dates_cache_needs_refresh():
            DataService._refresh_trade_dates_cache_worker()
        # Warm in-memory calendar.
        DataService._load_trade_dates()

    @staticmethod
    def _call_with_timeout(loader, timeout_seconds: float) -> object | None:
        payload: dict[str, object] = {}
        error: dict[str, Exception] = {}

        def runner() -> None:
            try:
                payload["value"] = loader()
            except Exception as exc:
                error["exc"] = exc

        import threading

        t = threading.Thread(target=runner, daemon=True)
        t.start()
        t.join(timeout=max(0.2, float(timeout_seconds)))
        if t.is_alive():
            return None
        if "exc" in error:
            raise error["exc"]
        return payload.get("value")
