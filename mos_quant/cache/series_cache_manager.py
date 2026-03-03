from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from .timeseries_cache import TimeSeriesCache


class SeriesCacheManager:
    """Centralized manager for time-series cache IO, merge and invalidation."""

    def __init__(self, cache_dir: Path) -> None:
        self._cache = TimeSeriesCache(cache_dir)

    def cache_paths(self, params: dict[str, str]) -> tuple[Path, Path]:
        return self._cache.cache_paths(params)

    def meta_path(self, params: dict[str, str]) -> Path:
        parquet_path, _ = self._cache.cache_paths(params)
        return parquet_path.with_suffix(".meta.json")

    def load_meta(self, params: dict[str, str]) -> dict[str, object] | None:
        path = self.meta_path(params)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else None
        except Exception:
            return None

    def save_meta(self, params: dict[str, str], meta: dict[str, object]) -> None:
        path = self.meta_path(params)
        payload = dict(meta)
        payload["updated_at"] = datetime.now().isoformat(timespec="seconds")
        try:
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            return

    def load(self, params: dict[str, str], minute_mode: bool) -> tuple[pd.DataFrame, Path | None]:
        return self._cache.load(params, minute_mode)

    def save(self, params: dict[str, str], df: pd.DataFrame) -> Path:
        return self._cache.save(params, df)

    def invalidate(self, params: dict[str, str]) -> None:
        parquet_path, pickle_path = self._cache.cache_paths(params)
        meta_path = self.meta_path(params)
        for path in (parquet_path, pickle_path, meta_path):
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                continue

    def merge(
        self,
        cached_df: pd.DataFrame,
        fetched_parts: list[pd.DataFrame],
        minute_mode: bool,
    ) -> pd.DataFrame:
        return self._cache.merge(cached_df, fetched_parts, minute_mode)

    def dataframe_changed(self, before: pd.DataFrame, after: pd.DataFrame) -> bool:
        return self._cache.dataframe_changed(before, after)

    def filter_by_range(
        self,
        df: pd.DataFrame,
        start_value: str,
        end_value: str,
        minute_mode: bool,
    ) -> pd.DataFrame:
        return self._cache.filter_by_range(df, start_value, end_value, minute_mode)

    def covers_range(
        self,
        df: pd.DataFrame,
        start_value: str,
        end_value: str,
        minute_mode: bool,
    ) -> bool:
        return self._cache.covers_range(df, start_value, end_value, minute_mode)

    def missing_ranges(
        self,
        df: pd.DataFrame,
        start_value: str,
        end_value: str,
        minute_mode: bool,
    ) -> list[tuple[str, str]]:
        return self._cache.missing_ranges(df, start_value, end_value, minute_mode)

    def normalize(self, df: pd.DataFrame, minute_mode: bool) -> pd.DataFrame:
        return self._cache.normalize(df, minute_mode)

