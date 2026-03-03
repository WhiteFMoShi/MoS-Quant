from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas.api.types import is_string_dtype

from core.parquet_compat import has_parquet_engine


class TimeSeriesCache:
    """Incremental cache helper for time-series datasets."""

    DAILY_WEEKEND_GRACE_DAYS = 3
    MINUTE_NON_TRADING_GRACE_DAYS = 10

    def __init__(self, cache_dir: Path) -> None:
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def cache_paths(self, params: dict[str, str]) -> tuple[Path, Path]:
        dataset = params.get("dataset", "unknown")
        symbol = "".join(ch for ch in params.get("symbol", "").lower() if ch.isalnum()) or "none"
        period = params.get("period", "daily")
        stem = f"series_{dataset}_{symbol}_{period}"
        return (
            self._cache_dir / f"{stem}.parquet",
            self._cache_dir / f"{stem}.pkl",
        )

    def load(self, params: dict[str, str], minute_mode: bool) -> tuple[pd.DataFrame, Path | None]:
        parquet_path, pickle_path = self.cache_paths(params)
        candidates: list[Path] = []
        if pickle_path.exists():
            candidates.append(pickle_path)
        if parquet_path.exists() and has_parquet_engine():
            candidates.append(parquet_path)
        if not candidates:
            return pd.DataFrame(), None
        for candidate in candidates:
            try:
                loaded = self.read_dataframe(candidate)
                return self.normalize(loaded, minute_mode), candidate
            except Exception:
                continue
        return pd.DataFrame(), None

    def save(self, params: dict[str, str], df: pd.DataFrame) -> Path:
        parquet_path, pickle_path = self.cache_paths(params)
        return self.write_dataframe(df, parquet_path, pickle_path)

    @staticmethod
    def existing_path(parquet_path: Path, pickle_path: Path) -> Path | None:
        if parquet_path.exists():
            return parquet_path
        if pickle_path.exists():
            return pickle_path
        return None

    @staticmethod
    def read_dataframe(path: Path) -> pd.DataFrame:
        if path.suffix == ".parquet":
            return pd.read_parquet(path)
        return pd.read_pickle(path)

    @staticmethod
    def _pickle_safe(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        safe = df
        try:
            for col in df.columns:
                if is_string_dtype(df[col].dtype):
                    if safe is df:
                        safe = df.copy()
                    safe[col] = safe[col].astype(object)
        except Exception:
            return df
        return safe

    @staticmethod
    def write_dataframe(df: pd.DataFrame, parquet_path: Path, pickle_path: Path) -> Path:
        safe_df = TimeSeriesCache._pickle_safe(df)
        try:
            safe_df.to_pickle(pickle_path)
        except Exception:
            pass
        if has_parquet_engine():
            try:
                safe_df.to_parquet(parquet_path, index=False)
                return parquet_path
            except Exception:
                try:
                    if parquet_path.exists():
                        parquet_path.unlink()
                except Exception:
                    pass
        return pickle_path

    @classmethod
    def merge(
        cls,
        cached_df: pd.DataFrame,
        fetched_parts: list[pd.DataFrame],
        minute_mode: bool,
    ) -> pd.DataFrame:
        merged = cached_df
        for part in fetched_parts:
            part_norm = cls.normalize(part, minute_mode)
            if part_norm.empty:
                continue
            if merged.empty:
                merged = part_norm
            else:
                merged = cls.normalize(pd.concat([merged, part_norm], ignore_index=True), minute_mode)
        return merged

    @staticmethod
    def dataframe_changed(before: pd.DataFrame, after: pd.DataFrame) -> bool:
        if before.empty and after.empty:
            return False
        if len(before.index) != len(after.index) or len(before.columns) != len(after.columns):
            return True
        try:
            return not before.equals(after)
        except Exception:
            return True

    @classmethod
    def normalize(cls, df: pd.DataFrame, minute_mode: bool) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        _, ts = cls._extract_ts(df)
        if ts is None:
            return df.reset_index(drop=True)
        safe = df.copy()
        safe["_ts_cache_"] = ts
        safe = safe.dropna(subset=["_ts_cache_"])
        if safe.empty:
            return pd.DataFrame()
        safe = safe.sort_values(by="_ts_cache_", kind="stable")
        safe = safe.drop_duplicates(subset=["_ts_cache_"], keep="last")
        safe = safe.drop(columns=["_ts_cache_"])
        return safe.reset_index(drop=True)

    @classmethod
    def filter_by_range(
        cls,
        df: pd.DataFrame,
        start_value: str,
        end_value: str,
        minute_mode: bool,
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        _, ts = cls._extract_ts(df)
        if ts is None:
            return df.reset_index(drop=True)
        start_ts = cls._range_text_to_ts(start_value, minute_mode)
        end_ts = cls._range_text_to_ts(end_value, minute_mode)
        if start_ts is None or end_ts is None:
            return df.reset_index(drop=True)
        mask = ts.between(start_ts, end_ts)
        return df.loc[mask].reset_index(drop=True)

    @classmethod
    def covers_range(
        cls,
        df: pd.DataFrame,
        start_value: str,
        end_value: str,
        minute_mode: bool,
    ) -> bool:
        if df is None or df.empty:
            return False
        _, ts = cls._extract_ts(df)
        if ts is None:
            return False
        start_ts = cls._range_text_to_ts(start_value, minute_mode)
        end_ts = cls._range_text_to_ts(end_value, minute_mode)
        if start_ts is None or end_ts is None:
            return False
        ts_min = ts.min()
        ts_max = ts.max()
        if pd.isna(ts_min) or pd.isna(ts_max):
            return False
        start_ok = bool(ts_min <= start_ts) or cls._is_tolerable_left_gap(start_ts, ts_min, minute_mode)
        end_ok = bool(ts_max >= end_ts) or cls._is_tolerable_right_gap(ts_max, end_ts, minute_mode)
        return start_ok and end_ok

    @classmethod
    def missing_ranges(
        cls,
        df: pd.DataFrame,
        start_value: str,
        end_value: str,
        minute_mode: bool,
    ) -> list[tuple[str, str]]:
        _, ts = cls._extract_ts(df)
        if ts is None:
            return [(start_value, end_value)]
        start_ts = cls._range_text_to_ts(start_value, minute_mode)
        end_ts = cls._range_text_to_ts(end_value, minute_mode)
        if start_ts is None or end_ts is None:
            return [(start_value, end_value)]
        ts_min = ts.min()
        ts_max = ts.max()
        if pd.isna(ts_min) or pd.isna(ts_max):
            return [(start_value, end_value)]

        delta = pd.Timedelta(minutes=1) if minute_mode else pd.Timedelta(days=1)
        ranges: list[tuple[str, str]] = []

        if minute_mode:
            # Minute feeds are windowed, but for user-selected ranges we still try to fill both edges.
            if start_ts < ts_min and not cls._is_tolerable_left_gap(start_ts, ts_min, minute_mode):
                left_end = ts_min - delta
                if left_end >= start_ts:
                    ranges.append(
                        (
                            cls._ts_to_range_text(start_ts, minute_mode),
                            cls._ts_to_range_text(left_end, minute_mode),
                        )
                    )
            if end_ts > ts_max and not cls._is_tolerable_right_gap(ts_max, end_ts, minute_mode):
                right_start = ts_max + delta
                if right_start <= end_ts:
                    ranges.append(
                        (
                            cls._ts_to_range_text(right_start, minute_mode),
                            cls._ts_to_range_text(end_ts, minute_mode),
                        )
                    )
            return ranges

        if start_ts < ts_min and not cls._is_tolerable_left_gap(start_ts, ts_min, minute_mode):
            left_end = ts_min - delta
            if left_end >= start_ts:
                ranges.append(
                    (
                        cls._ts_to_range_text(start_ts, minute_mode),
                        cls._ts_to_range_text(left_end, minute_mode),
                    )
                )

        if end_ts > ts_max and not cls._is_tolerable_right_gap(ts_max, end_ts, minute_mode):
            right_start = ts_max + delta
            if right_start <= end_ts:
                ranges.append(
                    (
                        cls._ts_to_range_text(right_start, minute_mode),
                        cls._ts_to_range_text(end_ts, minute_mode),
                    )
                )

        return ranges

    @classmethod
    def _is_tolerable_left_gap(
        cls,
        start_ts: pd.Timestamp,
        ts_min: pd.Timestamp,
        minute_mode: bool,
    ) -> bool:
        if minute_mode:
            gap = ts_min - start_ts
            if gap <= pd.Timedelta(0):
                return True
            # Ignore small leading gaps caused by non-trading minutes/days.
            return gap <= pd.Timedelta(days=cls.MINUTE_NON_TRADING_GRACE_DAYS)
        gap = ts_min - start_ts
        if gap <= pd.Timedelta(0):
            return True
        # Some benchmarks/old symbols do not have data before a known first trade year.
        return gap <= pd.Timedelta(days=400)

    @classmethod
    def _is_tolerable_right_gap(
        cls,
        ts_max: pd.Timestamp,
        end_ts: pd.Timestamp,
        minute_mode: bool,
    ) -> bool:
        gap = end_ts - ts_max
        if gap <= pd.Timedelta(0):
            return True
        if minute_mode:
            if gap > pd.Timedelta(days=cls.MINUTE_NON_TRADING_GRACE_DAYS):
                return False
            today = pd.Timestamp(datetime.now().date())
            end_day = end_ts.normalize()
            tail_day = pd.Timestamp(ts_max).normalize()
            # Same-session right-edge tiny gap (e.g. 14:59 vs 15:00) should not force a network fetch loop.
            if tail_day == end_day and gap <= pd.Timedelta(minutes=5):
                return True
            # Real-time minute feeds are often 1-5 minutes behind; avoid needless re-fetch loops.
            if end_day == today and gap <= pd.Timedelta(minutes=5):
                return True
            # During market close (today/weekend), minute quote may stop at last close.
            if end_day > today:
                return True
            if end_day == today:
                return bool(today.dayofweek >= 5)
            if end_day.dayofweek >= 5:
                return True
            # Holiday gaps are common in A-share markets.
            return end_day >= (today - pd.Timedelta(days=1))
        if gap > pd.Timedelta(days=cls.DAILY_WEEKEND_GRACE_DAYS):
            return False
        today = pd.Timestamp(datetime.now().date())
        # If requested end is today/future, market may not have produced close data yet.
        if end_ts >= today:
            if pd.Timestamp(ts_max).normalize() >= today:
                return True
            now_ts = pd.Timestamp.now()
            if today.dayofweek >= 5:
                return True
            market_open = today + pd.Timedelta(hours=9, minutes=30)
            if now_ts < market_open:
                return True
            return False
        if end_ts < (today - pd.Timedelta(days=1)):
            return False
        return end_ts.dayofweek >= 5

    @staticmethod
    def _extract_ts(df: pd.DataFrame) -> tuple[str | None, pd.Series | None]:
        date_col = None
        for candidate in ("时间", "日期", "date", "datetime", "trade_date", "day"):
            if candidate in df.columns:
                date_col = candidate
                break
        if date_col is None:
            return None, None
        ts = TimeSeriesCache._parse_datetime_series(df[date_col])
        if ts.notna().sum() == 0:
            return None, None
        return date_col, ts

    @staticmethod
    def _parse_datetime_series(values: pd.Series) -> pd.Series:
        index = values.index
        parsers = (
            lambda: pd.to_datetime(values, errors="coerce"),
            lambda: pd.to_datetime(values, errors="coerce", format="ISO8601", utc=True),
            lambda: pd.to_datetime(values, errors="coerce", format="mixed", utc=True),
        )
        for parser in parsers:
            try:
                parsed = parser()
                ts = parsed if isinstance(parsed, pd.Series) else pd.Series(parsed, index=index)
                try:
                    if hasattr(ts, "dt") and ts.dt.tz is not None:
                        ts = ts.dt.tz_convert(None)
                except Exception:
                    try:
                        ts = ts.dt.tz_localize(None)
                    except Exception:
                        pass
                if ts.notna().sum() > 0:
                    return ts
            except Exception:
                continue
        return pd.Series(pd.NaT, index=index)

    @staticmethod
    def _range_text_to_ts(value: str, minute_mode: bool) -> pd.Timestamp | None:
        try:
            if minute_mode:
                return pd.to_datetime(value, errors="coerce")
            return pd.to_datetime(value, format="%Y%m%d", errors="coerce")
        except Exception:
            return None

    @staticmethod
    def _ts_to_range_text(ts: pd.Timestamp, minute_mode: bool) -> str:
        if minute_mode:
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        return ts.strftime("%Y%m%d")
