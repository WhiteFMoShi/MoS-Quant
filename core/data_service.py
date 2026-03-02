from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
import time
from typing import Callable

import akshare as ak
import pandas as pd
from requests.exceptions import RequestException

from core.cache_paths import cache_root, legacy_cache_roots
from core.series_cache_manager import SeriesCacheManager
from core.timeseries_cache import TimeSeriesCache


class DataServiceError(Exception):
    """Raised when data fetch parameters are invalid or fetch fails."""


@dataclass(frozen=True)
class FetchRequest:
    dataset: str
    symbol: str
    start_date: str
    end_date: str
    single_date: str
    force_refresh: bool = False
    period: str = "daily"


@dataclass(frozen=True)
class FetchResponse:
    title: str
    dataframe: pd.DataFrame
    cache_path: str
    from_cache: bool


class DataService:
    """Encapsulates AKShare data access for GUI and scripts."""
    DAILY_CLOSE_READY_HOUR = 15
    DAILY_CLOSE_READY_MINUTE = 5

    def __init__(self, cache_dir: Path | None = None) -> None:
        self._cache_dir = cache_dir or (cache_root() / "datasets")
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._series_cache = SeriesCacheManager(self._cache_dir)
        self._adopt_legacy_cache_files("datasets")
        self._cleanup_obsolete_series_cache_files()

    def fetch(
        self,
        request: FetchRequest,
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> FetchResponse:
        if request.dataset == "stock_daily":
            return self._fetch_stock_daily(request, progress_cb=progress_cb)
        if request.dataset == "index_daily":
            return self._fetch_index_daily(request, progress_cb=progress_cb)
        if request.dataset == "szse_summary":
            return self._fetch_szse_summary(request, progress_cb=progress_cb)
        raise DataServiceError(f"Unsupported dataset: {request.dataset}")

    def _fetch_stock_daily(
        self,
        request: FetchRequest,
        *,
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> FetchResponse:
        raw_symbol = request.symbol.strip()
        if not raw_symbol:
            raise DataServiceError("A股日线需要填写股票代码，例如 000001")
        symbol = self._canonical_market_symbol(raw_symbol)
        period = self._normalize_period(request.period)
        title_prefix = f"A股{self._period_label(period)}"
        params = {
            "dataset": "stock_daily",
            "symbol": symbol,
            "period": period,
            "adjust": "qfq",
        }
        if self._is_minute_period(period):
            start = self._as_datetime_text(request.start_date, start_of_day=True)
            end = self._as_datetime_text(request.end_date, start_of_day=False)
            start, end = self._clip_minute_range(start, end, period=period)
            title = f"{title_prefix} {symbol} {start}-{end}"
            params["start_date"] = start
            params["end_date"] = end
            fetcher: Callable[[str, str], pd.DataFrame] = lambda s, e: self._fetch_stock_minute(
                symbol=symbol,
                start_date=s,
                end_date=e,
                period=period,
            )
            minute_mode = True
        else:
            start = self._as_compact_date(request.start_date)
            end = self._as_compact_date(request.end_date)
            end = self._clip_end_for_period(end, period=period)
            title = f"{title_prefix} {symbol} {start}-{end}"
            params["start_date"] = start
            params["end_date"] = end
            fetcher = lambda s, e: self._fetch_stock_daily_fallback(
                symbol=symbol,
                start_date=s,
                end_date=e,
                period=period,
            )
            minute_mode = False
        try:
            return self._fetch_time_series_incremental(
                request=request,
                title=title,
                params=params,
                action_name=f"{title_prefix} {symbol}",
                fetch_range_func=fetcher,
                start_value=start,
                end_value=end,
                minute_mode=minute_mode,
                progress_cb=progress_cb,
            )
        except Exception as exc:
            raise DataServiceError(f"{title_prefix}抓取失败: {exc}") from exc

    def _try_stock_fallback_for_index_request(
        self,
        *,
        request: FetchRequest,
        source_error: str,
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> FetchResponse | None:
        if not self._should_try_stock_fallback(request.symbol, source_error):
            return None
        self._emit_progress(progress_cb, 42, "指数源异常，尝试A股源")
        stock_request = FetchRequest(
            dataset="stock_daily",
            symbol=request.symbol,
            start_date=request.start_date,
            end_date=request.end_date,
            single_date=request.single_date,
            force_refresh=request.force_refresh,
            period=request.period,
        )
        try:
            response = self._fetch_stock_daily(stock_request, progress_cb=progress_cb)
            return FetchResponse(
                title=f"{response.title}（自动按A股代码处理）",
                dataframe=response.dataframe,
                cache_path=response.cache_path,
                from_cache=response.from_cache,
            )
        except Exception:
            return None

    @staticmethod
    def _should_try_stock_fallback(symbol: str, source_error: str) -> bool:
        code = symbol.strip().lower().replace("sh", "").replace("sz", "").replace("bj", "")
        if not (code.isdigit() and len(code) == 6):
            return False
        if code.startswith("399"):
            return False
        known_index_codes = {
            "000001",
            "000016",
            "000300",
            "000688",
            "000852",
            "000905",
            "399001",
            "399005",
            "399006",
            "399300",
            "399905",
            "399673",
        }
        if code in known_index_codes:
            return False
        err = (source_error or "").lower()
        # Typical signatures when a stock code is queried by index endpoints.
        return ("sina:date" in err) or ("list index out of range" in err)

    def _fetch_index_daily(
        self,
        request: FetchRequest,
        *,
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> FetchResponse:
        raw_symbol = request.symbol.strip()
        if not raw_symbol:
            raise DataServiceError("指数日线需要填写指数代码，例如 000300")
        symbol = self._canonical_market_symbol(raw_symbol)
        period = self._normalize_period(request.period)
        title_prefix = f"指数{self._period_label(period)}"
        params = {
            "dataset": "index_daily",
            "symbol": symbol,
            "period": period,
        }
        if self._is_minute_period(period):
            start = self._as_datetime_text(request.start_date, start_of_day=True)
            end = self._as_datetime_text(request.end_date, start_of_day=False)
            start, end = self._clip_minute_range(start, end, period=period)
            title = f"{title_prefix} {symbol} {start}-{end}"
            params["start_date"] = start
            params["end_date"] = end
            fetcher: Callable[[str, str], pd.DataFrame] = lambda s, e: self._fetch_index_minute(
                symbol=symbol,
                start_date=s,
                end_date=e,
                period=period,
            )
            minute_mode = True
        else:
            start = self._as_compact_date(request.start_date)
            end = self._as_compact_date(request.end_date)
            end = self._clip_end_for_period(end, period=period)
            title = f"{title_prefix} {symbol} {start}-{end}"
            params["start_date"] = start
            params["end_date"] = end
            fetcher = lambda s, e: self._fetch_index_daily_fallback(
                symbol=symbol,
                start_date=s,
                end_date=e,
                period=period,
            )
            minute_mode = False
        try:
            return self._fetch_time_series_incremental(
                request=request,
                title=title,
                params=params,
                action_name=f"{title_prefix} {symbol}",
                fetch_range_func=fetcher,
                start_value=start,
                end_value=end,
                minute_mode=minute_mode,
                progress_cb=progress_cb,
            )
        except Exception as exc:
            fallback = self._try_stock_fallback_for_index_request(
                request=request,
                source_error=str(exc),
                progress_cb=progress_cb,
            )
            if fallback is not None:
                return fallback
            raise DataServiceError(f"{title_prefix}抓取失败: {exc}") from exc

    def _fetch_szse_summary(
        self,
        request: FetchRequest,
        *,
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> FetchResponse:
        date_value = self._as_compact_date(request.single_date)
        title = f"深交所市场总貌 {date_value}"
        params = {
            "dataset": "szse_summary",
            "date": date_value,
        }
        try:
            return self._fetch_with_cache(
                request=request,
                title=title,
                params=params,
                action_name=f"深交所市场总貌 {date_value}",
                fetch_func=lambda: ak.stock_szse_summary(date=date_value),
                progress_cb=progress_cb,
            )
        except Exception as exc:
            raise DataServiceError(f"深交所市场总貌抓取失败: {exc}") from exc

    def _fetch_time_series_incremental(
        self,
        request: FetchRequest,
        title: str,
        params: dict[str, str],
        action_name: str,
        fetch_range_func: Callable[[str, str], pd.DataFrame],
        start_value: str,
        end_value: str,
        minute_mode: bool,
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> FetchResponse:
        self._emit_progress(progress_cb, 4, "检查缓存")
        parquet_path, pickle_path = self._series_cache.cache_paths(params)
        cached_df = pd.DataFrame()
        cached_path: Path | None = None
        span_days = self._range_span_days(start_value, end_value, minute_mode)
        validate_daily = (not minute_mode) and (request.period == "daily")
        range_start_value = start_value
        fresh_hash = self._series_fresh_hash(params, minute_mode)
        if not request.force_refresh:
            meta = self._series_cache.load_meta(params)
            if meta is not None and str(meta.get("fresh_hash", "")) == fresh_hash:
                cached_df, cached_path = self._series_cache.load(params, minute_mode)
                if not cached_df.empty:
                    filtered = self._series_cache.filter_by_range(cached_df, range_start_value, end_value, minute_mode)
                    covered = self._series_cache.covers_range(cached_df, range_start_value, end_value, minute_mode)
                    if covered and ((not validate_daily) or self._is_valid_daily_series(filtered, span_days)):
                        self._emit_progress(progress_cb, 100, "命中缓存(hash)")
                        return FetchResponse(
                            title=title,
                            dataframe=filtered,
                            cache_path=str(cached_path if cached_path is not None else (parquet_path if parquet_path.exists() else pickle_path)),
                            from_cache=True,
                        )
            if cached_df.empty:
                cached_df, cached_path = self._series_cache.load(params, minute_mode)
            if self._is_sparse_result(cached_df, span_days, minute_mode):
                cached_df = pd.DataFrame()
                cached_path = None
            elif validate_daily and not self._is_valid_daily_series(cached_df, span_days):
                self._emit_progress(progress_cb, 7, "缓存形态偏稀，继续增量")
                range_start_value = self._effective_start_for_cached_non_minute(start_value, cached_df)
            elif not minute_mode:
                range_start_value = self._effective_start_for_cached_non_minute(start_value, cached_df)

        force_full_refresh_due_invalid_daily = False
        if not request.force_refresh and not cached_df.empty:
            if self._series_cache.covers_range(cached_df, range_start_value, end_value, minute_mode):
                filtered = self._series_cache.filter_by_range(cached_df, range_start_value, end_value, minute_mode)
                if (not validate_daily) or self._is_valid_daily_series(filtered, span_days):
                    self._emit_progress(progress_cb, 100, "命中缓存")
                    return FetchResponse(
                        title=title,
                        dataframe=filtered,
                        cache_path=str(cached_path if cached_path is not None else (parquet_path if parquet_path.exists() else pickle_path)),
                        from_cache=True,
                    )
                force_full_refresh_due_invalid_daily = True
                self._emit_progress(progress_cb, 12, "缓存覆盖但质量不足，准备修复")

        fetched_parts: list[pd.DataFrame] = []
        fetch_failures: list[str] = []
        ranges_to_fetch: list[tuple[str, str]]
        if request.force_refresh or cached_df.empty or force_full_refresh_due_invalid_daily:
            ranges_to_fetch = [(start_value, end_value)]
        else:
            ranges = self._series_cache.missing_ranges(cached_df, range_start_value, end_value, minute_mode)
            if not ranges:
                filtered = self._series_cache.filter_by_range(cached_df, range_start_value, end_value, minute_mode)
                if (not validate_daily) or self._is_valid_daily_series(filtered, span_days):
                    self._emit_progress(progress_cb, 100, "命中缓存")
                    return FetchResponse(
                        title=title,
                        dataframe=filtered,
                        cache_path=str(cached_path if cached_path is not None else (parquet_path if parquet_path.exists() else pickle_path)),
                        from_cache=True,
                    )
                ranges_to_fetch = [(start_value, end_value)]
                self._emit_progress(progress_cb, 14, "缓存完整但质量不足，执行全区间修复")
            else:
                ranges_to_fetch = ranges

        if minute_mode:
            expanded: list[tuple[str, str]] = []
            chunk_days = 20 if (span_days is None or span_days <= 240) else 45
            for start_text, end_text in ranges_to_fetch:
                expanded.extend(self._split_minute_ranges(start_text, end_text, chunk_days=chunk_days))
            ranges_to_fetch = expanded

        total = len(ranges_to_fetch)
        if total > 0 and not cached_df.empty:
            self._emit_progress(progress_cb, 16, f"缓存增量区间 {total} 段")
        for idx, (start_text, end_text) in enumerate(ranges_to_fetch, start=1):
            phase = 18 + int((idx - 1) / max(total, 1) * 52)
            label = "下载主数据" if total == 1 else f"下载区间 {idx}/{total}"
            self._emit_progress(progress_cb, phase, label)
            quick_incremental = (not request.force_refresh) and (not cached_df.empty)
            try:
                fetched_parts.append(
                    self._call_with_retry(
                        fetch_func=lambda s=start_text, e=end_text: fetch_range_func(s, e),
                        action_name=f"{action_name} {start_text}-{end_text}",
                        retries=1 if quick_incremental else (2 if minute_mode else 3),
                        delay_seconds=0.2 if quick_incremental else (0.6 if minute_mode else 0.8),
                    )
                )
            except Exception as exc:
                fetch_failures.append(str(exc))
                if cached_df.empty:
                    raise
        self._emit_progress(progress_cb, 72, "区间下载完成")

        merged = self._series_cache.merge(cached_df, fetched_parts, minute_mode)
        if merged.empty and not cached_df.empty:
            merged = cached_df
        filtered = self._series_cache.filter_by_range(merged, start_value, end_value, minute_mode)

        needs_repair = self._is_sparse_result(filtered, span_days, minute_mode) or (
            validate_daily and (not self._is_valid_daily_series(filtered, span_days))
        )
        attempt_repair = needs_repair
        if attempt_repair:
            # Avoid heavy full-range re-download when cache is already usable.
            if (not request.force_refresh) and (not cached_df.empty) and (not filtered.empty):
                attempt_repair = False
            # If this request has already downloaded the whole requested range, do not repeat
            # another full-range repair in the same round (high cost, low gain).
            if (
                (not request.force_refresh)
                and cached_df.empty
                and len(ranges_to_fetch) == 1
                and ranges_to_fetch[0][0] == start_value
                and ranges_to_fetch[0][1] == end_value
            ):
                attempt_repair = False
            if minute_mode and span_days is not None and span_days > 180:
                attempt_repair = False
            if (not minute_mode) and span_days is not None and span_days > 3650 and (not cached_df.empty):
                attempt_repair = False

        # Guard against bad upstream snapshots or stale sparse/low-density cache.
        if attempt_repair:
            try:
                self._emit_progress(progress_cb, 78, "尝试全量修复")
                if minute_mode and span_days is not None and span_days > 180:
                    raise RuntimeError("minute_range_too_large")
                repaired = self._call_with_retry(
                    fetch_func=lambda: fetch_range_func(start_value, end_value),
                    action_name=f"{action_name} 全量修复",
                    retries=1 if minute_mode else 2,
                    delay_seconds=0.6 if minute_mode else 0.8,
                )
                repaired_norm = self._series_cache.normalize(repaired, minute_mode)
                if not repaired_norm.empty:
                    merged = self._series_cache.merge(merged, [repaired_norm], minute_mode)
                    filtered = self._series_cache.filter_by_range(merged, start_value, end_value, minute_mode)
            except Exception:
                # Keep previously merged cache/result as fallback.
                pass
        elif needs_repair:
            self._emit_progress(progress_cb, 80, "跳过全量修复(缓存优先)")

        if filtered.empty and not cached_df.empty and fetch_failures:
            # Missing range can be all non-trading days; prefer stale-but-usable cache.
            filtered = self._series_cache.filter_by_range(cached_df, start_value, end_value, minute_mode)

        if validate_daily and not self._is_valid_daily_series(filtered, span_days):
            if not cached_df.empty and self._is_valid_daily_series(cached_df, span_days):
                filtered = self._series_cache.filter_by_range(cached_df, start_value, end_value, minute_mode)
                merged = cached_df

        self._emit_progress(progress_cb, 88, "合并缓存")
        changed = self._series_cache.dataframe_changed(cached_df, merged)
        if changed or cached_path is None:
            cache_path = self._series_cache.save(params, merged)
        else:
            cache_path = cached_path
        self._series_cache.save_meta(
            params,
            self._build_series_meta(merged, minute_mode=minute_mode, fresh_hash=fresh_hash),
        )
        self._emit_progress(progress_cb, 100, "完成")
        return FetchResponse(
            title=title,
            dataframe=filtered,
            cache_path=str(cache_path),
            from_cache=not changed and cached_path is not None,
        )

    def _fetch_with_cache(
        self,
        request: FetchRequest,
        title: str,
        params: dict[str, str],
        action_name: str,
        fetch_func: Callable[[], pd.DataFrame],
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> FetchResponse:
        self._emit_progress(progress_cb, 6, "检查缓存")
        parquet_path, pickle_path = self._cache_file_paths(params)
        if not request.force_refresh:
            cached_path = self._existing_cache_file(parquet_path, pickle_path)
            if cached_path is not None:
                try:
                    cached_df = self._read_cached_dataframe(cached_path)
                    if cached_path.suffix == ".pkl" and not parquet_path.exists():
                        try:
                            cached_df.to_parquet(parquet_path, index=False)
                            cached_path = parquet_path
                        except Exception:
                            pass
                    self._emit_progress(progress_cb, 100, "命中缓存")
                    return FetchResponse(
                        title=title,
                        dataframe=cached_df,
                        cache_path=str(cached_path),
                        from_cache=True,
                    )
                except Exception:
                    # corrupted cache should not block a fresh fetch
                    pass

        self._emit_progress(progress_cb, 28, "下载数据")
        df = self._call_with_retry(fetch_func=fetch_func, action_name=action_name)
        self._emit_progress(progress_cb, 80, "写入缓存")
        cache_path = self._write_cache_dataframe(df, parquet_path, pickle_path)
        self._emit_progress(progress_cb, 100, "完成")
        return FetchResponse(
            title=title,
            dataframe=df,
            cache_path=str(cache_path),
            from_cache=False,
        )

    @staticmethod
    def _emit_progress(
        progress_cb: Callable[[int, str], None] | None,
        percent: int,
        message: str,
    ) -> None:
        if progress_cb is None:
            return
        try:
            bounded = max(0, min(100, int(percent)))
            progress_cb(bounded, message)
        except Exception:
            return

    def _fetch_stock_daily_fallback(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = "daily",
    ) -> pd.DataFrame:
        prefixed_symbol = self._normalize_stock_symbol(symbol)
        errors: list[str] = []
        span_days = self._range_span_days(start_date, end_date, minute_mode=False)
        sparse_candidate: pd.DataFrame | None = None

        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            if not self._is_sparse_result(df, span_days, minute_mode=False):
                if period != "daily" or self._is_valid_daily_series(df, span_days):
                    return df
                sparse_candidate = df
                errors.append("eastmoney:low_density")
            else:
                sparse_candidate = df
                errors.append("eastmoney:sparse_result")
        except Exception as exc:
            errors.append(f"eastmoney:{exc}")

        if period != "daily":
            resampled = self._fetch_non_daily_from_daily_sources(
                period=period,
                start_date=start_date,
                end_date=end_date,
                loaders=[
                    (
                        "eastmoney_daily",
                        lambda: ak.stock_zh_a_hist(
                            symbol=symbol,
                            period="daily",
                            start_date=start_date,
                            end_date=end_date,
                            adjust="qfq",
                        ),
                    ),
                    (
                        "sina",
                        lambda: ak.stock_zh_a_daily(
                            symbol=prefixed_symbol,
                            start_date=self._as_dash_date(start_date),
                            end_date=self._as_dash_date(end_date),
                            adjust="qfq",
                        ),
                    ),
                ],
                errors=errors,
            )
            if resampled is not None:
                return resampled
            if sparse_candidate is not None:
                return sparse_candidate.reset_index(drop=True)
            raise RuntimeError(" | ".join(errors))

        try:
            df = ak.stock_zh_a_daily(
                symbol=prefixed_symbol,
                start_date=self._as_dash_date(start_date),
                end_date=self._as_dash_date(end_date),
                adjust="qfq",
            )
            if not self._is_sparse_result(df, span_days, minute_mode=False):
                if self._is_valid_daily_series(df, span_days):
                    return df
                sparse_candidate = df
                errors.append("sina:low_density")
            else:
                sparse_candidate = df
                errors.append("sina:sparse_result")
                if span_days is not None and span_days <= 3:
                    return sparse_candidate.reset_index(drop=True)
        except Exception as exc:
            errors.append(f"sina:{exc}")

        if sparse_candidate is not None:
            return sparse_candidate.reset_index(drop=True)
        raise RuntimeError(" | ".join(errors))

    def _fetch_index_daily_fallback(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = "daily",
    ) -> pd.DataFrame:
        prefixed_symbol = self._normalize_index_symbol(symbol)
        errors: list[str] = []
        span_days = self._range_span_days(start_date, end_date, minute_mode=False)
        sparse_candidate: pd.DataFrame | None = None

        try:
            df = ak.index_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
            )
            if not self._is_sparse_result(df, span_days, minute_mode=False):
                if period != "daily" or self._is_valid_daily_series(df, span_days):
                    return df
                sparse_candidate = df
                errors.append("eastmoney:low_density")
            else:
                sparse_candidate = df
                errors.append("eastmoney:sparse_result")
        except Exception as exc:
            errors.append(f"eastmoney:{exc}")

        if period != "daily":
            resampled = self._fetch_non_daily_from_daily_sources(
                period=period,
                start_date=start_date,
                end_date=end_date,
                loaders=[
                    (
                        "eastmoney_daily",
                        lambda: ak.index_zh_a_hist(
                            symbol=symbol,
                            period="daily",
                            start_date=start_date,
                            end_date=end_date,
                        ),
                    ),
                    (
                        "sina",
                        lambda: self._filter_by_date(
                            ak.stock_zh_index_daily(symbol=prefixed_symbol),
                            start_date,
                            end_date,
                        ),
                    ),
                ],
                errors=errors,
            )
            if resampled is not None:
                return resampled
            if sparse_candidate is not None:
                return sparse_candidate.reset_index(drop=True)
            raise RuntimeError(" | ".join(errors))

        try:
            df = ak.stock_zh_index_daily(symbol=prefixed_symbol)
            filtered = self._filter_by_date(df, start_date, end_date)
            if not self._is_sparse_result(filtered, span_days, minute_mode=False):
                if self._is_valid_daily_series(filtered, span_days):
                    return filtered
                sparse_candidate = filtered
                errors.append("sina:low_density")
            else:
                sparse_candidate = filtered
                errors.append("sina:sparse_result")
                if span_days is not None and span_days <= 3:
                    return sparse_candidate.reset_index(drop=True)
        except Exception as exc:
            errors.append(f"sina:{exc}")

        if sparse_candidate is not None:
            return sparse_candidate.reset_index(drop=True)
        raise RuntimeError(" | ".join(errors))

    def _fetch_stock_minute(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
    ) -> pd.DataFrame:
        errors: list[str] = []

        try:
            df = ak.stock_zh_a_hist_min_em(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                period=period,
                adjust="",
            )
            filtered = self._filter_by_datetime(df, start_date, end_date)
            if filtered is not None and not filtered.empty:
                return filtered
            return df
        except Exception as exc:
            errors.append(f"eastmoney_min:{exc}")

        try:
            rv = ak.rv_from_stock_zh_a_hist_min_em(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                period=period,
                adjust="hfq",
            )
            filtered = self._filter_by_datetime(rv, start_date, end_date)
            if filtered is not None and not filtered.empty:
                return filtered
            return rv
        except Exception as exc:
            errors.append(f"eastmoney_rv:{exc}")

        try:
            quote_symbol = self._normalize_stock_symbol(symbol)
            minute_df = ak.stock_zh_a_minute(symbol=quote_symbol, period=period, adjust="")
            if minute_df is not None and not minute_df.empty and "day" in minute_df.columns:
                minute_df = minute_df.rename(columns={"day": "datetime"})
            filtered = self._filter_by_datetime(minute_df, start_date, end_date)
            if filtered is not None and not filtered.empty:
                return filtered
            if minute_df is not None and not minute_df.empty:
                return minute_df
        except Exception as exc:
            errors.append(f"sina_min:{exc}")

        raise RuntimeError(" | ".join(errors))

    def _fetch_index_minute(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
    ) -> pd.DataFrame:
        errors: list[str] = []
        candidates = [symbol, symbol.strip().lower().replace("sh", "").replace("sz", "")]
        tried: set[str] = set()
        for candidate in candidates:
            key = candidate.strip()
            if not key or key in tried:
                continue
            tried.add(key)
            try:
                df = ak.index_zh_a_hist_min_em(
                    symbol=key,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                )
                filtered = self._filter_by_datetime(df, start_date, end_date)
                if filtered is not None and not filtered.empty:
                    return filtered
                return df
            except Exception as exc:
                errors.append(f"index_min:{key}:{exc}")

        try:
            quote_symbol = self._normalize_index_symbol(symbol)
            minute_df = ak.stock_zh_a_minute(symbol=quote_symbol, period=period, adjust="")
            if minute_df is not None and not minute_df.empty and "day" in minute_df.columns:
                minute_df = minute_df.rename(columns={"day": "datetime"})
            filtered = self._filter_by_datetime(minute_df, start_date, end_date)
            if filtered is not None and not filtered.empty:
                return filtered
            if minute_df is not None and not minute_df.empty:
                return minute_df
        except Exception as exc:
            errors.append(f"sina_min:{exc}")
        raise RuntimeError(" | ".join(errors))

    def _cache_file_paths(self, params: dict[str, str]) -> tuple[Path, Path]:
        payload = json.dumps(params, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
        stem = f"{params['dataset']}_{digest}"
        return (
            self._cache_dir / f"{stem}.parquet",
            self._cache_dir / f"{stem}.pkl",
        )

    def _timeseries_cache_paths(self, params: dict[str, str]) -> tuple[Path, Path]:
        return self._series_cache.cache_paths(params)

    @staticmethod
    def _existing_cache_file(parquet_path: Path, pickle_path: Path) -> Path | None:
        if parquet_path.exists():
            return parquet_path
        if pickle_path.exists():
            return pickle_path
        return None

    def _adopt_legacy_cache_files(self, subdir: str) -> None:
        for legacy_root in legacy_cache_roots():
            legacy_dir = legacy_root / subdir
            if not legacy_dir.exists():
                continue
            for file_path in legacy_dir.glob("*"):
                if not file_path.is_file() or file_path.suffix not in {".parquet", ".pkl"}:
                    continue
                target = self._cache_dir / file_path.name
                if target.exists():
                    continue
                try:
                    file_path.replace(target)
                except Exception:
                    continue
            self._cleanup_empty_dir(legacy_dir)
            self._cleanup_empty_dir(legacy_root)

    def _cleanup_obsolete_series_cache_files(self) -> None:
        patterns = (
            "stock_daily_*.parquet",
            "stock_daily_*.pkl",
            "index_daily_*.parquet",
            "index_daily_*.pkl",
        )
        for pattern in patterns:
            for file_path in self._cache_dir.glob(pattern):
                if file_path.name.startswith("series_"):
                    continue
                try:
                    file_path.unlink()
                except Exception:
                    continue

    @staticmethod
    def _cleanup_empty_dir(path: Path) -> None:
        try:
            if path.exists() and path.is_dir() and not any(path.iterdir()):
                path.rmdir()
        except Exception:
            pass

    @staticmethod
    def _read_cached_dataframe(path: Path) -> pd.DataFrame:
        if path.suffix == ".parquet":
            return pd.read_parquet(path)
        return pd.read_pickle(path)

    @staticmethod
    def _write_cache_dataframe(df: pd.DataFrame, parquet_path: Path, pickle_path: Path) -> Path:
        try:
            df.to_parquet(parquet_path, index=False)
            return parquet_path
        except Exception:
            df.to_pickle(pickle_path)
            return pickle_path

    @staticmethod
    def _call_with_retry(
        fetch_func: Callable[[], pd.DataFrame],
        action_name: str,
        retries: int = 3,
        delay_seconds: float = 0.8,
    ) -> pd.DataFrame:
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                return fetch_func()
            except RequestException as exc:
                last_error = exc
                if attempt < retries:
                    time.sleep(delay_seconds * attempt)
                    continue
                raise RuntimeError(
                    f"{action_name} 请求失败（已重试 {retries} 次）"
                ) from exc
            except Exception as exc:
                last_error = exc
                if attempt < retries:
                    time.sleep(delay_seconds * attempt)
                    continue
                raise RuntimeError(
                    f"{action_name} 异常（已重试 {retries} 次）: {exc}"
                ) from exc
        raise RuntimeError(f"{action_name} 失败: {last_error}")

    @classmethod
    def _normalize_time_series(cls, df: pd.DataFrame, minute_mode: bool) -> pd.DataFrame:
        return TimeSeriesCache.normalize(df, minute_mode)

    @classmethod
    def _filter_time_series_by_range(
        cls,
        df: pd.DataFrame,
        start_value: str,
        end_value: str,
        minute_mode: bool,
    ) -> pd.DataFrame:
        return TimeSeriesCache.filter_by_range(df, start_value, end_value, minute_mode)

    @classmethod
    def _covers_range(
        cls,
        df: pd.DataFrame,
        start_value: str,
        end_value: str,
        minute_mode: bool,
    ) -> bool:
        return TimeSeriesCache.covers_range(df, start_value, end_value, minute_mode)

    @classmethod
    def _missing_ranges(
        cls,
        df: pd.DataFrame,
        start_value: str,
        end_value: str,
        minute_mode: bool,
    ) -> list[tuple[str, str]]:
        return TimeSeriesCache.missing_ranges(df, start_value, end_value, minute_mode)

    @staticmethod
    def _extract_ts(df: pd.DataFrame) -> tuple[str | None, pd.Series | None]:
        date_col = None
        for candidate in ("时间", "日期", "date", "datetime", "trade_date", "day"):
            if candidate in df.columns:
                date_col = candidate
                break
        if date_col is None:
            return None, None
        ts = DataService._parse_datetime_series(df[date_col])
        if ts.notna().sum() == 0:
            return None, None
        return date_col, ts

    @staticmethod
    def _range_text_to_ts(value: str, minute_mode: bool) -> pd.Timestamp | None:
        try:
            if minute_mode:
                return pd.to_datetime(value, errors="coerce")
            return pd.to_datetime(value, format="%Y%m%d", errors="coerce")
        except Exception:
            return None

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
    def _ts_to_range_text(ts: pd.Timestamp, minute_mode: bool) -> str:
        if minute_mode:
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        return ts.strftime("%Y%m%d")

    @staticmethod
    def _as_compact_date(value: str) -> str:
        """
        Accepts YYYY-MM-DD or YYYYMMDD and returns YYYYMMDD.
        """
        raw = value.strip()
        if not raw:
            raise DataServiceError("日期不能为空")
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                parsed = datetime.strptime(raw, fmt)
                return parsed.strftime("%Y%m%d")
            except ValueError:
                continue
        raise DataServiceError(f"无效日期: {value}，请使用 YYYY-MM-DD")

    @staticmethod
    def _as_dash_date(value: str) -> str:
        return datetime.strptime(value, "%Y%m%d").strftime("%Y-%m-%d")

    @staticmethod
    def _as_datetime_text(value: str, *, start_of_day: bool) -> str:
        raw = value.strip()
        if not raw:
            raise DataServiceError("日期不能为空")
        formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d")
        for fmt in formats:
            try:
                parsed = datetime.strptime(raw, fmt)
                if fmt != "%Y-%m-%d %H:%M:%S":
                    if start_of_day:
                        parsed = parsed.replace(hour=9, minute=30, second=0)
                    else:
                        parsed = parsed.replace(hour=15, minute=0, second=0)
                return parsed.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        raise DataServiceError(f"无效日期时间: {value}")

    @staticmethod
    def _normalize_period(period: str) -> str:
        raw = (period or "daily").strip().lower()
        if raw in {"1", "5", "15", "30", "60", "daily", "weekly", "monthly"}:
            return raw
        return "daily"

    @staticmethod
    def _is_minute_period(period: str) -> bool:
        return period in {"1", "5", "15", "30", "60"}

    @staticmethod
    def _period_label(period: str) -> str:
        mapping = {
            "1": "1分钟",
            "5": "5分钟",
            "15": "15分钟",
            "30": "30分钟",
            "60": "60分钟",
            "daily": "日线",
            "weekly": "周线",
            "monthly": "月线",
        }
        return mapping.get(period, "日线")

    @staticmethod
    def _minute_window_days(period: str) -> int:
        mapping = {
            "1": 10,
            "5": 45,
            "15": 120,
            "30": 240,
            "60": 365,
        }
        return int(mapping.get(period, 90))

    @classmethod
    def _clip_minute_range(cls, start_value: str, end_value: str, *, period: str) -> tuple[str, str]:
        start_ts = pd.to_datetime(start_value, errors="coerce")
        end_ts = pd.to_datetime(end_value, errors="coerce")
        if pd.isna(start_ts) or pd.isna(end_ts) or start_ts > end_ts:
            return start_value, end_value
        max_days = cls._minute_window_days(period)
        min_start = (end_ts.normalize() - pd.Timedelta(days=max_days)).replace(hour=9, minute=30, second=0)
        clipped_start = max(start_ts, min_start)
        return (
            clipped_start.strftime("%Y-%m-%d %H:%M:%S"),
            end_ts.strftime("%Y-%m-%d %H:%M:%S"),
        )

    @classmethod
    def _clip_end_for_period(cls, end_value: str, *, period: str) -> str:
        if period == "daily":
            return end_value
        end_ts = pd.to_datetime(end_value, format="%Y%m%d", errors="coerce")
        if pd.isna(end_ts):
            return end_value
        day = pd.Timestamp(end_ts).normalize()
        now = pd.Timestamp.now()
        today = now.normalize()

        if period == "weekly":
            reference = day
            same_week = (
                int(reference.isocalendar().year) == int(today.isocalendar().year)
                and int(reference.isocalendar().week) == int(today.isocalendar().week)
            )
            if same_week:
                if today.dayofweek < 4:
                    reference = today
                elif today.dayofweek == 4 and (now.hour, now.minute) < (
                    cls.DAILY_CLOSE_READY_HOUR,
                    cls.DAILY_CLOSE_READY_MINUTE,
                ):
                    reference = today - pd.Timedelta(days=1)
            last_friday = reference - pd.Timedelta(days=(reference.dayofweek - 4) % 7)
            while last_friday.dayofweek >= 5:
                last_friday -= pd.Timedelta(days=1)
            return last_friday.strftime("%Y%m%d")

        if period == "monthly":
            reference = day
            month_end = reference + pd.offsets.MonthEnd(0)
            current_month = (reference.year == today.year) and (reference.month == today.month)
            month_closed = (
                current_month
                and (
                    (today > month_end)
                    or (
                        today == month_end
                        and (
                            today.dayofweek >= 5
                            or (now.hour, now.minute)
                            >= (cls.DAILY_CLOSE_READY_HOUR, cls.DAILY_CLOSE_READY_MINUTE)
                        )
                    )
                )
            )
            if current_month and not month_closed:
                target = reference + pd.offsets.MonthEnd(-1)
            else:
                target = month_end
            target = pd.Timestamp(target).normalize()
            while target.dayofweek >= 5:
                target -= pd.Timedelta(days=1)
            return target.strftime("%Y%m%d")

        return end_value

    @classmethod
    def _series_fresh_hash(cls, params: dict[str, str], minute_mode: bool) -> str:
        period = str(params.get("period", "daily"))
        payload = {
            "v": 1,
            "dataset": params.get("dataset", ""),
            "symbol": params.get("symbol", ""),
            "period": period,
            "mode": "minute" if minute_mode else "daily",
            "checkpoint": cls._market_checkpoint(minute_mode=minute_mode, period=period),
        }
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(text.encode("utf-8")).hexdigest()

    @classmethod
    def _build_series_meta(cls, df: pd.DataFrame, *, minute_mode: bool, fresh_hash: str) -> dict[str, object]:
        _, ts = cls._extract_ts(df)
        ts_clean = ts.dropna() if ts is not None else None
        ts_min = ts_clean.min() if ts_clean is not None and not ts_clean.empty else None
        ts_max = ts_clean.max() if ts_clean is not None and not ts_clean.empty else None
        return {
            "fresh_hash": fresh_hash,
            "rows": int(len(df.index)),
            "ts_min": None if ts_min is None or pd.isna(ts_min) else pd.Timestamp(ts_min).isoformat(),
            "ts_max": None if ts_max is None or pd.isna(ts_max) else pd.Timestamp(ts_max).isoformat(),
            "minute_mode": bool(minute_mode),
        }

    @classmethod
    def _market_checkpoint(cls, *, minute_mode: bool, period: str = "daily") -> str:
        if minute_mode:
            return cls._minute_checkpoint()
        if period == "weekly":
            return cls._weekly_checkpoint()
        if period == "monthly":
            return cls._monthly_checkpoint()
        return cls._daily_checkpoint()

    @classmethod
    def _daily_checkpoint(cls) -> str:
        now = pd.Timestamp.now()
        day = now.normalize()
        if day.dayofweek >= 5:
            while day.dayofweek >= 5:
                day -= pd.Timedelta(days=1)
            return day.strftime("%Y%m%d")
        if (now.hour, now.minute) < (cls.DAILY_CLOSE_READY_HOUR, cls.DAILY_CLOSE_READY_MINUTE):
            day -= pd.Timedelta(days=1)
        while day.dayofweek >= 5:
            day -= pd.Timedelta(days=1)
        return day.strftime("%Y%m%d")

    @staticmethod
    def _minute_checkpoint() -> str:
        now = pd.Timestamp.now()
        day = now.normalize()

        def to_trade_day(d: pd.Timestamp) -> pd.Timestamp:
            t = d
            while t.dayofweek >= 5:
                t -= pd.Timedelta(days=1)
            return t

        if day.dayofweek >= 5:
            trade_day = to_trade_day(day)
            checkpoint = trade_day + pd.Timedelta(hours=15)
            return checkpoint.strftime("%Y%m%d%H%M")

        if (now.hour, now.minute) < (9, 30):
            trade_day = to_trade_day(day - pd.Timedelta(days=1))
            checkpoint = trade_day + pd.Timedelta(hours=15)
            return checkpoint.strftime("%Y%m%d%H%M")
        if (now.hour, now.minute) < (11, 30):
            return now.floor("min").strftime("%Y%m%d%H%M")
        if (now.hour, now.minute) < (13, 0):
            checkpoint = day + pd.Timedelta(hours=11, minutes=30)
            return checkpoint.strftime("%Y%m%d%H%M")
        if (now.hour, now.minute) < (15, 0):
            return now.floor("min").strftime("%Y%m%d%H%M")
        checkpoint = day + pd.Timedelta(hours=15)
        return checkpoint.strftime("%Y%m%d%H%M")

    @classmethod
    def _weekly_checkpoint(cls) -> str:
        now = pd.Timestamp.now()
        day = now.normalize()
        if day.dayofweek < 4:
            day -= pd.Timedelta(days=day.dayofweek + 3)
        elif day.dayofweek == 4 and (now.hour, now.minute) < (
            cls.DAILY_CLOSE_READY_HOUR,
            cls.DAILY_CLOSE_READY_MINUTE,
        ):
            day -= pd.Timedelta(days=7)
        elif day.dayofweek >= 5:
            day -= pd.Timedelta(days=day.dayofweek - 4)
        return day.strftime("%Y%m%d")

    @classmethod
    def _monthly_checkpoint(cls) -> str:
        now = pd.Timestamp.now()
        day = now.normalize()
        month_end = day + pd.offsets.MonthEnd(0)
        if day == month_end and day.dayofweek < 5 and (now.hour, now.minute) >= (
            cls.DAILY_CLOSE_READY_HOUR,
            cls.DAILY_CLOSE_READY_MINUTE,
        ):
            target = month_end
        elif day == month_end and day.dayofweek >= 5:
            target = month_end
        else:
            target = day + pd.offsets.MonthEnd(-1)
        target = pd.Timestamp(target).normalize()
        while target.dayofweek >= 5:
            target -= pd.Timedelta(days=1)
        return target.strftime("%Y%m%d")

    @staticmethod
    def _normalize_stock_symbol(symbol: str) -> str:
        raw = symbol.strip().lower()
        if raw.startswith(("sh", "sz", "bj")):
            return raw
        if raw.startswith(("6", "5", "9")):
            return f"sh{raw}"
        if raw.startswith("8") or raw.startswith("4"):
            return f"bj{raw}"
        return f"sz{raw}"

    @staticmethod
    def _normalize_index_symbol(symbol: str) -> str:
        raw = symbol.strip().lower()
        if raw.startswith(("sh", "sz")):
            return raw
        if raw.startswith(("399", "980")):
            return f"sz{raw}"
        return f"sh{raw}"

    @staticmethod
    def _canonical_market_symbol(symbol: str) -> str:
        raw = str(symbol or "").strip().lower()
        if not raw:
            return ""
        code = raw.replace("sh", "").replace("sz", "").replace("bj", "")
        if code.isdigit() and len(code) == 6:
            return code
        return raw

    @staticmethod
    def _range_span_days(start_value: str, end_value: str, minute_mode: bool) -> int | None:
        try:
            if minute_mode:
                start_ts = pd.to_datetime(start_value, errors="coerce")
                end_ts = pd.to_datetime(end_value, errors="coerce")
            else:
                start_ts = pd.to_datetime(start_value, format="%Y%m%d", errors="coerce")
                end_ts = pd.to_datetime(end_value, format="%Y%m%d", errors="coerce")
            if pd.isna(start_ts) or pd.isna(end_ts):
                return None
            return max(0, int((end_ts - start_ts).days))
        except Exception:
            return None

    @staticmethod
    def _is_sparse_result(df: pd.DataFrame, span_days: int | None, minute_mode: bool) -> bool:
        if df is None or df.empty:
            return True
        rows = len(df.index)
        if span_days is None:
            return rows <= 1
        if minute_mode:
            if span_days >= 2:
                return rows <= 8
            return rows <= 1
        if span_days >= 20:
            return rows <= 2
        if span_days >= 7:
            return rows <= 1
        return False

    @classmethod
    def _is_valid_daily_series(cls, df: pd.DataFrame, span_days: int | None) -> bool:
        if df is None or df.empty:
            return False
        _, ts = cls._extract_ts(df)
        if ts is None:
            return True
        ts = ts.dropna().sort_values().drop_duplicates()
        if ts.empty:
            return False

        rows = int(len(ts.index))
        observed_days = max(1, int((ts.iloc[-1] - ts.iloc[0]).days))
        if observed_days < 365:
            return rows >= 20

        density = rows / observed_days
        max_gap = int(ts.diff().dt.days.max() or 0)

        if observed_days >= 365 * 3:
            if density < 0.35:
                return False
            if max_gap > 45:
                return False
        elif observed_days >= 365 * 2:
            if density < 0.30:
                return False
            if max_gap > 60:
                return False

        # Requested window may be much larger than listing age; avoid false invalidation.
        if span_days is not None and span_days >= 365 * 3 and observed_days >= 365 and rows < 180:
            return False
        return True

    @classmethod
    def _effective_start_for_cached_non_minute(cls, start_value: str, cached_df: pd.DataFrame) -> str:
        _, ts = cls._extract_ts(cached_df)
        if ts is None or ts.dropna().empty:
            return start_value
        req_start = cls._range_text_to_ts(start_value, minute_mode=False)
        if req_start is None:
            return start_value
        ts_min = ts.dropna().min()
        if pd.isna(ts_min) or ts_min <= req_start:
            return start_value
        return cls._ts_to_range_text(pd.Timestamp(ts_min), minute_mode=False)

    def _fetch_non_daily_from_daily_sources(
        self,
        *,
        period: str,
        start_date: str,
        end_date: str,
        loaders: list[tuple[str, Callable[[], pd.DataFrame]]],
        errors: list[str],
    ) -> pd.DataFrame | None:
        for source, loader in loaders:
            try:
                daily_df = loader()
                daily_filtered = self._filter_by_date(daily_df, start_date, end_date)
                if daily_filtered is None or daily_filtered.empty:
                    errors.append(f"{source}:sparse_result")
                    continue
                resampled = self._resample_daily_to_period(daily_filtered, period=period)
                if resampled is not None and not resampled.empty:
                    return resampled
                errors.append(f"{source}:resample_empty")
            except Exception as exc:
                errors.append(f"{source}:{exc}")
        return None

    @classmethod
    def _resample_daily_to_period(cls, df: pd.DataFrame, *, period: str) -> pd.DataFrame:
        mode = cls._normalize_period(period)
        if mode not in {"weekly", "monthly"}:
            return cls._normalize_time_series(df, minute_mode=False)
        if df is None or df.empty:
            return pd.DataFrame()

        date_col = cls._first_existing_column(df, ("日期", "date", "trade_date", "datetime", "时间"))
        if date_col is None:
            return pd.DataFrame()

        open_col = cls._first_existing_column(df, ("开盘", "open", "open_price"))
        high_col = cls._first_existing_column(df, ("最高", "high", "high_price"))
        low_col = cls._first_existing_column(df, ("最低", "low", "low_price"))
        close_col = cls._first_existing_column(df, ("收盘", "收盘价", "close", "latest"))
        volume_col = cls._first_existing_column(df, ("成交量", "volume", "vol"))
        amount_col = cls._first_existing_column(df, ("成交额", "amount", "turnover"))

        if close_col is None:
            return pd.DataFrame()

        safe = df.copy()
        safe["_resample_ts_"] = cls._parse_datetime_series(safe[date_col])
        safe = safe.dropna(subset=["_resample_ts_"])
        if safe.empty:
            return pd.DataFrame()
        safe = safe.sort_values("_resample_ts_")

        numeric_cols = [col for col in (open_col, high_col, low_col, close_col, volume_col, amount_col) if col is not None]
        for col in numeric_cols:
            safe[col] = pd.to_numeric(safe[col], errors="coerce")

        safe["_grp_"] = (
            safe["_resample_ts_"].dt.to_period("W-FRI")
            if mode == "weekly"
            else safe["_resample_ts_"].dt.to_period("M")
        )
        grouped = safe.groupby("_grp_", sort=True, observed=False)

        out = pd.DataFrame(
            {
                "日期": grouped["_resample_ts_"].max(),
                "收盘": grouped[close_col].apply(cls._last_valid),
            }
        )
        if open_col is not None:
            out["开盘"] = grouped[open_col].apply(cls._first_valid)
        if high_col is not None:
            out["最高"] = grouped[high_col].max()
        if low_col is not None:
            out["最低"] = grouped[low_col].min()
        if volume_col is not None:
            out["成交量"] = grouped[volume_col].sum(min_count=1)
        if amount_col is not None:
            out["成交额"] = grouped[amount_col].sum(min_count=1)

        out = out.dropna(subset=["日期", "收盘"])
        if out.empty:
            return pd.DataFrame()
        out["日期"] = cls._parse_datetime_series(out["日期"]).dt.strftime("%Y-%m-%d")
        out = out.dropna(subset=["日期"]).reset_index(drop=True)
        return out

    @staticmethod
    def _first_existing_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
        for name in candidates:
            if name in df.columns:
                return name
        return None

    @staticmethod
    def _first_valid(series: pd.Series) -> float | None:
        values = pd.to_numeric(series, errors="coerce").dropna()
        if values.empty:
            return None
        return float(values.iloc[0])

    @staticmethod
    def _last_valid(series: pd.Series) -> float | None:
        values = pd.to_numeric(series, errors="coerce").dropna()
        if values.empty:
            return None
        return float(values.iloc[-1])

    @staticmethod
    def _split_minute_ranges(
        start_value: str,
        end_value: str,
        *,
        chunk_days: int = 20,
    ) -> list[tuple[str, str]]:
        try:
            start_ts = pd.to_datetime(start_value, errors="coerce")
            end_ts = pd.to_datetime(end_value, errors="coerce")
            if pd.isna(start_ts) or pd.isna(end_ts) or start_ts > end_ts:
                return [(start_value, end_value)]
            step = pd.Timedelta(days=max(1, int(chunk_days)))
            cursor = start_ts
            ranges: list[tuple[str, str]] = []
            while cursor <= end_ts:
                chunk_end = min(cursor + step, end_ts)
                ranges.append(
                    (
                        cursor.strftime("%Y-%m-%d %H:%M:%S"),
                        chunk_end.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                )
                cursor = chunk_end + pd.Timedelta(minutes=1)
            return ranges if ranges else [(start_value, end_value)]
        except Exception:
            return [(start_value, end_value)]

    @staticmethod
    def _filter_by_date(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        date_col = None
        for candidate in ("date", "日期"):
            if candidate in df.columns:
                date_col = candidate
                break
        if date_col is None:
            return df
        start = pd.to_datetime(start_date, format="%Y%m%d")
        end = pd.to_datetime(end_date, format="%Y%m%d")
        date_series = DataService._parse_datetime_series(df[date_col])
        mask = date_series.between(start, end)
        return df.loc[mask].reset_index(drop=True)

    @staticmethod
    def _filter_by_datetime(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        date_col = None
        for candidate in ("时间", "日期", "date", "datetime", "trade_time", "day"):
            if candidate in df.columns:
                date_col = candidate
                break
        if date_col is None:
            return df
        start = pd.to_datetime(start_date, errors="coerce")
        end = pd.to_datetime(end_date, errors="coerce")
        if pd.isna(start) or pd.isna(end):
            return df
        date_series = DataService._parse_datetime_series(df[date_col])
        mask = date_series.between(start, end)
        filtered = df.loc[mask].reset_index(drop=True)
        return filtered if not filtered.empty else df
