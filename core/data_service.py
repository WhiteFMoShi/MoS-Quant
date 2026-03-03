from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading
import time
from typing import Callable
from collections import OrderedDict

import akshare as ak
import pandas as pd
import requests
from requests.exceptions import RequestException

from core.cache_paths import cache_root, legacy_cache_roots
from core.parquet_compat import has_parquet_engine
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
    # Many upstream daily endpoints lag after 15:00; using too-early checkpoints
    # causes false "stale" errors right after close.
    DAILY_CLOSE_READY_MINUTE = 35
    _TRADE_DATES: set[str] | None = None
    _TRADE_DATES_MAX_DAY: pd.Timestamp | None = None
    _TRADE_DATES_REFRESHING: bool = False
    _TRADE_DATES_SYNC_LOCK = threading.Lock()
    CHECKPOINT_MAX_LAG_DAYS = 45

    def __init__(self, cache_dir: Path | None = None) -> None:
        self._cache_dir = cache_dir or (cache_root() / "datasets")
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._series_cache = SeriesCacheManager(self._cache_dir)
        self._adopt_legacy_cache_files("datasets")
        self._cleanup_obsolete_series_cache_files()
        self._ensure_trade_dates_cache_refresh()
        self._minute_window_mem: OrderedDict[tuple[str, str, str], tuple[pd.DataFrame, float]] = OrderedDict()
        self._minute_window_mem_limit = 6
        self._minute_window_mem_ttl_seconds = 25.0

    def _call_df_with_timeout(self, loader: Callable[[], pd.DataFrame], *, timeout_seconds: float = 12.0) -> pd.DataFrame:
        payload: dict[str, object] = {}
        error: dict[str, Exception] = {}

        def runner() -> None:
            try:
                payload["df"] = loader()
            except Exception as exc:
                error["exc"] = exc

        t = threading.Thread(target=runner, daemon=True)
        t.start()
        t.join(timeout=max(0.2, float(timeout_seconds)))
        if t.is_alive():
            raise TimeoutError("timeout")
        if "exc" in error:
            raise error["exc"]
        df = payload.get("df")
        if not isinstance(df, pd.DataFrame):
            return pd.DataFrame()
        return df

    def _get_minute_window_cached(self, *, key: tuple[str, str, str], loader: Callable[[], pd.DataFrame]) -> pd.DataFrame:
        cached_item = self._minute_window_mem.get(key)
        if cached_item is not None:
            cached_df, cached_ts = cached_item
            if cached_df is not None and not cached_df.empty:
                # If the cached snapshot looks extremely old, bypass TTL and refetch.
                # This prevents rare stale snapshots from freezing the UI for ~TTL seconds.
                if self._minute_window_snapshot_suspiciously_stale(cached_df):
                    try:
                        del self._minute_window_mem[key]
                    except KeyError:
                        pass
                else:
                    try:
                        cache_age = float(time.time() - float(cached_ts))
                    except Exception:
                        cache_age = float(self._minute_window_mem_ttl_seconds) + 1.0
                    # Window minute endpoints are dynamic; keep a short TTL to avoid freezing tails
                    # when the app stays open for a long time.
                    if cache_age <= float(self._minute_window_mem_ttl_seconds):
                        self._minute_window_mem.move_to_end(key, last=True)
                        return cached_df

        df = loader()
        if df is None:
            df = pd.DataFrame()
        # Do not memoize extremely stale snapshots; allow next call to refetch immediately.
        if not self._minute_window_snapshot_suspiciously_stale(df):
            self._minute_window_mem[key] = (df, float(time.time()))
            self._minute_window_mem.move_to_end(key, last=True)
        while len(self._minute_window_mem) > int(self._minute_window_mem_limit):
            self._minute_window_mem.popitem(last=False)
        return df

    @staticmethod
    def _minute_window_snapshot_suspiciously_stale(df: pd.DataFrame, *, max_lag_days: int = 10) -> bool:
        if df is None or df.empty:
            return False
        try:
            _, ts = DataService._extract_ts(df)
            if ts is None:
                return False
            ts_clean = ts.dropna()
            if ts_clean.empty:
                return False
            tail = pd.Timestamp(ts_clean.max())
            now = pd.Timestamp.now()
            return (now - tail) > pd.Timedelta(days=max(3, int(max_lag_days)))
        except Exception:
            return False

    def _ensure_trade_dates_cache_refresh(self) -> None:
        if DataService._TRADE_DATES_REFRESHING:
            return
        if not DataService._trade_dates_cache_needs_refresh():
            return
        DataService._TRADE_DATES_REFRESHING = True
        threading.Thread(target=DataService._refresh_trade_dates_cache_worker, daemon=True).start()

    @classmethod
    def _trade_dates_look_valid(cls, ts: pd.Series, *, now: pd.Timestamp) -> bool:
        """
        Validate trade calendar quality.
        Reject calendars that:
        - end too far in the past (stale)
        - are too sparse in recent years (often indicates parsing wrong column)
        """
        ts_clean = cls._normalize_trade_dates(ts)
        if ts_clean.empty:
            return False
        now_day = pd.Timestamp(now).normalize()
        ts_max = pd.Timestamp(ts_clean.max()).normalize()
        if not cls._trade_dates_structure_ok(ts_clean, now=now_day):
            return False
        # During long holidays, the latest trade day may lag behind "today" by >5 days.
        # Only treat as stale when it is *very* far behind (typically indicates parsing/source issue).
        if ts_max < (now_day - pd.Timedelta(days=20)):
            return False
        return True

    @classmethod
    def _normalize_trade_dates(cls, ts: pd.Series) -> pd.Series:
        try:
            return pd.to_datetime(ts, errors="coerce").dropna().dt.normalize().drop_duplicates()
        except Exception:
            return pd.Series(dtype="datetime64[ns]")

    @classmethod
    def _trade_dates_structure_ok(cls, ts_clean: pd.Series, *, now: pd.Timestamp) -> bool:
        """
        Validate calendar *structure* without requiring it to be up-to-date.

        This is used when loading a potentially stale on-disk calendar so we can still:
        - keep a coverage boundary (max day) for holiday freshness guards
        - avoid accepting obviously wrong/sparse calendars (e.g. wrong column parsed)
        """
        if ts_clean is None or len(ts_clean.index) == 0:
            return False
        try:
            now_day = pd.Timestamp(now).normalize()
            ts_min = pd.Timestamp(ts_clean.min()).normalize()
            ts_max = pd.Timestamp(ts_clean.max()).normalize()
        except Exception:
            return False

        # Synthetic weekday calendars tend to start at 1990-01-01 and extend far into the future.
        if ts_min < pd.Timestamp("1990-12-01"):
            return False
        if ts_max > (now_day + pd.Timedelta(days=370)):
            return False

        # Too-small calendars are rarely real trade calendars.
        if int(len(ts_clean.index)) < 3000:
            return False

        # The last full year should have a realistic amount of trading days.
        last_year = int(now_day.year) - 1
        if last_year < 1991:
            return False
        last_year_start = pd.Timestamp(f"{last_year}-01-01")
        last_year_end = pd.Timestamp(f"{last_year}-12-31")
        if ts_max < last_year_start:
            return False
        last_year_days = ts_clean[(ts_clean >= last_year_start) & (ts_clean <= last_year_end)]
        if int(len(last_year_days.index)) < 180:
            return False

        # If the calendar claims to cover the current year, it should not be extremely sparse.
        this_year_start = pd.Timestamp(f"{now_day.year}-01-01")
        if ts_max >= this_year_start and int(now_day.month) >= 2:
            this_year_days = ts_clean[(ts_clean >= this_year_start) & (ts_clean <= now_day)]
            if int(len(this_year_days.index)) < 10:
                return False

        return True

    @classmethod
    def _trade_dates_cache_needs_refresh(cls) -> bool:
        market_dir = cache_root() / "market"
        pickle_path = market_dir / "trade_dates.pkl"
        parquet_path = market_dir / "trade_dates.parquet"
        candidates: list[Path] = []
        if pickle_path.exists():
            candidates.append(pickle_path)
        if parquet_path.exists() and has_parquet_engine():
            candidates.append(parquet_path)
        if not candidates:
            return True
        for path in candidates:
            try:
                df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_pickle(path)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            col = "trade_date" if "trade_date" in df.columns else df.columns[0]
            ts = pd.to_datetime(df[col], errors="coerce").dropna()
            if ts.empty:
                continue
            now = pd.Timestamp.now().normalize()
            ts_min = pd.Timestamp(ts.min()).normalize()
            ts_max = pd.Timestamp(ts.max()).normalize()
            # Weekday-generated placeholders tend to start at 1990-01-01 and extend years into the future.
            if ts_min < pd.Timestamp("1990-12-01"):
                return True
            if ts_max > (now + pd.Timedelta(days=370)):
                return True
            if not cls._trade_dates_look_valid(ts, now=now):
                return True
            return False
        return True

    @classmethod
    def _persist_trade_dates_snapshot(cls, *, source: str, dates: list[str], ts_max: pd.Timestamp) -> None:
        if not dates:
            return
        market_dir = cache_root() / "market"
        market_dir.mkdir(parents=True, exist_ok=True)
        pickle_path = market_dir / "trade_dates.pkl"
        parquet_path = market_dir / "trade_dates.parquet"
        meta_path = market_dir / "trade_dates.meta.json"

        # Avoid overwriting a better existing cache with a worse snapshot.
        try:
            existing = None
            if pickle_path.exists():
                existing = pd.read_pickle(pickle_path)
            elif parquet_path.exists() and has_parquet_engine():
                existing = pd.read_parquet(parquet_path)
            if existing is not None and not existing.empty:
                col = "trade_date" if "trade_date" in existing.columns else existing.columns[0]
                old_ts = cls._normalize_trade_dates(existing[col])
                if not old_ts.empty and pd.Timestamp(old_ts.max()).normalize() > pd.Timestamp(ts_max).normalize():
                    return
        except Exception:
            pass

        cache_df = pd.DataFrame({"trade_date": pd.Series(dates, dtype="object")})
        try:
            cache_df.to_pickle(pickle_path)
        except Exception:
            pass
        if has_parquet_engine():
            try:
                cache_df.to_parquet(parquet_path, index=False)
            except Exception:
                try:
                    if parquet_path.exists():
                        parquet_path.unlink()
                except Exception:
                    pass
        try:
            meta = {
                "source": str(source),
                "rows": int(len(dates)),
                "ts_max": pd.Timestamp(ts_max).strftime("%Y-%m-%d"),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    @classmethod
    def _fetch_trade_dates_remote_best(
        cls,
        *,
        timeout_seconds: float,
        now: pd.Timestamp | None = None,
    ) -> tuple[str, pd.Timestamp, list[str]]:
        """
        Fetch a fresh (up-to-date) trade calendar from network within a time budget.
        Returns (source, ts_max, dates_list).
        """
        now_day = pd.Timestamp(now or pd.Timestamp.now()).normalize()
        deadline = time.monotonic() + max(0.2, float(timeout_seconds))

        def call_with_timeout(loader: Callable[[], pd.DataFrame], timeout_seconds: float) -> tuple[pd.DataFrame, str | None]:
            payload: dict[str, object] = {}
            error: dict[str, Exception] = {}

            def runner() -> None:
                try:
                    payload["df"] = loader()
                except Exception as exc:
                    error["exc"] = exc

            t = threading.Thread(target=runner, daemon=True)
            t.start()
            t.join(timeout=max(0.2, float(timeout_seconds)))
            if t.is_alive():
                return pd.DataFrame(), "timeout"
            if "exc" in error:
                return pd.DataFrame(), f"{type(error['exc']).__name__}:{error['exc']}"
            df = payload.get("df")
            if not isinstance(df, pd.DataFrame):
                return pd.DataFrame(), "invalid_df"
            return df, None

        def fetch_sina_calendar_via_subprocess(budget_seconds: float) -> tuple[str, pd.Timestamp, list[str]] | None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            budget = min(max(0.2, float(budget_seconds)), remaining)
            code = r"""
import json, datetime
import requests
import pandas as pd
from akshare.stock.cons import hk_js_decode
import py_mini_racer

url = "https://finance.sina.com.cn/realstock/company/klc_td_sh.txt"
r = requests.get(url, timeout=6)
text = r.text
payload = text.split("=", 1)[1].split(";", 1)[0].replace('"', "")
js_code = py_mini_racer.MiniRacer()
js_code.eval(hk_js_decode)
raw = js_code.call("d", payload)
out = []
for item in (raw or []):
    if isinstance(item, dict):
        v = item.get("date") or item.get("trade_date") or item.get("day")
    else:
        v = item
    if v is None:
        continue
    out.append(str(v)[:10])
# Sina calendar misses 1992-05-04; align with akshare behavior
out.append("1992-05-04")
out = sorted(set(out))
print(json.dumps(out, ensure_ascii=False))
"""
            try:
                proc = subprocess.run(
                    [sys.executable, "-c", code],
                    capture_output=True,
                    text=True,
                    timeout=float(budget),
                    check=False,
                )
            except subprocess.TimeoutExpired:
                return None
            if proc.returncode != 0:
                return None
            try:
                dates = json.loads((proc.stdout or "").strip() or "[]")
            except Exception:
                return None
            if not isinstance(dates, list) or not dates:
                return None
            ts = cls._normalize_trade_dates(pd.Series(dates, dtype="object"))
            if ts.empty or (not cls._trade_dates_look_valid(ts, now=now_day)):
                return None
            dates_list = sorted(set(ts.dt.strftime("%Y-%m-%d").tolist()))
            ts_max = pd.Timestamp(ts.max()).normalize()
            return ("sina_trade_calendar", ts_max, dates_list)

        loaders: tuple[tuple[str, Callable[[], pd.DataFrame], float], ...] = (
            # Avoid in-process py_mini_racer (can be unstable under GUI threads); use subprocess.
            ("sina_trade_calendar(subprocess)", lambda: pd.DataFrame(), 6.5),
            # Fallback: infer calendar from Tencent daily (no py_mini_racer, but can be slow).
            ("tencent_index_daily_calendar", lambda: ak.stock_zh_index_daily_tx(symbol="sh000001"), 3.0),
        )

        errors: list[str] = []
        best: tuple[str, pd.Timestamp, list[str]] | None = None

        # 1) Sina calendar via subprocess (preferred).
        cand = fetch_sina_calendar_via_subprocess(6.5)
        if cand is not None:
            return cand

        for source_name, loader, per_source_budget in loaders:
            if source_name.startswith("sina_trade_calendar"):
                # already tried via subprocess
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            budget = min(float(per_source_budget), max(0.2, remaining))
            df, err = call_with_timeout(loader, timeout_seconds=budget)
            if df is None or df.empty:
                errors.append(f"{source_name}:{err or 'empty'}")
                continue
            col = None
            for candidate in ("trade_date", "date", "日期", "datetime", "时间", "day"):
                if candidate in df.columns:
                    col = candidate
                    break
            if col is None:
                col = df.columns[0]
            ts = cls._normalize_trade_dates(df[col])
            if ts.empty:
                errors.append(f"{source_name}:empty_ts")
                continue
            if not cls._trade_dates_look_valid(ts, now=now_day):
                ts_max = pd.Timestamp(ts.max()).normalize() if not ts.empty else None
                errors.append(f"{source_name}:stale_or_sparse(max={ts_max})")
                continue
            dates_list = sorted(set(ts.dt.strftime("%Y-%m-%d").tolist()))
            if len(dates_list) < 3000:
                errors.append(f"{source_name}:too_few_rows({len(dates_list)})")
                continue
            ts_max = pd.Timestamp(ts.max()).normalize()
            cand = (str(source_name), ts_max, dates_list)
            if best is None or (cand[1] > best[1]) or (cand[1] == best[1] and len(cand[2]) > len(best[2])):
                best = cand
            # If we got a valid dedicated calendar, return immediately to save time budget.
            if source_name == "sina_trade_calendar":
                return cand

        if best is None:
            detail = " | ".join(errors) if errors else "no_source"
            raise TimeoutError(f"trade_dates fetch failed within {timeout_seconds:.1f}s: {detail}")
        return best

    @classmethod
    def _ensure_trade_dates_ready_sync(cls, *, timeout_seconds: float = 10.0) -> None:
        """
        Ensure trade dates are loaded (and preferably cached) for correctness.
        This runs in worker threads too; keep it best-effort and guarded.
        """
        with cls._TRADE_DATES_SYNC_LOCK:
            source, ts_max, dates_list = cls._fetch_trade_dates_remote_best(
                timeout_seconds=float(timeout_seconds),
                now=pd.Timestamp.now(),
            )
            cls._persist_trade_dates_snapshot(source=source, dates=dates_list, ts_max=ts_max)
            cls._TRADE_DATES = set(dates_list)
            cls._TRADE_DATES_MAX_DAY = pd.Timestamp(ts_max).normalize()

    @classmethod
    def _refresh_trade_dates_cache_worker(cls) -> None:
        try:
            source, ts_max, dates_list = cls._fetch_trade_dates_remote_best(timeout_seconds=18.0, now=pd.Timestamp.now())
            cls._persist_trade_dates_snapshot(source=source, dates=dates_list, ts_max=ts_max)
            cls._TRADE_DATES = None
            cls._TRADE_DATES_MAX_DAY = None
        finally:
            cls._TRADE_DATES_REFRESHING = False

    def fetch(
        self,
        request: FetchRequest,
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> FetchResponse:
        # Trade calendar is mandatory for correctness (holiday checks, minute freshness).
        dates = DataService._load_trade_dates()
        if (not dates) or (DataService._TRADE_DATES_MAX_DAY is None):
            raise DataServiceError("交易日历未准备好（需要网络拉取成功后才能下载）。请重启应用或检查网络。")
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
            if str(request.end_date or "").strip():
                end = self._as_datetime_text(request.end_date, start_of_day=False)
            else:
                checkpoint = self._minute_checkpoint_for_period(period)
                end_ts = pd.to_datetime(checkpoint, format="%Y%m%d%H%M", errors="coerce")
                end = (pd.Timestamp(end_ts) if not pd.isna(end_ts) else pd.Timestamp.now().floor("min")).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            if str(request.start_date or "").strip():
                start = self._as_datetime_text(request.start_date, start_of_day=True)
            else:
                start_ts = (pd.to_datetime(end, errors="coerce").normalize() - pd.Timedelta(days=self._minute_window_days(period))).replace(
                    hour=9, minute=30, second=0
                )
                start = pd.Timestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S")
            end = self._clip_minute_end_by_checkpoint(end, period=period)
            start, end = self._clip_minute_range(start, end, period=period)
            title = f"{title_prefix} {symbol} {start}-{end}"
            params["start_date"] = start
            params["end_date"] = end
            fetcher = lambda s, e: self._fetch_stock_minute(
                symbol=symbol,
                start_date=s,
                end_date=e,
                period=period,
                preferred_source=None,
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
                progress_cb=progress_cb,
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
            if str(request.end_date or "").strip():
                end = self._as_datetime_text(request.end_date, start_of_day=False)
            else:
                checkpoint = self._minute_checkpoint_for_period(period)
                end_ts = pd.to_datetime(checkpoint, format="%Y%m%d%H%M", errors="coerce")
                end = (pd.Timestamp(end_ts) if not pd.isna(end_ts) else pd.Timestamp.now().floor("min")).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            if str(request.start_date or "").strip():
                start = self._as_datetime_text(request.start_date, start_of_day=True)
            else:
                start_ts = (pd.to_datetime(end, errors="coerce").normalize() - pd.Timedelta(days=self._minute_window_days(period))).replace(
                    hour=9, minute=30, second=0
                )
                start = pd.Timestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S")
            end = self._clip_minute_end_by_checkpoint(end, period=period)
            start, end = self._clip_minute_range(start, end, period=period)
            title = f"{title_prefix} {symbol} {start}-{end}"
            params["start_date"] = start
            params["end_date"] = end
            fetcher = lambda s, e: self._fetch_index_minute(
                symbol=symbol,
                start_date=s,
                end_date=e,
                period=period,
                preferred_source=None,
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
                progress_cb=progress_cb,
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
        title_out = title
        self._emit_progress(progress_cb, 4, "检查缓存")
        parquet_path, pickle_path = self._series_cache.cache_paths(params)
        cached_df = pd.DataFrame()
        cached_path: Path | None = None
        span_days = self._range_span_days(start_value, end_value, minute_mode)
        validate_daily = (not minute_mode) and (request.period == "daily")
        period_mode = self._normalize_period(str(params.get("period", request.period or "daily")))
        range_start_value = start_value
        fresh_hash = self._series_fresh_hash(params, minute_mode)
        force_tail_refresh_due_stale = False
        minute_require_fresh = False
        if not request.force_refresh:
            meta = self._series_cache.load_meta(params)
            if meta is not None and str(meta.get("fresh_hash", "")) == fresh_hash:
                cached_df, cached_path = self._series_cache.load(params, minute_mode)
                if not cached_df.empty:
                    filtered = self._series_cache.filter_by_range(cached_df, range_start_value, end_value, minute_mode)
                    covered = self._series_cache.covers_range(cached_df, range_start_value, end_value, minute_mode)
                    fresh_enough = self._is_result_fresh_for_request_end(
                        filtered,
                        end_value=end_value,
                        period=period_mode,
                        minute_mode=minute_mode,
                    )
                    if covered and ((not validate_daily) or self._is_valid_daily_series(filtered, span_days)) and fresh_enough:
                        if minute_mode and not self._minute_left_coverage_ok(filtered, start_datetime=start_value):
                            self._emit_progress(progress_cb, 8, "缓存左侧缺口，继续增量")
                        else:
                            self._emit_progress(progress_cb, 100, "命中缓存(hash)")
                            return FetchResponse(
                                title=title,
                                dataframe=filtered,
                                cache_path=str(
                                    cached_path
                                    if cached_path is not None
                                    else (parquet_path if parquet_path.exists() else pickle_path)
                                ),
                                from_cache=True,
                            )
                    if covered and not fresh_enough:
                        self._emit_progress(progress_cb, 8, "缓存末端落后，继续增量")
            if cached_df.empty:
                cached_df, cached_path = self._series_cache.load(params, minute_mode)
            if minute_mode and (cached_df is not None) and (not cached_df.empty):
                lag_days = self._minute_tail_lag_days(cached_df, end_value=end_value)
                if lag_days is not None and int(lag_days) >= 7:
                    self._emit_progress(progress_cb, 8, f"分钟缓存末端严重滞后({lag_days}天)，自动失效重拉")
                    try:
                        self._series_cache.invalidate(params)
                    except Exception:
                        pass
                    cached_df = pd.DataFrame()
                    cached_path = None
                    minute_require_fresh = True
            if minute_mode and not cached_df.empty and self._is_low_density_minute_result(
                cached_df,
                span_days=span_days,
                period=period_mode,
            ):
                cached_df = pd.DataFrame()
                cached_path = None
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
                fresh_enough = self._is_result_fresh_for_request_end(
                    filtered,
                    end_value=end_value,
                    period=period_mode,
                    minute_mode=minute_mode,
                )
                if ((not validate_daily) or self._is_valid_daily_series(filtered, span_days)) and fresh_enough:
                    if minute_mode and not self._minute_left_coverage_ok(filtered, start_datetime=start_value):
                        self._emit_progress(progress_cb, 12, "缓存左侧缺口，准备修复")
                    else:
                        self._emit_progress(progress_cb, 100, "命中缓存")
                        return FetchResponse(
                            title=title,
                            dataframe=filtered,
                            cache_path=str(
                                cached_path
                                if cached_path is not None
                                else (parquet_path if parquet_path.exists() else pickle_path)
                            ),
                            from_cache=True,
                        )
                if not fresh_enough:
                    self._emit_progress(progress_cb, 12, "缓存覆盖但末端落后，准备更新")
                    force_tail_refresh_due_stale = True
                else:
                    force_full_refresh_due_invalid_daily = True
                    self._emit_progress(progress_cb, 12, "缓存覆盖但质量不足，准备修复")

        fetched_parts: list[pd.DataFrame] = []
        fetch_failures: list[str] = []
        ranges_to_fetch: list[tuple[str, str]]
        if request.force_refresh or cached_df.empty or force_full_refresh_due_invalid_daily:
            ranges_to_fetch = [(start_value, end_value)]
        elif force_tail_refresh_due_stale:
            ranges_to_fetch = self._build_tail_update_ranges(
                cached_df=cached_df,
                end_value=end_value,
                minute_mode=minute_mode,
            )
            if not ranges_to_fetch:
                filtered = self._series_cache.filter_by_range(cached_df, start_value, end_value, minute_mode)
                self._emit_progress(progress_cb, 100, "命中缓存")
                return FetchResponse(
                    title=title,
                    dataframe=filtered,
                    cache_path=str(cached_path if cached_path is not None else (parquet_path if parquet_path.exists() else pickle_path)),
                    from_cache=True,
                )
            self._emit_progress(progress_cb, 14, "执行末端增量更新")
        else:
            if minute_mode and not cached_df.empty:
                # Minute endpoints are often window-limited and unstable; prioritise tail-only incremental
                # to avoid repeated old-range chunk failures blocking the whole request.
                tail_ranges = self._build_tail_update_ranges(
                    cached_df=cached_df,
                    end_value=end_value,
                    minute_mode=True,
                )
                if tail_ranges:
                    ranges_to_fetch = tail_ranges
                    self._emit_progress(progress_cb, 14, "分钟模式: 末端增量更新")
                else:
                    filtered = self._series_cache.filter_by_range(cached_df, range_start_value, end_value, True)
                    self._emit_progress(progress_cb, 100, "命中缓存")
                    return FetchResponse(
                        title=title,
                        dataframe=filtered,
                        cache_path=str(
                            cached_path if cached_path is not None else (parquet_path if parquet_path.exists() else pickle_path)
                        ),
                        from_cache=True,
                    )
            else:
                ranges = self._series_cache.missing_ranges(cached_df, range_start_value, end_value, minute_mode)
                if not ranges:
                    filtered = self._series_cache.filter_by_range(cached_df, range_start_value, end_value, minute_mode)
                    fresh_enough = self._is_result_fresh_for_request_end(
                        filtered,
                        end_value=end_value,
                        period=period_mode,
                        minute_mode=minute_mode,
                    )
                    if ((not validate_daily) or self._is_valid_daily_series(filtered, span_days)) and fresh_enough:
                        if minute_mode and not self._minute_left_coverage_ok(filtered, start_datetime=start_value):
                            ranges_to_fetch = [(start_value, end_value)]
                            self._emit_progress(progress_cb, 14, "缓存左侧缺口，执行全区间修复")
                        else:
                            self._emit_progress(progress_cb, 100, "命中缓存")
                            return FetchResponse(
                                title=title,
                                dataframe=filtered,
                                cache_path=str(
                                    cached_path
                                    if cached_path is not None
                                    else (parquet_path if parquet_path.exists() else pickle_path)
                                ),
                                from_cache=True,
                            )
                    else:
                        ranges_to_fetch = [(start_value, end_value)]
                        self._emit_progress(progress_cb, 14, "缓存完整但质量不足，执行全区间修复")
                else:
                    ranges_to_fetch = ranges

        if minute_mode:
            expanded: list[tuple[str, str]] = []
            chunk_days = self._minute_chunk_days(
                period=period_mode,
                span_days=span_days,
                has_cache=not cached_df.empty,
            )
            for start_text, end_text in ranges_to_fetch:
                expanded.extend(
                    self._split_minute_ranges(
                        start_text,
                        end_text,
                        chunk_days=chunk_days,
                        period=period_mode,
                    )
                )
            # Prioritize recent chunks first to maximize probability of getting latest tail.
            ranges_to_fetch = list(reversed(expanded))

        total = len(ranges_to_fetch)
        if total > 0 and not cached_df.empty:
            self._emit_progress(progress_cb, 16, f"缓存增量区间 {total} 段")
        for idx, (start_text, end_text) in enumerate(ranges_to_fetch, start=1):
            phase = 18 + int((idx - 1) / max(total, 1) * 52)
            if total == 1:
                label = f"下载主数据 {start_text}~{end_text}"
            else:
                label = f"下载区间 {idx}/{total} {start_text}~{end_text}"
            self._emit_progress(progress_cb, phase, label)
            quick_incremental = (not request.force_refresh) and (not cached_df.empty)
            try:
                if minute_mode:
                    retries = 3 if quick_incremental else 3
                    delay_seconds = 0.3 if quick_incremental else 0.6
                else:
                    retries = 1 if quick_incremental else 3
                    delay_seconds = 0.2 if quick_incremental else 0.8
                fetched_parts.append(
                    self._call_with_retry(
                        fetch_func=lambda s=start_text, e=end_text: fetch_range_func(s, e),
                        action_name=f"{action_name} {start_text}-{end_text}",
                        retries=retries,
                        delay_seconds=delay_seconds,
                        progress_cb=progress_cb,
                        progress_percent=phase,
                        task_label=(f"区间{idx}/{total}" if total > 1 else "主区间"),
                    )
                )
            except Exception as exc:
                fetch_failures.append(str(exc))
                if cached_df.empty and (not minute_mode):
                    raise
                if cached_df.empty and minute_mode and total <= 1:
                    raise
        self._emit_progress(progress_cb, 72, "区间下载完成")

        merged = self._series_cache.merge(cached_df, fetched_parts, minute_mode)
        if merged.empty and not cached_df.empty:
            merged = cached_df
        filtered = self._series_cache.filter_by_range(merged, start_value, end_value, minute_mode)
        if filtered.empty and (not minute_mode) and (merged is not None) and (not merged.empty):
            # Some upstream tables use unexpected date column names/formats and may be excluded by strict range filter.
            # Keep usable non-minute payload instead of hard-failing the whole request.
            filtered = merged.reset_index(drop=True)

        needs_repair = self._is_sparse_result(filtered, span_days, minute_mode) or (
            validate_daily and (not self._is_valid_daily_series(filtered, span_days))
        )
        if minute_mode and (not filtered.empty) and (not self._minute_left_coverage_ok(filtered, start_datetime=start_value)):
            needs_repair = True
        if minute_mode and (not filtered.empty) and self._is_low_density_minute_result(
            filtered,
            span_days=span_days,
            period=period_mode,
        ):
            needs_repair = True
        attempt_repair = needs_repair
        if minute_mode and fetch_failures and ((not cached_df.empty) or (not filtered.empty)):
            # Keep fast path when we already have usable minute data from cache or fetched parts.
            attempt_repair = False
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
                repair_retries = 1 if minute_mode else 2
                if minute_mode and cached_df.empty and filtered.empty:
                    # All minute chunks failed with no usable payload; allow one extra rescue retry.
                    repair_retries = 2
                repaired = self._call_with_retry(
                    fetch_func=lambda: fetch_range_func(start_value, end_value),
                    action_name=f"{action_name} 全量修复",
                    retries=repair_retries,
                    delay_seconds=0.6 if minute_mode else 0.8,
                    progress_cb=progress_cb,
                    progress_percent=78,
                    task_label="全量修复",
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
            cached_filtered = self._series_cache.filter_by_range(cached_df, start_value, end_value, minute_mode)
            if self._is_result_fresh_for_request_end(
                cached_filtered,
                end_value=end_value,
                period=period_mode,
                minute_mode=minute_mode,
            ):
                filtered = cached_filtered
                merged = cached_df
                self._emit_progress(progress_cb, 84, "网络失败，回退新鲜缓存")
            else:
                detail = " | ".join(fetch_failures) if fetch_failures else "stale_tail"
                tail_ts = self._series_tail_timestamp(cached_filtered)
                tail_text = tail_ts.strftime("%Y-%m-%d %H:%M:%S") if tail_ts is not None else "--"
                raise RuntimeError(
                    f"all_sources_stale|{detail}|tail={tail_text}|end={end_value}|rows={len(cached_filtered.index)}"
                )
        if filtered.empty and cached_df.empty and fetch_failures:
            detail = " | ".join(fetch_failures)
            raise RuntimeError(detail)

        if validate_daily and not self._is_valid_daily_series(filtered, span_days):
            if not cached_df.empty and self._is_valid_daily_series(cached_df, span_days):
                cached_filtered = self._series_cache.filter_by_range(cached_df, start_value, end_value, minute_mode)
                if self._is_result_fresh_for_request_end(
                    cached_filtered,
                    end_value=end_value,
                    period=period_mode,
                    minute_mode=minute_mode,
                ):
                    filtered = cached_filtered
                    merged = cached_df

        if not filtered.empty:
            fresh_enough = self._is_result_fresh_for_request_end(
                filtered,
                end_value=end_value,
                period=period_mode,
                minute_mode=minute_mode,
            )
            if not fresh_enough:
                if not cached_df.empty:
                    cached_filtered = self._series_cache.filter_by_range(cached_df, start_value, end_value, minute_mode)
                    if self._is_result_fresh_for_request_end(
                        cached_filtered,
                        end_value=end_value,
                        period=period_mode,
                        minute_mode=minute_mode,
                    ):
                        filtered = cached_filtered
                        merged = cached_df
                        fresh_enough = True
            if not fresh_enough:
                if minute_mode:
                    lag_days = self._minute_tail_lag_days(filtered, end_value=end_value)
                    if minute_require_fresh or (lag_days is not None and int(lag_days) >= 7):
                        detail_base = " | ".join(fetch_failures) if fetch_failures else "stale_tail"
                        tail_ts = self._series_tail_timestamp(filtered)
                        tail_text = tail_ts.strftime("%Y-%m-%d %H:%M:%S") if tail_ts is not None else "--"
                        try:
                            checkpoint = self._minute_checkpoint_for_period(period_mode)
                        except Exception:
                            checkpoint = "--"
                        detail = f"{detail_base}|tail={tail_text}|end={end_value}|cp={checkpoint}|rows={len(filtered.index)}"
                        raise RuntimeError(f"all_sources_stale|{detail}")
                    tail_ts = self._series_tail_timestamp(filtered)
                    tail_text = tail_ts.strftime("%Y-%m-%d %H:%M:%S") if tail_ts is not None else "--"
                    title_out = f"{title_out}（分钟末端滞后: {tail_text}）"
                else:
                    detail_base = " | ".join(fetch_failures) if fetch_failures else "stale_tail"
                    tail_ts = self._series_tail_timestamp(filtered)
                    tail_text = tail_ts.strftime("%Y-%m-%d %H:%M:%S") if tail_ts is not None else "--"
                    try:
                        checkpoint = self._daily_checkpoint()
                    except Exception:
                        checkpoint = "--"
                    detail = f"{detail_base}|tail={tail_text}|end={end_value}|cp={checkpoint}|rows={len(filtered.index)}"
                    raise RuntimeError(f"all_sources_stale|{detail}")

        if minute_mode and (merged is not None) and (not merged.empty):
            try:
                covered = self._series_cache.covers_range(merged, start_value, end_value, minute_mode)
            except Exception:
                covered = True
            if not covered:
                _, ts = self._extract_ts(merged)
                if ts is not None:
                    ts_clean = ts.dropna()
                    if not ts_clean.empty:
                        actual_min = pd.Timestamp(ts_clean.min()).strftime("%Y-%m-%d %H:%M:%S")
                        actual_max = pd.Timestamp(ts_clean.max()).strftime("%Y-%m-%d %H:%M:%S")
                        title_out = f"{title_out}（实际 {actual_min}-{actual_max}）"

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
            title=title_out,
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
        df = self._call_with_retry(
            fetch_func=fetch_func,
            action_name=action_name,
            progress_cb=progress_cb,
            progress_percent=28,
            task_label="主任务",
        )
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
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> pd.DataFrame:
        prefixed_symbol = self._normalize_stock_symbol(symbol)
        errors: list[str] = []
        span_days = self._range_span_days(start_date, end_date, minute_mode=False)
        candidates: list[tuple[str, pd.DataFrame]] = []
        tried_sina_daily = False

        # Fast path: Sina daily is usually much faster than eastmoney full-history pagination.
        try:
            tried_sina_daily = True
            self._emit_progress(progress_cb, 30, f"网络任务: 新浪日线 {prefixed_symbol} 快速探测")
            sina_daily = self._fetch_stock_daily_sina_safe(
                symbol=prefixed_symbol,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
                progress_cb=None,
            )
            if period == "daily":
                if sina_daily is not None and not sina_daily.empty:
                    candidates.append(("sina", sina_daily))
                    if self._is_result_fresh_for_request_end(
                        sina_daily,
                        end_value=end_date,
                        period="daily",
                        minute_mode=False,
                    ) and self._is_valid_daily_series(sina_daily, span_days):
                        return sina_daily.reset_index(drop=True)
            else:
                sina_resampled = self._resample_daily_to_period(
                    self._filter_by_date(sina_daily, start_date, end_date),
                    period=period,
                )
                if sina_resampled is not None and not sina_resampled.empty:
                    candidates.append(("sina_resampled", sina_resampled))
                    if self._is_result_fresh_for_request_end(
                        sina_resampled,
                        end_value=end_date,
                        period=period,
                        minute_mode=False,
                    ):
                        return sina_resampled.reset_index(drop=True)
                else:
                    errors.append("sina_resampled:sparse_result")
        except Exception as exc:
            errors.append(f"sina:{exc}")

        try:
            self._emit_progress(progress_cb, 32, f"网络任务: 东财日线 {symbol} 任务15%")
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            if df is not None and not df.empty:
                candidates.append(("eastmoney", df))
                if self._is_result_fresh_for_request_end(
                    df,
                    end_value=end_date,
                    period=period,
                    minute_mode=False,
                ):
                    if period != "daily" or self._is_valid_daily_series(df, span_days):
                        return df.reset_index(drop=True)
            else:
                errors.append("eastmoney:sparse_result")
        except Exception as exc:
            errors.append(f"eastmoney:{exc}")

        if period != "daily":
            # Fast path: only use eastmoney daily for resample first.
            try:
                self._emit_progress(progress_cb, 35, f"网络任务: 东财日线重采样{period}")
                em_daily = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                )
                em_resampled = self._resample_daily_to_period(
                    self._filter_by_date(em_daily, start_date, end_date),
                    period=period,
                )
                if em_resampled is not None and not em_resampled.empty:
                    candidates.append(("eastmoney_resampled", em_resampled))
                    if self._is_result_fresh_for_request_end(
                        em_resampled,
                        end_value=end_date,
                        period=period,
                        minute_mode=False,
                    ):
                        return em_resampled.reset_index(drop=True)
                else:
                    errors.append("eastmoney_resampled:sparse_result")
            except Exception as exc:
                errors.append(f"eastmoney_resampled:{exc}")

            resampled = self._fetch_non_daily_from_daily_sources(
                period=period,
                start_date=start_date,
                end_date=end_date,
                loaders=[
                    (
                        "sina",
                        lambda: self._fetch_stock_daily_sina_safe(
                            symbol=prefixed_symbol,
                            start_date=start_date,
                            end_date=end_date,
                            adjust="qfq",
                        ),
                    ),
                    (
                        "tencent",
                        lambda: self._fetch_stock_daily_tx_safe(
                            symbol=prefixed_symbol,
                            start_date=start_date,
                            end_date=end_date,
                            adjust="qfq",
                        ),
                    ),
                ],
                errors=errors,
                progress_cb=progress_cb,
            )
            if resampled is not None and not resampled.empty:
                candidates.append(("resampled", resampled))
            picked = self._pick_freshest_source_result(
                candidates,
                end_value=end_date,
                period=period,
                minute_mode=False,
                span_days=span_days,
                require_daily_valid=False,
            )
            if picked is not None:
                return picked[1].reset_index(drop=True)
            for _, df in candidates:
                if df is not None and not df.empty:
                    return df.reset_index(drop=True)
            raise RuntimeError(" | ".join(errors))

        self._emit_progress(progress_cb, 34, f"网络任务: 日线补充源 {prefixed_symbol}")
        fallback_tasks: list[tuple[str, Callable[[], pd.DataFrame]]] = []
        if not tried_sina_daily:
            fallback_tasks.append(
                (
                    "sina",
                    lambda: self._fetch_stock_daily_sina_safe(
                        symbol=prefixed_symbol,
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq",
                        progress_cb=None,
                    ),
                )
            )
        fallback_tasks.append(
            (
                "tencent",
                lambda: self._fetch_stock_daily_tx_safe(
                    symbol=prefixed_symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                    progress_cb=None,
                ),
            )
        )
        candidates.extend(
            self._run_parallel_dataframe_tasks(
                fallback_tasks,
                errors,
                max_workers=2,
            )
        )

        picked_daily = self._pick_freshest_source_result(
            candidates,
            end_value=end_date,
            period="daily",
            minute_mode=False,
            span_days=span_days,
            require_daily_valid=True,
        )
        if picked_daily is not None:
            return picked_daily[1].reset_index(drop=True)
        for _, df in candidates:
            if df is not None and not df.empty:
                return df.reset_index(drop=True)
        raise RuntimeError(" | ".join(errors))

    def _fetch_stock_daily_sina_safe(
        self,
        *,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> pd.DataFrame:
        errors: list[str] = []
        try:
            self._emit_progress(progress_cb, 36, f"网络任务: 新浪区间 {start_date}-{end_date} 任务60%")
            df = ak.stock_zh_a_daily(
                symbol=symbol,
                start_date=self._as_dash_date(start_date),
                end_date=self._as_dash_date(end_date),
                adjust=adjust,
            )
            if df is not None and not df.empty:
                return df
            errors.append("empty_range")
        except Exception as exc:
            errors.append(str(exc))

        # Sina occasionally fails while slicing by date; full fetch + local filter is more resilient.
        try:
            self._emit_progress(progress_cb, 36, "网络任务: 新浪全量回退 任务70%")
            df_all = ak.stock_zh_a_daily(symbol=symbol, adjust=adjust)
            filtered = self._filter_by_date(df_all, start_date, end_date)
            if filtered is not None and not filtered.empty:
                return filtered
            if df_all is not None and not df_all.empty:
                return df_all
            errors.append("empty_full")
        except Exception as exc:
            errors.append(str(exc))
        raise RuntimeError(" | ".join(errors))

    def _fetch_stock_daily_tx_safe(
        self,
        *,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> pd.DataFrame:
        errors: list[str] = []
        try:
            self._emit_progress(progress_cb, 36, f"网络任务: 腾讯区间 {start_date}-{end_date} 任务88%")
            df = ak.stock_zh_a_hist_tx(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
            if df is not None and not df.empty:
                return df
            errors.append("empty_range")
        except Exception as exc:
            errors.append(str(exc))

        # Some symbols intermittently fail when TX does internal date slicing.
        # Fallback to full fetch and filter locally.
        try:
            self._emit_progress(progress_cb, 36, "网络任务: 腾讯全量回退 任务94%")
            df_all = ak.stock_zh_a_hist_tx(symbol=symbol, adjust=adjust)
            filtered = self._filter_by_date(df_all, start_date, end_date)
            if filtered is not None and not filtered.empty:
                return filtered
            if df_all is not None and not df_all.empty:
                return df_all
            errors.append("empty_full")
        except Exception as exc:
            errors.append(str(exc))
        raise RuntimeError(" | ".join(errors))

    def _fetch_index_daily_fallback(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = "daily",
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> pd.DataFrame:
        prefixed_symbol = self._normalize_index_symbol(symbol)
        errors: list[str] = []
        span_days = self._range_span_days(start_date, end_date, minute_mode=False)
        candidates: list[tuple[str, pd.DataFrame]] = []
        tried_sina_daily = False

        # Fast path for index daily: Sina is usually very fast and returns full history directly.
        try:
            tried_sina_daily = True
            self._emit_progress(progress_cb, 30, f"网络任务: 新浪指数 {prefixed_symbol} 快速探测")
            sina_daily = self._filter_by_date(
                ak.stock_zh_index_daily(symbol=prefixed_symbol),
                start_date,
                end_date,
            )
            if period == "daily":
                if sina_daily is not None and not sina_daily.empty:
                    candidates.append(("sina", sina_daily))
                    if self._is_result_fresh_for_request_end(
                        sina_daily,
                        end_value=end_date,
                        period="daily",
                        minute_mode=False,
                    ) and self._is_valid_daily_series(sina_daily, span_days):
                        return sina_daily.reset_index(drop=True)
            else:
                sina_resampled = self._resample_daily_to_period(sina_daily, period=period)
                if sina_resampled is not None and not sina_resampled.empty:
                    candidates.append(("sina_resampled", sina_resampled))
                    if self._is_result_fresh_for_request_end(
                        sina_resampled,
                        end_value=end_date,
                        period=period,
                        minute_mode=False,
                    ):
                        return sina_resampled.reset_index(drop=True)
                else:
                    errors.append("sina_resampled:sparse_result")
        except Exception as exc:
            errors.append(f"sina:{exc}")

        try:
            self._emit_progress(progress_cb, 32, f"网络任务: 东财指数 {symbol} 任务15%")
            df = ak.index_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
            )
            if df is not None and not df.empty:
                candidates.append(("eastmoney", df))
                if self._is_result_fresh_for_request_end(
                    df,
                    end_value=end_date,
                    period=period,
                    minute_mode=False,
                ):
                    if period != "daily" or self._is_valid_daily_series(df, span_days):
                        return df.reset_index(drop=True)
            else:
                errors.append("eastmoney:sparse_result")
        except Exception as exc:
            errors.append(f"eastmoney:{exc}")

        if period != "daily":
            # Fast path: only use eastmoney daily for resample first.
            try:
                self._emit_progress(progress_cb, 35, f"网络任务: 东财指数重采样{period}")
                em_daily = ak.index_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                )
                em_resampled = self._resample_daily_to_period(
                    self._filter_by_date(em_daily, start_date, end_date),
                    period=period,
                )
                if em_resampled is not None and not em_resampled.empty:
                    candidates.append(("eastmoney_resampled", em_resampled))
                    if self._is_result_fresh_for_request_end(
                        em_resampled,
                        end_value=end_date,
                        period=period,
                        minute_mode=False,
                    ):
                        return em_resampled.reset_index(drop=True)
                else:
                    errors.append("eastmoney_resampled:sparse_result")
            except Exception as exc:
                errors.append(f"eastmoney_resampled:{exc}")

            resampled = self._fetch_non_daily_from_daily_sources(
                period=period,
                start_date=start_date,
                end_date=end_date,
                loaders=[
                    (
                        "tencent",
                        lambda: self._filter_by_date(
                            ak.stock_zh_index_daily_tx(symbol=prefixed_symbol),
                            start_date,
                            end_date,
                        ),
                    ),
                ],
                errors=errors,
                progress_cb=progress_cb,
            )
            if resampled is not None and not resampled.empty:
                candidates.append(("resampled", resampled))
            picked = self._pick_freshest_source_result(
                candidates,
                end_value=end_date,
                period=period,
                minute_mode=False,
                span_days=span_days,
                require_daily_valid=False,
            )
            if picked is not None:
                return picked[1].reset_index(drop=True)
            for _, df in candidates:
                if df is not None and not df.empty:
                    return df.reset_index(drop=True)
            raise RuntimeError(" | ".join(errors))

        self._emit_progress(progress_cb, 34, f"网络任务: 指数日线补充源 {prefixed_symbol}")
        fallback_tasks: list[tuple[str, Callable[[], pd.DataFrame]]] = []
        if not tried_sina_daily:
            fallback_tasks.append(
                (
                    "sina",
                    lambda: self._filter_by_date(
                        ak.stock_zh_index_daily(symbol=prefixed_symbol),
                        start_date,
                        end_date,
                    ),
                )
            )
        fallback_tasks.append(
            (
                "tencent",
                lambda: self._filter_by_date(
                    ak.stock_zh_index_daily_tx(symbol=prefixed_symbol),
                    start_date,
                    end_date,
                ),
            )
        )
        candidates.extend(
            self._run_parallel_dataframe_tasks(
                fallback_tasks,
                errors,
                max_workers=2,
            )
        )

        picked_daily = self._pick_freshest_source_result(
            candidates,
            end_value=end_date,
            period="daily",
            minute_mode=False,
            span_days=span_days,
            require_daily_valid=True,
        )
        if picked_daily is not None:
            return picked_daily[1].reset_index(drop=True)
        for _, df in candidates:
            if df is not None and not df.empty:
                return df.reset_index(drop=True)
        raise RuntimeError(" | ".join(errors))

    def _fetch_stock_minute(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        preferred_source: str | None = None,
        *,
        allow_resample: bool = True,
    ) -> pd.DataFrame:
        errors: list[str] = []
        span_days = self._range_span_days(start_date, end_date, minute_mode=True)
        sources = self._build_stock_minute_sources(symbol=symbol, period=period)
        ordered_names = list(sources.keys())
        if preferred_source in sources:
            ordered_names = [str(preferred_source)] + [n for n in ordered_names if n != preferred_source]

        # Sequential fallback is more stable than multi-source parallel burst for minute endpoints.
        candidates: list[tuple[str, pd.DataFrame]] = []
        partial_candidates: list[tuple[str, pd.DataFrame]] = []
        source_reports: list[str] = []

        def report(name: str, df: pd.DataFrame) -> None:
            try:
                tail = self._series_tail_timestamp(df)
                tail_text = tail.strftime("%Y-%m-%d %H:%M:%S") if tail is not None else "--"
                source_reports.append(f"{name}:rows={len(df.index)} tail={tail_text}")
            except Exception:
                return

        for name in ordered_names:
            try:
                df = sources[name](start_date, end_date)
            except Exception as exc:
                errors.append(f"{name}:{exc}")
                continue
            if df is None or df.empty:
                errors.append(f"{name}:sparse_result")
                continue
            report(name, df)
            left_ok = self._minute_left_coverage_ok(df, start_datetime=start_date)
            if left_ok:
                candidates.append((name, df))
            else:
                partial_candidates.append((name, df))
                errors.append(f"{name}:left_gap")
            if left_ok and self._is_result_fresh_for_request_end(
                df,
                end_value=end_date,
                period=period,
                minute_mode=True,
            ):
                return df.reset_index(drop=True)

        if allow_resample and str(period) == "15":
            try:
                base_5m = self._fetch_stock_minute(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    period="5",
                    preferred_source=preferred_source,
                    allow_resample=False,
                )
                resampled = self._resample_minute_to_minutes(base_5m, target_minutes=15)
                resampled = self._filter_by_datetime(resampled, start_date, end_date)
                if resampled is not None and not resampled.empty:
                    report("resample_5m", resampled)
                    left_ok = self._minute_left_coverage_ok(resampled, start_datetime=start_date)
                    if left_ok:
                        candidates.append(("resample_5m", resampled))
                    else:
                        partial_candidates.append(("resample_5m", resampled))
                        errors.append("resample_5m:left_gap")
                    if left_ok and self._is_result_fresh_for_request_end(
                        resampled,
                        end_value=end_date,
                        period=period,
                        minute_mode=True,
                    ):
                        return resampled.reset_index(drop=True)
            except Exception as exc:
                errors.append(f"resample_5m:{exc}")

        # Final rescue: pull a raw minute window once and slice locally.
        # This avoids chunk-level repeated failures causing complete request failure.
        window_sources: list[tuple[str, Callable[[], pd.DataFrame]]] = [
            ("tx_window", lambda: self._fetch_stock_minute_window_tx(symbol=symbol, period=period)),
            ("sina_window", lambda: self._fetch_stock_minute_window(symbol=symbol, period=period)),
        ]
        for window_name, loader in window_sources:
            try:
                raw_df = loader()
                rescue_df = self._filter_minute_source_with_fallback(
                    raw_df,
                    start_date=start_date,
                    end_date=end_date,
                )
                if rescue_df is not None and not rescue_df.empty:
                    report(window_name, rescue_df)
                    left_ok = self._minute_left_coverage_ok(rescue_df, start_datetime=start_date)
                    if left_ok:
                        candidates.append((window_name, rescue_df))
                    else:
                        partial_candidates.append((window_name, rescue_df))
                        errors.append(f"{window_name}:left_gap")
                    if left_ok and self._is_result_fresh_for_request_end(
                        rescue_df,
                        end_value=end_date,
                        period=period,
                        minute_mode=True,
                    ):
                        return rescue_df.reset_index(drop=True)
                else:
                    errors.append(f"{window_name}:sparse_result")
            except Exception as exc:
                errors.append(f"{window_name}:{exc}")

        final_candidates = candidates + partial_candidates
        picked = self._pick_freshest_source_result(
            final_candidates,
            end_value=end_date,
            period=period,
            minute_mode=True,
            span_days=span_days,
            require_daily_valid=False,
        )
        best_source: str | None = None
        best_df: pd.DataFrame | None = None
        if picked is not None:
            best_source, best_df = picked[0], picked[1]
        else:
            for src, df in final_candidates:
                if df is not None and not df.empty:
                    best_source, best_df = src, df
                    break
        if best_df is None or best_df.empty:
            raise RuntimeError(" | ".join(errors))

        if not self._is_result_fresh_for_request_end(
            best_df,
            end_value=end_date,
            period=period,
            minute_mode=True,
        ):
            tail = self._series_tail_timestamp(best_df)
            tail_text = tail.strftime("%Y-%m-%d %H:%M:%S") if tail is not None else "--"
            detail_parts: list[str] = [
                "stale_tail",
                f"best={best_source or '--'}",
                f"tail={tail_text}",
                f"end={end_date}",
                f"rows={len(best_df.index)}",
            ]
            # Keep the payload short but actionable for GUI dialogs.
            detail_parts.extend(source_reports[:6])
            detail_parts.extend(errors[:6])
            raise RuntimeError("|".join(detail_parts))

        return best_df.reset_index(drop=True)

    def _fetch_index_minute(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        preferred_source: str | None = None,
        *,
        allow_resample: bool = True,
    ) -> pd.DataFrame:
        errors: list[str] = []
        span_days = self._range_span_days(start_date, end_date, minute_mode=True)
        sources = self._build_index_minute_sources(symbol=symbol, period=period)
        ordered_names = list(sources.keys())
        if preferred_source in sources:
            ordered_names = [str(preferred_source)] + [n for n in ordered_names if n != preferred_source]

        # Sequential fallback is more stable than multi-source parallel burst for minute endpoints.
        candidates_df: list[tuple[str, pd.DataFrame]] = []
        partial_candidates_df: list[tuple[str, pd.DataFrame]] = []
        source_reports: list[str] = []

        def report(name: str, df: pd.DataFrame) -> None:
            try:
                tail = self._series_tail_timestamp(df)
                tail_text = tail.strftime("%Y-%m-%d %H:%M:%S") if tail is not None else "--"
                source_reports.append(f"{name}:rows={len(df.index)} tail={tail_text}")
            except Exception:
                return

        for name in ordered_names:
            try:
                df = sources[name](start_date, end_date)
            except Exception as exc:
                errors.append(f"{name}:{exc}")
                continue
            if df is None or df.empty:
                errors.append(f"{name}:sparse_result")
                continue
            report(name, df)
            left_ok = self._minute_left_coverage_ok(df, start_datetime=start_date)
            if left_ok:
                candidates_df.append((name, df))
            else:
                partial_candidates_df.append((name, df))
                errors.append(f"{name}:left_gap")
            if left_ok and self._is_result_fresh_for_request_end(
                df,
                end_value=end_date,
                period=period,
                minute_mode=True,
            ):
                return df.reset_index(drop=True)

        if allow_resample and str(period) == "15":
            try:
                base_5m = self._fetch_index_minute(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    period="5",
                    preferred_source=preferred_source,
                    allow_resample=False,
                )
                resampled = self._resample_minute_to_minutes(base_5m, target_minutes=15)
                resampled = self._filter_by_datetime(resampled, start_date, end_date)
                if resampled is not None and not resampled.empty:
                    report("resample_5m", resampled)
                    left_ok = self._minute_left_coverage_ok(resampled, start_datetime=start_date)
                    if left_ok:
                        candidates_df.append(("resample_5m", resampled))
                    else:
                        partial_candidates_df.append(("resample_5m", resampled))
                        errors.append("resample_5m:left_gap")
                    if left_ok and self._is_result_fresh_for_request_end(
                        resampled,
                        end_value=end_date,
                        period=period,
                        minute_mode=True,
                    ):
                        return resampled.reset_index(drop=True)
            except Exception as exc:
                errors.append(f"resample_5m:{exc}")

        # Final rescue: pull a raw minute window once and slice locally.
        window_sources: list[tuple[str, Callable[[], pd.DataFrame]]] = [
            ("tx_window", lambda: self._fetch_index_minute_window_tx(symbol=symbol, period=period)),
            ("sina_window", lambda: self._fetch_index_minute_window(symbol=symbol, period=period)),
        ]
        for window_name, loader in window_sources:
            try:
                raw_df = loader()
                rescue_df = self._filter_minute_source_with_fallback(
                    raw_df,
                    start_date=start_date,
                    end_date=end_date,
                )
                if rescue_df is not None and not rescue_df.empty:
                    report(window_name, rescue_df)
                    left_ok = self._minute_left_coverage_ok(rescue_df, start_datetime=start_date)
                    if left_ok:
                        candidates_df.append((window_name, rescue_df))
                    else:
                        partial_candidates_df.append((window_name, rescue_df))
                        errors.append(f"{window_name}:left_gap")
                    if left_ok and self._is_result_fresh_for_request_end(
                        rescue_df,
                        end_value=end_date,
                        period=period,
                        minute_mode=True,
                    ):
                        return rescue_df.reset_index(drop=True)
                else:
                    errors.append(f"{window_name}:sparse_result")
            except Exception as exc:
                errors.append(f"{window_name}:{exc}")

        final_candidates_df = candidates_df + partial_candidates_df
        picked = self._pick_freshest_source_result(
            final_candidates_df,
            end_value=end_date,
            period=period,
            minute_mode=True,
            span_days=span_days,
            require_daily_valid=False,
        )
        best_source: str | None = None
        best_df: pd.DataFrame | None = None
        if picked is not None:
            best_source, best_df = picked[0], picked[1]
        else:
            for src, df in final_candidates_df:
                if df is not None and not df.empty:
                    best_source, best_df = src, df
                    break
        if best_df is None or best_df.empty:
            raise RuntimeError(" | ".join(errors))

        if not self._is_result_fresh_for_request_end(
            best_df,
            end_value=end_date,
            period=period,
            minute_mode=True,
        ):
            tail = self._series_tail_timestamp(best_df)
            tail_text = tail.strftime("%Y-%m-%d %H:%M:%S") if tail is not None else "--"
            detail_parts: list[str] = [
                "stale_tail",
                f"best={best_source or '--'}",
                f"tail={tail_text}",
                f"end={end_date}",
                f"rows={len(best_df.index)}",
            ]
            detail_parts.extend(source_reports[:6])
            detail_parts.extend(errors[:6])
            raise RuntimeError("|".join(detail_parts))

        return best_df.reset_index(drop=True)

    def _build_stock_minute_sources(
        self,
        *,
        symbol: str,
        period: str,
    ) -> dict[str, Callable[[str, str], pd.DataFrame]]:
        return {
            "tx_min": lambda s, e: self._filter_minute_source_with_fallback(
                self._fetch_stock_minute_window_tx(symbol=symbol, period=period),
                start_date=s,
                end_date=e,
            ),
            "sina_min": lambda s, e: self._filter_minute_source_with_fallback(
                self._fetch_stock_minute_window(symbol=symbol, period=period),
                start_date=s,
                end_date=e,
            ),
            "eastmoney_min": lambda s, e: self._filter_by_datetime(
                self._call_df_with_timeout(
                    lambda: ak.stock_zh_a_hist_min_em(
                        symbol=symbol,
                        start_date=s,
                        end_date=e,
                        period=period,
                        adjust="",
                    ),
                    timeout_seconds=12.0,
                ),
                s,
                e,
            ),
        }

    def _build_index_minute_sources(
        self,
        *,
        symbol: str,
        period: str,
    ) -> dict[str, Callable[[str, str], pd.DataFrame]]:
        sources: dict[str, Callable[[str, str], pd.DataFrame]] = {}
        key = symbol.strip().lower().replace("sh", "").replace("sz", "").strip()
        if key:
            sources["tx_min"] = (
                lambda s, e: self._filter_minute_source_with_fallback(
                    self._fetch_index_minute_window_tx(symbol=symbol, period=period),
                    start_date=s,
                    end_date=e,
                )
            )
            sources["sina_min"] = (
                lambda s, e: self._filter_minute_source_with_fallback(
                    self._fetch_index_minute_window(symbol=symbol, period=period),
                    start_date=s,
                    end_date=e,
                )
            )
            sources[f"index_min:{key}"] = (
                lambda s, e, k=key: self._filter_by_datetime(
                    self._call_df_with_timeout(
                        lambda: ak.index_zh_a_hist_min_em(
                            symbol=k,
                            period=period,
                            start_date=s,
                            end_date=e,
                        ),
                        timeout_seconds=12.0,
                    ),
                    s,
                    e,
                )
            )
        return sources

    @classmethod
    def _minute_left_coverage_ok(cls, df: pd.DataFrame, *, start_datetime: str) -> bool:
        if df is None or df.empty:
            return False
        start_ts = pd.to_datetime(start_datetime, errors="coerce")
        if pd.isna(start_ts):
            return True
        _, ts = cls._extract_ts(df)
        if ts is None:
            return True
        ts_clean = ts.dropna()
        if ts_clean.empty:
            return True
        start_day = pd.Timestamp(start_ts).normalize()
        head_day = pd.Timestamp(ts_clean.min()).normalize()
        if head_day <= start_day:
            return True
        cursor = start_day
        for _ in range(370):
            if cursor >= head_day:
                break
            if cls._is_trade_day(cursor):
                return False
            cursor += pd.Timedelta(days=1)
        return True

    def _fetch_stock_minute_window(self, *, symbol: str, period: str) -> pd.DataFrame:
        quote_symbol = self._normalize_stock_symbol(symbol)
        return self._get_minute_window_cached(
            key=("sina_stock", quote_symbol, str(period)),
            loader=lambda: self._rename_day_to_datetime(
                self._call_df_with_timeout(
                    lambda: ak.stock_zh_a_minute(symbol=quote_symbol, period=period, adjust=""),
                    timeout_seconds=10.0,
                )
            ),
        )

    def _fetch_index_minute_window(self, *, symbol: str, period: str) -> pd.DataFrame:
        quote_symbol = self._normalize_index_symbol(symbol)
        return self._get_minute_window_cached(
            key=("sina_index", quote_symbol, str(period)),
            loader=lambda: self._rename_day_to_datetime(
                self._call_df_with_timeout(
                    lambda: ak.stock_zh_a_minute(symbol=quote_symbol, period=period, adjust=""),
                    timeout_seconds=10.0,
                )
            ),
        )

    def _fetch_stock_minute_window_tx(self, *, symbol: str, period: str) -> pd.DataFrame:
        quote_symbol = self._normalize_stock_symbol(symbol)
        return self._get_minute_window_cached(
            key=("tx_stock", quote_symbol, str(period)),
            loader=lambda: self._fetch_minute_window_tx(symbol=quote_symbol, period=period),
        )

    def _fetch_index_minute_window_tx(self, *, symbol: str, period: str) -> pd.DataFrame:
        quote_symbol = self._normalize_index_symbol(symbol)
        return self._get_minute_window_cached(
            key=("tx_index", quote_symbol, str(period)),
            loader=lambda: self._fetch_minute_window_tx(symbol=quote_symbol, period=period),
        )

    @staticmethod
    def _fetch_minute_window_tx(*, symbol: str, period: str) -> pd.DataFrame:
        mode = DataService._normalize_period(period)
        if mode not in {"1", "5", "15", "30", "60"}:
            return pd.DataFrame()
        raw_symbol = str(symbol or "").strip().lower()
        if not raw_symbol:
            return pd.DataFrame()
        if not raw_symbol.startswith(("sh", "sz", "bj")):
            if raw_symbol.startswith(("6", "5", "9")):
                raw_symbol = f"sh{raw_symbol}"
            else:
                raw_symbol = f"sz{raw_symbol}"
        url = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/kline/mkline"
        # Add a cache-buster param to avoid intermediary/proxy caches freezing the window response.
        params = {"param": f"{raw_symbol},m{mode},,640", "_": str(int(time.time() * 1000))}
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://gu.qq.com/",
            "Accept": "application/json,text/plain,*/*",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        data_json = response.json()
        node = data_json.get("data", {}).get(raw_symbol, {})
        rows = node.get(f"m{mode}", []) if isinstance(node, dict) else []
        if not isinstance(rows, list) or not rows:
            return pd.DataFrame()
        records: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 6:
                continue
            ts_raw = str(row[0]).strip()
            try:
                ts = datetime.strptime(ts_raw, "%Y%m%d%H%M")
            except Exception:
                continue
            amount_val = None
            if len(row) > 7:
                try:
                    amount_val = float(row[7])
                except Exception:
                    amount_val = None
            records.append(
                {
                    "datetime": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "open": pd.to_numeric(row[1], errors="coerce"),
                    "close": pd.to_numeric(row[2], errors="coerce"),
                    "high": pd.to_numeric(row[3], errors="coerce"),
                    "low": pd.to_numeric(row[4], errors="coerce"),
                    "volume": pd.to_numeric(row[5], errors="coerce"),
                    "amount": amount_val,
                }
            )
        if not records:
            return pd.DataFrame()
        out = pd.DataFrame(records)
        out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        out = out.dropna(subset=["datetime"]).drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime")
        return out.reset_index(drop=True)

    def _select_stock_minute_source(
        self,
        *,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> str | None:
        sources = self._build_stock_minute_sources(symbol=symbol, period=period)
        return self._select_minute_source(
            sources=sources,
            start_date=start_date,
            end_date=end_date,
            period=period,
            progress_cb=progress_cb,
        )

    def _select_index_minute_source(
        self,
        *,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> str | None:
        sources = self._build_index_minute_sources(symbol=symbol, period=period)
        return self._select_minute_source(
            sources=sources,
            start_date=start_date,
            end_date=end_date,
            period=period,
            progress_cb=progress_cb,
        )

    def _select_minute_source(
        self,
        *,
        sources: dict[str, Callable[[str, str], pd.DataFrame]],
        start_date: str,
        end_date: str,
        period: str,
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> str | None:
        if not sources:
            return None
        probe_start, probe_end = self._minute_probe_range(start_date, end_date, period=period)
        self._emit_progress(progress_cb, 26, f"网络任务: 分钟源探测 {probe_start}~{probe_end}")
        best_name: str | None = None
        best_tail: pd.Timestamp | None = None
        for name, loader in sources.items():
            try:
                df = loader(probe_start, probe_end)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            tail = self._series_tail_timestamp(df)
            if tail is not None and (best_tail is None or tail > best_tail):
                best_tail = tail
                best_name = name
            if not self._is_minute_tail_stale(df, end_datetime=probe_end):
                self._emit_progress(progress_cb, 27, f"分钟源已选: {name}")
                return name
        if best_name is not None:
            self._emit_progress(progress_cb, 27, f"分钟源兜底: {best_name}")
            return best_name
        return None

    @staticmethod
    def _rename_day_to_datetime(df: pd.DataFrame | None) -> pd.DataFrame | None:
        if df is None:
            return None
        if hasattr(df, "columns") and "day" in df.columns:
            return df.rename(columns={"day": "datetime"})
        return df

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
        if pickle_path.exists():
            return pickle_path
        if parquet_path.exists() and has_parquet_engine():
            return parquet_path
        if parquet_path.exists():
            return parquet_path
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

    @staticmethod
    def _call_with_retry(
        fetch_func: Callable[[], pd.DataFrame],
        action_name: str,
        retries: int = 3,
        delay_seconds: float = 0.8,
        progress_cb: Callable[[int, str], None] | None = None,
        progress_percent: int | None = None,
        task_label: str = "任务",
    ) -> pd.DataFrame:
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                if progress_cb is not None and progress_percent is not None:
                    start_pct = int(((attempt - 1) / max(1, retries)) * 100)
                    DataService._emit_progress(
                        progress_cb,
                        progress_percent,
                        f"{task_label} 任务{start_pct}%",
                    )
                return fetch_func()
            except RequestException as exc:
                last_error = exc
                if attempt < retries:
                    if progress_cb is not None and progress_percent is not None:
                        retry_pct = int((attempt / max(1, retries)) * 100)
                        DataService._emit_progress(
                            progress_cb,
                            progress_percent,
                            f"{task_label} 重试{attempt + 1}/{retries} 任务{retry_pct}%",
                        )
                    time.sleep(delay_seconds * attempt)
                    continue
                raise RuntimeError(
                    f"{action_name} 请求失败（已重试 {retries} 次）"
                ) from exc
            except Exception as exc:
                last_error = exc
                if attempt < retries:
                    if progress_cb is not None and progress_percent is not None:
                        retry_pct = int((attempt / max(1, retries)) * 100)
                        DataService._emit_progress(
                            progress_cb,
                            progress_percent,
                            f"{task_label} 重试{attempt + 1}/{retries} 任务{retry_pct}%",
                        )
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
        # For UI auto windows (near full-window span), anchor start to end-based window
        # so cache keys stay stable during non-trading days/weekends.
        span_days = int((end_ts - start_ts).days)
        if span_days >= max(1, max_days - 3):
            clipped_start = min_start
        else:
            clipped_start = max(start_ts, min_start)
        return (
            clipped_start.strftime("%Y-%m-%d %H:%M:%S"),
            end_ts.strftime("%Y-%m-%d %H:%M:%S"),
        )

    @classmethod
    def _clip_end_for_period(cls, end_value: str, *, period: str) -> str:
        if period in {"daily", "weekly", "monthly"}:
            end_ts = pd.to_datetime(end_value, format="%Y%m%d", errors="coerce")
            checkpoint_ts = pd.to_datetime(cls._daily_checkpoint(), format="%Y%m%d", errors="coerce")
            if pd.isna(end_ts) or pd.isna(checkpoint_ts):
                return end_value
            clipped = min(pd.Timestamp(end_ts).normalize(), pd.Timestamp(checkpoint_ts).normalize())
            return clipped.strftime("%Y%m%d")
        return end_value

    @classmethod
    def _clip_minute_end_by_checkpoint(cls, end_value: str, *, period: str) -> str:
        end_ts = pd.to_datetime(end_value, errors="coerce")
        checkpoint_ts = pd.to_datetime(cls._minute_checkpoint_for_period(period), format="%Y%m%d%H%M", errors="coerce")
        if pd.isna(end_ts) or pd.isna(checkpoint_ts):
            return end_value
        clipped = min(pd.Timestamp(end_ts), pd.Timestamp(checkpoint_ts))
        return clipped.strftime("%Y-%m-%d %H:%M:%S")

    @classmethod
    def _load_trade_dates(cls) -> set[str]:
        if cls._TRADE_DATES is None:
            cls._TRADE_DATES = set()
            cls._TRADE_DATES_MAX_DAY = None
            market_dir = cache_root() / "market"
            parquet_path = market_dir / "trade_dates.parquet"
            pickle_path = market_dir / "trade_dates.pkl"
            candidates: list[Path] = []
            if pickle_path.exists():
                candidates.append(pickle_path)
            if parquet_path.exists() and has_parquet_engine():
                candidates.append(parquet_path)
            for path in candidates:
                try:
                    df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_pickle(path)
                    if df is None or df.empty:
                        continue
                    col = "trade_date" if "trade_date" in df.columns else df.columns[0]
                    ts = cls._normalize_trade_dates(df[col])
                    if ts.empty:
                        continue
                    now = pd.Timestamp.now().normalize()
                    # Allow loading a stale-but-structurally-correct calendar so staleness guards can
                    # still work around holidays when the calendar source is temporarily unavailable.
                    if not cls._trade_dates_structure_ok(ts, now=now):
                        continue
                    cls._TRADE_DATES = set(ts.dt.strftime("%Y-%m-%d").tolist())
                    cls._TRADE_DATES_MAX_DAY = pd.Timestamp(ts.max()).normalize()
                    break
                except Exception:
                    continue

        return cls._TRADE_DATES or set()

    @classmethod
    def _is_trade_day(cls, day: pd.Timestamp) -> bool:
        d = pd.Timestamp(day).normalize()
        key = d.strftime("%Y-%m-%d")
        dates = cls._load_trade_dates()
        if dates:
            # If the local calendar does not yet cover this date, fallback to weekday.
            # This prevents outdated calendar files from freezing checkpoints to year-end.
            if cls._TRADE_DATES_MAX_DAY is not None and d > cls._TRADE_DATES_MAX_DAY:
                return d.dayofweek < 5
            return key in dates
        return d.dayofweek < 5

    @classmethod
    def _previous_trade_day(cls, day: pd.Timestamp) -> pd.Timestamp:
        d = pd.Timestamp(day).normalize()
        for _ in range(370):
            d -= pd.Timedelta(days=1)
            if cls._is_trade_day(d):
                return d
        return d

    @staticmethod
    def _previous_weekday(day: pd.Timestamp) -> pd.Timestamp:
        d = pd.Timestamp(day).normalize()
        while d.dayofweek >= 5:
            d -= pd.Timedelta(days=1)
        return d

    @classmethod
    def _guard_checkpoint_day(cls, checkpoint_day: pd.Timestamp, *, now: pd.Timestamp) -> pd.Timestamp:
        cp = pd.Timestamp(checkpoint_day).normalize()
        lag_days = int((now.normalize() - cp).days)
        if lag_days <= int(cls.CHECKPOINT_MAX_LAG_DAYS):
            return cp
        fallback = cls._previous_weekday(now.normalize())
        if (now.hour, now.minute) < (cls.DAILY_CLOSE_READY_HOUR, cls.DAILY_CLOSE_READY_MINUTE):
            fallback = cls._previous_weekday(fallback - pd.Timedelta(days=1))
        return fallback.normalize()

    @classmethod
    def _guard_checkpoint_minute(cls, checkpoint_ts: pd.Timestamp, *, now: pd.Timestamp) -> pd.Timestamp:
        cp = pd.Timestamp(checkpoint_ts)
        lag_days = int((now.normalize() - cp.normalize()).days)
        if lag_days <= int(cls.CHECKPOINT_MAX_LAG_DAYS):
            return cp
        day = cls._previous_weekday(now.normalize())
        if (now.hour, now.minute) < (9, 30):
            day = cls._previous_weekday(day - pd.Timedelta(days=1))
        return day + pd.Timedelta(hours=15)

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
            return cls._minute_checkpoint_for_period(period)
        # Weekly/monthly bars are updated by newest daily bar inside current period.
        if period in {"weekly", "monthly"}:
            return cls._daily_checkpoint()
        return cls._daily_checkpoint()

    @classmethod
    def _daily_checkpoint(cls) -> str:
        now = pd.Timestamp.now()
        day = now.normalize()
        if not cls._is_trade_day(day):
            cp = cls._previous_trade_day(day)
            cp = cls._guard_checkpoint_day(cp, now=now)
            return cp.strftime("%Y%m%d")
        if (now.hour, now.minute) < (cls.DAILY_CLOSE_READY_HOUR, cls.DAILY_CLOSE_READY_MINUTE):
            cp = cls._previous_trade_day(day)
            cp = cls._guard_checkpoint_day(cp, now=now)
            return cp.strftime("%Y%m%d")
        cp = cls._guard_checkpoint_day(day, now=now)
        return cp.strftime("%Y%m%d")

    @classmethod
    def _minute_checkpoint(cls) -> str:
        now = pd.Timestamp.now()
        day = now.normalize()

        if not cls._is_trade_day(day):
            trade_day = cls._previous_trade_day(day)
            checkpoint = trade_day + pd.Timedelta(hours=15)
            checkpoint = cls._guard_checkpoint_minute(checkpoint, now=now)
            return checkpoint.strftime("%Y%m%d%H%M")

        if (now.hour, now.minute) < (9, 30):
            trade_day = cls._previous_trade_day(day)
            checkpoint = trade_day + pd.Timedelta(hours=15)
            checkpoint = cls._guard_checkpoint_minute(checkpoint, now=now)
            return checkpoint.strftime("%Y%m%d%H%M")
        if (now.hour, now.minute) < (11, 30):
            checkpoint = cls._guard_checkpoint_minute(now.floor("min"), now=now)
            return checkpoint.strftime("%Y%m%d%H%M")
        if (now.hour, now.minute) < (13, 0):
            checkpoint = day + pd.Timedelta(hours=11, minutes=30)
            checkpoint = cls._guard_checkpoint_minute(checkpoint, now=now)
            return checkpoint.strftime("%Y%m%d%H%M")
        if (now.hour, now.minute) < (15, 0):
            checkpoint = cls._guard_checkpoint_minute(now.floor("min"), now=now)
            return checkpoint.strftime("%Y%m%d%H%M")
        checkpoint = day + pd.Timedelta(hours=15)
        checkpoint = cls._guard_checkpoint_minute(checkpoint, now=now)
        return checkpoint.strftime("%Y%m%d%H%M")

    @classmethod
    def _minute_checkpoint_for_period(cls, period: str) -> str:
        mode = cls._normalize_period(period)
        if mode not in {"1", "5", "15", "30", "60"}:
            return cls._minute_checkpoint()
        bar_minutes = int(mode)

        now = pd.Timestamp.now()
        day = now.normalize()

        if not cls._is_trade_day(day):
            trade_day = cls._previous_trade_day(day)
            checkpoint = trade_day + pd.Timedelta(hours=15)
            checkpoint = cls._guard_checkpoint_minute(checkpoint, now=now)
            return checkpoint.strftime("%Y%m%d%H%M")

        morning_start = day + pd.Timedelta(hours=9, minutes=30)
        morning_end = day + pd.Timedelta(hours=11, minutes=30)
        afternoon_start = day + pd.Timedelta(hours=13)
        afternoon_end = day + pd.Timedelta(hours=15)

        if now < morning_start:
            trade_day = cls._previous_trade_day(day)
            checkpoint = trade_day + pd.Timedelta(hours=15)
            checkpoint = cls._guard_checkpoint_minute(checkpoint, now=now)
            return checkpoint.strftime("%Y%m%d%H%M")

        if now < (morning_start + pd.Timedelta(minutes=bar_minutes)):
            trade_day = cls._previous_trade_day(day)
            checkpoint = trade_day + pd.Timedelta(hours=15)
            checkpoint = cls._guard_checkpoint_minute(checkpoint, now=now)
            return checkpoint.strftime("%Y%m%d%H%M")

        if now <= morning_end:
            minutes = int((min(now, morning_end) - morning_start).total_seconds() // 60)
            bars = max(1, minutes // bar_minutes)
            checkpoint = morning_start + pd.Timedelta(minutes=bars * bar_minutes)
            checkpoint = min(checkpoint, morning_end)
            checkpoint = cls._guard_checkpoint_minute(checkpoint, now=now)
            return checkpoint.strftime("%Y%m%d%H%M")

        if now < afternoon_start:
            checkpoint = cls._guard_checkpoint_minute(morning_end, now=now)
            return checkpoint.strftime("%Y%m%d%H%M")

        if now < (afternoon_start + pd.Timedelta(minutes=bar_minutes)):
            checkpoint = cls._guard_checkpoint_minute(morning_end, now=now)
            return checkpoint.strftime("%Y%m%d%H%M")

        if now <= afternoon_end:
            minutes = int((min(now, afternoon_end) - afternoon_start).total_seconds() // 60)
            bars = max(1, minutes // bar_minutes)
            checkpoint = afternoon_start + pd.Timedelta(minutes=bars * bar_minutes)
            checkpoint = min(checkpoint, afternoon_end)
            checkpoint = cls._guard_checkpoint_minute(checkpoint, now=now)
            return checkpoint.strftime("%Y%m%d%H%M")

        checkpoint = cls._guard_checkpoint_minute(afternoon_end, now=now)
        return checkpoint.strftime("%Y%m%d%H%M")

    @classmethod
    def _minute_tail_lag_days(cls, df: pd.DataFrame, *, end_value: str) -> int | None:
        if df is None or df.empty:
            return None
        tail_ts = cls._series_tail_timestamp(df)
        end_ts = pd.to_datetime(end_value, errors="coerce")
        if tail_ts is None or pd.isna(end_ts):
            return None
        try:
            return int((pd.Timestamp(end_ts).normalize() - pd.Timestamp(tail_ts).normalize()).days)
        except Exception:
            return None

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
    def _is_low_density_minute_result(cls, df: pd.DataFrame, *, span_days: int | None, period: str) -> bool:
        if df is None or df.empty:
            return True
        if span_days is None:
            return len(df.index) <= 1
        mode = cls._normalize_period(period)
        bars_per_day = {
            "1": 240,
            "5": 48,
            "15": 16,
            "30": 8,
            "60": 4,
        }.get(mode, 48)
        rows = int(len(df.index))
        days = max(1, int(span_days))
        # Heuristic: for large spans, returning only a tiny tail window is usually a windowed source fallback
        # and will lock incremental mode on a stale tail (e.g. only one day returned for a 120-day window).
        window_days = min(20, days)
        min_expected = int(bars_per_day * window_days * 0.5)
        return rows < max(12, min_expected)

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

        if observed_days >= 365 * 3:
            if density < 0.35:
                return False
        elif observed_days >= 365 * 2:
            if density < 0.30:
                return False

        # Requested window may be much larger than listing age; avoid false invalidation.
        if span_days is not None and span_days >= 365 * 3 and observed_days >= 365 and rows < 180:
            return False
        return True

    @classmethod
    def _is_daily_tail_stale(cls, df: pd.DataFrame, *, end_date: str, grace_days: int = 15) -> bool:
        return cls._is_series_tail_stale(df, end_date=end_date, period="daily", grace_days=grace_days)

    @classmethod
    def _tail_staleness_grace_days(cls, period: str) -> int:
        mode = cls._normalize_period(period)
        if mode == "monthly":
            return 25
        if mode == "weekly":
            return 12
        return 10

    @classmethod
    def _is_minute_tail_stale(cls, df: pd.DataFrame, *, end_datetime: str, grace_days: int = 3) -> bool:
        if df is None or df.empty:
            return True
        _, ts = cls._extract_ts(df)
        if ts is None:
            return False
        ts_clean = ts.dropna()
        if ts_clean.empty:
            return True
        end_ts = pd.to_datetime(end_datetime, errors="coerce")
        if pd.isna(end_ts):
            return False
        tail_ts = pd.Timestamp(ts_clean.max())
        target_ts = pd.Timestamp(end_ts)
        return (target_ts - tail_ts) > pd.Timedelta(days=max(1, int(grace_days)))

    @classmethod
    def _is_series_tail_stale(
        cls,
        df: pd.DataFrame,
        *,
        end_date: str,
        period: str,
        grace_days: int | None = None,
    ) -> bool:
        if df is None or df.empty:
            return True
        _, ts = cls._extract_ts(df)
        if ts is None:
            # If no parsable date column exists, do not block this source.
            return False
        ts_clean = ts.dropna()
        if ts_clean.empty:
            return True
        end_ts = pd.to_datetime(end_date, format="%Y%m%d", errors="coerce")
        if pd.isna(end_ts):
            return False
        tail_ts = pd.Timestamp(ts_clean.max()).normalize()
        target_ts = pd.Timestamp(end_ts).normalize()
        grace = cls._tail_staleness_grace_days(period) if grace_days is None else max(1, int(grace_days))
        return (target_ts - tail_ts) > pd.Timedelta(days=grace)

    @classmethod
    def _is_result_fresh_for_request_end(
        cls,
        df: pd.DataFrame,
        *,
        end_value: str,
        period: str,
        minute_mode: bool,
    ) -> bool:
        if df is None or df.empty:
            return False
        _, ts = cls._extract_ts(df)
        if ts is None:
            return True
        ts_clean = ts.dropna()
        if ts_clean.empty:
            return False
        tail_ts = pd.Timestamp(ts_clean.max())
        if minute_mode:
            end_ts = pd.to_datetime(end_value, errors="coerce")
            if pd.isna(end_ts):
                return True
            return cls._minute_fresh_enough_for_end(
                tail_ts=pd.Timestamp(tail_ts),
                end_ts=pd.Timestamp(end_ts),
                period=period,
                now=pd.Timestamp.now(),
            )

        # keep period in signature for call-site clarity and future policy tuning
        _ = cls._normalize_period(period)
        end_ts = pd.to_datetime(end_value, format="%Y%m%d", errors="coerce")
        if pd.isna(end_ts):
            return True
        target_day = pd.Timestamp(end_ts).normalize()
        tail_day = pd.Timestamp(tail_ts).normalize()
        if tail_day >= target_day:
            return True
        # Calendar coverage guard: if the local calendar ends before target day, allow a small gap
        # right after the last known calendar day to avoid false-stale during calendar outages.
        if cls._TRADE_DATES_MAX_DAY is not None and target_day > cls._TRADE_DATES_MAX_DAY:
            if tail_day <= cls._TRADE_DATES_MAX_DAY and (target_day - tail_day) <= pd.Timedelta(days=5):
                return True
        # If request end is a holiday/weekend, previous trade day tail is acceptable.
        if not cls._is_trade_day(target_day):
            return tail_day >= cls._previous_trade_day(target_day).normalize()
        return False

    @classmethod
    def _minute_fresh_enough_for_end(
        cls,
        *,
        tail_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        period: str,
        now: pd.Timestamp,
    ) -> bool:
        mode = cls._normalize_period(period)
        bar_minutes = 1
        try:
            if mode in {"1", "5", "15", "30", "60"}:
                bar_minutes = max(1, int(mode))
        except Exception:
            bar_minutes = 1

        gap = pd.Timestamp(end_ts) - pd.Timestamp(tail_ts)
        if gap <= pd.Timedelta(0):
            return True

        end_day = pd.Timestamp(end_ts).normalize()
        tail_day = pd.Timestamp(tail_ts).normalize()
        now_ts = pd.Timestamp(now)
        now_day = now_ts.normalize()

        # Same-session gaps: 15m bars may end at 14:45 while UI end is 15:00.
        if tail_day == end_day:
            return gap <= pd.Timedelta(minutes=max(8, int(bar_minutes) * 2))

        # If request end falls on a non-trading day, previous trade day's close is acceptable.
        # If the local calendar does not yet cover this day, avoid false-stale errors for short gaps
        # immediately after the calendar's last known day (e.g. year-end holidays).
        if cls._TRADE_DATES_MAX_DAY is not None and end_day > cls._TRADE_DATES_MAX_DAY:
            if tail_day <= cls._TRADE_DATES_MAX_DAY and (end_day - tail_day) <= pd.Timedelta(days=5):
                return True
        if not cls._is_trade_day(end_day):
            prev = cls._previous_trade_day(end_day).normalize()
            return tail_day >= prev

        # If user asks for today's intraday window, yesterday's minutes are NOT acceptable
        # once market is open; this would lock the UI on stale cached tails.
        if end_day == now_day:
            market_open = now_day + pd.Timedelta(hours=9, minutes=30)
            if now_ts < market_open:
                prev = cls._previous_trade_day(end_day).normalize()
                return tail_day >= prev
            return False

        # For historical trade days, tail must reach that day.
        return tail_day >= end_day

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

    @classmethod
    def _series_tail_timestamp(cls, df: pd.DataFrame) -> pd.Timestamp | None:
        if df is None or df.empty:
            return None
        _, ts = cls._extract_ts(df)
        if ts is None:
            return None
        ts_clean = ts.dropna()
        if ts_clean.empty:
            return None
        return pd.Timestamp(ts_clean.max())

    @staticmethod
    def _run_parallel_dataframe_tasks(
        tasks: list[tuple[str, Callable[[], pd.DataFrame]]],
        errors: list[str],
        *,
        max_workers: int = 3,
    ) -> list[tuple[str, pd.DataFrame]]:
        if not tasks:
            return []
        workers = max(1, min(int(max_workers), len(tasks)))
        results: list[tuple[str, pd.DataFrame]] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            fut_map = {pool.submit(loader): name for name, loader in tasks}
            for fut in as_completed(fut_map):
                name = fut_map[fut]
                try:
                    df = fut.result()
                    if df is not None and not df.empty:
                        results.append((name, df))
                    else:
                        errors.append(f"{name}:sparse_result")
                except Exception as exc:
                    errors.append(f"{name}:{exc}")
        return results

    def _pick_freshest_source_result(
        self,
        candidates: list[tuple[str, pd.DataFrame]],
        *,
        end_value: str,
        period: str,
        minute_mode: bool,
        span_days: int | None = None,
        require_daily_valid: bool = False,
    ) -> tuple[str, pd.DataFrame] | None:
        pool: list[tuple[str, pd.DataFrame, pd.Timestamp]] = []
        valid_pool: list[tuple[str, pd.DataFrame, pd.Timestamp]] = []
        for source, df in candidates:
            if df is None or df.empty:
                continue
            if self._is_sparse_result(df, span_days, minute_mode):
                continue
            tail_ts = self._series_tail_timestamp(df)
            if tail_ts is None:
                continue
            item = (source, df, tail_ts)
            pool.append(item)
            if (not require_daily_valid) or self._is_valid_daily_series(df, span_days):
                valid_pool.append(item)
        if not pool:
            return None
        target = valid_pool if valid_pool else pool

        def rank(item: tuple[str, pd.DataFrame, pd.Timestamp]) -> tuple[int, int, int]:
            source, df, tail_ts = item
            if minute_mode:
                stale = self._is_minute_tail_stale(df, end_datetime=end_value)
            else:
                stale = self._is_series_tail_stale(df, end_date=end_value, period=period)
            return (int(tail_ts.value), int(not stale), int(len(df.index)))

        best = max(target, key=rank)
        return best[0], best[1]

    def _fetch_non_daily_from_daily_sources(
        self,
        *,
        period: str,
        start_date: str,
        end_date: str,
        loaders: list[tuple[str, Callable[[], pd.DataFrame]]],
        errors: list[str],
        progress_cb: Callable[[int, str], None] | None = None,
    ) -> pd.DataFrame | None:
        self._emit_progress(progress_cb, 36, f"网络任务: 多源并行重采样{period}")
        span_days = self._range_span_days(start_date, end_date, minute_mode=False)
        tasks: list[tuple[str, Callable[[], pd.DataFrame]]] = []
        for source, loader in loaders:
            tasks.append(
                (
                    source,
                    lambda ld=loader: self._resample_daily_to_period(
                        self._filter_by_date(ld(), start_date, end_date),
                        period=period,
                    ),
                )
            )
        candidates = self._run_parallel_dataframe_tasks(tasks, errors, max_workers=3)
        picked = self._pick_freshest_source_result(
            candidates,
            end_value=end_date,
            period=period,
            minute_mode=False,
            span_days=span_days,
            require_daily_valid=False,
        )
        if picked is None:
            return None
        return picked[1]

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

    @classmethod
    def _resample_minute_to_minutes(cls, df: pd.DataFrame, *, target_minutes: int) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        try:
            target = max(1, int(target_minutes))
        except Exception:
            target = 15

        _, ts = cls._extract_ts(df)
        if ts is None:
            return pd.DataFrame()
        safe = df.copy()
        safe["_dt_"] = ts
        safe = safe.dropna(subset=["_dt_"])
        if safe.empty:
            return pd.DataFrame()
        safe = safe.sort_values("_dt_", kind="stable")

        open_col = cls._first_existing_column(safe, ("open", "开盘", "open_price", "开盘价"))
        high_col = cls._first_existing_column(safe, ("high", "最高", "high_price", "最高价"))
        low_col = cls._first_existing_column(safe, ("low", "最低", "low_price", "最低价"))
        close_col = cls._first_existing_column(safe, ("close", "收盘", "收盘价", "latest", "现价"))
        volume_col = cls._first_existing_column(safe, ("volume", "成交量", "vol"))
        amount_col = cls._first_existing_column(safe, ("amount", "成交额", "turnover"))

        if close_col is None:
            return pd.DataFrame()

        numeric_cols = [c for c in (open_col, high_col, low_col, close_col, volume_col, amount_col) if c is not None]
        for col in numeric_cols:
            safe[col] = pd.to_numeric(safe[col], errors="coerce")

        # Group 5m/1m bars into target-minute bars, using end-time labeling semantics.
        safe["_bin_"] = safe["_dt_"].dt.ceil(f"{target}min")
        grouped = safe.groupby("_bin_", sort=True, observed=False)
        open_source_col = open_col or close_col

        out = pd.DataFrame(
            {
                "datetime": grouped["_dt_"].max(),
                "open": grouped[open_source_col].apply(cls._first_valid),
                "high": grouped[high_col].max() if high_col is not None else grouped[close_col].max(),
                "low": grouped[low_col].min() if low_col is not None else grouped[close_col].min(),
                "close": grouped[close_col].apply(cls._last_valid),
            }
        )
        if volume_col is not None:
            out["volume"] = grouped[volume_col].sum(min_count=1)
        if amount_col is not None:
            out["amount"] = grouped[amount_col].sum(min_count=1)

        out = out.dropna(subset=["datetime", "close"])
        if out.empty:
            return pd.DataFrame()
        out["datetime"] = cls._parse_datetime_series(out["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        out = out.dropna(subset=["datetime"]).drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime")
        return out.reset_index(drop=True)

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
        period: str = "1",
    ) -> list[tuple[str, str]]:
        try:
            start_ts = pd.to_datetime(start_value, errors="coerce")
            end_ts = pd.to_datetime(end_value, errors="coerce")
            if pd.isna(start_ts) or pd.isna(end_ts) or start_ts > end_ts:
                return [(start_value, end_value)]
            bar_minutes = 1
            try:
                mode = DataService._normalize_period(period)
                if mode in {"1", "5", "15", "30", "60"}:
                    bar_minutes = max(1, int(mode))
            except Exception:
                bar_minutes = 1
            step = pd.Timedelta(days=max(1, int(chunk_days)))
            cursor = start_ts
            ranges: list[tuple[str, str]] = []
            while cursor <= end_ts:
                chunk_end = cursor + step
                # Avoid generating a tiny trailing fragment such as 09:31~15:00,
                # which adds one more fragile network round-trip with low value.
                if (end_ts - chunk_end) <= pd.Timedelta(hours=8):
                    chunk_end = end_ts
                else:
                    chunk_end = min(chunk_end, end_ts)
                ranges.append(
                    (
                        cursor.strftime("%Y-%m-%d %H:%M:%S"),
                        chunk_end.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                )
                cursor = chunk_end + pd.Timedelta(minutes=bar_minutes)
            return ranges if ranges else [(start_value, end_value)]
        except Exception:
            return [(start_value, end_value)]

    @classmethod
    def _filter_minute_source_with_fallback(
        cls,
        df: pd.DataFrame | None,
        *,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        filtered = cls._filter_by_datetime(df, start_date, end_date)
        if filtered is not None and not filtered.empty:
            return filtered
        tail_ts = cls._series_tail_timestamp(df)
        end_ts = pd.to_datetime(end_date, errors="coerce")
        if tail_ts is None or pd.isna(end_ts):
            return filtered if filtered is not None else pd.DataFrame()
        # When fixed-window minute sources return only recent data, keep it as fallback
        # only if tail is close to request end (avoid polluting older chunks).
        if pd.Timestamp(tail_ts) > (pd.Timestamp(end_ts) + pd.Timedelta(days=1)):
            return filtered if filtered is not None else pd.DataFrame()
        if TimeSeriesCache._is_tolerable_right_gap(
            pd.Timestamp(tail_ts),
            pd.Timestamp(end_ts),
            True,
        ):
            return df
        return filtered if filtered is not None else pd.DataFrame()

    @classmethod
    def _build_tail_update_ranges(
        cls,
        *,
        cached_df: pd.DataFrame,
        end_value: str,
        minute_mode: bool,
    ) -> list[tuple[str, str]]:
        _, ts = cls._extract_ts(cached_df)
        if ts is None:
            return []
        ts_clean = ts.dropna()
        if ts_clean.empty:
            return []
        tail_ts = pd.Timestamp(ts_clean.max())
        end_ts = cls._range_text_to_ts(end_value, minute_mode)
        if end_ts is None:
            return []
        delta = pd.Timedelta(minutes=1) if minute_mode else pd.Timedelta(days=1)
        start_ts = tail_ts + delta
        if start_ts > end_ts:
            return []
        return [
            (
                cls._ts_to_range_text(pd.Timestamp(start_ts), minute_mode),
                cls._ts_to_range_text(pd.Timestamp(end_ts), minute_mode),
            )
        ]

    @staticmethod
    def _minute_chunk_days(
        *,
        period: str,
        span_days: int | None,
        has_cache: bool,
    ) -> int:
        mode = DataService._normalize_period(period)
        # Prefer smaller chunks on cold-start for 5/15m to reduce single-request fragility.
        if not has_cache:
            base = {
                "1": 8,
                "5": 12,
                "15": 20,
                "30": 30,
                "60": 45,
            }.get(mode, 18)
        else:
            base = {
                "1": 11,
                "5": 46,
                "15": 121,
                "30": 241,
                "60": 366,
            }.get(mode, 30)
        if has_cache:
            # Incremental updates prefer single larger chunk to reduce request count.
            base = max(base, 60)
        if span_days is not None and span_days > 220:
            base = max(base, 180)
        return int(base)

    @staticmethod
    def _minute_probe_range(
        start_date: str,
        end_date: str,
        *,
        period: str,
    ) -> tuple[str, str]:
        start_ts = pd.to_datetime(start_date, errors="coerce")
        end_ts = pd.to_datetime(end_date, errors="coerce")
        if pd.isna(start_ts) or pd.isna(end_ts) or start_ts > end_ts:
            return start_date, end_date
        probe_days = {
            "1": 2,
            "5": 4,
            "15": 7,
            "30": 10,
            "60": 14,
        }.get(DataService._normalize_period(period), 5)
        probe_start = max(
            pd.Timestamp(start_ts),
            (pd.Timestamp(end_ts).normalize() - pd.Timedelta(days=int(probe_days))).replace(hour=9, minute=30, second=0),
        )
        return (
            probe_start.strftime("%Y-%m-%d %H:%M:%S"),
            pd.Timestamp(end_ts).strftime("%Y-%m-%d %H:%M:%S"),
        )

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
        return df.loc[mask].reset_index(drop=True)
