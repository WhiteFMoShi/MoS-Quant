from __future__ import annotations

import json
import re
import threading
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable

import pandas as pd
import requests

from ..caching.file_cache import CacheKey, FileCache, FreshnessLevel
from ..data.network_probe import DataSourceProbe, ProbeResult


class LoaderError(RuntimeError):
    """Raised when MoS Quant loader fails (fail-fast)."""


ProgressCallback = Callable[[str], None]


@dataclass(frozen=True)
class DefaultSourceRecord:
    url: str
    chosen_at: str
    attempts: int
    success_count: int
    success_rate: float
    avg_latency_ms: float


@dataclass
class LoaderConfig:
    # Step 1/2: data source probing
    probe_attempts: int = 2
    probe_timeout: float = 6.0
    probe_interval: float = 0.2
    probe_urls_path: Path = Path("config/probe_urls.json")

    # State file (not config): remember the chosen default URL for later main program
    default_source_path: Path = Path("cache/watch/default_data_source.json")

    # Step 3: trade calendar cache
    trade_calendar_cache_dir: Path = Path("cache/market/trade_calendar")
    trade_calendar_freshness: FreshnessLevel = FreshnessLevel.DAY
    trade_calendar_request_timeout: float = 10.0

    # Step 4: A-share stock list cache (for local fuzzy match / quick search)
    stock_list_cache_dir: Path = Path("cache/market/stock_list")
    stock_list_freshness: FreshnessLevel = FreshnessLevel.DAY


@dataclass(frozen=True)
class LoaderContext:
    default_url: str
    trade_calendar: pd.DataFrame
    a_stock_list: pd.DataFrame
    probe_results: list[ProbeResult]


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_trade_date_series(values: pd.Series) -> pd.Series:
    """
    Normalize a series of mixed date-like values into python `date` objects.

    Upstream sources sometimes mix formats like:
    - 1991-01-02
    - 1991-01-02T00:00:00.000Z
    - 1991-Jan-02T00:00:00.000Z
    - 19910102
    """
    s = values.copy()
    try:
        dt = pd.to_datetime(s, utc=True, errors="coerce", format="mixed")
    except Exception:
        dt = pd.to_datetime(s, utc=True, errors="coerce")

    if dt.isna().any():
        # Fallback 1: compact YYYYMMDD tokens.
        try:
            dt2 = pd.to_datetime(s.astype(str), utc=True, errors="coerce", format="%Y%m%d")
            dt = dt.fillna(dt2)
        except Exception:
            pass

    if dt.isna().any():
        # Fallback 2: force strings and let pandas infer per-element.
        try:
            dt3 = pd.to_datetime(s.astype(str), utc=True, errors="coerce", format="mixed")
        except Exception:
            dt3 = pd.to_datetime(s.astype(str), utc=True, errors="coerce")
        dt = dt.fillna(dt3)

    if dt.isna().any():
        bad = s[dt.isna()].astype(str).head(3).tolist()
        raise LoaderError(f"Failed to parse trade_date tokens (sample={bad}).")

    return dt.dt.date


def fetch_trade_calendar_from_sina_url(url: str, timeout: float = 10.0) -> pd.DataFrame:
    """
    Fetch CN trade calendar from Sina's encoded endpoint.

    The endpoint returns JS like: var datelist="LC/....";
    We decode it via the same JS decoder AkShare uses (hk_js_decode).
    """
    try:
        from akshare.stock.cons import hk_js_decode  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise LoaderError(f"Missing AkShare decoder dependency: {exc}") from exc

    try:
        import py_mini_racer  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise LoaderError(
            "Missing dependency providing module 'py_mini_racer' "
            "(recommended: pip install mini-racer)."
        ) from exc

    headers = {
        "User-Agent": DataSourceProbe.DEFAULT_HEADERS["User-Agent"],
        "Accept": "*/*",
        "Connection": "close",
    }

    last_exc: Exception | None = None
    for _ in range(2):
        try:
            resp = requests.get(url, timeout=timeout, headers=headers)
            resp.raise_for_status()
            text = resp.text or ""
            if "=" not in text:
                raise LoaderError("Unexpected Sina response: missing '=' delimiter.")

            # Extract the quoted payload between '=' and ';'
            encoded = text.split("=", 1)[1].split(";", 1)[0].replace('"', "").strip()
            if not encoded:
                raise LoaderError("Unexpected Sina response: empty encoded payload.")

            js = py_mini_racer.MiniRacer()
            js.eval(hk_js_decode)
            decoded = js.call("d", encoded)
            df = pd.DataFrame(decoded, columns=["trade_date"])
            df["trade_date"] = _normalize_trade_date_series(df["trade_date"])

            # Sina omits 1992-05-04 (known AkShare fix)
            dates = df["trade_date"].tolist()
            missing = date(year=1992, month=5, day=4)
            if missing not in dates:
                dates.append(missing)
            dates = sorted(set(dates))
            return pd.DataFrame({"trade_date": dates})
        except Exception as exc:  # pragma: no cover - network / upstream failures
            last_exc = exc

    raise LoaderError(f"Failed to fetch trade calendar from {url}: {last_exc}") from last_exc


def fetch_trade_calendar_from_url(url: str, timeout: float = 10.0) -> pd.DataFrame:
    """
    Fetch trade calendar from a URL.

    Supported:
    - Sina encoded endpoint (var datelist="...";) via JS decoder.
    - Plain-text/JSON-like pages that contain many date tokens.
    """
    headers = {
        "User-Agent": DataSourceProbe.DEFAULT_HEADERS["User-Agent"],
        "Accept": "*/*",
        "Connection": "close",
    }
    try:
        resp = requests.get(url, timeout=timeout, headers=headers)
        resp.raise_for_status()
        text = resp.text or ""
    except Exception as exc:  # pragma: no cover - network path
        raise LoaderError(f"Failed to request trade calendar: {type(exc).__name__}: {exc}") from exc

    if "var datelist" in text and "=" in text:
        return fetch_trade_calendar_from_sina_url(url, timeout=timeout)

    # Fallback: extract many date/datetime tokens and treat them as trade dates.
    # Examples: 2025-01-02, 2025-01-02T00:00:00.000Z
    iso_dates = re.findall(
        r"\b(?:19|20)\d{2}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?\b",
        text,
    )
    compact_dates = re.findall(r"\b(?:19|20)\d{6}\b", text)

    parsed: list[date] = []
    for token in iso_dates:
        try:
            parsed.append(pd.to_datetime(token, utc=True).date())
        except Exception:
            pass
    for token in compact_dates:
        try:
            parsed.append(pd.to_datetime(token, format="%Y%m%d").date())
        except Exception:
            pass

    parsed = sorted(set(parsed))
    if len(parsed) < 1000:
        raise LoaderError(
            f"Unsupported trade calendar response format from {url} "
            f"(only parsed {len(parsed)} date tokens)."
        )
    return pd.DataFrame({"trade_date": parsed})


def fetch_a_stock_list_from_akshare() -> pd.DataFrame:
    """
    Fetch CN A-share stock list (code + name) from AkShare.

    This is used for local fuzzy matching / quick search UI.
    """
    try:
        import akshare as ak  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise LoaderError(f"Missing dependency AkShare: {exc}") from exc

    fetch_fns = []
    if hasattr(ak, "stock_info_a_code_name"):
        fetch_fns.append(("stock_info_a_code_name", getattr(ak, "stock_info_a_code_name")))
    if hasattr(ak, "stock_zh_a_spot_em"):
        fetch_fns.append(("stock_zh_a_spot_em", getattr(ak, "stock_zh_a_spot_em")))
    if hasattr(ak, "stock_zh_a_spot"):
        fetch_fns.append(("stock_zh_a_spot", getattr(ak, "stock_zh_a_spot")))

    last_exc: Exception | None = None
    df: pd.DataFrame | None = None
    for name, fn in fetch_fns:
        try:
            df = fn()
            if isinstance(df, pd.DataFrame) and not df.empty:
                break
        except Exception as exc:
            last_exc = exc
            df = None

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        if isinstance(last_exc, json.JSONDecodeError):
            detail = (
                ": JSONDecodeError: 返回内容不是JSON（通常是HTML页面，以'<'开头，可能是验证码/拦截/重定向/临时故障）"
            )
        else:
            detail = "" if last_exc is None else f": {type(last_exc).__name__}: {last_exc}"
        raise LoaderError(f"Failed to fetch A-share stock list from AkShare{detail}")

    def pick_col(candidates: tuple[str, ...]) -> str | None:
        cols = set(map(str, df.columns))
        for c in candidates:
            if c in cols:
                return c
        return None

    col_code = pick_col(("code", "代码", "symbol", "股票代码", "证券代码"))
    col_name = pick_col(("名称", "name", "股票简称", "证券简称"))
    if col_code is None or col_name is None:
        raise LoaderError(
            f"Unsupported stock list schema from AkShare: "
            f"missing columns code={col_code!r} name={col_name!r}; "
            f"available={list(map(str, df.columns))}"
        )

    out = pd.DataFrame(
        {
            "code": df[col_code].astype(str).str.strip(),
            "name": df[col_name].astype(str).str.strip(),
        }
    )
    out = out[(out["code"] != "") & (out["code"].str.lower() != "nan")]
    out = out[(out["name"] != "") & (out["name"].str.lower() != "nan")]
    out = out.drop_duplicates(subset=["code"], keep="first").sort_values("code").reset_index(drop=True)

    if out.empty:
        raise LoaderError("A-share stock list fetch returned empty after normalization.")
    return out


class MoSQuantLoader:
    """
    MoS Quant loader (portal) with fail-fast semantics.

    Steps:
    1) Probe connectivity for all configured URLs.
    2) Choose the best URL as default source.
    3) Fetch trade calendar from the default URL and cache it locally.
    4) Fetch A-share stock list and cache it locally.

    If any step fails, raise LoaderError and stop.
    """

    def __init__(
        self,
        config: LoaderConfig | None = None,
        *,
        probe: DataSourceProbe | None = None,
        cache: FileCache | None = None,
        stock_cache: FileCache | None = None,
        calendar_fetcher: Callable[[str, float], pd.DataFrame] | None = None,
        stock_list_fetcher: Callable[[], pd.DataFrame] | None = None,
    ) -> None:
        self.config = config or LoaderConfig()
        self.probe = probe or DataSourceProbe(
            attempts=self.config.probe_attempts,
            timeout=self.config.probe_timeout,
            interval=self.config.probe_interval,
            config_path=self.config.probe_urls_path,
        )
        self.cache = cache or FileCache(base_dir=self.config.trade_calendar_cache_dir)
        self.stock_cache = stock_cache or FileCache(base_dir=self.config.stock_list_cache_dir)
        self.calendar_fetcher = calendar_fetcher or fetch_trade_calendar_from_url
        self.stock_list_fetcher = stock_list_fetcher or fetch_a_stock_list_from_akshare

    def step1_probe_sources(self) -> list[ProbeResult]:
        results = self.probe.probe_all()
        if not results:
            raise LoaderError(f"No URLs configured in {self.config.probe_urls_path}.")
        return results

    @staticmethod
    def _best_result(results: list[ProbeResult]) -> ProbeResult:
        if not results:
            raise LoaderError("No probe results available.")
        best = results[0]
        if best.success_count == 0:
            raise LoaderError("All URLs are unreachable in current network.")
        return best

    def step2_choose_default(self, results: list[ProbeResult]) -> str:
        best = self._best_result(results)
        return best.target.url

    def step3_get_trade_calendar(self, default_url: str) -> pd.DataFrame:
        key = CacheKey(namespace="trade_calendar", params={"url": default_url})
        cached = self.cache.get_df(key, self.config.trade_calendar_freshness)
        if cached is not None and not cached.empty:
            return cached

        try:
            df = self.calendar_fetcher(default_url, self.config.trade_calendar_request_timeout)
        except LoaderError:
            raise
        except Exception as exc:
            raise LoaderError(f"Trade calendar fetch failed: {type(exc).__name__}: {exc}") from exc
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise LoaderError("Trade calendar fetch returned empty DataFrame.")
        if "trade_date" not in df.columns:
            raise LoaderError("Trade calendar DataFrame must contain column 'trade_date'.")

        self.cache.set_df(key, self.config.trade_calendar_freshness, df)
        return df

    def step4_get_a_stock_list(self) -> pd.DataFrame:
        key = CacheKey(namespace="a_stock_list", params={"market": "CN_A"})
        cached = self.stock_cache.get_df(key, self.config.stock_list_freshness)
        if cached is not None and not cached.empty:
            return cached

        try:
            df = self.stock_list_fetcher()
        except LoaderError:
            raise
        except Exception as exc:
            raise LoaderError(f"A-share stock list fetch failed: {type(exc).__name__}: {exc}") from exc

        if not isinstance(df, pd.DataFrame) or df.empty:
            raise LoaderError("A-share stock list fetch returned empty DataFrame.")
        if "code" not in df.columns or "name" not in df.columns:
            raise LoaderError("A-share stock list DataFrame must contain columns 'code' and 'name'.")

        df = df[["code", "name"]].copy()
        df["code"] = df["code"].astype(str).str.strip()
        df["name"] = df["name"].astype(str).str.strip()
        df = df[(df["code"] != "") & (df["name"] != "")]
        df = df.drop_duplicates(subset=["code"], keep="first").sort_values("code").reset_index(drop=True)
        if df.empty:
            raise LoaderError("A-share stock list became empty after normalization.")

        self.stock_cache.set_df(key, self.config.stock_list_freshness, df)
        return df

    def _persist_default_source(self, url: str, best: ProbeResult) -> None:
        record = DefaultSourceRecord(
            url=url,
            chosen_at=_now_iso(),
            attempts=int(best.attempts),
            success_count=int(best.success_count),
            success_rate=float(best.success_rate),
            avg_latency_ms=float(best.avg_latency_ms),
        )
        _write_json(self.config.default_source_path, asdict(record))

    def run(self, *, progress_cb: ProgressCallback | None = None) -> LoaderContext:
        def log(msg: str) -> None:
            if progress_cb is not None:
                progress_cb(msg)

        def emit_progress(pct: int, text: str) -> None:
            safe_pct = max(0, min(100, int(pct)))
            log(f"[progress] {safe_pct} {text}")

        def call_with_progress_heartbeat(
            *,
            status_prefix: str,
            base_pct: int,
            max_pct: int,
            fn: Callable[[], pd.DataFrame],
            interval_seconds: float = 1.0,
        ) -> pd.DataFrame:
            stop = threading.Event()
            started = time.monotonic()

            def tick() -> None:
                while not stop.wait(max(0.2, float(interval_seconds))):
                    elapsed = int(time.monotonic() - started)
                    # We can't get true network progress from upstream APIs; show a smooth,
                    # monotonic estimate to reassure users the app is still working.
                    span = max(1, int(max_pct) - int(base_pct))
                    k = 10.0  # larger -> slower ramp
                    est = int(base_pct + span * (elapsed / (elapsed + k)))
                    est = max(int(base_pct), min(int(max_pct), est))
                    emit_progress(est, f"{status_prefix}  {est}%（已用时 {elapsed}s）")

            t = threading.Thread(target=tick, name="mos_quant_loader_heartbeat", daemon=True)
            t.start()
            try:
                return fn()
            finally:
                stop.set()
                t.join(timeout=max(0.2, float(interval_seconds)) * 2.0)

        emit_progress(2, "正在探测数据源…")
        log("Step 1/4: probing data sources ...")
        results = self.step1_probe_sources()
        best = self._best_result(results)
        log(
            f"Probe done: best_url={best.target.url} "
            f"success={best.success_count}/{best.attempts} "
            f"latency_ms={best.avg_latency_ms:.1f}"
        )

        emit_progress(25, "数据源探测完成")
        log("Step 2/4: choosing default source ...")
        default_url = self.step2_choose_default(results)
        log(f"Default source selected: {default_url}")

        emit_progress(50, "已选择默认数据源")
        log("Step 3/4: fetching trade calendar (with local cache) ...")
        trade_calendar = call_with_progress_heartbeat(
            status_prefix="正在获取交易日历…",
            base_pct=50,
            max_pct=74,
            fn=lambda: self.step3_get_trade_calendar(default_url),
            interval_seconds=1.0,
        )
        log(f"Trade calendar ready: rows={len(trade_calendar)}")
        emit_progress(75, f"交易日历就绪：{len(trade_calendar)} 行")

        log("Step 4/4: fetching A-share stock list (with local cache) ...")
        a_stock_list = call_with_progress_heartbeat(
            status_prefix="正在获取A股股票列表…",
            base_pct=75,
            max_pct=99,
            fn=self.step4_get_a_stock_list,
            interval_seconds=1.0,
        )
        log(f"A-share stock list ready: rows={len(a_stock_list)}")
        emit_progress(99, f"A股股票列表就绪：{len(a_stock_list)} 只")

        self._persist_default_source(default_url, best)
        log(f"Default source saved to {self.config.default_source_path}")
        emit_progress(100, "就绪，进入主界面…")
        return LoaderContext(
            default_url=default_url,
            trade_calendar=trade_calendar,
            a_stock_list=a_stock_list,
            probe_results=results,
        )


def main() -> None:  # pragma: no cover
    ctx = MoSQuantLoader().run()
    df = ctx.trade_calendar
    first = df["trade_date"].iloc[0] if not df.empty else None
    last = df["trade_date"].iloc[-1] if not df.empty else None
    print(f"default_url={ctx.default_url}")
    print(f"trade_calendar_rows={len(df)} range={first}..{last}")


if __name__ == "__main__":  # pragma: no cover
    main()
