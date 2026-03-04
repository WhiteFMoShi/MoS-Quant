from __future__ import annotations

import json
import re
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


@dataclass(frozen=True)
class LoaderContext:
    default_url: str
    trade_calendar: pd.DataFrame
    probe_results: list[ProbeResult]


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

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

    # Fallback: extract many date tokens and treat them as trade dates.
    iso_dates = re.findall(r"\b(?:19|20)\d{2}-\d{2}-\d{2}\b", text)
    compact_dates = re.findall(r"\b(?:19|20)\d{6}\b", text)

    parsed: list[date] = []
    for token in iso_dates:
        try:
            parsed.append(pd.to_datetime(token).date())
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


class MoSQuantLoader:
    """
    MoS Quant loader (portal) with fail-fast semantics.

    Steps:
    1) Probe connectivity for all configured URLs.
    2) Choose the best URL as default source.
    3) Fetch trade calendar from the default URL and cache it locally.

    If any step fails, raise LoaderError and stop.
    """

    def __init__(
        self,
        config: LoaderConfig | None = None,
        *,
        probe: DataSourceProbe | None = None,
        cache: FileCache | None = None,
        calendar_fetcher: Callable[[str, float], pd.DataFrame] | None = None,
    ) -> None:
        self.config = config or LoaderConfig()
        self.probe = probe or DataSourceProbe(
            attempts=self.config.probe_attempts,
            timeout=self.config.probe_timeout,
            interval=self.config.probe_interval,
            config_path=self.config.probe_urls_path,
        )
        self.cache = cache or FileCache(base_dir=self.config.trade_calendar_cache_dir)
        self.calendar_fetcher = calendar_fetcher or fetch_trade_calendar_from_url

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

        log("Step 1/3: probing data sources ...")
        results = self.step1_probe_sources()
        best = self._best_result(results)
        log(
            f"Probe done: best_url={best.target.url} "
            f"success={best.success_count}/{best.attempts} "
            f"latency_ms={best.avg_latency_ms:.1f}"
        )

        log("Step 2/3: choosing default source ...")
        default_url = self.step2_choose_default(results)
        log(f"Default source selected: {default_url}")

        log("Step 3/3: fetching trade calendar (with local cache) ...")
        trade_calendar = self.step3_get_trade_calendar(default_url)
        log(f"Trade calendar ready: rows={len(trade_calendar)}")

        self._persist_default_source(default_url, best)
        log(f"Default source saved to {self.config.default_source_path}")
        return LoaderContext(
            default_url=default_url,
            trade_calendar=trade_calendar,
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
