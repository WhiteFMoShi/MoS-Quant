from __future__ import annotations

import hashlib
import inspect
import json
import random
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

import akshare as ak
import pandas as pd


class DataFetchError(RuntimeError):
    """Raised when data fetching fails after retries."""


@dataclass
class FetchConfig:
    retries: int = 3
    retry_interval: float = 1.5
    retry_jitter: float = 0.3
    enable_cache: bool = True
    cache_dir: Path = Path("cache/akshare")
    source_config_path: Path = Path("config/akshare_sources.json")
    source_cooldown_seconds: float = 120.0
    source_failure_threshold: int = 2


@dataclass
class SourceRoute:
    name: str
    api_name: str
    priority: int = 100
    enabled: bool = True


@dataclass
class SourceHealth:
    success_count: int = 0
    failure_count: int = 0
    consecutive_failures: int = 0
    ewma_latency_ms: float | None = None
    open_until: float = 0.0
    last_error: str = ""


class SourceRouteStore:
    def __init__(
        self,
        config_path: Path,
        default_routes: dict[str, list[dict[str, Any]]],
        legacy_config_paths: list[Path] | None = None,
    ) -> None:
        self.config_path = config_path
        self.default_routes = default_routes
        self.legacy_config_paths = legacy_config_paths or []
        self.routes = self._load()

    def _load(self) -> dict[str, list[SourceRoute]]:
        raw = self._read_json(self.config_path)
        if raw is not None:
            loaded = self._from_dict(raw)
            merged = self._merge_with_defaults(loaded)
            if self._need_persist(loaded, merged):
                self._save(merged)
            return merged

        for legacy_path in self.legacy_config_paths:
            legacy_raw = self._read_json(legacy_path)
            if legacy_raw is None:
                continue
            loaded = self._from_dict(legacy_raw)
            merged = self._merge_with_defaults(loaded)
            self._save(merged)
            return merged

        try:
            routes = self._from_dict(self.default_routes)
            self._save(routes)
            return routes
        except Exception:
            return self._from_dict(self.default_routes)

    def _save(self, routes: dict[str, list[SourceRoute]] | None = None) -> None:
        current = routes if routes is not None else self.routes
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {k: [asdict(item) for item in v] for k, v in current.items()}
        self.config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    @staticmethod
    def _from_dict(raw: dict[str, Any]) -> dict[str, list[SourceRoute]]:
        parsed: dict[str, list[SourceRoute]] = {}
        for dataset, items in raw.items():
            parsed[dataset] = []
            for item in items:
                parsed[dataset].append(
                    SourceRoute(
                        name=item["name"],
                        api_name=item["api_name"],
                        priority=int(item.get("priority", 100)),
                        enabled=bool(item.get("enabled", True)),
                    )
                )
        return parsed

    def _merge_with_defaults(self, loaded: dict[str, list[SourceRoute]]) -> dict[str, list[SourceRoute]]:
        merged = {dataset: list(items) for dataset, items in loaded.items()}
        defaults = self._from_dict(self.default_routes)

        for dataset, default_items in defaults.items():
            bucket = merged.setdefault(dataset, [])
            existing_names = {item.name for item in bucket}
            for route in default_items:
                if route.name not in existing_names:
                    bucket.append(route)
            bucket.sort(key=lambda x: x.priority)
        return merged

    @staticmethod
    def _need_persist(
        original: dict[str, list[SourceRoute]],
        merged: dict[str, list[SourceRoute]],
    ) -> bool:
        def to_name_map(source: dict[str, list[SourceRoute]]) -> dict[str, set[str]]:
            return {dataset: {item.name for item in items} for dataset, items in source.items()}

        return to_name_map(original) != to_name_map(merged)

    def list(self, dataset: str | None = None) -> dict[str, list[SourceRoute]] | list[SourceRoute]:
        if dataset is None:
            return self.routes
        return self.routes.get(dataset, [])

    def get_enabled(self, dataset: str) -> list[SourceRoute]:
        return [item for item in self.routes.get(dataset, []) if item.enabled]

    def add(self, dataset: str, route: SourceRoute) -> None:
        bucket = self.routes.setdefault(dataset, [])
        if any(item.name == route.name for item in bucket):
            raise ValueError(f"Source '{route.name}' already exists for dataset '{dataset}'.")
        bucket.append(route)
        bucket.sort(key=lambda x: x.priority)
        self._save()

    def remove(self, dataset: str, source_name: str) -> bool:
        bucket = self.routes.get(dataset, [])
        before = len(bucket)
        self.routes[dataset] = [item for item in bucket if item.name != source_name]
        changed = len(self.routes[dataset]) != before
        if changed:
            self._save()
        return changed

    def set_enabled(self, dataset: str, source_name: str, enabled: bool) -> bool:
        for item in self.routes.get(dataset, []):
            if item.name == source_name:
                item.enabled = enabled
                self._save()
                return True
        return False


class SourceHealthTracker:
    def __init__(self, failure_threshold: int, cooldown_seconds: float, alpha: float = 0.35) -> None:
        self.failure_threshold = max(1, failure_threshold)
        self.cooldown_seconds = max(0.0, cooldown_seconds)
        self.alpha = min(max(alpha, 0.05), 0.95)
        self._stats: dict[str, SourceHealth] = {}

    def _key(self, dataset: str, source_name: str) -> str:
        return f"{dataset}:{source_name}"

    def get(self, dataset: str, source_name: str) -> SourceHealth:
        key = self._key(dataset, source_name)
        if key not in self._stats:
            self._stats[key] = SourceHealth()
        return self._stats[key]

    def is_open(self, dataset: str, source_name: str) -> bool:
        return self.get(dataset, source_name).open_until > time.time()

    def record_success(self, dataset: str, source_name: str, latency_ms: float) -> None:
        stat = self.get(dataset, source_name)
        stat.success_count += 1
        stat.consecutive_failures = 0
        stat.open_until = 0.0
        if stat.ewma_latency_ms is None:
            stat.ewma_latency_ms = latency_ms
        else:
            stat.ewma_latency_ms = self.alpha * latency_ms + (1 - self.alpha) * stat.ewma_latency_ms

    def record_failure(self, dataset: str, source_name: str, exc: Exception, cooldown_seconds: float) -> None:
        stat = self.get(dataset, source_name)
        stat.failure_count += 1
        stat.consecutive_failures += 1
        stat.last_error = f"{type(exc).__name__}: {exc}"
        if stat.consecutive_failures >= self.failure_threshold:
            stat.open_until = time.time() + cooldown_seconds

    def rank_key(self, dataset: str, route: SourceRoute) -> tuple[Any, ...]:
        stat = self.get(dataset, route.name)
        is_open = stat.open_until > time.time()
        # 拉普拉斯平滑，避免新源没有历史分时被无限放大
        success_rate = (stat.success_count + 1) / (stat.success_count + stat.failure_count + 2)
        latency = stat.ewma_latency_ms if stat.ewma_latency_ms is not None else 10_000.0
        return (is_open, -success_rate, latency, route.priority, route.name)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return {
            key: {
                "success_count": value.success_count,
                "failure_count": value.failure_count,
                "consecutive_failures": value.consecutive_failures,
                "ewma_latency_ms": value.ewma_latency_ms,
                "open_until": value.open_until,
                "last_error": value.last_error,
            }
            for key, value in self._stats.items()
        }


class AKShareStockFetcher:
    """Unified AkShare wrapper with configurable multi-source routing and fallback."""

    DATASET_TO_API = {
        "a_spot": "stock_zh_a_spot_em",
        "a_hist": "stock_zh_a_hist",
        "a_minute": "stock_zh_a_minute",
        "stock_info": "stock_individual_info_em",
        "index_spot": "stock_zh_index_spot_em",
        "index_hist": "stock_zh_index_daily_em",
        "etf_spot": "fund_etf_spot_em",
        "etf_hist": "fund_etf_hist_em",
        "trade_calendar": "tool_trade_date_hist_sina",
        "industry_list": "stock_board_industry_name_em",
        "industry_constituents": "stock_board_industry_cons_em",
        "sector_fund_flow": "stock_sector_fund_flow_rank",
        "shareholder_count": "stock_zh_a_gdhs",
        "dragon_tiger": "stock_lhb_detail_em",
        "notice": "stock_notice_report",
    }

    DEFAULT_SOURCE_ROUTES: dict[str, list[dict[str, Any]]] = {
        "a_spot": [
            {"name": "eastmoney", "api_name": "stock_zh_a_spot_em", "priority": 10, "enabled": True},
            {"name": "sina", "api_name": "stock_zh_a_spot", "priority": 20, "enabled": True},
        ],
        "a_hist": [
            {"name": "eastmoney", "api_name": "stock_zh_a_hist", "priority": 10, "enabled": True},
            {"name": "tencent", "api_name": "stock_zh_a_hist_tx", "priority": 20, "enabled": True},
            {"name": "sina", "api_name": "stock_zh_a_daily", "priority": 30, "enabled": True},
        ],
        "index_spot": [
            {"name": "eastmoney", "api_name": "stock_zh_index_spot_em", "priority": 10, "enabled": True},
            {"name": "sina", "api_name": "stock_zh_index_spot_sina", "priority": 20, "enabled": True},
            {"name": "tx_fallback", "api_name": "__index_spot_from_tx__", "priority": 30, "enabled": True},
        ],
        "index_hist": [
            {"name": "eastmoney", "api_name": "stock_zh_index_daily_em", "priority": 10, "enabled": True},
            {"name": "tencent", "api_name": "stock_zh_index_daily_tx", "priority": 20, "enabled": True},
        ],
        "etf_spot": [
            {"name": "eastmoney", "api_name": "fund_etf_spot_em", "priority": 10, "enabled": True},
            {"name": "ths", "api_name": "fund_etf_spot_ths", "priority": 20, "enabled": True},
        ],
        "etf_hist": [
            {"name": "eastmoney", "api_name": "fund_etf_hist_em", "priority": 10, "enabled": True},
            {"name": "sina", "api_name": "fund_etf_hist_sina", "priority": 20, "enabled": True},
        ],
        "trade_calendar": [
            {"name": "sina", "api_name": "tool_trade_date_hist_sina", "priority": 10, "enabled": True},
        ],
        "a_minute": [
            {"name": "eastmoney", "api_name": "stock_zh_a_minute", "priority": 10, "enabled": True}
        ],
        "stock_info": [
            {"name": "eastmoney", "api_name": "stock_individual_info_em", "priority": 10, "enabled": True}
        ],
        "industry_list": [
            {"name": "eastmoney", "api_name": "stock_board_industry_name_em", "priority": 10, "enabled": True}
        ],
        "industry_constituents": [
            {"name": "eastmoney", "api_name": "stock_board_industry_cons_em", "priority": 10, "enabled": True}
        ],
        "sector_fund_flow": [
            {"name": "eastmoney", "api_name": "stock_sector_fund_flow_rank", "priority": 10, "enabled": True}
        ],
        "shareholder_count": [
            {"name": "eastmoney", "api_name": "stock_zh_a_gdhs", "priority": 10, "enabled": True}
        ],
        "dragon_tiger": [
            {"name": "eastmoney", "api_name": "stock_lhb_detail_em", "priority": 10, "enabled": True}
        ],
        "notice": [
            {"name": "eastmoney", "api_name": "stock_notice_report", "priority": 10, "enabled": True}
        ],
    }

    def __init__(self, config: FetchConfig | None = None) -> None:
        self.config = config or FetchConfig()
        if self.config.enable_cache:
            self.config.cache_dir.mkdir(parents=True, exist_ok=True)

        self.route_store = SourceRouteStore(
            config_path=self.config.source_config_path,
            default_routes=self.DEFAULT_SOURCE_ROUTES,
            legacy_config_paths=[Path("cache/akshare_sources.json")],
        )
        self.health_tracker = SourceHealthTracker(
            failure_threshold=self.config.source_failure_threshold,
            cooldown_seconds=self.config.source_cooldown_seconds,
        )

    def available_datasets(self) -> list[str]:
        return sorted(self.DATASET_TO_API.keys())

    # ---------- source management ----------
    def list_sources(self, dataset: str | None = None) -> dict[str, list[dict[str, Any]]] | list[dict[str, Any]]:
        def serialize(items: list[SourceRoute]) -> list[dict[str, Any]]:
            return [asdict(item) for item in items]

        if dataset is None:
            raw = self.route_store.list()
            return {k: serialize(v) for k, v in raw.items()}
        return serialize(self.route_store.list(dataset))

    def add_source(
        self,
        dataset: str,
        source_name: str,
        api_name: str,
        priority: int = 100,
        enabled: bool = True,
    ) -> None:
        self.route_store.add(
            dataset,
            SourceRoute(name=source_name, api_name=api_name, priority=priority, enabled=enabled),
        )

    def remove_source(self, dataset: str, source_name: str) -> bool:
        return self.route_store.remove(dataset, source_name)

    def set_source_enabled(self, dataset: str, source_name: str, enabled: bool) -> bool:
        return self.route_store.set_enabled(dataset, source_name, enabled)

    def source_health(self) -> dict[str, dict[str, Any]]:
        return self.health_tracker.snapshot()

    # ---------- main fetch ----------
    def fetch(self, dataset: str, **params: Any) -> pd.DataFrame:
        routes = self._ordered_routes(dataset)
        if not routes:
            available = ", ".join(self.available_datasets())
            raise ValueError(f"Unknown dataset: {dataset}. Available datasets: {available}")

        errors: list[str] = []
        for route in routes:
            if self.health_tracker.is_open(dataset, route.name):
                errors.append(f"{route.name}: circuit_open")
                continue

            adapted = self._adapt_params(dataset=dataset, api_name=route.api_name, params=params)
            start = time.perf_counter()
            try:
                result = self._call_api(route.api_name, **adapted)
                result = self._post_process(dataset=dataset, api_name=route.api_name, params=adapted, df=result)
                latency_ms = (time.perf_counter() - start) * 1000
                self.health_tracker.record_success(dataset, route.name, latency_ms)
                return result
            except Exception as exc:  # pragma: no cover - external APIs
                self.health_tracker.record_failure(
                    dataset=dataset,
                    source_name=route.name,
                    exc=exc,
                    cooldown_seconds=self.config.source_cooldown_seconds,
                )
                errors.append(f"{route.name}: {type(exc).__name__}: {exc}")

        raise DataFetchError(
            f"Failed dataset '{dataset}' from all sources. Errors: {' | '.join(errors[-6:])}"
        )

    def _ordered_routes(self, dataset: str) -> list[SourceRoute]:
        routes = self.route_store.get_enabled(dataset)
        if not routes:
            api_name = self.DATASET_TO_API.get(dataset)
            if not api_name:
                return []
            routes = [SourceRoute(name="default", api_name=api_name, priority=100, enabled=True)]
        return sorted(routes, key=lambda x: self.health_tracker.rank_key(dataset, x))

    # ---------- typed methods ----------
    def get_a_spot(self) -> pd.DataFrame:
        return self.fetch("a_spot")

    def get_a_hist(
        self,
        symbol: str,
        start_date: str = "19700101",
        end_date: str = "20500101",
        period: str = "daily",
        adjust: str = "",
    ) -> pd.DataFrame:
        return self.fetch(
            "a_hist",
            symbol=symbol,
            start_date=self._normalize_date(start_date),
            end_date=self._normalize_date(end_date),
            period=period,
            adjust=adjust,
        )

    def get_a_minute(self, symbol: str, period: str = "1", adjust: str = "") -> pd.DataFrame:
        return self.fetch("a_minute", symbol=symbol, period=period, adjust=adjust)

    def get_stock_info(self, symbol: str) -> pd.DataFrame:
        return self.fetch("stock_info", symbol=symbol)

    def get_index_spot(self, symbol: str = "上证系列指数") -> pd.DataFrame:
        return self.fetch("index_spot", symbol=symbol)

    def get_index_hist(
        self,
        symbol: str,
        start_date: str = "19900101",
        end_date: str = "20500101",
    ) -> pd.DataFrame:
        return self.fetch(
            "index_hist",
            symbol=symbol,
            start_date=self._normalize_date(start_date),
            end_date=self._normalize_date(end_date),
        )

    def get_etf_spot(self) -> pd.DataFrame:
        return self.fetch("etf_spot")

    def get_etf_hist(
        self,
        symbol: str,
        start_date: str = "19700101",
        end_date: str = "20500101",
        period: str = "daily",
        adjust: str = "",
    ) -> pd.DataFrame:
        return self.fetch(
            "etf_hist",
            symbol=symbol,
            start_date=self._normalize_date(start_date),
            end_date=self._normalize_date(end_date),
            period=period,
            adjust=adjust,
        )

    def get_trade_calendar(self) -> pd.DataFrame:
        return self.fetch("trade_calendar")

    def get_industry_list(self) -> pd.DataFrame:
        return self.fetch("industry_list")

    def get_industry_constituents(self, industry_name: str) -> pd.DataFrame:
        return self.fetch("industry_constituents", symbol=industry_name)

    def get_sector_fund_flow(
        self,
        indicator: str = "今日",
        sector_type: str = "行业资金流",
    ) -> pd.DataFrame:
        return self.fetch("sector_fund_flow", indicator=indicator, sector_type=sector_type)

    def get_shareholder_count(self, report_date: str) -> pd.DataFrame:
        return self.fetch("shareholder_count", symbol=self._normalize_date(report_date))

    def get_dragon_tiger(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.fetch(
            "dragon_tiger",
            start_date=self._normalize_date(start_date),
            end_date=self._normalize_date(end_date),
        )

    def get_notice(self, date: str, symbol: str = "全部") -> pd.DataFrame:
        return self.fetch("notice", symbol=symbol, date=self._normalize_date(date))

    @staticmethod
    def save(df: pd.DataFrame, output_path: str | Path) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            df.to_csv(path, index=False, encoding="utf-8-sig")
        elif suffix == ".parquet":
            df.to_parquet(path, index=False)
        else:
            raise ValueError("Only .csv and .parquet are supported.")
        return path

    # ---------- internals ----------
    def _call_api(self, api_name: str, **params: Any) -> pd.DataFrame:
        fn = self._resolve_api(api_name)
        cleaned = {k: v for k, v in params.items() if v is not None}

        cache_path = self._build_cache_path(api_name, cleaned)
        if cache_path and cache_path.exists():
            return pd.read_parquet(cache_path)

        last_error: Exception | None = None
        signature = inspect.signature(fn)
        accepts_timeout = "timeout" in signature.parameters

        for attempt in range(1, self.config.retries + 1):
            try:
                payload = cleaned.copy()
                if accepts_timeout and "timeout" not in payload:
                    payload["timeout"] = 12
                result = fn(**payload)
                if not isinstance(result, pd.DataFrame):
                    raise TypeError(f"API {api_name} did not return DataFrame.")
                if cache_path:
                    result.to_parquet(cache_path, index=False)
                return result
            except Exception as exc:  # pragma: no cover - external API failure path
                last_error = exc
                if attempt < self.config.retries:
                    delay = self.config.retry_interval * (2 ** (attempt - 1))
                    if self.config.retry_jitter > 0:
                        delay += random.uniform(0, self.config.retry_jitter)
                    time.sleep(delay)

        raise DataFetchError(
            f"Failed to fetch data from '{api_name}' after {self.config.retries} retries: {last_error}"
        ) from last_error

    @staticmethod
    def _resolve_api(api_name: str) -> Callable[..., pd.DataFrame]:
        if api_name == "__index_spot_from_tx__":
            return AKShareStockFetcher._index_spot_from_tx

        fn = getattr(ak, api_name, None)
        if fn is None:
            raise AttributeError(f"AkShare API '{api_name}' does not exist in current version.")
        return fn

    def _build_cache_path(self, api_name: str, params: dict[str, Any]) -> Path | None:
        if not self.config.enable_cache:
            return None
        payload = json.dumps(params, ensure_ascii=False, sort_keys=True, default=str)
        digest = hashlib.sha1(f"{api_name}:{payload}".encode("utf-8")).hexdigest()[:16]
        return self.config.cache_dir / f"{api_name}_{digest}.parquet"

    def _adapt_params(self, dataset: str, api_name: str, params: dict[str, Any]) -> dict[str, Any]:
        adapted = {k: v for k, v in params.items() if v is not None}

        if api_name in {"stock_zh_a_hist_tx", "stock_zh_a_daily"} and "symbol" in adapted:
            adapted["symbol"] = self._to_prefixed_stock_symbol(str(adapted["symbol"]))

        if api_name == "stock_zh_index_daily_tx" and "symbol" in adapted:
            adapted["symbol"] = self._to_prefixed_index_symbol(str(adapted["symbol"]))
            adapted.pop("start_date", None)
            adapted.pop("end_date", None)

        if api_name == "fund_etf_hist_sina" and "symbol" in adapted:
            adapted["symbol"] = self._to_prefixed_stock_symbol(str(adapted["symbol"]))
            adapted.pop("start_date", None)
            adapted.pop("end_date", None)
            adapted.pop("period", None)
            adapted.pop("adjust", None)

        if api_name in {"stock_zh_a_hist_tx", "stock_zh_a_daily"}:
            adapted.pop("period", None)

        if api_name == "stock_zh_index_spot_sina":
            adapted.pop("symbol", None)

        if api_name == "__index_spot_from_tx__":
            adapted.pop("symbol", None)

        if api_name == "fund_etf_spot_ths":
            adapted.pop("symbol", None)

        # 参数最小化，避免不兼容参数传入
        sig = inspect.signature(self._resolve_api(api_name))
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in adapted.items() if k in allowed}

    def _post_process(self, dataset: str, api_name: str, params: dict[str, Any], df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        if dataset in {"a_hist", "index_hist", "etf_hist"}:
            # 统一日期过滤，兼容某些源不支持请求区间
            date_col = None
            for candidate in ["日期", "date", "trade_date"]:
                if candidate in df.columns:
                    date_col = candidate
                    break

            if date_col and ("start_date" in params or "end_date" in params):
                tmp = df.copy()
                tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
                if params.get("start_date"):
                    start = pd.to_datetime(params["start_date"], errors="coerce")
                    tmp = tmp[tmp[date_col] >= start]
                if params.get("end_date"):
                    end = pd.to_datetime(params["end_date"], errors="coerce")
                    tmp = tmp[tmp[date_col] <= end]
                return tmp.reset_index(drop=True)

        return df

    @staticmethod
    def _normalize_date(value: str) -> str:
        date_str = str(value).strip()
        digits = date_str.replace("-", "").replace("/", "")
        if len(digits) != 8 or not digits.isdigit():
            raise ValueError(f"Invalid date: {value}. Expected YYYYMMDD or YYYY-MM-DD.")
        return digits

    @staticmethod
    def _to_prefixed_stock_symbol(symbol: str) -> str:
        raw = symbol.strip().lower()
        if raw.startswith(("sh", "sz", "bj")):
            return raw

        digits = "".join(ch for ch in raw if ch.isdigit())
        if len(digits) != 6:
            return raw

        if digits.startswith(("5", "6", "9")):
            return f"sh{digits}"
        if digits.startswith(("4", "8")):
            return f"bj{digits}"
        return f"sz{digits}"

    @staticmethod
    def _to_prefixed_index_symbol(symbol: str) -> str:
        raw = symbol.strip().lower()
        if raw.startswith(("sh", "sz", "csi")):
            return raw

        digits = "".join(ch for ch in raw if ch.isdigit())
        if len(digits) != 6:
            return raw

        if digits.startswith(("0", "9")):
            return f"sh{digits}"
        return f"sz{digits}"

    @staticmethod
    def _index_spot_from_tx() -> pd.DataFrame:
        """
        Fallback for index spot when realtime sources are unavailable.
        Build pseudo-spot values from latest two daily bars.
        """
        targets = [
            ("sh000001", "上证指数"),
            ("sz399001", "深证成指"),
            ("sz399006", "创业板指"),
            ("sh000300", "沪深300"),
        ]
        rows: list[dict[str, Any]] = []
        for code, name in targets:
            daily_df = ak.stock_zh_index_daily_tx(symbol=code)
            if daily_df is None or daily_df.empty:
                continue

            daily_df = daily_df.sort_values("date").reset_index(drop=True)
            latest = daily_df.iloc[-1]
            prev = daily_df.iloc[-2] if len(daily_df) > 1 else latest
            latest_close = float(latest["close"])
            prev_close = float(prev["close"])
            change_value = latest_close - prev_close
            change_ratio = (change_value / prev_close * 100) if prev_close else 0.0

            rows.append(
                {
                    "代码": code,
                    "名称": name,
                    "最新价": latest_close,
                    "涨跌额": change_value,
                    "涨跌幅": change_ratio,
                    "最高": float(latest["high"]),
                    "最低": float(latest["low"]),
                    "今开": float(latest["open"]),
                    "昨收": prev_close,
                    "成交额": float(latest["amount"]),
                }
            )

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)
