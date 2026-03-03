from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import requests


@dataclass(frozen=True)
class ProbeTarget:
    name: str
    provider: str
    url: str
    enabled: bool = True


@dataclass
class ProbeResult:
    target: ProbeTarget
    attempts: int
    success_count: int
    avg_latency_ms: float
    errors: list[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.attempts == 0:
            return 0.0
        return self.success_count / self.attempts


@dataclass
class ProviderProbeResult:
    provider: str
    attempts: int
    success_count: int
    avg_latency_ms: float
    best_url: str

    @property
    def success_rate(self) -> float:
        if self.attempts == 0:
            return 0.0
        return self.success_count / self.attempts


@dataclass
class SingleUrlProbeResult:
    url: str
    ok: bool
    status_code: int | None
    latency_ms: float | None
    error: str = ""
    body_size: int = 0


class DataSourceProbe:
    """Lightweight URL availability probe.

    Config file format (recommended):
    [
      "https://a.example.com/ping",
      "https://b.example.com/health"
    ]
    """

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Connection": "close",
    }

    def __init__(
        self,
        attempts: int = 2,
        timeout: float = 6.0,
        interval: float = 0.2,
        config_path: Path | None = None,
    ) -> None:
        self.attempts = max(1, attempts)
        self.timeout = timeout
        self.interval = max(0.0, interval)
        self.config_path = config_path or Path("config/probe_urls.json")
        self.urls = self._load_urls()

    # ---------- config ----------
    def _load_urls(self) -> list[str]:
        primary = self._read_json(self.config_path)
        if primary is not None:
            return self._extract_urls(primary)

        return []

    def _save_urls(self, urls: list[str] | None = None) -> None:
        current = urls if urls is not None else self.urls
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _read_json(path: Path) -> list | None:
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
            return None
        except Exception:
            return None

    @staticmethod
    def _extract_urls(items: list) -> list[str]:
        urls: list[str] = []
        for item in items:
            if isinstance(item, str):
                cleaned = item.strip()
                if cleaned:
                    urls.append(cleaned)
                continue

            if isinstance(item, dict):
                url = str(item.get("url", "")).strip()
                if not url:
                    continue
                if bool(item.get("enabled", True)):
                    urls.append(url)
        # de-dup with order
        return list(dict.fromkeys(urls))

    # ---------- simple user APIs ----------
    def list_urls(self) -> list[str]:
        return list(self.urls)

    def add_url(self, url: str) -> bool:
        cleaned = url.strip()
        if not cleaned:
            return False
        if cleaned in self.urls:
            return False
        self.urls.append(cleaned)
        self._save_urls()
        return True

    def remove_url(self, url: str) -> bool:
        cleaned = url.strip()
        before = len(self.urls)
        self.urls = [item for item in self.urls if item != cleaned]
        changed = len(self.urls) != before
        if changed:
            self._save_urls()
        return changed

    def replace_urls(self, urls: list[str]) -> None:
        self.urls = self._extract_urls(urls)
        self._save_urls()

    # Backward-compatible helpers
    def list_targets_simple(self, enabled_only: bool = False) -> list[dict[str, str | bool]]:
        _ = enabled_only
        return [{"name": f"url_{idx}", "url": url, "provider": "custom", "enabled": True} for idx, url in enumerate(self.urls, start=1)]

    def upsert_target(
        self,
        *,
        name: str,
        url: str,
        provider: str = "custom",
        enabled: bool = True,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        expected_tokens: tuple[str, ...] = (),
    ) -> None:
        _ = (name, provider, params, headers, expected_tokens)
        if enabled:
            self.add_url(url)
        else:
            self.remove_url(url)

    def remove_target_by_url(self, url: str) -> bool:
        return self.remove_url(url)

    # ---------- single URL probe ----------
    @classmethod
    def probe_url(
        cls,
        url: str,
        timeout: float = 5.0,
        method: str = "GET",
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        expected_tokens: tuple[str, ...] = (),
        min_status: int = 200,
        max_status: int = 299,
    ) -> SingleUrlProbeResult:
        request_headers = cls.DEFAULT_HEADERS.copy()
        if headers:
            request_headers.update(headers)

        started = time.perf_counter()
        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                params=params,
                headers=request_headers,
                timeout=timeout,
            )
            latency_ms = (time.perf_counter() - started) * 1000
            body = resp.text or ""
            status_ok = min_status <= resp.status_code <= max_status
            tokens_ok = all(token in body for token in expected_tokens)
            ok = status_ok and tokens_ok
            return SingleUrlProbeResult(
                url=resp.url,
                ok=ok,
                status_code=resp.status_code,
                latency_ms=latency_ms,
                error="" if ok else "status_or_token_check_failed",
                body_size=len(body),
            )
        except Exception as exc:  # pragma: no cover - network path
            return SingleUrlProbeResult(
                url=url,
                ok=False,
                status_code=None,
                latency_ms=(time.perf_counter() - started) * 1000,
                error=f"{type(exc).__name__}: {exc}",
                body_size=0,
            )

    @classmethod
    def is_url_available(
        cls,
        url: str,
        timeout: float = 5.0,
        method: str = "GET",
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> bool:
        result = cls.probe_url(
            url=url,
            timeout=timeout,
            method=method,
            params=params,
            headers=headers,
        )
        return result.ok

    # ---------- multi URL probe ----------
    def probe_all(self, targets: Iterable[ProbeTarget] | None = None) -> list[ProbeResult]:
        if targets is not None:
            target_list = [item for item in targets if item.enabled]
        else:
            target_list = [
                ProbeTarget(name=f"url_{idx}", provider="custom", url=url, enabled=True)
                for idx, url in enumerate(self.urls, start=1)
            ]

        if not target_list:
            return []

        results = [self._probe_target(target) for target in target_list]
        return sorted(results, key=lambda item: (item.success_rate, -item.avg_latency_ms), reverse=True)

    def _probe_target(self, target: ProbeTarget) -> ProbeResult:
        success_count = 0
        latencies: list[float] = []
        errors: list[str] = []

        for idx in range(self.attempts):
            result = self.probe_url(url=target.url, timeout=self.timeout)
            if result.ok:
                success_count += 1
                if result.latency_ms is not None:
                    latencies.append(result.latency_ms)
            else:
                errors.append(result.error or f"status={result.status_code}")

            if idx < self.attempts - 1 and self.interval > 0:
                time.sleep(self.interval)

        avg_latency_ms = sum(latencies) / len(latencies) if latencies else float("inf")
        return ProbeResult(
            target=target,
            attempts=self.attempts,
            success_count=success_count,
            avg_latency_ms=avg_latency_ms,
            errors=errors[-3:],
        )

    def probe_best_provider(self, targets: Iterable[ProbeTarget] | None = None) -> ProviderProbeResult:
        results = self.probe_all(targets=targets)
        if not results:
            raise RuntimeError("No URLs configured.")

        best = results[0]
        return ProviderProbeResult(
            provider=best.target.provider,
            attempts=best.attempts,
            success_count=best.success_count,
            avg_latency_ms=best.avg_latency_ms,
            best_url=best.target.url,
        )

    def probe_best_url(self, targets: Iterable[ProbeTarget] | None = None) -> str:
        best = self.probe_best_provider(targets=targets)
        if best.success_count == 0:
            raise RuntimeError("All URLs are unreachable in current network.")
        return best.best_url


def main() -> None:
    probe = DataSourceProbe()
    results = probe.probe_all()
    if not results:
        print("No URLs configured in config/probe_urls.json")
        return

    for item in results:
        latency = "inf" if item.avg_latency_ms == float("inf") else f"{item.avg_latency_ms:.1f} ms"
        print(
            f"{item.target.url} success={item.success_count}/{item.attempts} "
            f"rate={item.success_rate:.2f} latency={latency}"
        )

    print("\nBest URL:")
    print(probe.probe_best_url())


if __name__ == "__main__":
    main()
