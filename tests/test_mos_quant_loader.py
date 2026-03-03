import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from mos_quant import FileCache, LoaderConfig, LoaderError, MoSQuantLoader
from mos_quant.data.network_probe import DataSourceProbe, ProbeResult, ProbeTarget


class FakeProbe(DataSourceProbe):
    def __init__(self, results: list[ProbeResult]) -> None:
        self._results = results

    def probe_all(self, targets=None):  # type: ignore[override]
        return list(self._results)


def _mk_result(url: str, success: int, attempts: int, latency: float) -> ProbeResult:
    return ProbeResult(
        target=ProbeTarget(name="t", provider="custom", url=url, enabled=True),
        attempts=attempts,
        success_count=success,
        avg_latency_ms=latency,
        errors=[],
    )


class TestMoSQuantLoader(unittest.TestCase):
    def test_fail_fast_when_no_urls(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            cfg = LoaderConfig(
                probe_urls_path=base / "config/probe_urls.json",
                default_source_path=base / "cache/watch/default_data_source.json",
                trade_calendar_cache_dir=base / "cache/market/trade_calendar",
            )
            loader = MoSQuantLoader(
                cfg,
                probe=FakeProbe(results=[]),
                cache=FileCache(base_dir=cfg.trade_calendar_cache_dir),
                calendar_fetcher=lambda url, timeout: pd.DataFrame({"trade_date": []}),
            )

            with self.assertRaises(LoaderError):
                loader.run()

            self.assertFalse(cfg.default_source_path.exists())
            self.assertFalse(cfg.trade_calendar_cache_dir.exists() and any(cfg.trade_calendar_cache_dir.iterdir()))

    def test_success_and_cache_hit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            cfg = LoaderConfig(
                probe_urls_path=base / "config/probe_urls.json",
                default_source_path=base / "cache/watch/default_data_source.json",
                trade_calendar_cache_dir=base / "cache/market/trade_calendar",
            )

            best_url = "https://example.com/calendar"
            results = [_mk_result(best_url, success=2, attempts=2, latency=12.3)]
            calls = {"n": 0}

            def cal_fetch(url: str, timeout: float) -> pd.DataFrame:
                calls["n"] += 1
                self.assertEqual(url, best_url)
                return pd.DataFrame({"trade_date": [pd.Timestamp("2025-01-02").date()]})

            loader = MoSQuantLoader(
                cfg,
                probe=FakeProbe(results=results),
                cache=FileCache(base_dir=cfg.trade_calendar_cache_dir),
                calendar_fetcher=cal_fetch,
            )

            ctx1 = loader.run()
            self.assertEqual(ctx1.default_url, best_url)
            self.assertEqual(calls["n"], 1)
            self.assertTrue(cfg.default_source_path.exists())
            saved = json.loads(cfg.default_source_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["url"], best_url)

            # Second run should hit cache (no additional fetch)
            ctx2 = loader.run()
            self.assertEqual(ctx2.default_url, best_url)
            self.assertEqual(calls["n"], 1)

    def test_fail_fast_when_calendar_fetch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            cfg = LoaderConfig(
                probe_urls_path=base / "config/probe_urls.json",
                default_source_path=base / "cache/watch/default_data_source.json",
                trade_calendar_cache_dir=base / "cache/market/trade_calendar",
            )

            best_url = "https://example.com/calendar"
            results = [_mk_result(best_url, success=2, attempts=2, latency=10.0)]

            def cal_fetch(url: str, timeout: float) -> pd.DataFrame:
                raise RuntimeError("boom")

            loader = MoSQuantLoader(
                cfg,
                probe=FakeProbe(results=results),
                cache=FileCache(base_dir=cfg.trade_calendar_cache_dir),
                calendar_fetcher=cal_fetch,
            )

            with self.assertRaises(LoaderError):
                loader.run()

            # We only persist default source after step 3 succeeds.
            self.assertFalse(cfg.default_source_path.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
