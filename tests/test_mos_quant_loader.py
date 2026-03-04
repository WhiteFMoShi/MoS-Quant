import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from mos_quant import FileCache, LoaderConfig, LoaderError, MoSQuantLoader
from mos_quant.core.loader import _normalize_trade_date_series
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
    def test_normalize_trade_date_series_mixed_formats(self) -> None:
        s = pd.Series(
            [
                "1991-01-02T00:00:00.000Z",
                "1991-01-03",
                "1991-Jan-04T00:00:00.000Z",
                "19910105",
                pd.Timestamp("1991-01-06"),
            ]
        )
        out = _normalize_trade_date_series(s)
        self.assertEqual(str(out.iloc[0]), "1991-01-02")
        self.assertEqual(str(out.iloc[1]), "1991-01-03")
        self.assertEqual(str(out.iloc[2]), "1991-01-04")
        self.assertEqual(str(out.iloc[3]), "1991-01-05")
        self.assertEqual(str(out.iloc[4]), "1991-01-06")

    def test_fail_fast_when_no_urls(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            cfg = LoaderConfig(
                probe_urls_path=base / "config/probe_urls.json",
                default_source_path=base / "cache/watch/default_data_source.json",
                trade_calendar_cache_dir=base / "cache/market/trade_calendar",
                stock_list_cache_dir=base / "cache/market/stock_list",
            )
            loader = MoSQuantLoader(
                cfg,
                probe=FakeProbe(results=[]),
                cache=FileCache(base_dir=cfg.trade_calendar_cache_dir),
                stock_cache=FileCache(base_dir=cfg.stock_list_cache_dir),
                calendar_fetcher=lambda url, timeout: pd.DataFrame({"trade_date": []}),
                stock_list_fetcher=lambda: pd.DataFrame({"code": ["000001"], "name": ["平安银行"]}),
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
                stock_list_cache_dir=base / "cache/market/stock_list",
            )

            best_url = "https://example.com/calendar"
            results = [_mk_result(best_url, success=2, attempts=2, latency=12.3)]
            calls = {"n": 0}
            stock_calls = {"n": 0}

            def cal_fetch(url: str, timeout: float) -> pd.DataFrame:
                calls["n"] += 1
                self.assertEqual(url, best_url)
                return pd.DataFrame({"trade_date": [pd.Timestamp("2025-01-02").date()]})

            def stock_fetch() -> pd.DataFrame:
                stock_calls["n"] += 1
                return pd.DataFrame({"code": ["000001", "000002"], "name": ["平安银行", "万科A"]})

            loader = MoSQuantLoader(
                cfg,
                probe=FakeProbe(results=results),
                cache=FileCache(base_dir=cfg.trade_calendar_cache_dir),
                calendar_fetcher=cal_fetch,
                stock_cache=FileCache(base_dir=cfg.stock_list_cache_dir),
                stock_list_fetcher=stock_fetch,
            )

            ctx1 = loader.run()
            self.assertEqual(ctx1.default_url, best_url)
            self.assertEqual(calls["n"], 1)
            self.assertEqual(stock_calls["n"], 1)
            self.assertTrue(cfg.default_source_path.exists())
            saved = json.loads(cfg.default_source_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["url"], best_url)
            self.assertTrue(hasattr(ctx1, "a_stock_list"))
            self.assertFalse(ctx1.a_stock_list.empty)

            # Second run should hit cache (no additional fetch)
            ctx2 = loader.run()
            self.assertEqual(ctx2.default_url, best_url)
            self.assertEqual(calls["n"], 1)
            self.assertEqual(stock_calls["n"], 1)

            # Clear caches, third run should re-fetch both.
            for cache_dir in (cfg.trade_calendar_cache_dir, cfg.stock_list_cache_dir):
                if cache_dir.exists():
                    for p in cache_dir.rglob("*"):
                        if p.is_file():
                            p.unlink()

            ctx3 = loader.run()
            self.assertEqual(ctx3.default_url, best_url)
            self.assertEqual(calls["n"], 2)
            self.assertEqual(stock_calls["n"], 2)

    def test_fail_fast_when_calendar_fetch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            cfg = LoaderConfig(
                probe_urls_path=base / "config/probe_urls.json",
                default_source_path=base / "cache/watch/default_data_source.json",
                trade_calendar_cache_dir=base / "cache/market/trade_calendar",
                stock_list_cache_dir=base / "cache/market/stock_list",
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
                stock_cache=FileCache(base_dir=cfg.stock_list_cache_dir),
                stock_list_fetcher=lambda: pd.DataFrame({"code": ["000001"], "name": ["平安银行"]}),
            )

            with self.assertRaises(LoaderError):
                loader.run()

            # We only persist default source after all steps succeed.
            self.assertFalse(cfg.default_source_path.exists())

    def test_fail_fast_when_stock_list_fetch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            cfg = LoaderConfig(
                probe_urls_path=base / "config/probe_urls.json",
                default_source_path=base / "cache/watch/default_data_source.json",
                trade_calendar_cache_dir=base / "cache/market/trade_calendar",
                stock_list_cache_dir=base / "cache/market/stock_list",
            )

            best_url = "https://example.com/calendar"
            results = [_mk_result(best_url, success=2, attempts=2, latency=10.0)]

            def cal_fetch(url: str, timeout: float) -> pd.DataFrame:
                return pd.DataFrame({"trade_date": [pd.Timestamp("2025-01-02").date()]})

            def stock_fetch() -> pd.DataFrame:
                raise RuntimeError("upstream blocked")

            loader = MoSQuantLoader(
                cfg,
                probe=FakeProbe(results=results),
                cache=FileCache(base_dir=cfg.trade_calendar_cache_dir),
                calendar_fetcher=cal_fetch,
                stock_cache=FileCache(base_dir=cfg.stock_list_cache_dir),
                stock_list_fetcher=stock_fetch,
            )

            with self.assertRaises(LoaderError):
                loader.run()

            self.assertFalse(cfg.default_source_path.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
