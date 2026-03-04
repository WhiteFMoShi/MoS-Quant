import tempfile
import unittest
from pathlib import Path

import pandas as pd

from mos_quant import SeriesCacheManager, TimeSeriesCache


class TestTimeSeriesCache(unittest.TestCase):
    def test_normalize_dedup_by_date(self) -> None:
        cache = TimeSeriesCache(Path(tempfile.mkdtemp()))
        df = pd.DataFrame(
            {
                "date": ["2025-01-02", "2025-01-02", "2025-01-03"],
                "x": [1, 2, 3],
            }
        )
        out = cache.normalize(df, minute_mode=False)
        self.assertEqual(out["date"].tolist(), ["2025-01-02", "2025-01-03"])
        self.assertEqual(out["x"].tolist(), [2, 3])

    def test_missing_ranges_daily(self) -> None:
        cache = TimeSeriesCache(Path(tempfile.mkdtemp()))
        df = pd.DataFrame({"date": ["2025-01-02", "2025-01-03"]})
        ranges = cache.missing_ranges(df, "20250101", "20250104", minute_mode=False)
        # Left gap is intentionally tolerated for daily mode; right gap must be fetched.
        self.assertEqual(ranges, [("20250104", "20250104")])


class TestSeriesCacheManager(unittest.TestCase):
    def test_meta_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mgr = SeriesCacheManager(Path(td))
            params = {"dataset": "demo", "symbol": "000001", "period": "daily"}
            mgr.save_meta(params, {"a": 1})
            loaded = mgr.load_meta(params)
            self.assertIsInstance(loaded, dict)
            self.assertEqual(loaded["a"], 1)
            self.assertIn("updated_at", loaded)


if __name__ == "__main__":
    unittest.main(verbosity=2)

