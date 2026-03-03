from __future__ import annotations

import time
import unittest

import pandas as pd

from core.timeseries_cache import TimeSeriesCache


class TestCacheSpeed(unittest.TestCase):
    def test_normalize_and_filter_fast(self) -> None:
        rows = 5000
        start = pd.Timestamp("2026-01-01 09:30:00")
        df = pd.DataFrame(
            {
                "datetime": [(start + pd.Timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S") for i in range(rows)],
                "close": [float(i) for i in range(rows)],
            }
        )
        t0 = time.perf_counter()
        norm = TimeSeriesCache.normalize(df, minute_mode=True)
        mid = time.perf_counter()
        out = TimeSeriesCache.filter_by_range(
            norm,
            start_value="2026-01-01 10:00:00",
            end_value="2026-01-01 12:00:00",
            minute_mode=True,
        )
        t1 = time.perf_counter()
        self.assertGreater(len(out.index), 0)
        # Budget is intentionally loose to avoid flakiness on slow machines.
        self.assertLess(mid - t0, 0.35)
        self.assertLess(t1 - mid, 0.35)

