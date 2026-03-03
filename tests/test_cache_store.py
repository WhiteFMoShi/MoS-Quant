import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from mos_quant import CacheKey, FileCache, FreshnessLevel


class TestFileCache(unittest.TestCase):
    def test_second_freshness(self):
        base = datetime(2026, 3, 3, 12, 0, 0, tzinfo=timezone.utc)
        clock = {"now": base}

        def now_fn():
            return clock["now"]

        with tempfile.TemporaryDirectory() as tmp:
            cache = FileCache(base_dir=tmp, now_fn=now_fn)
            key = CacheKey(namespace="demo", params={"a": 1})
            df = pd.DataFrame({"x": [1, 2, 3]})

            self.assertIsNone(cache.get_df(key, FreshnessLevel.SECOND))
            cache.set_df(key, FreshnessLevel.SECOND, df)
            hit = cache.get_df(key, FreshnessLevel.SECOND)
            self.assertIsNotNone(hit)
            self.assertEqual(hit.shape, df.shape)

            clock["now"] = base + timedelta(seconds=1)
            self.assertIsNone(cache.get_df(key, FreshnessLevel.SECOND))

    def test_minute_freshness(self):
        base = datetime(2026, 3, 3, 12, 0, 30, tzinfo=timezone.utc)
        clock = {"now": base}

        def now_fn():
            return clock["now"]

        with tempfile.TemporaryDirectory() as tmp:
            cache = FileCache(base_dir=tmp, now_fn=now_fn)
            key = CacheKey(namespace="demo", params={"a": 1})
            df = pd.DataFrame({"x": [1]})

            cache.set_df(key, FreshnessLevel.MINUTE, df)
            self.assertIsNotNone(cache.get_df(key, FreshnessLevel.MINUTE))

            clock["now"] = base + timedelta(seconds=20)  # still same minute
            self.assertIsNotNone(cache.get_df(key, FreshnessLevel.MINUTE))

            clock["now"] = base + timedelta(minutes=1)
            self.assertIsNone(cache.get_df(key, FreshnessLevel.MINUTE))

    def test_day_freshness(self):
        base = datetime(2026, 3, 3, 23, 50, 0, tzinfo=timezone.utc)
        clock = {"now": base}

        def now_fn():
            return clock["now"]

        with tempfile.TemporaryDirectory() as tmp:
            cache = FileCache(base_dir=tmp, now_fn=now_fn)
            key = CacheKey(namespace="demo", params={"a": 1})
            df = pd.DataFrame({"x": [1]})

            cache.set_df(key, FreshnessLevel.DAY, df)
            self.assertIsNotNone(cache.get_df(key, FreshnessLevel.DAY))

            clock["now"] = base + timedelta(minutes=1)  # still same day
            self.assertIsNotNone(cache.get_df(key, FreshnessLevel.DAY))

            clock["now"] = base + timedelta(days=1)
            self.assertIsNone(cache.get_df(key, FreshnessLevel.DAY))

    def test_mismatch_freshness_level_is_miss(self):
        base = datetime(2026, 3, 3, 12, 0, 0, tzinfo=timezone.utc)

        def now_fn():
            return base

        with tempfile.TemporaryDirectory() as tmp:
            cache = FileCache(base_dir=tmp, now_fn=now_fn)
            key = CacheKey(namespace="demo", params={"a": 1})
            df = pd.DataFrame({"x": [1]})

            cache.set_df(key, FreshnessLevel.DAY, df)
            self.assertIsNone(cache.get_df(key, FreshnessLevel.MINUTE))


if __name__ == "__main__":
    unittest.main(verbosity=2)
