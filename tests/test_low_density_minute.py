from __future__ import annotations

import unittest

import pandas as pd

from core.data_service import DataService


class TestLowDensityMinute(unittest.TestCase):
    def test_detects_tail_window_only_payload(self) -> None:
        # 120-day 15m window should not be satisfied by ~1-2 days of bars.
        start = pd.Timestamp("2026-01-01 09:30:00")
        ts = [start + pd.Timedelta(minutes=15 * i) for i in range(80)]
        df = pd.DataFrame({"datetime": [t.strftime("%Y-%m-%d %H:%M:%S") for t in ts], "close": [1.0] * len(ts)})
        self.assertTrue(
            DataService._is_low_density_minute_result(
                df,
                span_days=120,
                period="15",
            )
        )

    def test_allows_reasonable_density(self) -> None:
        start = pd.Timestamp("2026-01-01 09:30:00")
        ts = [start + pd.Timedelta(minutes=15 * i) for i in range(220)]
        df = pd.DataFrame({"datetime": [t.strftime("%Y-%m-%d %H:%M:%S") for t in ts], "close": [1.0] * len(ts)})
        self.assertFalse(
            DataService._is_low_density_minute_result(
                df,
                span_days=120,
                period="15",
            )
        )

