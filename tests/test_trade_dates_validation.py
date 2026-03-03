from __future__ import annotations

import unittest

import pandas as pd

from core.data_service import DataService


class TestTradeDatesValidation(unittest.TestCase):
    def test_rejects_stale_calendar(self) -> None:
        now = pd.Timestamp("2026-03-03 12:00:00")
        ts = pd.date_range("1990-12-19", "2025-12-31", freq="B")
        self.assertFalse(DataService._trade_dates_look_valid(pd.Series(ts), now=now))

    def test_rejects_sparse_recent_year(self) -> None:
        now = pd.Timestamp("2026-03-03 12:00:00")
        # Too few days in 2025 (last full year): only one per week.
        ts_2025 = pd.date_range("2025-01-01", "2025-12-31", freq="7D")
        ts_2026 = pd.date_range("2026-01-05", "2026-03-03", freq="B")
        ts = pd.to_datetime(list(ts_2025) + list(ts_2026))
        self.assertFalse(DataService._trade_dates_look_valid(pd.Series(ts), now=now))

    def test_accepts_reasonable_calendar(self) -> None:
        now = pd.Timestamp("2026-03-03 12:00:00")
        # Needs enough history to pass structural checks (>=3000 rows).
        ts = pd.date_range("2010-01-01", "2026-12-31", freq="B")
        self.assertTrue(DataService._trade_dates_look_valid(pd.Series(ts), now=now))
