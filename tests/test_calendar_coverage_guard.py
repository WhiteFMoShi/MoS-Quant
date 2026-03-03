from __future__ import annotations

import unittest

import pandas as pd

from core.data_service import DataService


class TestCalendarCoverageGuard(unittest.TestCase):
    def setUp(self) -> None:
        self._old_dates = DataService._TRADE_DATES
        self._old_max = DataService._TRADE_DATES_MAX_DAY
        DataService._TRADE_DATES = set()
        DataService._TRADE_DATES_MAX_DAY = pd.Timestamp("2025-12-31")

    def tearDown(self) -> None:
        DataService._TRADE_DATES = self._old_dates
        DataService._TRADE_DATES_MAX_DAY = self._old_max

    def test_minute_guard_allows_small_gap_after_calendar_end(self) -> None:
        # Calendar ends at 2025-12-31; requesting 2026-01-02 should not force stale
        # if tail is 2025-12-31 and the gap is small.
        now = pd.Timestamp("2026-03-03 12:00:00")
        self.assertTrue(
            DataService._minute_fresh_enough_for_end(
                tail_ts=pd.Timestamp("2025-12-31 15:00:00"),
                end_ts=pd.Timestamp("2026-01-02 10:00:00"),
                period="15",
                now=now,
            )
        )

    def test_daily_guard_allows_small_gap_after_calendar_end(self) -> None:
        df = pd.DataFrame({"date": ["2025-12-31"], "close": [1.0]})
        self.assertTrue(
            DataService._is_result_fresh_for_request_end(
                df,
                end_value="20260102",
                period="daily",
                minute_mode=False,
            )
        )

