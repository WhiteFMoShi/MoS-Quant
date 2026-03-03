from __future__ import annotations

import unittest

import pandas as pd

from core.data_service import DataService


class TestMinuteFreshnessPolicy(unittest.TestCase):
    def test_15m_same_session_gap_ok(self) -> None:
        now = pd.Timestamp("2026-03-03 15:30:00")
        end_ts = pd.Timestamp("2026-03-03 15:00:00")
        tail_ts = pd.Timestamp("2026-03-03 14:45:00")
        self.assertTrue(
            DataService._minute_fresh_enough_for_end(
                tail_ts=tail_ts,
                end_ts=end_ts,
                period="15",
                now=now,
            )
        )

    def test_intraday_today_requires_today_tail(self) -> None:
        # During market open, returning only yesterday's minutes should be considered stale.
        now = pd.Timestamp("2026-03-03 10:05:00")
        end_ts = pd.Timestamp("2026-03-03 10:00:00")
        tail_ts = pd.Timestamp("2026-03-02 15:00:00")
        self.assertFalse(
            DataService._minute_fresh_enough_for_end(
                tail_ts=tail_ts,
                end_ts=end_ts,
                period="15",
                now=now,
            )
        )

    def test_before_open_allows_previous_trade_day(self) -> None:
        now = pd.Timestamp("2026-03-03 08:30:00")
        end_ts = pd.Timestamp("2026-03-03 09:00:00")
        tail_ts = pd.Timestamp("2026-03-02 15:00:00")
        self.assertTrue(
            DataService._minute_fresh_enough_for_end(
                tail_ts=tail_ts,
                end_ts=end_ts,
                period="15",
                now=now,
            )
        )

    def test_weekend_end_allows_previous_trade_day(self) -> None:
        # End day is a Saturday; Friday close should be acceptable.
        now = pd.Timestamp("2026-03-09 10:00:00")  # Monday
        end_ts = pd.Timestamp("2026-03-07 15:00:00")  # Saturday
        tail_ts = pd.Timestamp("2026-03-06 15:00:00")  # Friday
        self.assertTrue(
            DataService._minute_fresh_enough_for_end(
                tail_ts=tail_ts,
                end_ts=end_ts,
                period="15",
                now=now,
            )
        )

