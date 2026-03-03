import unittest

import pandas as pd

import core.data_service as data_service


class TestMinuteCheckpointForPeriod(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_now = data_service.pd.Timestamp.now
        self._orig_is_trade_day = data_service.DataService._is_trade_day
        self._orig_previous_trade_day = data_service.DataService._previous_trade_day

        data_service.DataService._is_trade_day = classmethod(lambda cls, day: True)
        data_service.DataService._previous_trade_day = classmethod(lambda cls, day: pd.Timestamp(day).normalize() - pd.Timedelta(days=1))

    def tearDown(self) -> None:
        data_service.pd.Timestamp.now = self._orig_now
        data_service.DataService._is_trade_day = self._orig_is_trade_day
        data_service.DataService._previous_trade_day = self._orig_previous_trade_day

    def _set_now(self, ts: str) -> None:
        fixed = pd.Timestamp(ts)
        data_service.pd.Timestamp.now = lambda *args, **kwargs: fixed

    def test_lunch_break_returns_1130(self) -> None:
        self._set_now("2026-03-03 12:10:00")
        self.assertEqual(data_service.DataService._minute_checkpoint_for_period("1"), "202603031130")
        self.assertEqual(data_service.DataService._minute_checkpoint_for_period("15"), "202603031130")

    def test_15m_early_afternoon_before_first_bar(self) -> None:
        self._set_now("2026-03-03 13:10:00")
        self.assertEqual(data_service.DataService._minute_checkpoint_for_period("15"), "202603031130")

    def test_15m_after_first_bar(self) -> None:
        self._set_now("2026-03-03 13:16:00")
        self.assertEqual(data_service.DataService._minute_checkpoint_for_period("15"), "202603031315")

    def test_5m_before_first_bar(self) -> None:
        self._set_now("2026-03-03 09:32:00")
        self.assertEqual(data_service.DataService._minute_checkpoint_for_period("5"), "202603021500")

    def test_5m_after_first_bar(self) -> None:
        self._set_now("2026-03-03 09:36:00")
        self.assertEqual(data_service.DataService._minute_checkpoint_for_period("5"), "202603030935")
