import unittest

import pandas as pd

from core.data_service import DataService


class TestResampleMinute15m(unittest.TestCase):
    def test_resample_5m_to_15m_tail(self) -> None:
        # 5m bars ending at 09:35, 09:40, 09:45 -> should become one 15m bar ending 09:45
        df = pd.DataFrame(
            {
                "datetime": [
                    "2026-03-03 09:35:00",
                    "2026-03-03 09:40:00",
                    "2026-03-03 09:45:00",
                ],
                "open": [10.0, 10.5, 10.6],
                "high": [10.8, 10.9, 11.0],
                "low": [9.9, 10.2, 10.4],
                "close": [10.5, 10.6, 10.7],
                "volume": [100, 200, 300],
            }
        )
        out = DataService._resample_minute_to_minutes(df, target_minutes=15)
        self.assertEqual(len(out.index), 1)
        self.assertEqual(out.iloc[0]["datetime"], "2026-03-03 09:45:00")
        self.assertAlmostEqual(float(out.iloc[0]["open"]), 10.0)
        self.assertAlmostEqual(float(out.iloc[0]["close"]), 10.7)
        self.assertAlmostEqual(float(out.iloc[0]["high"]), 11.0)
        self.assertAlmostEqual(float(out.iloc[0]["low"]), 9.9)
        self.assertAlmostEqual(float(out.iloc[0]["volume"]), 600.0)

    def test_fetch_15m_uses_5m_resample_when_15m_is_stale_intraday(self) -> None:
        class FakeDataService(DataService):
            def _build_stock_minute_sources(self, *, symbol: str, period: str):  # type: ignore[override]
                def df_15m(_s: str, _e: str) -> pd.DataFrame:
                    # Looks "recent" under a 3-day grace rule, but is stale for intraday today.
                    return pd.DataFrame(
                        {
                            "datetime": ["2026-03-02 15:00:00"],
                            "open": [10.0],
                            "high": [10.0],
                            "low": [10.0],
                            "close": [10.0],
                            "volume": [100],
                        }
                    )

                def df_5m(_s: str, _e: str) -> pd.DataFrame:
                    return pd.DataFrame(
                        {
                            "datetime": [
                                "2026-03-03 09:50:00",
                                "2026-03-03 09:55:00",
                                "2026-03-03 10:00:00",
                            ],
                            "open": [10.0, 10.0, 10.0],
                            "high": [10.0, 10.0, 10.0],
                            "low": [10.0, 10.0, 10.0],
                            "close": [10.0, 10.0, 10.0],
                            "volume": [100, 100, 100],
                        }
                    )

                if str(period) == "15":
                    return {"fake_15m": df_15m}
                if str(period) == "5":
                    return {"fake_5m": df_5m}
                return {}

        svc = FakeDataService()
        out = svc._fetch_stock_minute(
            symbol="000603",
            start_date="2026-03-03 09:30:00",
            end_date="2026-03-03 10:00:00",
            period="15",
            preferred_source=None,
        )
        self.assertFalse(out.empty)
        self.assertEqual(out.iloc[-1]["datetime"], "2026-03-03 10:00:00")

    def test_fetch_15m_prefers_fresher_partial_over_stale_full(self) -> None:
        class FakeDataService(DataService):
            def _build_stock_minute_sources(self, *, symbol: str, period: str):  # type: ignore[override]
                def df_15m(_s: str, _e: str) -> pd.DataFrame:
                    # Covers the left side (Feb) but is stale vs end (Mar 3).
                    return pd.DataFrame(
                        {
                            "datetime": [
                                "2026-02-11 09:45:00",
                                "2026-02-28 15:00:00",
                            ],
                            "open": [10.0, 10.0],
                            "high": [10.0, 10.0],
                            "low": [10.0, 10.0],
                            "close": [10.0, 10.0],
                            "volume": [100, 100],
                        }
                    )

                if str(period) == "15":
                    return {"fake_15m": df_15m}
                if str(period) == "5":
                    return {
                        "fake_5m": lambda _s, _e: pd.DataFrame(
                            {
                                "datetime": [
                                    "2026-03-03 09:50:00",
                                    "2026-03-03 09:55:00",
                                    "2026-03-03 10:00:00",
                                ],
                                "open": [10.0, 10.0, 10.0],
                                "high": [10.0, 10.0, 10.0],
                                "low": [10.0, 10.0, 10.0],
                                "close": [10.0, 10.0, 10.0],
                                "volume": [100, 100, 100],
                            }
                        )
                    }
                return {}

        svc = FakeDataService()
        out = svc._fetch_stock_minute(
            symbol="000603",
            start_date="2026-02-11 09:30:00",
            end_date="2026-03-03 10:00:00",
            period="15",
            preferred_source=None,
        )
        self.assertFalse(out.empty)
        # Even if the 5m-only tail does not cover the left side, it must win on freshness.
        self.assertEqual(out.iloc[-1]["datetime"], "2026-03-03 10:00:00")
