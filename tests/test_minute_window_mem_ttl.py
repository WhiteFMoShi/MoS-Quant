import unittest

import pandas as pd

import core.data_service as data_service


class TestMinuteWindowMemTTL(unittest.TestCase):
    def test_refreshes_after_ttl(self) -> None:
        svc = data_service.DataService()
        key = ("tx_stock", "sz000603", "15")

        calls = {"n": 0}

        def loader():
            calls["n"] += 1
            return pd.DataFrame(
                {
                    "datetime": ["2026-03-03 13:30:00"],
                    "open": [1.0],
                    "close": [1.0],
                    "high": [1.0],
                    "low": [1.0],
                    "volume": [1.0],
                }
            )

        # prime
        df1 = svc._get_minute_window_cached(key=key, loader=loader)
        self.assertEqual(calls["n"], 1)
        self.assertFalse(df1.empty)

        # within TTL should reuse
        df2 = svc._get_minute_window_cached(key=key, loader=loader)
        self.assertEqual(calls["n"], 1)
        self.assertFalse(df2.empty)

        # force expire by rewinding cached timestamp
        cached_df, cached_ts = svc._minute_window_mem[key]
        svc._minute_window_mem[key] = (cached_df, float(cached_ts) - float(svc._minute_window_mem_ttl_seconds) - 1.0)
        df3 = svc._get_minute_window_cached(key=key, loader=loader)
        self.assertEqual(calls["n"], 2)
        self.assertFalse(df3.empty)

