from __future__ import annotations

import os
import time
import unittest

from core.data_service import DataService, FetchRequest


class TestNetworkBenchOptional(unittest.TestCase):
    @unittest.skipUnless(os.getenv("RUN_NETWORK_BENCH") == "1", "set RUN_NETWORK_BENCH=1 to enable")
    def test_network_fetch_budget(self) -> None:
        budget = float(os.getenv("NETWORK_BUDGET_SECONDS", "1.0"))
        svc = DataService()
        req = FetchRequest(
            dataset="stock_daily",
            symbol="000001",
            start_date="2026-03-01",
            end_date="2026-03-03",
            single_date="",
            force_refresh=True,
            period="15",
        )
        t0 = time.perf_counter()
        _ = svc.fetch(req)
        t1 = time.perf_counter()
        self.assertLess(t1 - t0, budget)

