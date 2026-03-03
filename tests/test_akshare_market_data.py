import unittest

import pandas as pd

from mos_quant import AKShareStockFetcher, DataFetchError, FetchConfig


class TestAKShareMarketData(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fetcher = AKShareStockFetcher(
            FetchConfig(
                retries=2,
                retry_interval=1.0,
                enable_cache=False,
            )
        )

    def _assert_non_empty_dataframe(self, df: pd.DataFrame, case_name: str) -> None:
        self.assertIsInstance(df, pd.DataFrame, f"{case_name} must return pandas.DataFrame")
        self.assertFalse(df.empty, f"{case_name} returned empty DataFrame")

    def test_a_share_data(self) -> None:
        try:
            df = self.fetcher.get_a_hist(
                symbol="000001",
                start_date="2025-01-01",
                end_date="2025-01-20",
                period="daily",
                adjust="qfq",
            )
        except DataFetchError as exc:
            self.fail(f"A股数据获取失败: {exc}")
        self._assert_non_empty_dataframe(df, "A股历史行情")

    def test_index_data(self) -> None:
        try:
            df = self.fetcher.get_index_spot(symbol="上证系列指数")
        except DataFetchError as exc:
            self.fail(f"指数数据获取失败: {exc}")
        self._assert_non_empty_dataframe(df, "指数实时行情")

    def test_etf_data(self) -> None:
        try:
            df = self.fetcher.get_etf_spot()
        except DataFetchError as exc:
            self.fail(f"ETF数据获取失败: {exc}")
        self._assert_non_empty_dataframe(df, "ETF实时行情")

    def test_trade_calendar(self) -> None:
        try:
            df = self.fetcher.get_trade_calendar()
        except DataFetchError as exc:
            self.fail(f"交易日历获取失败: {exc}")
        self._assert_non_empty_dataframe(df, "交易日历")


if __name__ == "__main__":
    unittest.main(verbosity=2)
