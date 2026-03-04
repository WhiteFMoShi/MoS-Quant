import unittest

from mos_quant.core.stock_search import is_abbr_query, name_to_abbr


class TestStockSearch(unittest.TestCase):
    def test_is_abbr_query(self) -> None:
        self.assertTrue(is_abbr_query("payh"))
        self.assertTrue(is_abbr_query("000001"))
        self.assertFalse(is_abbr_query("平安银行"))
        self.assertTrue(is_abbr_query("pa yh"))

    def test_name_to_abbr_ascii_fallback(self) -> None:
        # Even without pypinyin installed, ASCII parts should be preserved.
        abbr = name_to_abbr("*ST国华")
        self.assertTrue(abbr.startswith("st"))
        self.assertTrue(abbr.isascii())
        self.assertTrue(abbr.isalnum())


if __name__ == "__main__":
    unittest.main(verbosity=2)
