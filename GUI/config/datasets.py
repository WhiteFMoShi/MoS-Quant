from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetOption:
    label: str
    value: str
    hint: str
    requires_symbol: bool


DATASET_OPTIONS = [
    DatasetOption("A股日线", "stock_daily", "股票行情数据", True),
    DatasetOption("指数日线", "index_daily", "指数行情数据", True),
    DatasetOption("深交所总貌", "szse_summary", "按单日抓取，不依赖代码", False),
]

DATASET_BY_VALUE = {option.value: option for option in DATASET_OPTIONS}
