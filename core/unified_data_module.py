from __future__ import annotations

import threading
from typing import Iterable

from core.data_service import DataService, FetchRequest, FetchResponse


class UnifiedDataModule:
    """
    Unified data access entry:
    - centralizes cache+network policy inside DataService
    - provides dataset auto-classification for market symbols
    """

    _instance: "UnifiedDataModule | None" = None
    _instance_lock = threading.Lock()

    KNOWN_INDEX_CODES = {
        "000001",
        "000016",
        "000300",
        "000688",
        "000852",
        "000905",
        "399001",
        "399005",
        "399006",
        "399300",
        "399905",
        "399673",
        "980017",
    }

    def __init__(self) -> None:
        self._service = DataService()

    @classmethod
    def instance(cls) -> "UnifiedDataModule":
        if cls._instance is not None:
            return cls._instance
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance

    def fetch(
        self,
        request: FetchRequest,
        *,
        progress_cb=None,
    ) -> FetchResponse:
        return self._service.fetch(request, progress_cb=progress_cb)

    @classmethod
    def resolve_market_dataset(
        cls,
        symbol: str,
        *,
        stock_codes: Iterable[str] | None = None,
        fallback: str = "index_daily",
    ) -> str:
        raw = str(symbol or "").strip().lower()
        if not raw:
            return fallback
        if "指数" in raw:
            return "index_daily"

        code = raw.replace("sh", "").replace("sz", "").replace("bj", "")
        if not code.isdigit():
            return fallback

        stock_code_set = {str(c).strip() for c in (stock_codes or []) if str(c).strip()}
        in_stock_pool = code in stock_code_set
        in_index_pool = code in cls.KNOWN_INDEX_CODES or code.startswith(("399", "980"))

        # Prefix + known benchmark code should prefer index semantics.
        if raw.startswith(("sh", "sz")) and code in cls.KNOWN_INDEX_CODES:
            return "index_daily"

        if in_stock_pool and not in_index_pool:
            return "stock_daily"
        if in_index_pool and not in_stock_pool:
            return "index_daily"

        # Ambiguous code (e.g. 000001): default to stock for user-entered stock symbols.
        if in_stock_pool:
            return "stock_daily"

        # Heuristic fallback by code pattern.
        if code.startswith(("399", "980")):
            return "index_daily"
        if code.startswith(("600", "601", "603", "605", "688", "000", "001", "002", "003", "300", "301", "830", "831", "832", "833", "835", "836", "837", "838", "839")):
            return "stock_daily"
        return fallback
