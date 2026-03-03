"""MoS Quant core package.

This package contains:
- data: upstream data source probing and AkShare fetcher
- cache: local dataframe cache
- core: loader/bootstrapping logic

UI code lives in mos_quant.ui and is intentionally not imported here.
"""

from typing import TYPE_CHECKING

from .cache.store import CacheKey, DataFormat, FileCache, FreshnessLevel
from .core.loader import LoaderConfig, LoaderContext, LoaderError, MoSQuantLoader
from .data.akshare_fetcher import AKShareStockFetcher, DataFetchError, FetchConfig, SourceRoute

if TYPE_CHECKING:
    from .data.network_probe import (
        DataSourceProbe,
        ProbeResult,
        ProbeTarget,
        ProviderProbeResult,
        SingleUrlProbeResult,
    )

__all__ = [
    "AKShareStockFetcher",
    "FetchConfig",
    "DataFetchError",
    "SourceRoute",
    "FileCache",
    "CacheKey",
    "FreshnessLevel",
    "DataFormat",
    "MoSQuantLoader",
    "LoaderConfig",
    "LoaderContext",
    "LoaderError",
    "DataSourceProbe",
    "ProbeTarget",
    "ProbeResult",
    "ProviderProbeResult",
    "SingleUrlProbeResult",
]


def __getattr__(name: str):
    if name in {
        "DataSourceProbe",
        "ProbeTarget",
        "ProbeResult",
        "ProviderProbeResult",
        "SingleUrlProbeResult",
    }:
        from .data import network_probe

        return getattr(network_probe, name)
    raise AttributeError(f"module 'mos_quant' has no attribute '{name}'")
