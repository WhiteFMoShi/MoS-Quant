from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    # mos_quant/cache/paths.py -> <root>/mos_quant/cache/paths.py
    return Path(__file__).resolve().parents[2]


def cache_root() -> Path:
    return project_root() / "cache"


def legacy_cache_roots() -> list[Path]:
    root = project_root()
    return [
        root / "GUI" / "cache",
        root / "GUI" / "windows" / "cache",
    ]

