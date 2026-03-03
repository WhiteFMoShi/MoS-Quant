from __future__ import annotations


def has_parquet_engine() -> bool:
    """Return True if pandas parquet IO engines are available.

    This project runs in environments where optional dependencies like `pyarrow`
    are not installed. In that case, attempting to read/write parquet will raise
    ImportError and can cause caches to look "missing" even when a pickle exists.
    """

    try:
        import pyarrow  # noqa: F401

        return True
    except Exception:
        pass
    try:
        import fastparquet  # noqa: F401

        return True
    except Exception:
        return False

