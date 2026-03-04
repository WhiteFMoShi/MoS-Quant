"""Generic on-disk cache for pandas DataFrames."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from hashlib import blake2b
from pathlib import Path
from typing import Any, Callable

import pandas as pd


class FreshnessLevel(str, Enum):
    SECOND = "second"
    MINUTE = "minute"
    DAY = "day"


class DataFormat(str, Enum):
    PARQUET = "parquet"
    PICKLE = "pickle"


@dataclass(frozen=True)
class CacheKey:
    namespace: str
    params: dict[str, Any]

    def stable_payload(self) -> str:
        return json.dumps(
            {"namespace": self.namespace, "params": self.params},
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )


@dataclass
class CacheMeta:
    version: int
    key: dict[str, Any]
    created_at: str
    data_format: str
    freshness_level: str
    freshness_bucket: str
    freshness_hash: str


class FileCache:
    """
    Efficient on-disk cache for pandas DataFrames using Parquet + JSON metadata.

    Freshness is verified by hashing (key + freshness_level + current time bucket).
    """

    def __init__(
        self,
        base_dir: str | Path = "cache/data_cache",
        now_fn: Callable[[], datetime] | None = None,
        data_format: DataFormat | str = "auto",
        meta_dir: str | Path | None = None,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        # meta.json is also cache; default to the same cache directory as data files
        self.meta_dir = Path(meta_dir) if meta_dir is not None else self.base_dir
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        self.now_fn = now_fn or (lambda: datetime.now().astimezone())
        self.data_format = self._resolve_format(data_format)

    def get_df(self, key: CacheKey, freshness: FreshnessLevel) -> pd.DataFrame | None:
        cache_id = self._cache_id(key)
        meta_path = self._meta_path(cache_id)
        if not meta_path.exists():
            return None

        meta = self._read_meta(meta_path)
        if meta is None:
            return None

        if meta.freshness_level != freshness.value:
            return None

        bucket = self._bucket(self.now_fn(), freshness)
        expected_hash = self._freshness_hash(key, freshness, bucket)
        if meta.freshness_hash != expected_hash:
            return None

        try:
            data_path = self._data_path(cache_id, DataFormat(meta.data_format))
            if not data_path.exists():
                return None
            return self._read_df(data_path, DataFormat(meta.data_format))
        except Exception:
            return None

    def set_df(self, key: CacheKey, freshness: FreshnessLevel, df: pd.DataFrame) -> Path:
        cache_id = self._cache_id(key)
        data_path = self._data_path(cache_id, self.data_format)
        meta_path = self._meta_path(cache_id)

        now = self.now_fn()
        bucket = self._bucket(now, freshness)
        meta = CacheMeta(
            version=1,
            key={"namespace": key.namespace, "params": key.params},
            created_at=now.isoformat(),
            data_format=self.data_format.value,
            freshness_level=freshness.value,
            freshness_bucket=bucket,
            freshness_hash=self._freshness_hash(key, freshness, bucket),
        )

        self._atomic_write_df(df, data_path, self.data_format)
        self._atomic_write_text(meta_path, json.dumps(asdict(meta), ensure_ascii=False, indent=2))
        return data_path

    def invalidate(self, key: CacheKey) -> None:
        cache_id = self._cache_id(key)
        for path in (
            self._data_path(cache_id, DataFormat.PARQUET),
            self._data_path(cache_id, DataFormat.PICKLE),
            self._meta_path(cache_id),
        ):
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    def paths(self, key: CacheKey) -> tuple[Path, Path]:
        cache_id = self._cache_id(key)
        return self._data_path(cache_id, self.data_format), self._meta_path(cache_id)

    # ---------- internals ----------
    def _cache_id(self, key: CacheKey) -> str:
        return self._hash_hex(key.stable_payload().encode("utf-8"), digest_size=16)

    def _data_path(self, cache_id: str, data_format: DataFormat) -> Path:
        suffix = ".parquet" if data_format == DataFormat.PARQUET else ".pkl"
        return self.base_dir / f"{cache_id}{suffix}"

    def _meta_path(self, cache_id: str) -> Path:
        return self.meta_dir / f"{cache_id}.meta.json"

    @staticmethod
    def _hash_hex(payload: bytes, digest_size: int = 16) -> str:
        return blake2b(payload, digest_size=digest_size).hexdigest()

    def _freshness_hash(self, key: CacheKey, freshness: FreshnessLevel, bucket: str) -> str:
        payload = f"{key.stable_payload()}|{freshness.value}|{bucket}".encode("utf-8")
        return self._hash_hex(payload, digest_size=16)

    @staticmethod
    def _bucket(now: datetime, freshness: FreshnessLevel) -> str:
        # Use local timezone-aware datetime (caller can inject now_fn if desired)
        if freshness == FreshnessLevel.SECOND:
            return now.strftime("%Y%m%d%H%M%S")
        if freshness == FreshnessLevel.MINUTE:
            return now.strftime("%Y%m%d%H%M")
        if freshness == FreshnessLevel.DAY:
            return now.strftime("%Y%m%d")
        raise ValueError(f"Unsupported freshness level: {freshness}")

    @staticmethod
    def _read_meta(path: Path) -> CacheMeta | None:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return CacheMeta(
                version=int(raw["version"]),
                key=dict(raw["key"]),
                created_at=str(raw["created_at"]),
                data_format=str(raw.get("data_format", DataFormat.PARQUET.value)),
                freshness_level=str(raw["freshness_level"]),
                freshness_bucket=str(raw["freshness_bucket"]),
                freshness_hash=str(raw["freshness_hash"]),
            )
        except Exception:
            return None

    @staticmethod
    def _atomic_write_text(path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
            tmp_path.replace(path)
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    @staticmethod
    def _atomic_write_df(df: pd.DataFrame, path: Path, data_format: DataFormat) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        # Best-effort cleanup for previous crashes
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        if data_format == DataFormat.PARQUET:
            df.to_parquet(str(tmp_path), index=False)
        else:
            df.to_pickle(str(tmp_path))
        if not tmp_path.exists():
            raise RuntimeError(f"Cache write failed: temp file missing: {tmp_path}")
        tmp_path.replace(path)

    @staticmethod
    def _read_df(path: Path, data_format: DataFormat) -> pd.DataFrame:
        if data_format == DataFormat.PARQUET:
            return pd.read_parquet(path)
        return pd.read_pickle(path)

    @staticmethod
    def _resolve_format(value: DataFormat | str) -> DataFormat:
        if isinstance(value, DataFormat):
            return value
        text = str(value).strip().lower()
        if text in {"parquet", "pickle"}:
            return DataFormat(text)
        if text != "auto":
            raise ValueError("data_format must be 'auto', 'parquet', or 'pickle'")

        if FileCache._parquet_available():
            return DataFormat.PARQUET
        return DataFormat.PICKLE

    @staticmethod
    def _parquet_available() -> bool:
        try:
            import pyarrow  # noqa: F401

            return True
        except Exception:
            pass
        try:
            import fastparquet  # type: ignore[import-not-found]  # noqa: F401

            return True
        except Exception:
            return False
