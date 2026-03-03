from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import akshare as ak
import pandas as pd

from core.cache_paths import legacy_cache_roots
from core.parquet_compat import has_parquet_engine


@dataclass(frozen=True)
class SymbolData:
    records: list[tuple[str, str]]
    candidates: list[str]
    name_to_code: dict[str, str]


class SymbolService:
    def __init__(self, cache_dir: Path) -> None:
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._parquet_path = self._cache_dir / "a_share_codes.parquet"
        self._pickle_path = self._cache_dir / "a_share_codes.pkl"
        self._adopt_legacy_cache_files("symbols")

    def load_cache(self) -> SymbolData | None:
        if self._pickle_path.exists():
            candidates: list[Path] = [self._pickle_path]
        else:
            candidates = []
        if self._parquet_path.exists() and has_parquet_engine():
            candidates.append(self._parquet_path)
        if not candidates:
            return None
        for path in candidates:
            try:
                df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_pickle(path)
                symbol_data = self._to_symbol_data(df)
                if symbol_data is not None and symbol_data.records:
                    return symbol_data
            except Exception:
                continue
        return None

    def save_cache(self, symbol_data: SymbolData) -> None:
        if not symbol_data.records:
            return
        df = pd.DataFrame(symbol_data.records, columns=["code", "name"])
        try:
            df.to_pickle(self._pickle_path)
        except Exception:
            pass
        if has_parquet_engine():
            try:
                df.to_parquet(self._parquet_path, index=False)
            except Exception:
                try:
                    if self._parquet_path.exists():
                        self._parquet_path.unlink()
                except Exception:
                    pass

    def fetch_remote(self) -> SymbolData:
        errors: list[str] = []
        loaders = (
            ("stock_info_a_code_name", lambda: ak.stock_info_a_code_name()),
            ("stock_zh_a_spot_em", lambda: self._extract_code_name_df(ak.stock_zh_a_spot_em())),
            ("stock_zh_a_spot", lambda: self._extract_code_name_df(ak.stock_zh_a_spot())),
        )
        for source_name, loader in loaders:
            try:
                df = loader()
                symbol_data = self._to_symbol_data(df)
                if symbol_data is not None and symbol_data.records:
                    return symbol_data
                errors.append(f"{source_name}:empty")
            except Exception as exc:
                errors.append(f"{source_name}:{exc}")
        detail = " | ".join(errors) if errors else "no_source"
        raise RuntimeError(f"股票列表为空: {detail}")

    @staticmethod
    def build_suggestions(query: str, records: list[tuple[str, str]], limit: int = 20) -> list[str]:
        if not records:
            return []

        query_lower = query.lower()
        starts: list[str] = []
        contains: list[str] = []

        for code, name in records:
            candidate = f"{code} {name}"
            code_lower = code.lower()
            name_lower = name.lower()
            if code_lower.startswith(query_lower) or name_lower.startswith(query_lower):
                starts.append(candidate)
            elif query_lower in code_lower or query_lower in name_lower:
                contains.append(candidate)

        combined = starts + contains
        return combined[:limit]

    @staticmethod
    def resolve_code(raw_text: str, name_to_code: dict[str, str]) -> str:
        text = raw_text.strip()
        if not text:
            return ""
        first = text.split()[0]
        if first.isdigit():
            return first
        if first in name_to_code:
            return name_to_code[first]
        if text in name_to_code:
            return name_to_code[text]
        return first

    @staticmethod
    def _to_symbol_data(df: pd.DataFrame | None) -> SymbolData | None:
        if df is None or df.empty or "code" not in df.columns or "name" not in df.columns:
            return None

        records: list[tuple[str, str]] = []
        candidates: list[str] = []
        name_to_code: dict[str, str] = {}

        for row in df[["code", "name"]].dropna().itertuples(index=False):
            code = str(row[0]).strip()
            name = str(row[1]).strip()
            if not code:
                continue
            records.append((code, name))
            candidates.append(f"{code} {name}")
            if name:
                name_to_code[name] = code

        if not records:
            return None
        return SymbolData(records=records, candidates=candidates, name_to_code=name_to_code)

    @staticmethod
    def _extract_code_name_df(df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["code", "name"])
        code_col = None
        name_col = None
        for candidate in ("代码", "证券代码", "symbol", "code"):
            if candidate in df.columns:
                code_col = candidate
                break
        for candidate in ("名称", "证券名称", "name"):
            if candidate in df.columns:
                name_col = candidate
                break
        if code_col is None or name_col is None:
            return pd.DataFrame(columns=["code", "name"])
        out = df[[code_col, name_col]].copy()
        out.columns = ["code", "name"]
        out["code"] = out["code"].astype(str).str.strip().str.replace(".0", "", regex=False)
        out["code"] = out["code"].where(out["code"].str.isdigit(), out["code"])
        out["code"] = out["code"].apply(lambda x: x.zfill(6) if isinstance(x, str) and x.isdigit() else x)
        out["name"] = out["name"].astype(str).str.strip()
        out = out[(out["code"] != "") & (out["name"] != "")]
        out = out.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
        return out

    def _adopt_legacy_cache_files(self, subdir: str) -> None:
        for legacy_root in legacy_cache_roots():
            legacy_dir = legacy_root / subdir
            if not legacy_dir.exists():
                continue
            for file_path in legacy_dir.glob("*"):
                if not file_path.is_file() or file_path.suffix not in {".parquet", ".pkl"}:
                    continue
                target = self._cache_dir / file_path.name
                if target.exists():
                    continue
                try:
                    file_path.replace(target)
                except Exception:
                    continue
            self._cleanup_empty_dir(legacy_dir)
            self._cleanup_empty_dir(legacy_root)

    @staticmethod
    def _cleanup_empty_dir(path: Path) -> None:
        try:
            if path.exists() and path.is_dir() and not any(path.iterdir()):
                path.rmdir()
        except Exception:
            pass
