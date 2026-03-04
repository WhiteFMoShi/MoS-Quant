from __future__ import annotations

import re


_SPACE_RE = re.compile(r"\s+")
_ASCII_ALNUM_RE = re.compile(r"[a-z0-9]+", re.IGNORECASE)


def normalize_name(text: str) -> str:
    return _SPACE_RE.sub("", str(text)).strip().lower()


def name_to_abbr(name: str) -> str:
    """
    Convert a stock name into an ASCII abbreviation, mainly for pinyin initials.

    Examples (when `pypinyin` is installed):
    - 平安银行 -> payh
    - 招商银行 -> zsyh
    - *ST国华 -> stgh

    If `pypinyin` is not installed, this falls back to keeping ASCII letters/digits only
    (abbreviation matching for pure Chinese names will be unavailable).
    """
    raw = normalize_name(name)

    try:
        from pypinyin import Style, pinyin  # type: ignore

        parts = pinyin(raw, style=Style.FIRST_LETTER, errors=lambda x: list(x))
        buf: list[str] = []
        for item in parts:
            if not item:
                continue
            token = str(item[0])
            for ch in token:
                if ch.isascii() and ch.isalnum():
                    buf.append(ch.lower())
        return "".join(buf)
    except Exception:
        # Best-effort fallback without external deps.
        return "".join(ch.lower() for ch in raw if ch.isascii() and ch.isalnum())


def is_abbr_query(query: str) -> bool:
    q = normalize_name(query)
    return bool(q) and _ASCII_ALNUM_RE.fullmatch(q) is not None

