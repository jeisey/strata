"""Signature hashing.

The "signature" of a blob is an MD5 hash of its header row -- line #1, the
column names.  Two blobs that share the same column headers share the same
signature, which is what lets the framework recognise a known shape even as
other files drift over time (new sheets, reordered files, etc.).

This is a direct port of the classic SQL ``MD5(header_line)`` trick to Python,
with a couple of normalisation knobs so cosmetic differences (trailing
whitespace, capitalisation) do not accidentally fork a signature.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Optional

# Unit Separator: extremely unlikely to appear inside a real column name, so it
# is a safe delimiter when joining header cells before hashing.  Using a plain
# comma would let ``["a,b"]`` collide with ``["a", "b"]``.
_FIELD_SEP = "\x1f"


def normalize_header(
    cells: Iterable[object],
    *,
    lower: bool = False,
    strip: bool = True,
    collapse_whitespace: bool = True,
) -> list[str]:
    """Return a normalised list of header cell strings.

    Parameters
    ----------
    cells:
        The raw cells of the header row (any objects; coerced to ``str``).
    lower:
        Lower-case each header cell. Off by default so signatures stay
        case-sensitive unless you opt in.
    strip:
        Strip surrounding whitespace from each cell.
    collapse_whitespace:
        Collapse internal runs of whitespace to a single space.
    """
    out: list[str] = []
    for cell in cells:
        text = "" if cell is None else str(cell)
        if strip:
            text = text.strip()
        if collapse_whitespace:
            text = " ".join(text.split())
        if lower:
            text = text.lower()
        out.append(text)
    return out


def header_signature(
    cells: Iterable[object],
    *,
    lower: bool = False,
    strip: bool = True,
    collapse_whitespace: bool = True,
) -> str:
    """Compute the MD5 signature hash for a header row.

    Returns a 32-character lowercase hex digest.  The same set of normalisation
    options must be used consistently for signatures to line up over time.
    """
    normalized = normalize_header(
        cells, lower=lower, strip=strip, collapse_whitespace=collapse_whitespace
    )
    joined = _FIELD_SEP.join(normalized)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def content_hash(data: bytes) -> str:
    """Return the MD5 hex digest of raw file bytes (used to dedupe ingests)."""
    return hashlib.md5(data).hexdigest()


def new_guid() -> str:
    """Return a fresh GUID (uuid4 hex) for source files, blobs, and outputs."""
    import uuid

    return uuid.uuid4().hex


def short_guid(guid: Optional[str], length: int = 8) -> str:
    """Return a short, display-friendly prefix of a GUID."""
    if not guid:
        return ""
    return guid[:length]
