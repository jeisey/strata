"""Schema interpretation for the structured layer.

Once a blob is recognised as *table-like*, we can interpret its metadata --
column names, inferred types, row counts -- exactly as the second commenter
describes:

    "there is a csv blob here and it has the columns xyz, column x is a integer
     column y is a datetime etc. it has x total rows etc."

``StructuredBlob`` wraps a raw :class:`~strata.models.Blob` and exposes it as
records (dicts keyed by column name), which is what handlers consume.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from datetime import date, datetime
from typing import Optional

from .models import Blob, ColumnMeta

_INT_RE = re.compile(r"^[+-]?\d+$")
_HAS_DIGIT_RE = re.compile(r"\d")

# Type lattice, from most specific to least.  Aggregation walks down this list.
_BOOL_LITERALS = {"true", "false"}


def infer_value_type(value: Optional[str]) -> str:
    """Classify a single cell string into a coarse type label.

    Returns one of: ``empty``, ``bool``, ``int``, ``float``, ``date``,
    ``datetime``, ``str``.
    """
    if value is None:
        return "empty"
    text = value.strip()
    if text == "":
        return "empty"

    if text.lower() in _BOOL_LITERALS:
        return "bool"

    if _INT_RE.match(text):
        return "int"

    if _HAS_DIGIT_RE.search(text):
        try:
            float(text)
            return "float"
        except ValueError:
            pass

    # date.fromisoformat is strict (YYYY-MM-DD); datetime.fromisoformat is wider.
    try:
        date.fromisoformat(text)
        return "date"
    except ValueError:
        pass
    try:
        datetime.fromisoformat(text)
        return "datetime"
    except ValueError:
        pass

    return "str"


def infer_column_type(values: Iterable[Optional[str]]) -> tuple[str, int, bool]:
    """Infer a column's type from its values.

    Returns ``(inferred_type, n_nonnull, nullable)``.  The column type is the
    most specific type consistent with *every* non-empty value; mixed columns
    fall back to ``str``.
    """
    seen: set[str] = set()
    n_nonnull = 0
    n_total = 0
    for v in values:
        n_total += 1
        t = infer_value_type(v)
        if t == "empty":
            continue
        n_nonnull += 1
        seen.add(t)

    nullable = n_nonnull < n_total
    if not seen:
        return "empty", 0, True

    if seen == {"bool"}:
        col_type = "bool"
    elif seen <= {"int"}:
        col_type = "int"
    elif seen <= {"int", "float"}:
        col_type = "float"
    elif seen <= {"date"}:
        col_type = "date"
    elif seen <= {"date", "datetime"}:
        col_type = "datetime"
    else:
        col_type = "str"
    return col_type, n_nonnull, nullable


def _unique_names(header: list[Optional[str]]) -> list[str]:
    """Make safe, unique column names for record dicts.

    Blank headers become ``column_<n>``; duplicates get a ``__<n>`` suffix.
    """
    names: list[str] = []
    counts: dict[str, int] = {}
    for i, raw in enumerate(header):
        name = "" if raw is None else str(raw).strip()
        if name == "":
            name = f"column_{i}"
        if name in counts:
            counts[name] += 1
            name = f"{name}__{counts[name]}"
        else:
            counts[name] = 0
        names.append(name)
    return names


class StructuredBlob:
    """A table-like view over a raw blob.

    Parameters
    ----------
    blob:
        The raw :class:`~strata.models.Blob`.
    header_row:
        Index of the header row (line #1 by default).
    """

    def __init__(self, blob: Blob, header_row: int = 0):
        self.blob = blob
        self.header_row = header_row
        raw_header = blob.header(header_row)
        self.raw_header: list[str] = [("" if c is None else str(c)) for c in raw_header]
        self.columns_names: list[str] = _unique_names(raw_header)
        self._data_rows: list[list[Optional[str]]] = blob.data[header_row + 1 :]
        self._schema: Optional[list[ColumnMeta]] = None

    @property
    def blob_id(self) -> str:
        return self.blob.blob_id

    @property
    def source_id(self) -> str:
        return self.blob.source_id

    @property
    def n_rows(self) -> int:
        """Number of data rows (excluding the header)."""
        return len(self._data_rows)

    @property
    def n_columns(self) -> int:
        return len(self.columns_names)

    @property
    def rows(self) -> list[list[Optional[str]]]:
        """The raw data rows (excluding the header)."""
        return self._data_rows

    def records(self) -> Iterator[dict[str, Optional[str]]]:
        """Yield each data row as a dict keyed by (unique) column name."""
        names = self.columns_names
        width = len(names)
        for row in self._data_rows:
            # Pad/truncate ragged rows to the header width.
            padded = list(row[:width]) + [None] * (width - len(row))
            yield dict(zip(names, padded))

    @property
    def schema(self) -> list[ColumnMeta]:
        """Inferred per-column metadata (cached)."""
        if self._schema is None:
            self._schema = self._build_schema()
        return self._schema

    def _build_schema(self) -> list[ColumnMeta]:
        cols: list[ColumnMeta] = []
        for i, name in enumerate(self.columns_names):
            values = [row[i] if i < len(row) else None for row in self._data_rows]
            col_type, n_nonnull, nullable = infer_column_type(values)
            sample = next((v for v in values if v not in (None, "")), None)
            cols.append(
                ColumnMeta(
                    name=name,
                    index=i,
                    inferred_type=col_type,
                    nullable=nullable,
                    n_nonnull=n_nonnull,
                    sample=sample,
                )
            )
        return cols
