"""Excel reader built on the optional ``openpyxl`` dependency.

Install with ``pip install strata-etl[excel]``.  Every worksheet in a workbook
becomes its own :class:`Sheet` (and therefore its own blob), which is the
"dumps each sheet into multiple blobs" behaviour from the first comment.
"""

from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
from typing import Optional, Union

from ..exceptions import MissingDependencyError, ReaderError
from .base import Sheet

PathLike = Union[str, Path]


def _cell_to_str(value: object) -> Optional[str]:
    """Coerce an Excel cell value to a string, preserving readability.

    Blanks stay ``None``; integral floats lose their ``.0``; dates/times use
    ISO formatting so the structured layer can re-infer them cleanly.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return repr(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return str(value)


def _trim_trailing_empty(rows: list[list[Optional[str]]]) -> list[list[Optional[str]]]:
    """Drop fully-empty trailing rows that openpyxl often appends."""
    end = len(rows)
    while end > 0 and all(c is None or c == "" for c in rows[end - 1]):
        end -= 1
    return rows[:end]


def read_excel(path: PathLike) -> list[Sheet]:
    """Read every worksheet of an ``.xlsx``/``.xlsm`` workbook into sheets."""
    try:
        import openpyxl
    except ImportError as exc:  # pragma: no cover - exercised only without openpyxl
        raise MissingDependencyError(
            "reading Excel files requires openpyxl; install with "
            "`pip install strata-etl[excel]`"
        ) from exc

    p = Path(path)
    try:
        workbook = openpyxl.load_workbook(p, read_only=True, data_only=True)
    except Exception as exc:  # openpyxl raises a variety of errors
        raise ReaderError(f"could not read Excel workbook {p}: {exc}") from exc

    sheets: list[Sheet] = []
    try:
        for index, worksheet in enumerate(workbook.worksheets):
            rows = [
                [_cell_to_str(cell) for cell in row]
                for row in worksheet.iter_rows(values_only=True)
            ]
            sheets.append(Sheet(name=worksheet.title, index=index, rows=_trim_trailing_empty(rows)))
    finally:
        workbook.close()
    return sheets
